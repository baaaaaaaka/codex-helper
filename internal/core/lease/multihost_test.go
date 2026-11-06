//go:build unix

// The multi-host drill freezes contender processes with SIGSTOP/SIGCONT,
// which exist only on Unix.

package lease

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Multi-host contention on a directory that several machines share (an
// NFS export mounted by several containers; see TAKEOVER-REPORT.md). It is
// skipped unless LEASE_MULTIHOST_DIR is set.
//
//	LEASE_MULTIHOST_DIR=/mnt/n/run LEASE_MULTIHOST_HOST=host-a \
//	  LEASE_MULTIHOST_SECONDS=120 go test -run TestMultiHostContention
//
// Each machine runs this test; it starts LEASE_MULTIHOST_PROCS contender
// processes (default 2) and, at random, freezes one of them (SIGSTOP) for
// 0.5 to 6 s, longer than the stale bound (3 s) about half the time. A
// contender takes the lease, waits out the plan's quiet time, activates,
// and then, for a random tenure, calls Check every 20 ms and logs each
// success with the time taken before the call. It then releases, or
// abandons the lease as a crash would (its renewals stop, its process
// lives on). Every event goes to a log of its own in the directory.
//
// LEASE_MULTIHOST_VERIFY=1 checks the logs of all machines (the clocks
// must be one clock: containers of one kernel):
//   - each generation was won by one contender, and activated by it at most
//     once;
//   - every successful Check of generation g started before the first
//     activation of any later generation: no two owners ever act at once;
//   - the lease kept moving: activations by every machine.
func TestMultiHostContention(t *testing.T) {
	dir := os.Getenv("LEASE_MULTIHOST_DIR")
	if dir == "" {
		t.Skip("LEASE_MULTIHOST_DIR not set")
	}
	switch {
	case os.Getenv("LEASE_MULTIHOST_VERIFY") == "1":
		verifyMultiHost(t, dir)
	case os.Getenv("LEASE_MULTIHOST_CHILD") != "":
		contend(t, dir)
	default:
		superviseContenders(t, dir)
	}
}

const multiHostStale = 3 * time.Second

type mhEvent struct {
	Ev   string `json:"ev"`
	ID   string `json:"id"`
	Host string `json:"host"`
	Gen  int64  `json:"gen"`
	T    int64  `json:"t"` // UnixNano
	Note string `json:"note,omitempty"`
}

func superviseContenders(t *testing.T, dir string) {
	host := os.Getenv("LEASE_MULTIHOST_HOST")
	secs, _ := strconv.Atoi(os.Getenv("LEASE_MULTIHOST_SECONDS"))
	if secs <= 0 {
		secs = 60
	}
	procs, _ := strconv.Atoi(os.Getenv("LEASE_MULTIHOST_PROCS"))
	if procs <= 0 {
		procs = 2
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	var cmds []*exec.Cmd
	for i := range procs {
		cmd := exec.Command(os.Args[0], "-test.run=^TestMultiHostContention$", "-test.v")
		cmd.Env = append(os.Environ(),
			"LEASE_MULTIHOST_CHILD="+fmt.Sprintf("%s-%d", host, i),
			"LEASE_MULTIHOST_DEADLINE="+strconv.FormatInt(deadline.UnixNano(), 10))
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cmds = append(cmds, cmd)
	}
	freezes := 0
	for time.Now().Before(deadline.Add(-8 * time.Second)) {
		time.Sleep(time.Duration(2000+rand.IntN(4000)) * time.Millisecond)
		c := cmds[rand.IntN(len(cmds))]
		d := time.Duration(500+rand.IntN(5500)) * time.Millisecond
		_ = c.Process.Signal(syscall.SIGSTOP)
		time.Sleep(d)
		_ = c.Process.Signal(syscall.SIGCONT)
		freezes++
	}
	for _, c := range cmds {
		_ = c.Process.Signal(syscall.SIGCONT)
		if err := c.Wait(); err != nil {
			t.Errorf("contender: %v", err)
		}
	}
	t.Logf("%s: %d contenders, %d freezes", host, procs, freezes)
}

func contend(t *testing.T, dir string) {
	id := os.Getenv("LEASE_MULTIHOST_CHILD")
	host := os.Getenv("LEASE_MULTIHOST_HOST")
	dl, _ := strconv.ParseInt(os.Getenv("LEASE_MULTIHOST_DEADLINE"), 10, 64)
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, dl))
	defer cancel()
	logf, err := os.OpenFile(filepath.Join(dir, "events-"+id+".jsonl"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer logf.Close()
	w := bufio.NewWriter(logf)
	defer w.Flush()
	emit := func(ev string, gen int64, at time.Time, note string) {
		b, _ := json.Marshal(mhEvent{Ev: ev, ID: id, Host: host, Gen: gen, T: at.UnixNano(), Note: note})
		_, _ = w.Write(append(b, '\n'))
	}
	boot, _ := os.ReadFile("/proc/sys/kernel/random/boot_id")
	cfg := Config{
		Anchor: filepath.Join(dir, "core.sqlite"),
		Self:   Local(host, strings.TrimSpace(string(boot))),
		Timing: Scaled(multiHostStale),
		Wait:   true,
	}
	for ctx.Err() == nil {
		h, err := Acquire(ctx, cfg)
		if err != nil {
			if ctx.Err() == nil {
				emit("acquire-error", 0, time.Now(), err.Error())
			}
			break
		}
		emit("won", h.Gen(), time.Now(), h.Plan().Why)
		if err := h.WaitQuiet(ctx); err != nil {
			emit("lost-quiet", h.Gen(), time.Now(), err.Error())
			_ = h.Release()
			continue
		}
		if err := h.Activate(); err != nil {
			emit("lost-activate", h.Gen(), time.Now(), err.Error())
			_ = h.Release()
			continue
		}
		emit("active", h.Gen(), time.Now(), "")
		tenure := time.Now().Add(time.Duration(300+rand.IntN(5700)) * time.Millisecond)
		lost := false
		for time.Now().Before(tenure) && ctx.Err() == nil {
			t0 := time.Now()
			if err := h.Check(); err != nil {
				emit("lost", h.Gen(), time.Now(), err.Error())
				lost = true
				break
			}
			emit("ok", h.Gen(), t0, "")
			time.Sleep(20 * time.Millisecond)
		}
		if !lost {
			// Frozen past the end of its tenure, a holder finds out here.
			if err := h.Check(); err != nil {
				emit("lost", h.Gen(), time.Now(), err.Error())
				lost = true
			}
		}
		switch {
		case lost:
			_ = h.Release()
		case rand.IntN(4) == 0:
			// A crash as other machines see it: renewals stop, nothing
			// is released; the process (pid) lives on.
			h.halt()
			emit("abandoned", h.Gen(), time.Now(), "")
			time.Sleep(multiHostStale / 2)
		default:
			_ = h.Release()
			emit("released", h.Gen(), time.Now(), "")
		}
		w.Flush()
		time.Sleep(time.Duration(rand.IntN(300)) * time.Millisecond)
	}
}

func verifyMultiHost(t *testing.T, dir string) {
	files, _ := filepath.Glob(filepath.Join(dir, "events-*.jsonl"))
	if len(files) == 0 {
		t.Fatal("no event logs")
	}
	var evs []mhEvent
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if line == "" {
				continue
			}
			var e mhEvent
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			evs = append(evs, e)
		}
	}
	won := map[int64][]string{}
	active := map[int64][]mhEvent{}
	count := map[string]int{}
	activeBy := map[string]int{}
	for _, e := range evs {
		count[e.Ev]++
		switch e.Ev {
		case "won":
			won[e.Gen] = append(won[e.Gen], e.ID)
		case "active":
			active[e.Gen] = append(active[e.Gen], e)
			activeBy[e.Host]++
		case "acquire-error":
			t.Errorf("acquire error: %+v", e)
		}
	}
	for g, ids := range won {
		if len(ids) != 1 {
			t.Errorf("generation %d won by %v", g, ids)
		}
	}
	gens := make([]int64, 0, len(active))
	for g, as := range active {
		if len(as) != 1 || len(won[g]) != 1 || as[0].ID != won[g][0] {
			t.Errorf("generation %d activated %d times (won by %v)", g, len(as), won[g])
		}
		gens = append(gens, g)
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] < gens[j] })
	// firstLater[i]: the earliest activation of any generation after gens[i].
	firstLater := make([]int64, len(gens))
	next := int64(1<<63 - 1)
	for i := len(gens) - 1; i >= 0; i-- {
		firstLater[i] = next
		if t := active[gens[i]][0].T; t < next {
			next = t
		}
	}
	idx := map[int64]int{}
	for i, g := range gens {
		idx[g] = i
	}
	violations := 0
	for _, e := range evs {
		if e.Ev != "ok" {
			continue
		}
		if i, ok := idx[e.Gen]; ok && e.T >= firstLater[i] {
			violations++
			if violations <= 10 {
				t.Errorf("generation %d (%s) acted %s after a later generation became active",
					e.Gen, e.ID, time.Duration(e.T-firstLater[i]))
			}
		}
	}
	if violations > 0 {
		t.Errorf("%d checks of a superseded generation succeeded", violations)
	}
	if len(activeBy) < 2 {
		t.Errorf("activations by %v: the lease did not move between machines", activeBy)
	}
	t.Logf("events %v; generations %d; activations by machine %v", count, len(gens), activeBy)
}
