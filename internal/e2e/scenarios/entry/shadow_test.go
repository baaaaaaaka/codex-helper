package entry

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
)

// Environment of the real-data shadow run (never set in CI; the seed and
// the analysis are scripts/fidelity/shadow_prep.py and shadow_analyze.py):
//
//	E2E_SHADOW_STORE       read-only copy of the old helper's scope
//	E2E_SHADOW_SEED        seed.jsonl.gz from scripts/fidelity/shadow_prep.py
//	E2E_SHADOW_CODEX_HOME  the Codex home the helper reads: a tmpfs whose
//	                       sessions/ is the owner's sessions, mounted read-only
//	E2E_SHADOW_OUT         a writable directory for the dumps
//	E2E_SHADOW_MODE        "full" (drain, idle, restart), "kill" (SIGKILL
//	                       mid-drain, restart, drain) or "live" (full, with
//	                       an owner prompt and a local session turn injected
//	                       mid-drain: shadowLive)
//	E2E_SHADOW_IDLE        idle observation after the drain (default 10m)
//	E2E_SHADOW_KILL_AFTER  posts before the SIGKILL (kill mode, default 800)
//	E2E_SHADOW_LIVE_AFTER  posts before the injection (live mode, default 1500)
const (
	envShadowLiveAfter = "E2E_SHADOW_LIVE_AFTER"
	envShadowStore     = "E2E_SHADOW_STORE"
	envShadowSeed      = "E2E_SHADOW_SEED"
	envShadowCodexHome = "E2E_SHADOW_CODEX_HOME"
	envShadowOut       = "E2E_SHADOW_OUT"
	envShadowMode      = "E2E_SHADOW_MODE"
	envShadowIdle      = "E2E_SHADOW_IDLE"
	envShadowKillAfter = "E2E_SHADOW_KILL_AFTER"
)

const routePostMessage = "POST /chats/{chat}/messages"

// SHADOW-REALDATA: the production entry point converts a copy of the
// owner's real old store at its first start and catches up the owner's real
// Codex sessions (read-only) against a fake Graph holding every real chat
// with its real history. The run dumps what the helper posted, its request
// pattern and CPU for scripts/fidelity/shadow_analyze.py, which checks the
// per-chat expectations; this test checks what needs no oracle: Codex never
// runs, nothing is posted twice per chat beyond the history's own repeats,
// no write during the idle window or after a restart, one conversion
// summary.
func TestShadowRealData(t *testing.T) {
	src, seed, home, outDir := os.Getenv(envShadowStore), os.Getenv(envShadowSeed), os.Getenv(envShadowCodexHome), os.Getenv(envShadowOut)
	if src == "" || seed == "" || home == "" || outDir == "" {
		t.Skip("real-data shadow run only: set " + strings.Join([]string{envShadowStore, envShadowSeed, envShadowCodexHome, envShadowOut}, ", "))
	}
	mode := os.Getenv(envShadowMode)
	if mode == "" {
		mode = "full"
	}
	idle := 10 * time.Minute
	if v := os.Getenv(envShadowIdle); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatal(err)
		}
		idle = d
	}
	killAfter := 800
	if v := os.Getenv(envShadowKillAfter); v != "" {
		fmt.Sscan(v, &killAfter)
	}
	liveAfter := 1500
	if v := os.Getenv(envShadowLiveAfter); v != "" {
		fmt.Sscan(v, &liveAfter)
	}
	// The Codex home must be a throwaway tmpfs and its sessions read-only:
	// the run writes config.toml there and nothing else may be written.
	if err := shadowCheckMounts(home); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte("# e2e shadow\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}

	s := harness.NewScenario(t, "SHADOW-REALDATA", harness.Options{
		Guarantees: []string{"auto-migrate", "catch-up", "no-hot-loop"},
		Title:      "shadow run on the owner's real store and sessions: " + mode,
		Budget:     4 * time.Hour,
		NoStart:    true,
		Seam:       harness.Seam{ProductionTiming: true},
		Args:       []string{"--machine-registry"},
		UserEnv:    map[string]string{"CODEX_HOME": home},
		Golden:     harness.GoldenOptions{Skip: "opt-in shadow run on the owner's private data: its transcript is not committed"},
		Invariants: harness.Invariants{CPUBudget: 4 * time.Hour},
	})
	defer s.Finish()

	scope := filepath.Join(teamsDir(s), "scopes", filepath.Base(filepath.Clean(src)))
	copyStart := time.Now()
	if err := copyTree(src, scope); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(filepath.Join(scope, "core.sqlite"))
	s.Note("copied %s in %s", src, time.Since(copyStart).Round(time.Second))
	control, chats, err := realStoreChats(filepath.Join(scope, "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	seededChats := map[string]bool{}
	for _, id := range chats {
		s.Graph.SeedChat(id, "")
		seededChats[id] = true
	}
	nSeed, err := shadowSeed(s.Graph, seed, seededChats)
	if err != nil {
		t.Fatal(err)
	}
	var allSeeded []string
	for id := range seededChats {
		allSeeded = append(allSeeded, id)
	}
	sort.Strings(allSeeded)
	s.Note("seeded %d chats (%d known to the old store) with %d history messages; control chat %s", len(allSeeded), len(chats), nSeed, control)

	s.StartSUT()
	start := time.Now()
	if !s.WaitFor("the conversion completes", 20*time.Minute, func() (bool, string) {
		return strings.Contains(s.SUT.Output(), "Teams store converted in"), s.OutputTail(400)
	}) {
		return
	}
	s.Note("conversion: %s", grepLine(s.SUT.Output(), "Teams store converted in"))
	pid := shadowPID(s)
	core := filepath.Join(scope, "core.sqlite")
	phases := map[string]any{"mode": mode, "seeded_chats": len(allSeeded), "seed_messages": nSeed,
		"conversion_s": time.Since(start).Seconds()}

	if mode == "kill" {
		s.WaitFor(fmt.Sprintf("%d catch-up posts before the SIGKILL", killAfter), 2*time.Hour, func() (bool, string) {
			n := len(s.Graph.Requests(fakegraph.RequestFilter{Route: routePostMessage, OnlyOK: true}))
			return n >= killAfter, fmt.Sprint(n)
		})
		killedAt := len(s.Graph.Requests(fakegraph.RequestFilter{Route: routePostMessage, OnlyOK: true}))
		s.CrashSUT()
		phases["killed_after_posts"] = killedAt
		shadowDump(t, s, filepath.Join(outDir, "messages-at-kill.jsonl"), allSeeded)
		s.Note("SIGKILL after %d posts", killedAt)
		s.StartSUT()
		pid = shadowPID(s)
	}

	var live *shadowLive
	if mode == "live" {
		live = startShadowLive(s, home, control, liveAfter)
	}
	drainStart := time.Now()
	lag := ""
	active := sampleCPU(pid, 3*time.Hour, func() bool {
		ok, why := shadowDrained(s, core)
		lag = why
		return ok
	})
	drained := time.Since(drainStart)
	asum := summarizeCPU(active)
	phases["drain_s"] = drained.Seconds()
	phases["drain_cpu"] = map[string]any{"samples": len(active.perSec), "mean_pct": asum.mean, "p99_pct": asum.p99, "max_pct": asum.max, "total_cpu_s": asum.totalSec}
	s.Note("drain %s (%s); CPU mean %.2f%% p99 %.1f%% max %.1f%% of a core, total %.1fs", drained.Round(time.Second), lag, asum.mean, asum.p99, asum.max, asum.totalSec)
	s.Check("the catch-up drains", lag == "", "drained", lag)
	if live != nil {
		<-live.done
		finals, locals, texts := shadowNotifications(s, control)
		live.res["control_final_notices"] = finals
		live.res["control_local_notices"] = locals
		live.res["control_notices"] = texts
		phases["live"] = live.res
		s.Check("live injection completes", len(live.errs) == 0, "no error", strings.Join(live.errs, "; "))
		// Catch-up notifies nobody; the two live finals and the live
		// session do (no Workflow card in the fake: control @mentions,
		// two finals possibly in one notice).
		s.Check("owner notified of the live activity only", locals == 1 && finals >= 1 && finals <= 2,
			"1 local + 1-2 final notices", fmt.Sprintf("%d local, %d final", locals, finals), texts...)
	}
	shadowDump(t, s, filepath.Join(outDir, "messages-drained.jsonl"), allSeeded)

	// Idle: no write at all, reads recorded.
	mark := shadowSeq(s)
	isamples := sampleCPU(pid, idle, nil)
	isum := summarizeCPU(isamples)
	iw, ir, ih := shadowRequests(s, mark)
	phases["idle_s"] = idle.Seconds()
	phases["idle_writes"] = iw
	phases["idle_reads"] = ir
	phases["idle_heartbeats"] = ih
	phases["idle_cpu"] = map[string]any{"mean_pct": isum.mean, "p99_pct": isum.p99, "max_pct": isum.max, "total_cpu_s": isum.totalSec}
	s.Check("no write in the idle window after the drain", len(iw) == 0, "0", fmt.Sprint(iw))
	s.Check("idle: registry heartbeats only at the heartbeat rate", ih <= shadowHeartbeatsMax(idle, 0),
		fmt.Sprintf("<= %d", shadowHeartbeatsMax(idle, 0)), fmt.Sprint(ih))
	s.Note("idle %s: reads %v; CPU mean %.3f%% max %.1f%%", idle, ir, isum.mean, isum.max)

	if mode == "full" || mode == "live" {
		// Restart on the same store: nothing to post again.
		s.StopSUT()
		mark = shadowSeq(s)
		s.StartSUT()
		time.Sleep(3 * time.Minute)
		rw, rr, rh := shadowRequests(s, mark)
		phases["restart_writes"] = rw
		phases["restart_reads"] = rr
		phases["restart_heartbeats"] = rh
		s.Check("no write in 3 min after a restart", len(rw) == 0, "0", fmt.Sprint(rw))
		s.Check("restart: registry heartbeats only at the heartbeat rate", rh <= shadowHeartbeatsMax(3*time.Minute, 1),
			fmt.Sprintf("<= %d", shadowHeartbeatsMax(3*time.Minute, 1)), fmt.Sprint(rh))
	}
	s.StopSUT()

	calls := s.Codex.Calls()
	var prompts []string
	for _, c := range calls {
		prompts = append(prompts, fmt.Sprintf("%s %s %q", c.Kind, c.ChatID, clip(c.Prompt, 80)))
	}
	phases["codex_calls"] = prompts
	if mode == "live" {
		s.Check("fake Codex ran the owner's prompt only", len(calls) == 1, "1 call", fmt.Sprint(len(calls)), prompts...)
	} else {
		s.Check("fake Codex never ran anything", len(calls) == 0, "0 calls", fmt.Sprint(len(calls)), prompts...)
	}

	summaries := 0
	for _, m := range s.Graph.Messages(control) {
		if m.Origin == fakegraph.OriginSUT && strings.Contains(m.PlainText(), "Teams helper data converted") {
			summaries++
		}
	}
	s.Check("exactly one conversion summary", summaries == 1, "1", fmt.Sprint(summaries))
	rep, line := worstRepeat(s.SUT.Output())
	phases["worst_log_repeat_per_min"] = rep
	phases["worst_log_line"] = line

	shadowDump(t, s, filepath.Join(outDir, "messages-final.jsonl"), allSeeded)
	shadowDumpPosts(t, s, filepath.Join(outDir, "writes.jsonl"))
	if err := os.WriteFile(filepath.Join(outDir, "sut-output.log"), []byte(s.SUT.Output()), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"core.sqlite", "core.sqlite-wal", "migration-report.txt"} {
		if err := shadowCopyFile(filepath.Join(scope, name), filepath.Join(outDir, name)); err != nil {
			s.Note("copy %s: %v", name, err)
		}
	}
	raw, _ := json.MarshalIndent(phases, "", " ")
	if err := os.WriteFile(filepath.Join(outDir, "phases.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// shadowCheckMounts requires home to be a tmpfs mount and home/sessions a
// read-only mount (/proc/mounts).
func shadowCheckMounts(home string) error {
	raw, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return err
	}
	home = filepath.Clean(home)
	var tmpfs, roSessions bool
	under, rwUnder := 0, 0
	for _, l := range strings.Split(string(raw), "\n") {
		f := strings.Fields(l)
		if len(f) < 4 {
			continue
		}
		switch f[1] {
		case home:
			tmpfs = f[2] == "tmpfs"
		case filepath.Join(home, "sessions"):
			for _, o := range strings.Split(f[3], ",") {
				if o == "ro" {
					roSessions = true
				}
			}
		default:
			// live mode: the real day directories (and today's rollouts)
			// are mounted read-only one by one under a writable sessions/
			if strings.HasPrefix(f[1], filepath.Join(home, "sessions")+"/") {
				under++
				if !slices.Contains(strings.Split(f[3], ","), "ro") {
					rwUnder++
				}
			}
		}
	}
	if !roSessions && under > 0 && rwUnder == 0 {
		roSessions = true
	}
	if !tmpfs || !roSessions {
		return fmt.Errorf("%s must be a tmpfs (got %v) and %s/sessions a read-only mount (got %v)", home, tmpfs, home, roSessions)
	}
	return nil
}

// shadowSeed loads the chats' history into the fake.
func shadowSeed(g *fakegraph.Server, path string, chats map[string]bool) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return 0, err
	}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	n := 0
	for sc.Scan() {
		var m struct {
			Chat, ID, HTML string
			At             int64
		}
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			return n, err
		}
		if !chats[m.Chat] {
			g.SeedChat(m.Chat, "")
			chats[m.Chat] = true
		}
		if err := g.SeedMessage(m.Chat, m.ID, time.UnixMilli(m.At), m.HTML); err != nil {
			return n, err
		}
		n++
	}
	return n, sc.Err()
}

func shadowPID(s *harness.Scenario) int {
	pid := 0
	s.WaitFor("the helper runs", 60*time.Second, func() (bool, string) {
		if p, ok := s.SUT.(interface{ PID() int }); ok {
			pid = p.PID()
		}
		return pid != 0 && strings.Contains(s.SUT.Output(), runningLine), s.OutputTail(300)
	})
	return pid
}

func shadowSeq(s *harness.Scenario) int64 {
	rs := s.Graph.Requests(fakegraph.RequestFilter{})
	if len(rs) == 0 {
		return 0
	}
	return rs[len(rs)-1].Seq
}

// shadowRequests counts the Graph requests after seq: writes (any non-GET
// to Graph) by route, reads by route.
// shadowRegistryTopic is the machine registry inbox. Its card is edited
// in place once per heartbeat (shadowRegistryHeartbeat, as in the old
// helper), plus once when an instance starts or stops.
const shadowRegistryTopic = "CXP Machine Registry"

// shadowRegistryHeartbeat is the documented production interval of the
// machine card heartbeat (F-XM-2: every 5 minutes).
const shadowRegistryHeartbeat = 5 * time.Minute

// shadowRequests splits the Graph requests since a mark into writes and
// reads by route. Edits of the registry card are counted apart, as
// heartbeats: they are the only write expected once the catch-up is done.
func shadowRequests(s *harness.Scenario, since int64) (writes, reads map[string]int, heartbeats int) {
	writes, reads = map[string]int{}, map[string]int{}
	registry := map[string]bool{}
	for _, c := range s.Graph.Chats() {
		if c.Topic == shadowRegistryTopic {
			registry[c.ID] = true
		}
	}
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Since: since}) {
		if strings.Contains(r.Host, "login.") {
			continue
		}
		switch {
		case r.Method == "GET":
			reads[r.Route]++
		case r.Method == "PATCH" && r.Route == "PATCH /chats/{chat}/messages/{message}" && registry[r.ChatID]:
			heartbeats++
		default:
			writes[r.Route]++
		}
	}
	return writes, reads, heartbeats
}

// shadowHeartbeatsMax: one card edit per heartbeat interval in the window,
// plus one for each instance start and stop.
func shadowHeartbeatsMax(window time.Duration, restarts int) int {
	return int(window/shadowRegistryHeartbeat) + 1 + 2*restarts
}

// shadowDrained: nothing queued in the outbox, every tracked rollout read
// to its end (rollouts changed in the last 10 minutes aside: the owner may
// be using Codex), and no post or chat creation in the last 60 s.
func shadowDrained(s *harness.Scenario, core string) (bool, string) {
	now := time.Now()
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Method: "POST"}) {
		if now.Sub(r.At) < 60*time.Second && (r.Route == routePostMessage || strings.Contains(r.Route, "createOrGet") || r.Route == "POST /chats") {
			return false, "posting"
		}
	}
	db, err := sql.Open("sqlite", "file:"+core+"?mode=ro")
	if err != nil {
		return false, err.Error()
	}
	defer db.Close()
	var pending int
	if err := db.QueryRow(`SELECT count(*) FROM outbox WHERE state IN ('queued','sending','unknown')`).Scan(&pending); err != nil {
		return false, err.Error()
	}
	if pending > 0 {
		return false, fmt.Sprintf("%d outbox rows pending", pending)
	}
	rows, err := db.Query(`SELECT s.path, s.offset FROM transcript_sources s JOIN conversations c ON c.id = s.conv_id
		WHERE s.state = 'tracking' AND c.state != 'closed'`)
	if err != nil {
		return false, err.Error()
	}
	defer rows.Close()
	behind := 0
	for rows.Next() {
		var p string
		var off int64
		if err := rows.Scan(&p, &off); err != nil {
			return false, err.Error()
		}
		fi, err := os.Stat(p)
		if err != nil || now.Sub(fi.ModTime()) < 10*time.Minute {
			continue
		}
		if off < fi.Size() && !shadowOpenTurnOnly(p, off, fi.Size()) {
			behind++
		}
	}
	if behind > 0 {
		return false, fmt.Sprintf("%d tracked rollouts not read to the end", behind)
	}
	return true, ""
}

type shadowMsg struct {
	Chat    string `json:"chat"`
	Topic   string `json:"topic"`
	Seeded  bool   `json:"seeded"`
	ID      string `json:"id"`
	Created int64  `json:"created"`
	HTML    string `json:"html"`
	Text    string `json:"text"`
	Patches int    `json:"patches"`
}

// shadowDump writes every message the helper posted, per chat.
func shadowDump(t *testing.T, s *harness.Scenario, path string, seeded []string) {
	old := map[string]bool{}
	for _, id := range seeded {
		old[id] = true
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, c := range s.Graph.Chats() {
		for _, m := range s.Graph.Messages(c.ID) {
			if m.Origin != fakegraph.OriginSUT {
				continue
			}
			_ = enc.Encode(shadowMsg{Chat: c.ID, Topic: c.Topic, Seeded: old[c.ID], ID: m.ID, Created: m.Created.UnixMilli(),
				HTML: m.Content, Text: m.PlainText(), Patches: m.PatchCount})
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

// shadowDumpPosts writes every Graph write (time, route, chat, status).
func shadowDumpPosts(t *testing.T, s *harness.Scenario, path string) {
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	reads := map[int64]int{}
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{}) {
		if strings.Contains(r.Host, "login.") {
			continue
		}
		if r.Method == "GET" {
			reads[r.At.Unix()/60]++
			continue
		}
		_ = enc.Encode(map[string]any{"at": r.At.UnixMilli(), "route": r.Route, "chat": r.ChatID, "status": r.Status, "instance": r.Instance})
	}
	_ = enc.Encode(map[string]any{"reads_per_minute": reads})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

func shadowCopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// shadowOpenTurnOnly reports whether what follows off in a rollout is one
// turn that never ended (a session left mid-turn): the tracker resumes at
// that turn's start, so the source is read to its end although its offset
// is short of the size. A tail with a turn end in it, or too long to scan,
// is behind.
func shadowOpenTurnOnly(path string, off, size int64) bool {
	if size-off > 64<<20 {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	tail := make([]byte, size-off)
	if _, err := f.ReadAt(tail, off); err != nil {
		return false
	}
	starts := bytes.Count(tail, []byte(`"type":"task_started"`))
	return starts <= 1 && !bytes.Contains(tail, []byte(`"type":"task_complete"`)) && !bytes.Contains(tail, []byte(`"type":"turn_aborted"`))
}
