package lease

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a clock that only moves when a standby sleeps.
type fakeClock struct {
	mu      sync.Mutex
	t       time.Time
	onSleep func() // runs after each sleep (the other host's activity)
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_800_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Advance(d)
	if c.onSleep != nil {
		c.onSleep()
	}
	return nil
}

func ident(host string, pid int) Ident {
	return Ident{Host: host, Boot: "boot-" + host, PIDNS: "pid:[1]", PID: pid, Start: "100"}
}

type env struct {
	t      *testing.T
	dir    string
	anchor string
	clock  *fakeClock
	alive  map[int]bool
}

func newEnv(t *testing.T) *env {
	dir := t.TempDir()
	return &env{t: t, dir: dir, anchor: filepath.Join(dir, "core.sqlite"), clock: newFakeClock(), alive: map[int]bool{}}
}

func (e *env) cfg(self Ident, wait bool) Config {
	return Config{
		Anchor: e.anchor, Self: self, Timing: Production(), Wait: wait,
		Clock: e.clock.Now, Sleep: e.clock.Sleep,
		Alive:     func(pid int) bool { return e.alive[pid] },
		StartTime: func(int) string { return "100" },
	}
}

// put writes a record as another process would.
func (e *env) put(r Record) []byte {
	e.t.Helper()
	data, err := encode(r)
	if err != nil {
		e.t.Fatal(err)
	}
	if err := writeSlot(Path(e.anchor), r.Gen, data); err != nil {
		e.t.Fatal(err)
	}
	return data
}

func rec(gen int64, st State, id Ident, db string) Record {
	return Record{Gen: gen, State: st, Host: id.Host, Boot: id.Boot, PIDNS: id.PIDNS, PID: id.PID, Start: id.Start, Nonce: "n", DB: db}
}

func (e *env) acquire(cfg Config) (*Holder, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h, err := Acquire(ctx, cfg)
	if h != nil {
		e.t.Cleanup(func() { h.Release() })
	}
	return h, err
}

func TestEncodeDecode(t *testing.T) {
	r := rec(7, Active, ident("a", 1), "core.sqlite")
	data, err := encode(r)
	if err != nil || len(data) != slotSize {
		t.Fatalf("encode: %v len %d", err, len(data))
	}
	got, ok, err := decode(data)
	if err != nil || !ok || got.Gen != 7 || got.Host != "a" || got.V != recordVersion {
		t.Fatalf("decode: %+v %v %v", got, ok, err)
	}
	if _, ok, err := decode(make([]byte, slotSize)); ok || err != nil {
		t.Fatalf("empty slot: %v %v", ok, err)
	}
	torn := append([]byte(nil), data...)
	torn[slotHeader+3] ^= 0xff
	if _, ok, err := decode(torn); !ok || err == nil {
		t.Fatalf("torn slot must be non-empty and invalid: %v %v", ok, err)
	}
	if _, ok, err := decode([]byte("garbage")); !ok || err == nil {
		t.Fatalf("garbage: %v %v", ok, err)
	}
}

func TestColdClaimInPlaceThenReleasedSuccessor(t *testing.T) {
	e := newEnv(t)
	a := ident("a", 1)
	h, err := e.acquire(e.cfg(a, false))
	if err != nil {
		t.Fatal(err)
	}
	pl := h.Plan()
	if pl.Gen != 1 || pl.Why != "cold" || !pl.InPlace || pl.Quiet || pl.DB != e.anchor {
		t.Fatalf("cold plan: %+v", pl)
	}
	if err := h.WaitQuiet(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.Activate(); err != nil {
		t.Fatal(err)
	}
	in, _ := Read(e.anchor)
	if in.CurrentGen != 1 || in.DB != e.anchor || in.Head.State != Active || !in.Readable("a") || in.Readable("b") {
		t.Fatalf("info: %+v", in)
	}
	if err := h.Release(); err != nil {
		t.Fatal(err)
	}
	// Same machine, released: in place, at once.
	h2, err := e.acquire(e.cfg(ident("a", 2), false))
	if err != nil {
		t.Fatal(err)
	}
	if pl := h2.Plan(); pl.Gen != 2 || pl.Why != "released" || !pl.InPlace || pl.Quiet || pl.FromGen != 1 {
		t.Fatalf("released plan: %+v", pl)
	}
	// Another host after a release: copies, no quiet.
	h2.Activate()
	h2.Release()
	h3, err := e.acquire(e.cfg(ident("b", 3), false))
	if err != nil {
		t.Fatal(err)
	}
	pl3 := h3.Plan()
	if pl3.InPlace || pl3.Quiet || pl3.DB != filepath.Join(e.dir, "core.g3.sqlite") || pl3.Src != e.anchor || !pl3.CrossHost(ident("b", 3)) {
		t.Fatalf("cross-host plan: %+v", pl3)
	}
}

func TestStaleOwnerTakenOverAfterStaleByLocalClock(t *testing.T) {
	e := newEnv(t)
	a := ident("a", 1)
	// The owner's clock is a day ahead: it must not matter.
	r := rec(1, Active, a, "core.sqlite")
	r.Wall = e.clock.Now().Add(24 * time.Hour).UnixMilli()
	e.put(r)
	start := e.clock.Now()
	h, err := e.acquire(e.cfg(ident("b", 2), true))
	if err != nil {
		t.Fatal(err)
	}
	waited := e.clock.Now().Sub(start)
	tm := Production()
	if waited < tm.Stale || waited > tm.Stale+tm.Poll {
		t.Fatalf("took over after %v, want within [%v, %v]", waited, tm.Stale, tm.Stale+tm.Poll)
	}
	pl := h.Plan()
	if pl.Why != "stale" || pl.InPlace || !pl.Quiet || pl.FromGen != 1 || pl.From.Host != "a" || pl.Src != e.anchor {
		t.Fatalf("plan: %+v", pl)
	}
	// Quiet is waited by the local clock.
	q := e.clock.Now()
	if err := h.WaitQuiet(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := e.clock.Now().Sub(q); d < tm.Quiet {
		t.Fatalf("quiet %v < %v", d, tm.Quiet)
	}
}

func TestRenewingOwnerIsNeverPreempted(t *testing.T) {
	e := newEnv(t)
	a := ident("a", 1)
	r := rec(1, Active, a, "core.sqlite")
	e.put(r)
	e.clock.onSleep = func() { r.Beat++; e.put(r) }
	cfg := e.cfg(ident("b", 2), true)
	var reasons []Reason
	cfg.Report = func(s Standby) { reasons = append(reasons, s.Reason) }
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	cfg.Sleep = func(c context.Context, d time.Duration) error {
		if n++; n > 200 { // 200 polls = over an hour
			cancel()
		}
		return e.clock.Sleep(c, d)
	}
	if h, err := Acquire(ctx, cfg); h != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("standby took a renewing owner's lease: %v %v", h, err)
	}
	if len(reasons) != 2 || reasons[0] != ReasonWatching || reasons[1] != ReasonLive {
		t.Fatalf("reports: %v", reasons)
	}
	// A foreground run refuses once it sees a renewal.
	e.clock.onSleep = func() { r.Beat++; e.put(r) }
	if _, err := e.acquire(e.cfg(ident("b", 3), false)); !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("foreground: %v", err)
	}
}

func TestSameMachine(t *testing.T) {
	e := newEnv(t)
	a := ident("a", 1)
	e.put(rec(1, Active, a, "core.sqlite"))

	// Alive owner on this machine: a second process refuses at once.
	e.alive[1] = true
	start := e.clock.Now()
	if _, err := e.acquire(e.cfg(ident("a", 2), false)); !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("second process: %v", err)
	}
	if e.clock.Now() != start {
		t.Fatalf("refusal waited %v", e.clock.Now().Sub(start))
	}

	// Pid reused by another process: dead.
	cfg := e.cfg(ident("a", 2), false)
	cfg.StartTime = func(int) string { return "999" }
	h, err := e.acquire(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if pl := h.Plan(); pl.Why != "dead" || !pl.InPlace || pl.Quiet {
		t.Fatalf("reused pid plan: %+v", pl)
	}
	if e.clock.Now() != start {
		t.Fatal("a dead owner of this machine must be replaced at once")
	}
}

func TestDeadOwnerHereAndRebootedHost(t *testing.T) {
	e := newEnv(t)
	a := ident("a", 1)
	e.put(rec(1, Active, a, "core.sqlite"))
	h, err := e.acquire(e.cfg(ident("a", 2), false))
	if err != nil {
		t.Fatal(err)
	}
	if pl := h.Plan(); pl.Why != "dead" || !pl.InPlace {
		t.Fatalf("dead plan: %+v", pl)
	}
	h.Activate()
	// The host rebooted: its old process is dead whatever its pid.
	after := ident("a", 2)
	after.Boot = "boot-new"
	e.alive[2] = true
	h.halt() // the process "vanishes" without releasing
	h.done = true
	h2, err := e.acquire(e.cfg(after, false))
	if err != nil {
		t.Fatal(err)
	}
	if pl := h2.Plan(); pl.Why != "dead" || !pl.InPlace || pl.Quiet {
		t.Fatalf("rebooted plan: %+v", pl)
	}
}

func TestFrozenOwnerIsFencedAfterTakeover(t *testing.T) {
	e := newEnv(t)
	a, err := e.acquire(e.cfg(ident("a", 1), false))
	if err != nil {
		t.Fatal(err)
	}
	a.Activate()
	// Freeze a: its goroutine stops renewing.
	a.halt()
	if err := a.Check(); err != nil {
		t.Fatalf("within Valid: %v", err)
	}
	b, err := e.acquire(e.cfg(ident("b", 2), true))
	if err != nil {
		t.Fatal(err)
	}
	if pl := b.Plan(); pl.Why != "stale" || pl.InPlace || !pl.Quiet {
		t.Fatalf("plan: %+v", pl)
	}
	// Thaw: its lease ran out, the renewal finds the claim.
	if err := a.Check(); !errors.Is(err, ErrFenced) {
		t.Fatalf("thawed check: %v", err)
	}
	select {
	case <-a.Lost():
	default:
		t.Fatal("Lost not closed")
	}
	if err := a.Verify(); !errors.Is(err, ErrFenced) {
		t.Fatalf("verify: %v", err)
	}
	// The fenced owner's release leaves b's lease alone.
	if err := a.Release(); err != nil {
		t.Fatal(err)
	}
	if err := b.Verify(); err != nil {
		t.Fatalf("b lost its lease: %v", err)
	}
	in, _ := Read(e.anchor)
	if in.HeadGen != 2 || in.Head.Host != "b" {
		t.Fatalf("head: %+v", in)
	}
}

func TestVerifyFencesWithinValid(t *testing.T) {
	e := newEnv(t)
	a, _ := e.acquire(e.cfg(ident("a", 1), false))
	a.Activate()
	e.put(rec(2, Claimed, ident("b", 2), ""))
	if err := a.Check(); err != nil {
		t.Fatalf("Check is cheap within Valid: %v", err)
	}
	if err := a.Verify(); !errors.Is(err, ErrFenced) {
		t.Fatalf("Verify must read the file: %v", err)
	}
}

func TestOwnerThawedBeforeStaleKeepsLease(t *testing.T) {
	e := newEnv(t)
	a, _ := e.acquire(e.cfg(ident("a", 1), false))
	a.Activate()
	a.halt()
	// a is frozen for most of Stale, then renews (a Check past Valid).
	cfg := e.cfg(ident("b", 2), true)
	tm := cfg.Timing
	frozenFor := tm.Stale - tm.Poll
	thawed := false
	start := e.clock.Now()
	e.clock.onSleep = func() {
		if !thawed && e.clock.Now().Sub(start) >= frozenFor {
			thawed = true
			if err := a.Check(); err != nil {
				t.Errorf("thawed owner: %v", err)
			}
		}
		if thawed {
			a.renew(true)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	cfg.Sleep = func(c context.Context, d time.Duration) error {
		if n++; n > 100 {
			cancel()
		}
		return e.clock.Sleep(c, d)
	}
	if h, err := Acquire(ctx, cfg); h != nil {
		t.Fatalf("took over from a thawed owner: %+v %v", h.Plan(), err)
	}
	if err := a.Verify(); err != nil {
		t.Fatalf("owner lost its lease: %v", err)
	}
}

func TestClaimAbortsWhenWatchedSlotChanged(t *testing.T) {
	e := newEnv(t)
	a, _ := e.acquire(e.cfg(ident("a", 1), false))
	a.Activate()
	cfg := e.cfg(ident("b", 2), true)
	cfg.defaults()
	v, _ := readView(Path(e.anchor))
	obs := observation{gen: 1, raw: v.raw(1), valid: true}
	// The owner renews between the standby's decision and its claim.
	if err := a.renew(true); err != nil {
		t.Fatal(err)
	}
	h, err := tryClaim(&cfg, newPaths(e.anchor), 2, obs, "stale")
	if h != nil || err != nil {
		t.Fatalf("claim over a renewing owner: %v %v", h, err)
	}
	// The aborted slot does not fence the owner.
	if err := a.Verify(); err != nil {
		t.Fatalf("owner fenced by an aborted claim: %v", err)
	}
	if r, _ := v.record(1); r.State != Active {
		t.Fatalf("slot 1: %+v", r)
	}
	nv, _ := readView(Path(e.anchor))
	if r, ok := nv.record(2); !ok || r.State != Aborted || nv.head() != 1 {
		t.Fatalf("slot 2: %+v head %d", r, nv.head())
	}
}

func TestGapForcesCopyAndQuiet(t *testing.T) {
	e := newEnv(t)
	a := ident("a", 1)
	e.put(rec(1, Released, a, "core.sqlite"))
	e.put(rec(2, Claimed, ident("c", 5), "")) // a claimer that stopped
	e.put(rec(3, Aborted, ident("d", 6), "")) // an abort is no gap
	h, err := e.acquire(e.cfg(ident("a", 2), true))
	if err != nil {
		t.Fatal(err)
	}
	pl := h.Plan()
	if pl.Gen != 4 || !pl.Gap || pl.InPlace || !pl.Quiet || pl.FromGen != 1 || pl.Src != e.anchor {
		t.Fatalf("plan: %+v", pl)
	}
}

func TestKillSwitch(t *testing.T) {
	e := newEnv(t)
	os.WriteFile(filepath.Join(e.dir, DisabledFile), nil, 0o600)
	if _, err := e.acquire(e.cfg(ident("a", 1), false)); !errors.Is(err, ErrDisabled) {
		t.Fatalf("foreground: %v", err)
	}
	cfg := e.cfg(ident("a", 1), true)
	var got []Reason
	cfg.Report = func(s Standby) { got = append(got, s.Reason) }
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	cfg.Sleep = func(c context.Context, d time.Duration) error {
		if n++; n > 10 {
			cancel()
		}
		return e.clock.Sleep(c, d)
	}
	if h, _ := Acquire(ctx, cfg); h != nil {
		t.Fatal("claimed under the kill switch")
	}
	if len(got) != 1 || got[0] != ReasonDisabled {
		t.Fatalf("reports: %v", got)
	}
	if _, err := os.Stat(Path(e.anchor)); !os.IsNotExist(err) {
		t.Fatal("the kill switch must not create the lease file")
	}
	// An owner keeps the lease (no one else may claim) until it releases:
	// it stops through the store's heartbeat.
	os.Remove(filepath.Join(e.dir, DisabledFile))
	h, err := e.acquire(e.cfg(ident("a", 1), false))
	if err != nil {
		t.Fatal(err)
	}
	h.Activate()
	os.WriteFile(filepath.Join(e.dir, DisabledFile), nil, 0o600)
	if err := h.Verify(); err != nil {
		t.Fatalf("owner: %v", err)
	}
	if err := h.renew(true); err != nil {
		t.Fatalf("renew: %v", err)
	}
	h.Release()
	if in, _ := Read(e.anchor); in.Head.State != Released || !in.Disabled {
		t.Fatalf("after kill switch: %+v", in)
	}
}

func TestBatchHostDefers(t *testing.T) {
	e := newEnv(t)
	e.put(rec(1, Active, ident("a", 1), "core.sqlite"))
	batch := ident("hpc", 2)
	batch.Batch = true
	start := e.clock.Now()
	h, err := e.acquire(e.cfg(batch, true))
	if err != nil {
		t.Fatal(err)
	}
	tm := Production()
	if d := e.clock.Now().Sub(start); d < tm.Stale+tm.BatchDefer {
		t.Fatalf("batch host took over after %v", d)
	}
	h.Activate()
	h.Release()
	// Its own host's successor is not deferred.
	start = e.clock.Now()
	b2 := ident("hpc", 3)
	b2.Batch = true
	if _, err := e.acquire(e.cfg(b2, true)); err != nil || e.clock.Now() != start {
		t.Fatalf("same-host batch successor waited %v (%v)", e.clock.Now().Sub(start), err)
	}
	if !IsBatch(func(k string) string { return map[string]string{"SLURM_JOB_ID": "7"}[k] }) {
		t.Fatal("SLURM_JOB_ID")
	}
	if IsBatch(func(k string) string {
		return map[string]string{"SLURM_JOB_ID": "7", MachineKindEnv: "interactive"}[k]
	}) {
		t.Fatal("override")
	}
}

func TestUnknownSlotIsWaitedOutNotTrusted(t *testing.T) {
	e := newEnv(t)
	garbage := make([]byte, slotSize)
	copy(garbage, "CXL9{from a newer helper}")
	writeSlot(Path(e.anchor), 1, garbage)
	start := e.clock.Now()
	h, err := e.acquire(e.cfg(ident("b", 2), false))
	if err != nil {
		t.Fatal(err)
	}
	if d := e.clock.Now().Sub(start); d < Production().Stale {
		t.Fatalf("took an unknown slot after %v", d)
	}
	// No store yet (an unfinished first start): created in place after
	// the quiet wait.
	if pl := h.Plan(); pl.Gen != 2 || !pl.InPlace || !pl.Quiet || !pl.Gap {
		t.Fatalf("plan: %+v", pl)
	}
	h.Release()
	// With a store, it is copied.
	os.WriteFile(e.anchor, []byte("x"), 0o600)
	h, err = e.acquire(e.cfg(ident("b", 3), false))
	if err != nil {
		t.Fatal(err)
	}
	if pl := h.Plan(); pl.Gen != 3 || pl.InPlace || !pl.Quiet || !pl.Gap {
		t.Fatalf("plan: %+v", pl)
	}
}

func TestCrashedClaimer(t *testing.T) {
	e := newEnv(t)
	e.put(rec(1, Released, ident("a", 1), "core.sqlite"))
	// b won generation 2 and died before writing its slot.
	p := newPaths(e.anchor)
	if won, err := claim(p, 2, "b-x"); !won || err != nil {
		t.Fatal(won, err)
	}
	start := e.clock.Now()
	h, err := e.acquire(e.cfg(ident("c", 3), true))
	if err != nil {
		t.Fatal(err)
	}
	if d := e.clock.Now().Sub(start); d < Production().Stale {
		t.Fatalf("claimed over an unwritten claim after %v", d)
	}
	if pl := h.Plan(); pl.Gen != 3 || pl.FromGen != 1 || pl.Gap {
		t.Fatalf("plan: %+v", pl)
	}
	// A claimer of this machine that died after writing its slot is
	// replaced at once.
	h.Release()
	e.put(rec(4, Claimed, ident("c", 9), ""))
	start = e.clock.Now()
	h2, err := e.acquire(e.cfg(ident("c", 10), true))
	if err != nil || e.clock.Now() != start {
		t.Fatalf("dead claimer: %v after %v", err, e.clock.Now().Sub(start))
	}
	if pl := h2.Plan(); pl.Gen != 5 || !pl.Gap || pl.InPlace {
		t.Fatalf("plan: %+v", pl)
	}
}

func TestSimultaneousStartExactlyOneOwner(t *testing.T) {
	dir := t.TempDir()
	anchor := filepath.Join(dir, "core.sqlite")
	tm := Scaled(900 * time.Millisecond)
	const n = 8
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var (
		wg      sync.WaitGroup
		owners  atomic.Int32
		holders = make(chan *Holder, n)
	)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			host := "h" + string(rune('a'+i))
			h, err := Acquire(ctx, Config{Anchor: anchor, Self: ident(host, 100+i), Timing: tm, Wait: true})
			if err == nil {
				owners.Add(1)
				h.Activate()
				holders <- h
			}
		}()
	}
	wg.Wait()
	close(holders)
	if got := owners.Load(); got != 1 {
		t.Fatalf("%d owners", got)
	}
	h := <-holders
	if err := h.Verify(); err != nil {
		t.Fatalf("owner lost the lease while renewing: %v", err)
	}
	h.Release()
}

func TestCheckRenewsAfterValid(t *testing.T) {
	e := newEnv(t)
	h, _ := e.acquire(e.cfg(ident("a", 1), false))
	h.Activate()
	before := h.Record().Beat
	if err := h.Check(); err != nil || h.Record().Beat != before {
		t.Fatalf("within Valid: %v beat %d", err, h.Record().Beat)
	}
	e.clock.Advance(Production().Valid)
	if err := h.Check(); err != nil || h.Record().Beat != before+1 {
		t.Fatalf("past Valid: %v beat %d", err, h.Record().Beat)
	}
}

func TestActivateCleansOldGenerations(t *testing.T) {
	e := newEnv(t)
	for _, name := range []string{"core.sqlite", "core.g2.sqlite", "core.g2.sqlite-wal", "core.g5.sqlite", "core.g3.sqlite.tmp", "core.lease.3"} {
		os.WriteFile(filepath.Join(e.dir, name), []byte("x"), 0o600)
	}
	old := time.Now().Add(-2 * time.Hour)
	os.Chtimes(filepath.Join(e.dir, "core.g3.sqlite.tmp"), old, old)
	e.put(rec(5, Released, ident("a", 1), "core.g5.sqlite"))
	h, err := e.acquire(e.cfg(ident("b", 2), false))
	if err != nil {
		t.Fatal(err)
	}
	if pl := h.Plan(); pl.Src != filepath.Join(e.dir, "core.g5.sqlite") || pl.DB != filepath.Join(e.dir, "core.g6.sqlite") {
		t.Fatalf("plan: %+v", pl)
	}
	os.WriteFile(filepath.Join(e.dir, "core.g6.sqlite"), []byte("x"), 0o600)
	if err := h.Activate(); err != nil {
		t.Fatal(err)
	}
	var left []string
	entries, _ := os.ReadDir(e.dir)
	for _, en := range entries {
		left = append(left, en.Name())
	}
	want := map[string]bool{"core.sqlite": true, "core.g5.sqlite": true, "core.g6.sqlite": true, "core.lease": true, "core.lease.3": true, "core.lease.6": true}
	if len(left) != len(want) {
		t.Fatalf("left %v", left)
	}
	for _, n := range left {
		if !want[n] {
			t.Fatalf("left %v", left)
		}
	}
}

func TestColdSourcePrefersNewestGenerationStore(t *testing.T) {
	e := newEnv(t)
	os.WriteFile(filepath.Join(e.dir, "core.sqlite"), []byte("x"), 0o600)
	os.WriteFile(filepath.Join(e.dir, "core.g4.sqlite"), []byte("x"), 0o600)
	os.WriteFile(filepath.Join(e.dir, "core.g12.sqlite"), []byte("x"), 0o600)
	h, err := e.acquire(e.cfg(ident("a", 1), false))
	if err != nil {
		t.Fatal(err)
	}
	if pl := h.Plan(); !pl.InPlace || pl.DB != filepath.Join(e.dir, "core.g12.sqlite") {
		t.Fatalf("plan: %+v", pl)
	}
}

func TestImmediateNeverWaits(t *testing.T) {
	e := newEnv(t)
	e.put(rec(1, Active, ident("b", 1), "core.sqlite"))
	cfg := e.cfg(ident("a", 2), false)
	cfg.Immediate = true
	start := e.clock.Now()
	if _, err := e.acquire(cfg); !errors.Is(err, ErrOwnerLive) || e.clock.Now() != start {
		t.Fatalf("immediate: %v after %v", err, e.clock.Now().Sub(start))
	}
	e.put(rec(1, Released, ident("a", 1), "core.sqlite"))
	h, err := e.acquire(cfg)
	if err != nil || !h.Plan().InPlace {
		t.Fatalf("released: %v", err)
	}
}
