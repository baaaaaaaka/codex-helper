package codex

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/internal/leakcheck"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

const (
	envFake    = "CORE_CODEX_TEST_FAKE"
	envSleeper = "CORE_CODEX_TEST_SLEEP"
)

// TestMain doubles as the fake Codex binary (the test binary re-executed
// with envFake) and as a long sleeper for the PID file tests.
func TestMain(m *testing.M) {
	switch {
	case os.Getenv(envSleeper) == "1":
		time.Sleep(time.Minute)
		os.Exit(0)
	case os.Getenv(envFake) == "1":
		home := os.Getenv("CODEX_HOME")
		err := fakecodex.Serve(fakecodex.Config{
			In: os.Stdin, Out: os.Stdout, Log: io.Discard,
			Store:       fakecodex.Store{Dir: filepath.Join(home, "e2e-fakecodex")},
			SessionsDir: filepath.Join(home, "sessions"),
			Brain:       os.Getenv(fakecodex.EnvBrainURL), Kind: "work",
		})
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	leakcheck.Main(m)
}

// brain scripts the fake Codex by prompt prefix.
type brain struct {
	mu      sync.Mutex
	seq     int
	plans   map[int]string // seq -> prompt
	ends    map[int]string // seq -> outcome reported by the fake
	release chan struct{}
	begins  []fakecodex.BeginRequest
}

func newBrain(t *testing.T) (*brain, string) {
	b := &brain{plans: map[int]string{}, ends: map[int]string{}, release: make(chan struct{})}
	srv := httptest.NewServer(b)
	t.Cleanup(func() {
		b.mu.Lock()
		select {
		case <-b.release:
		default:
			close(b.release)
		}
		b.mu.Unlock()
		srv.CloseClientConnections()
		srv.Close()
	})
	return b, srv.URL
}

func (b *brain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	seq, _ := strconv.Atoi(r.URL.Query().Get("seq"))
	switch r.URL.Path {
	case "/begin":
		var req fakecodex.BeginRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		b.mu.Lock()
		b.seq++
		n := b.seq
		b.plans[n] = req.Prompt
		b.begins = append(b.begins, req)
		b.mu.Unlock()
		plan := fakecodex.Plan{Seq: n}
		switch {
		case strings.HasPrefix(req.Prompt, "reject"):
			plan.RejectStart = "rejected by test"
		case strings.HasPrefix(req.Prompt, "crash"):
			plan.Crash = true
		case strings.HasPrefix(req.Prompt, "hold"):
			plan.HoldCompletion = true
		case strings.HasPrefix(req.Prompt, "comment"):
			plan.Commentary = "thinking about it"
		case strings.HasPrefix(req.Prompt, "unphased"):
			plan.Unphased = true
			plan.Commentary = "step one"
			plan.MoreCommentary = []string{"step two"}
		case strings.HasPrefix(req.Prompt, "compact"):
			plan.Compactions = 2
		case strings.HasPrefix(req.Prompt, "retry"):
			plan.Retries = []fakecodex.Retry{
				{Message: "Reconnecting... 1/5", Details: "stream disconnected:\n via http://h/_cxp/tok/v1", Info: map[string]any{"responseStreamDisconnected": map[string]any{"httpStatusCode": nil}}},
				{Message: "", Info: "serverOverloaded"},
			}
		}
		_ = json.NewEncoder(w).Encode(plan)
	case "/await":
		b.mu.Lock()
		prompt := b.plans[seq]
		rel := b.release
		b.mu.Unlock()
		res := fakecodex.AwaitResult{Outcome: "answered", Answer: "ANSWER " + prompt}
		switch {
		case strings.HasPrefix(prompt, "capacity"):
			res = fakecodex.AwaitResult{Outcome: "failed", Error: "Selected model is at capacity", Capacity: true}
		case strings.HasPrefix(prompt, "block"):
			select {
			case <-rel:
			case <-r.Context().Done():
				return
			}
		}
		_ = json.NewEncoder(w).Encode(res)
	case "/resume":
		_ = json.NewEncoder(w).Encode(fakecodex.ResumeDecision{})
	case "/end":
		var req fakecodex.EndRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		b.mu.Lock()
		b.ends[seq] = req.Outcome
		b.mu.Unlock()
		_, _ = w.Write([]byte("{}"))
	default:
		http.NotFound(w, r)
	}
}

func (b *brain) clientIDs() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, r := range b.begins {
		out = append(out, r.ClientID)
	}
	return out
}

func fakeConfig(t *testing.T, brainURL, home string) Config {
	t.Helper()
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	return Config{
		Bin: exe,
		Env: []string{envFake + "=1", "CODEX_HOME=" + home, fakecodex.EnvBrainURL + "=" + brainURL},
		Dir: home,
	}
}

func startFake(t *testing.T) (*Server, *brain, Config) {
	t.Helper()
	b, url := newBrain(t)
	cfg := fakeConfig(t, url, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := Start(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(5 * time.Second) })
	return s, b, cfg
}

func tctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestRunTurnAnswerClientIDAndFind(t *testing.T) {
	s, b, cfg := startFake(t)
	ctx := tctx(t)
	th, err := s.ThreadStart(ctx, ThreadStartParams{Cwd: cfg.Dir})
	if err != nil || th.ID == "" {
		t.Fatalf("thread/start: %v %+v", err, th)
	}
	var started string
	var progress []string
	var pmu sync.Mutex
	res, err := s.RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "cxp-0000abcd-1", Input: []Input{{Text: "comment hello"}}}, time.Minute, TurnHooks{
		Started:  func(id string) { started = id },
		Progress: func(text string) { pmu.Lock(); progress = append(progress, text); pmu.Unlock() },
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted || res.Final != "ANSWER comment hello" || res.TurnID != started || started == "" {
		t.Fatalf("result %+v started %q", res, started)
	}
	if res.Usage == nil || res.Usage.Total != 1280 {
		t.Fatalf("usage %+v", res.Usage)
	}
	pmu.Lock()
	if len(progress) != 1 || progress[0] != "thinking about it" {
		t.Fatalf("progress %q", progress)
	}
	pmu.Unlock()
	if ids := b.clientIDs(); len(ids) != 1 || ids[0] != "cxp-0000abcd-1" {
		t.Fatalf("clientIds %q", ids)
	}

	// A new server process (helper restart) finds the turn by client id.
	s.Close(5 * time.Second)
	s2, err := Start(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close(5 * time.Second)
	turn, ok, err := s2.FindTurn(ctx, th.ID, "cxp-0000abcd-1")
	if err != nil || !ok || turn.ID != started || turn.Status != StatusCompleted || turn.Final() != "ANSWER comment hello" {
		t.Fatalf("find: %v %v %+v", err, ok, turn)
	}
	if _, ok, _ := s2.FindTurn(ctx, th.ID, "cxp-0000abcd-2"); ok {
		t.Fatal("found an unknown client id")
	}
	// Starting a turn on an unloaded thread needs a resume.
	if _, err := s2.ThreadResume(ctx, th.ID); err != nil {
		t.Fatal(err)
	}
	res, err = s2.RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "cxp-0000abcd-3", Input: []Input{{Text: "again"}}}, time.Minute, TurnHooks{})
	if err != nil || res.Final != "ANSWER again" {
		t.Fatalf("second turn: %v %+v", err, res)
	}
}

// Agent messages without a phase: every one but the last is progress; the
// last is the final and never progress (v0.1.20 look-behind).
func TestRunTurnUnphasedFinalIsNotProgress(t *testing.T) {
	s, _, cfg := startFake(t)
	ctx := tctx(t)
	th, err := s.ThreadStart(ctx, ThreadStartParams{Cwd: cfg.Dir})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var progress []string
	res, err := s.RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "k-unphased", Input: []Input{{Text: "unphased"}}}, time.Minute, TurnHooks{
		Progress: func(text string) { mu.Lock(); progress = append(progress, text); mu.Unlock() },
	})
	if err != nil || res.Status != StatusCompleted || res.Final != "ANSWER unphased" {
		t.Fatalf("%v %+v", err, res)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(progress, "|") != "step one|step two" {
		t.Fatalf("progress %q, want [step one step two]", progress)
	}
}

// Each context compaction of a turn is reported once, though Codex reports
// it both as a contextCompaction item and as thread/compacted.
func TestRunTurnReportsEachCompactionOnce(t *testing.T) {
	s, _, cfg := startFake(t)
	ctx := tctx(t)
	th, err := s.ThreadStart(ctx, ThreadStartParams{Cwd: cfg.Dir})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	n := 0
	res, err := s.RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "k-compact", Input: []Input{{Text: "compact twice"}}}, time.Minute, TurnHooks{
		Compacted: func() { mu.Lock(); n++; mu.Unlock() },
	})
	if err != nil || res.Status != StatusCompleted {
		t.Fatalf("%v %+v", err, res)
	}
	mu.Lock()
	defer mu.Unlock()
	if n != 2 {
		t.Fatalf("%d compactions reported, want 2", n)
	}
}

func TestRunTurnLostCompletionIsPolled(t *testing.T) {
	s, _, cfg := startFake(t)
	ctx := tctx(t)
	th, err := s.ThreadStart(ctx, ThreadStartParams{Cwd: cfg.Dir})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "k1", Input: []Input{{Text: "hold me"}}}, 100*time.Millisecond, TurnHooks{})
	if err != nil || res.Status != StatusCompleted || res.Final != "ANSWER hold me" {
		t.Fatalf("%v %+v", err, res)
	}
}

func TestRunTurnRejectedAndCapacity(t *testing.T) {
	s, _, cfg := startFake(t)
	ctx := tctx(t)
	th, err := s.ThreadStart(ctx, ThreadStartParams{Cwd: cfg.Dir})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "k1", Input: []Input{{Text: "reject"}}}, time.Minute, TurnHooks{})
	var ns *ErrNotStarted
	if !errors.As(err, &ns) {
		t.Fatalf("want ErrNotStarted, got %v", err)
	}
	res, err := s.RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "k2", Input: []Input{{Text: "capacity"}}}, time.Minute, TurnHooks{})
	if err != nil || res.Status != StatusFailed || !res.Err.Capacity() {
		t.Fatalf("%v %+v", err, res)
	}
	if (&TurnError{Message: "other"}).Capacity() || (*TurnError)(nil).Capacity() {
		t.Fatal("capacity false positive")
	}
	// Real Codex shapes (E2E-M3 probe d).
	for _, te := range []TurnError{
		{Message: "Selected model is at capacity. Please try a different model.", CodexErrorInfo: json.RawMessage(`"serverOverloaded"`)},
		{Message: "exceeded retry limit, last status: 429 Too Many Requests", CodexErrorInfo: json.RawMessage(`{"responseTooManyFailedAttempts":{"httpStatusCode":429}}`)},
	} {
		if !te.Capacity() {
			t.Fatalf("not capacity: %+v", te)
		}
	}
}

func TestRunTurnInterrupt(t *testing.T) {
	s, b, cfg := startFake(t)
	ctx := tctx(t)
	th, err := s.ThreadStart(ctx, ThreadStartParams{Cwd: cfg.Dir})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "k1", Input: []Input{{Text: "block"}}}, time.Minute, TurnHooks{
		Started: func(id string) {
			go func() { _ = s.TurnInterrupt(ctx, th.ID, id) }()
		},
	})
	if err != nil || res.Status != StatusInterrupted || res.Final != "" {
		t.Fatalf("%v %+v", err, res)
	}
	_ = b
}

func TestRunTurnServerCrashThenReconcile(t *testing.T) {
	s, _, cfg := startFake(t)
	ctx := tctx(t)
	th, err := s.ThreadStart(ctx, ThreadStartParams{Cwd: cfg.Dir})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "k-crash", Input: []Input{{Text: "crash"}}}, time.Minute, TurnHooks{})
	if !errors.Is(err, ErrExited) || res.TurnID == "" {
		t.Fatalf("want ErrExited with turn id, got %v %+v", err, res)
	}
	if err := s.Call(ctx, "model/list", nil, nil); !errors.Is(err, ErrExited) {
		t.Fatalf("call after exit: %v", err)
	}
	s2, err := Start(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close(5 * time.Second)
	turn, ok, err := s2.FindTurn(ctx, th.ID, "k-crash")
	if err != nil || !ok || turn.Status != StatusInterrupted {
		t.Fatalf("reconcile: %v %v %+v", err, ok, turn)
	}
	models, err := s2.Models(ctx)
	// The fake lists a second, non-default model (gap batch B).
	if err != nil || len(models) != 2 || !models[0].IsDefault || len(models[0].Efforts) != 3 || models[1].IsDefault {
		t.Fatalf("models %v %+v", err, models)
	}
	fork, err := s2.ThreadFork(ctx, th.ID, "", cfg.Dir)
	if err != nil || fork.ID == "" || fork.ID == th.ID {
		t.Fatalf("fork %v %+v", err, fork)
	}
}

func TestPoolWarmReapAndPIDFile(t *testing.T) {
	b, url := newBrain(t)
	_ = b
	home := t.TempDir()
	cfg := fakeConfig(t, url, home)
	now := time.Unix(1_000_000, 0)
	var nmu sync.Mutex
	clock := func() time.Time { nmu.Lock(); defer nmu.Unlock(); return now }
	pidFile := filepath.Join(home, "run", "appservers.json")
	p := NewPool(PoolConfig{Idle: 10 * time.Minute, PIDFile: pidFile, Now: clock})
	defer p.Close(5 * time.Second)
	ctx := tctx(t)

	l1, err := p.Acquire(ctx, "conv-1", cfg)
	if err != nil {
		t.Fatal(err)
	}
	th, err := l1.Server().ThreadStart(ctx, ThreadStartParams{Cwd: home})
	if err != nil {
		t.Fatal(err)
	}
	l1.Loaded(th.ID)
	if err := l1.Load(ctx, th.ID); err != nil {
		t.Fatal(err)
	}
	l2, err := p.Acquire(ctx, "conv-1", cfg)
	if err != nil || l2.Server() != l1.Server() {
		t.Fatalf("second lease: %v", err)
	}
	var recs []pidRecord
	data, _ := os.ReadFile(pidFile)
	if json.Unmarshal(data, &recs) != nil || len(recs) != 1 || recs[0].PID != l1.Server().PID() {
		t.Fatalf("pid file %s", data)
	}
	l1.Release()
	l2.Release()
	l2.Release() // idempotent
	if n := p.Reap(); n != 0 {
		t.Fatalf("reaped a warm server: %d", n)
	}
	nmu.Lock()
	now = now.Add(10 * time.Minute)
	nmu.Unlock()
	srv := l1.Server()
	if n := p.Reap(); n != 1 || !srv.Exited() || p.Size() != 0 {
		t.Fatalf("reap: %d exited=%v size=%d", n, srv.Exited(), p.Size())
	}
	data, _ = os.ReadFile(pidFile)
	if strings.TrimSpace(string(data)) != "[]" {
		t.Fatalf("pid file after reap %s", data)
	}
	// A new server must resume the thread before a turn.
	l3, err := p.Acquire(ctx, "conv-1", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l3.Release()
	if err := l3.Load(ctx, th.ID); err != nil {
		t.Fatal(err)
	}
	res, err := l3.Server().RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "k", Input: []Input{{Text: "hi"}}}, time.Minute, TurnHooks{})
	if err != nil || res.Final != "ANSWER hi" {
		t.Fatalf("%v %+v", err, res)
	}
	p.Close(5 * time.Second)
	if _, err := p.Acquire(ctx, "conv-2", cfg); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("acquire after close: %v", err)
	}
}

func TestKillStale(t *testing.T) {
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	start := func() *exec.Cmd {
		cmd := exec.Command(exe)
		cmd.Env = append(os.Environ(), envSleeper+"=1")
		configure(cmd)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd
	}
	victim, bystander, moved := start(), start(), start()
	waited := make(chan error, 2)
	reaping := false
	defer func() {
		cs := []*exec.Cmd{bystander, moved}
		if !reaping { // skipped before the victim's reaper started
			cs = append(cs, victim)
		}
		for _, c := range cs {
			_ = c.Process.Kill()
			_ = c.Wait()
		}
	}()
	// Wait until /proc shows all three.
	deadline := time.Now().Add(10 * time.Second)
	for startTime(victim.Process.Pid) == "" || startTime(bystander.Process.Pid) == "" || startTime(moved.Process.Pid) == "" {
		if time.Now().After(deadline) {
			t.Skip("no /proc start times on this platform")
		}
		time.Sleep(10 * time.Millisecond)
	}
	reaping = true
	go func() { waited <- victim.Wait() }()
	pidFile := filepath.Join(t.TempDir(), "appservers.json")
	recs := []pidRecord{
		{PID: victim.Process.Pid, Start: startTime(victim.Process.Pid)},
		{PID: bystander.Process.Pid, Start: "1"}, // recycled pid: leave alone
		// A home copied from another machine (or a file from before a
		// reboot): even a matching pid and start time mean nothing here.
		{PID: moved.Process.Pid, Start: startTime(moved.Process.Pid), Boot: "another-machine"},
	}
	data, _ := json.Marshal(recs)
	if err := os.WriteFile(pidFile, data, 0o600); err != nil {
		t.Fatal(err)
	}
	// The victim is our child: reap it as soon as it dies so the group is
	// really gone (a zombie still counts as alive).
	killed, err := KillStale(pidFile, 10*time.Second)
	if err != nil || killed != 1 {
		t.Fatalf("KillStale: %d %v", killed, err)
	}
	select {
	case <-waited:
	case <-time.After(10 * time.Second):
		t.Fatal("victim not killed")
	}
	if !groupAlive(bystander.Process.Pid) {
		t.Fatal("bystander killed")
	}
	if bootID() != "" && !groupAlive(moved.Process.Pid) {
		t.Fatal("killed a pid recorded on another machine")
	}
	data, _ = os.ReadFile(pidFile)
	if string(data) != "[]" {
		t.Fatalf("pid file %s", data)
	}
	if n, err := KillStale(filepath.Join(t.TempDir(), "missing"), time.Second); n != 0 || err != nil {
		t.Fatalf("missing file: %d %v", n, err)
	}
}

func TestSameProcess(t *testing.T) {
	for _, c := range []struct {
		name            string
		r               pidRecord
		boot, ns, start string
		want            bool
	}{
		{"same boot and start", pidRecord{Start: "7", Boot: "b1", NS: "n1"}, "b1", "n1", "7", true},
		{"recycled pid", pidRecord{Start: "7", Boot: "b1"}, "b1", "", "8", false},
		{"leader gone, same boot", pidRecord{Start: "7", Boot: "b1"}, "b1", "", "", true},
		{"another machine, same start", pidRecord{Start: "7", Boot: "b0"}, "b1", "", "7", false},
		{"another pid namespace", pidRecord{Start: "7", Boot: "b1", NS: "n0"}, "b1", "n1", "7", false},
		{"no start times, same boot (macOS)", pidRecord{Boot: "b1"}, "b1", "", "", true},
		{"no start times, another boot", pidRecord{Boot: "b0"}, "b1", "", "", false},
		{"nothing to check", pidRecord{}, "", "", "", false},
		{"old record, start matches", pidRecord{Start: "7"}, "b1", "", "7", true},
		{"old record, leader gone", pidRecord{Start: "7"}, "b1", "", "", false},
	} {
		if got := sameProcess(c.r, c.boot, c.ns, c.start); got != c.want {
			t.Errorf("%s: sameProcess = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestAutoApprove(t *testing.T) {
	res, ok, err := AutoApprove("item/commandExecution/requestApproval", json.RawMessage(`{}`))
	if err != nil || !ok || !strings.Contains(string(res), "accept") {
		t.Fatalf("%s %v %v", res, ok, err)
	}
	if _, ok, _ := AutoApprove("something/else", nil); ok {
		t.Fatal("unknown request handled")
	}
	if !IsActiveWriter(&RPCError{Code: -32000, Message: "thread has an active writer"}) || IsActiveWriter(errors.New("active writer")) {
		t.Fatal("IsActiveWriter")
	}
}
