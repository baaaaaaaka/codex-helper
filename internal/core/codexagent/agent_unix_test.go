//go:build !windows

package codexagent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/internal/leakcheck"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

const envFake = "CODEXAGENT_TEST_FAKE_CODEX"

// TestMain doubles as the fake Codex binary.
func TestMain(m *testing.M) {
	if os.Getenv(envFake) == "1" {
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

// Review M4: when turn/start fails with an error that is neither a
// rejection nor an exit, the turn may be running; the agent kills the
// app-server's process group before reporting the outcome unknown, so the
// turn cannot go on beside the next one.
func TestUnknownStartErrorKillsTheServer(t *testing.T) {
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/begin":
			_ = json.NewEncoder(w).Encode(fakecodex.Plan{Seq: 1, MalformedStart: true})
		case "/await":
			<-r.Context().Done() // the turn runs until its server dies
		default:
			_, _ = w.Write([]byte("{}"))
		}
	}))
	defer func() { brain.CloseClientConnections(); brain.Close() }()
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	pidFile := filepath.Join(home, "pids.json")
	a := New(Config{
		Server:  codex.Config{Bin: exe, Env: []string{envFake + "=1", "CODEX_HOME=" + home, fakecodex.EnvBrainURL + "=" + brain.URL}, Dir: home},
		PIDFile: pidFile, Poll: 100 * time.Millisecond,
	})
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var pid int
	_, err = a.Run(ctx, engine.TurnRequest{ConvID: 1, Cwd: home, TurnKey: "k-1", Text: "hello"}, engine.TurnHooks{
		ThreadStarted: func(string) error {
			data, err := os.ReadFile(pidFile)
			if err != nil {
				return err
			}
			var recs []struct {
				PID int `json:"pid"`
			}
			if err := json.Unmarshal(data, &recs); err != nil || len(recs) != 1 {
				return errors.New("pid file: " + string(data))
			}
			pid = recs[0].PID
			return nil
		},
	})
	if !errors.Is(err, engine.ErrUnknownOutcome) {
		t.Fatalf("Run: %v, want ErrUnknownOutcome", err)
	}
	if pid == 0 {
		t.Fatal("no app-server pid recorded")
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("app-server %d still alive after an unknown outcome (kill 0: %v)", pid, err)
	}
}

// Review L4: a cancel that arrives while the app-server is still starting
// (Interrupt finds no registered turn) stops the turn before turn/start.
func TestCancelDuringStartNeverStartsTheTurn(t *testing.T) {
	var begins atomic.Int32
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/begin" {
			begins.Add(1)
			_ = json.NewEncoder(w).Encode(fakecodex.Plan{Seq: 1})
			return
		}
		_, _ = w.Write([]byte("{}"))
	}))
	defer func() { brain.CloseClientConnections(); brain.Close() }()
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	a := New(Config{
		Server: codex.Config{Bin: exe, Env: []string{envFake + "=1", "CODEX_HOME=" + home, fakecodex.EnvBrainURL + "=" + brain.URL}, Dir: home},
		Poll:   100 * time.Millisecond,
	})
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var cancelled atomic.Bool
	out, err := a.Run(ctx, engine.TurnRequest{ConvID: 1, Cwd: home, TurnKey: "k-1", Text: "hello"}, engine.TurnHooks{
		ThreadStarted: func(string) error {
			// The engine marks the turn cancelled, then calls Interrupt.
			cancelled.Store(true)
			if err := a.Interrupt(ctx, 1); err != nil {
				t.Errorf("Interrupt during start: %v", err)
			}
			return nil
		},
		Cancelled: cancelled.Load,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !out.NotStarted {
		t.Fatalf("outcome %+v, want not started", out)
	}
	if n := begins.Load(); n != 0 {
		t.Fatalf("Codex began %d turn(s) after the cancel", n)
	}
}

// Review L10: a lookup with no running conversation server does not start
// one without the conversation's working directory.
func TestLookupDoesNotStartAConversationServer(t *testing.T) {
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/begin":
			_ = json.NewEncoder(w).Encode(fakecodex.Plan{Seq: 1})
		case "/await":
			_ = json.NewEncoder(w).Encode(fakecodex.AwaitResult{Outcome: "answered", Answer: "done"})
		default:
			_, _ = w.Write([]byte("{}"))
		}
	}))
	defer func() { brain.CloseClientConnections(); brain.Close() }()
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	a := New(Config{
		Server: codex.Config{Bin: exe, Env: []string{envFake + "=1", "CODEX_HOME=" + home, fakecodex.EnvBrainURL + "=" + brain.URL}, Dir: home},
		Poll:   100 * time.Millisecond,
	})
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var thread string
	out, err := a.Run(ctx, engine.TurnRequest{ConvID: 1, Cwd: home, TurnKey: "k-1", Text: "hello"}, engine.TurnHooks{
		ThreadStarted: func(id string) error { thread = id; return nil },
	})
	if err != nil || out.Status != engine.StatusCompleted {
		t.Fatalf("Run: %+v, %v", out, err)
	}
	a.Kill(1) // as after a restart or an unknown outcome
	found, ok, err := a.Lookup(ctx, 1, thread, "k-1")
	if err != nil || !ok || found.Status != engine.StatusCompleted {
		t.Fatalf("Lookup: %+v, %v, %v", found, ok, err)
	}
	if a.pool.Busy(key(1)) {
		t.Fatal("Lookup started the conversation's server without its working directory")
	}
}

// The goal calls of the agent (2026-11-07): Goal loads the thread on the
// conversation's server and reads or sets its goal; Info reports the
// active goal and the model the server knows; StopGoal pauses the goal.
func TestGoalInfoAndStopGoal(t *testing.T) {
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/begin":
			_ = json.NewEncoder(w).Encode(fakecodex.Plan{Seq: 1})
		case "/await":
			_ = json.NewEncoder(w).Encode(fakecodex.AwaitResult{Outcome: "answered", Answer: "done"})
		case "/resume":
			_ = json.NewEncoder(w).Encode(fakecodex.ResumeDecision{})
		default:
			_, _ = w.Write([]byte("{}"))
		}
	}))
	defer func() { brain.CloseClientConnections(); brain.Close() }()
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	a := New(Config{Server: codex.Config{Bin: exe, Env: []string{envFake + "=1", "CODEX_HOME=" + home, fakecodex.EnvBrainURL + "=" + brain.URL}, Dir: home},
		Poll: 100 * time.Millisecond})
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var thread string
	out, err := a.Run(ctx, engine.TurnRequest{ConvID: 1, Cwd: home, TurnKey: "k-1", Text: "hello"}, engine.TurnHooks{
		ThreadStarted: func(id string) error { thread = id; return nil },
	})
	if err != nil || out.Status != engine.StatusCompleted {
		t.Fatalf("turn: %+v %v", out, err)
	}
	if i := a.Info(1, thread); i.Model != "gpt-5-codex" || i.Goal != "" || i.SelfTurn {
		t.Fatalf("info: %+v", i)
	}
	if i := a.Info(2, thread); i != (engine.TurnInfo{}) {
		t.Fatalf("info of a conversation without a server: %+v", i)
	}
	if g, err := a.Goal(ctx, 1, thread, home, engine.GoalState{}); err != nil || g.Status != "" {
		t.Fatalf("no goal: %+v %v", g, err)
	}
	if g, err := a.Goal(ctx, 1, thread, home, engine.GoalState{Objective: "Ship it", Status: engine.GoalActive}); err != nil || g.Status != engine.GoalActive {
		t.Fatalf("set: %+v %v", g, err)
	}
	if i := a.Info(1, thread); i.Goal != "Ship it" {
		t.Fatalf("info with the goal: %+v", i)
	}
	res, err := a.StopGoal(ctx, 1, thread)
	if err != nil || !res.Paused || res.Interrupted || res.Objective != "Ship it" {
		t.Fatalf("stop: %+v %v", res, err)
	}
	if g, err := a.Goal(ctx, 1, thread, home, engine.GoalState{}); err != nil || g.Status != engine.GoalPaused {
		t.Fatalf("after stop: %+v %v", g, err)
	}
	if res, err := a.StopGoal(ctx, 1, thread); err != nil || res.Paused {
		t.Fatalf("second stop: %+v %v", res, err)
	}
}
