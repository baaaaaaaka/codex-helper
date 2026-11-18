package codexagent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
)

// Default native installations may have goals disabled. Ordinary cancel must
// still work, while the unobserved goal snapshot must not claim known absence.
func TestRealCodexGoalDisabledStop(t *testing.T) {
	bin := os.Getenv("E2E_REAL_CODEX")
	if bin == "" {
		t.Skip("offline native contract: E2E_REAL_CODEX is not set")
	}
	home := t.TempDir()
	responses := fakecodex.NewResponses()
	defer responses.Close()
	if err := fakecodex.WriteCodexHome(home, responses.URL()); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(home+"/config.toml", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("\ngoals = false\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	a := New(Config{Server: codex.Config{Bin: bin, Env: fakecodex.CodexEnv(home), Dir: home}})
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	lease, err := a.pool.Acquire(ctx, key(1), a.serverConfig(home))
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	th, err := lease.Server().ThreadStart(ctx, codex.ThreadStartParams{Cwd: home})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Server().GoalGet(ctx, th.ID); !codex.IsGoalsUnavailable(err) {
		t.Fatalf("expected capability error: %v", err)
	}
	if _, err := a.StopGoal(ctx, 1, th.ID, home, "", false); err != nil {
		t.Fatal(err)
	}
	if a.Info(1, th.ID).Goal.Known {
		t.Fatal("unsupported capability became known absence")
	}
	if len(responses.Requests()) != 0 {
		t.Fatal("disabled goal stop triggered inference")
	}
}

// Exercise the production adapter from a cold pool, with a persisted active
// goal. A regression that calls lease.Load before a read would start execution.
func TestRealCodexGoalReadDoesNotLoadExecution(t *testing.T) {
	bin := os.Getenv("E2E_REAL_CODEX")
	if bin == "" {
		t.Skip("offline native contract: E2E_REAL_CODEX is not set")
	}
	home := t.TempDir()
	responses := fakecodex.NewResponses()
	defer responses.Close()
	if err := fakecodex.WriteCodexHome(home, responses.URL()); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(home+"/config.toml", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("\ngoals = true\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := codex.Config{Bin: bin, Env: fakecodex.CodexEnv(home), Dir: home}
	writer, err := codex.Start(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Kill()
	th, err := writer.ThreadStart(ctx, codex.ThreadStartParams{Cwd: home})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.GoalSet(ctx, th.ID, "read safely", codex.GoalPaused); err != nil {
		t.Fatal(err)
	}
	writer.Kill()
	seed, err := codex.Start(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Kill()
	if _, err := seed.GoalSet(ctx, th.ID, "", codex.GoalActive); err != nil {
		t.Fatal(err)
	}
	seed.Kill()
	a := New(Config{Server: cfg})
	defer a.Close()
	got, err := a.Goal(ctx, 1, th.ID, home)
	if err != nil || got.Status != engine.GoalActive {
		t.Fatalf("cold read: %+v %v", got, err)
	}
	stopped, err := a.StopGoal(ctx, 1, th.ID, home, "", false)
	if err != nil || !stopped.Paused {
		t.Fatalf("cold stop: %+v %v", stopped, err)
	}
	srv := a.pool.Peek(key(1))
	var loaded struct {
		Data []string `json:"data"`
	}
	if err := srv.Call(ctx, "thread/loaded/list", map[string]any{}, &loaded); err != nil {
		t.Fatal(err)
	}
	if len(loaded.Data) != 0 || len(responses.Requests()) != 0 {
		t.Fatalf("observation resumed execution: %v, requests %d", loaded.Data, len(responses.Requests()))
	}
	// Set active while still unloaded, then exercise the explicit command path.
	// Query must leave it idle; explicit activation must load it even if its
	// persisted status already says active.
	if _, err := srv.GoalSet(ctx, th.ID, "", codex.GoalActive); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ActivateGoal(ctx, 1, th.ID, home, "", "", ""); err != nil {
		t.Fatal(err)
	}
	if err := srv.Call(ctx, "thread/loaded/list", map[string]any{}, &loaded); err != nil {
		t.Fatal(err)
	}
	if len(loaded.Data) != 1 || loaded.Data[0] != th.ID {
		t.Fatalf("explicit activation did not load execution: %v", loaded.Data)
	}
	// The fixture owns this execution; stop it before teardown.
	if _, err := srv.GoalSet(ctx, th.ID, "", codex.GoalPaused); err != nil {
		t.Fatal(err)
	}
}

// Exercise native autonomous turns against a loopback endpoint. Block each
// response until interrupted so the switch must actually stop the old turn.
func TestRealCodexGoalSwitchAndPause(t *testing.T) {
	bin := os.Getenv("E2E_REAL_CODEX")
	if bin == "" {
		t.Skip("offline native contract: E2E_REAL_CODEX is not set")
	}
	type call struct {
		Model     string
		Reasoning struct{ Effort string }
		Stopped   <-chan struct{} `json:"-"`
	}
	calls := make(chan call, 8)
	responses := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost || req.URL.Path != "/v1/responses" {
			http.NotFound(w, req)
			return
		}
		var c call
		if err := json.NewDecoder(req.Body).Decode(&c); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		c.Stopped = req.Context().Done()
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintln(w, "event: response.created")
		fmt.Fprintln(w, `data: {"type":"response.created","response":{"id":"held-goal"}}`)
		fmt.Fprintln(w)
		w.(http.Flusher).Flush()
		select {
		case calls <- c:
		case <-c.Stopped:
		}
		<-c.Stopped
	}))
	defer responses.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	home := t.TempDir()
	if err := fakecodex.WriteCodexHome(home, responses.URL+"/v1"); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(home+"/config.toml", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("\ngoals = true\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	a := New(Config{Server: codex.Config{Bin: bin, Env: fakecodex.CodexEnv(home), Dir: home}})
	defer a.Close()

	lease, err := a.pool.Acquire(ctx, key(1), a.serverConfig(home))
	if err != nil {
		t.Fatal(err)
	}
	srv := lease.Server()
	thread, err := srv.ThreadStart(ctx, codex.ThreadStartParams{Cwd: home, Model: "old-goal-model"})
	if err != nil {
		lease.Release()
		t.Fatal(err)
	}
	lease.Loaded(thread.ID)
	lease.Release()

	nextCall := func(model, effort string) call {
		t.Helper()
		select {
		case req := <-calls:
			if req.Model != model || req.Reasoning.Effort != effort {
				t.Fatalf("autonomous request used %s/%s, want %s/%s", req.Model, req.Reasoning.Effort, model, effort)
			}
			return req
		case <-ctx.Done():
			t.Fatalf("no autonomous request for %s/%s: %v", model, effort, ctx.Err())
			return call{}
		}
	}
	waitStopped := func(req call) {
		t.Helper()
		select {
		case <-req.Stopped:
		case <-ctx.Done():
			t.Fatalf("request did not stop: %v", ctx.Err())
		}
	}
	const objective = "Preserve this goal across settings changes"
	start := func(objective, model, effort string) engine.GoalState {
		t.Helper()
		g, err := a.ActivateGoal(ctx, 1, thread.ID, home, objective, model, effort)
		if err != nil || g.Status != engine.GoalActive {
			t.Fatalf("activate goal: %+v %v", g, err)
		}
		return g
	}
	initial := start(objective, "old-goal-model", "low")
	first := nextCall("old-goal-model", "low")
	firstTurn := a.Info(1, thread.ID).NativeTurn
	switched := start("", "new-goal-model", "high")
	waitStopped(first)
	second := nextCall("new-goal-model", "high")
	secondTurn := a.Info(1, thread.ID).NativeTurn
	if firstTurn == "" || secondTurn == "" || firstTurn == secondTurn ||
		switched.Objective != initial.Objective || switched.CreatedAt != initial.CreatedAt {
		t.Fatalf("switch replaced the goal or reused the old turn: %+v -> %+v, %s -> %s", initial, switched, firstTurn, secondTurn)
	}
	if paused, err := a.StopGoal(ctx, 1, thread.ID, home, secondTurn, true); err != nil || !paused.Paused || !paused.Interrupted {
		t.Fatalf("pause goal: %+v %v", paused, err)
	}
	waitStopped(second)
	if state := a.Info(1, thread.ID); state.SelfTurn || state.Goal.State.Status != engine.GoalPaused {
		t.Fatalf("pause returned with a running goal: %+v", state)
	}
	start("", "resumed-goal-model", "medium")
	third := nextCall("resumed-goal-model", "medium")
	if _, err := a.StopGoal(ctx, 1, thread.ID, home, a.Info(1, thread.ID).NativeTurn, true); err != nil {
		t.Fatal(err)
	}
	waitStopped(third)

	// Reopen a persisted active goal on a cold server. Its first request must
	// already have the new settings because thread/resume can start it.
	a.Kill(1)
	seed, err := codex.Start(ctx, a.serverConfig(home))
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Kill()
	if _, err := seed.GoalSet(ctx, thread.ID, "", codex.GoalActive); err != nil {
		t.Fatal(err)
	}
	seed.Kill()
	start("", "cold-goal-model", "high")
	cold := nextCall("cold-goal-model", "high")
	if _, err := a.StopGoal(ctx, 1, thread.ID, home, a.Info(1, thread.ID).NativeTurn, true); err != nil {
		t.Fatal(err)
	}
	waitStopped(cold)
}
