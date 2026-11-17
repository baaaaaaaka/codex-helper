package codexagent

import (
	"context"
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
	if _, err := a.StopGoal(ctx, 1, th.ID, home, ""); err != nil {
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
	got, err := a.Goal(ctx, 1, th.ID, home, engine.GoalState{})
	if err != nil || got.Status != engine.GoalActive {
		t.Fatalf("cold read: %+v %v", got, err)
	}
	stopped, err := a.StopGoal(ctx, 1, th.ID, home, "")
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
	if _, err := a.Goal(ctx, 1, th.ID, home, engine.GoalState{Status: engine.GoalActive}); err != nil {
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
