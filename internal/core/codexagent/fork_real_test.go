package codexagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
)

// Use the production Agent.Fork path, not just raw RPC. The parent remains
// blocked in its native model request while the child runs in another process.
func TestRealCodexAgentFork(t *testing.T) {
	bin := os.Getenv("E2E_REAL_CODEX")
	if bin == "" {
		t.Skip("E2E_REAL_CODEX is not set")
	}
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared=%v", shared), func(t *testing.T) {
			responses := fakecodex.NewResponses()
			defer responses.Close()
			home := t.TempDir()
			if err := fakecodex.WriteCodexHome(home, responses.URL()); err != nil {
				t.Fatal(err)
			}
			cfg := codex.Config{Bin: bin, Env: fakecodex.CodexEnv(home), Dir: home}
			if shared {
				cfg.SharedDir = home
			}
			a := New(Config{Server: cfg, PIDFile: filepath.Join(home, "pids.sqlite"), Poll: time.Second})
			defer a.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			var thread string
			run := func(conv int64, thread, key, text string, hooks engine.TurnHooks) (engine.TurnOutcome, error) {
				return a.Run(ctx, engine.TurnRequest{ConvID: conv, ThreadID: thread, Cwd: home, TurnKey: key, Text: text, Model: "probe-model", Effort: "low"}, hooks)
			}
			first, err := run(1, "", "parent-first", "Remember the completed parent turn.", engine.TurnHooks{ThreadStarted: func(id string) error { thread = id; return nil }})
			if err != nil || first.Status != engine.StatusCompleted {
				t.Fatalf("first: %+v %v", first, err)
			}
			parent := a.pool.Peek(key(1))
			done := make(chan error, 1)
			go func() {
				out, err := run(1, thread, "parent-running", "probe:hold:agent-fork", engine.TurnHooks{})
				if err == nil && out.Status != engine.StatusCompleted {
					err = fmt.Errorf("parent ended with %s", out.Status)
				}
				done <- err
			}()
			select {
			case <-responses.Started("agent-fork"):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			child, err := a.Fork(ctx, 1, thread, first.CodexTurnID, home)
			if err != nil || child.ThreadID == "" {
				t.Fatalf("fork: %+v %v", child, err)
			}
			if a.pool.Size() != 1 || a.pool.Peek(key(1)) != parent || parent.Exited() {
				t.Fatal("fork retained a temporary server or changed the parent")
			}
			out, err := run(2, child.ThreadID, "child-first", "Answer in the child.", engine.TurnHooks{})
			if err != nil || out.Status != engine.StatusCompleted {
				t.Fatalf("child: %+v %v", out, err)
			}
			if a.pool.Peek(key(2)) == parent {
				t.Fatal("child reused parent server")
			}
			select {
			case err := <-done:
				t.Fatalf("parent stopped before release: %v", err)
			default:
			}
			responses.Release("agent-fork")
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}
