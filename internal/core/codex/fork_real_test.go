package codex

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// A fork is loaded in its creator even before its first turn. Exercise the
// cross-process contract with the actual binary and an offline model server.
func TestRealCodexForkHandoff(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared=%v", shared), func(t *testing.T) {
			parent, responses, thread := realTitleServer(t)
			cfg := parent.cfg
			if shared {
				cfg.SharedDir = parent.cfg.Dir
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			first, err := parent.RunTurn(ctx, TurnStartParams{ThreadID: thread, ClientID: "fork-cutoff", Input: []Input{{Text: "Remember the completed parent turn."}}}, time.Second, TurnHooks{})
			if err != nil || first.Status != StatusCompleted {
				t.Fatalf("parent first turn: %+v, %v", first, err)
			}
			done := make(chan error, 1)
			go func() {
				out, err := parent.RunTurn(ctx, TurnStartParams{ThreadID: thread, ClientID: "fork-running", Input: []Input{{Text: "probe:hold:fork-parent"}}}, time.Second, TurnHooks{})
				if err == nil && out.Status != StatusCompleted {
					err = fmt.Errorf("parent ended with %s", out.Status)
				}
				done <- err
			}()
			select {
			case <-responses.Started("fork-parent"):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			pool := NewPool(PoolConfig{PIDFile: filepath.Join(parent.cfg.Dir, "fork-pids.sqlite")})
			defer pool.Close(time.Second)
			lease, err := pool.Acquire(ctx, "fork-probe", cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release()
			forker := lease.Server()
			child, err := forker.ThreadFork(ctx, thread, first.TurnID, parent.cfg.Dir)
			if err != nil || child.ID == "" {
				t.Fatalf("fork active parent from another process: %+v, %v", child, err)
			}
			reader, err := Start(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Kill()
			// Current Codex retains the writer; a future version may release
			// it earlier. Either is safe as long as handoff works after exit.
			if _, err := reader.ThreadResume(ctx, child.ID); err != nil && !IsActiveWriter(err) {
				t.Fatalf("unexpected rejection before creator exit: %v", err)
			}
			forker.Close(time.Second)
			if !forker.Exited() {
				t.Fatal("fork creator did not exit")
			}
			if _, err := reader.ThreadResume(ctx, child.ID); err != nil {
				t.Fatalf("resume after creator exit: %v", err)
			}
			history, err := reader.ThreadRead(ctx, child.ID, true)
			if err != nil || len(history.Turns) != 1 || history.Turns[0].ID != first.TurnID {
				t.Fatalf("fork lost cutoff or copied running turn: %+v, %v", history.Turns, err)
			}
			out, err := reader.RunTurn(ctx, TurnStartParams{ThreadID: child.ID, ClientID: "fork-child", Input: []Input{{Text: "Answer in the child."}}}, time.Second, TurnHooks{})
			if err != nil || out.Status != StatusCompleted {
				t.Fatalf("child turn: %+v, %v", out, err)
			}
			select {
			case err := <-done:
				t.Fatalf("parent stopped before release: %v", err)
			default:
			}
			responses.Release("fork-parent")
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
