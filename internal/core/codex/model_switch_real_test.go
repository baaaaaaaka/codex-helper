package codex

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Interrupt acknowledgement is not a terminal event. Observe the terminal,
// then verify input retained in history before starting on different settings.
// This is an offline contract with the actual Codex binary, not fake RPC.
func TestRealCodexSharedModelSwitch(t *testing.T) {
	for _, queue := range []bool{false, true} {
		t.Run(fmt.Sprintf("queue=%v", queue), func(t *testing.T) {
			srv, responses, thread := realShared(t)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			started := make(chan string, 1)
			type result struct {
				out TurnResult
				err error
			}
			done := make(chan result, 1)
			go func() {
				out, err := srv.RunTurn(ctx, TurnStartParams{ThreadID: thread, ClientID: "switch-original", Input: []Input{{Text: "original probe:hold:switch"}}, Model: "gpt-5-codex", Effort: "low", Queue: queue}, time.Second, TurnHooks{Started: func(id string) { started <- id }})
				done <- result{out, err}
			}()
			var turn string
			select {
			case turn = <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-responses.Started("switch"):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := srv.TurnInterrupt(ctx, thread, turn); err != nil {
				t.Fatal(err)
			}
			select {
			case r := <-done:
				if r.err != nil || r.out.Status != StatusInterrupted {
					t.Fatalf("old terminal: %+v, %v", r.out, r.err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			for _, client := range []string{"switch-original"} {
				recorded, found, err := srv.FindTurn(ctx, thread, client)
				if err != nil || !found || recorded.ID != turn {
					t.Fatalf("retained %s: found=%v turn=%s err=%v", client, found, recorded.ID, err)
				}
			}
			out, err := srv.RunTurn(ctx, TurnStartParams{ThreadID: thread, ClientID: "switch-continuation", Input: []Input{{Text: "Continue the unfinished task."}}, Model: "gpt-5.1-codex", Effort: "high", Queue: queue}, time.Second, TurnHooks{})
			if err != nil || out.Status != StatusCompleted || out.TurnID == turn {
				t.Fatalf("continuation: %+v, %v", out, err)
			}
			reqs := responses.Requests()
			if len(reqs) != 2 {
				t.Fatalf("model requests: %+v", reqs)
			}
			if reqs[1].Model != "gpt-5.1-codex" || reqs[1].Effort != "high" {
				t.Fatalf("new request lost settings or history: %+v", reqs[1])
			}
		})
	}
}

// An accepted steer can still be pending model input when interrupt completes.
// Record both outcomes: the engine must require history evidence, not success
// of the steer RPC. A persisted steer is also tested after model admission.
func TestRealCodexSharedSwitchInputBoundary(t *testing.T) {
	for _, sampled := range []bool{false, true} {
		t.Run(fmt.Sprintf("sampled=%v", sampled), func(t *testing.T) {
			srv, responses, thread := realShared(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			turn, err := srv.TurnStart(ctx, TurnStartParams{ThreadID: thread, ClientID: "boundary-original", Input: []Input{{Text: "initial probe:hold:boundary-original"}}})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-responses.Started("boundary-original"):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err = srv.TurnSteer(ctx, TurnStartParams{ThreadID: thread, ClientID: "boundary-supplement", Input: []Input{{Text: "supplement probe:hold:boundary-supplement"}}}, turn); err != nil {
				t.Fatal(err)
			}
			if sampled {
				responses.Release("boundary-original")
				select {
				case <-responses.Started("boundary-supplement"):
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			terminal := make(chan struct{}, 1)
			unsub := srv.Subscribe(thread, func(n Notification) {
				if n.Method == "turn/completed" {
					select {
					case terminal <- struct{}{}:
					default:
					}
				}
			})
			defer unsub()
			if err = srv.TurnInterrupt(ctx, thread, turn); err != nil {
				t.Fatal(err)
			}
			select {
			case <-terminal:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			retained, found, err := srv.FindTurn(ctx, thread, "boundary-supplement")
			if err != nil {
				t.Fatal(err)
			}
			if sampled && (!found || retained.ID != turn) {
				t.Fatalf("sampled input disappeared: found=%v turn=%s", found, retained.ID)
			}
			if found && retained.ID != turn {
				t.Fatal("input moved to unrelated turn")
			}
			t.Logf("steer RPC succeeded; sampled=%v, retained after terminal=%v", sampled, found)
		})
	}
}
