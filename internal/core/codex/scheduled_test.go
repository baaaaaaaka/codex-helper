package codex

import (
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestScheduledSleepItemRequiresNativeEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		want      bool
	}{
		{"native", `{"type":"sleep","id":"s","durationMs":600000}`, true},
		{"ordinary", `{"type":"agentMessage","id":"s","durationMs":600000}`, false},
		{"missing id", `{"type":"sleep","durationMs":600000}`, false},
		{"negative", `{"type":"sleep","id":"s","durationMs":-1}`, false},
		{"overflow", `{"type":"sleep","id":"s","durationMs":9223372036854775808}`, false},
		{"over limit", `{"type":"sleep","id":"s","durationMs":43200001}`, false},
		{"malformed", `{"type":"sleep","id":"s","durationMs":"600000"}`, false},
		{"ordinary malformed optional field", `{"type":"agentMessage","text":"ordinary","durationMs":{"future":true}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var it Item
			if err := json.Unmarshal([]byte(tc.raw), &it); err != nil {
				t.Fatal(err)
			}
			d, ok := nativeSleepDuration(it, json.RawMessage(`{"item":`+tc.raw+`}`))
			if ok != tc.want || ok && d != 10*time.Minute {
				t.Fatalf("duration %v valid %v", d, ok)
			}
		})
	}
}

// Control the wire order instead of relying on a process being fast or slow:
// native items may arrive before turn/start (or queue/start) returns.
func TestScheduledSleepBeforeStartAcknowledgement(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, completedEarly := range []bool{false, true} {
			t.Run(fmt.Sprintf("queue=%t/complete=%t", queued, completedEarly), func(t *testing.T) {
				ctx := tctx(t)
				wire := make(replySink, 8)
				s := &Server{in: wire, cfg: Config{Log: io.Discard}, fault: make(chan struct{}), pending: map[int64]chan rpcResult{}, subs: map[string]map[int]func(Notification){}}
				events := make(chan bool, 4)
				started, done := make(chan struct{}), make(chan error, 1)
				go func() {
					_, err := s.RunTurn(ctx, TurnStartParams{ThreadID: "th", ClientID: "client", Queue: queued}, time.Hour, TurnHooks{
						Started: func(string) { close(started) }, Waiting: func(_ string, _ time.Duration, start bool) { events <- start },
					})
					done <- err
				}()
				request := func(method string) wireMsg {
					t.Helper()
					var req wireMsg
					select {
					case b := <-wire:
						if err := json.Unmarshal(b, &req); err != nil || req.Method != method {
							t.Fatalf("request %+v: %v", req, err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					return req
				}
				respond := func(req wireMsg, result string) {
					s.handleLine([]byte(`{"id":` + string(req.ID) + `,"result":` + result + `}`))
				}
				var req wireMsg
				if queued {
					req = request("thread/queue/add")
					// Earlier native work belongs to somebody else.
					s.handleLine([]byte(`{"method":"item/started","params":{"threadId":"th","turnId":"previous","item":{"type":"sleep","id":"previous-sleep","durationMs":1000}}}`))
					respond(req, `{"queuedSubmission":{"id":"q"}}`)
					req = request("thread/queue/start")
				} else {
					req = request("turn/start")
				}
				s.handleLine([]byte(`{"method":"item/started","params":{"threadId":"th","turnId":"mine","item":{"type":"userMessage","clientId":"client"}}}`))
				s.handleLine([]byte(`{"method":"item/started","params":{"threadId":"th","turnId":"mine","item":{"type":"sleep","id":"sleep","durationMs":1000}}}`))
				if completedEarly {
					s.handleLine([]byte(`{"method":"item/completed","params":{"threadId":"th","turnId":"mine","item":{"type":"sleep","id":"sleep","durationMs":1000}}}`))
				}
				// A foreign turn cannot replace the buffered evidence for mine.
				s.handleLine([]byte(`{"method":"item/started","params":{"threadId":"th","turnId":"foreign","item":{"type":"sleep","id":"foreign-sleep","durationMs":1000}}}`))
				respond(req, `{"turn":{"id":"mine"}}`)
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case on := <-events:
					if on == completedEarly {
						t.Fatal("restored a completed wait or lost an active wait")
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				s.handleLine([]byte(`{"method":"turn/completed","params":{"threadId":"th","turn":{"id":"mine","status":"completed","items":[{"type":"agentMessage","phase":"final_answer","text":"done"}]}}}`))
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if len(events) != 0 {
					t.Fatal("duplicate or foreign wait callback")
				}
			})
		}
	}
}

func TestScheduledRunTurnSleepLifecycleAndTurnIsolation(t *testing.T) {
	s, b, cfg := startFake(t)
	ctx := tctx(t)
	th, err := s.ThreadStart(ctx, ThreadStartParams{Cwd: cfg.Dir})
	if err != nil {
		t.Fatal(err)
	}
	type event struct {
		id       string
		duration time.Duration
		started  bool
	}
	events := make(chan event, 8)
	started := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		_, err := s.RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "scheduled-wait-test", Input: []Input{{Text: "block for sleep lifecycle"}}}, time.Minute, TurnHooks{
			Started: func(id string) { started <- id }, Waiting: func(id string, d time.Duration, on bool) { events <- event{id, d, on} },
		})
		done <- err
	}()
	var id string
	select {
	case id = <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	emit := func(turn, kind, item string, duration any) {
		data, _ := json.Marshal(map[string]any{"method": kind, "params": map[string]any{"threadId": th.ID, "turnId": turn, "item": map[string]any{"type": "sleep", "id": item, "durationMs": duration}}})
		s.handleLine(data)
	}
	emit("other-turn", "item/started", "other", 600000)
	emit(id, "item/started", "invalid", -1)
	emit(id, "item/started", "sleep-1", 600000)
	emit("other-turn", "item/completed", "sleep-1", 600000)
	emit(id, "item/completed", "sleep-1", 600000)
	for i, want := range []event{{"sleep-1", 10 * time.Minute, true}, {"sleep-1", 10 * time.Minute, false}} {
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("event %d: %+v want %+v", i, got, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case extra := <-events:
		t.Fatalf("unexpected event %+v", extra)
	default:
	}
	b.mu.Lock()
	close(b.release)
	b.mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The callback is unsubscribed with the run, so late items cannot affect it.
	emit(id, "item/started", "late", 600000)
	select {
	case extra := <-events:
		t.Fatal(fmt.Sprintf("late callback %+v", extra))
	default:
	}
}
