package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"testing/synctest"
	"time"
)

// What a server knows of a thread (2026-11-07): the model of the start
// response, a turn/start override (thread/settings/updated), the goal
// (goal calls, thread/goal/* notifications) and the running turn, which
// keeps the server out of Reap.
func TestServerTracksThreadGoalSettingsAndTurn(t *testing.T) {
	s, b, cfg := startFake(t)
	ctx := tctx(t)
	th, err := s.ThreadStart(ctx, ThreadStartParams{Cwd: cfg.Dir})
	if err != nil {
		t.Fatal(err)
	}
	if i := s.Info(th.ID); i.Model != "gpt-5-codex" || i.Effort != "" || i.Tier != "" || i.Goal != nil {
		t.Fatalf("after start: %+v", i)
	}
	if g, err := s.GoalGet(ctx, th.ID); err != nil || g != nil {
		t.Fatalf("no goal: %+v %v", g, err)
	}
	g, err := s.GoalSet(ctx, th.ID, "Make it green", "")
	if err != nil || g.Status != GoalActive || g.Objective != "Make it green" {
		t.Fatalf("set: %+v %v", g, err)
	}
	if g, err := s.GoalSet(ctx, th.ID, "", GoalPaused); err != nil || g.Status != GoalPaused || g.Objective != "Make it green" {
		t.Fatalf("pause: %+v %v", g, err)
	}
	if i := s.Info(th.ID); i.Goal == nil || i.Goal.Status != GoalPaused {
		t.Fatalf("after pause: %+v", i)
	}
	started := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "k1", Model: "gpt-5-mini", Effort: "low", Input: []Input{{Text: "block"}}}, time.Minute,
			TurnHooks{Started: func(id string) { started <- id }})
	}()
	id := <-started
	deadline := time.Now().Add(10 * time.Second)
	for {
		i := s.Info(th.ID)
		if i.Turn == id && i.Model == "gpt-5-mini" && i.Effort == "low" && s.Running() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("during the turn: %+v running=%v", i, s.Running())
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.mu.Lock()
	close(b.release)
	b.mu.Unlock()
	<-done
	if i := s.Info(th.ID); i.Turn != "" || s.Running() {
		t.Fatalf("after the turn: %+v", i)
	}
	if err := s.GoalClear(ctx, th.ID); err != nil {
		t.Fatal(err)
	}
	if i := s.Info(th.ID); i.Goal != nil {
		t.Fatalf("after clear: %+v", i)
	}
}

func TestApplyThreadSettingsRequiresNativeSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := goalTestServer()
		s.subs = make(map[string]map[int]func(Notification))
		r, w := io.Pipe()
		defer r.Close()
		defer w.Close()
		s.in = w
		s.setSettings("thread", "old-model", nil, nil)
		wire := make(chan error, 1)
		go func() {
			var request wireMsg
			if err := json.NewDecoder(r).Decode(&request); err != nil {
				wire <- err
				return
			}
			if request.Method != "thread/settings/update" {
				wire <- fmt.Errorf("unexpected method %s", request.Method)
				return
			}
			s.handleLine([]byte(fmt.Sprintf(`{"id":%s,"result":{}}`, request.ID)))
			wire <- nil
		}()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := s.ApplyThreadSettings(ctx, "thread", "new-model", "high"); err == nil {
			t.Fatal("an acknowledged update without applied settings reported success")
		}
		if err := <-wire; err != nil {
			t.Fatal(err)
		}
	})
}
