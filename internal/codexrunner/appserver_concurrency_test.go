package codexrunner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAppServerRunnerParallelTurnsInterleavedResponsesAndApprovals(t *testing.T) {
	transport := newMultiplexAppServerTransport()
	handler := newParallelApprovalHandler(2)
	runner := &AppServerRunner{
		Transport:            transport,
		ServerRequestHandler: handler,
	}
	defer runner.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type turnOutcome struct {
		result TurnResult
		err    error
	}
	results := make(chan turnOutcome, 2)
	for _, threadID := range []string{"thread-a", "thread-b"} {
		threadID := threadID
		go func() {
			result, err := runner.StartTurn(ctx, StartTurnInput{
				ThreadID:  threadID,
				TurnInput: TurnInput{Prompt: "run " + threadID},
			})
			results <- turnOutcome{result: result, err: err}
		}()
	}

	got := make(map[string]TurnResult)
	for range 2 {
		outcome := <-results
		if outcome.err != nil {
			t.Fatalf("parallel turn failed: result=%#v err=%v", outcome.result, outcome.err)
		}
		got[outcome.result.ThreadID] = outcome.result
	}
	for _, threadID := range []string{"thread-a", "thread-b"} {
		result := got[threadID]
		if result.Status != TurnStatusCompleted || result.FinalAgentMessage != "done "+threadID {
			t.Fatalf("result for %s = %#v", threadID, result)
		}
	}
	if handler.maxConcurrent() != 2 {
		t.Fatalf("approval handlers max concurrency = %d, want 2", handler.maxConcurrent())
	}

	deadline := time.Now().Add(time.Second)
	for {
		runner.protocolMu.Lock()
		pending := len(runner.pendingRequests)
		subscribers := len(runner.turnSubscribers)
		serverRequests := len(runner.serverRequests)
		runner.protocolMu.Unlock()
		if pending == 0 && subscribers == 0 && serverRequests == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("protocol state leaked: pending=%d subscribers=%d serverRequests=%d", pending, subscribers, serverRequests)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAppServerRunnerCancelDuringApprovalDelayRejectsLateApproval(t *testing.T) {
	transport := newCancelApprovalTransport()
	handler := &cancelAwareApprovalHandler{started: make(chan struct{}), delay: time.Second}
	runner := &AppServerRunner{
		Transport:            transport,
		ServerRequestHandler: handler,
	}
	defer runner.Close()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID:  "thread-cancel",
			TurnInput: TurnInput{Prompt: "cancel me"},
		})
		result <- err
	}()
	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("approval request was not sent")
	}
	cancel()
	select {
	case err := <-result:
		if !IsKind(err, ErrorCanceled) {
			t.Fatalf("turn error = %v, want canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("turn did not stop after cancellation")
	}
	select {
	case raw := <-transport.approvalResponse:
		var response appServerMessage
		if json.Unmarshal(raw, &response) != nil || response.Error == nil || len(response.Result) != 0 {
			t.Fatalf("approval response = %s, want fail-closed error", raw)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled approval did not receive a fail-closed response")
	}
}

func TestAppServerRunnerDisconnectDuringApprovalDelayFailsPendingTurn(t *testing.T) {
	transport := newCancelApprovalTransport()
	handler := &cancelAwareApprovalHandler{started: make(chan struct{}), delay: 10 * time.Second}
	runner := &AppServerRunner{Transport: transport, ServerRequestHandler: handler}
	result := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(context.Background(), StartTurnInput{
			ThreadID:  "thread-disconnect",
			TurnInput: TurnInput{Prompt: "disconnect me"},
		})
		result <- err
	}()
	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("approval request was not sent")
	}
	closed := make(chan error, 1)
	go func() { closed <- runner.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("runner close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner close waited for the full approval delay")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("pending turn succeeded after app-server disconnect")
		}
	case <-time.After(time.Second):
		t.Fatal("pending turn was not failed after app-server disconnect")
	}
}

func TestAppServerRunnerSerializesTurnsOnSameThread(t *testing.T) {
	transport := newSameThreadTurnTransport()
	runner := &AppServerRunner{Transport: transport}
	defer runner.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := runner.StartTurn(ctx, StartTurnInput{
				ThreadID:  "shared-thread",
				TurnInput: TurnInput{Prompt: "run"},
			})
			results <- err
		}()
	}

	if sequence := <-transport.started; sequence != 1 {
		t.Fatalf("first turn sequence = %d, want 1", sequence)
	}
	select {
	case sequence := <-transport.started:
		t.Fatalf("same-thread turn %d started before turn 1 completed", sequence)
	case <-time.After(50 * time.Millisecond):
	}
	transport.complete(ctx, 1)
	if err := <-results; err != nil {
		t.Fatalf("first same-thread turn failed: %v", err)
	}
	if sequence := <-transport.started; sequence != 2 {
		t.Fatalf("second turn sequence = %d, want 2", sequence)
	}
	transport.complete(ctx, 2)
	if err := <-results; err != nil {
		t.Fatalf("second same-thread turn failed: %v", err)
	}
}

func TestAppServerRunnerExplicitCancelInterruptsBeforeNextSameThreadTurn(t *testing.T) {
	transport := newExplicitCancelTurnTransport()
	runner := &AppServerRunner{Transport: transport}
	defer runner.Close()

	ctx, cancel := context.WithCancelCause(context.Background())
	turnObserved := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID: "shared-thread",
			TurnInput: TurnInput{
				Prompt: "cancel me",
				EventHandler: func(event StreamEvent) {
					if event.Kind == StreamEventTurnStarted {
						close(turnObserved)
					}
				},
			},
		})
		firstDone <- err
	}()

	if sequence := <-transport.started; sequence != 1 {
		t.Fatalf("first turn sequence = %d, want 1", sequence)
	}
	select {
	case <-turnObserved:
	case <-time.After(time.Second):
		t.Fatal("first turn was not observed as started")
	}
	cancel(ErrTurnInterruptRequested)

	secondDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(context.Background(), StartTurnInput{
			ThreadID:  "shared-thread",
			TurnInput: TurnInput{Prompt: "run after cancel"},
		})
		secondDone <- err
	}()

	select {
	case ref := <-transport.interrupted:
		if ref.ThreadID != "shared-thread" || ref.TurnID != "turn-1" {
			t.Fatalf("interrupt ref = %#v, want shared-thread/turn-1", ref)
		}
	case sequence := <-transport.started:
		t.Fatalf("same-thread turn %d started before turn 1 was interrupted", sequence)
	case <-time.After(time.Second):
		t.Fatal("explicit cancellation did not send turn/interrupt")
	}

	transport.finishInterrupt(context.Background())
	if err := <-firstDone; !IsKind(err, ErrorCanceled) {
		t.Fatalf("first turn error = %v, want canceled", err)
	}
	if sequence := <-transport.started; sequence != 2 {
		t.Fatalf("second turn sequence = %d, want 2", sequence)
	}
	transport.complete(context.Background(), 2)
	if err := <-secondDone; err != nil {
		t.Fatalf("second turn failed: %v", err)
	}
}

func TestAppServerRunnerInterruptAckDoesNotReleaseThreadBeforeTerminal(t *testing.T) {
	transport := newExplicitCancelTurnTransport()
	runner := &AppServerRunner{Transport: transport}
	defer runner.Close()

	ctx, cancel := context.WithCancelCause(context.Background())
	turnObserved := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID: "shared-thread",
			TurnInput: TurnInput{
				Prompt: "cancel me",
				EventHandler: func(event StreamEvent) {
					if event.Kind == StreamEventTurnStarted {
						close(turnObserved)
					}
				},
			},
		})
		firstDone <- err
	}()
	if sequence := <-transport.started; sequence != 1 {
		t.Fatalf("first turn sequence = %d, want 1", sequence)
	}
	select {
	case <-turnObserved:
	case <-time.After(time.Second):
		t.Fatal("first turn was not observed as started")
	}
	cancel(ErrTurnInterruptRequested)
	<-transport.interrupted
	transport.ackInterrupt(context.Background())

	secondDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(context.Background(), StartTurnInput{
			ThreadID:  "shared-thread",
			TurnInput: TurnInput{Prompt: "run only after terminal"},
		})
		secondDone <- err
	}()
	select {
	case err := <-firstDone:
		t.Fatalf("first turn returned before turn/completed: %v", err)
	case sequence := <-transport.started:
		t.Fatalf("same-thread turn %d started before turn/completed", sequence)
	case <-time.After(50 * time.Millisecond):
	}

	transport.finishInterruptedTurn(context.Background())
	if err := <-firstDone; !IsKind(err, ErrorCanceled) {
		t.Fatalf("first turn error = %v, want canceled", err)
	}
	if sequence := <-transport.started; sequence != 2 {
		t.Fatalf("second turn sequence = %d, want 2", sequence)
	}
	transport.complete(context.Background(), 2)
	if err := <-secondDone; err != nil {
		t.Fatalf("second turn failed: %v", err)
	}
}

func TestAppServerRunnerExplicitCancelDuringTurnStartWaitsForExactInterrupt(t *testing.T) {
	transport := newExplicitCancelTurnTransport()
	transport.delayFirstStartResponse = true
	runner := &AppServerRunner{Transport: transport}
	defer runner.Close()

	ctx, cancel := context.WithCancelCause(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID:  "shared-thread",
			TurnInput: TurnInput{Prompt: "cancel during start"},
		})
		firstDone <- err
	}()
	if sequence := <-transport.started; sequence != 1 {
		t.Fatalf("first turn sequence = %d, want 1", sequence)
	}
	cancel(ErrTurnInterruptRequested)

	select {
	case ref := <-transport.interrupted:
		t.Fatalf("turn/interrupt was sent without an exact turn id: %#v", ref)
	case <-time.After(50 * time.Millisecond):
	}
	transport.finishStartResponse(context.Background())
	select {
	case ref := <-transport.interrupted:
		if ref.ThreadID != "shared-thread" || ref.TurnID != "turn-1" {
			t.Fatalf("interrupt ref = %#v, want shared-thread/turn-1", ref)
		}
	case <-time.After(time.Second):
		t.Fatal("turn/start response did not trigger an exact interrupt")
	}
	transport.finishInterrupt(context.Background())
	select {
	case err := <-firstDone:
		if !IsKind(err, ErrorCanceled) {
			t.Fatalf("turn error = %v, want canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("turn did not return after exact interrupt confirmation")
	}
}

func TestAppServerRunnerTurnStartErrorWinningCancelRaceIsPreserved(t *testing.T) {
	transport := newExplicitCancelTurnTransport()
	transport.delayFirstStartResponse = true
	runner := &AppServerRunner{Transport: transport}
	defer runner.Close()

	ctx, cancel := context.WithCancelCause(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID:  "shared-thread",
			TurnInput: TurnInput{Prompt: "rejected while canceling"},
		})
		firstDone <- err
	}()
	if sequence := <-transport.started; sequence != 1 {
		t.Fatalf("first turn sequence = %d, want 1", sequence)
	}
	cancel(ErrTurnInterruptRequested)
	transport.finishStartError(context.Background(), "turn rejected before start")
	select {
	case err := <-firstDone:
		if !IsKind(err, ErrorCodex) || !strings.Contains(err.Error(), "turn rejected before start") {
			t.Fatalf("turn/start error = %v, want original Codex rejection", err)
		}
	case <-time.After(time.Second):
		t.Fatal("turn/start rejection was replaced by cancellation handoff wait")
	}
	select {
	case ref := <-transport.interrupted:
		t.Fatalf("rejected turn unexpectedly sent interrupt: %#v", ref)
	default:
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(context.Background(), StartTurnInput{
			ThreadID:  "shared-thread",
			TurnInput: TurnInput{Prompt: "run after rejected start"},
		})
		secondDone <- err
	}()
	if sequence := <-transport.started; sequence != 2 {
		t.Fatalf("second turn sequence = %d, want 2", sequence)
	}
	transport.complete(context.Background(), 2)
	if err := <-secondDone; err != nil {
		t.Fatalf("second turn failed after rejected start: %v", err)
	}
}

func TestAppServerRunnerUnconfirmedTurnStartReturnsStartedStatus(t *testing.T) {
	transport := newExplicitCancelTurnTransport()
	transport.delayFirstStartResponse = true
	runner := &AppServerRunner{Transport: transport}
	defer runner.Close()

	ctx, cancel := context.WithCancelCause(context.Background())
	type outcome struct {
		result TurnResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID:  "shared-thread",
			TurnInput: TurnInput{Prompt: "lose start response"},
		})
		done <- outcome{result: result, err: err}
	}()
	if sequence := <-transport.started; sequence != 1 {
		t.Fatalf("turn sequence = %d, want 1", sequence)
	}
	cancel(ErrTurnInterruptRequested)
	_ = transport.Close()
	select {
	case got := <-done:
		if !IsKind(got.err, ErrorCodex) || !errors.Is(got.err, errAppServerTurnStartUnconfirmed) {
			t.Fatalf("unconfirmed turn/start error = %v", got.err)
		}
		if got.result.ThreadID != "shared-thread" || got.result.TurnID != "" || got.result.Status != TurnStatusStarted {
			t.Fatalf("unconfirmed turn/start result = %#v, want started ambiguity", got.result)
		}
	case <-time.After(time.Second):
		t.Fatal("transport failure did not end turn/start confirmation")
	}
}

func TestAppServerRunnerLifecycleCancelDoesNotInterruptTurn(t *testing.T) {
	transport := newExplicitCancelTurnTransport()
	runner := &AppServerRunner{Transport: transport}
	defer runner.Close()

	ctx, cancel := context.WithCancel(context.Background())
	turnObserved := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID: "shared-thread",
			TurnInput: TurnInput{
				Prompt: "detach without interrupt",
				EventHandler: func(event StreamEvent) {
					if event.Kind == StreamEventTurnStarted {
						close(turnObserved)
					}
				},
			},
		})
		done <- err
	}()
	if sequence := <-transport.started; sequence != 1 {
		t.Fatalf("turn sequence = %d, want 1", sequence)
	}
	select {
	case <-turnObserved:
	case <-time.After(time.Second):
		t.Fatal("turn was not observed as started")
	}
	cancel()
	if err := <-done; !IsKind(err, ErrorCanceled) {
		t.Fatalf("turn error = %v, want lifecycle cancellation", err)
	}
	select {
	case ref := <-transport.interrupted:
		t.Fatalf("lifecycle cancellation unexpectedly interrupted turn: %#v", ref)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAppServerRunnerUnconfirmedExplicitInterruptRetriesBeforeNextTurn(t *testing.T) {
	transport := newExplicitCancelTurnTransport()
	transport.exactInterruptError = "expected active turn id turn-1 but found turn-other"
	transport.exactInterruptErrorsRemaining = 1
	runner := &AppServerRunner{Transport: transport}
	defer runner.Close()

	ctx, cancel := context.WithCancelCause(context.Background())
	turnObserved := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID: "shared-thread",
			TurnInput: TurnInput{
				Prompt: "cancel with ambiguous interrupt",
				EventHandler: func(event StreamEvent) {
					if event.Kind == StreamEventTurnStarted {
						close(turnObserved)
					}
				},
			},
		})
		firstDone <- err
	}()
	if sequence := <-transport.started; sequence != 1 {
		t.Fatalf("turn sequence = %d, want 1", sequence)
	}
	select {
	case <-turnObserved:
	case <-time.After(time.Second):
		t.Fatal("turn was not observed as started")
	}
	cancel(ErrTurnInterruptRequested)
	select {
	case ref := <-transport.interrupted:
		if ref.TurnID != "turn-1" {
			t.Fatalf("interrupt ref = %#v, want turn-1", ref)
		}
	case <-time.After(time.Second):
		t.Fatal("explicit cancellation did not send exact interrupt")
	}
	if err := <-firstDone; !IsKind(err, ErrorCodex) {
		t.Fatalf("first turn error = %v, want unconfirmed Codex error", err)
	} else if !errors.Is(err, ErrTurnCancelFenceUnconfirmed) {
		t.Fatalf("first turn error = %v, want cancellation-fence sentinel", err)
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(context.Background(), StartTurnInput{
			ThreadID:  "shared-thread",
			TurnInput: TurnInput{Prompt: "retry cleanup first"},
		})
		secondDone <- err
	}()
	select {
	case ref := <-transport.interrupted:
		if ref.ThreadID != "shared-thread" || ref.TurnID != "turn-1" {
			t.Fatalf("cleanup interrupt ref = %#v, want shared-thread/turn-1", ref)
		}
	case sequence := <-transport.started:
		t.Fatalf("same-thread turn %d started before cancel fence was cleared", sequence)
	case <-time.After(time.Second):
		t.Fatal("next same-thread turn did not retry exact interrupt")
	}
	transport.finishInterrupt(context.Background())
	if sequence := <-transport.started; sequence != 2 {
		t.Fatalf("second turn sequence = %d, want 2", sequence)
	}
	transport.complete(context.Background(), 2)
	if err := <-secondDone; err != nil {
		t.Fatalf("second turn failed after cleanup retry: %v", err)
	}
}

func TestAppServerRunnerPersistentCancelFenceDoesNotStartSameThread(t *testing.T) {
	transport := newExplicitCancelTurnTransport()
	transport.exactInterruptError = "turn remains active"
	transport.exactInterruptErrorsRemaining = -1
	runner := &AppServerRunner{Transport: transport}
	defer runner.Close()

	ctx, cancel := context.WithCancelCause(context.Background())
	turnObserved := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID: "shared-thread",
			TurnInput: TurnInput{
				Prompt: "leave a cancel fence",
				EventHandler: func(event StreamEvent) {
					if event.Kind == StreamEventTurnStarted {
						close(turnObserved)
					}
				},
			},
		})
		firstDone <- err
	}()
	if sequence := <-transport.started; sequence != 1 {
		t.Fatalf("turn sequence = %d, want 1", sequence)
	}
	select {
	case <-turnObserved:
	case <-time.After(time.Second):
		t.Fatal("turn was not observed as started")
	}
	cancel(ErrTurnInterruptRequested)
	<-transport.interrupted
	if err := <-firstDone; !IsKind(err, ErrorCodex) {
		t.Fatalf("first turn error = %v, want unconfirmed Codex error", err)
	}

	_, err := runner.StartTurn(context.Background(), StartTurnInput{
		ThreadID:  "shared-thread",
		TurnInput: TurnInput{Prompt: "must fail closed"},
	})
	if !IsKind(err, ErrorCodex) {
		t.Fatalf("next same-thread turn error = %v, want recoverable cancel-fence error", err)
	}
	if !errors.Is(err, ErrTurnCancelFenceUnconfirmed) {
		t.Fatalf("next same-thread turn error = %v, want cancellation-fence sentinel", err)
	}
	if !strings.Contains(err.Error(), "Previous Codex turn cancellation is still unconfirmed; no new turn was started") {
		t.Fatalf("next same-thread turn error = %v, want explicit cancellation-fence diagnostic", err)
	}
	select {
	case sequence := <-transport.started:
		t.Fatalf("fenced same-thread turn unexpectedly started as sequence %d", sequence)
	default:
	}

	otherDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(context.Background(), StartTurnInput{
			ThreadID:  "other-thread",
			TurnInput: TurnInput{Prompt: "unrelated thread remains usable"},
		})
		otherDone <- err
	}()
	if sequence := <-transport.started; sequence != 2 {
		t.Fatalf("other-thread turn sequence = %d, want 2", sequence)
	}
	transport.completeFor(context.Background(), "other-thread", 2)
	if err := <-otherDone; err != nil {
		t.Fatalf("cancel fence affected unrelated thread: %v", err)
	}
}

func TestAppServerRunnerUnknownTurnCancelFenceWaitsForIdle(t *testing.T) {
	transport := newExplicitCancelTurnTransport()
	runner := &AppServerRunner{Transport: transport}
	defer runner.Close()
	if err := runner.ensureReady(context.Background()); err != nil {
		t.Fatalf("initialize runner: %v", err)
	}

	gate, unlock := runner.lockThreadTurn("shared-thread")
	defer unlock()
	gate.cancelFence = &appServerTurnCancelFence{}
	if err := runner.reconcileTurnCancelFence("shared-thread", gate); !IsKind(err, ErrorCodex) {
		t.Fatalf("active unknown-turn fence error = %v, want Codex error", err)
	}
	transport.mu.Lock()
	transport.threadStatus = "idle"
	transport.mu.Unlock()
	if err := runner.reconcileTurnCancelFence("shared-thread", gate); err != nil {
		t.Fatalf("idle unknown-turn fence did not recover: %v", err)
	}
	if gate.cancelFence != nil {
		t.Fatal("idle unknown-turn fence was not cleared")
	}
}

func TestAppServerRunnerCompletionWinningCancelRaceDoesNotBlockNextTurn(t *testing.T) {
	transport := newExplicitCancelTurnTransport()
	transport.completeBeforeExactInterrupt = true
	runner := &AppServerRunner{Transport: transport}
	defer runner.Close()

	ctx, cancel := context.WithCancelCause(context.Background())
	turnObserved := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID: "shared-thread",
			TurnInput: TurnInput{
				Prompt: "finish while canceling",
				EventHandler: func(event StreamEvent) {
					if event.Kind == StreamEventTurnStarted {
						close(turnObserved)
					}
				},
			},
		})
		firstDone <- err
	}()
	if sequence := <-transport.started; sequence != 1 {
		t.Fatalf("turn sequence = %d, want 1", sequence)
	}
	select {
	case <-turnObserved:
	case <-time.After(time.Second):
		t.Fatal("turn was not observed as started")
	}
	cancel(ErrTurnInterruptRequested)
	select {
	case ref := <-transport.interrupted:
		if ref.TurnID != "turn-1" {
			t.Fatalf("interrupt ref = %#v, want turn-1", ref)
		}
	case <-time.After(time.Second):
		t.Fatal("explicit cancellation did not send exact interrupt")
	}
	if err := <-firstDone; !IsKind(err, ErrorCanceled) {
		t.Fatalf("first turn error = %v, want canceled race result", err)
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(context.Background(), StartTurnInput{
			ThreadID:  "shared-thread",
			TurnInput: TurnInput{Prompt: "run after completed race"},
		})
		secondDone <- err
	}()
	if sequence := <-transport.started; sequence != 2 {
		t.Fatalf("second turn sequence = %d, want 2", sequence)
	}
	transport.complete(context.Background(), 2)
	if err := <-secondDone; err != nil {
		t.Fatalf("second turn failed after completion race: %v", err)
	}
}

func TestDecodeThreadIdle(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		idle bool
		ok   bool
	}{
		{name: "nested string idle", raw: `{"thread":{"id":"thread","status":"idle"}}`, idle: true, ok: true},
		{name: "nested object idle", raw: `{"thread":{"id":"thread","status":{"type":"idle"}}}`, idle: true, ok: true},
		{name: "top-level active", raw: `{"status":"active"}`, idle: false, ok: true},
		{name: "nested object active", raw: `{"thread":{"status":{"type":"active","activeFlags":[]}}}`, idle: false, ok: true},
		{name: "not loaded fails closed", raw: `{"thread":{"status":{"type":"notLoaded"}}}`, idle: false, ok: true},
		{name: "system error fails closed", raw: `{"thread":{"status":{"type":"systemError"}}}`, idle: false, ok: true},
		{name: "unknown fails closed", raw: `{"thread":{"status":"futureStatus"}}`, idle: false, ok: false},
		{name: "missing fails closed", raw: `{"thread":{"id":"thread"}}`, idle: false, ok: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idle, ok := decodeThreadIdle(json.RawMessage(test.raw))
			if idle != test.idle || ok != test.ok {
				t.Fatalf("decodeThreadIdle() = (%t, %t), want (%t, %t)", idle, ok, test.idle, test.ok)
			}
		})
	}
}

func TestAppServerTerminalNotificationForTurnRequiresExactTerminal(t *testing.T) {
	tests := []struct {
		name string
		line string
		want bool
	}{
		{name: "exact interrupted", line: `{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"interrupted","items":[]}}}`, want: true},
		{name: "exact completed", line: `{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed","items":[]}}}`, want: true},
		{name: "wrong turn", line: `{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"other","status":"interrupted","items":[]}}}`},
		{name: "wrong thread", line: `{"method":"turn/completed","params":{"threadId":"other","turn":{"id":"turn","status":"interrupted","items":[]}}}`},
		{name: "not terminal", line: `{"method":"turn/started","params":{"threadId":"thread","turn":{"id":"turn","status":"inProgress","items":[]}}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := appServerTerminalNotificationForTurn([]byte(test.line), "thread", "turn"); got != test.want {
				t.Fatalf("appServerTerminalNotificationForTurn() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestAppServerRunnerDuplicateApprovalRequestIsIdempotent(t *testing.T) {
	transport := &recordingAppServerTransport{}
	handler := &countingApprovalHandler{release: make(chan struct{})}
	runner := &AppServerRunner{
		Transport:               transport,
		ServerRequestHandler:    handler,
		serverRequests:          make(map[string]*appServerServerRequestState),
		completedServerRequests: make(map[string][]byte),
	}
	message := appServerMessage{
		ID:     json.RawMessage(`90`),
		Method: "item/commandExecution/requestApproval",
		Params: json.RawMessage(`{"threadId":"thread","turnId":"turn"}`),
	}
	runner.dispatchServerRequest(context.Background(), message)
	runner.dispatchServerRequest(context.Background(), message)
	handler.waitForCalls(t, 1)
	close(handler.release)
	runner.serverWG.Wait()

	// A duplicate that arrives after completion is answered from the bounded
	// response cache instead of applying the approval policy a second time.
	runner.dispatchServerRequest(context.Background(), message)
	runner.serverWG.Wait()
	if calls := handler.callCount(); calls != 1 {
		t.Fatalf("approval handler calls = %d, want 1", calls)
	}
	if writes := transport.writeCount(); writes != 3 {
		t.Fatalf("approval response writes = %d, want 3", writes)
	}
}

func TestAppServerRunnerApprovalStateSoakIsBounded(t *testing.T) {
	transport := &recordingAppServerTransport{}
	handler := &countingApprovalHandler{}
	runner := &AppServerRunner{
		Transport:               transport,
		ServerRequestHandler:    handler,
		serverRequests:          make(map[string]*appServerServerRequestState),
		completedServerRequests: make(map[string][]byte),
	}
	for id := 1; id <= 1000; id++ {
		runner.dispatchServerRequest(context.Background(), appServerMessage{
			ID:     json.RawMessage(fmt.Sprintf("%d", id)),
			Method: "item/commandExecution/requestApproval",
			Params: json.RawMessage(`{"threadId":"thread","turnId":"turn"}`),
		})
		runner.serverWG.Wait()
	}
	runner.protocolMu.Lock()
	active := len(runner.serverRequests)
	completed := len(runner.completedServerRequests)
	order := len(runner.completedServerOrder)
	runner.protocolMu.Unlock()
	if active != 0 {
		t.Fatalf("active server requests after soak = %d, want 0", active)
	}
	if completed != appServerCompletedRequestLimit || order != appServerCompletedRequestLimit {
		t.Fatalf("completed cache after soak = (%d,%d), want (%d,%d)", completed, order, appServerCompletedRequestLimit, appServerCompletedRequestLimit)
	}
}

type cancelAwareApprovalHandler struct {
	started chan struct{}
	once    sync.Once
	delay   time.Duration
}

type countingApprovalHandler struct {
	mu      sync.Mutex
	calls   int
	release chan struct{}
}

func (h *countingApprovalHandler) HandleServerRequest(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	if h.release != nil {
		select {
		case <-ctx.Done():
			return nil, true, ctx.Err()
		case <-h.release:
		}
	}
	return automaticApprovalResult(method, params)
}

func (h *countingApprovalHandler) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func (h *countingApprovalHandler) waitForCalls(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for h.callCount() != want {
		if time.Now().After(deadline) {
			t.Fatalf("approval handler calls = %d, want %d", h.callCount(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

type recordingAppServerTransport struct {
	mu     sync.Mutex
	writes int
}

func (t *recordingAppServerTransport) WriteLine(context.Context, []byte) error {
	t.mu.Lock()
	t.writes++
	t.mu.Unlock()
	return nil
}

func (t *recordingAppServerTransport) ReadLine(context.Context) ([]byte, error) {
	return nil, io.EOF
}

func (t *recordingAppServerTransport) Close() error { return nil }

func (t *recordingAppServerTransport) writeCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.writes
}

func (h *cancelAwareApprovalHandler) HandleServerRequest(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
	h.once.Do(func() { close(h.started) })
	return (AutomaticApprovalHandler{Delay: h.delay}).HandleServerRequest(ctx, method, params)
}

type parallelApprovalHandler struct {
	want    int
	mu      sync.Mutex
	active  int
	max     int
	arrived int
	release chan struct{}
}

func newParallelApprovalHandler(want int) *parallelApprovalHandler {
	return &parallelApprovalHandler{want: want, release: make(chan struct{})}
}

func (h *parallelApprovalHandler) HandleServerRequest(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
	result, handled, err := automaticApprovalResult(method, params)
	if err != nil || !handled {
		return result, handled, err
	}
	h.mu.Lock()
	h.active++
	if h.active > h.max {
		h.max = h.active
	}
	h.arrived++
	if h.arrived == h.want {
		close(h.release)
	}
	h.mu.Unlock()
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-h.release:
	}
	h.mu.Lock()
	h.active--
	h.mu.Unlock()
	return result, true, err
}

func (h *parallelApprovalHandler) maxConcurrent() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.max
}

type multiplexTurnRequest struct {
	id       int64
	threadID string
}

type multiplexAppServerTransport struct {
	mu                sync.Mutex
	reads             chan []byte
	closed            chan struct{}
	closeOnce         sync.Once
	turns             []multiplexTurnRequest
	approvalResponses int
}

func newMultiplexAppServerTransport() *multiplexAppServerTransport {
	return &multiplexAppServerTransport{
		reads:  make(chan []byte, 32),
		closed: make(chan struct{}),
	}
}

func (t *multiplexAppServerTransport) WriteLine(ctx context.Context, line []byte) error {
	var message appServerMessage
	if err := json.Unmarshal(line, &message); err != nil {
		return err
	}
	if message.Method == "" {
		id, ok := appServerNumericID(message.ID)
		if ok && (id == 90 || id == 91) {
			t.mu.Lock()
			t.approvalResponses++
			complete := t.approvalResponses == 2
			turns := append([]multiplexTurnRequest(nil), t.turns...)
			t.mu.Unlock()
			if complete {
				for _, turn := range turns {
					t.send(ctx, fmt.Sprintf(`{"method":"future/unknown","params":{"threadId":%q,"turnId":%q,"newField":true}}`, turn.threadID, "turn-"+turn.threadID))
					t.send(ctx, fmt.Sprintf(`{"method":"item/completed","params":{"threadId":%q,"turnId":%q,"item":{"id":"item","type":"agentMessage","text":%q}}}`, turn.threadID, "turn-"+turn.threadID, "done "+turn.threadID))
					t.send(ctx, fmt.Sprintf(`{"method":"turn/completed","params":{"threadId":%q,"turn":{"id":%q,"status":"completed","items":[]}}}`, turn.threadID, "turn-"+turn.threadID))
				}
			}
		}
		return nil
	}
	id, ok := appServerNumericID(message.ID)
	if !ok && message.Method != appServerMethodInitialized {
		return fmt.Errorf("request %s did not include numeric id", message.Method)
	}
	switch message.Method {
	case appServerMethodInitialized:
		return nil
	case appServerMethodInitialize:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{}}`, id))
	case appServerMethodThreadList:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"data":[]}}`, id))
	case appServerMethodTurnStart:
		var params struct {
			ThreadID string `json:"threadId"`
		}
		if err := json.Unmarshal(message.Params, &params); err != nil {
			return err
		}
		t.mu.Lock()
		t.turns = append(t.turns, multiplexTurnRequest{id: id, threadID: params.ThreadID})
		ready := len(t.turns) == 2
		turns := append([]multiplexTurnRequest(nil), t.turns...)
		t.mu.Unlock()
		if ready {
			for index := len(turns) - 1; index >= 0; index-- {
				turn := turns[index]
				t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"turn":{"id":%q,"status":"inProgress","items":[]}}}`, turn.id, "turn-"+turn.threadID))
			}
			t.send(ctx, `{"jsonrpc":"2.0","id":90,"method":"item/commandExecution/requestApproval","params":{"threadId":"thread-a","turnId":"turn-thread-a"}}`)
			t.send(ctx, `{"jsonrpc":"2.0","id":91,"method":"item/commandExecution/requestApproval","params":{"threadId":"thread-b","turnId":"turn-thread-b"}}`)
		}
	default:
		return fmt.Errorf("unexpected method %q", message.Method)
	}
	return nil
}

func (t *multiplexAppServerTransport) send(ctx context.Context, line string) {
	select {
	case t.reads <- []byte(line):
	case <-ctx.Done():
	case <-t.closed:
	}
}

func (t *multiplexAppServerTransport) ReadLine(ctx context.Context) ([]byte, error) {
	select {
	case line := <-t.reads:
		return line, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *multiplexAppServerTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

type cancelApprovalTransport struct {
	reads            chan []byte
	closed           chan struct{}
	closeOnce        sync.Once
	approvalSent     chan struct{}
	approvalOnce     sync.Once
	approvalResponse chan []byte
}

func newCancelApprovalTransport() *cancelApprovalTransport {
	return &cancelApprovalTransport{
		reads:            make(chan []byte, 8),
		closed:           make(chan struct{}),
		approvalSent:     make(chan struct{}),
		approvalResponse: make(chan []byte, 1),
	}
}

func (t *cancelApprovalTransport) WriteLine(ctx context.Context, line []byte) error {
	var message appServerMessage
	if err := json.Unmarshal(line, &message); err != nil {
		return err
	}
	if message.Method == "" {
		if id, ok := appServerNumericID(message.ID); ok && id == 90 {
			t.approvalResponse <- append([]byte(nil), line...)
		}
		return nil
	}
	id, _ := appServerNumericID(message.ID)
	switch message.Method {
	case appServerMethodInitialized:
		return nil
	case appServerMethodInitialize:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{}}`, id))
	case appServerMethodThreadList:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"data":[]}}`, id))
	case appServerMethodTurnStart:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"turn":{"id":"turn-cancel","status":"inProgress","items":[]}}}`, id))
		t.send(ctx, `{"jsonrpc":"2.0","id":90,"method":"item/commandExecution/requestApproval","params":{"threadId":"thread-cancel","turnId":"turn-cancel"}}`)
		t.approvalOnce.Do(func() { close(t.approvalSent) })
	}
	return nil
}

func (t *cancelApprovalTransport) send(ctx context.Context, line string) {
	select {
	case t.reads <- []byte(line):
	case <-ctx.Done():
	case <-t.closed:
	}
}

func (t *cancelApprovalTransport) ReadLine(ctx context.Context) ([]byte, error) {
	select {
	case line := <-t.reads:
		return line, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *cancelApprovalTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

type sameThreadTurnTransport struct {
	mu        sync.Mutex
	reads     chan []byte
	closed    chan struct{}
	closeOnce sync.Once
	sequence  int
	started   chan int
}

type explicitCancelTurnTransport struct {
	mu                            sync.Mutex
	reads                         chan []byte
	closed                        chan struct{}
	closeOnce                     sync.Once
	sequence                      int
	started                       chan int
	interrupted                   chan TurnRef
	interruptID                   int64
	interruptReady                chan struct{}
	delayFirstStartResponse       bool
	firstStartID                  int64
	exactInterruptError           string
	exactInterruptErrorsRemaining int
	completeBeforeExactInterrupt  bool
	threadStatus                  string
}

func newExplicitCancelTurnTransport() *explicitCancelTurnTransport {
	return &explicitCancelTurnTransport{
		reads:          make(chan []byte, 32),
		closed:         make(chan struct{}),
		started:        make(chan int, 2),
		interrupted:    make(chan TurnRef, 8),
		interruptReady: make(chan struct{}),
		threadStatus:   "active",
	}
}

func (t *explicitCancelTurnTransport) WriteLine(ctx context.Context, line []byte) error {
	var message appServerMessage
	if err := json.Unmarshal(line, &message); err != nil {
		return err
	}
	if message.Method == appServerMethodInitialized {
		return nil
	}
	id, ok := appServerNumericID(message.ID)
	if !ok {
		return fmt.Errorf("request %s did not include numeric id", message.Method)
	}
	switch message.Method {
	case appServerMethodInitialize:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{}}`, id))
	case appServerMethodThreadList:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"data":[]}}`, id))
	case appServerMethodTurnStart:
		t.mu.Lock()
		t.sequence++
		sequence := t.sequence
		t.mu.Unlock()
		if !t.delayFirstStartResponse || sequence != 1 {
			t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"turn":{"id":"turn-%d","status":"inProgress","items":[]}}}`, id, sequence))
			t.send(ctx, fmt.Sprintf(`{"method":"turn/started","params":{"threadId":"shared-thread","turn":{"id":"turn-%d","status":"inProgress","items":[]}}}`, sequence))
		} else {
			t.mu.Lock()
			t.firstStartID = id
			t.mu.Unlock()
		}
		select {
		case t.started <- sequence:
		case <-ctx.Done():
			return ctx.Err()
		case <-t.closed:
			return io.EOF
		}
	case appServerMethodTurnInterrupt:
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
		}
		if err := json.Unmarshal(message.Params, &params); err != nil {
			return err
		}
		select {
		case t.interrupted <- TurnRef{ThreadID: params.ThreadID, TurnID: params.TurnID}:
		case <-ctx.Done():
			return ctx.Err()
		case <-t.closed:
			return io.EOF
		}
		if params.TurnID == "" {
			return fmt.Errorf("turn/interrupt did not include an exact turn id")
		}
		t.mu.Lock()
		interruptError := t.exactInterruptError
		if t.exactInterruptErrorsRemaining > 0 {
			t.exactInterruptErrorsRemaining--
		} else if t.exactInterruptErrorsRemaining == 0 {
			interruptError = ""
		}
		t.mu.Unlock()
		if interruptError != "" {
			t.send(ctx, fmt.Sprintf(`{"id":%d,"error":{"code":-32600,"message":%q}}`, id, interruptError))
			return nil
		}
		if t.completeBeforeExactInterrupt {
			t.mu.Lock()
			t.threadStatus = "idle"
			t.mu.Unlock()
			t.send(ctx, `{"method":"turn/completed","params":{"threadId":"shared-thread","turn":{"id":"turn-1","status":"completed","items":[]}}}`)
			t.send(ctx, fmt.Sprintf(`{"id":%d,"error":{"code":-32600,"message":"no active turn to interrupt"}}`, id))
			return nil
		}
		t.mu.Lock()
		t.interruptID = id
		t.mu.Unlock()
		close(t.interruptReady)
	case appServerMethodThreadRead:
		var params struct {
			ThreadID string `json:"threadId"`
		}
		if err := json.Unmarshal(message.Params, &params); err != nil {
			return err
		}
		t.mu.Lock()
		status := t.threadStatus
		t.mu.Unlock()
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"thread":{"id":%q,"status":%q}}}`, id, params.ThreadID, status))
	default:
		return fmt.Errorf("unexpected method %q", message.Method)
	}
	return nil
}

func (t *explicitCancelTurnTransport) finishInterrupt(ctx context.Context) {
	t.finishInterruptedTurn(ctx)
	t.ackInterrupt(ctx)
}

func (t *explicitCancelTurnTransport) ackInterrupt(ctx context.Context) {
	select {
	case <-t.interruptReady:
	case <-ctx.Done():
		return
	}
	t.mu.Lock()
	id := t.interruptID
	t.mu.Unlock()
	t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{}}`, id))
}

func (t *explicitCancelTurnTransport) finishInterruptedTurn(ctx context.Context) {
	t.mu.Lock()
	t.threadStatus = "idle"
	t.mu.Unlock()
	t.send(ctx, `{"method":"turn/completed","params":{"threadId":"shared-thread","turn":{"id":"turn-1","status":"interrupted","items":[]}}}`)
}

func (t *explicitCancelTurnTransport) finishStartResponse(ctx context.Context) {
	t.mu.Lock()
	id := t.firstStartID
	t.mu.Unlock()
	t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`, id))
}

func (t *explicitCancelTurnTransport) finishStartError(ctx context.Context, message string) {
	t.mu.Lock()
	id := t.firstStartID
	t.mu.Unlock()
	t.send(ctx, fmt.Sprintf(`{"id":%d,"error":{"code":-32600,"message":%q}}`, id, message))
}

func (t *explicitCancelTurnTransport) complete(ctx context.Context, sequence int) {
	t.completeFor(ctx, "shared-thread", sequence)
}

func (t *explicitCancelTurnTransport) completeFor(ctx context.Context, threadID string, sequence int) {
	t.send(ctx, fmt.Sprintf(`{"method":"item/completed","params":{"threadId":%q,"turnId":"turn-%d","item":{"id":"item-%d","type":"agentMessage","text":"done-%d"}}}`, threadID, sequence, sequence, sequence))
	t.send(ctx, fmt.Sprintf(`{"method":"turn/completed","params":{"threadId":%q,"turn":{"id":"turn-%d","status":"completed","items":[]}}}`, threadID, sequence))
}

func (t *explicitCancelTurnTransport) send(ctx context.Context, line string) {
	select {
	case t.reads <- []byte(line):
	case <-ctx.Done():
	case <-t.closed:
	}
}

func (t *explicitCancelTurnTransport) ReadLine(ctx context.Context) ([]byte, error) {
	select {
	case line := <-t.reads:
		return line, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *explicitCancelTurnTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func newSameThreadTurnTransport() *sameThreadTurnTransport {
	return &sameThreadTurnTransport{
		reads:   make(chan []byte, 16),
		closed:  make(chan struct{}),
		started: make(chan int, 2),
	}
}

func (t *sameThreadTurnTransport) WriteLine(ctx context.Context, line []byte) error {
	var message appServerMessage
	if err := json.Unmarshal(line, &message); err != nil {
		return err
	}
	if message.Method == appServerMethodInitialized {
		return nil
	}
	id, ok := appServerNumericID(message.ID)
	if !ok {
		return fmt.Errorf("request %s did not include numeric id", message.Method)
	}
	switch message.Method {
	case appServerMethodInitialize:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{}}`, id))
	case appServerMethodThreadList:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"data":[]}}`, id))
	case appServerMethodTurnStart:
		t.mu.Lock()
		t.sequence++
		sequence := t.sequence
		t.mu.Unlock()
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"turn":{"id":"turn-%d","status":"inProgress","items":[]}}}`, id, sequence))
		select {
		case t.started <- sequence:
		case <-ctx.Done():
			return ctx.Err()
		case <-t.closed:
			return io.EOF
		}
	default:
		return fmt.Errorf("unexpected method %q", message.Method)
	}
	return nil
}

func (t *sameThreadTurnTransport) complete(ctx context.Context, sequence int) {
	t.send(ctx, fmt.Sprintf(`{"method":"item/completed","params":{"threadId":"shared-thread","turnId":"turn-%d","item":{"id":"item-%d","type":"agentMessage","text":"done-%d"}}}`, sequence, sequence, sequence))
	t.send(ctx, fmt.Sprintf(`{"method":"turn/completed","params":{"threadId":"shared-thread","turn":{"id":"turn-%d","status":"completed","items":[]}}}`, sequence))
}

func (t *sameThreadTurnTransport) send(ctx context.Context, line string) {
	select {
	case t.reads <- []byte(line):
	case <-ctx.Done():
	case <-t.closed:
	}
}

func (t *sameThreadTurnTransport) ReadLine(ctx context.Context) ([]byte, error) {
	select {
	case line := <-t.reads:
		return line, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *sameThreadTurnTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}
