package codexrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestCapacityRetryPolicyDefaultsToTenRetries(t *testing.T) {
	policy := (capacityRetryPolicy{}).normalized()
	if policy.MaxRetries != 10 {
		t.Fatalf("default capacity retry limit = %d, want 10", policy.MaxRetries)
	}
	if policy.MaxTotalRetries != defaultCapacityRetryTotalLimit {
		t.Fatalf("default total capacity retry limit = %d, want %d", policy.MaxTotalRetries, defaultCapacityRetryTotalLimit)
	}
	if policy.MaxElapsed != defaultCapacityRetryMaxElapsed {
		t.Fatalf("default capacity retry elapsed limit = %s, want %s", policy.MaxElapsed, defaultCapacityRetryMaxElapsed)
	}
	if got := (capacityRetryPolicy{MaxRetries: -1}).normalized().MaxRetries; got >= 0 {
		t.Fatalf("negative capacity retry limit = %d, want disabled negative value", got)
	}
}

func TestIsModelCapacityFailureMessageIsNarrow(t *testing.T) {
	for _, message := range []string{
		"Selected model is at capacity. Please try a different model.",
		"server overloaded; retry later",
		"-32001: Server overloaded; retry later",
	} {
		if !IsModelCapacityFailureMessage(message) {
			t.Fatalf("IsModelCapacityFailureMessage(%q) = false", message)
		}
	}
	for _, message := range []string{
		"tool failed: capacity is too small",
		"authentication failed",
		"model returned invalid JSON",
	} {
		if IsModelCapacityFailureMessage(message) {
			t.Fatalf("IsModelCapacityFailureMessage(%q) = true", message)
		}
	}
	if !isModelCapacityFailureCode("serverOverloaded") {
		t.Fatal("serverOverloaded code was not recognized")
	}
	if isModelCapacityFailureCode("-32001") {
		t.Fatal("numeric -32001 code was recognized without its overload message")
	}
	if IsModelCapacityFailure(TurnResult{}, fmt.Errorf("Selected model is at capacity. Please try a different model.")) {
		t.Fatal("unstructured wrapped error was recognized as capacity failure")
	}
}

func TestIsModelCapacityFailureRecognizesStructuredServerOverloadedCode(t *testing.T) {
	err := appServerResponseError(&appServerErrorField{
		Code:    json.RawMessage(`-32001`),
		Message: "temporarily unavailable",
		Data:    json.RawMessage(`{"codexErrorInfo":{"serverOverloaded":{}}}`),
	})
	if !IsModelCapacityFailure(TurnResult{}, err) {
		t.Fatalf("structured serverOverloaded error was not recognized: %v", err)
	}
}

func TestAppServerRunnerRetriesCapacityWithoutDuplicatingPrompt(t *testing.T) {
	reads := appServerCapacityRetryHandshake("thread-new")
	appendCapacityFailedTurn(&reads, 4, "thread-new", "turn-1", false)
	appendSuccessfulTurn(&reads, 5, "thread-new", "turn-2", "recovered")
	transport := newFakeAppServerTransport(reads...)
	var delays int
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 2,
			Wait: func(context.Context, time.Duration) error {
				delays++
				return nil
			},
		},
	}
	var events []StreamEvent
	got, err := runner.StartThread(context.Background(), TurnInput{
		Prompt: "do the work",
		EventHandler: func(event StreamEvent) {
			events = append(events, event)
		},
	})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	if got.Status != TurnStatusCompleted || got.TurnID != "turn-2" || got.FinalAgentMessage != "recovered" {
		t.Fatalf("result = %#v", got)
	}
	if delays != 1 {
		t.Fatalf("wait calls = %d, want 1", delays)
	}

	var retries, failures int
	for _, event := range events {
		switch event.Kind {
		case StreamEventStreamRetry:
			retries++
			if !event.WillRetry || event.Failure == nil {
				t.Fatalf("capacity retry event = %#v", event)
			}
		case StreamEventTurnFailed:
			failures++
		}
	}
	if retries != 1 || failures != 0 {
		t.Fatalf("capacity events = retries %d failures %d: %#v", retries, failures, events)
	}

	writes := transport.decodedWrites(t)
	var turnStarts []map[string]any
	for _, write := range writes {
		if write["method"] == appServerMethodTurnStart {
			turnStarts = append(turnStarts, write)
		}
	}
	if len(turnStarts) != 2 {
		t.Fatalf("turn/start writes = %d, want 2: %#v", len(turnStarts), writes)
	}
	assertTextInput(t, turnStarts[0], "do the work")
	params := turnStarts[1]["params"].(map[string]any)
	input, ok := params["input"].([]any)
	if !ok || len(input) != 1 {
		t.Fatalf("retry input = %#v, want one continuation input", params["input"])
	}
	assertTextInput(t, turnStarts[1], capacityRetryContinuationPrompt)
}

func TestAppServerRunnerRetriesCapacityBeforeTurnIsAcceptedWithOriginalPrompt(t *testing.T) {
	reads := appServerCapacityRetryHandshake("thread-new")
	reads = append(reads,
		`{"id":4,"error":{"code":"model_capacity","message":"Selected model is at capacity. Please try a different model."}}`,
	)
	appendSuccessfulTurn(&reads, 5, "thread-new", "turn-1", "recovered")
	transport := newFakeAppServerTransport(reads...)
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 1,
			Wait:       func(context.Context, time.Duration) error { return nil },
		},
	}
	if _, err := runner.StartThread(context.Background(), TurnInput{Prompt: "do the work"}); err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	writes := transport.decodedWrites(t)
	var turnStarts []map[string]any
	for _, write := range writes {
		if write["method"] == appServerMethodTurnStart {
			turnStarts = append(turnStarts, write)
		}
	}
	if len(turnStarts) != 2 {
		t.Fatalf("turn/start writes = %d, want 2: %#v", len(turnStarts), writes)
	}
	assertTextInput(t, turnStarts[0], "do the work")
	assertTextInput(t, turnStarts[1], "do the work")
}

func TestAppServerRunnerRetriesStructuredCapacityCodeWithoutKnownMessage(t *testing.T) {
	reads := appServerCapacityRetryHandshake("thread-new")
	reads = append(reads,
		`{"id":4,"error":{"code":"model_capacity","message":"temporarily unavailable"}}`,
	)
	appendSuccessfulTurn(&reads, 5, "thread-new", "turn-1", "recovered")
	transport := newFakeAppServerTransport(reads...)
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 1,
			Wait:       func(context.Context, time.Duration) error { return nil },
		},
	}
	if _, err := runner.StartThread(context.Background(), TurnInput{Prompt: "do the work"}); err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	if got := countMethodWrites(t, transport, appServerMethodTurnStart); got != 2 {
		t.Fatalf("turn/start writes = %d, want 2", got)
	}
}

func TestAppServerRunnerRetriesCodeOnlyTerminalCapacityFailure(t *testing.T) {
	reads := appServerCapacityRetryHandshake("thread-new")
	reads = append(reads,
		`{"id":4,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"method":"turn/started","params":{"threadId":"thread-new","turn":{"id":"turn-1"}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-new","turn":{"id":"turn-1","status":"failed","error":{"code":"serverOverloaded","message":"temporarily unavailable"},"items":[]}}}`,
	)
	appendSuccessfulTurn(&reads, 5, "thread-new", "turn-2", "recovered")
	transport := newFakeAppServerTransport(reads...)
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 1,
			Wait:       func(context.Context, time.Duration) error { return nil },
		},
	}
	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "do the work"})
	if err != nil || got.Status != TurnStatusCompleted || got.TurnID != "turn-2" {
		t.Fatalf("code-only terminal retry result/error = %#v / %v", got, err)
	}
	if got := countMethodWrites(t, transport, appServerMethodTurnStart); got != 2 {
		t.Fatalf("turn/start writes = %d, want 2", got)
	}
}

func TestAppServerRunnerDefaultCapacityRetryLimitIsTen(t *testing.T) {
	reads := appServerCapacityRetryHandshake("thread-new")
	const expectedRetries = 10
	for i := 0; i < expectedRetries; i++ {
		appendCapacityFailedTurn(&reads, 4+i, "thread-new", fmt.Sprintf("turn-%d", i+1), false)
	}
	appendSuccessfulTurn(&reads, 4+expectedRetries, "thread-new", "turn-final", "recovered")
	transport := newFakeAppServerTransport(reads...)
	waits := 0
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			Wait: func(context.Context, time.Duration) error {
				waits++
				return nil
			},
		},
	}
	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "do the work"})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	if got.Status != TurnStatusCompleted || got.TurnID != "turn-final" {
		t.Fatalf("result = %#v", got)
	}
	if waits != expectedRetries {
		t.Fatalf("wait calls = %d, want %d", waits, expectedRetries)
	}
	if got := countMethodWrites(t, transport, appServerMethodTurnStart); got != expectedRetries+1 {
		t.Fatalf("turn/start writes = %d, want %d", got, expectedRetries+1)
	}
}

func TestAppServerRunnerStartTurnRetriesAcceptedCapacityFailure(t *testing.T) {
	reads := appServerCapacityRetryTurnHandshake()
	appendCapacityFailedTurn(&reads, 3, "thread-existing", "turn-1", false)
	appendSuccessfulTurn(&reads, 4, "thread-existing", "turn-2", "recovered")
	transport := newFakeAppServerTransport(reads...)
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 1,
			Wait:       func(context.Context, time.Duration) error { return nil },
		},
	}
	got, err := runner.StartTurn(context.Background(), StartTurnInput{
		ThreadID:  "thread-existing",
		TurnInput: TurnInput{Prompt: "do the work"},
	})
	if err != nil {
		t.Fatalf("StartTurn error: %v", err)
	}
	if got.Status != TurnStatusCompleted || got.TurnID != "turn-2" {
		t.Fatalf("result = %#v", got)
	}
	writes := transport.decodedWrites(t)
	var turnStarts []map[string]any
	for _, write := range writes {
		if write["method"] == appServerMethodTurnStart {
			turnStarts = append(turnStarts, write)
		}
	}
	if len(turnStarts) != 2 {
		t.Fatalf("turn/start writes = %d, want 2", len(turnStarts))
	}
	assertTextInput(t, turnStarts[0], "do the work")
	assertTextInput(t, turnStarts[1], capacityRetryContinuationPrompt)
}

func TestAppServerRunnerCapacityRetryCancellationDuringWait(t *testing.T) {
	reads := appServerCapacityRetryTurnHandshake()
	appendCapacityFailedTurn(&reads, 3, "thread-existing", "turn-1", false)
	transport := newFakeAppServerTransport(reads...)
	waitStarted := make(chan struct{})
	var retries int
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 1,
			Wait: func(ctx context.Context, _ time.Duration) error {
				close(waitStarted)
				<-ctx.Done()
				return ctx.Err()
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID: "thread-existing",
			TurnInput: TurnInput{
				Prompt: "do the work",
				EventHandler: func(event StreamEvent) {
					if event.Kind == StreamEventStreamRetry {
						retries++
					}
				},
			},
		})
		done <- err
	}()
	select {
	case <-waitStarted:
	case <-time.After(time.Second):
		t.Fatal("capacity retry did not enter wait")
	}
	cancel()
	select {
	case err := <-done:
		if !IsKind(err, ErrorCanceled) {
			t.Fatalf("StartTurn error = %v, want canceled error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StartTurn did not return after cancellation")
	}
	if got := countMethodWrites(t, transport, appServerMethodTurnStart); got != 1 {
		t.Fatalf("turn/start writes = %d, want 1 after cancellation", got)
	}
	if retries != 0 {
		t.Fatalf("retry events after cancellation = %d, want 0", retries)
	}
}

func TestAppServerRunnerCapacityRetryCancellationUsesTimerPath(t *testing.T) {
	reads := appServerCapacityRetryTurnHandshake()
	appendCapacityFailedTurn(&reads, 3, "thread-existing", "turn-1", false)
	transport := newFakeAppServerTransport(reads...)
	waitStarted := make(chan struct{})
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries:   1,
			InitialDelay: time.Second,
			MaxDelay:     time.Second,
			Jitter: func(delay time.Duration) time.Duration {
				close(waitStarted)
				return delay
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID:  "thread-existing",
			TurnInput: TurnInput{Prompt: "do the work"},
		})
		done <- err
	}()
	select {
	case <-waitStarted:
	case <-time.After(time.Second):
		t.Fatal("capacity retry did not enter timer wait")
	}
	cancel()
	select {
	case err := <-done:
		if !IsKind(err, ErrorCanceled) {
			t.Fatalf("StartTurn error = %v, want canceled error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timer-backed retry did not return after cancellation")
	}
	if got := countMethodWrites(t, transport, appServerMethodTurnStart); got != 1 {
		t.Fatalf("turn/start writes = %d, want 1 after timer cancellation", got)
	}
}

func TestCapacityRetryDeadlineIsTimeoutNotExplicitCancellation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errCapacityRetryDeadline)
	if explicitTurnInterruptRequested(ctx) {
		t.Fatal("capacity retry deadline was classified as an explicit user interrupt")
	}
	if !capacityRetryDeadlineRequested(ctx) || !turnStopRequested(ctx) {
		t.Fatal("capacity retry deadline was not treated as an internal turn stop")
	}
	if !IsKind(turnStopError(ctx), ErrorTimeout) {
		t.Fatalf("capacity retry deadline error = %v, want timeout", turnStopError(ctx))
	}
}

func TestAppServerRunnerCapacityRetryElapsedLimitStopsBackoff(t *testing.T) {
	reads := appServerCapacityRetryTurnHandshake()
	appendCapacityFailedTurn(&reads, 3, "thread-existing", "turn-1", false)
	transport := newFakeAppServerTransport(reads...)
	var finalFailure StreamEvent
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries:   1,
			MaxElapsed:   10 * time.Millisecond,
			InitialDelay: time.Second,
			MaxDelay:     time.Second,
			Jitter:       func(delay time.Duration) time.Duration { return delay },
		},
	}
	got, err := runner.StartTurn(context.Background(), StartTurnInput{
		ThreadID: "thread-existing",
		TurnInput: TurnInput{
			Prompt: "do the work",
			EventHandler: func(event StreamEvent) {
				if event.Kind == StreamEventTurnFailed {
					finalFailure = event
				}
			},
		},
	})
	if err == nil || !IsKind(err, ErrorCodex) {
		t.Fatalf("StartTurn error = %v, want terminal capacity error", err)
	}
	if got.Status != TurnStatusFailed || got.TurnID != "turn-1" || got.Failure == nil || !IsModelCapacityFailure(got, nil) {
		t.Fatalf("result = %#v, want terminal capacity failure", got)
	}
	if finalFailure.Kind != StreamEventTurnFailed || finalFailure.TurnID != "turn-1" || finalFailure.Failure == nil || finalFailure.Failure.Code != "model_capacity" {
		t.Fatalf("final failure event = %#v", finalFailure)
	}
	if got := countMethodWrites(t, transport, appServerMethodTurnStart); got != 1 {
		t.Fatalf("turn/start writes = %d, want no retry after elapsed limit", got)
	}
}

func TestAppServerRunnerSameThreadRetryWaitHonorsQueuedCallerContext(t *testing.T) {
	reads := appServerCapacityRetryTurnHandshake()
	appendCapacityFailedTurn(&reads, 3, "thread-existing", "turn-1", false)
	appendSuccessfulTurn(&reads, 4, "thread-existing", "turn-2", "first recovered")
	transport := newFakeAppServerTransport(reads...)
	waitStarted := make(chan struct{})
	releaseWait := make(chan struct{})
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 1,
			Wait: func(ctx context.Context, _ time.Duration) error {
				close(waitStarted)
				select {
				case <-releaseWait:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		},
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(context.Background(), StartTurnInput{
			ThreadID:  "thread-existing",
			TurnInput: TurnInput{Prompt: "first"},
		})
		firstDone <- err
	}()
	select {
	case <-waitStarted:
	case <-time.After(time.Second):
		t.Fatal("first capacity retry did not enter wait")
	}
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelSecond()
	secondDone := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(secondCtx, StartTurnInput{
			ThreadID:  "thread-existing",
			TurnInput: TurnInput{Prompt: "second"},
		})
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if !IsKind(err, ErrorTimeout) {
			t.Fatalf("queued same-thread turn error = %v, want timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued same-thread turn did not honor its context")
	}
	if got := countMethodWrites(t, transport, appServerMethodTurnStart); got != 1 {
		t.Fatalf("turn/start writes while first retry waits = %d, want 1", got)
	}
	close(releaseWait)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first StartTurn error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first StartTurn did not complete")
	}
	if got := countMethodWrites(t, transport, appServerMethodTurnStart); got != 2 {
		t.Fatalf("turn/start writes after first retry = %d, want 2", got)
	}
}

func TestAppServerRunnerTotalCapacityRetryLimitBoundsResets(t *testing.T) {
	reads := appServerCapacityRetryHandshake("thread-new")
	appendCapacityFailedTurn(&reads, 4, "thread-new", "turn-1", false)
	appendCapacityFailedTurn(&reads, 5, "thread-new", "turn-2", true)
	appendCapacityFailedTurn(&reads, 6, "thread-new", "turn-3", false)
	transport := newFakeAppServerTransport(reads...)
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries:      1,
			MaxTotalRetries: 2,
			Wait:            func(context.Context, time.Duration) error { return nil },
		},
	}
	_, err := runner.StartThread(context.Background(), TurnInput{Prompt: "try"})
	if err == nil || !IsKind(err, ErrorCodex) {
		t.Fatalf("StartThread error = %v, want terminal Codex error", err)
	}
	if got := countMethodWrites(t, transport, appServerMethodTurnStart); got != 3 {
		t.Fatalf("turn/start writes = %d, want initial plus two total retries", got)
	}
}

func TestAppServerRunnerEmitsFinalFailureForPreAcceptanceCapacityError(t *testing.T) {
	reads := appServerCapacityRetryHandshake("thread-new")
	reads = append(reads,
		`{"id":4,"error":{"code":"model_capacity","message":"Selected model is at capacity. Please try a different model."}}`,
		`{"id":5,"error":{"code":"model_capacity","message":"Selected model is at capacity. Please try a different model."}}`,
	)
	transport := newFakeAppServerTransport(reads...)
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 1,
			Wait:       func(context.Context, time.Duration) error { return nil },
		},
	}
	var failures int
	var finalFailure StreamEvent
	got, err := runner.StartThread(context.Background(), TurnInput{
		Prompt: "try",
		EventHandler: func(event StreamEvent) {
			if event.Kind == StreamEventTurnFailed {
				failures++
				finalFailure = event
			}
		},
	})
	if err == nil || !IsKind(err, ErrorCodex) {
		t.Fatalf("StartThread error = %v, want terminal Codex error", err)
	}
	if failures != 1 || finalFailure.Kind != StreamEventTurnFailed || finalFailure.ThreadID != "thread-new" || finalFailure.TurnID != "" || finalFailure.Failure == nil || finalFailure.Failure.Code != "model_capacity" || finalFailure.Failure.Message != "model_capacity: Selected model is at capacity. Please try a different model." {
		t.Fatalf("final capacity failure event = %#v, failures = %d", finalFailure, failures)
	}
	if got.Status != TurnStatusUnknown || got.TurnID != "" || got.Failure != nil {
		t.Fatalf("final pre-acceptance result = %#v", got)
	}
	if got := countMethodWrites(t, transport, appServerMethodTurnStart); got != 2 {
		t.Fatalf("turn/start writes = %d, want initial plus one retry", got)
	}
}

func TestAppServerRunnerDoesNotRetryNonCapacityFailure(t *testing.T) {
	reads := appServerCapacityRetryTurnHandshake()
	reads = append(reads,
		`{"id":3,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"method":"turn/started","params":{"threadId":"thread-existing","turn":{"id":"turn-1"}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-existing","turn":{"id":"turn-1","status":"failed","error":{"code":"tool_error","message":"tool failed"},"items":[]}}}`,
	)
	transport := newFakeAppServerTransport(reads...)
	var retries int
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 1,
			Wait:       func(context.Context, time.Duration) error { return nil },
		},
	}
	got, err := runner.StartTurn(context.Background(), StartTurnInput{
		ThreadID: "thread-existing",
		TurnInput: TurnInput{
			Prompt: "try",
			EventHandler: func(event StreamEvent) {
				if event.Kind == StreamEventStreamRetry {
					retries++
				}
			},
		},
	})
	if err == nil || !IsKind(err, ErrorCodex) || got.Status != TurnStatusFailed || got.Failure == nil || got.Failure.Code != "tool_error" {
		t.Fatalf("non-capacity result/error = %#v / %v", got, err)
	}
	if retries != 0 || countMethodWrites(t, transport, appServerMethodTurnStart) != 1 {
		t.Fatalf("non-capacity retries = %d, turn/start writes = %d", retries, countMethodWrites(t, transport, appServerMethodTurnStart))
	}
}

func TestAppServerRunnerDoesNotRetryAcceptedCapacityFailureAfterCommand(t *testing.T) {
	reads := appServerCapacityRetryTurnHandshake()
	appendCapacityFailedTurnWithCommand(&reads, 3, "thread-existing", "turn-1")
	transport := newFakeAppServerTransport(reads...)
	var retries int
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 1,
			Wait:       func(context.Context, time.Duration) error { return nil },
		},
	}
	got, err := runner.StartTurn(context.Background(), StartTurnInput{
		ThreadID: "thread-existing",
		TurnInput: TurnInput{
			Prompt: "try",
			EventHandler: func(event StreamEvent) {
				if event.Kind == StreamEventStreamRetry {
					retries++
				}
			},
		},
	})
	if err == nil || !IsKind(err, ErrorCodex) || got.Status != TurnStatusFailed || got.Failure == nil || got.Failure.Code != "model_capacity" {
		t.Fatalf("side-effect capacity result/error = %#v / %v", got, err)
	}
	if retries != 0 || countMethodWrites(t, transport, appServerMethodTurnStart) != 1 {
		t.Fatalf("side-effect capacity retries = %d, turn/start writes = %d", retries, countMethodWrites(t, transport, appServerMethodTurnStart))
	}
}

func TestAppServerRunnerDoesNotRetryTerminalPayloadWithCommand(t *testing.T) {
	reads := appServerCapacityRetryTurnHandshake()
	reads = append(reads,
		`{"id":3,"result":{"turn":{"id":"turn-1","status":"failed","error":{"code":"model_capacity","message":"Selected model is at capacity. Please try a different model."},"items":[{"id":"command-1","type":"commandExecution","command":"touch workspace-file","status":"completed"}]}}}`,
	)
	transport := newFakeAppServerTransport(reads...)
	var retries, failures int
	var finalFailure StreamEvent
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 1,
			Wait:       func(context.Context, time.Duration) error { return nil },
		},
	}
	got, err := runner.StartTurn(context.Background(), StartTurnInput{
		ThreadID: "thread-existing",
		TurnInput: TurnInput{
			Prompt: "try",
			EventHandler: func(event StreamEvent) {
				switch event.Kind {
				case StreamEventStreamRetry:
					retries++
				case StreamEventTurnFailed:
					failures++
					finalFailure = event
				}
			},
		},
	})
	if err == nil || !IsKind(err, ErrorCodex) || got.Status != TurnStatusFailed || got.TurnID != "turn-1" || got.Failure == nil || got.Failure.Code != "model_capacity" {
		t.Fatalf("terminal payload result/error = %#v / %v", got, err)
	}
	if retries != 0 || failures != 1 || finalFailure.Failure == nil || finalFailure.TurnID != "turn-1" || countMethodWrites(t, transport, appServerMethodTurnStart) != 1 {
		t.Fatalf("terminal payload events = retries %d failures %d final=%#v turn/start writes = %d", retries, failures, finalFailure, countMethodWrites(t, transport, appServerMethodTurnStart))
	}
}

func TestAppServerRunnerDoesNotRetryTerminalPayloadWithSnakeCaseTool(t *testing.T) {
	reads := appServerCapacityRetryTurnHandshake()
	reads = append(reads,
		`{"id":3,"result":{"turn":{"id":"turn-1","status":"failed","error":{"code":"model_capacity","message":"Selected model is at capacity. Please try a different model."},"items":[{"id":"tool-1","type":"mcp_tool_call","status":"completed"}]}}}`,
	)
	transport := newFakeAppServerTransport(reads...)
	var retries, failures int
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 1,
			Wait:       func(context.Context, time.Duration) error { return nil },
		},
	}
	got, err := runner.StartTurn(context.Background(), StartTurnInput{
		ThreadID: "thread-existing",
		TurnInput: TurnInput{
			Prompt: "try",
			EventHandler: func(event StreamEvent) {
				switch event.Kind {
				case StreamEventStreamRetry:
					retries++
				case StreamEventTurnFailed:
					failures++
				}
			},
		},
	})
	if err == nil || got.Status != TurnStatusFailed || got.Failure == nil || !IsModelCapacityFailure(got, nil) {
		t.Fatalf("snake_case side-effect result/error = %#v / %v", got, err)
	}
	if retries != 0 || failures != 1 || countMethodWrites(t, transport, appServerMethodTurnStart) != 1 {
		t.Fatalf("snake_case side-effect events = retries %d failures %d, turn/start writes = %d", retries, failures, countMethodWrites(t, transport, appServerMethodTurnStart))
	}
}

func TestCanRetryModelCapacityFailureRejectsAmbiguousAndBackfillErrors(t *testing.T) {
	failure := &TurnFailure{Code: "model_capacity", Message: "Selected model is at capacity. Please try a different model."}
	if canRetryModelCapacityFailure(TurnResult{Status: TurnStatusCompleted, TurnID: "turn-1"}, &Error{Kind: ErrorCodex, Message: failure.Message}) {
		t.Fatal("completed turn/backfill error was considered retryable")
	}
	if canRetryModelCapacityFailure(TurnResult{Status: TurnStatusStarted, TurnID: "turn-1"}, &Error{Kind: ErrorCodex, Message: failure.Message}) {
		t.Fatal("non-terminal accepted turn was considered retryable")
	}
	if !canRetryModelCapacityFailure(TurnResult{Status: TurnStatusFailed, TurnID: "turn-1", Failure: failure}, nil) {
		t.Fatal("accepted terminal capacity failure was not retryable")
	}
}

func TestAppServerRunnerResetsCapacityRetryBudgetAfterSubstantiveUpdate(t *testing.T) {
	reads := appServerCapacityRetryHandshake("thread-new")
	appendCapacityFailedTurn(&reads, 4, "thread-new", "turn-1", false)
	appendCapacityFailedTurn(&reads, 5, "thread-new", "turn-2", true)
	appendCapacityFailedTurn(&reads, 6, "thread-new", "turn-3", false)
	appendSuccessfulTurn(&reads, 7, "thread-new", "turn-4", "done")
	transport := newFakeAppServerTransport(reads...)
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 2,
			Wait:       func(context.Context, time.Duration) error { return nil },
		},
	}
	var retries int
	var sawPartial bool
	got, err := runner.StartThread(context.Background(), TurnInput{
		Prompt: "continue",
		EventHandler: func(event StreamEvent) {
			if event.Kind == StreamEventStreamRetry {
				retries++
			}
			if event.Kind == StreamEventAgentMessage && event.Text == "partial progress" {
				sawPartial = true
			}
		},
	})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	if got.FinalAgentMessage != "done" || got.TurnID != "turn-4" {
		t.Fatalf("result = %#v", got)
	}
	if !sawPartial {
		t.Fatal("substantive partial update was not forwarded")
	}
	if retries != 3 {
		t.Fatalf("retry events = %d, want 3", retries)
	}
}

func TestAppServerRunnerStopsAfterCapacityRetryBudget(t *testing.T) {
	reads := appServerCapacityRetryHandshake("thread-new")
	appendCapacityFailedTurn(&reads, 4, "thread-new", "turn-1", false)
	appendCapacityFailedTurn(&reads, 5, "thread-new", "turn-2", false)
	appendCapacityFailedTurn(&reads, 6, "thread-new", "turn-3", false)
	transport := newFakeAppServerTransport(reads...)
	runner := &AppServerRunner{
		Transport: transport,
		capacityRetry: capacityRetryPolicy{
			MaxRetries: 2,
			Wait:       func(context.Context, time.Duration) error { return nil },
		},
	}
	var failures, retries int
	got, err := runner.StartThread(context.Background(), TurnInput{
		Prompt: "try",
		EventHandler: func(event StreamEvent) {
			if event.Kind == StreamEventTurnFailed {
				failures++
			}
			if event.Kind == StreamEventStreamRetry {
				retries++
			}
		},
	})
	if err == nil || !IsKind(err, ErrorCodex) {
		t.Fatalf("StartThread error = %v, want terminal Codex error", err)
	}
	if got.Status != TurnStatusFailed || got.TurnID != "turn-3" || got.Failure == nil || got.Failure.Code != "model_capacity" || got.Failure.Message != "Selected model is at capacity. Please try a different model." {
		t.Fatalf("terminal capacity result = %#v", got)
	}
	if retries != 2 || failures != 1 {
		t.Fatalf("events = retries %d failures %d, want 2 and 1", retries, failures)
	}
	writes := transport.decodedWrites(t)
	var turnStarts int
	for _, write := range writes {
		if write["method"] == appServerMethodTurnStart {
			turnStarts++
		}
	}
	if turnStarts != 3 {
		t.Fatalf("turn/start writes = %d, want initial plus two retries", turnStarts)
	}
}

func appServerCapacityRetryHandshake(threadID string) []string {
	return []string{
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		fmt.Sprintf(`{"id":3,"result":{"thread":{"id":%q}}}`, threadID),
	}
}

func appServerCapacityRetryTurnHandshake() []string {
	return []string{
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
	}
}

func appendCapacityFailedTurn(reads *[]string, requestID int, threadID string, turnID string, partial bool) {
	*reads = append(*reads,
		fmt.Sprintf(`{"id":%d,"result":{"turn":{"id":%q,"status":"inProgress","items":[]}}}`, requestID, turnID),
		fmt.Sprintf(`{"method":"turn/started","params":{"threadId":%q,"turn":{"id":%q}}}`, threadID, turnID),
	)
	if partial {
		*reads = append(*reads, fmt.Sprintf(`{"method":"item/completed","params":{"threadId":%q,"turnId":%q,"item":{"id":%q,"type":"agentMessage","text":"partial progress"}}}`, threadID, turnID, turnID+"-partial"))
	}
	*reads = append(*reads, fmt.Sprintf(`{"method":"turn/completed","params":{"threadId":%q,"turn":{"id":%q,"status":"failed","error":{"code":"model_capacity","message":"Selected model is at capacity. Please try a different model."},"items":[]}}}`, threadID, turnID))
}

func appendCapacityFailedTurnWithCommand(reads *[]string, requestID int, threadID string, turnID string) {
	*reads = append(*reads,
		fmt.Sprintf(`{"id":%d,"result":{"turn":{"id":%q,"status":"inProgress","items":[]}}}`, requestID, turnID),
		fmt.Sprintf(`{"method":"turn/started","params":{"threadId":%q,"turn":{"id":%q}}}`, threadID, turnID),
		fmt.Sprintf(`{"method":"item/completed","params":{"threadId":%q,"turnId":%q,"item":{"id":%q,"type":"commandExecution","command":"touch workspace-file","status":"completed","aggregatedOutput":"done"}}}`, threadID, turnID, turnID+"-command"),
		fmt.Sprintf(`{"method":"turn/completed","params":{"threadId":%q,"turn":{"id":%q,"status":"failed","error":{"code":"model_capacity","message":"Selected model is at capacity. Please try a different model."},"items":[]}}}`, threadID, turnID),
	)
}

func appendSuccessfulTurn(reads *[]string, requestID int, threadID string, turnID string, message string) {
	*reads = append(*reads,
		fmt.Sprintf(`{"id":%d,"result":{"turn":{"id":%q,"status":"inProgress","items":[]}}}`, requestID, turnID),
		fmt.Sprintf(`{"method":"turn/started","params":{"threadId":%q,"turn":{"id":%q}}}`, threadID, turnID),
		fmt.Sprintf(`{"method":"item/completed","params":{"threadId":%q,"turnId":%q,"item":{"id":%q,"type":"agentMessage","text":%q}}}`, threadID, turnID, turnID+"-message", message),
		fmt.Sprintf(`{"method":"turn/completed","params":{"threadId":%q,"turn":{"id":%q,"status":"completed","items":[]}}}`, threadID, turnID),
	)
}

func countMethodWrites(t *testing.T, transport *fakeAppServerTransport, method string) int {
	t.Helper()
	count := 0
	for _, write := range transport.decodedWrites(t) {
		if write["method"] == method {
			count++
		}
	}
	return count
}
