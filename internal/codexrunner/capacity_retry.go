package codexrunner

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"time"
)

const (
	defaultCapacityRetryLimit      = 10
	defaultCapacityRetryTotalLimit = 30

	defaultCapacityRetryInitialDelay = time.Second
	defaultCapacityRetryMaxDelay     = 30 * time.Second
	defaultCapacityRetryMaxElapsed   = 5 * time.Minute

	// A terminal capacity failure already belongs to an accepted turn. The
	// app-server protocol requires non-empty input for a new turn, so use a
	// small continuation instruction rather than resubmitting the original
	// user prompt or sending an invalid empty input.
	capacityRetryContinuationPrompt = "Continue the previous task from where you left off. Do not repeat commands or actions that have already completed."
)

var errCapacityRetryDeadline = errors.New("codex capacity retry deadline exceeded")

// capacityRetryPolicy is deliberately private: this is an internal recovery
// policy for the managed AppServerRunner path, not part of the runner's public
// protocol. The legacy one-shot ExecRunner has no accepted-turn continuation
// contract and intentionally does not replay a CLI process here. MaxRetries
// is the consecutive retry budget; MaxTotalRetries is a defensive lifetime
// cap so substantive updates cannot turn the reset behavior into an unbounded
// loop.
type capacityRetryPolicy struct {
	MaxRetries      int
	MaxTotalRetries int
	MaxElapsed      time.Duration
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	Wait            func(context.Context, time.Duration) error
	Jitter          func(time.Duration) time.Duration
}

func (p capacityRetryPolicy) normalized() capacityRetryPolicy {
	if p.MaxRetries == 0 {
		p.MaxRetries = defaultCapacityRetryLimit
	}
	if p.MaxTotalRetries == 0 {
		p.MaxTotalRetries = defaultCapacityRetryTotalLimit
	}
	if p.MaxElapsed <= 0 {
		p.MaxElapsed = defaultCapacityRetryMaxElapsed
	}
	if p.InitialDelay <= 0 {
		p.InitialDelay = defaultCapacityRetryInitialDelay
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = defaultCapacityRetryMaxDelay
	}
	if p.MaxDelay < p.InitialDelay {
		p.MaxDelay = p.InitialDelay
	}
	return p
}

func (p capacityRetryPolicy) wait(ctx context.Context, retryNumber int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay := p.backoff(retryNumber)
	if p.Jitter != nil {
		delay = p.Jitter(delay)
	} else {
		delay = fullJitter(delay)
	}
	if delay < 0 {
		delay = 0
	}
	if p.Wait != nil {
		return p.Wait(ctx, delay)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p capacityRetryPolicy) backoff(retryNumber int) time.Duration {
	if retryNumber <= 1 {
		return p.InitialDelay
	}
	delay := p.InitialDelay
	for i := 1; i < retryNumber && delay < p.MaxDelay; i++ {
		if delay > p.MaxDelay/2 {
			return p.MaxDelay
		}
		delay *= 2
	}
	if delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}

func fullJitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(delay) + 1))
}

// IsModelCapacityFailureMessage recognizes the transient capacity responses
// emitted by Codex and the app-server's equivalent overloaded response.
// Matching is intentionally narrow so ordinary model/tool failures do not
// become duplicate turns.
func IsModelCapacityFailureMessage(message string) bool {
	message = strings.ToLower(strings.TrimSpace(message))
	return strings.Contains(message, "selected model is at capacity") ||
		strings.Contains(message, "server overloaded; retry later")
}

func isModelCapacityFailureCode(code string) bool {
	code = strings.ToLower(strings.TrimSpace(code))
	switch code {
	case "model_capacity", "model-at-capacity", "model_at_capacity", "modelcapacity",
		"server_overloaded", "server-overloaded", "serveroverloaded":
		return true
	default:
		return false
	}
}

// IsModelCapacityFailure reports whether a turn result or its structured
// JSON-RPC error carries a transient capacity response. Callers must
// separately verify that a known turn is terminal before resubmitting it.
func IsModelCapacityFailure(result TurnResult, err error) bool {
	if result.Failure != nil && (IsModelCapacityFailureMessage(result.Failure.Message) || isModelCapacityFailureCode(result.Failure.Code)) {
		return true
	}
	if err == nil {
		return false
	}
	var codexErr *Error
	if !errors.As(err, &codexErr) || codexErr == nil {
		return false
	}
	code := ""
	if codexErr.Details != nil {
		code = codexErr.Details.ErrorCode
	}
	return IsModelCapacityFailureMessage(codexErr.Message) ||
		isModelCapacityFailureCode(code) ||
		isModelCapacityFailureCode(codexErrorCodeFromMessage(codexErr.Message))
}

func codexErrorCodeFromMessage(message string) string {
	message = strings.TrimSpace(message)
	separator := strings.Index(message, ":")
	if separator <= 0 {
		return ""
	}
	code := strings.TrimSpace(message[:separator])
	if strings.ContainsAny(code, " \t\r\n") {
		return ""
	}
	return code
}

func isModelCapacityFailureEvent(event StreamEvent) bool {
	return event.Kind == StreamEventTurnFailed && event.Failure != nil &&
		(IsModelCapacityFailureMessage(event.Failure.Message) || isModelCapacityFailureCode(event.Failure.Code))
}

func canRetryModelCapacityFailure(result TurnResult, err error) bool {
	if !IsModelCapacityFailure(result, err) {
		return false
	}
	// A terminal failed turn was accepted by Codex. It can only be retried by
	// starting a valid non-empty continuation turn; never resend the original
	// prompt against an accepted turn.
	if result.Status == TurnStatusFailed {
		return strings.TrimSpace(result.TurnID) != "" && result.Failure != nil
	}
	// A JSON-RPC turn/start rejection has no accepted turn. Restrict this path
	// to a structured app-server error and an otherwise empty result so a
	// backfill or ambiguous transport error cannot create a duplicate turn.
	if result.Status != TurnStatusUnknown || strings.TrimSpace(result.TurnID) != "" {
		return false
	}
	var codexErr *Error
	return errors.As(err, &codexErr) && codexErr != nil && IsModelCapacityFailure(TurnResult{}, codexErr)
}

func shouldFlushModelCapacityFailure(result TurnResult, err error) bool {
	if !IsModelCapacityFailure(result, err) {
		return false
	}
	if result.Status == TurnStatusFailed {
		return true
	}
	if result.Status != TurnStatusUnknown || strings.TrimSpace(result.TurnID) != "" {
		return false
	}
	var codexErr *Error
	return errors.As(err, &codexErr) && codexErr != nil
}

func modelCapacityRetryEvent(result TurnResult, err error, pending *StreamEvent, threadID string) StreamEvent {
	var event StreamEvent
	if pending != nil {
		event = *pending
		if pending.Failure != nil {
			failure := *pending.Failure
			event.Failure = &failure
		}
	} else {
		event = StreamEvent{
			Kind:     StreamEventStreamRetry,
			ThreadID: firstNonEmpty(result.ThreadID, threadID),
			TurnID:   result.TurnID,
			Failure:  modelCapacityFailure(result, err),
		}
	}
	event.Kind = StreamEventStreamRetry
	event.WillRetry = true
	event.ThreadID = firstNonEmpty(event.ThreadID, result.ThreadID, threadID)
	if event.Failure == nil {
		event.Failure = modelCapacityFailure(result, err)
	}
	return event
}

func modelCapacityFailure(result TurnResult, err error) *TurnFailure {
	if result.Failure != nil {
		failure := *result.Failure
		return &failure
	}
	message := ""
	code := ""
	var codexErr *Error
	if errors.As(err, &codexErr) && codexErr != nil {
		message = strings.TrimSpace(codexErr.Message)
		if codexErr.Details != nil {
			code = strings.TrimSpace(codexErr.Details.ErrorCode)
		}
		if code == "" {
			code = codexErrorCodeFromMessage(message)
		}
	}
	if message == "" {
		message = "Selected model is at capacity. Please try a different model."
	}
	return &TurnFailure{Code: code, Message: message}
}

func flushModelCapacityFailureEvent(handler EventHandler, event *StreamEvent, result TurnResult, err error, threadID string) {
	if handler == nil {
		return
	}
	if event != nil {
		handler(*event)
		return
	}
	handler(StreamEvent{
		Kind:     StreamEventTurnFailed,
		ThreadID: firstNonEmpty(result.ThreadID, threadID),
		TurnID:   result.TurnID,
		Failure:  modelCapacityFailure(result, err),
	})
}

func isSubstantiveStreamEvent(event StreamEvent) bool {
	switch event.Kind {
	case StreamEventAgentMessage:
		return strings.TrimSpace(event.Text) != ""
	case StreamEventCommandStarted:
		return strings.TrimSpace(event.Command) != "" || strings.TrimSpace(event.Text) != ""
	case StreamEventCommandCompleted:
		return strings.TrimSpace(event.AggregatedOutput) != "" ||
			strings.TrimSpace(event.Status) != "" ||
			event.ExitCode != nil
	case StreamEventContextCompacted:
		return true
	case StreamEventTurnCompleted:
		return strings.TrimSpace(event.Text) != ""
	default:
		return false
	}
}
