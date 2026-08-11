package teams

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
)

type Executor interface {
	Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error)
}

// errExecutorNotDispatched distinguishes cancellation before the executor was
// called from cancellation of an already-started Codex request. The former
// must return the durable turn to queued without creating an interrupted
// execution anchor or a retry-looking answer.
var errExecutorNotDispatched = errors.New("Codex executor was not dispatched")

// ForkExecutor is optional so existing custom Executors remain source
// compatible. The Teams native fork path is only enabled when the configured
// Codex runner implements this capability; it never degrades to a prompt.
type ForkExecutor interface {
	ForkThread(ctx context.Context, session *Session, cutoffCodexTurnID string) (ForkResult, error)
}

// ExecutionFenceReconciler is an optional executor capability. It never
// starts Codex work; it only reports whether the runner had an existing
// same-thread cancellation fence and confirmed it clear.
type ExecutionFenceReconciler interface {
	ReconcileExecutionFence(context.Context, *Session) (bool, error)
}

// ForkReconciler is the read-only recovery capability for a fork request whose
// native response was lost. Implementations must never issue another fork
// request; they may only inspect the runtime and return a uniquely identified
// child.
type ForkReconciler interface {
	ReconcileForkThread(ctx context.Context, session *Session, cutoffCodexTurnID string, windowStart time.Time, windowEnd time.Time) (ForkReconcileResult, error)
}

type ForkReconcileResult struct {
	MatchCount int
	Result     ForkResult
}

type ForkResult struct {
	CodexThreadID    string
	CodexThreadTitle string
}

// ReasoningEffortCatalogProvider is implemented by executors that can ask
// their actual Codex runtime for the current model's effort choices.
type ReasoningEffortCatalogProvider interface {
	ReasoningEffortCatalog(context.Context, *Session) (ReasoningEffortCatalog, error)
}

// ReadOnlyReasoningEffortCatalogProvider is the non-allocating variant used by
// status/list commands. Implementations must inspect only an already-cached
// runner; they must not start a new app-server or evict a runner that owns an
// unresolved same-thread execution.
type ReadOnlyReasoningEffortCatalogProvider interface {
	CachedReasoningEffortCatalog(context.Context, *Session) (ReasoningEffortCatalog, error)
}

// ReasoningEffortDefaultProvider exposes the launch-level default already
// configured on an executor. Per-chat state overrides this value once the user
// explicitly switches or resets the chat.
type ReasoningEffortDefaultProvider interface {
	DefaultReasoningEffort() string
}

type ReasoningEffortCatalog struct {
	Model         string
	DisplayName   string
	DefaultEffort string
	Options       []ReasoningEffortOption
}

type ReasoningEffortOption struct {
	Effort      string
	Description string
}

type StreamingExecutor interface {
	RunWithEventHandler(ctx context.Context, session *Session, prompt string, handler codexrunner.EventHandler) (ExecutionResult, error)
}

type ExecutionInput struct {
	Prompt          string
	ImagePaths      []string
	BeforeFirstTurn codexrunner.BeforeFirstTurnHook `json:"-"`
}

type InputExecutor interface {
	RunInput(ctx context.Context, session *Session, input ExecutionInput) (ExecutionResult, error)
}

type StreamingInputExecutor interface {
	RunInputWithEventHandler(ctx context.Context, session *Session, input ExecutionInput, handler codexrunner.EventHandler) (ExecutionResult, error)
}

type ExecutionResult struct {
	Text             string
	CodexThreadID    string
	CodexThreadTitle string
	CodexTurnID      string

	canonicalTranscriptFinal bool
}

type AmbiguousExecutionError struct {
	ThreadID string
	TurnID   string
	Err      error
}

func (e *AmbiguousExecutionError) Error() string {
	if e == nil || e.Err == nil {
		return "codex execution may still be running"
	}
	return "codex execution may still be running: " + e.Err.Error()
}

func (e *AmbiguousExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func IsAmbiguousExecutionError(err error) bool {
	var ambiguous *AmbiguousExecutionError
	return errors.As(err, &ambiguous)
}

func isCanceledExecutionError(err error) bool {
	return errors.Is(err, context.Canceled) || codexrunner.IsKind(err, codexrunner.ErrorCanceled)
}

func isUnresolvedExecutionFenceError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, codexrunner.ErrTurnCancelFenceUnconfirmed) {
		return true
	}
	// Keep compatibility with older app-server adapters that predate the
	// exported sentinel but already emitted the stable fence message.
	if !codexrunner.IsKind(err, codexrunner.ErrorCodex) {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "previous codex turn cancellation is still unconfirmed") ||
		strings.Contains(text, "codex turn cancel was not confirmed") ||
		strings.Contains(text, "turn cancel could not be confirmed")
}

type EchoExecutor struct{}

func (EchoExecutor) Run(_ context.Context, _ *Session, prompt string) (ExecutionResult, error) {
	return ExecutionResult{Text: "echo: " + strings.TrimSpace(prompt)}, nil
}

type RunnerExecutor struct {
	Runner    codexrunner.Runner
	WorkDir   string
	ExtraArgs []string
	Timeout   time.Duration
}

func (e RunnerExecutor) ReconcileExecutionFence(ctx context.Context, session *Session) (bool, error) {
	if session == nil {
		return false, nil
	}
	reconciler, ok := e.Runner.(codexrunner.TurnCancelFenceReconciler)
	if !ok {
		return false, nil
	}
	return reconciler.ReconcileTurnCancelFence(ctx, strings.TrimSpace(session.CodexThreadID))
}

func (e RunnerExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunWithEventHandler(ctx, session, prompt, nil)
}

func (e RunnerExecutor) ForkThread(ctx context.Context, session *Session, cutoffCodexTurnID string) (ForkResult, error) {
	if session == nil || strings.TrimSpace(session.CodexThreadID) == "" {
		return ForkResult{}, fmt.Errorf("Codex parent thread is required for fork")
	}
	cutoffCodexTurnID = strings.TrimSpace(cutoffCodexTurnID)
	if cutoffCodexTurnID == "" {
		return ForkResult{}, fmt.Errorf("last completed Codex turn is required for fork")
	}
	forger, ok := e.Runner.(codexrunner.ThreadForker)
	if !ok {
		return ForkResult{}, codexrunner.UnsupportedError("thread/fork")
	}
	child, err := forger.ForkThread(ctx, codexrunner.ThreadForkParams{
		ThreadID:              strings.TrimSpace(session.CodexThreadID),
		LastTurnID:            cutoffCodexTurnID,
		ExcludeTurns:          true,
		DeferGoalContinuation: true,
		Ephemeral:             false,
		WorkingDir:            strings.TrimSpace(session.Cwd),
	})
	if err != nil {
		return ForkResult{}, err
	}
	return ForkResult{CodexThreadID: child.ID, CodexThreadTitle: child.Name}, nil
}

func (e RunnerExecutor) RunWithEventHandler(ctx context.Context, session *Session, prompt string, handler codexrunner.EventHandler) (ExecutionResult, error) {
	return e.RunInputWithEventHandler(ctx, session, ExecutionInput{Prompt: prompt}, handler)
}

func (e RunnerExecutor) RunInput(ctx context.Context, session *Session, input ExecutionInput) (ExecutionResult, error) {
	return e.RunInputWithEventHandler(ctx, session, input, nil)
}

func (e RunnerExecutor) RunInputWithEventHandler(ctx context.Context, session *Session, input ExecutionInput, handler codexrunner.EventHandler) (ExecutionResult, error) {
	runner := e.Runner
	workDir := strings.TrimSpace(e.WorkDir)
	if session != nil && strings.TrimSpace(session.Cwd) != "" {
		workDir = strings.TrimSpace(session.Cwd)
	}
	if runner == nil {
		runner = &codexrunner.ExecRunner{
			Command:    "codex",
			WorkingDir: workDir,
			ExtraArgs:  e.ExtraArgs,
			Timeout:    e.Timeout,
		}
	}
	threadID := ""
	if session != nil {
		threadID = strings.TrimSpace(session.CodexThreadID)
	}
	result, err := runner.StartTurn(ctx, codexrunner.StartTurnInput{
		ThreadID: threadID,
		TurnInput: codexrunner.TurnInput{
			Prompt:          input.Prompt,
			ImagePaths:      append([]string{}, input.ImagePaths...),
			WorkingDir:      workDir,
			ExtraArgs:       e.ExtraArgs,
			Timeout:         e.Timeout,
			EventHandler:    handler,
			BeforeFirstTurn: input.BeforeFirstTurn,
			ReasoningEffort: func() string {
				if session == nil {
					return ""
				}
				return strings.TrimSpace(session.ReasoningEffort)
			}(),
		},
	})
	if err != nil {
		if codexTurnCompletedDespiteCanceledError(result, err) {
			out := successfulExecutionResultFromCodexTurn(result)
			if strings.TrimSpace(out.Text) == "" {
				return out, missingFinalExecutionError(result, err)
			}
			if threadID != "" && out.CodexThreadID != "" && out.CodexThreadID != threadID {
				return out, fmt.Errorf("resume emitted Codex thread %q, expected %q", out.CodexThreadID, threadID)
			}
			return out, nil
		}
		out := executionResultFromCodexTurn(result)
		if result.Status == codexrunner.TurnStatusCompleted && result.Failure == nil && !result.FinalAgentMessageComplete {
			return out, missingFinalExecutionError(result, err)
		}
		if codexTurnMayStillBeRunning(result) {
			return out, &AmbiguousExecutionError{ThreadID: result.ThreadID, TurnID: result.TurnID, Err: err}
		}
		return out, err
	}
	out := successfulExecutionResultFromCodexTurn(result)
	if strings.TrimSpace(out.Text) == "" || (result.Status == codexrunner.TurnStatusCompleted && !result.FinalAgentMessageComplete) {
		return out, missingFinalExecutionError(result, nil)
	}
	if threadID != "" && out.CodexThreadID != "" && out.CodexThreadID != threadID {
		return out, fmt.Errorf("resume emitted Codex thread %q, expected %q", out.CodexThreadID, threadID)
	}
	return out, nil
}

func missingFinalExecutionError(result codexrunner.TurnResult, cause error) error {
	if cause == nil {
		cause = fmt.Errorf("Codex turn completed without a final agent message")
	} else {
		cause = fmt.Errorf("Codex turn completed without a final agent message: %w", cause)
	}
	if strings.TrimSpace(result.TurnID) != "" {
		return &AmbiguousExecutionError{ThreadID: result.ThreadID, TurnID: result.TurnID, Err: cause}
	}
	return cause
}

func codexTurnCompletedDespiteCanceledError(result codexrunner.TurnResult, err error) bool {
	return err != nil &&
		(errors.Is(err, context.Canceled) || codexrunner.IsKind(err, codexrunner.ErrorCanceled)) &&
		result.Status == codexrunner.TurnStatusCompleted &&
		result.FinalAgentMessageComplete &&
		result.Failure == nil
}

func codexTurnMayStillBeRunning(result codexrunner.TurnResult) bool {
	switch result.Status {
	case codexrunner.TurnStatusStarted, codexrunner.TurnStatusInProgress:
		return true
	case codexrunner.TurnStatusUnknown:
		return strings.TrimSpace(result.TurnID) != ""
	default:
		return false
	}
}

func executionResultFromCodexTurn(result codexrunner.TurnResult) ExecutionResult {
	return ExecutionResult{
		Text:             strings.TrimSpace(result.FinalAgentMessage),
		CodexThreadID:    result.ThreadID,
		CodexThreadTitle: strings.TrimSpace(result.ThreadName),
		CodexTurnID:      result.TurnID,
	}
}

func successfulExecutionResultFromCodexTurn(result codexrunner.TurnResult) ExecutionResult {
	return ExecutionResult{
		Text:             strings.TrimSpace(result.FinalAgentMessage),
		CodexThreadID:    result.ThreadID,
		CodexThreadTitle: strings.TrimSpace(result.ThreadName),
		CodexTurnID:      result.TurnID,
	}
}

type CodexExecutor struct {
	CodexPath string
	WorkDir   string
	ExtraArgs []string
	Timeout   time.Duration
}

func (CodexExecutor) ForkThread(context.Context, *Session, string) (ForkResult, error) {
	return ForkResult{}, codexrunner.UnsupportedError("thread/fork")
}

func (e CodexExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunInput(ctx, session, ExecutionInput{Prompt: prompt})
}

func (e CodexExecutor) RunInput(ctx context.Context, session *Session, input ExecutionInput) (ExecutionResult, error) {
	timeout := e.Timeout
	command := strings.TrimSpace(e.CodexPath)
	if command == "" {
		command = "codex"
	}
	workDir := strings.TrimSpace(e.WorkDir)
	if session != nil && strings.TrimSpace(session.Cwd) != "" {
		workDir = strings.TrimSpace(session.Cwd)
	}
	runner := &codexrunner.ExecRunner{
		Command:    command,
		WorkingDir: workDir,
		ExtraArgs:  e.ExtraArgs,
		Timeout:    timeout,
	}
	return RunnerExecutor{
		Runner:  runner,
		WorkDir: workDir,
		Timeout: timeout,
	}.RunInput(ctx, session, input)
}

func parseCodexJSONL(output string) ExecutionResult {
	turn, err := codexrunner.ParseJSONL(strings.NewReader(output))
	if err != nil {
		return ExecutionResult{}
	}
	return ExecutionResult{
		Text:             turn.FinalAgentMessage,
		CodexThreadID:    turn.ThreadID,
		CodexThreadTitle: strings.TrimSpace(turn.ThreadName),
		CodexTurnID:      turn.TurnID,
	}
}
