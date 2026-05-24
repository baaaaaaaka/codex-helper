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

type StreamingExecutor interface {
	RunWithEventHandler(ctx context.Context, session *Session, prompt string, handler codexrunner.EventHandler) (ExecutionResult, error)
}

type ExecutionInput struct {
	Prompt     string
	ImagePaths []string
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

func (e RunnerExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunWithEventHandler(ctx, session, prompt, nil)
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
			Prompt:       input.Prompt,
			ImagePaths:   append([]string{}, input.ImagePaths...),
			WorkingDir:   workDir,
			ExtraArgs:    e.ExtraArgs,
			Timeout:      e.Timeout,
			EventHandler: handler,
		},
	})
	if err != nil {
		if codexTurnCompletedDespiteCanceledError(result, err) {
			out := successfulExecutionResultFromCodexTurn(result)
			if threadID != "" && out.CodexThreadID != "" && out.CodexThreadID != threadID {
				return out, fmt.Errorf("resume emitted Codex thread %q, expected %q", out.CodexThreadID, threadID)
			}
			return out, nil
		}
		out := executionResultFromCodexTurn(result)
		if codexTurnMayStillBeRunning(result) {
			return out, &AmbiguousExecutionError{ThreadID: result.ThreadID, TurnID: result.TurnID, Err: err}
		}
		return out, err
	}
	out := successfulExecutionResultFromCodexTurn(result)
	if threadID != "" && out.CodexThreadID != "" && out.CodexThreadID != threadID {
		return out, fmt.Errorf("resume emitted Codex thread %q, expected %q", out.CodexThreadID, threadID)
	}
	return out, nil
}

func codexTurnCompletedDespiteCanceledError(result codexrunner.TurnResult, err error) bool {
	return err != nil &&
		(errors.Is(err, context.Canceled) || codexrunner.IsKind(err, codexrunner.ErrorCanceled)) &&
		result.Status == codexrunner.TurnStatusCompleted &&
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
	text := strings.TrimSpace(result.FinalAgentMessage)
	if text == "" {
		text = "(Codex finished without a final message.)"
	}
	return ExecutionResult{
		Text:             text,
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
