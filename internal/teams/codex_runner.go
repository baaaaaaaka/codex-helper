package teams

import (
	"context"
	"errors"
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

type ExecutionResult struct {
	Text          string
	CodexThreadID string
	CodexTurnID   string
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
			Prompt:       prompt,
			WorkingDir:   workDir,
			ExtraArgs:    e.ExtraArgs,
			Timeout:      e.Timeout,
			EventHandler: handler,
		},
	})
	if err != nil {
		out := ExecutionResult{CodexThreadID: result.ThreadID, CodexTurnID: result.TurnID}
		if result.ThreadID != "" || result.TurnID != "" || result.Status == codexrunner.TurnStatusStarted || result.Status == codexrunner.TurnStatusInProgress {
			return out, &AmbiguousExecutionError{ThreadID: result.ThreadID, TurnID: result.TurnID, Err: err}
		}
		return out, err
	}
	text := strings.TrimSpace(result.FinalAgentMessage)
	if text == "" {
		text = "(Codex finished without a final message.)"
	}
	return ExecutionResult{
		Text:          text,
		CodexThreadID: result.ThreadID,
		CodexTurnID:   result.TurnID,
	}, nil
}

type CodexExecutor struct {
	CodexPath string
	WorkDir   string
	ExtraArgs []string
	Timeout   time.Duration
}

func (e CodexExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
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
		Runner:    runner,
		WorkDir:   workDir,
		ExtraArgs: e.ExtraArgs,
		Timeout:   timeout,
	}.Run(ctx, session, prompt)
}

func parseCodexJSONL(output string) ExecutionResult {
	turn, err := codexrunner.ParseJSONL(strings.NewReader(output))
	if err != nil {
		return ExecutionResult{}
	}
	return ExecutionResult{
		Text:          turn.FinalAgentMessage,
		CodexThreadID: turn.ThreadID,
		CodexTurnID:   turn.TurnID,
	}
}
