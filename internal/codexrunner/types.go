package codexrunner

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Runner interface {
	StartThread(ctx context.Context, input TurnInput) (TurnResult, error)
	ResumeThread(ctx context.Context, threadID string, input TurnInput) (TurnResult, error)
	StartTurn(ctx context.Context, input StartTurnInput) (TurnResult, error)
	InterruptTurn(ctx context.Context, ref TurnRef) error
	ReadThread(ctx context.Context, threadID string) (Thread, error)
	ListThreads(ctx context.Context, opts ListThreadsOptions) ([]Thread, error)
}

type TurnInput struct {
	Prompt       string
	WorkingDir   string
	ExtraArgs    []string
	Timeout      time.Duration
	EventHandler EventHandler
}

type StartTurnInput struct {
	ThreadID string
	TurnInput
}

type TurnRef struct {
	ThreadID string
	TurnID   string
}

type Thread struct {
	ID string
}

type ListThreadsOptions struct {
	WorkingDir string
	Limit      int
}

type TurnStatus string

const (
	TurnStatusUnknown     TurnStatus = ""
	TurnStatusStarted     TurnStatus = "started"
	TurnStatusInProgress  TurnStatus = "inProgress"
	TurnStatusCompleted   TurnStatus = "completed"
	TurnStatusFailed      TurnStatus = "failed"
	TurnStatusInterrupted TurnStatus = "interrupted"
)

type TurnResult struct {
	ThreadID            string
	TurnID              string
	Status              TurnStatus
	FinalAgentMessage   string
	Failure             *TurnFailure
	Usage               Usage
	RawCompletedMessage []byte
}

type TurnFailure struct {
	Code    string
	Message string
}

type Usage struct {
	InputTokens       int64
	OutputTokens      int64
	TotalTokens       int64
	CachedInputTokens int64
}

type CommandLauncher interface {
	Launch(ctx context.Context, req LaunchRequest) (LaunchResult, error)
}

type LaunchRequest struct {
	Command      string
	Args         []string
	Dir          string
	Stdin        string
	Timeout      time.Duration
	EventHandler EventHandler
}

type LaunchResult struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
}

type ErrorKind string

const (
	ErrorInvalidRequest ErrorKind = "invalid_request"
	ErrorLaunch         ErrorKind = "launch_failure"
	ErrorCodex          ErrorKind = "codex_failure"
	ErrorTimeout        ErrorKind = "timeout"
	ErrorCanceled       ErrorKind = "canceled"
	ErrorParse          ErrorKind = "parse_failure"
	ErrorUnsupported    ErrorKind = "unsupported"
)

type Error struct {
	Kind    ErrorKind
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return fmt.Sprintf("%s: %s", e.Kind, e.Message)
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Kind, e.Err)
	}
	return string(e.Kind)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func IsKind(err error, kind ErrorKind) bool {
	var target *Error
	return errors.As(err, &target) && target.Kind == kind
}

func unsupported(operation string) error {
	return &Error{Kind: ErrorUnsupported, Message: operation + " is not supported by this runner"}
}
