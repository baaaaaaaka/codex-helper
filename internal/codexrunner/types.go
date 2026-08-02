package codexrunner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrTurnInterruptRequested distinguishes an explicit request to stop the
// active Codex turn from a caller lifecycle context that merely went away.
var ErrTurnInterruptRequested = errors.New("codex turn interrupt requested")

type Runner interface {
	StartThread(ctx context.Context, input TurnInput) (TurnResult, error)
	ResumeThread(ctx context.Context, threadID string, input TurnInput) (TurnResult, error)
	StartTurn(ctx context.Context, input StartTurnInput) (TurnResult, error)
	InterruptTurn(ctx context.Context, ref TurnRef) error
	ReadThread(ctx context.Context, threadID string) (Thread, error)
	ListThreads(ctx context.Context, opts ListThreadsOptions) ([]Thread, error)
}

// ModelCatalogReader is an optional runner capability. Keeping it separate
// from Runner lets lightweight and test runners continue to implement only
// turn execution while rich app-server clients can discover model-specific
// reasoning effort choices.
type ModelCatalogReader interface {
	ListModels(context.Context) ([]ModelInfo, error)
}

// ThreadForker is an optional app-server capability. It is deliberately kept
// separate from Runner so lightweight runners and older integrations do not
// need to implement native thread forking.
type ThreadForker interface {
	ForkThread(context.Context, ThreadForkParams) (Thread, error)
}

type ThreadForkParams struct {
	ThreadID              string
	LastTurnID            string
	BeforeTurnID          string
	ExcludeTurns          bool
	DeferGoalContinuation bool
	Ephemeral             bool
	WorkingDir            string
	RuntimeWorkspaceRoots []string
}

type ModelInfo struct {
	ID                     string
	Model                  string
	DisplayName            string
	IsDefault              bool
	DefaultReasoningEffort string
	ReasoningEfforts       []ReasoningEffortOption
}

type ReasoningEffortOption struct {
	Effort      string
	Description string
}

type TurnInput struct {
	Prompt string
	// Model is the exact model slug to apply to this turn. Empty preserves the
	// app-server/thread default.
	Model              string
	ImagePaths         []string
	AdditionalDirs     []string
	OutputSchema       json.RawMessage
	WorkingDir         string
	ExtraArgs          []string
	Timeout            time.Duration
	EventHandler       EventHandler
	BackfillThreadName bool
	// ReasoningEffort is the exact model-advertised wire value to apply to this turn.
	ReasoningEffort string
	// Ephemeral creates a pathless thread that is not persisted by Codex.
	Ephemeral bool
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
	ID           string
	Name         string
	ForkedFromID string
	// ForkedFromTurnID is retained for source compatibility with older test
	// runners, but current app-server Thread responses do not expose this
	// field. Reconciliation must prove the cutoff from the child history.
	ForkedFromTurnID string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	LatestTurnID     string
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
	ThreadID          string
	ThreadName        string
	TurnID            string
	Status            TurnStatus
	FinalAgentMessage string
	// FinalAgentMessageComplete distinguishes a terminal agent item from
	// provisional deltas that happen to contain non-empty text.
	FinalAgentMessageComplete bool `json:"-"`
	Failure                   *TurnFailure
	Usage                     Usage
	RawCompletedMessage       []byte
}

type TurnFailure struct {
	Code    string
	Message string
}

type Usage struct {
	InputTokens           int64
	OutputTokens          int64
	TotalTokens           int64
	CachedInputTokens     int64
	ReasoningOutputTokens int64
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
	Stdout          []byte
	StdoutTruncated bool
	Stderr          []byte
	StderrTruncated bool
	ExitCode        int
	ParsedResult    *TurnResult
	ParseErr        error
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
	ErrorAmbiguous      ErrorKind = "ambiguous"
)

type Error struct {
	Kind    ErrorKind
	Message string
	Err     error
	Details *CodexErrorDetails
}

// CodexErrorDetails preserves the bounded, non-secret part of an app-server
// JSON-RPC error data object. Keeping these fields structured lets CLI and
// Teams callers explain setup failures without exposing an upstream HTML body,
// authentication material, or CXP's loopback capability token.
type CodexErrorDetails struct {
	Reason     string
	ErrorCode  string
	StatusCode int
	Detail     string
	RequestID  string
	Cloudflare bool
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

// UnsupportedError exposes the same typed error used by built-in runners to
// optional integrations such as the Teams native fork workflow.
func UnsupportedError(operation string) error {
	return unsupported(operation)
}
