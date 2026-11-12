package engine

import (
	"context"
	"errors"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// QuestionAgent delivers native requests and message events from every loaded
// thread, including turns initiated by an attached TUI.
type QuestionAgent interface {
	SetQuestionHandler(func(QuestionEvent) error)
	AnswerQuestions(context.Context, string, map[string][]string) error
	QuestionPending(string) bool
}

type QuestionEvent struct {
	Group            store.QuestionGroup
	Resolved         string
	ThreadID, TurnID string
	Interrupted      bool
	Finished         bool
	Answered         []string
}

var ErrQuestionExpired = errors.New("the question is no longer pending")

// Agent runs Codex turns. The engine knows nothing about app-server
// processes; internal/core/codexagent implements Agent over
// internal/core/codex.
type Agent interface {
	// Prepare runs once at startup, before any reconciliation: it kills
	// (or confirms dead) every app-server a previous helper process left
	// behind, so no old server can still change a turn.
	Prepare(ctx context.Context) error
	// Run executes one turn and blocks until it ends. It returns
	// ErrUnknownOutcome when the turn may have started but its end was not
	// observed (server died); the engine then reconciles with Lookup.
	// A ctx error means the engine is shutting down: the engine records
	// nothing and reconciles at the next start.
	Run(ctx context.Context, req TurnRequest, hooks TurnHooks) (TurnOutcome, error)
	// Lookup finds the turn whose user message carries turnKey on threadID.
	Lookup(ctx context.Context, convID int64, threadID, turnKey string) (TurnOutcome, bool, error)
	// Interrupt asks Codex to stop the running turn of convID.
	Interrupt(ctx context.Context, convID int64) error
	// Kill ends the app-server of convID at once (its running turn is lost).
	Kill(convID int64)
	// Fork forks threadID after lastTurnID ("" = everything) into a new thread.
	Fork(ctx context.Context, convID int64, threadID, lastTurnID, cwd string) (Forked, error)
	// Models lists the models Codex offers.
	Models(ctx context.Context) ([]Model, error)
	// Maintain reaps idle app-servers.
	Maintain()
	// Close ends every app-server (running turns are lost and reconciled at
	// the next start).
	Close()
}

// GoalAgent is an Agent that knows Codex thread goals and settings
// (owner request 2026-11-07; codexagent). Without it the helper shows no
// goal or model line and `helper goal` answers that Codex has no goals.
type GoalAgent interface {
	// Info is what the app-server of convID knows of threadID now: its
	// active goal and its effective model, effort and tier (zero when
	// convID has no app-server).
	Info(convID int64, threadID string) TurnInfo
	// Goal reads the goal of threadID (change zero) or sets it as the
	// user: a new objective and/or a status (GoalActive, GoalPaused). An
	// active goal on an idle thread makes Codex start working on it.
	Goal(ctx context.Context, convID int64, threadID, cwd string, change GoalState) (GoalState, error)
	// StopGoal is the goal part of `helper cancel`: it pauses the active
	// goal of threadID (if the app-server of convID has it loaded) and
	// interrupts a turn Codex runs by itself in it (a goal continuation).
	StopGoal(ctx context.Context, convID int64, threadID string) (GoalStopped, error)
}

// ErrActiveWriter: Codex refused to load the thread because another
// Codex process is writing to it (GoalAgent.Goal).
var ErrActiveWriter = errors.New("the session is open in another Codex window that is writing to it")

// TurnInfo is shown with a Codex message: the objective of the active
// goal ("" when no goal is active) and the effective model settings.
type TurnInfo struct {
	Goal                string
	Model, Effort, Tier string
	// SelfTurn: Codex runs a turn in the thread that the helper did not
	// start (a goal continuation).
	SelfTurn bool
}

// GoalState is a thread goal (Status "" = no goal).
type GoalState struct {
	Objective, Status string
}

// Goal statuses the helper sets.
const (
	GoalActive = "active"
	GoalPaused = "paused"
)

// GoalStopped reports what StopGoal did.
type GoalStopped struct {
	Paused      bool   // an active goal was paused
	Interrupted bool   // a turn Codex ran by itself was interrupted
	Objective   string // the paused goal's objective
}

// Forked is the new thread of a fork: its id and, when Codex names it, its
// rollout file (it may already hold a copy of the parent's history, as
// Codex 0.133-0.145 wrote it, or only its own session_meta, as 0.166).
type Forked struct {
	ThreadID string
	Path     string
}

// TurnRequest is one turn to run.
type TurnRequest struct {
	GenerateTitle bool   // only a newly created ordinary Teams work thread
	TitleText     string // unwrapped first user request
	Queue         bool
	ConvID        int64
	ThreadID      string // "" starts a new thread (reported through ThreadStarted first)
	Cwd           string
	Model         string
	Effort        string
	TurnKey       string // sent as clientUserMessageId
	Text          string
	Images        []string
}

// TurnHooks observe a running turn.
type TurnHooks struct {
	// ThreadStarted is called with a new thread id before turn/start; the
	// turn does not start unless it returns nil (the binding must be
	// durable first so a restart continues the same thread).
	ThreadStarted func(threadID string) error
	Title         func(context.Context, func(context.Context) (string, error)) error // serialize saving a generated name with manual renames
	Ready         func()                                                             // backend state changed; pending input may now steer
	// Started is called once Codex accepted the turn.
	Started func(codexTurnID string)
	// Progress receives commentary.
	Progress func(text string)
	// Compacted is called once per context compaction during the turn.
	Compacted func()
	// Activity is called on any Codex event of the running turn (silence
	// hints and the stall bound, R-19). It must not block.
	Activity func()
	// Retrying is called when Codex reports an error it retries by itself
	// (a dropped stream it reconnects), with its message and reason code.
	Retrying func(message, code string)
	// Cancelled reports whether the user cancelled the turn. The agent
	// checks it once the turn is registered for Interrupt and before
	// turn/start, so a cancel while Codex is still starting stops the turn
	// before it runs.
	Cancelled func() bool
}

// Turn statuses reported by the agent.
const (
	StatusCompleted   = "completed"
	StatusFailed      = "failed"
	StatusInterrupted = "interrupted"
	StatusInProgress  = "inProgress"
)

// TurnOutcome is how a turn ended (or what Lookup found).
type TurnOutcome struct {
	CodexTurnID string
	Status      string
	Final       string
	Error       string
	// Capacity marks a model-capacity failure (retried within 5 minutes).
	Capacity bool
	// ReconnectExhausted marks a terminal network connection failure
	// after Codex's own reconnect attempts. The engine retries it like capacity.
	ReconnectExhausted bool
	// NotStarted: turn/start was rejected; the turn certainly did not run.
	NotStarted bool
	// Unconfirmed distinguishes an absent history result from a definite
	// pre-submission rejection. A model handoff must pause in this case.
	Unconfirmed bool
	// ActiveWriter: another Codex process holds the thread (e.g. the TUI).
	ActiveWriter bool
	Usage        Usage
	// Progressed: Codex streamed output during the turn (set by the
	// engine). A retryable failure after progress restarts the retry window.
	Progressed bool
}

// Usage is token usage of a turn.
type Usage struct {
	Input, Cached, Output, Reasoning, Total int64
}

// Model is one model offered by Codex.
type Model struct {
	ID          string
	DisplayName string
	Default     bool
	Efforts     []string
	Effort      string
}

// ErrRestart is returned by Run when the owner asked for a helper restart
// (`helper restart`); the caller starts the helper again on the same state
// (the service exits with code 75).
var ErrRestart = errors.New("engine: restart requested")

// ErrUnknownOutcome means the turn's end was not observed.
var ErrUnknownOutcome = errors.New("engine: turn outcome unknown")

// Transcriber turns a voice attachment into text (ASR). Implementations
// live outside the engine; nil disables voice.
//
// An error wrapping ErrTranscribeLater keeps the attachment pending (and
// the turn waiting) for a later pass, e.g. while the recognizer is still
// being installed. An error with a UserText() string method supplies the
// reason shown to the owner and to Codex; it must carry no paths.
type Transcriber interface {
	Transcribe(ctx context.Context, path string) (string, error)
}

// ErrTranscribeLater: the transcriber cannot run yet; try again later.
var ErrTranscribeLater = errors.New("engine: transcription not ready yet")

// SteeringAgent can append input to an exact running turn. A definite
// finished-target rejection is ErrSteerExpired; other errors are never replayed.
type SteeringAgent interface {
	Steer(context.Context, TurnRequest, string) error
	CurrentTurn(int64, string) string
}

var ErrSteerExpired = errors.New("Codex turn finished before steering")

// ErrBackendUncertain preserves shared native work after a transport failure.
var ErrBackendUncertain = errors.New("shared Codex connection lost; input outcome is unknown")

// InputRejected is a definite rejection: no input was accepted.
type InputRejected struct{ Reason string }

func (e *InputRejected) Error() string { return e.Reason }

// TranscriptAgent records completed commentary before Run returns. Its durable
// rollout is the sole source of progress when the work chat has a mirror.
type TranscriptAgent interface{ TranscriptProgress() bool }

// TitleAgent cancels best-effort naming when the owner takes control.
type TitleAgent interface{ CancelTitle(int64) }
