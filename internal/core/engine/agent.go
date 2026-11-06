package engine

import (
	"context"
	"errors"
)

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

// Forked is the new thread of a fork: its id and, when Codex names it, its
// rollout file (it may already hold a copy of the parent's history, as
// Codex 0.133-0.145 wrote it, or only its own session_meta, as 0.166).
type Forked struct {
	ThreadID string
	Path     string
}

// TurnRequest is one turn to run.
type TurnRequest struct {
	ConvID   int64
	ThreadID string // "" starts a new thread (reported through ThreadStarted first)
	Cwd      string
	Model    string
	Effort   string
	TurnKey  string // sent as clientUserMessageId
	Text     string
	Images   []string
}

// TurnHooks observe a running turn.
type TurnHooks struct {
	// ThreadStarted is called with a new thread id before turn/start; the
	// turn does not start unless it returns nil (the binding must be
	// durable first so a restart continues the same thread).
	ThreadStarted func(threadID string) error
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
	// NotStarted: turn/start was rejected; the turn certainly did not run.
	NotStarted bool
	// ActiveWriter: another Codex process holds the thread (e.g. the TUI).
	ActiveWriter bool
	Usage        Usage
	// Progressed: Codex streamed output during the turn (set by the
	// engine). A capacity failure after progress restarts the retry window.
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
