// Package fakecodex is a fake Codex app-server (`codex app-server`): a
// stdio JSON-RPC server speaking the subset of the app-server v2 protocol
// the Teams helper uses (initialize, thread/start|resume|read|list|fork,
// thread/turns/list, model/list, turn/start|interrupt and the turn
// notifications).
//
// Like real Codex it persists threads and turns (under its state dir, by
// default $CODEX_HOME/e2e-fakecodex) and writes a rollout JSONL per thread
// under $CODEX_HOME/sessions, so a restarted helper (or a new app-server
// process) sees the same threads.
//
// The fake has no "model": for every turn it asks the test process (the
// "brain", harness.FakeCodex) what to do over HTTP, so the call log, the
// scripting and block/release all live in the test. See cmd/fakecodex for
// the binary and harness.FakeCodex.Handler for the brain.
package fakecodex

import "time"

// Environment variables read by the binary.
const (
	// EnvBrainURL is the base URL of the brain (required).
	EnvBrainURL = "E2E_FAKECODEX_BRAIN"
	// EnvStateDir overrides the state dir (default $CODEX_HOME/e2e-fakecodex).
	EnvStateDir = "E2E_FAKECODEX_STATE"
	// EnvKind labels the calls ("work" or "control").
	EnvKind = "E2E_FAKECODEX_KIND"
	// EnvInstance labels the calls with the SUT instance that launched it.
	EnvInstance = "E2E_FAKECODEX_INSTANCE"
)

// BeginRequest is sent by the app-server when a turn/start arrives.
type BeginRequest struct {
	Instance string   `json:"instance"`
	Kind     string   `json:"kind"`
	Pid      int      `json:"pid"`
	ThreadID string   `json:"threadId"`
	TurnID   string   `json:"turnId"`
	ClientID string   `json:"clientId,omitempty"` // turn/start clientUserMessageId
	Cwd      string   `json:"cwd"`
	Prompt   string   `json:"prompt"`
	Images   []string `json:"images,omitempty"`
	Model    string   `json:"model,omitempty"`  // turn/start model override
	Effort   string   `json:"effort,omitempty"` // turn/start effort override
	// Exe is the path of the Codex executable that runs the app-server
	// (which installation the helper chose).
	Exe string `json:"exe,omitempty"`
	// MCPServers are the MCP servers of config.toml the thread was loaded
	// with in this process (which config the turn runs with).
	MCPServers []string `json:"mcpServers,omitempty"`
}

// ResumeRequest is sent by the app-server when a thread/resume arrives for a
// thread this process has not loaded yet (POST /resume).
type ResumeRequest struct {
	Instance string `json:"instance"`
	Kind     string `json:"kind"`
	Pid      int    `json:"pid"`
	ThreadID string `json:"threadId"`
}

// ResumeDecision is the brain's answer to a ResumeRequest.
type ResumeDecision struct {
	// Reject, if set, is returned as the JSON-RPC error (-32600) of
	// thread/resume ("{thread}" becomes the thread id), and the thread stays
	// unloaded: a following turn/start fails with "thread not found".
	Reject string `json:"reject,omitempty"`
}

// Plan is the brain's answer to a BeginRequest.
type Plan struct {
	Questions *QuestionScript `json:"questions,omitempty"`
	Seq       int             `json:"seq"`
	// RejectStart, if set, is returned as the JSON-RPC error of turn/start.
	RejectStart string `json:"rejectStart,omitempty"`
	Commentary  string `json:"commentary,omitempty"`
	// MoreCommentary is streamed after Commentary, one agent message each.
	MoreCommentary []string `json:"moreCommentary,omitempty"`
	// Exec are shell commands (sh -c) the turn runs before it answers, in
	// the turn's cwd with the app-server's environment, as Codex's shell
	// tool would. Each gets 60 s; output goes to the app-server log.
	Exec []string `json:"exec,omitempty"`
	// Unphased sends every agent message (commentary and final) without a
	// phase, as Codex streams them for models without message phases.
	Unphased bool `json:"unphased,omitempty"`
	// Compactions is how many context compactions the turn reports before
	// its commentary, each as a contextCompaction item and the legacy
	// thread/compacted notification (as Codex does).
	Compactions    int  `json:"compactions,omitempty"`
	Crash          bool `json:"crash,omitempty"`
	HoldCompletion bool `json:"holdCompletion,omitempty"`
	SurviveHelper  bool `json:"surviveHelper,omitempty"`
	// MalformedStart answers turn/start with a result the client cannot
	// decode while the turn runs on (an error that is neither a JSON-RPC
	// rejection nor an exit).
	MalformedStart bool `json:"malformedStart,omitempty"`
	// Retries are error notifications Codex retries by itself, sent after
	// the commentary and before the turn's outcome.
	Retries []Retry `json:"retries,omitempty"`
}

// QuestionScript uses the native wire shapes. It is not an engine shortcut:
// sync answers must arrive as JSON-RPC responses over the client transport.
type QuestionScript struct {
	Async     bool           `json:"async"`
	Blocking  bool           `json:"blocking"`
	Questions []QuestionSpec `json:"questions"`
}
type QuestionSpec struct {
	ID       string           `json:"id"`
	Question string           `json:"question"`
	Options  []QuestionOption `json:"options,omitempty"`
}
type QuestionOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

// Retry is one `error` notification with willRetry (real Codex: a dropped
// response stream it reconnects, e.g. message "Reconnecting... 1/5",
// additionalDetails the transport error, codexErrorInfo
// {"responseStreamDisconnected":{"httpStatusCode":null}}).
type Retry struct {
	// Delay is the wait (cancellable) before it is sent, from the previous
	// one or the commentary.
	Delay   time.Duration `json:"delay,omitempty"`
	Message string        `json:"message"`
	Details string        `json:"details,omitempty"`
	Info    any           `json:"info,omitempty"` // codexErrorInfo
}

// AwaitResult is the brain's final answer for a turn (POST /await?seq=N
// blocks until the behaviour resolves; closing the request cancels it).
type AwaitResult struct {
	Outcome  string `json:"outcome"` // answered | failed | cancelled
	Answer   string `json:"answer,omitempty"`
	Error    string `json:"error,omitempty"`
	Capacity bool   `json:"capacity,omitempty"`
	// RateLimited fails the turn the way Codex reports provider 429s after
	// its retries (codexErrorInfo responseTooManyFailedAttempts).
	RateLimited bool `json:"rateLimited,omitempty"`
}

// EndRequest reports an outcome the brain cannot see by itself
// (POST /end?seq=N), e.g. "crashed".
type EndRequest struct {
	Outcome string `json:"outcome"`
	Error   string `json:"error,omitempty"`
}
