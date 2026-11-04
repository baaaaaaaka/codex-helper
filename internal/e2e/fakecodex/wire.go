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
}

// Plan is the brain's answer to a BeginRequest.
type Plan struct {
	Seq int `json:"seq"`
	// RejectStart, if set, is returned as the JSON-RPC error of turn/start.
	RejectStart    string `json:"rejectStart,omitempty"`
	Commentary     string `json:"commentary,omitempty"`
	Crash          bool   `json:"crash,omitempty"`
	HoldCompletion bool   `json:"holdCompletion,omitempty"`
	SurviveHelper  bool   `json:"surviveHelper,omitempty"`
}

// AwaitResult is the brain's final answer for a turn (POST /await?seq=N
// blocks until the behaviour resolves; closing the request cancels it).
type AwaitResult struct {
	Outcome  string `json:"outcome"` // answered | failed | cancelled
	Answer   string `json:"answer,omitempty"`
	Error    string `json:"error,omitempty"`
	Capacity bool   `json:"capacity,omitempty"`
}

// EndRequest reports an outcome the brain cannot see by itself
// (POST /end?seq=N), e.g. "crashed".
type EndRequest struct {
	Outcome string `json:"outcome"`
	Error   string `json:"error,omitempty"`
}
