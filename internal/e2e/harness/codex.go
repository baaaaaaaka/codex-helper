package harness

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"time"
)

// CodexCall is one Codex invocation as the fake Codex saw it. It is the
// SUT-agnostic record; adapters fill what their runtime exposes.
type CodexCall struct {
	Seq       int
	Instance  string
	Kind      string // "work" or "control"
	SessionID string
	ChatID    string
	Cwd       string
	ThreadID  string
	TurnID    string
	ClientID  string // turn/start clientUserMessageId (app-server only)
	Prompt    string
	Images    []string // local image paths passed as input items
	Tokens    []string // E2E tokens found in the prompt
	Started   time.Time
	Ended     time.Time
	// Outcome is one of
	//   running    still in progress
	//   answered   final answer produced
	//   failed     turn failed (scripted error)
	//   cancelled  interrupted by the helper (turn/interrupt), or the
	//              in-process caller's ctx ended
	//   rejected   turn/start refused (active writer)
	//   crashed    scripted app-server crash mid-turn
	//   abandoned  app-server saw stdin EOF (helper died or closed it)
	//   killed     app-server process vanished mid-turn (e.g. SIGKILLed by
	//              the helper's runner on shutdown)
	Outcome string
	Answer  string
	Err     string
	// Pid is the fake app-server process that ran the call (0 in-process).
	Pid int
}

// HasToken reports whether the prompt contained token.
func (c CodexCall) HasToken(token string) bool {
	for _, t := range c.Tokens {
		if t == token {
			return true
		}
	}
	return false
}

// CodexBehavior is what the fake Codex does for one call.
type CodexBehavior struct {
	Answer string        // final answer text; empty = default answer
	Err    error         // return this error instead of an answer
	Block  bool          // wait for Release/ReleaseAll or ctx cancellation
	Delay  time.Duration // sleep (cancellable) before answering

	// The fields below are honoured by the fake app-server binary only
	// (the in-process Executor maps FailKind to an error and ignores the
	// rest).

	// Commentary is streamed as a commentary agent message before the final.
	Commentary string
	// FailKind selects how Err is surfaced: "" (turn fails with Err's
	// message), FailCapacity (turn fails with the model-capacity error) or
	// FailActiveWriter (turn/start is rejected: another process holds the
	// thread writer, as when the TUI has the session open).
	FailKind string
	// Crash makes the app-server process exit right after the turn started.
	Crash bool
	// HoldCompletion: the turn completes inside Codex (persisted, rollout
	// written) but turn/completed is never sent ("lost callback").
	HoldCompletion bool
	// SurviveHelper keeps the turn running when the helper goes away (stdin
	// EOF); the app-server exits only after the turn finished, so the result
	// is persisted while no helper is listening. Default: the app-server
	// exits on stdin EOF and the turn is lost (interrupted).
	SurviveHelper bool
}

// FailKind values.
const (
	FailCapacity     = "capacity"
	FailActiveWriter = "active-writer"
)

// TokenPattern matches the unique tokens scenarios embed in prompts.
var TokenPattern = regexp.MustCompile(`E2E[A-Z0-9]*-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*`)

// FakeCodex is a scripted, recording stand-in for Codex.
type FakeCodex struct {
	mu      sync.Mutex
	calls   []*CodexCall
	script  func(CodexCall) CodexBehavior
	gates   map[int]chan struct{}
	release map[int]bool
	all     bool

	behaviors map[int]CodexBehavior
	server    *httptest.Server
	changed   chan struct{}
}

// NewFakeCodex returns a fake that answers every call immediately.
func NewFakeCodex() *FakeCodex {
	return &FakeCodex{gates: map[int]chan struct{}{}, release: map[int]bool{}, behaviors: map[int]CodexBehavior{}, changed: make(chan struct{})}
}

// SetScript installs the behaviour function. nil restores the default.
func (f *FakeCodex) SetScript(fn func(CodexCall) CodexBehavior) {
	f.mu.Lock()
	f.script = fn
	f.mu.Unlock()
}

// DefaultAnswer is the answer used when the script gives none.
func DefaultAnswer(c CodexCall) string {
	tok := "no-token"
	if len(c.Tokens) > 0 {
		tok = c.Tokens[len(c.Tokens)-1]
	}
	return fmt.Sprintf("E2EANSWER-%d answer for %s", c.Seq, tok)
}

// Invoke runs one call. The in-process Executor adapter calls it directly;
// the fake app-server reaches the same logic through Handler (begin + await).
func (f *FakeCodex) Invoke(ctx context.Context, call CodexCall) (CodexCall, error) {
	seq, behavior := f.begin(call)
	if behavior.FailKind == FailActiveWriter {
		return f.finishSeq(seq, "rejected", "", behaviorErr(behavior))
	}
	return f.await(ctx, seq, behavior)
}

func behaviorErr(b CodexBehavior) error {
	if b.Err != nil {
		return b.Err
	}
	switch b.FailKind {
	case FailCapacity:
		return errors.New("Selected model is at capacity. Please try a different model.")
	case FailActiveWriter:
		return errors.New("thread is in use by another Codex process (active writer)")
	}
	return nil
}

// begin records a call and returns its sequence number and behaviour.
func (f *FakeCodex) begin(call CodexCall) (int, CodexBehavior) {
	f.mu.Lock()
	call.Seq = len(f.calls) + 1
	call.Started = time.Now()
	call.Outcome = "running"
	call.Tokens = uniqueTokens(call.Prompt)
	rec := &call
	f.calls = append(f.calls, rec)
	script := f.script
	gate := make(chan struct{})
	f.gates[call.Seq] = gate
	if f.all || f.release[call.Seq] {
		close(gate)
	}
	f.notifyLocked()
	snapshot := *rec
	f.mu.Unlock()
	behavior := CodexBehavior{}
	if script != nil {
		behavior = script(snapshot)
	}
	f.mu.Lock()
	f.behaviors[call.Seq] = behavior
	f.mu.Unlock()
	return call.Seq, behavior
}

func (f *FakeCodex) finishSeq(seq int, outcome, answer string, err error) (CodexCall, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if seq < 1 || seq > len(f.calls) {
		return CodexCall{}, fmt.Errorf("fake codex: unknown call %d", seq)
	}
	rec := f.calls[seq-1]
	if rec.Outcome != "running" {
		// First terminal outcome wins (e.g. "crashed" reported before the
		// await connection dropped).
		return *rec, err
	}
	rec.Ended = time.Now()
	rec.Outcome = outcome
	rec.Answer = answer
	if err != nil {
		rec.Err = err.Error()
	}
	f.notifyLocked()
	return *rec, err
}

// await applies the behaviour of call seq: delay, block, then answer or fail.
// A cancelled ctx ends the call as "cancelled" (in-process Invoke).
func (f *FakeCodex) await(ctx context.Context, seq int, behavior CodexBehavior) (CodexCall, error) {
	return f.awaitMode(ctx, seq, behavior, true)
}

// awaitMode is await; with finishOnCancel false a cancelled ctx leaves the
// call open (the app-server reports interrupts and exits explicitly, and a
// vanished process is detected by reapDead).
func (f *FakeCodex) awaitMode(ctx context.Context, seq int, behavior CodexBehavior, finishOnCancel bool) (CodexCall, error) {
	f.mu.Lock()
	gate := f.gates[seq]
	snapshot := *f.calls[seq-1]
	f.mu.Unlock()
	if behavior.Delay > 0 {
		select {
		case <-time.After(behavior.Delay):
		case <-ctx.Done():
			return f.cancelled(seq, ctx.Err(), finishOnCancel)
		}
	}
	if behavior.Block {
		select {
		case <-gate:
		case <-ctx.Done():
			return f.cancelled(seq, ctx.Err(), finishOnCancel)
		}
	}
	if ctx.Err() != nil {
		return f.cancelled(seq, ctx.Err(), finishOnCancel)
	}
	if err := behaviorErr(behavior); err != nil {
		return f.finishSeq(seq, "failed", "", err)
	}
	answer := behavior.Answer
	if answer == "" {
		answer = DefaultAnswer(snapshot)
	}
	return f.finishSeq(seq, "answered", answer, nil)
}

func uniqueTokens(prompt string) []string {
	seen := map[string]bool{}
	var out []string
	for _, tok := range TokenPattern.FindAllString(prompt, -1) {
		if strings.HasPrefix(tok, "E2EANSWER") || seen[tok] {
			continue
		}
		seen[tok] = true
		out = append(out, tok)
	}
	return out
}

func (f *FakeCodex) notifyLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}

// Release unblocks call seq (now or when it arrives).
func (f *FakeCodex) Release(seq int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.release[seq] {
		return
	}
	f.release[seq] = true
	if g, ok := f.gates[seq]; ok {
		select {
		case <-g:
		default:
			close(g)
		}
	}
}

// ReleaseAll unblocks every current and future call.
func (f *FakeCodex) ReleaseAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.all = true
	for seq, g := range f.gates {
		f.release[seq] = true
		select {
		case <-g:
		default:
			close(g)
		}
	}
}

func (f *FakeCodex) cancelled(seq int, err error, finish bool) (CodexCall, error) {
	if finish {
		return f.finishSeq(seq, "cancelled", "", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return *f.calls[seq-1], err
}

// Calls returns a snapshot of all calls.
func (f *FakeCodex) Calls() []CodexCall {
	f.reapDead()
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]CodexCall, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, *c)
	}
	return out
}

// CallsWithToken returns calls whose prompt contained token.
func (f *FakeCodex) CallsWithToken(token string) []CodexCall {
	var out []CodexCall
	for _, c := range f.Calls() {
		if c.HasToken(token) {
			out = append(out, c)
		}
	}
	return out
}

// ErrScripted is a generic scripted Codex failure.
var ErrScripted = errors.New("e2e scripted codex failure")
