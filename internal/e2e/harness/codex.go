package harness

import (
	"context"
	"errors"
	"fmt"
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
	Prompt    string
	Tokens    []string // E2E tokens found in the prompt
	Started   time.Time
	Ended     time.Time
	Outcome   string // "running", "answered", "failed", "cancelled"
	Answer    string
	Err       string
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
}

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
	changed chan struct{}
}

// NewFakeCodex returns a fake that answers every call immediately.
func NewFakeCodex() *FakeCodex {
	return &FakeCodex{gates: map[int]chan struct{}{}, release: map[int]bool{}, changed: make(chan struct{})}
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

// Invoke runs one call. Adapters call it from their Executor/app-server.
func (f *FakeCodex) Invoke(ctx context.Context, call CodexCall) (CodexCall, error) {
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
	finish := func(outcome, answer string, err error) (CodexCall, error) {
		f.mu.Lock()
		rec.Ended = time.Now()
		rec.Outcome = outcome
		rec.Answer = answer
		if err != nil {
			rec.Err = err.Error()
		}
		f.notifyLocked()
		out := *rec
		f.mu.Unlock()
		return out, err
	}
	if behavior.Delay > 0 {
		select {
		case <-time.After(behavior.Delay):
		case <-ctx.Done():
			return finish("cancelled", "", ctx.Err())
		}
	}
	if behavior.Block {
		select {
		case <-gate:
		case <-ctx.Done():
			return finish("cancelled", "", ctx.Err())
		}
	}
	if ctx.Err() != nil {
		return finish("cancelled", "", ctx.Err())
	}
	if behavior.Err != nil {
		return finish("failed", "", behavior.Err)
	}
	answer := behavior.Answer
	if answer == "" {
		answer = DefaultAnswer(snapshot)
	}
	return finish("answered", answer, nil)
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

// Calls returns a snapshot of all calls.
func (f *FakeCodex) Calls() []CodexCall {
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
