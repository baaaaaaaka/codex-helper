package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/internal/leakcheck"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func TestMain(m *testing.M) { leakcheck.Main(m) }

// fakeAgent is a scripted Codex.
type fakeAgent struct {
	mu      sync.Mutex
	runs    []TurnRequest
	threads int
	block   map[string]chan struct{} // text substring -> release
	outcome func(TurnRequest) (TurnOutcome, error)
	lookup  map[string]TurnOutcome // turn key -> outcome
	irq     map[int64]chan struct{}
}

func newAgent() *fakeAgent {
	return &fakeAgent{block: map[string]chan struct{}{}, lookup: map[string]TurnOutcome{}, irq: map[int64]chan struct{}{}}
}

func (a *fakeAgent) Prepare(context.Context) error { return nil }

func (a *fakeAgent) Run(ctx context.Context, req TurnRequest, h TurnHooks) (TurnOutcome, error) {
	a.mu.Lock()
	a.runs = append(a.runs, req)
	n := len(a.runs)
	if req.ThreadID == "" {
		a.threads++
		req.ThreadID = fmt.Sprintf("thread-%d", a.threads)
		a.mu.Unlock()
		if err := h.ThreadStarted(req.ThreadID); err != nil {
			return TurnOutcome{NotStarted: true, Status: StatusFailed, Error: err.Error()}, nil
		}
		a.mu.Lock()
	}
	var wait chan struct{}
	for k, ch := range a.block {
		if strings.Contains(req.Text, k) {
			wait = ch
		}
	}
	irq := make(chan struct{})
	a.irq[req.ConvID] = irq
	out := a.outcome
	a.mu.Unlock()
	h.Started(fmt.Sprintf("turn-%d", n))
	if wait != nil {
		select {
		case <-wait:
		case <-irq:
			return TurnOutcome{CodexTurnID: fmt.Sprintf("turn-%d", n), Status: StatusInterrupted}, nil
		case <-ctx.Done():
			return TurnOutcome{}, ctx.Err()
		}
	}
	if out != nil {
		return out(req)
	}
	return TurnOutcome{CodexTurnID: fmt.Sprintf("turn-%d", n), Status: StatusCompleted, Final: "ANSWER to " + req.Text}, nil
}

func (a *fakeAgent) Lookup(_ context.Context, _ int64, _, key string) (TurnOutcome, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	o, ok := a.lookup[key]
	return o, ok, nil
}

func (a *fakeAgent) Interrupt(_ context.Context, conv int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if ch := a.irq[conv]; ch != nil {
		close(ch)
		delete(a.irq, conv)
	}
	return nil
}

func (a *fakeAgent) Kill(int64) {}
func (a *fakeAgent) Fork(context.Context, int64, string, string, string) (string, error) {
	return "", errors.New("no fork")
}
func (a *fakeAgent) Models(context.Context) ([]Model, error) { return nil, nil }
func (a *fakeAgent) Maintain()                               {}
func (a *fakeAgent) Close()                                  {}

func (a *fakeAgent) calls(sub string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, r := range a.runs {
		if strings.Contains(r.Text, sub) {
			n++
		}
	}
	return n
}

type rig struct {
	t      *testing.T
	ch     *channeltest.Fake
	ag     *fakeAgent
	st     *store.Store
	path   string
	cancel context.CancelFunc
	done   chan error
	log    *strings.Builder
	logMu  *sync.Mutex
	tweak  func(*Config)
}

type lockedWriter struct {
	mu *sync.Mutex
	b  *strings.Builder
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func newRig(t *testing.T, tweak ...func(*Config)) *rig {
	r := &rig{t: t, ch: channeltest.New(), ag: newAgent(), path: filepath.Join(t.TempDir(), "core.db"), log: &strings.Builder{}, logMu: &sync.Mutex{}}
	r.tweak = func(c *Config) {
		for _, f := range tweak {
			f(c)
		}
	}
	t.Cleanup(func() {
		r.stop()
		if t.Failed() {
			r.logMu.Lock()
			t.Logf("engine log:\n%s", r.log.String())
			r.logMu.Unlock()
		}
	})
	r.start()
	return r
}

func (r *rig) start() {
	st, err := store.Open(r.path)
	if err != nil {
		r.t.Fatal(err)
	}
	if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		r.t.Fatal(err)
	}
	r.st = st
	cfg := Config{Store: st, Channel: r.ch, Agent: r.ag, Timing: FastTiming(20 * time.Millisecond),
		Machine: "m", Log: lockedWriter{r.logMu, r.log}}
	r.tweak(&cfg)
	e, err := New(cfg)
	if err != nil {
		r.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel, r.done = cancel, make(chan error, 1)
	go func() { r.done <- e.Run(ctx) }()
}

func (r *rig) stop() {
	if r.cancel == nil {
		return
	}
	r.cancel()
	<-r.done
	r.st.Release()
	r.st.Close()
	r.cancel = nil
}

func (r *rig) wait(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.t.Fatalf("timed out waiting for %s", what)
}

// chatOf returns the chat id of the conversation with label.
func (r *rig) chatOf(label string) string {
	convs, err := r.st.Conversations()
	if err != nil {
		r.t.Fatal(err)
	}
	for _, c := range convs {
		if c.Label == label {
			return c.ExtChatID
		}
	}
	return ""
}

// own returns the helper's messages in chat containing sub.
func (r *rig) own(chat, sub string) []channel.InboundMsg {
	var out []channel.InboundMsg
	for _, m := range r.ch.Messages(chat) {
		if m.Marker != "" && strings.Contains(m.Text, sub) {
			out = append(out, m)
		}
	}
	return out
}

func (r *rig) control() string {
	var id string
	r.wait("control chat", func() bool {
		id = r.chatOf("control")
		return id != "" && len(r.own(id, "control chat is ready")) == 1
	})
	return id
}

func (r *rig) newWork(dir string) string {
	ctl := r.control()
	n := len(r.own(ctl, "Work chat created"))
	r.ch.PostUser(ctl, "new "+dir)
	r.wait("work chat", func() bool { return len(r.own(ctl, "Work chat created")) > n })
	convs, _ := r.st.Conversations()
	work := convs[len(convs)-1].ExtChatID
	r.wait("work chat ready", func() bool { return len(r.own(work, "Work chat is ready")) == 1 })
	return work
}

func TestPromptIsAckedAnsweredOnceAndControlMentioned(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "hello one")
	r.ch.PostUser(work, "hello two")
	r.wait("answers", func() bool {
		return len(r.own(work, "ANSWER to hello one")) == 1 && len(r.own(work, "ANSWER to hello two")) == 1
	})
	time.Sleep(200 * time.Millisecond)
	if n := len(r.own(work, "Request accepted")); n != 2 {
		t.Fatalf("%d acks", n)
	}
	if r.ag.calls("hello one") != 1 || r.ag.calls("hello two") != 1 {
		t.Fatalf("calls %+v", r.ag.runs)
	}
	// Order: answer one before answer two; same thread for both.
	msgs := r.ch.Messages(work)
	i1, i2 := -1, -1
	for i, m := range msgs {
		if strings.Contains(m.Text, "ANSWER to hello one") {
			i1 = i
		}
		if strings.Contains(m.Text, "ANSWER to hello two") {
			i2 = i
		}
	}
	if i1 < 0 || i2 < i1 {
		t.Fatalf("order %d %d", i1, i2)
	}
	if r.ag.runs[1].ThreadID != "thread-1" {
		t.Fatalf("second turn on %q", r.ag.runs[1].ThreadID)
	}
	// Exactly one owner notification per final: a control-chat mention.
	ctl := r.chatOf("control")
	r.wait("mentions", func() bool { return len(r.own(ctl, "Codex finished")) == 2 })
	for _, m := range r.own(ctl, "Codex finished") {
		if !m.MentionsOwner {
			t.Fatalf("notification without mention: %q", m.Text)
		}
	}
	for _, m := range r.own(work, "ANSWER") {
		if m.MentionsOwner {
			t.Fatal("final carries the mention")
		}
	}
}

func TestDuplicatePromptIgnoredAndRetry(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "same thing")
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to same thing")) == 1 })
	r.ch.PostUser(work, "same  THING")
	r.wait("duplicate notice", func() bool { return len(r.own(work, "Duplicate request ignored")) == 1 })
	if r.ag.calls("same") != 1 {
		t.Fatal("duplicate ran")
	}
	r.ch.PostUser(work, "helper retry")
	r.wait("retry", func() bool { return r.ag.calls("same") == 2 })
}

func TestCancelRunningTurn(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	release := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.block["slow"] = release
	r.ag.mu.Unlock()
	defer close(release)
	r.ch.PostUser(work, "slow job")
	r.wait("running", func() bool { return r.ag.calls("slow") == 1 })
	r.ch.PostUser(work, "helper cancel last")
	r.wait("cancelled", func() bool { return len(r.own(work, "Request cancelled.")) == 1 })
	r.ch.PostUser(work, "next one")
	r.wait("next", func() bool { return len(r.own(work, "ANSWER to next one")) == 1 })
	if len(r.own(work, "ANSWER to slow")) != 0 {
		t.Fatal("late answer")
	}
}

func TestRestartReconcilesInterruptedTurnWithOneNotice(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	release := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.block["long"] = release
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "long job")
	r.wait("running", func() bool { return r.ag.calls("long") == 1 })
	var key string
	r.wait("turn running in store", func() bool {
		turns, _ := r.st.OpenTurns()
		if len(turns) == 1 && turns[0].State == store.TurnRunning {
			key = turns[0].TurnKey
			return true
		}
		return false
	})
	if !strings.HasPrefix(key, "cxp-") {
		t.Fatalf("turn key %q", key)
	}
	r.stop()
	close(release)
	r.ag.mu.Lock()
	r.ag.lookup[key] = TurnOutcome{Status: StatusInterrupted}
	r.ag.mu.Unlock()
	r.start()
	r.wait("interruption notice", func() bool { return len(r.own(work, "interrupted when the helper stopped")) == 1 })
	r.ch.PostUser(work, "after restart")
	r.wait("next answer", func() bool { return len(r.own(work, "ANSWER to after restart")) == 1 })
	time.Sleep(200 * time.Millisecond)
	if n := len(r.own(work, "interrupted")); n != 1 {
		t.Fatalf("%d notices", n)
	}
	if r.ag.calls("long") != 1 {
		t.Fatal("interrupted turn re-run")
	}
	last := r.ag.runs[len(r.ag.runs)-1]
	if last.ThreadID != "thread-1" {
		t.Fatalf("thread after restart %q", last.ThreadID)
	}
}

func TestBreakerHoldsPromptsAndUnpauseDoesNotRunThem(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	// Marker-less copies of our own messages (a bridge or another tool
	// echoing them) count toward the breaker.
	r.ch.PostUser(work, "first prompt")
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to first prompt")) == 1 })
	var ours []string
	for _, m := range r.ch.Messages(work) {
		if m.Marker != "" {
			ours = append(ours, m.Text)
		}
	}
	for _, text := range ours[len(ours)-3:] {
		r.ch.PostUser(work, text)
	}
	r.wait("pause notice", func() bool { return len(r.own(work, "Paused auto-start")) == 1 })
	r.ch.PostUser(work, "held prompt")
	r.wait("held ack", func() bool { return len(r.own(work, "Request held")) == 1 })
	r.ch.PostUser(work, "helper unpause")
	r.wait("unpaused", func() bool { return len(r.own(work, "Resumed:")) == 1 })
	r.ch.PostUser(work, "fresh prompt")
	r.wait("fresh answer", func() bool { return len(r.own(work, "ANSWER to fresh prompt")) == 1 })
	if r.ag.calls("held prompt") != 0 {
		t.Fatal("held prompt ran after unpause")
	}
	if n := len(r.own(work, "Paused auto-start")); n != 1 {
		t.Fatalf("%d pause notices", n)
	}
}

func TestUnknownSendResolvedByReadBack(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	var once sync.Once
	r.ch.SendFault = func(chat string, p channel.Part) (bool, error) {
		if strings.Contains(p.Body, "ANSWER") {
			fail := false
			once.Do(func() { fail = true })
			if fail {
				return true, &channel.Ambiguous{Err: errors.New("response lost")}
			}
		}
		return false, nil
	}
	r.ch.PostUser(work, "lost reply")
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to lost reply")) == 1 })
	r.wait("resolved", func() bool {
		rows, _ := r.st.UnknownOutbox()
		return len(rows) == 0
	})
	r.ch.PostUser(work, "second")
	r.wait("second answer", func() bool { return len(r.own(work, "ANSWER to second")) == 1 })
	if n := len(r.own(work, "ANSWER to lost reply")); n != 1 {
		t.Fatalf("%d copies", n)
	}
}

func TestUserErrorHidesInternalIDs(t *testing.T) {
	in := "thread 019e2e00-1234-7abc-8def-0123456789ab already has an active writer at /home/u/.codex/sessions/2026/x.jsonl in 19:meeting_abc@thread.v2"
	got := userError(in)
	for _, bad := range []string{"019e2e00", ".codex", "19:meeting"} {
		if strings.Contains(got, bad) {
			t.Fatalf("%q leaks %q", got, bad)
		}
	}
	if !strings.Contains(got, "active writer") {
		t.Fatalf("%q lost the message", got)
	}
}

func TestRegistryInboxCreatedOnceSilentAndNeverRuns(t *testing.T) {
	r := newRig(t, func(c *Config) { c.Registry = true })
	r.control()
	var inbox string
	r.wait("inbox chat", func() bool { inbox = r.chatOf("inbox"); return inbox != "" })
	if info, _ := r.ch.Chat(inbox); info.Title != "CXP Machine Inbox m" {
		t.Fatalf("inbox title %q", info.Title)
	}
	r.ch.PostUser(inbox, "please run this")
	time.Sleep(1500 * time.Millisecond) // > one safety read in fast timing
	if n := len(r.ch.Messages(inbox)); n != 1 {
		t.Fatalf("inbox has %d messages, want only the user's", n)
	}
	if r.ag.calls("please run this") != 0 {
		t.Fatal("an inbox message started a turn")
	}
	if len(r.own(r.control(), "inbox")) != 0 {
		t.Fatal("the inbox was announced in the control chat")
	}
	r.stop()
	r.start()
	r.control()
	time.Sleep(200 * time.Millisecond)
	n := 0
	convs, _ := r.st.Conversations()
	for _, c := range convs {
		if c.Label == "inbox" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d inbox conversations after restart", n)
	}
}
