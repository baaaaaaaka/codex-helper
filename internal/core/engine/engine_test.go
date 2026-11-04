package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
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
	notes   []string // commentary streamed by turns whose text contains "progress"
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
	if strings.Contains(req.Text, "progress") && h.Progress != nil {
		a.mu.Lock()
		notes := append([]string(nil), a.notes...)
		a.mu.Unlock()
		for _, t := range notes {
			h.Progress(t + " (" + strings.SplitN(req.Text, "\n", 2)[0] + ")")
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
		var chats []string
		if t.Failed() && r.st != nil {
			if convs, err := r.st.Conversations(); err == nil {
				for _, c := range convs {
					chats = append(chats, c.Label+"="+c.ExtChatID)
					_ = r.st.Update(func(tx *store.Tx) error {
						rows, _ := tx.PendingOutbox(c.ID)
						for _, o := range rows {
							t.Logf("pending outbox conv=%d id=%d key=%s kind=%s state=%s next=%d", c.ID, o.ID, o.DeliveryKey, o.Kind, o.State, o.NextAttemptAt)
						}
						return nil
					})
				}
			}
		}
		r.stop()
		if t.Failed() {
			for _, lc := range chats {
				label, chat, _ := strings.Cut(lc, "=")
				for _, m := range r.ch.Messages(chat) {
					t.Logf("[%s] marker=%v own=%v %q", label, m.Marker != "", m.ByOwner, m.Text)
				}
			}
			r.ag.mu.Lock()
			for _, run := range r.ag.runs {
				t.Logf("run conv=%d thread=%q text=%.60q", run.ConvID, run.ThreadID, run.Text)
			}
			r.ag.mu.Unlock()
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
	r.waitFor(what, 10*time.Second, cond)
}

// waitFor is wait with a deadline of d; on timeout it also logs the
// engine's log.
func (r *rig) waitFor(what string, d time.Duration, cond func() bool) {
	r.t.Helper()
	start := time.Now()
	deadline := start.Add(d)
	var dumps []string
	nextDump := start.Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		if now := time.Now(); now.After(nextDump) && len(dumps) < 3 {
			var buf bytes.Buffer
			_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
			dumps = append(dumps, now.Format("15:04:05.000")+"\n"+buf.String())
			nextDump = now.Add(2 * time.Second)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, d := range dumps {
		r.t.Logf("goroutines during wait at %s", d)
	}
	r.logMu.Lock()
	r.t.Logf("engine log:\n%s", r.log.String())
	r.logMu.Unlock()
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
	// Exactly one owner notification per final: a control-chat mention
	// (two finals delivered close together may share one, F2).
	ctl := r.chatOf("control")
	covered := func() int {
		n := 0
		for _, m := range r.own(ctl, "Codex finished") {
			if strings.Contains(m.Text, "(2 answers)") {
				n += 2
			} else {
				n++
			}
		}
		return n
	}
	r.wait("mentions", func() bool { return covered() >= 2 })
	time.Sleep(500 * time.Millisecond)
	if n := covered(); n != 2 {
		t.Fatalf("mentions cover %d finals, want 2", n)
	}
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

// absentSends makes the first n sends whose body contains sub ambiguous
// without storing them (the channel never got the message), and records the
// time of every such send attempt.
func absentSends(r *rig, sub string, n int) func() []time.Time {
	var mu sync.Mutex
	var at []time.Time
	r.ch.SendFault = func(chat string, p channel.Part) (bool, error) {
		if !strings.Contains(p.Body, sub) {
			return false, nil
		}
		mu.Lock()
		defer mu.Unlock()
		at = append(at, time.Now())
		if len(at) <= n {
			return false, &channel.Ambiguous{Err: errors.New("connection reset")}
		}
		return false, nil
	}
	return func() []time.Time {
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Time(nil), at...)
	}
}

func quickResolve(c *Config) {
	c.Timing.ResolveDelay = 100 * time.Millisecond
	c.Timing.ResolveAbsent = time.Second
}

func TestAbsentFinalIsResentOnceAfterTheAbsenceWindow(t *testing.T) {
	r := newRig(t, quickResolve)
	work := r.newWork(t.TempDir())
	attempts := absentSends(r, "ANSWER to vanished", 1)
	r.ch.PostUser(work, "vanished")
	r.wait("answer resent", func() bool { return len(r.own(work, "ANSWER to vanished")) == 1 })
	at := attempts()
	if len(at) != 2 {
		t.Fatalf("%d send attempts, want 2", len(at))
	}
	if gap := at[1].Sub(at[0]); gap < time.Second {
		t.Fatalf("resent after %s, before the 1s absence window", gap)
	}
	r.ch.PostUser(work, "after")
	r.wait("next answer", func() bool { return len(r.own(work, "ANSWER to after")) == 1 })
	if n := len(r.own(work, "could not be confirmed")); n != 0 {
		t.Fatalf("%d unconfirmed notices for a resent answer", n)
	}
	if n := len(r.own(work, "ANSWER to vanished")); n != 1 || len(attempts()) != 2 {
		t.Fatalf("%d copies after %d attempts", n, len(attempts()))
	}
}

func TestAbsentResentFinalIsDroppedWithOneNotice(t *testing.T) {
	r := newRig(t, quickResolve)
	work := r.newWork(t.TempDir())
	attempts := absentSends(r, "ANSWER to twice", 2)
	r.ch.PostUser(work, "twice")
	r.wait("notice", func() bool { return len(r.own(work, "even after sending it a second time")) == 1 })
	r.ch.PostUser(work, "later")
	r.wait("next answer", func() bool { return len(r.own(work, "ANSWER to later")) == 1 })
	if n := len(attempts()); n != 2 {
		t.Fatalf("%d send attempts, want 2 (one resend only)", n)
	}
	if n := len(r.ch.Messages(work)); n == 0 {
		t.Fatal("no messages")
	}
	for _, m := range r.ch.Messages(work) {
		if strings.Contains(m.Text, "ANSWER to twice") {
			t.Fatalf("absent answer appeared: %q", m.Text)
		}
	}
	if n := len(r.own(work, "could not be confirmed")); n != 1 {
		t.Fatalf("%d notices", n)
	}
}

func TestAbsentAckIsNotResent(t *testing.T) {
	r := newRig(t, quickResolve)
	work := r.newWork(t.TempDir())
	attempts := absentSends(r, "Request accepted", 1)
	r.ch.PostUser(work, "acked")
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to acked")) == 1 })
	r.wait("ack settled", func() bool {
		rows, _ := r.st.UnknownOutbox()
		return len(rows) == 0
	})
	r.ch.PostUser(work, "next")
	r.wait("next answer", func() bool { return len(r.own(work, "ANSWER to next")) == 1 })
	// The second prompt's ack is the second "Request accepted" send.
	if n := len(attempts()); n != 2 {
		t.Fatalf("%d ack sends, want 2 (the lost ack is not resent)", n)
	}
	if n := len(r.own(work, "could not be confirmed")); n != 0 {
		t.Fatalf("%d notices for a lost ack", n)
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

// fakeASR transcribes after release; fail makes it return an error.
type fakeASR struct {
	release chan struct{}
	fail    bool
	mu      sync.Mutex
	paths   []string
}

func (a *fakeASR) Transcribe(ctx context.Context, path string) (string, error) {
	a.mu.Lock()
	a.paths = append(a.paths, path)
	a.mu.Unlock()
	select {
	case <-a.release:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if a.fail {
		return "", errors.New("asr: model crashed at /secret/path")
	}
	return "  hello from voice  ", nil
}

func TestVoiceTurnWaitsForTranscript(t *testing.T) {
	asr := &fakeASR{release: make(chan struct{})}
	files := t.TempDir()
	r := newRig(t, func(c *Config) { c.Transcriber = asr; c.FilesDir = files })
	work := r.newWork(t.TempDir())
	ref := r.ch.AddFile("voice.ogg", []byte("OggS..."))
	ref.Kind = channel.FileVoice
	r.ch.PostUser(work, "", ref)
	r.wait("transcription started", func() bool { asr.mu.Lock(); defer asr.mu.Unlock(); return len(asr.paths) == 1 })
	time.Sleep(300 * time.Millisecond)
	r.ag.mu.Lock()
	n := len(r.ag.runs)
	r.ag.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d turns started before the transcript was ready", n)
	}
	close(asr.release)
	r.wait("voice answered", func() bool { return r.ag.calls(voiceLead+"\nhello from voice") == 1 })
	r.wait("voice answer posted", func() bool { return len(r.own(work, "ANSWER to Voice message")) == 1 })
}

func TestVoiceTranscriptionFailureStillRunsWithoutLeaks(t *testing.T) {
	asr := &fakeASR{release: make(chan struct{}), fail: true}
	close(asr.release)
	r := newRig(t, func(c *Config) { c.Transcriber = asr; c.FilesDir = t.TempDir() })
	work := r.newWork(t.TempDir())
	ref := r.ch.AddFile("voice.ogg", []byte("OggS..."))
	ref.Kind = channel.FileVoice
	r.ch.PostUser(work, "see voice", ref)
	r.wait("turn ran", func() bool { return r.ag.calls("could not be transcribed") == 1 })
	r.ag.mu.Lock()
	text := r.ag.runs[len(r.ag.runs)-1].Text
	r.ag.mu.Unlock()
	if strings.Contains(text, "/secret/path") || !strings.HasPrefix(text, "see voice") {
		t.Fatalf("turn text %q", text)
	}
}

// With a short account read budget the control chat (polled every tick in
// fast timing) must not starve a work chat: due reads go most overdue first.
func TestReadBudgetSharedFairlyWithControlChat(t *testing.T) {
	r := newRig(t, func(c *Config) { c.ReadRate = 1 })
	work := r.newWork(t.TempDir())
	start := time.Now()
	for i := 0; i < 3; i++ {
		r.ch.PostUser(work, fmt.Sprintf("fair %d", i))
		r.wait("fair answer", func() bool { return len(r.own(work, fmt.Sprintf("ANSWER to fair %d", i))) == 1 })
	}
	if d := time.Since(start); d > 15*time.Second {
		t.Fatalf("three prompts took %s at 1 read/s", d)
	}
}

// laterASR is not ready for the first calls (setup), then transcribes.
type laterASR struct {
	mu    sync.Mutex
	calls int
	ready int
}

func (a *laterASR) Transcribe(ctx context.Context, path string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	if a.calls <= a.ready {
		return "", fmt.Errorf("setup running: %w", ErrTranscribeLater)
	}
	return "voice after setup", nil
}

func TestVoiceWaitsForRecognizerSetupWithOneNotice(t *testing.T) {
	asr := &laterASR{ready: 2}
	r := newRig(t, func(c *Config) {
		c.Transcriber = asr
		c.FilesDir = t.TempDir()
		c.Timing.Maintenance = 300 * time.Millisecond // asr retry cadence
	})
	work := r.newWork(t.TempDir())
	ref := r.ch.AddFile("voice.ogg", []byte("OggS..."))
	ref.Kind = channel.FileVoice
	r.ch.PostUser(work, "", ref)
	r.wait("voice answered after setup", func() bool { return r.ag.calls("voice after setup") == 1 })
	if n := len(r.own(work, "Voice recognition is being set up")); n != 1 {
		t.Fatalf("%d setup notices, want 1", n)
	}
	r.ag.mu.Lock()
	n := len(r.ag.runs)
	r.ag.mu.Unlock()
	if n != 1 {
		t.Fatalf("%d turns, want 1", n)
	}
}

func TestProgressPostedInOrderBeforeFinal(t *testing.T) {
	r := newRig(t)
	r.ag.mu.Lock()
	r.ag.notes = []string{"step A", "step B", "step C"}
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "progress live")
	r.wait("final", func() bool { return len(r.own(work, "ANSWER to progress live")) == 1 })
	var order []string
	for _, m := range r.ch.Messages(work) {
		for _, k := range []string{"step A", "step B", "step C", "ANSWER to progress live"} {
			if m.Marker != "" && strings.Contains(m.Text, k) {
				order = append(order, k)
			}
		}
	}
	if strings.Join(order, ",") != "step A,step B,step C,ANSWER to progress live" {
		t.Fatalf("order %q", order)
	}
}

func TestProgressCoalescedWhenTurnsWaitBehind(t *testing.T) {
	r := newRig(t)
	r.ag.mu.Lock()
	r.ag.notes = []string{"step A", "step B", "step C"}
	r.ag.mu.Unlock()
	release := make(chan struct{})
	r.ag.block["progress first"] = release
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "progress first")
	r.ch.PostUser(work, "second request")
	r.wait("both admitted", func() bool { return len(r.own(work, "Request accepted")) == 2 })
	// The fake streams the first turn's commentary after the release, while
	// the second request waits behind it.
	close(release)
	r.wait("both answered", func() bool {
		return len(r.own(work, "ANSWER to progress first")) == 1 && len(r.own(work, "ANSWER to second request")) == 1
	})
	n := 0
	for _, m := range r.ch.Messages(work) {
		if m.Marker != "" && strings.Contains(m.Text, "step ") && strings.Contains(m.Text, "(progress first)") {
			n++
		}
	}
	if n > 1 {
		t.Fatalf("%d progress messages for a turn with another queued behind it, want at most 1", n)
	}
}

// Review H3: a backlog too large to read in order is skipped once with one
// notice that says what was skipped; the chat is not left deaf and the
// too-large walk is not repeated.
func TestTooLargeBacklogSkippedWithOneNotice(t *testing.T) {
	var paused atomic.Value
	paused.Store("")
	r := newRig(t, func(c *Config) {
		f := c.Channel.(*channeltest.Fake)
		f.MaxWindow = 6
		f.ReceiveFault = func(stream string) error {
			if stream == paused.Load().(string) {
				return errors.New("test: reads paused")
			}
			return nil
		}
	})
	work := r.newWork(t.TempDir())
	paused.Store(work)
	for i := 0; i < 10; i++ {
		r.ch.PostUser(work, fmt.Sprintf("burst %d", i))
	}
	paused.Store("")
	r.wait("skip notice", func() bool { return len(r.own(work, "Too many new messages")) == 1 })
	r.ch.PostUser(work, "later prompt")
	r.wait("later prompt answered", func() bool { return len(r.own(work, "ANSWER to later prompt")) == 1 })
	notes := r.own(work, "Too many new messages")
	if len(notes) != 1 {
		t.Fatalf("%d skip notices, want 1", len(notes))
	}
	if txt := notes[0].Text; !strings.Contains(txt, "at least 10 messages") || !strings.Contains(txt, "UTC") || !strings.Contains(txt, "Scroll up") {
		t.Errorf("notice does not say what was skipped and how to see it: %q", txt)
	}
	if n := r.ag.calls("burst"); n != 0 {
		t.Errorf("%d skipped messages were run", n)
	}
	r.logMu.Lock()
	walks := strings.Count(r.log.String(), "receive window larger")
	r.logMu.Unlock()
	if walks != 1 {
		t.Errorf("too-large window read %d times, want 1", walks)
	}
}

// Review H4: twelve chats with an outbox backlog share the write budget
// round robin; a backlog in low-id chats does not starve the others.
func TestSenderServesChatsRoundRobin(t *testing.T) {
	r := &rig{t: t, ch: channeltest.New(), ag: newAgent(), path: filepath.Join(t.TempDir(), "core.db"), log: &strings.Builder{}, logMu: &sync.Mutex{}}
	r.tweak = func(*Config) {}
	t.Cleanup(r.stop)
	ctx := context.Background()
	ctl, _ := r.ch.CreateChat(ctx, "control", "c")
	var chats []string
	for i := 0; i < 12; i++ {
		ci, _ := r.ch.CreateChat(ctx, "w", fmt.Sprintf("k%02d", i))
		chats = append(chats, ci.ID)
	}
	st, err := store.Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	err = st.Import(func(im *store.Importer) error {
		if _, err := im.Conversation(store.Conversation{Label: "control", Kind: store.KindControl, Channel: "teams", ExtChatID: ctl.ID, State: store.ConvActive}); err != nil {
			return err
		}
		for i, chat := range chats {
			id, err := im.Conversation(store.Conversation{Label: fmt.Sprintf("s%03d", i+1), Kind: store.KindWork, Channel: "teams", ExtChatID: chat, State: store.ConvActive})
			if err != nil {
				return err
			}
			if err := im.Stream(store.Stream{Key: store.ChatStreamKey("teams", chat), ConvID: id, Cursor: "0", Seeded: true}); err != nil {
				return err
			}
			for k := 0; k < 40; k++ {
				key := fmt.Sprintf("rr:%s:%02d", chat, k)
				if _, _, err := im.Outbox(store.Outbox{ConvID: id, DeliveryKey: key, Seq: int64(k + 1), Kind: store.OutNotice, Body: "backlog " + key, State: store.OutQueued, CreatedAt: im.Now()}, nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	r.start()
	counts := func() (got []int, sum int) {
		for _, c := range chats {
			n := len(r.own(c, "backlog"))
			got, sum = append(got, n), sum+n
		}
		return got, sum
	}
	r.wait("30 backlog posts", func() bool { _, sum := counts(); return sum >= 30 })
	got, _ := counts()
	lo, hi := got[0], got[0]
	for _, n := range got {
		lo, hi = min(lo, n), max(hi, n)
	}
	if lo < 2 || hi-lo > 2 {
		t.Fatalf("posts per chat after 30 sends: %v, want an even spread (each >= 2, spread <= 2)", got)
	}
}

// slowModels blocks Models until released (a Codex slow to start).
type slowModels struct {
	*fakeAgent
	release chan struct{}
}

func (a slowModels) Models(ctx context.Context) ([]Model, error) {
	select {
	case <-a.release:
	case <-ctx.Done():
	}
	return nil, errors.New("test: no models")
}

// Review M2: a slow command does not delay `helper cancel` in the same chat
// nor prompts and commands elsewhere.
func TestSlowCommandDoesNotBlockCancelOrOtherChats(t *testing.T) {
	release := make(chan struct{})
	r := newRig(t, func(c *Config) { c.Agent = slowModels{c.Agent.(*fakeAgent), release} })
	defer close(release)
	work := r.newWork(t.TempDir())
	ctl := r.control()
	r.ch.PostUser(work, "helper model list")
	time.Sleep(200 * time.Millisecond)
	r.ch.PostUser(work, "helper cancel")
	r.wait("cancel reply while model is stuck", func() bool { return len(r.own(work, "Nothing to cancel")) == 1 })
	r.ch.PostUser(ctl, "status")
	r.wait("control status while model is stuck", func() bool { return len(r.own(ctl, "sessions (")) == 1 })
	r.ch.PostUser(work, "prompt during model")
	r.wait("prompt answered while model is stuck", func() bool { return len(r.own(work, "ANSWER to prompt during model")) == 1 })
	if n := len(r.own(work, "did not list its models")); n != 0 {
		t.Fatalf("model command finished early (%d replies)", n)
	}
}

// R-19 (review M5): a silent Codex turn gets one hint at each silence
// threshold, then is stopped at the stall bound with exactly one notice,
// and the chat runs its next request.
func TestSilentTurnHintsThenStopsAtStallBound(t *testing.T) {
	r := newRig(t, func(c *Config) {
		c.Timing.SilenceHint, c.Timing.SilenceStuck, c.Timing.StallLimit = 400*time.Millisecond, 900*time.Millisecond, 1500*time.Millisecond
	})
	r.ag.mu.Lock()
	r.ag.block["hang forever"] = make(chan struct{})
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "hang forever")
	r.wait("stall notice", func() bool { return len(r.own(work, "so the helper stopped the request")) == 1 })
	r.ch.PostUser(work, "after the stall")
	r.wait("next request answered", func() bool { return len(r.own(work, "ANSWER to after the stall")) == 1 })
	for sub, want := range map[string]int{
		"Still working. No new Codex update yet": 1,
		"It may be stuck":                        1,
		"so the helper stopped the request":      1,
		"interrupted when the helper stopped":    0,
	} {
		if got := len(r.own(work, sub)); got != want {
			t.Errorf("%d notices with %q, want %d", got, sub, want)
		}
	}
}

// R-35 (review M5): a request queued behind a running one gets a reminder
// while it waits and one "starting" notice; a request that did not wait
// gets neither.
func TestQueuedTurnRemindedAndAnnouncedWhenStarting(t *testing.T) {
	r := newRig(t, func(c *Config) {
		c.Timing.QueueReminder, c.Timing.Maintenance = 500*time.Millisecond, 100*time.Millisecond
	})
	release := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.block["first job"] = release
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "first job")
	r.wait("first job running", func() bool { return r.ag.calls("first job") == 1 })
	r.ch.PostUser(work, "second job")
	r.wait("queued reminder", func() bool { return len(r.own(work, "Still waiting")) >= 1 })
	if n := len(r.own(work, "“second job”")); n == 0 {
		t.Errorf("reminder does not name the queued request")
	}
	close(release)
	r.wait("second job answered", func() bool { return len(r.own(work, "ANSWER to second job")) == 1 })
	// A single queued request is named at the head of its first message
	// (here the answer: the turn posts no progress), not in a notice.
	if n := len(r.own(work, "ueued request: “second job”")); n != 1 {
		t.Errorf("%d messages name the queued request as it starts, want 1", n)
	}
	if n := len(r.own(work, "▶️ Queued request: “second job”\n\nANSWER to second job")); n != 1 {
		t.Errorf("the answer of the queued request does not name it first")
	}
	if n := len(r.own(work, "ueued request: “first job”")); n != 0 {
		t.Errorf("%d starting notices for a request that did not wait", n)
	}
	r.ch.PostUser(work, "third job")
	r.wait("third job answered", func() bool { return len(r.own(work, "ANSWER to third job")) == 1 })
	if n := len(r.own(work, "“third job”")); n != 0 {
		t.Errorf("a request that did not wait got %d queue notices", n)
	}
}

// Benchmark F5: a backlog of queued requests in one chat starts with one
// notice listing them, and each later request is named at the head of its
// first message: at most two posts per request (its ACK and its answer)
// plus the one notice.
func TestQueuedBacklogStartsWithOneNotice(t *testing.T) {
	r := newRig(t)
	release := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.block["job 0"] = release
	r.ag.notes = []string{"step A"}
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	before := len(r.own(work, ""))
	r.ch.PostUser(work, "job 0")
	r.wait("job 0 running", func() bool { return r.ag.calls("job 0") == 1 })
	jobs := []string{"job 1", "job 2", "job 3 progress", "job 4"}
	for _, j := range jobs {
		r.ch.PostUser(work, j)
	}
	r.wait("queued ACKs", func() bool { return len(r.own(work, "queued behind")) == len(jobs) })
	close(release)
	for _, j := range append([]string{"job 0"}, jobs...) {
		j := j
		r.wait("answer to "+j, func() bool { return len(r.own(work, "ANSWER to "+j)) == 1 })
	}
	batch := r.own(work, "starting your 4 queued requests")
	if len(batch) != 1 {
		t.Fatalf("%d batch notices, want 1", len(batch))
	}
	for _, j := range jobs {
		if !strings.Contains(batch[0].Text, "“"+j+"”") {
			t.Errorf("batch notice does not list %q:\n%s", j, batch[0].Text)
		}
	}
	if n := len(r.own(work, "starting this queued request: “job 1”")) + len(r.own(work, "Queued request: “job 1”")); n != 0 {
		t.Errorf("the first request of the batch is named again in %d messages", n)
	}
	for _, j := range []string{"job 2", "job 4"} {
		if a := r.own(work, "ANSWER to "+j); !strings.Contains(a[0].Text, "▶️ Queued request: “"+j+"”\n\nANSWER to "+j) {
			t.Errorf("answer of %q does not name it first:\n%s", j, a[0].Text)
		}
	}
	if p := r.own(work, "step A (job 3 progress)"); len(p) != 1 || !strings.Contains(p[0].Text, "▶️ Codex is starting this queued request: “job 3 progress”\n\nstep A") {
		t.Errorf("first progress of a queued request does not name it: %v", p)
	} else if a := r.own(work, "ANSWER to job 3 progress"); strings.Contains(a[0].Text, "Queued request") {
		t.Errorf("answer names the request again after its progress did")
	}
	posts := len(r.own(work, "")) - before
	turns := 1 + len(jobs)
	// ACK + answer per request, one progress message, one batch notice.
	if max := 2*turns + 1 + 1; posts > max {
		var texts []string
		for _, m := range r.own(work, "")[before:] {
			texts = append(texts, m.Text)
		}
		t.Fatalf("%d posts for %d requests, want at most %d:\n%s", posts, turns, max, strings.Join(texts, "\n---\n"))
	}
}

// lagScanner is an activity scan whose list the test scripts.
type lagScanner struct {
	*channeltest.Fake
	mu    sync.Mutex
	acts  []channel.ChatActivity
	calls []time.Time
	reads []string
}

func (s *lagScanner) ScanActivity(_ context.Context, since time.Time) ([]channel.ChatActivity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, since)
	var out []channel.ChatActivity
	for _, a := range s.acts {
		if a.Last.After(since) {
			out = append(out, a)
		}
	}
	return out, nil
}

func (s *lagScanner) Receive(ctx context.Context, stream, cursor string) (channel.Window, error) {
	s.mu.Lock()
	s.reads = append(s.reads, stream)
	s.mu.Unlock()
	return s.Fake.Receive(ctx, stream, cursor)
}

func (s *lagScanner) set(acts ...channel.ChatActivity) {
	s.mu.Lock()
	s.acts = acts
	s.mu.Unlock()
}

// Review M6: a chat whose preview shows up in the scan behind a newer
// preview of another chat is still read at once, not at the safety read.
func TestActivityScanCatchesLaggingPreview(t *testing.T) {
	var sc *lagScanner
	r := newRig(t, func(c *Config) {
		sc = &lagScanner{Fake: c.Channel.(*channeltest.Fake)}
		c.Channel, c.ActivityScan = sc, true
		// One scan per second: every scan borrows one of the 4 read tokens/s.
		c.Timing.HotFor, c.Timing.SafetyRead, c.Timing.ScanInterval = 200*time.Millisecond, time.Minute, time.Second
	})
	work := r.newWork(t.TempDir())
	ctl := r.control()
	newest := time.Now()
	sc.set(channel.ChatActivity{Chat: ctl, LastID: "ctl-1", Last: newest})
	time.Sleep(time.Second) // the work chat goes idle; the scan watermark moves to newest
	m := r.ch.PostUser(work, "lagging prompt")
	sc.set(channel.ChatActivity{Chat: ctl, LastID: "ctl-1", Last: newest},
		channel.ChatActivity{Chat: work, LastID: m.ID, Last: newest.Add(-30 * time.Second)})
	t.Cleanup(func() {
		if t.Failed() {
			sc.mu.Lock()
			t.Logf("scans: %d, last since %v (newest %v); work %s; reads %v", len(sc.calls), sc.calls[len(sc.calls)-1], newest, work, sc.reads[max(0, len(sc.reads)-30):])
			sc.mu.Unlock()
		}
	})
	r.wait("lagging prompt answered", func() bool { return len(r.own(work, "ANSWER to lagging prompt")) == 1 })
}

// stormScanner logs the activity scans and chat reads in order and answers
// them with an account throttle while storm is set.
type stormScanner struct {
	*channeltest.Fake
	mu    sync.Mutex
	storm bool
	ops   []stormOp
}

type stormOp struct {
	what      string // "scan" or "read"
	at        time.Time
	throttled bool
}

func (s *stormScanner) op(what string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops = append(s.ops, stormOp{what: what, at: time.Now(), throttled: s.storm})
	if s.storm {
		return &channel.Throttled{RetryAfter: time.Second, Scope: channel.ScopeAccount, Err: errors.New("429 storm")}
	}
	return nil
}

func (s *stormScanner) ScanActivity(_ context.Context, _ time.Time) ([]channel.ChatActivity, error) {
	if err := s.op("scan"); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *stormScanner) Receive(ctx context.Context, stream, cursor string) (channel.Window, error) {
	if err := s.op("read"); err != nil {
		return channel.Window{}, err
	}
	return s.Fake.Receive(ctx, stream, cursor)
}

// Benchmark S1/S2: after an account read throttle the first request when
// the Retry-After ends is the activity scan (it serves every chat), and the
// chat reads then resume one at a time at the paced rate, not as a burst.
func TestReadThrottleReleasesScanFirstThenPacedReads(t *testing.T) {
	var sc *stormScanner
	r := newRig(t, func(c *Config) {
		sc = &stormScanner{Fake: c.Channel.(*channeltest.Fake)}
		c.Channel, c.ActivityScan = sc, true
		c.Timing.ScanInterval = 5 * time.Second // the scan after the gate is not a tick
	})
	r.control()
	time.Sleep(300 * time.Millisecond)
	sc.mu.Lock()
	sc.storm = true
	sc.mu.Unlock()
	r.wait("a throttled read", func() bool {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		for _, o := range sc.ops {
			if o.throttled {
				return true
			}
		}
		return false
	})
	time.Sleep(200 * time.Millisecond)
	sc.mu.Lock()
	sc.storm = false
	sc.mu.Unlock()
	time.Sleep(3 * time.Second)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	last := -1
	for i, o := range sc.ops {
		if o.throttled {
			last = i
		}
	}
	after := sc.ops[last+1:]
	if len(after) < 4 {
		t.Fatalf("only %d requests in the 3 s after the storm", len(after))
	}
	if after[0].what != "scan" {
		var seq []string
		for _, o := range after[:min(len(after), 6)] {
			seq = append(seq, o.what)
		}
		t.Fatalf("first requests after the read gate opened: %v, want the scan first", seq)
	}
	var reads []time.Time
	for _, o := range after[1:] {
		if o.what == "read" {
			reads = append(reads, o.at)
		}
	}
	// The read pacer stepped down to 3.75/s with a burst of one: about
	// 267 ms between reads (the control chat polls every 20 ms otherwise).
	for i := 1; i < len(reads) && i < 6; i++ {
		if gap := reads[i].Sub(reads[i-1]); gap < 200*time.Millisecond {
			t.Fatalf("reads %d and %d after the Retry-After only %s apart: a burst", i-1, i, gap)
		}
	}
}

// Benchmark S3: while the account's reads were throttled within the last
// five minutes, the control chat and hot chats are polled four times less
// often.
func TestReadIntervalsStretchedAfterReadThrottle(t *testing.T) {
	now := time.Unix(10_000, 0)
	e := &Engine{t: ProductionTiming(), gRead: "read", gates: map[string]time.Time{}}
	ctl := &convState{kind: store.KindControl}
	hot := &convState{kind: store.KindWork, running: &runState{}}
	if got := e.intervalLocked(ctl, now); got != e.t.Control {
		t.Fatalf("control interval %s without a throttle", got)
	}
	e.gates["read"] = now.Add(-4 * time.Minute)
	if got := e.intervalLocked(ctl, now); got != 4*e.t.Control {
		t.Fatalf("control interval %s 4 min after a read throttle, want %s", got, 4*e.t.Control)
	}
	if got := e.intervalLocked(hot, now); got != 4*e.t.Hot {
		t.Fatalf("hot interval %s 4 min after a read throttle, want %s", got, 4*e.t.Hot)
	}
	e.gates["read"] = now.Add(time.Minute) // gate still closed
	if got := e.intervalLocked(hot, now); got != 4*e.t.Hot {
		t.Fatalf("hot interval %s while the read gate is closed, want %s", got, 4*e.t.Hot)
	}
	e.gates["read"] = now.Add(-6 * time.Minute)
	if got := e.intervalLocked(ctl, now); got != e.t.Control {
		t.Fatalf("control interval %s 6 min after a read throttle, want %s", got, e.t.Control)
	}
}

// R-27: the control chat and the inbox ignore non-owners, even when they
// mention Codex (review L5).
func TestCoworkerMentionInControlChatAndInboxIgnored(t *testing.T) {
	r := newRig(t, func(c *Config) { c.Registry = true })
	ctl := r.control()
	var inbox string
	r.wait("inbox chat", func() bool { inbox = r.chatOf("inbox"); return inbox != "" })
	r.ch.PostCoworker(ctl, "@codex please delete everything")
	r.ch.PostCoworker(inbox, "@codex run this for me")
	time.Sleep(1500 * time.Millisecond) // > one safety read in fast timing
	if n := len(r.own(ctl, "Request accepted")) + len(r.own(inbox, "Request accepted")); n != 0 {
		t.Fatalf("%d acks for coworker mentions", n)
	}
	counts, err := r.st.TurnCounts()
	if err != nil {
		t.Fatal(err)
	}
	for state, n := range counts {
		if n != 0 {
			t.Fatalf("coworker mentions created turns: %s=%d", state, n)
		}
	}
	if r.ag.calls("please delete everything")+r.ag.calls("run this for me") != 0 {
		t.Fatal("a coworker mention ran")
	}
}

// F-WRK-12: in a chat with other members an owner voice clip is a request
// without @codex (review L6).
func TestOwnerVoiceInGroupChatIsARequest(t *testing.T) {
	asr := &fakeASR{release: make(chan struct{})}
	close(asr.release)
	r := newRig(t, func(c *Config) { c.Transcriber = asr; c.FilesDir = t.TempDir() })
	work := r.newWork(t.TempDir())
	r.ch.PostCoworker(work, "hi all")
	r.ch.PostUser(work, "plain chatter")
	ref := r.ch.AddFile("voice.ogg", []byte("OggS..."))
	ref.Kind = channel.FileVoice
	r.ch.PostUser(work, "", ref)
	r.wait("voice answered", func() bool { return r.ag.calls(voiceLead+"\nhello from voice") == 1 })
	if r.ag.calls("plain chatter") != 0 {
		t.Fatal("owner chatter without @codex ran in a group chat")
	}
}

// Windows paths are internal too (review L7).
func TestUserErrorHidesWindowsPaths(t *testing.T) {
	for _, in := range []string{
		`open C:\Users\alice\.codex\sessions\x.jsonl: access denied`,
		`spawn d:\tools\codex.exe failed: access denied`,
		`share \\fileserver\home\alice\work unreachable: access denied`,
	} {
		got := userError(in)
		for _, bad := range []string{"alice", "Users", "tools", "fileserver", "codex.exe"} {
			if strings.Contains(got, bad) {
				t.Fatalf("%q -> %q leaks %q", in, got, bad)
			}
		}
		if !strings.Contains(got, "access denied") {
			t.Fatalf("%q -> %q lost the message", in, got)
		}
	}
}

// concurrentProgress streams progress from two goroutines at once, as the
// app-server reader and the early-item replay can.
type concurrentProgress struct{ *fakeAgent }

func (a concurrentProgress) Run(ctx context.Context, req TurnRequest, h TurnHooks) (TurnOutcome, error) {
	if !strings.Contains(req.Text, "parallel") {
		return a.fakeAgent.Run(ctx, req, h)
	}
	if req.ThreadID == "" {
		if err := h.ThreadStarted("thread-p"); err != nil {
			return TurnOutcome{NotStarted: true, Status: StatusFailed, Error: err.Error()}, nil
		}
	}
	h.Started("turn-p")
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-gate
			for i := 0; i < 4; i++ {
				h.Progress(fmt.Sprintf("step %c%d", 'a'+g, i))
			}
		}(g)
	}
	close(gate)
	wg.Wait()
	return TurnOutcome{CodexTurnID: "turn-p", Status: StatusCompleted, Final: "ANSWER parallel"}, nil
}

// Concurrent progress callbacks each post their own message (review L3).
func TestConcurrentProgressCallbacksAllPosted(t *testing.T) {
	r := newRig(t, func(c *Config) { c.Agent = concurrentProgress{c.Agent.(*fakeAgent)} })
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "parallel please")
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER parallel")) == 1 })
	for g := 'a'; g <= 'b'; g++ {
		for i := 0; i < 4; i++ {
			step := fmt.Sprintf("step %c%d", g, i)
			if n := len(r.own(work, step)); n != 1 {
				t.Fatalf("%q posted %d times, want 1", step, n)
			}
		}
	}
}

// flakyAck fails the n-th "Request accepted" render once.
type flakyAck struct {
	*channeltest.Fake
	mu     sync.Mutex
	acks   int
	failAt int
}

func (f *flakyAck) Render(ctx context.Context, doc channel.RichDoc) ([]channel.Part, error) {
	f.mu.Lock()
	fail := false
	if strings.Contains(doc.Markdown, "Request accepted") {
		f.acks++
		fail = f.acks == f.failAt
	}
	f.mu.Unlock()
	if fail {
		return nil, errors.New("test: render failed")
	}
	return f.Fake.Render(ctx, doc)
}

// A failed admit transaction leaves no in-memory trace: on the retry the
// first prompt is not taken for a duplicate of itself (review L2).
func TestFailedAdmitDoesNotMarkPromptsAsDuplicates(t *testing.T) {
	var paused atomic.Value
	paused.Store("")
	r := newRig(t, func(c *Config) {
		f := c.Channel.(*channeltest.Fake)
		f.ReceiveFault = func(stream string) error {
			if stream == paused.Load().(string) {
				return errors.New("test: reads paused")
			}
			return nil
		}
		c.Channel = &flakyAck{Fake: f, failAt: 2}
	})
	work := r.newWork(t.TempDir())
	paused.Store(work)
	r.ch.PostUser(work, "first prompt")
	r.ch.PostUser(work, "second prompt")
	paused.Store("")
	r.wait("both answered", func() bool {
		return len(r.own(work, "ANSWER to first prompt")) == 1 && len(r.own(work, "ANSWER to second prompt")) == 1
	})
	if n := len(r.own(work, "Duplicate request ignored")); n != 0 {
		t.Fatalf("%d duplicate notices after a failed admit", n)
	}
}

// A throttle response without Retry-After is not retried every tick
// (review L9): sends wait at least ThrottleFloor.
func TestThrottleWithoutRetryAfterWaitsTheFloor(t *testing.T) {
	var (
		mu       sync.Mutex
		attempts []time.Time
		until    time.Time
	)
	r := newRig(t, func(c *Config) {
		c.Timing.ChatRate = 10 * time.Millisecond // the floor, not the per-chat gap, spaces the retries
		c.Channel.(*channeltest.Fake).SendFault = func(chat string, p channel.Part) (bool, error) {
			if !strings.Contains(p.Body, "ANSWER") {
				return false, nil
			}
			mu.Lock()
			defer mu.Unlock()
			now := time.Now()
			attempts = append(attempts, now)
			if until.IsZero() {
				until = now.Add(time.Second)
			}
			if now.Before(until) {
				return false, &channel.Throttled{Scope: channel.ScopeChat, Err: errors.New("429 without Retry-After")}
			}
			return false, nil
		}
	})
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "throttled reply")
	// The bound is on the spacing of the attempts, not on how soon the
	// answer lands: under a loaded run (the whole package with -race) the
	// turn itself can take seconds, so the wait is generous.
	r.waitFor("answer", 60*time.Second, func() bool { return len(r.own(work, "ANSWER to throttled reply")) == 1 })
	mu.Lock()
	defer mu.Unlock()
	floor := FastTiming(20 * time.Millisecond).ThrottleFloor
	// One second of throttling at a 200 ms floor: about 6 attempts; the
	// old one-tick retry made dozens.
	if max := int(time.Second/floor) + 3; len(attempts) > max {
		t.Fatalf("%d send attempts in one second of throttling, want at most %d", len(attempts), max)
	}
	for i := 1; i < len(attempts); i++ {
		if gap := attempts[i].Sub(attempts[i-1]); gap < floor-floor/4 {
			t.Fatalf("attempt %d only %s after the previous one", i, gap)
		}
	}
}

// Commands left unfinished at a stop are all re-run after the restart,
// however many, and keep their group-chat quote (review L9).
func TestRequeuedCommandsNotDroppedAndKeepQuote(t *testing.T) {
	var (
		mu      sync.Mutex
		replies []string // ReplyTo of every "Unknown helper command" reply sent
	)
	r := newRig(t, func(c *Config) {
		c.WriteRate, c.Timing.ChatRate = 1000, time.Millisecond
		c.Channel.(*channeltest.Fake).SendFault = func(chat string, p channel.Part) (bool, error) {
			if strings.Contains(p.Body, "Unknown helper command") {
				mu.Lock()
				replies = append(replies, p.ReplyTo)
				mu.Unlock()
			}
			return false, nil
		}
	})
	work := r.newWork(t.TempDir())
	r.ch.PostCoworker(work, "hello all")
	r.ch.PostUser(work, "helper frobnicate")
	r.wait("group chat reply", func() bool { return len(r.own(work, "Unknown helper command")) == 1 })
	r.stop()

	const n = 300 // more than the command queue holds
	st, err := store.Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	streams, err := st.Streams()
	if err != nil {
		t.Fatal(err)
	}
	convs, err := st.Conversations()
	if err != nil {
		t.Fatal(err)
	}
	err = st.Update(func(tx *store.Tx) error {
		var conv int64
		for _, c := range convs {
			if c.ExtChatID == work {
				conv = c.ID
			}
		}
		for _, s := range streams {
			if s.ConvID != conv {
				continue
			}
			var msgs []store.InboundMsg
			for i := 0; i < n; i++ {
				msgs = append(msgs, store.InboundMsg{ExtMsgID: fmt.Sprintf("left-%d", i), ExtTime: time.Now().UnixMilli(),
					Sender: store.SenderOwner, Kind: store.InCommand, Body: fmt.Sprintf("helper frob%d", i)})
			}
			_, err := tx.AdmitWindow(s.Key, conv, msgs, s.Cursor)
			return err
		}
		return errors.New("no stream for the work chat")
	})
	st.Release()
	st.Close()
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	replies = nil
	mu.Unlock()
	r.start()
	r.wait("every left command answered", func() bool { mu.Lock(); defer mu.Unlock(); return len(replies) >= n })
	mu.Lock()
	defer mu.Unlock()
	quoted := map[string]bool{}
	for _, q := range replies {
		if !strings.HasPrefix(q, "left-") {
			t.Fatalf("requeued reply quotes %q, want the command it answers", q)
		}
		quoted[q] = true
	}
	if len(quoted) != n {
		t.Fatalf("%d distinct commands answered, want %d", len(quoted), n)
	}
}

// A failed finish transaction is retried; the chat is not left with an
// open turn (review L1).
func TestFailedTurnFinishIsRetried(t *testing.T) {
	var fails atomic.Int32
	fails.Store(3)
	r := newRig(t, func(c *Config) {
		c.finishFault = func() error {
			if fails.Add(-1) >= 0 {
				return errors.New("test: disk full")
			}
			return nil
		}
	})
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "finish me")
	r.wait("answer after failed finishes", func() bool { return len(r.own(work, "ANSWER to finish me")) == 1 })
	r.ch.PostUser(work, "and the next")
	r.wait("next answer", func() bool { return len(r.own(work, "ANSWER to and the next")) == 1 })
}

// Benchmark F2: without a Workflow card, finals delivered within the
// mention window after an @mention are named together in one control-chat
// @mention; each final is covered exactly once.
func TestOwnerMentionsCoalescedWithinWindow(t *testing.T) {
	r := newRig(t, func(c *Config) { c.Timing.MentionWindow = 4 * time.Second })
	a := r.newWork(t.TempDir())
	b := r.newWork(t.TempDir())
	c := r.newWork(t.TempDir())
	ctl := r.chatOf("control")
	r.ch.PostUser(a, "first final")
	r.wait("first mention", func() bool { return len(r.own(ctl, "Codex finished")) == 1 })
	first := time.Now()
	r.ch.PostUser(b, "second final")
	r.ch.PostUser(c, "third final")
	r.wait("answers", func() bool {
		return len(r.own(b, "ANSWER to second final")) == 1 && len(r.own(c, "ANSWER to third final")) == 1
	})
	if time.Since(first) > 3*time.Second {
		t.Skip("the answers took longer than the window; nothing to coalesce")
	}
	r.wait("second mention", func() bool { return len(r.own(ctl, "Codex finished")) == 2 })
	time.Sleep(4500 * time.Millisecond) // a further window: nothing else is sent
	ms := r.own(ctl, "Codex finished")
	if len(ms) != 2 {
		t.Fatalf("%d mentions for 3 finals, want 2 (the second covering two)", len(ms))
	}
	if strings.Contains(ms[0].Text, " and ") {
		t.Fatalf("first mention names more than one final: %q", ms[0].Text)
	}
	if !strings.Contains(ms[1].Text, " and ") || !ms[1].MentionsOwner {
		t.Fatalf("second mention does not name both finals: %q", ms[1].Text)
	}
	var labels []string
	convs, _ := r.st.Conversations()
	for _, cv := range convs {
		if cv.ExtChatID == b || cv.ExtChatID == c {
			labels = append(labels, firstNonEmpty(cv.Title, cv.Label))
		}
	}
	for _, l := range labels {
		if strings.Count(ms[1].Text, l) != 1 {
			t.Fatalf("coalesced mention names %q %d times: %q", l, strings.Count(ms[1].Text, l), ms[1].Text)
		}
	}
}

// cardNotifier records Workflow cards and whether the answer was already
// in the chat when each was posted.
type cardNotifier struct {
	mu     sync.Mutex
	r      *rig
	chat   string
	cards  []string
	early  int // cards posted before the answer was visible
	answer string
}

func (n *cardNotifier) Available() bool { return true }
func (n *cardNotifier) NotifyFinal(_ context.Context, conv, title, text, url string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cards = append(n.cards, text)
	if len(n.r.own(n.chat, n.answer)) == 0 {
		n.early++
	}
	return nil
}

// The owner card is sent once the final is in the chat, and survives a
// restart between the turn's end and the final's delivery (review L8).
func TestOwnerCardFollowsTheFinalAcrossRestart(t *testing.T) {
	var hold atomic.Bool
	hold.Store(true)
	n := &cardNotifier{answer: "ANSWER to card me"}
	r := newRig(t, func(c *Config) {
		c.Notifier = n
		c.Channel.(*channeltest.Fake).SendFault = func(chat string, p channel.Part) (bool, error) {
			if hold.Load() && strings.Contains(p.Body, "ANSWER") {
				return false, &channel.Throttled{RetryAfter: 50 * time.Millisecond, Scope: channel.ScopeChat, Err: errors.New("held")}
			}
			return false, nil
		}
	})
	n.r = r
	work := r.newWork(t.TempDir())
	n.mu.Lock()
	n.chat = work
	n.mu.Unlock()
	r.ch.PostUser(work, "card me")
	r.wait("turn finished", func() bool { c, _ := r.st.TurnCounts(); return c[store.TurnCompleted] == 1 })
	time.Sleep(300 * time.Millisecond)
	cards := func() int { n.mu.Lock(); defer n.mu.Unlock(); return len(n.cards) }
	if got := cards(); got != 0 {
		t.Fatalf("%d cards before the answer was sent", got)
	}
	r.stop()
	r.start()
	time.Sleep(300 * time.Millisecond)
	if got := cards(); got != 0 {
		t.Fatalf("%d cards after the restart, before the answer was sent", got)
	}
	hold.Store(false)
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to card me")) == 1 })
	r.wait("card", func() bool { return cards() == 1 })
	time.Sleep(300 * time.Millisecond)
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.cards) != 1 || n.early != 0 || !strings.Contains(n.cards[0], "ANSWER to card me") {
		t.Fatalf("cards %q, %d before the answer", n.cards, n.early)
	}
	if m := len(r.own(r.chatOf("control"), "Codex finished")); m != 0 {
		t.Fatalf("%d control-chat mentions besides the card", m)
	}
}
