package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func readQuestion(t *testing.T, e *Engine, key string) store.QuestionGroup {
	t.Helper()
	var g store.QuestionGroup
	if err := e.update(func(tx *store.Tx) (err error) { g, _, err = tx.QuestionGroup(key); return }); err != nil {
		t.Fatal(err)
	}
	return g
}

func TestQuestionCommandsKeepNaturalLanguagePrompts(t *testing.T) {
	for _, parse := range []func(string) parsed{parseControl, parseWork} {
		for _, text := range []string{"answer my earlier question", "codex answer my earlier question"} {
			if p := parse(text); p.form != formText {
				t.Fatalf("%q captured as command: %+v", text, p)
			}
		}
		for _, text := range []string{"helper answer Q42 A", "/answer Q42 A", "!answer Q42 A", "helper answer"} {
			if p := parse(text); p.name != cmdAnswer {
				t.Fatalf("%q not a command: %+v", text, p)
			}
		}
	}
}

func TestQuestionQuoteRoutingKeepsOtherInputMeanings(t *testing.T) {
	r, a, work := questionRig(t)
	g := questionGroup(false, 2)
	if err := a.emit(g); err != nil {
		t.Fatal(err)
	}
	r.wait("question messages", func() bool { return len(r.own(work, "Question from Codex")) == 2 })
	posts := r.own(work, "Question from Codex")
	conv := readQuestion(t, r.e, g.Key).ConvID
	for _, tc := range []struct {
		name, text string
		quotes     []channel.Quote
		want       string
		reject     bool
	}{
		{"ordinary", "B", nil, "", false},
		{"quote", "B", []channel.Quote{{ID: posts[0].ID}}, cmdAnswer, false},
		{"literal skip", "--skip", []channel.Quote{{ID: posts[0].ID}}, cmdAnswer, false},
		{"explicit queue", "helper queue B", []channel.Quote{{ID: posts[0].ID}}, "", false},
		{"explicit answer", "helper answer Q2 B", []channel.Quote{{ID: posts[0].ID}}, cmdAnswer, false},
		{"forward", "B", []channel.Quote{{ID: posts[0].ID, Forwarded: true}}, "", false},
		{"ambiguous", "B", []channel.Quote{{ID: posts[0].ID}, {ID: posts[1].ID}}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := channel.InboundMsg{ID: "routing-" + tc.name, Chat: work, ByOwner: true, From: "owner", Created: time.Now(), Text: tc.text, Quotes: tc.quotes}
			if err := r.e.update(func(tx *store.Tx) error {
				c, ok, err := r.e.classify(tx, conv, store.KindWork, false, m)
				if err != nil {
					return err
				}
				if !ok || c.cmd.name != tc.want || (c.reject != "") != tc.reject {
					t.Errorf("%+v", c)
				}
				if tc.name == "literal skip" && !strings.Contains(c.cmd.arg, "-- --skip") {
					t.Error(c.cmd)
				}
				if tc.name == "explicit queue" && (!c.in.Queue || c.in.Body != "B") {
					t.Error("queue changed meaning", c.in)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQuestionPartialNonblockingDisablesTimeout(t *testing.T) {
	r, a, work := questionRig(t)
	g := questionGroup(false, 2)
	g.Blocking = false
	if err := a.emit(g); err != nil {
		t.Fatal(err)
	}
	r.ch.PostUser(work, "helper answer Q1 A")
	r.wait("partial answer", func() bool { return readQuestion(t, r.e, g.Key).Questions[0].Answer != nil })
	if cur := readQuestion(t, r.e, g.Key); cur.Deadline != 0 {
		t.Fatal("timeout still enabled", cur)
	}
	if err := r.e.update(func(tx *store.Tx) error { _, err := txUpdateDeadline(tx, g.Key); return err }); err != nil {
		t.Fatal(err)
	}
	r.e.maintainQuestions(context.Background())
	if a.nreplies() != 0 {
		t.Fatal("partial answers timed out")
	}
	r.ch.PostUser(work, "helper answer Q2 B")
	r.wait("all answered", func() bool { return a.nreplies() == 1 })
}

func TestQuestionSyncReplyAllowedWhilePausedOrDraining(t *testing.T) {
	for _, mode := range []string{"paused", "draining"} {
		t.Run(mode, func(t *testing.T) {
			r, a, work := questionRig(t)
			g := questionGroup(false, 1)
			if err := a.emit(g); err != nil {
				t.Fatal(err)
			}
			conv := readQuestion(t, r.e, g.Key).ConvID
			if mode == "paused" {
				if err := r.e.update(func(tx *store.Tx) error {
					if err := tx.Pause(conv, "test"); err != nil {
						return err
					}
					_, err := r.e.refresh(tx, conv)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				r.e.drainFlag.Store(true)
				defer r.e.undrain()
			}
			r.ch.PostUser(work, "helper answer Q1 A")
			r.wait("RPC response despite scheduler hold", func() bool { return a.nreplies() == 1 })
			if len(a.runLog()) != 1 {
				t.Fatal("answer allocated execution")
			}
		})
	}
}

func TestQuestionAsyncReplyWaitsDuringDrain(t *testing.T) {
	r, a, work := questionRig(t)
	g := questionGroup(true, 1)
	if err := a.emit(g); err != nil {
		t.Fatal(err)
	}
	r.e.drainFlag.Store(true)
	r.ch.PostUser(work, "helper answer Q1 A")
	r.wait("answer persisted", func() bool { return readQuestion(t, r.e, g.Key).Questions[0].Answer != nil })
	if len(a.runLog()) != 1 || a.count() != 0 {
		t.Fatal("drain bypassed")
	}
	r.e.undrain()
	r.wait("follow-up after drain", func() bool { return len(a.runLog()) == 2 })
}

func TestQuestionAsyncEndStatesAndOtherClient(t *testing.T) {
	for _, state := range []string{"completed", "interrupted", "other client", "unrelated answer"} {
		t.Run(state, func(t *testing.T) {
			r, a, work := questionRig(t)
			g := questionGroup(true, 1)
			if err := a.emit(g); err != nil {
				t.Fatal(err)
			}
			ev := QuestionEvent{ThreadID: g.ThreadID, TurnID: g.TurnID, Finished: true, Interrupted: state == "interrupted"}
			if strings.Contains(state, "client") || state == "unrelated answer" {
				ev.Finished = false
				ev.Answered = []string{"q-0"}
				if state == "unrelated answer" {
					ev.Answered = []string{"other"}
				}
			}
			if err := r.e.questionEvent(ev); err != nil {
				t.Fatal(err)
			}
			cur := readQuestion(t, r.e, g.Key)
			want := "open"
			if state == "interrupted" {
				want = "expired"
			}
			if state == "other client" {
				want = "resolved"
			}
			if cur.State != want {
				t.Fatalf("got %s want %s", cur.State, want)
			}
			r.ch.PostUser(work, "helper answer Q1 B")
			if want == "open" {
				r.wait("late answer starts", func() bool { return len(a.runLog()) == 2 })
			} else {
				r.wait("closed reply rejected", func() bool { return len(r.own(work, "already resolved")) > 0 })
				if len(a.runLog()) != 1 {
					t.Fatal("closed answer executed")
				}
			}
		})
	}
}

func TestQuestionSyncOrphanAndUncertainRecovery(t *testing.T) {
	for _, submitting := range []bool{false, true} {
		t.Run(fmt.Sprint(submitting), func(t *testing.T) {
			r, a, work := questionRig(t)
			g := questionGroup(false, 1)
			if err := a.emit(g); err != nil {
				t.Fatal(err)
			}
			if submitting {
				if err := r.e.update(func(tx *store.Tx) error { return tx.SetQuestionState(g.Key, "submitting") }); err != nil {
					t.Fatal(err)
				}
			}
			r.stop()
			a.qm.Lock()
			a.pending = map[string]bool{}
			a.qm.Unlock()
			r.start()
			want := "expired"
			if submitting {
				want = "uncertain"
			}
			r.wait("orphan closure", func() bool { return readQuestion(t, r.e, g.Key).State == want })
			r.ch.PostUser(work, "helper answer Q1 B")
			r.wait("orphan answer refused", func() bool { return len(r.own(work, "already resolved")) > 0 })
			if a.nreplies() != 0 {
				t.Fatal("orphan RPC replayed")
			}
		})
	}
}

func TestQuestionSecretAndAttachmentsDoNotReachCodex(t *testing.T) {
	r, a, work := questionRig(t)
	g := questionGroup(false, 1)
	g.Questions[0].Secret = true
	if err := a.emit(g); err != nil {
		t.Fatal(err)
	}
	r.ch.PostUser(work, "helper answer Q1 A")
	r.wait("secret answer refused", func() bool { return len(r.own(work, "Do not post secrets")) > 0 })
	if a.nreplies() != 0 {
		t.Fatal("secret sent")
	}
	for _, control := range []bool{false, true} {
		c := classified{control: control, cmd: parsed{form: formCommand, name: cmdAnswer}, in: store.InboundMsg{Kind: store.InCommand, Sender: store.SenderOwner}, msg: channel.InboundMsg{Files: []channel.FileRef{{Kind: channel.FileImage}}}}
		screen(&c)
		if !strings.Contains(c.reject, "text only") {
			t.Fatal(c.reject)
		}
		c.msg.Files = nil
		c.msg.Attachments = []string{"messageReference"}
		c.reject = ""
		screen(&c)
		if c.reject != "" {
			t.Fatal("quote rejected", c.reject)
		}
	}
	r.ch.PostUser(work, "helper answer Q1 --skip")
	r.wait("secret can be skipped", func() bool { return a.nreplies() == 1 })
}

// A Workflow failure can be ambiguous. It falls back once to the existing
// control-chat mention, with neither a retry nor final-answer side effects.
type questionNotifier struct {
	mu    sync.Mutex
	count int
	fail  bool
}

func (*questionNotifier) Available() bool { return true }
func (n *questionNotifier) Notify(_ context.Context, notice channel.OwnerNotice) error {
	if !strings.Contains(notice.Heading, "question") {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.count++
	if n.fail {
		return fmt.Errorf("workflow unavailable")
	}
	return nil
}
func (n *questionNotifier) calls() int { n.mu.Lock(); defer n.mu.Unlock(); return n.count }

func TestQuestionNotificationWorkflowAndFallback(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			a := newQuestionAgent()
			n := &questionNotifier{fail: fail}
			r := newRig(t, func(c *Config) { c.Agent = a; c.Notifier = n })
			r.ag = a.fakeAgent
			work := r.newWork(t.TempDir())
			r.ch.PostUser(work, "seed")
			r.wait("seed", func() bool { return len(r.own(work, "ANSWER to seed")) > 0 })
			g := questionGroup(false, 2)
			if err := a.emit(g); err != nil {
				t.Fatal(err)
			}
			r.wait("one group notification", func() bool { return n.calls() == 1 })
			if fail {
				r.wait("mention fallback", func() bool { return len(r.own(r.chatOf("control"), "Codex has 2 questions")) == 1 })
			}
			if err := a.emit(g); err != nil {
				t.Fatal(err)
			}
			r.e.pumpNotify(context.Background())
			if n.calls() != 1 {
				t.Fatal("question notification retried")
			}
		})
	}
}

func TestQuestionAnswerBeforeDeliverySuppressesNotification(t *testing.T) {
	a := newQuestionAgent()
	n := &questionNotifier{}
	var hold atomic.Bool
	hold.Store(true)
	r := newRig(t, func(c *Config) {
		c.Agent, c.Notifier = a, n
		c.Channel.(*channeltest.Fake).SendFault = func(_ string, p channel.Part) (bool, error) {
			if hold.Load() && strings.Contains(p.Body, "Question from Codex") {
				return false, &channel.Throttled{RetryAfter: 50 * time.Millisecond, Scope: channel.ScopeChat, Err: fmt.Errorf("held question")}
			}
			return false, nil
		}
	})
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "seed")
	r.wait("seed", func() bool { return len(r.own(work, "ANSWER to seed")) > 0 })
	g := questionGroup(false, 1)
	if err := a.emit(g); err != nil {
		t.Fatal(err)
	}
	r.wait("body registered but delivery held", func() bool {
		rows, err := r.e.st.OutboxByKeyPrefix(questionPrefix(1) + ":")
		return err == nil && len(rows) > 0
	})
	r.ch.PostUser(work, "helper answer Q1 A")
	r.wait("answer before body", func() bool { return a.nreplies() == 1 })
	hold.Store(false)
	r.e.wakeSender()
	r.wait("eventual resolved edit", func() bool { return len(r.own(work, "Answer submitted")) > 0 })
	r.e.pumpNotify(context.Background())
	if n.calls() != 0 {
		t.Fatal("resolved question notified")
	}
}

func TestQuestionRebindRejectsAnswerAndQueuedFollowup(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			r, a, work := questionRig(t)
			g := questionGroup(queued, 1)
			if err := a.emit(g); err != nil {
				t.Fatal(err)
			}
			conv := readQuestion(t, r.e, g.Key).ConvID
			if queued {
				r.e.drainFlag.Store(true)
				r.ch.PostUser(work, "helper answer Q1 A")
				r.wait("accepted before rebind", func() bool { return readQuestion(t, r.e, g.Key).Questions[0].Answer != nil })
			}
			if err := r.e.update(func(tx *store.Tx) error {
				th, _, err := tx.Thread(conv)
				if err != nil {
					return err
				}
				th.ThreadID = "different-thread"
				if err = tx.Bind(th); err != nil {
					return err
				}
				_, err = r.e.refresh(tx, conv)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if queued {
				r.e.undrain()
				r.wait("queued input consumed without running", func() bool {
					var none bool
					_ = r.e.update(func(tx *store.Tx) error { turns, err := tx.OpenTurnsOf(conv); none = len(turns) == 0; return err })
					return none
				})
			} else {
				r.ch.PostUser(work, "helper answer Q1 B")
				r.wait("old thread refused", func() bool { return len(r.own(work, "different Codex session")) > 0 })
			}
			if len(a.runLog()) != 1 || a.nreplies() != 0 {
				t.Fatal("old question delivered to new thread")
			}
		})
	}
}

func TestQuestionExpiredQueuedReplyDoesNotStartOrSteer(t *testing.T) {
	for _, running := range []bool{false, true} {
		t.Run(fmt.Sprint(running), func(t *testing.T) {
			r, a, work := questionRig(t)
			g := questionGroup(true, 1)
			if err := a.emit(g); err != nil {
				t.Fatal(err)
			}
			r.e.drainFlag.Store(true)
			r.ch.PostUser(work, "helper answer Q1 A")
			r.wait("answer queued during drain", func() bool { return readQuestion(t, r.e, g.Key).Questions[0].Answer != nil })
			if err := r.e.questionEvent(QuestionEvent{ThreadID: g.ThreadID, TurnID: g.TurnID, Finished: true, Interrupted: true}); err != nil {
				t.Fatal(err)
			}
			if running {
				a.smu.Lock()
				a.current = "another-turn"
				a.smu.Unlock()
			}
			r.e.undrain()
			r.wait("expired input refused", func() bool { return len(r.own(work, "question is no longer available")) > 0 })
			if len(a.runLog()) != 1 || a.count() != 0 {
				t.Fatal("answer to an expired question executed")
			}
			a.smu.Lock()
			a.current = ""
			a.smu.Unlock()
			r.ch.PostUser(work, "next after expired answer")
			r.wait("execution slot remains usable", func() bool { return len(a.runLog()) == 2 })
		})
	}
}

type questionRenderChannel struct {
	*channeltest.Fake
	renders atomic.Int32
	entered chan struct{}
	release <-chan struct{}
}

func (c *questionRenderChannel) Render(ctx context.Context, doc channel.RichDoc) ([]channel.Part, error) {
	if strings.Contains(doc.Label, "Question from Codex") {
		c.renders.Add(1)
		if c.entered != nil {
			select {
			case c.entered <- struct{}{}:
			default:
			}
			select {
			case <-c.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return c.Fake.Render(ctx, doc)
}

func TestQuestionRenderingDoesNotBlockNativeEventsOrOtherChats(t *testing.T) {
	a := newQuestionAgent()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	r := newRig(t, func(c *Config) {
		c.Agent, c.Senders = a, 2
		c.Channel = &questionRenderChannel{Fake: c.Channel.(*channeltest.Fake), entered: entered, release: release}
	})
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "seed")
	r.wait("seed", func() bool { return len(r.own(work, "ANSWER to seed")) > 0 })
	g := questionGroup(true, 1)
	emitted := make(chan error, 1)
	go func() { emitted <- a.emit(g) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("question renderer did not start")
	}
	select {
	case err := <-emitted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("native event reader waited for question rendering")
	}
	if err := r.e.questionEvent(QuestionEvent{ThreadID: g.ThreadID, TurnID: g.TurnID, Finished: true, Interrupted: true}); err != nil {
		t.Fatal(err)
	}
	other := r.newWork(t.TempDir())
	r.ch.PostUser(other, "independent task")
	r.wait("other chat progresses while question rendering waits", func() bool { return len(r.own(other, "ANSWER to independent task")) > 0 })
	unblock.Do(func() { close(release) })
	conv := readQuestion(t, r.e, g.Key).ConvID
	r.wait("renderer released", func() bool {
		r.e.mu.Lock()
		defer r.e.mu.Unlock()
		return !r.e.convs[conv].sending
	})
	if len(r.own(work, "Question from Codex")) != 0 {
		t.Fatal("a resolved question published stale actions after rendering")
	}
}

func TestQuestionUnknownDeliveryBeforePublicationNeverReplays(t *testing.T) {
	a := newQuestionAgent()
	a.failure = fmt.Errorf("connection lost after write")
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	r := newRig(t, func(c *Config) {
		c.Agent, c.Senders = a, 2
		c.Channel = &questionRenderChannel{Fake: c.Channel.(*channeltest.Fake), entered: entered, release: release}
	})
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "seed")
	r.wait("seed", func() bool { return len(r.own(work, "ANSWER to seed")) > 0 })
	g := questionGroup(false, 1)
	if err := a.emit(g); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("question renderer did not start")
	}
	r.ch.PostUser(work, "helper answer Q1 A")
	r.wait("unknown result persisted before publication", func() bool {
		return readQuestion(t, r.e, g.Key).State == "uncertain"
	})
	r.e.maintainQuestions(context.Background())
	r.ch.PostUser(work, "helper answer Q1 B")
	unblock.Do(func() { close(release) })
	r.wait("second answer refused", func() bool { return len(r.own(work, "already resolved")) > 0 })
	conv := readQuestion(t, r.e, g.Key).ConvID
	r.wait("renderer released", func() bool {
		r.e.mu.Lock()
		defer r.e.mu.Unlock()
		return !r.e.convs[conv].sending
	})
	if a.nreplies() != 1 || len(r.own(work, "Question from Codex")) != 0 {
		t.Fatal("uncertain answer replayed or stale question published")
	}
}

func TestQuestionMaintenanceDoesNotRenderExistingMessages(t *testing.T) {
	a := newQuestionAgent()
	var ch *questionRenderChannel
	r := newRig(t, func(c *Config) {
		c.Agent = a
		ch = &questionRenderChannel{Fake: c.Channel.(*channeltest.Fake)}
		c.Channel = ch
	})
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "seed")
	r.wait("seed", func() bool { return len(r.own(work, "ANSWER to seed")) > 0 })
	if err := a.emit(questionGroup(false, 2)); err != nil {
		t.Fatal(err)
	}
	r.wait("question sent", func() bool { return len(r.own(work, "Question from Codex")) == 2 })
	before := ch.renders.Load()
	r.e.setGate(r.e.gWrite, r.e.now().Add(time.Minute), "test unchanged maintenance")
	for i := 0; i < 5; i++ {
		r.e.maintainQuestions(context.Background())
	}
	if after := ch.renders.Load(); after != before {
		t.Fatal("maintenance repeated question rendering", before, after)
	}
	r.e.mu.Lock()
	defer r.e.mu.Unlock()
	for _, edits := range r.e.edits {
		for _, edit := range edits {
			if strings.HasPrefix(edit.key, "question-publish:") {
				t.Fatal("unchanged maintenance consumed sender work")
			}
		}
	}
}

func TestQuestionAsyncSkipDoesNotConfirmAnotherQueuedAnswer(t *testing.T) {
	r, a, work := questionRig(t)
	g := questionGroup(true, 2)
	if err := a.emit(g); err != nil {
		t.Fatal(err)
	}
	r.e.drainFlag.Store(true)
	r.ch.PostUser(work, "helper answer Q1 A")
	r.ch.PostUser(work, "helper answer Q2 --skip")
	r.wait("both decisions recorded", func() bool {
		cur := readQuestion(t, r.e, g.Key)
		return cur.Questions[0].Answer != nil && cur.Questions[1].Skipped
	})
	if cur := readQuestion(t, r.e, g.Key); cur.State != "open" {
		t.Fatal("undelivered answer confirmed by skip", cur)
	}
	if err := r.e.questionEvent(QuestionEvent{ThreadID: g.ThreadID, Answered: []string{"unrelated"}}); err != nil {
		t.Fatal(err)
	}
	if cur := readQuestion(t, r.e, g.Key); cur.State != "open" {
		t.Fatal("unrelated answer closed group")
	}
	if err := r.e.questionEvent(QuestionEvent{ThreadID: g.ThreadID, Answered: []string{"q-0"}}); err != nil {
		t.Fatal(err)
	}
	if cur := readQuestion(t, r.e, g.Key); cur.State != "resolved" {
		t.Fatal("native confirmation not applied")
	}
	r.e.undrain()
}

func TestQuestionAsyncControlReplySteersWithNativeEnvelope(t *testing.T) {
	a := newQuestionAgent()
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	ctl := r.control()
	thread := "control-thread"
	if err := r.e.update(func(tx *store.Tx) error {
		if err := tx.Bind(store.Thread{ConvID: r.e.controlID, ThreadID: thread, Cwd: t.TempDir()}); err != nil {
			return err
		}
		_, err := r.e.refresh(tx, r.e.controlID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.e.bindThread(r.e.controlID, thread); err != nil {
		t.Fatal(err)
	}
	a.smu.Lock()
	a.current = "control-turn"
	a.smu.Unlock()
	g := questionGroup(true, 1)
	g.ThreadID = thread
	if err := a.emit(g); err != nil {
		t.Fatal(err)
	}
	r.ch.PostUser(ctl, "helper answer Q1 B")
	r.wait("control reply steer", func() bool { return a.count() == 1 })
	a.smu.Lock()
	text := a.inputs[0].Text
	a.smu.Unlock()
	if !strings.HasPrefix(text, questionReplyStart) || strings.Contains(text, "User message:") {
		t.Fatal("control envelope broke native reply", text)
	}
}

func TestQuestionDroppedBodyStillNotifiesWithoutResolvingRPC(t *testing.T) {
	a := newQuestionAgent()
	n := &questionNotifier{}
	r := newRig(t, func(c *Config) {
		c.Agent, c.Notifier = a, n
		c.Channel.(*channeltest.Fake).SendFault = func(_ string, p channel.Part) (bool, error) {
			if strings.Contains(p.Body, "Question from Codex") {
				return false, &channel.Rejected{Permanent: true, Err: fmt.Errorf("body rejected")}
			}
			return false, nil
		}
	})
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "seed")
	r.wait("seed", func() bool { return len(r.own(work, "ANSWER to seed")) > 0 })
	if err := a.emit(questionGroup(false, 1)); err != nil {
		t.Fatal(err)
	}
	r.wait("delivery failure notification", func() bool { return n.calls() == 1 })
	if a.nreplies() != 0 {
		t.Fatal("failed message delivery answered native question")
	}
	r.e.pumpNotify(context.Background())
	if n.calls() != 1 {
		t.Fatal("dropped message notification repeated")
	}
}
