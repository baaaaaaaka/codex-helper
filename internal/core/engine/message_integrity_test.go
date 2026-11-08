package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

func TestEarlierCapacityProgressSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire(store.Ident{Host: "test", PID: 1, BootID: "test"}); err != nil {
		t.Fatal(err)
	}
	var tr store.Turn
	err = s.Update(func(tx *store.Tx) error {
		conv, err := tx.CreateConversation(store.NewConversation{Label: "s001", Kind: store.KindWork, Channel: "teams", ExtChatID: "work"})
		if err != nil {
			return err
		}
		ad, err := tx.AdmitWindow("work", conv, []store.InboundMsg{{ExtMsgID: "m1", ExtTime: 1, Sender: store.SenderOwner, Kind: store.InPrompt, Body: "progress"}}, "cursor")
		if err != nil {
			return err
		}
		tr, _, err = tx.Turn(ad[0].TurnID)
		if err != nil {
			return err
		}
		if err := tx.MarkStarting(tr.ID, "thread"); err != nil {
			return err
		}
		if err := tx.SetTranscriptProgress(tr.ID); err != nil {
			return err
		}
		if err := tx.MarkRunning(tr.ID, "earlier"); err != nil {
			return err
		}
		if err := tx.RequeueTurn(tr.ID, tx.Now()+1000, "thread", "earlier"); err != nil {
			return err
		}
		if err := tx.MarkStarting(tr.ID, "thread"); err != nil {
			return err
		}
		if err := tx.MarkRunning(tr.ID, "latest"); err != nil {
			return err
		}
		return tx.FinishTurn(tr.ID, store.TurnCompleted, "", nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ids, clients, err := s.TranscriptProgressTurns()
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{st: s, mirror: &mirrorState{progressTurns: ids, progressClients: clients}}
	w, err := rollout.New(t.TempDir(), "/work", rollout.Options{ID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range []string{"earlier", "latest"} {
		_, err := w.AppendTurn(rollout.Turn{ID: attempt, ClientID: tr.TurnKey, Prompt: "request", Commentary: "progress from " + attempt, Answer: "answer"})
		if err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.Open(w.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	lines, _, err := transcript.Read(f, 0, mirrorChunk)
	if err != nil {
		t.Fatal(err)
	}
	tracker := e.newMirrorTracker("thread", 0, 0, func(client, _ string) bool { return strings.HasPrefix(client, "cxp-") })
	got := tracker.Feed(lines)
	var notes []string
	for _, o := range got {
		if o.Kind == transcript.OutputStatus {
			notes = append(notes, o.Text)
		}
	}
	if strings.Join(notes, ";") != "progress from earlier;progress from latest" {
		t.Fatalf("restart lost an earlier capacity attempt: %+v", got)
	}
}

// Native-shaped fixture: records precede the corresponding completion callback
// and the terminal record is flushed before Run returns.
type recordedAgent struct {
	*fakeAgent
	sessions string
}

func (a recordedAgent) TranscriptProgress() bool { return true }
func (a recordedAgent) Run(ctx context.Context, req TurnRequest, h TurnHooks) (TurnOutcome, error) {
	var w *rollout.Writer
	var open rollout.OpenTurn
	original := h
	thread := req.ThreadID
	h.ThreadStarted = func(id string) error { thread = id; return original.ThreadStarted(id) }
	h.Started = func(id string) {
		var err error
		w, err = rollout.New(a.sessions, "/work", rollout.Options{ID: thread})
		if err != nil {
			panic(err)
		}
		open, err = w.BeginTurn(rollout.Turn{ID: id, ClientID: req.TurnKey, Prompt: req.Text})
		if err != nil {
			panic(err)
		}
		original.Started(id)
	}
	n := 0
	h.Progress = func(text string) {
		n++
		if err := w.AppendRecord("response_item", map[string]any{"type": "message", "role": "assistant", "id": fmt.Sprintf("msg-progress-%d", n), "phase": "commentary", "content": []any{map[string]any{"type": "output_text", "text": text}}}); err != nil {
			panic(err)
		}
		original.Progress(text)
	}
	out, err := a.fakeAgent.Run(ctx, req, h)
	if w != nil {
		if x := w.FinishTurn(open, rollout.Turn{Answer: out.Final, Aborted: out.Status != StatusCompleted}); x != nil {
			panic(x)
		}
	}
	return out, err
}

func TestTranscriptProgressOver100OrderedUnderThrottleAndInGroup(t *testing.T) {
	sessions := t.TempDir()
	var attempts atomic.Int32
	r := newRig(t, func(c *Config) {
		c.SessionsDir = sessions
		c.Agent = recordedAgent{c.Agent.(*fakeAgent), sessions}
		c.Timing.ChatRate = time.Millisecond
		c.Channel.(*channeltest.Fake).SendFault = func(_ string, p channel.Part) (bool, error) {
			if strings.Contains(p.Body, "entry-000") && attempts.Add(1) == 1 {
				return false, &channel.Throttled{Scope: channel.ScopeAccount, RetryAfter: 120 * time.Millisecond}
			}
			return false, nil
		}
	})
	r.ag.mu.Lock()
	for i := 0; i < 120; i++ {
		r.ag.notes = append(r.ag.notes, fmt.Sprintf("entry-%03d", i))
	}
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	conv := convIDForTest(r, work)
	r.e.mu.Lock()
	r.e.convs[conv].group = true
	r.e.mu.Unlock()
	r.ch.PostUser(work, "@codex progress integrity")
	// Keep the production write budget. A race-instrumented CI worker may
	// publish smaller batches while the reader and sender share its CPUs.
	r.waitFor("complete answer", time.Minute, func() bool { return len(r.own(work, "ANSWER to")) == 1 })
	var all strings.Builder
	for _, m := range r.ch.Messages(work) {
		if m.Marker != "" {
			all.WriteString(m.Text)
			all.WriteByte('\n')
		}
	}
	text := all.String()
	last := -1
	for i := 0; i < 120; i++ {
		key := fmt.Sprintf("entry-%03d", i)
		if strings.Count(text, key) != 1 {
			t.Fatalf("%s lost or duplicated", key)
		}
		at := strings.Index(text, key)
		if at <= last {
			t.Fatalf("%s overtook its predecessor", key)
		}
		last = at
	}
	if strings.Index(text, "ANSWER to") < last {
		t.Fatal("final overtook commentary")
	}
	r.stop()
	r.start()
	r.control()
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 120; i++ {
		key := fmt.Sprintf("entry-%03d", i)
		n := 0
		for _, m := range r.ch.Messages(work) {
			if m.Marker != "" {
				n += strings.Count(m.Text, key)
			}
		}
		if n != 1 {
			t.Fatalf("restart duplicated %s: %d", key, n)
		}
	}
}

func convIDForTest(r *rig, chat string) int64 {
	r.e.mu.Lock()
	defer r.e.mu.Unlock()
	return r.e.byChat[chat]
}

func TestUnconfirmedContentRetainedAcrossRestartAndLateReadBack(t *testing.T) {
	r := newRig(t, quickResolve)
	work := r.newWork(t.TempDir())
	attempts := absentSends(r, "ANSWER to vanished", 1)
	r.ch.PostUser(work, "vanished")
	r.wait("control warning", func() bool { return len(r.own(r.control(), "Delivery unconfirmed")) == 1 })
	rows, err := r.st.UnknownOutbox()
	if err != nil || len(rows) != 1 {
		t.Fatalf("unknown content missing: %+v %v", rows, err)
	}
	row := rows[0]
	if row.Kind != store.OutFinal || !strings.Contains(row.Body, "ANSWER to vanished") {
		t.Fatalf("wrong retained content: %+v", row)
	}
	if len(attempts()) != 1 {
		t.Fatal("absence caused blind resend")
	}
	r.stop()
	r.start()
	r.control()
	other := r.newWork(t.TempDir())
	r.ch.PostUser(other, "unrelated")
	r.wait("other chat progresses", func() bool { return len(r.own(other, "ANSWER to unrelated")) == 1 })
	if len(attempts()) != 1 {
		t.Fatal("restart caused blind resend")
	}
	// Simulate a late Graph visibility result for the original accepted attempt.
	_, err = r.ch.Send(context.Background(), work, channel.Part{Index: 1, Count: 1, Body: row.Body, Hash: fmt.Sprintf("%x", row.BodyHash)}, r.e.marker(row.AttemptToken))
	if err != nil {
		t.Fatal(err)
	}
	r.wait("late result resolves", func() bool { rows, _ := r.st.UnknownOutbox(); return len(rows) == 0 })
	if len(r.own(work, "ANSWER to vanished")) != 1 {
		t.Fatal("late visibility duplicated answer")
	}
}

func TestUnconfirmedLegacyProgressIsRetained(t *testing.T) {
	r := newRig(t, quickResolve)
	work := r.newWork(t.TempDir())
	conv := convIDForTest(r, work)
	attempts := absentSends(r, "legacy progress", 1)
	msgs, err := r.e.render(context.Background(), statusDoc("legacy progress"), store.OutProgress, "legacy-progress")
	if err != nil {
		t.Fatal(err)
	}
	// Older stores have progress rows without progress_group. Their content
	// remains substantive even though it cannot join a new transport batch.
	if err := r.e.update(func(tx *store.Tx) error { _, err := tx.Enqueue(conv, msgs); return err }); err != nil {
		t.Fatal(err)
	}
	r.e.wakeSender()
	r.wait("legacy progress warns without dropping", func() bool {
		return len(r.own(r.control(), "Delivery unconfirmed")) == 1
	})
	rows, err := r.st.UnknownOutbox()
	if err != nil || len(rows) != 1 || rows[0].Kind != store.OutProgress || rows[0].ProgressGroup != "" || !strings.Contains(rows[0].Body, "legacy progress") {
		t.Fatalf("legacy progress lost: %+v %v", rows, err)
	}
	if len(attempts()) != 1 {
		t.Fatal("legacy progress was blindly resent")
	}
}

func TestStillWorkingUsesVisibleClockWhileToolsAreActive(t *testing.T) {
	r := newRig(t, func(c *Config) {
		c.Timing.SilenceHint = 100 * time.Millisecond
		c.Timing.SilenceRepeat = 150 * time.Millisecond
		c.Timing.SilenceStuck = 500 * time.Millisecond
		c.Timing.ChatRate = time.Millisecond
	})
	r.ag.mu.Lock()
	r.ag.during = func(_ TurnRequest, h TurnHooks) {
		end := time.Now().Add(650 * time.Millisecond)
		for time.Now().Before(end) {
			h.Activity()
			time.Sleep(20 * time.Millisecond)
		}
	}
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "active tools")
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to active tools")) == 1 })
	if n := len(r.own(work, "Still working")); n < 2 || n > 5 {
		t.Fatalf("visible-clock hints %d", n)
	}
	if len(r.own(work, "It may be stuck")) != 0 {
		t.Fatal("active tools were called stuck")
	}
}

func TestNativeRejectionReleasesProgressOwnership(t *testing.T) {
	sessions := t.TempDir()
	r := newRig(t, func(c *Config) {
		c.SessionsDir = sessions
		c.Agent = recordedAgent{c.Agent.(*fakeAgent), sessions}
	})
	r.ag.mu.Lock()
	r.ag.refuse = func(TurnRequest) (TurnOutcome, bool) {
		return TurnOutcome{NotStarted: true, Status: StatusFailed, Error: "rejected before native acceptance"}, true
	}
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	conv := convIDForTest(r, work)
	r.ch.PostUser(work, "a refused task")
	r.wait("refusal delivered and ownership released", func() bool {
		if len(r.own(work, "rejected before native acceptance")) != 1 {
			return false
		}
		r.e.mu.Lock()
		open := r.e.convs[conv].transcriptOpen
		r.e.mu.Unlock()
		r.e.mirror.mu.Lock()
		clients := len(r.e.mirror.progressClients)
		r.e.mirror.mu.Unlock()
		return !open && clients == 0
	})
}

func TestConsecutiveNativeTurnsPublishEveryFinal(t *testing.T) {
	sessions := t.TempDir()
	r := newRig(t, func(c *Config) {
		c.SessionsDir = sessions
		c.Agent = recordedAgent{c.Agent.(*fakeAgent), sessions}
		c.Timing.ChatRate = time.Millisecond
		c.Timing.Maintenance = time.Hour // source terminal must wake delivery itself
	})
	work := r.newWork(t.TempDir())
	for i := 0; i < 3; i++ {
		text := fmt.Sprintf("consecutive native request %d", i)
		r.ch.PostUser(work, text)
		r.wait("consecutive final", func() bool { return len(r.own(work, "ANSWER to "+text)) == 1 })
	}
}

func TestShutdownPreservesDefiniteSendRejection(t *testing.T) {
	r := newRig(t, func(c *Config) {
		c.Channel.(*channeltest.Fake).SendFault = func(_ string, p channel.Part) (bool, error) {
			if strings.Contains(p.Body, "shutdown-rejected") {
				return false, &channel.Throttled{Scope: channel.ScopeChat, RetryAfter: time.Hour}
			}
			return false, nil
		}
	})
	work := r.newWork(t.TempDir())
	conv := convIDForTest(r, work)
	var row store.Outbox
	var token string
	// The ready message can be visible before its send result commits.
	// Wait for that commit and prepare the test send in the same transaction,
	// so the manual BeginSend obeys the production conversation FIFO.
	r.wait("startup sends committed", func() bool {
		if err := r.e.update(func(tx *store.Tx) error {
			pending, err := tx.PendingOutbox(conv)
			if err != nil || len(pending) != 0 {
				return err
			}
			if _, err := tx.Enqueue(conv, []store.OutMsg{{DeliveryKey: "shutdown-rejected", Kind: store.OutProgress, Body: "shutdown-rejected", ProgressGroup: "g"}}); err != nil {
				return err
			}
			rows, err := tx.PendingOutbox(conv)
			if err != nil {
				return err
			}
			row = rows[len(rows)-1]
			token, err = tx.BeginSend(row.ID)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return token != ""
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the definite 429 arrives concurrently with shutdown
	r.e.send(ctx, row, work, token, nil)
	if err := r.e.update(func(tx *store.Tx) error {
		got, _, err := tx.Outbox(row.ID)
		if err != nil {
			return err
		}
		if got.State != store.OutQueued || got.NextAttemptAt <= tx.Now() {
			t.Fatalf("definite rejection became an unknown send at restart: %+v", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestNoninteractiveSourceDiscoveredBeforeBindingIsReconsidered(t *testing.T) {
	r, sessions := mirrorRig(t)
	r.mirrorInitialised()
	w, err := rollout.New(sessions, t.TempDir(), rollout.Options{Source: "exec", Originator: "codex_exec"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "noninteractive", Answer: "answer"}); err != nil {
		t.Fatal(err)
	}
	r.wait("noninteractive source initially ignored", func() bool {
		srcs, _ := r.st.Sources()
		for _, s := range srcs {
			if s.Path == w.Path {
				return s.State == store.SrcIgnored
			}
		}
		return false
	})
	work := r.newWork(t.TempDir())
	conv := convIDForTest(r, work)
	if err := r.e.bindThread(conv, w.SessionID); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if t.Failed() {
			srcs, _ := r.st.Sources()
			t.Logf("sources after bind: %+v", srcs)
			r.e.mirror.mu.Lock()
			t.Logf("known=%v bound=%v files=%v", r.e.mirror.known, r.e.mirror.bound, r.e.mirror.files)
			r.e.mirror.mu.Unlock()
		}
	}()
	r.wait("bound source is now tracked", func() bool {
		srcs, _ := r.st.Sources()
		for _, s := range srcs {
			if s.Path == w.Path {
				return s.State == store.SrcTracking && s.ConvID == conv
			}
		}
		return false
	})
}

type earlyRecordedAgent struct {
	*fakeAgent
	sessions string
	accept   <-chan struct{}
}

func (earlyRecordedAgent) TranscriptProgress() bool { return true }
func (a earlyRecordedAgent) Run(ctx context.Context, req TurnRequest, h TurnHooks) (TurnOutcome, error) {
	w, err := rollout.New(a.sessions, "/work", rollout.Options{Source: "exec", Originator: "codex_exec"})
	if err != nil {
		return TurnOutcome{}, err
	}
	open, err := w.BeginTurn(rollout.Turn{ID: "early-native", ClientID: req.TurnKey, Prompt: req.Text, Commentary: "early progress"})
	if err == nil {
		err = w.FinishTurn(open, rollout.Turn{Commentary: "early progress", Answer: "early native answer"})
	}
	if err != nil {
		return TurnOutcome{}, err
	}
	if err := h.ThreadStarted(w.SessionID); err != nil {
		return TurnOutcome{}, err
	}
	h.Progress("early progress")
	a.mu.Lock()
	a.lookup[req.TurnKey] = TurnOutcome{Status: StatusCompleted, CodexTurnID: "early-native", Final: "early native answer"}
	a.mu.Unlock()
	select {
	case <-a.accept:
	case <-ctx.Done():
		return TurnOutcome{}, ctx.Err()
	}
	h.Started("early-native")
	return TurnOutcome{Status: StatusCompleted, CodexTurnID: "early-native", Final: "early native answer"}, nil
}

func TestRolloutAheadOfStartedCallbackDoesNotLoseProgressOrFinal(t *testing.T) {
	accept := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(accept) }) }
	t.Cleanup(release)
	sessions := t.TempDir()
	r := newRig(t, func(c *Config) {
		c.SessionsDir = sessions
		c.Agent = earlyRecordedAgent{c.Agent.(*fakeAgent), sessions, accept}
	})
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "exercise early native writes")
	r.wait("reader reached the terminal before the acceptance callback", func() bool {
		r.logMu.Lock()
		defer r.logMu.Unlock()
		return strings.Contains(r.log.String(), "native acceptance for turn")
	})
	if len(r.own(work, "early native answer")) != 0 {
		t.Fatal("final overtook native acceptance and reader commit")
	}
	release()
	r.wait("early answer after reader barrier", func() bool { return len(r.own(work, "early native answer")) == 1 })
	if len(r.own(work, "early progress")) != 1 {
		t.Fatal("rollout read before Started lost or duplicated commentary")
	}
}

func TestRestartRecoversAcceptanceBeforeTranscriptBarrier(t *testing.T) {
	accept := make(chan struct{})
	sessions := t.TempDir()
	r := newRig(t, func(c *Config) {
		c.SessionsDir = sessions
		c.Agent = earlyRecordedAgent{c.Agent.(*fakeAgent), sessions, accept}
	})
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "recover early native writes")
	r.wait("rollout ahead of unrecorded acceptance", func() bool {
		r.logMu.Lock()
		defer r.logMu.Unlock()
		return strings.Contains(r.log.String(), "native acceptance for turn")
	})
	r.ch.PostUser(work, "helper close")
	r.wait("chat closes while its accepted native task is outstanding", func() bool { return len(r.own(work, "Session closed")) == 1 })
	r.stop() // The accepted native id is still absent from the local turn.
	r.start()
	r.wait("reconciled answer after complete transcript", func() bool { return len(r.own(work, "early native answer")) == 1 })
	if len(r.own(work, "early progress")) != 1 {
		t.Fatal("acceptance recovery lost commentary or duplicated it")
	}
	var text strings.Builder
	for _, m := range r.ch.Messages(work) {
		if m.Marker != "" {
			text.WriteString(m.Text)
		}
	}
	if strings.Index(text.String(), "early progress") > strings.Index(text.String(), "early native answer") {
		t.Fatal("reconciliation final overtook source progress")
	}
}

func TestTranscriptBackpressureLeavesUnreadContentForLaterDelivery(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown=%v", unknown), func(t *testing.T) { testTranscriptBackpressure(t, unknown) })
	}
}

func testTranscriptBackpressure(t *testing.T, unknown bool) {
	var throttled atomic.Bool
	var attempts atomic.Int32
	throttled.Store(true)
	sessions := t.TempDir()
	r := newRig(t, func(c *Config) {
		c.Turns = 1
		c.SessionsDir = sessions
		c.Agent = recordedAgent{c.Agent.(*fakeAgent), sessions}
		c.Timing.ChatRate = time.Millisecond
		c.Channel.(*channeltest.Fake).SendFault = func(_ string, p channel.Part) (bool, error) {
			if throttled.Load() && strings.Contains(p.Body, "pressure-") {
				attempts.Add(1)
				if unknown {
					return false, &channel.Ambiguous{Err: errors.New("accepted outcome not visible yet")}
				}
				return false, &channel.Throttled{Scope: channel.ScopeChat, RetryAfter: 50 * time.Millisecond}
			}
			return false, nil
		}
	})
	r.ag.mu.Lock()
	for i := 0; i < 1000; i++ {
		r.ag.notes = append(r.ag.notes, fmt.Sprintf("pressure-%04d", i))
	}
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	conv := convIDForTest(r, work)
	r.ch.PostUser(work, "progress under pressure")
	r.wait("reader reaches pending high-water mark", func() bool {
		yes, _ := r.st.PendingAtLeast(conv, mirrorRecordsMax)
		return yes
	})
	progressRows := func() int {
		n := 0
		if err := r.e.update(func(tx *store.Tx) error {
			rows, err := tx.PendingOutbox(conv)
			for _, row := range rows {
				if row.ProgressGroup != "" {
					n++
				}
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := progressRows()
	time.Sleep(150 * time.Millisecond)
	if after := progressRows(); after != before || after >= 1000 {
		t.Fatalf("blocked source kept filling outbox: %d -> %d", before, after)
	}
	if len(r.own(work, "ANSWER to")) != 0 {
		t.Fatal("answer passed unread progress")
	}
	r.wait("completed native execution has a durable held final", func() bool {
		found := false
		_ = r.e.update(func(tx *store.Tx) error {
			rows, err := tx.PendingOutbox(conv)
			for _, row := range rows {
				found = found || row.Kind == store.OutFinal && row.HoldTurn != 0
			}
			return err
		})
		return found
	})
	// The only native execution slot must be free even while this chat's
	// progress cannot advance. Restart must restore the held final without
	// executing the already completed native task again.
	r.stop()
	r.start()
	r.control()
	other := r.newWork(t.TempDir())
	r.ch.PostUser(other, "independent task")
	r.wait("other chat executes despite blocked progress", func() bool { return len(r.own(other, "ANSWER to")) == 1 })
	r.ag.mu.Lock()
	runs := len(r.ag.runs)
	r.ag.mu.Unlock()
	if runs != 2 {
		t.Fatalf("restart replayed completed native execution: %d runs", runs)
	}
	throttled.Store(false)
	if unknown {
		if n := attempts.Load(); n != 1 {
			t.Fatalf("blocked progress was blindly resent: %d attempts", n)
		}
		rows, err := r.st.UnknownOutbox()
		if err != nil || len(rows) != 1 {
			t.Fatalf("unknown batch lost across restart: %+v %v", rows, err)
		}
		row := rows[0]
		// Graph eventually exposes the original attempt. This does not
		// represent another helper POST; its original marker is preserved.
		_, err = r.ch.Send(context.Background(), work, channel.Part{Index: 1, Count: 1, Body: row.Body, Hash: fmt.Sprintf("%x", row.BodyHash)}, r.e.marker(row.AttemptToken))
		if err != nil {
			t.Fatal(err)
		}
	}
	// At the default account pace, even 63 full batches need about 18 s.
	// Allow the reader, SQLite race instrumentation, and concurrent rigs to
	// share the worker without changing the production pace or assertions.
	r.waitFor("all unread progress and final delivered", 90*time.Second, func() bool { return len(r.own(work, "ANSWER to")) == 1 })
	var all strings.Builder
	for _, m := range r.ch.Messages(work) {
		if m.Marker != "" {
			all.WriteString(m.Text)
		}
	}
	for i := 0; i < 1000; i++ {
		if strings.Count(all.String(), fmt.Sprintf("pressure-%04d", i)) != 1 {
			t.Fatalf("pressure record %d lost or duplicated after backpressure lifted", i)
		}
	}
	if strings.Index(all.String(), "ANSWER to") < strings.Index(all.String(), "pressure-0999") {
		t.Fatal("final overtook unread progress after restart")
	}
}
