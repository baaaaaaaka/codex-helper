package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Imported or partially initialized state can contain work without a control
// conversation. Recovery must create the warning destination before using it.
func TestUnknownRecoveryCreatesControlBeforeWarning(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "core.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	var token string
	if err := s.Update(func(tx *store.Tx) error {
		conv, err := tx.CreateConversation(store.NewConversation{Label: "s001", Kind: store.KindWork, Channel: "teams", ExtChatID: "existing-work"})
		if err != nil {
			return err
		}
		if _, err := tx.Enqueue(conv, []store.OutMsg{{DeliveryKey: "interrupted-final", Kind: store.OutFinal, Body: "retained final"}}); err != nil {
			return err
		}
		token, err = tx.BeginSend(1)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Store: s, Channel: channeltest.New(), Agent: newAgent()})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.start(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows, err := s.UnknownOutbox()
	if err != nil || len(rows) != 1 || rows[0].AttemptToken != token || rows[0].Body != "retained final" {
		t.Fatalf("recovery changed evidence: %+v %v", rows, err)
	}
	if err := s.Update(func(tx *store.Tx) error {
		rows, err := tx.ActiveOutbox(e.controlID)
		warnings := 0
		for _, row := range rows {
			if strings.Contains(row.Body, "Delivery unconfirmed") {
				warnings++
			}
		}
		if warnings != 1 {
			t.Fatalf("warnings: %d", warnings)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// Only reconciliation is held: incoming commands still use normal Receive.
type heldReadBack struct {
	channel.Channel
	entered chan struct{}
}

func (c heldReadBack) ReadBack(ctx context.Context, _ string, _ time.Time) ([]channel.InboundMsg, error) {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestUnknownMirrorAllowsCommandsAndNextTaskWithoutReadBack(t *testing.T) {
	entered := make(chan struct{}, 1)
	r := newRig(t, func(c *Config) { quickResolve(c); c.Channel = heldReadBack{c.Channel, entered} })
	work := r.newWork(t.TempDir())
	conv := convIDForTest(r, work)
	attempts := absentSends(r, "uncertain progress", 1)
	msgs, err := r.e.render(context.Background(), statusDoc("uncertain progress"), store.OutMirror, "uncertain-progress")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.e.update(func(tx *store.Tx) error { _, err := tx.Enqueue(conv, msgs); return err }); err != nil {
		t.Fatal(err)
	}
	r.e.wakeSender()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("read-back did not start")
	}
	r.ch.PostUser(work, "helper status")
	r.wait("status delivered while read-back is blocked", func() bool { return len(r.own(work, "1 unconfirmed")) == 1 })
	release := make(chan struct{})
	defer close(release)
	r.ag.mu.Lock()
	r.ag.block["slow"] = release
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "slow request")
	r.wait("task starts despite unknown", func() bool { return r.ag.calls("slow") == 1 })
	r.ch.PostUser(work, "helper cancel all")
	r.wait("cancel confirmation delivered", func() bool {
		return len(r.own(work, "cancel all requested")) == 1 && len(r.own(work, "Codex request canceled.")) == 1
	})
	r.stop()
	r.start()
	r.control()
	r.ch.PostUser(work, "after restart")
	r.wait("same chat answers after restart", func() bool { return len(r.own(work, "ANSWER to after restart")) == 1 })
	rows, err := r.st.UnknownOutbox()
	if err != nil || len(rows) != 1 || !strings.Contains(rows[0].Body, "uncertain progress") || rows[0].AttemptToken == "" {
		t.Fatalf("unknown content lost: %+v %v", rows, err)
	}
	if len(attempts()) != 1 || r.ag.calls("after restart") != 1 {
		t.Fatal("restart replayed a send or task")
	}
	if len(r.own(r.control(), "Delivery unconfirmed")) != 1 {
		t.Fatal("missing or duplicate warning")
	}
}

func TestUnknownProgressDoesNotBackpressureTranscript(t *testing.T) {
	sessions := t.TempDir()
	r := newRig(t, func(c *Config) {
		c.SessionsDir = sessions
		c.Agent = recordedAgent{c.Agent.(*fakeAgent), sessions}
		c.Timing.ChatRate = time.Millisecond
	})
	work := r.newWork(t.TempDir())
	const records = 1000
	r.ag.mu.Lock()
	for i := 0; i < records; i++ {
		r.ag.notes = append(r.ag.notes, fmt.Sprintf("unknown-record-%04d", i))
	}
	r.ag.mu.Unlock()
	attempts := absentSends(r, "unknown-record-", records)
	r.ch.PostUser(work, "progress through unknowns")
	r.waitFor("final delivered despite unresolved progress", 90*time.Second, func() bool { return len(r.own(work, "ANSWER to progress through unknowns")) == 1 })
	conv := convIDForTest(r, work)
	var text strings.Builder
	if err := r.st.View(func(q store.Querier) error {
		rows, err := q.QueryContext(context.Background(), `SELECT body FROM outbox WHERE conv_id=? AND progress_group IS NOT NULL`, conv)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var body string
			if err := rows.Scan(&body); err != nil {
				return err
			}
			text.WriteString(body)
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < records; i++ {
		if n := strings.Count(text.String(), fmt.Sprintf("unknown-record-%04d", i)); n != 1 {
			t.Fatalf("retained source record %d appears %d times", i, n)
		}
	}
	if yes, err := r.st.PendingAtLeast(conv, 1); err != nil || yes {
		t.Fatalf("unknown counted as active backlog: %v %v", yes, err)
	}
	before := len(attempts())
	r.stop()
	r.start()
	r.control()
	r.ch.PostUser(work, "following task")
	r.wait("following task answered", func() bool { return len(r.own(work, "ANSWER to following task")) == 1 })
	if len(attempts()) != before {
		t.Fatal("restart replayed unknown progress")
	}
}

func TestUnknownFinalDoesNotAnnounceAnAnswerReady(t *testing.T) {
	r := newRig(t, quickResolve)
	work := r.newWork(t.TempDir())
	attempts := absentSends(r, "ANSWER to lost final", 1)
	r.ch.PostUser(work, "lost final")
	r.wait("unknown final", func() bool { rows, _ := r.st.UnknownOutbox(); return len(rows) == 1 })
	r.ch.PostUser(work, "next answer")
	r.wait("next answer notified", func() bool { return len(r.own(r.control(), "Codex finished")) == 1 })
	if notices := r.own(r.control(), "Codex finished"); !strings.Contains(notices[0].Text, "next answer") || strings.Contains(notices[0].Text, "lost final") {
		t.Fatal("announced an answer that was never confirmed")
	}
	if err := r.e.update(func(tx *store.Tx) error {
		hit, err := tx.HasNotifications()
		if !hit {
			t.Fatal("original notification not retained")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := r.st.UnknownOutbox()
	if err != nil || len(rows) != 1 {
		t.Fatalf("unknown final lost: %+v %v", rows, err)
	}
	row := rows[0]
	if len(attempts()) != 1 {
		t.Fatal("original final resent before confirmation")
	}
	// Only a late view of the original attempt settles the notification.
	if _, err := r.ch.Send(context.Background(), work, channel.Part{Index: 1, Count: 1, Body: row.Body, Hash: fmt.Sprintf("%x", row.BodyHash)}, r.e.marker(row.AttemptToken)); err != nil {
		t.Fatal(err)
	}
	r.wait("late confirmation delivers original completion notice", func() bool { return len(r.own(r.control(), "Codex finished")) == 2 })
	if len(attempts()) != 2 { // original POST plus the test's late-visibility injection
		t.Fatal("original final resent")
	}
}
