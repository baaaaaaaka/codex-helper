package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

type questionDecorationChannel struct {
	*channeltest.Fake
	failClear, overflow bool
	last                channel.PendingQuestions
}

func (c *questionDecorationChannel) SetPendingQuestions(ctx context.Context, chat, id string, p channel.PendingQuestions) error {
	if c.failClear && len(p.Items) == 0 {
		return errors.New("injected cleanup failure")
	}
	if c.overflow && len(p.Items) > 2 {
		return channel.ErrQuestionDecorationTooLarge
	}
	if len(p.Items) > 0 {
		c.last = p
	}
	return c.Fake.SetPendingQuestions(ctx, chat, id, p)
}

// Exercise the durable handoff without background scheduling or timing races.
func TestPendingQuestionDecorationRecoversInterruptedHandoff(t *testing.T) {
	e, c, conv, chat := questionDecorationFixture(t, 1)
	old := postDecorationMessage(t, e, c, conv, chat, "old")
	run := func() error {
		e.queueQuestionMarker(conv)
		ed := e.edits[conv][0]
		delete(e.edits, conv)
		return ed.run(context.Background(), chat)
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	latest := postDecorationMessage(t, e, c, conv, chat, "new")
	c.failClear = true
	if err := run(); err == nil {
		t.Fatal("cleanup failure was not observed")
	}
	// The replacement must already be decorated when old-message cleanup fails.
	if marked := decorationIDs(c.Fake, chat); len(marked) != 2 {
		t.Fatalf("lost navigation during handoff: %v", marked)
	}
	if err := e.st.Read(func(tx *store.Tx) error {
		v, _, err := tx.Meta(questionMarkerKey(conv))
		previous, _, err := tx.Meta(questionMarkerPreviousKey(conv))
		if err != nil {
			return err
		}
		if v != latest || previous != old {
			t.Fatalf("handoff not recoverable: %q", v)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	c.failClear = false
	recovered, err := New(e.cfg)
	if err != nil {
		t.Fatal(err)
	}
	recovered.resumeQuestionMarkers()
	ed := recovered.edits[conv][0]
	if err := ed.run(context.Background(), chat); err != nil {
		t.Fatal(err)
	}
	marked := decorationIDs(c.Fake, chat)
	if len(marked) != 1 || marked[0] != latest || len(c.Messages(chat)) != 2 {
		t.Fatalf("recovery duplicated messages or retained marker: %v", marked)
	}
}

func TestPendingQuestionDecorationPublishesCompleteDirectory(t *testing.T) {
	e, c, conv, chat := questionDecorationFixture(t, 3)
	for i := 1; i <= 3; i++ {
		postDecorationMessage(t, e, c, conv, chat, fmt.Sprintf("question:%d:body:1", i))
	}
	postDecorationMessage(t, e, c, conv, chat, "status")
	c.overflow = true
	run := func() error {
		e.queueQuestionMarker(conv)
		ed := e.edits[conv][0]
		delete(e.edits, conv)
		return ed.run(context.Background(), chat)
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	var rows []store.Outbox
	if err := e.st.Read(func(tx *store.Tx) error { var err error; rows, err = tx.PendingOutbox(conv); return err }); err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 || !strings.HasPrefix(rows[0].DeliveryKey, "question-directory:") {
		t.Fatalf("no directory: %+v", rows)
	}
	for _, row := range rows {
		sent, err := c.Send(context.Background(), chat, channel.Part{Body: row.Body}, row.DeliveryKey)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.st.Update(func(tx *store.Tx) error {
			if _, err := tx.BeginSend(row.ID); err != nil {
				return err
			}
			return tx.MarkSent(row.ID, sent.ID)
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if c.last.Count != 3 || len(c.last.Items) != len(rows) {
		t.Fatalf("directory count/navigation: %+v", c.last)
	}
	all := ""
	for _, row := range rows {
		all += row.Body
	}
	for _, id := range []string{"Q1", "Q2", "Q3"} {
		if !strings.Contains(all, id) {
			t.Fatal("directory omitted " + id)
		}
	}
	if marked := decorationIDs(c.Fake, chat); len(marked) != 1 {
		t.Fatal(marked)
	}
}

func decorationIDs(c *channeltest.Fake, chat string) []string {
	var ids []string
	for _, m := range c.Messages(chat) {
		if strings.HasPrefix(m.Text, questionMarkerPrefix+"\n") {
			ids = append(ids, m.ID)
		}
	}
	return ids
}
func questionDecorationFixture(t *testing.T, n int) (*Engine, *questionDecorationChannel, int64, string) {
	t.Helper()
	s, err := store.OpenWith(filepath.Join(t.TempDir(), "core.db"), store.Options{Exclusive: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire(store.Ident{Host: "test", PID: 1, BootID: "test"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Release(); s.Close() })
	c := &questionDecorationChannel{Fake: channeltest.New()}
	chat, err := c.CreateChat(context.Background(), "questions", "questions")
	if err != nil {
		t.Fatal(err)
	}
	var conv int64
	if err := s.Update(func(tx *store.Tx) error {
		var err error
		conv, err = tx.CreateConversation(store.NewConversation{Label: "questions", Kind: store.KindWork, Channel: "teams", ExtChatID: chat.ID})
		if err != nil {
			return err
		}
		g := questionGroup(true, n)
		g.ConvID = conv
		_, err = tx.AddQuestionGroup(g)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Store: s, Channel: c, Agent: newAgent(), Timing: FastTiming(time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}
	return e, c, conv, chat.ID
}
func postDecorationMessage(t *testing.T, e *Engine, c *questionDecorationChannel, conv int64, chat, body string) string {
	t.Helper()
	sent, err := c.Send(context.Background(), chat, channel.Part{Body: body}, body)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.st.Update(func(tx *store.Tx) error {
		if _, err := tx.Enqueue(conv, []store.OutMsg{{DeliveryKey: body, Kind: store.OutNotice, Body: body}}); err != nil {
			return err
		}
		rows, err := tx.PendingOutbox(conv)
		if err != nil {
			return err
		}
		if _, err := tx.BeginSend(rows[0].ID); err != nil {
			return err
		}
		return tx.MarkSent(rows[0].ID, sent.ID)
	}); err != nil {
		t.Fatal(err)
	}
	return sent.ID
}

func TestPendingQuestionDecorationWaitsForPublicationBeforeDirectory(t *testing.T) {
	e, c, conv, chat := questionDecorationFixture(t, 3)
	postDecorationMessage(t, e, c, conv, chat, "status")
	c.overflow = true
	e.queueQuestionMarker(conv)
	if err := e.edits[conv][0].run(context.Background(), chat); err != nil {
		t.Fatal(err)
	}
	if c.last.Count != 3 || len(c.last.Items) != 1 || c.last.Items[0].MessageID != "" {
		t.Fatalf("publication summary: %+v", c.last)
	}
	if err := e.st.Read(func(tx *store.Tx) error {
		rows, err := tx.PendingOutbox(conv)
		if len(rows) != 0 {
			t.Fatal("incomplete publication generated directories")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
