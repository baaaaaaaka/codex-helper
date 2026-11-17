package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

type groupedReadbackChannel struct {
	channel.Channel
	read func(context.Context, string, time.Time) ([]channel.InboundMsg, error)
}

func (c *groupedReadbackChannel) ReadBack(ctx context.Context, chat string, since time.Time) ([]channel.InboundMsg, error) {
	return c.read(ctx, chat, since)
}

func resolverGroupFixture(t *testing.T, chats int) (*Engine, *store.Store, []store.Outbox, *groupedReadbackChannel) {
	t.Helper()
	now := time.Now()
	s, err := store.OpenWith(filepath.Join(t.TempDir(), "core.db"), store.Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if _, err := s.Acquire(store.Ident{Host: "test", PID: 1, BootID: "test"}); err != nil {
		t.Fatal(err)
	}
	ch := &groupedReadbackChannel{Channel: channeltest.New()}
	e, err := New(Config{Store: s, Channel: ch, Agent: newAgent()})
	if err != nil {
		t.Fatal(err)
	}
	e.now = func() time.Time { return now }
	var rows []store.Outbox
	for c := 0; c < chats; c++ {
		chat := fmt.Sprintf("chat-%d", c)
		var id int64
		if err := s.Update(func(tx *store.Tx) error {
			var err error
			id, err = tx.CreateConversation(store.NewConversation{Label: chat, Kind: store.KindWork, Channel: "teams", ExtChatID: chat})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		e.trackLocked(store.Conversation{ID: id, Label: chat, ExtChatID: chat, Kind: store.KindWork, State: store.ConvActive})
		for i := 0; i < 3; i++ {
			if err := s.Update(func(tx *store.Tx) error {
				if _, err := tx.Enqueue(id, []store.OutMsg{{DeliveryKey: fmt.Sprintf("%s-%d", chat, i), Kind: store.OutFinal, Body: "same body"}}); err != nil {
					return err
				}
				r, _, err := tx.LastOutbox(id, store.OutFinal)
				if err != nil {
					return err
				}
				if _, err := tx.BeginSend(r.ID); err != nil {
					return err
				}
				if err := tx.MarkUnknown(r.ID, "ambiguous"); err != nil {
					return err
				}
				r, _, err = tx.Outbox(r.ID)
				rows = append(rows, r)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			now = now.Add(2 * time.Minute)
		}
	}

	for _, r := range rows {
		e.noteUnknown(r.ID, now.Add(-time.Hour))
	}
	return e, s, rows, ch
}

func TestHotResolverGroupingRespectsGatesAndCancellation(t *testing.T) {
	for _, scope := range []channel.ThrottleScope{channel.ScopeChat, channel.ScopeAccount} {
		t.Run(scope.String(), func(t *testing.T) {
			e, _, _, ch := resolverGroupFixture(t, 2)
			calls := map[string]int{}
			ch.read = func(_ context.Context, chat string, _ time.Time) ([]channel.InboundMsg, error) {
				calls[chat]++
				if chat == "chat-0" {
					return nil, &channel.Throttled{Scope: scope, RetryAfter: time.Minute}
				}
				return nil, nil
			}
			e.resolvePass(context.Background())
			wantOther := 1
			if scope == channel.ScopeAccount {
				wantOther = 0
			}
			if calls["chat-0"] != 1 || calls["chat-1"] != wantOther {
				t.Fatal(calls)
			}
		})
	}
	t.Run("cancel", func(t *testing.T) {
		e, _, _, ch := resolverGroupFixture(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		ch.read = func(context.Context, string, time.Time) ([]channel.InboundMsg, error) {
			calls++
			cancel()
			return nil, context.Canceled
		}
		e.resolvePass(ctx)
		if calls != 1 {
			t.Fatal(calls)
		}
	})
	t.Run("not-due", func(t *testing.T) {
		e, _, _, ch := resolverGroupFixture(t, 1)
		for _, u := range e.unknown {
			u.next = e.now().Add(time.Hour)
		}
		ch.read = func(context.Context, string, time.Time) ([]channel.InboundMsg, error) {
			t.Fatal("read before due")
			return nil, nil
		}
		e.resolvePass(context.Background())
	})
}

func TestHotResolverDoesNotSettleNewAttemptFromStaleReadback(t *testing.T) {
	e, s, rows, ch := resolverGroupFixture(t, 1)
	r := rows[0]
	ch.read = func(context.Context, string, time.Time) ([]channel.InboundMsg, error) {
		if err := s.Update(func(tx *store.Tx) error {
			if err := tx.Requeue(r.ID, 0); err != nil {
				return err
			}
			_, err := tx.BeginSend(r.ID)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return []channel.InboundMsg{{ID: "old-post", Marker: e.marker(r.AttemptToken)}}, nil
	}
	e.resolvePass(context.Background())
	if err := s.Read(func(tx *store.Tx) error {
		current, _, err := tx.Outbox(r.ID)
		if current.State != store.OutSending || current.AttemptToken == r.AttemptToken {
			t.Fatalf("stale result changed new attempt: %+v", current)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestHotResolverSharedWindowKeepsRowTimeBoundary(t *testing.T) {
	e, s, rows, ch := resolverGroupFixture(t, 1)
	// A late-created row cannot match a marker outside its own read window,
	// even though an earlier row caused the shared request to include it.
	r := rows[1]
	calls := 0
	ch.read = func(_ context.Context, _ string, since time.Time) ([]channel.InboundMsg, error) {
		calls++
		if !since.Equal(fromMS(rows[0].CreatedAt).Add(-time.Minute)) {
			t.Fatal(since)
		}
		return []channel.InboundMsg{{ID: "too-old", Marker: e.marker(r.AttemptToken), Modified: fromMS(r.CreatedAt).Add(-2 * time.Minute)}}, nil
	}
	e.resolvePass(context.Background())
	if calls != 1 {
		t.Fatal(calls)
	}
	if err := s.Read(func(tx *store.Tx) error {
		current, _, err := tx.Outbox(r.ID)
		if current.State != store.OutUnknown {
			t.Fatal(current.State)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
