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
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

type hotReadBackCounter struct {
	channel.Channel
	calls int
}

func (c *hotReadBackCounter) ReadBack(context.Context, string, time.Time) ([]channel.InboundMsg, error) {
	c.calls++
	return nil, nil
}
func TestHotResolverSharesSameChatWindow(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "core.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Acquire(store.Ident{Host: "test", PID: 1, BootID: "test"}); err != nil {
		t.Fatal(err)
	}
	var conv int64
	var ids []int64
	err = s.Update(func(tx *store.Tx) error {
		var e error
		conv, e = tx.CreateConversation(store.NewConversation{Label: "audit", Kind: store.KindWork, Channel: "teams", ExtChatID: "same-chat"})
		if e != nil {
			return e
		}
		for i := 0; i < 20; i++ {
			if _, e := tx.Enqueue(conv, []store.OutMsg{{DeliveryKey: fmt.Sprint(i), Kind: store.OutFinal, Body: "body"}}); e != nil {
				return e
			}
			row, ok, e := tx.LastOutbox(conv, store.OutFinal)
			if e != nil || !ok {
				return fmt.Errorf("outbox: %v", e)
			}
			if _, e := tx.BeginSend(row.ID); e != nil {
				return e
			}
			if e := tx.MarkUnknown(row.ID, "injected ambiguity"); e != nil {
				return e
			}
			ids = append(ids, row.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ch := &hotReadBackCounter{Channel: channeltest.New()}
	e, err := New(Config{Store: s, Channel: ch, Agent: newAgent()})
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.trackLocked(store.Conversation{ID: conv, Kind: store.KindWork, State: store.ConvActive, ExtChatID: "same-chat", Label: "audit"})
	e.mu.Unlock()
	for _, id := range ids {
		e.noteUnknown(id, time.Now().Add(-10*time.Second))
	}
	e.resolvePass(context.Background())
	if ch.calls != 1 {
		t.Fatalf("read-back calls=%d", ch.calls)
	}
	t.Logf("%d unknown rows in one chat produce %d overlapping ReadBack calls in one resolver pass", len(ids), ch.calls)
}

func BenchmarkHotMirrorNotDue(b *testing.B) {
	now := time.Unix(1000000, 0)
	e := &Engine{now: func() time.Time { return now }, mirror: &mirrorState{files: map[string]*mirrorFile{}}}
	for i := 0; i < 10000; i++ {
		e.mirror.files[fmt.Sprint(i)] = &mirrorFile{nextStat: now.Add(time.Minute)}
	}
	e.statFile = func(string) (transcript.FileInfo, error) {
		b.Fatal("stat before deadline")
		return transcript.FileInfo{}, nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		e.mirrorPoll(context.Background())
	}
}
