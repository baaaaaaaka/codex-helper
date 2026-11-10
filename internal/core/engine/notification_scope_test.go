package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func TestNotificationReadBackWakesOnlyAffectedNotices(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "core.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Store: s, Channel: channeltest.New(), Agent: newAgent()})
	if err != nil {
		t.Fatal(err)
	}
	e.prefix = "test"
	var conv int64
	var token string
	if err = s.Update(func(tx *store.Tx) error {
		conv, err = tx.CreateConversation(store.NewConversation{Label: "s001", Kind: store.KindWork, Channel: "teams", ExtChatID: "work"})
		if err != nil {
			return err
		}
		if _, err = tx.Enqueue(conv, []store.OutMsg{{DeliveryKey: "turn:42:final:1", Kind: store.OutFinal, Body: "answer"}}); err != nil {
			return err
		}
		token, err = tx.BeginSend(1)
		if err != nil {
			return err
		}
		return tx.MarkUnknown(1, "lost response")
	}); err != nil {
		t.Fatal(err)
	}
	if err = e.update(func(tx *store.Tx) error {
		c, keep, err := e.classify(tx, conv, store.KindWork, false, channel.InboundMsg{ID: "external", Marker: e.marker(token)})
		if err == nil && (!keep || !c.resolved) {
			t.Error("read-back did not resolve unknown send")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, scope := range e.notifyDirty {
		if scope == (store.NotificationScope{}) {
			t.Fatal("read-back requested a full pending scan")
		}
		found = found || scope.ID == "turn:42"
	}
	if !found {
		t.Fatal("read-back lost its notification wake", e.notifyDirty)
	}
}

func TestNotificationWakeIsAfterCommit(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "core.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	e := &Engine{st: s, notifyWake: make(chan struct{}, 1)}
	rollback := errors.New("rollback")
	if err := e.update(func(tx *store.Tx) error {
		if err := e.addNotify(tx, pendingNotify{Key: "rolled-back"}); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if len(e.notifyDirty) != 0 {
		t.Fatal("rollback emitted a notification wake")
	}
	inserted, commit, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- e.update(func(tx *store.Tx) error {
			if err := e.addNotify(tx, pendingNotify{Key: "committed"}); err != nil {
				return err
			}
			close(inserted)
			<-commit
			return nil
		})
	}()
	<-inserted
	e.notifyMu.Lock()
	woke := len(e.notifyDirty) > 0
	e.notifyMu.Unlock()
	close(commit)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if woke {
		t.Fatal("uncommitted notification emitted a wake")
	}
	if len(e.notifyDirty) != 1 || e.notifyDirty[0].ID != "committed" {
		t.Fatal(e.notifyDirty)
	}
	if err := s.Read(func(tx *store.Tx) error {
		_, ok, err := tx.Notification("committed")
		if !ok {
			t.Error("wake preceded durable row")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestNotificationWakeCoalescesWithinBound(t *testing.T) {
	e := &Engine{notifyWake: make(chan struct{}, 1)}
	for range 500 {
		e.wakeNotify(store.NotificationScope{ID: "one"})
	}
	if len(e.notifyDirty) != 1 {
		t.Fatal("duplicate hints accumulated")
	}
	for i := 0; i < 300; i++ {
		e.wakeNotify(store.NotificationScope{ID: fmt.Sprint(i)})
	}
	if len(e.notifyDirty) != 1 || e.notifyDirty[0] != (store.NotificationScope{}) {
		t.Fatal("overflow did not become a recovery pass")
	}
}
