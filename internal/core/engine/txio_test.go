package engine

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// No write transaction spans I/O: a frozen or slow call inside one would
// hold the store's write lock (a standby could never take over), and a
// callee that writes the store itself would deadlock on the one writer
// connection.

// writerFree tries a real store write. The timeout bounds a regression that
// deadlocks on the caller's transaction; it is not a latency assertion.
func writerFree(st *store.Store) error {
	done := make(chan error, 1)
	go func() {
		done <- st.Update(func(*store.Tx) error { return nil })
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		return errors.New("store write blocked while probing an external call")
	}
}

// probeNotifier's Available reads files and may write the store (the CLI
// applies a pending Workflow change there).
type probeNotifier struct {
	st        *store.Store
	available int
	cards     int
	err       error
}

func (n *probeNotifier) Available() bool {
	n.available++
	n.err = errors.Join(n.err, writerFree(n.st))
	return true
}

func (n *probeNotifier) Notify(context.Context, channel.OwnerNotice) error {
	n.cards++
	return nil
}

func TestNotifierAvailabilityIsCheckedOutsideTheWriteTransaction(t *testing.T) {
	// Run the notification pump alone. This checks the transaction boundary
	// without starting receivers, senders or unrelated startup notifications.
	st, err := store.Open(filepath.Join(t.TempDir(), "core.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	defer st.Release()
	n := &probeNotifier{st: st}
	e, err := New(Config{Store: st, Channel: channeltest.New(), Agent: newAgent(), Notifier: n})
	if err != nil {
		t.Fatal(err)
	}
	// Seed one due notification, so the test also proves the availability
	// result is used by a real delivery, without counting startup cards.
	var conv int64
	if err := e.update(func(tx *store.Tx) error {
		var err error
		conv, err = tx.CreateConversation(store.NewConversation{Label: "control", Kind: store.KindControl, Channel: "teams", ExtChatID: "control"})
		if err != nil {
			return err
		}
		return e.addNotify(tx, pendingNotify{Conv: conv, Key: "boundary-probe", Kind: notifyService, Request: "test", Text: "transaction boundary"})
	}); err != nil {
		t.Fatal(err)
	}
	e.convs[conv] = &convState{id: conv, kind: store.KindControl, chat: "control"}
	e.pumpNotify(context.Background())
	if n.available == 0 || n.cards != 1 {
		t.Fatalf("availability checks=%d, delivered cards=%d; want a checked, delivered notification", n.available, n.cards)
	}
	if n.err != nil {
		t.Fatalf("availability could not write the store outside the engine transaction: %v", n.err)
	}
}

// probeRenderChannel records math renders (a local renderer process) made
// while the caller holds the write transaction.
type probeRenderChannel struct {
	*channeltest.Fake
	st     *store.Store
	inTx   atomic.Int32
	render atomic.Int32
}

func (c *probeRenderChannel) Render(ctx context.Context, doc channel.RichDoc) ([]channel.Part, error) {
	if doc.Math && c.st != nil {
		c.render.Add(1)
		if writerFree(c.st) != nil {
			c.inTx.Add(1)
		}
	}
	return c.Fake.Render(ctx, doc)
}

func TestImportedUnsentMessagesAreRenderedOutsideTheWriteTransaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.sqlite")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Import(func(im *store.Importer) error {
		conv, err := im.Conversation(store.Conversation{Label: "s1", Kind: store.KindWork, Channel: "teams", ExtChatID: "chat-legacy", State: store.ConvActive})
		if err != nil {
			return err
		}
		at := im.Now()
		_, _, err = im.Outbox(store.Outbox{ConvID: conv, Seq: 1, DeliveryKey: "legacy-outbox:1", Kind: store.OutFinal, Body: "unsent answer",
			BodyHash: []byte{1}, HashAlgo: store.HashLegacy, State: store.OutQueued, CreatedAt: at}, nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	defer st.Release()
	ch := &probeRenderChannel{Fake: channeltest.New(), st: st}
	var logMu sync.Mutex
	var log strings.Builder
	e, err := New(Config{Store: st, Channel: ch, Agent: newAgent(), Timing: FastTiming(20 * time.Millisecond), Machine: "m",
		Log: lockedWriter{&logMu, &log}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(15 * time.Second)
	for {
		logMu.Lock()
		rendered := strings.Contains(log.String(), "rendered 1 imported unsent messages")
		logMu.Unlock()
		if rendered {
			break
		}
		if time.Now().After(deadline) {
			logMu.Lock()
			defer logMu.Unlock()
			t.Fatalf("the imported unsent message was not rendered; log:\n%s", log.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ch.render.Load() == 0 {
		t.Fatal("no math render of the imported answer")
	}
	if c := ch.inTx.Load(); c != 0 {
		t.Fatalf("%d math renders ran inside a write transaction", c)
	}
}
