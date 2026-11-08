package engine

import (
	"context"
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

// writerFree reports whether a write transaction can start within 2 s. It
// cannot when the calling goroutine itself holds one; other goroutines'
// transactions are short.
func writerFree(st *store.Store) bool {
	done := make(chan struct{})
	go func() {
		_ = st.Update(func(*store.Tx) error { return nil })
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(2 * time.Second):
		return false
	}
}

// probeNotifier's Available reads files and may write the store (the CLI
// applies a pending Workflow change there).
type probeNotifier struct {
	st    atomic.Pointer[store.Store]
	inTx  atomic.Int32
	cards atomic.Int32
}

func (n *probeNotifier) Available() bool {
	if st := n.st.Load(); st != nil && !writerFree(st) {
		n.inTx.Add(1)
	}
	return true
}

func (n *probeNotifier) Notify(context.Context, channel.OwnerNotice) error {
	n.cards.Add(1)
	return nil
}

func TestNotifierAvailabilityIsCheckedOutsideTheWriteTransaction(t *testing.T) {
	n := &probeNotifier{}
	r := newRig(t, func(c *Config) { c.Notifier = n })
	n.st.Store(r.st)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "card me")
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to card me")) == 1 })
	r.wait("card", func() bool { return n.cards.Load() == 1 })
	if c := n.inTx.Load(); c != 0 {
		t.Fatalf("the Workflow availability was checked %d times inside a write transaction", c)
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
		if !writerFree(c.st) {
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
