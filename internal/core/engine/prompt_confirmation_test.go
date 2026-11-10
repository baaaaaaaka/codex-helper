package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

type confirmationChannel struct {
	*channeltest.Fake
	mu      sync.Mutex
	applied map[string]bool
	calls   int
	lost    bool
	refuse  bool
}

func (c *confirmationChannel) Annotate(_ context.Context, _, _ string, a channel.Annotation) error {
	if a.Confirmation == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.refuse {
		return channel.ErrNotEditable
	}
	if c.applied[a.ConfirmationKey] {
		return nil
	}
	c.applied[a.ConfirmationKey] = true
	if c.lost {
		c.lost = false
		return &channel.Ambiguous{Err: errors.New("PATCH response lost")}
	}
	return nil
}
func (c *confirmationChannel) count() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.applied) }

func TestPromptConfirmationReadThrottleFallsBack(t *testing.T) {
	r := newRig(t, func(c *Config) {
		c.Channel = &confirmationChannel{Fake: c.Channel.(*channeltest.Fake), applied: map[string]bool{}}
	})
	r.e.setGate(r.e.gRead, r.e.now().Add(time.Minute), "test read throttle")
	err := r.e.appendConfirmation(context.Background(), store.Outbox{}, "", channel.Part{})
	if !errors.Is(err, channel.ErrNotEditable) {
		t.Fatalf("read budget must select POST fallback: %v", err)
	}
	err = r.e.appendConfirmation(context.Background(), store.Outbox{NextAttemptAt: ms(r.e.now())}, "", channel.Part{})
	if !errors.Is(err, errEditLater) {
		t.Fatalf("an earlier PATCH must be read back before changing delivery: %v", err)
	}
}

func TestPromptConfirmationExcludedFromCopyNotice(t *testing.T) {
	want := textCopySkipped("other-host", []string{"original request"})
	got := textCopySkipped("other-host", []string{labelUser + ":\noriginal request\n———\n" + labelHelper + ":\n" + textAccepted})
	if got != want {
		t.Fatalf("copied request includes helper receipt: %q", got)
	}
}

func TestPromptConfirmationInitialOnlyAndUnknownRetry(t *testing.T) {
	var ch *confirmationChannel
	r := newRig(t, func(c *Config) {
		ch = &confirmationChannel{Fake: c.Channel.(*channeltest.Fake), applied: map[string]bool{}, lost: true}
		c.Channel = ch
	})
	work := r.newWork(t.TempDir())
	m := r.ch.PostUser(work, "ordinary receipt")
	r.wait("initial confirmation appended", func() bool { return ch.count() == 1 && r.ag.calls("ordinary receipt") == 1 })
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to")) == 1 })
	if len(r.own(work, "Request accepted")) != 0 {
		t.Fatal("separate confirmation after lost PATCH")
	}
	ch.mu.Lock()
	calls := ch.calls
	ch.mu.Unlock()
	if calls < 2 {
		t.Fatal("ambiguous PATCH was not retried")
	}
	conv := convIDForTest(r, work)
	if err := r.st.Read(func(tx *store.Tx) error {
		own, err := tx.IsOwn(conv, m.ID, nil, 0)
		if own {
			t.Error("user message registered as own post")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	r.ch.PostUser(work, "ordinary receipt")
	r.wait("duplicate appended", func() bool { return ch.count() == 2 })
	if len(r.own(work, "Duplicate request ignored")) != 0 {
		t.Fatal("duplicate receipt was posted separately")
	}
	r.ch.PostUser(work, "helper status")
	r.wait("status remains separate", func() bool { return len(r.own(work, "Directory:")) > 0 })
}

func TestPromptConfirmationDurableFallback(t *testing.T) {
	var ch *confirmationChannel
	r := newRig(t, func(c *Config) {
		ch = &confirmationChannel{Fake: c.Channel.(*channeltest.Fake), applied: map[string]bool{}, refuse: true}
		c.Channel = ch
	})
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "fallback receipt")
	r.wait("separate fallback", func() bool { return len(r.own(work, "Request accepted")) == 1 })
	rows, err := r.st.OutboxByKeyPrefix("ack:")
	if err != nil || len(rows) != 1 || rows[0].AppendTo != "" || rows[0].State != store.OutSent || rows[0].ExtMsgID == "" {
		t.Fatalf("fallback not persisted: %+v %v", rows, err)
	}
	r.stop()
	r.start()
	r.wait("recovered", func() bool { return len(r.own(work, "Request accepted")) == 1 })
}

func TestPromptConfirmationScope(t *testing.T) {
	r := newRig(t)
	for _, text := range []string{textAccepted, "Received. Adding this input to Codex", textQueuedAck("running", true, 1), textDuplicate, textHeld, textDeferredRestart, textDeferredUpgrade, textControlAck(12)} {
		for _, sender := range []string{store.SenderOwner, store.SenderOther} {
			c := &classified{in: store.InboundMsg{Sender: sender, Kind: store.InPrompt, ExtMsgID: "user-message"}}
			msgs, err := r.e.promptConfirmation(c, store.OutAck, "ack:test", text, "")
			if err != nil || len(msgs) != 1 {
				t.Fatalf("render: %+v %v", msgs, err)
			}
			if (msgs[0].AppendTo != "") != (sender == store.SenderOwner) {
				t.Fatalf("wrong scope: %+v", msgs)
			}
		}
	}
	msgs, err := r.e.noticeReply(store.OutNotice, "turn:waiting", "still queued", "")
	if err != nil || msgs[0].AppendTo != "" || !strings.Contains(msgs[0].Body, "still queued") {
		t.Fatal("follow-up changed")
	}
}

func TestPromptConfirmationRestartAfterAppliedPatch(t *testing.T) {
	var ch *confirmationChannel
	r := newRig(t, func(c *Config) {
		if ch == nil {
			ch = &confirmationChannel{Fake: c.Channel.(*channeltest.Fake), applied: map[string]bool{}}
		}
		c.Channel = ch
	})
	work := r.newWork(t.TempDir())
	msg := r.ch.PostUser(work, "restart receipt")
	r.wait("original task complete", func() bool { return len(r.own(work, "ANSWER to")) == 1 })
	conv := convIDForTest(r, work)
	rows, err := r.st.OutboxByKeyPrefix("ack:")
	if err != nil || len(rows) != 1 {
		t.Fatalf("ack: %+v %v", rows, err)
	}
	body := rows[0].Body
	r.stop()
	// Leave a durable sending row exactly as a process killed after PATCH,
	// before MarkSent. The channel has already applied that key.
	st, err := store.OpenWith(r.path, store.Options{Exclusive: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	const key = "ack:crash:1"
	err = st.Update(func(tx *store.Tx) error {
		if _, err := tx.Enqueue(conv, []store.OutMsg{{DeliveryKey: key, Kind: store.OutAck, Body: body, AppendTo: msg.ID}}); err != nil {
			return err
		}
		pending, err := tx.PendingOutbox(conv)
		if err != nil {
			return err
		}
		for _, row := range pending {
			if row.DeliveryKey == key {
				_, err = tx.BeginSend(row.ID)
				return err
			}
		}
		return errors.New("missing crash row")
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Release(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	ch.mu.Lock()
	ch.applied[key] = true
	ch.mu.Unlock()
	r.start()
	r.wait("applied confirmation recovered", func() bool {
		rows, err := r.st.OutboxByKeyPrefix(key)
		return err == nil && len(rows) == 1 && rows[0].State == store.OutSent && rows[0].ExtMsgID == ""
	})
	if ch.count() != 2 || len(r.own(work, "Request accepted")) != 0 {
		t.Fatal("recovery duplicated confirmation")
	}
}
