package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

type fakeRegistrar struct {
	mu    sync.Mutex
	cards []RegistryCard
	fail  error
}

func (f *fakeRegistrar) Publish(_ context.Context, c RegistryCard) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		err := f.fail
		f.fail = nil
		return err
	}
	f.cards = append(f.cards, c)
	return nil
}

func (f *fakeRegistrar) all() []RegistryCard {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]RegistryCard(nil), f.cards...)
}

// F-XM-1: the machine card names the engine's inbox chat, repeats on the
// heartbeat (also after a failed publish) and says "draining" when the
// helper stops.
func TestRegistryCardHeartbeatAndDrainingOnStop(t *testing.T) {
	reg := &fakeRegistrar{fail: &channel.Throttled{RetryAfter: 50 * time.Millisecond, Err: errors.New("429")}}
	r := newRig(t, func(c *Config) { c.Registry, c.Registrar = true, reg })
	r.control()
	var inbox string
	r.wait("inbox chat", func() bool { inbox = r.chatOf("inbox"); return inbox != "" })
	r.wait("two heartbeats", func() bool { return len(reg.all()) >= 2 })
	for _, c := range reg.all() {
		if c.InboxChat != inbox || c.InboxTitle != "CXP Machine Inbox m" || c.Draining {
			t.Fatalf("card %+v, want inbox %s accepting", c, inbox)
		}
	}
	r.stop()
	cards := reg.all()
	if last := cards[len(cards)-1]; !last.Draining || last.InboxChat != inbox {
		t.Fatalf("last card on stop = %+v, want draining", last)
	}
}

// The card is published as soon as the inbox chat exists, not a control
// interval later: the sender creates the chat under the write pacing and
// wakes the registry loop (e2e INBOX: a card posted 5 s after the start
// fell into the idle window).
func TestRegistryCardPublishedOnceTheInboxExists(t *testing.T) {
	reg := &fakeRegistrar{}
	r := newRig(t, func(c *Config) {
		c.Registry, c.Registrar = true, reg
		c.Timing.Control = time.Minute
	})
	var inbox string
	r.wait("inbox chat", func() bool { inbox = r.chatOf("inbox"); return inbox != "" })
	created := time.Now()
	r.waitFor("card", 5*time.Second, func() bool { return len(reg.all()) >= 1 })
	if lag := time.Since(created); lag > 2*time.Second {
		t.Fatalf("card published %v after the inbox chat was created", lag)
	}
}
