package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

type tuiAgent struct {
	*fakeAgent
	mu     sync.Mutex
	leases int
	done   chan struct{}
	bind   func(string) error
}

func (a *tuiAgent) Attach(_ context.Context, _ int64, _, _ string, bind func(string) error) (TUIEndpoint, func(), <-chan struct{}, error) {
	a.mu.Lock()
	a.leases++
	a.bind = bind
	a.mu.Unlock()
	return TUIEndpoint{URL: "ws://127.0.0.1:10000", Token: "test"}, func() { a.mu.Lock(); a.leases--; a.mu.Unlock() }, a.done, nil
}
func (a *tuiAgent) Active() bool { return false }

func TestNewTUIBindsNativeThreadOrClosesUnboundConversation(t *testing.T) {
	for _, created := range []bool{false, true} {
		t.Run(map[bool]string{false: "creation-failed", true: "native-created"}[created], func(t *testing.T) {
			a := &tuiAgent{fakeAgent: newAgent(), done: make(chan struct{})}
			r := newRig(t, func(c *Config) { c.Agent = a; c.SessionsDir = t.TempDir() })
			r.wait("TUI admission ready", func() bool {
				r.e.mu.Lock()
				defer r.e.mu.Unlock()
				return r.e.attachReady
			})
			ep, release, _, err := r.e.AttachNew(context.Background(), t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if ep.ThreadID != "" {
				t.Fatal("helper pre-created a thread")
			}
			if created {
				if err := a.bind("native-thread"); err != nil {
					t.Fatal(err)
				}
			}
			release()
			release()
			r.e.mu.Lock()
			defer r.e.mu.Unlock()
			if r.e.tuiLeases != 0 {
				t.Fatal("TUI lease leaked")
			}
			found := 0
			for _, c := range r.e.convs {
				if c.kind != store.KindWork {
					continue
				}
				found++
				if created {
					if c.threadID != "native-thread" || r.e.byThread[c.threadID] != c.id || c.state == store.ConvClosed {
						t.Fatal("native binding lost")
					}
				} else if c.state != store.ConvClosed {
					t.Fatal("unbound conversation leaked")
				}
			}
			if found != 1 {
				t.Fatalf("work conversations: %d", found)
			}
		})
	}
}

func TestOpenTUIBlocksUpgradeWithoutDrainingNormalWork(t *testing.T) {
	a := &tuiAgent{fakeAgent: newAgent(), done: make(chan struct{})}
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "seed")
	r.wait("seed", func() bool { return len(r.own(work, "ANSWER to seed")) == 1 })
	thread := a.runLog()[0].ThreadID
	if thread == "" {
		thread = "thread-1"
	}
	_, release, _, err := r.e.Attach(context.Background(), thread)
	if err != nil {
		t.Fatal(err)
	}
	drained := make(chan bool, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { drained <- r.e.drain(ctx, 5*time.Second) }()
	// New work must still be admitted while the updater waits for a window.
	r.ch.PostUser(work, "while window open")
	r.wait("work continues during update wait", func() bool { return len(r.own(work, "ANSWER to while window open")) == 1 })
	if r.e.draining() {
		t.Fatal("open TUI caused global drain")
	}
	release()
	release()
	select {
	case ok := <-drained:
		if !ok {
			t.Fatal("update did not drain after TUI closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain stuck")
	}
	a.mu.Lock()
	n := a.leases
	a.mu.Unlock()
	if n != 0 {
		t.Fatalf("release count %d", n)
	}
	r.e.undrain()
}
