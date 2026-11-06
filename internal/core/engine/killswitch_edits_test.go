package engine

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// The kill switch (`cxp teams stop`, `shutdown`) is the owner's stop: the
// chat edits still queued when the engine stops are made before it exits,
// as at a graceful stop. Before, an automatic title queued right after an
// answer was lost when the stop came first, and the next process named the
// chat by its old title in the following notification (STOP-RESTART).
func TestTheKillSwitchStopMakesTheQueuedEdits(t *testing.T) {
	r := newRig(t, func(c *Config) { c.Timing.Heartbeat = 50 * time.Millisecond })
	work := r.newWork(t.TempDir())
	var conv int64
	convs, _ := r.st.Conversations()
	for _, c := range convs {
		if c.ExtChatID == work {
			conv = c.ID
		}
	}
	var (
		mu    sync.Mutex
		calls int
		made  string
	)
	started := make(chan struct{})
	r.e.queueEdit(conv, chatEdit{key: "title", what: "chat title", run: func(ctx context.Context, chat string) error {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			close(started)
			<-ctx.Done() // in flight when the stop comes
			return ctx.Err()
		}
		mu.Lock()
		made = chat
		mu.Unlock()
		return nil
	}})
	r.e.wakeSender()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the edit did not start")
	}
	if _, err := store.WriteStop(filepath.Dir(r.path), store.StopRecord{Source: store.StopByCLI, Host: "h", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	var runErr error
	select {
	case runErr = <-r.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the engine kept running after the kill switch")
	}
	r.done <- runErr // for r.stop
	if !errors.Is(runErr, store.ErrDisabled) {
		t.Fatalf("Run returned %v, want store.ErrDisabled", runErr)
	}
	mu.Lock()
	defer mu.Unlock()
	if made != work || calls != 2 {
		t.Fatalf("the queued edit was made in %q after %d runs, want it made in the work chat %q at the stop", made, calls, work)
	}
}
