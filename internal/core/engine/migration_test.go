package engine

import (
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// The converter's summary is posted to the control chat exactly once, also
// across a restart, and the meta is cleared in the same transaction.
func TestMigrationSummaryPostedOnce(t *testing.T) {
	r := newRig(t)
	ctl := r.control()
	r.stop()

	st, err := store.Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Update(func(tx *store.Tx) error {
		return tx.SetMeta(store.MetaMigrationSummary, "Teams helper data converted from v0.1.22-rc.59\n\nWork chats: 3.")
	}); err != nil {
		t.Fatal(err)
	}
	st.Release()
	st.Close()

	r.start()
	r.wait("summary", func() bool { return len(r.own(ctl, "Teams helper data converted")) == 1 })
	if v, _, err := r.st.Meta(store.MetaMigrationSummary); err != nil || v != "" {
		t.Fatalf("summary meta after posting: %q, %v", v, err)
	}
	r.stop()
	r.start()
	time.Sleep(time.Second) // started and drained its outbox (20ms ticks)
	if n := len(r.own(ctl, "Teams helper data converted")); n != 1 {
		t.Fatalf("summary posted %d times", n)
	}
}
