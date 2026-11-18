package engine

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

func TestNativeQuietNoticeDroppedBeforeSending(t *testing.T) {
	for _, reason := range []string{"turn completed", "new progress queued"} {
		t.Run(reason, func(t *testing.T) {
			var elapsed atomic.Int64
			ag := newGoalAgent()
			r := newRig(t, func(c *Config) {
				c.Agent = ag
				c.Now = func() time.Time { return time.Now().Add(time.Duration(elapsed.Load())) }
				c.Timing.Maintenance = time.Hour
			})
			work := r.newWork(t.TempDir())
			id := convIDForTest(r, work)
			if err := r.e.bindThread(id, "test-thread"); err != nil {
				t.Fatal(err)
			}
			ag.gmu.Lock()
			ag.goals["test-thread"] = GoalState{Status: GoalActive, Objective: "Monitor the job"}
			ag.gmu.Unlock()
			elapsed.Add(int64(10 * time.Minute))
			r.e.remindQuietNativeTurns()
			hasNotice, err := r.st.OutboxHasKeyPrefix(nativeQuietPrefix)
			if err != nil || hasNotice {
				t.Fatalf("active goal without a live turn queued a notice: %t, %v", hasNotice, err)
			}

			ag.gmu.Lock()
			ag.selfTurn = true
			ag.nativeTurn = "test-native-turn"
			ag.gmu.Unlock()
			r.e.remindQuietNativeTurns()
			r.e.setGate(r.e.gWrite, r.e.now().Add(6*time.Minute), "hold the sender")
			elapsed.Add(int64(4 * time.Minute))
			r.e.remindQuietNativeTurns()
			key := nativeQuietKey(id, "test-native-turn") + ":"
			hasNotice, err = r.st.OutboxHasKeyPrefix(key)
			if err != nil || hasNotice {
				t.Fatalf("queued ahead of the quiet threshold: %t, %v", hasNotice, err)
			}
			elapsed.Add(int64(time.Minute))
			r.e.remindQuietNativeTurns()
			rows, err := r.st.OutboxByKeyPrefix(key)
			if err != nil || len(rows) != 1 || rows[0].State != store.OutQueued {
				t.Fatalf("wanted exactly one queued notice: %+v, %v", rows, err)
			}

			if reason == "turn completed" {
				ag.gmu.Lock()
				ag.selfTurn = false
				ag.gmu.Unlock()
			} else {
				if err := r.e.update(func(tx *store.Tx) error {
					return r.e.enqueueStatus(tx, id, "fresh-progress", "New evaluation ready.")
				}); err != nil {
					t.Fatal(err)
				}
			}
			elapsed.Add(int64(2 * time.Minute))
			r.e.wakeSender()
			r.wait("stale notice dropped", func() bool {
				rows, err := r.st.OutboxByKeyPrefix(key)
				return err == nil && len(rows) == 1 && rows[0].State == store.OutDropped
			})
			if n := len(r.own(work, "Codex is still running.")); n != 0 {
				t.Fatalf("sent %d stale notices", n)
			}
			if reason == "new progress queued" {
				r.wait("new progress passes the stale notice", func() bool {
					return len(r.own(work, "New evaluation ready.")) == 1
				})
			}
		})
	}
}

func TestNativeQuietCountsFromMirroredProgress(t *testing.T) {
	sessions, cwd := t.TempDir(), t.TempDir()
	var elapsed atomic.Int64
	ag := newGoalAgent()
	r := newRig(t, func(c *Config) {
		c.SessionsDir = sessions
		c.Agent = ag
		c.Now = func() time.Time { return time.Now().Add(time.Duration(elapsed.Load())) }
		c.Timing.Maintenance = time.Hour
	})
	r.mirrorInitialised()
	work := r.newWork(cwd)
	id := convIDForTest(r, work)
	if err := r.e.bindThread(id, "test-thread"); err != nil {
		t.Fatal(err)
	}
	w, err := rollout.New(sessions, cwd, rollout.Options{ID: "test-thread"})
	if err != nil {
		t.Fatal(err)
	}
	const turn = "test-native-turn"
	if _, err := w.BeginTurn(rollout.Turn{ID: turn, GoalContinuation: true}); err != nil {
		t.Fatal(err)
	}
	ag.gmu.Lock()
	ag.selfTurn, ag.nativeTurn = true, turn
	ag.gmu.Unlock()
	r.e.remindQuietNativeTurns()

	waitDelivery := func(message string) {
		t.Helper()
		r.wait(message, func() bool {
			if len(r.own(work, message)) != 1 {
				return false
			}
			var pending bool
			err := r.e.update(func(tx *store.Tx) error {
				var err error
				pending, err = tx.HasOtherActiveOutbox(id, 0)
				return err
			})
			return err == nil && !pending
		})
	}
	elapsed.Add(int64(4 * time.Minute))
	if err := w.AppendRecord("response_item", map[string]any{
		"type": "message", "role": "assistant", "id": "progress", "phase": "commentary",
		"content": []any{map[string]any{"type": "output_text", "text": "New evaluation ready."}},
	}); err != nil {
		t.Fatal(err)
	}
	waitDelivery("New evaluation ready.")

	elapsed.Add(int64(2 * time.Minute))
	r.e.remindQuietNativeTurns()
	hasNotice, err := r.st.OutboxHasKeyPrefix(nativeQuietKey(id, turn) + ":")
	if err != nil || hasNotice {
		t.Fatalf("mirrored progress did not reset the timer: %t, %v", hasNotice, err)
	}

	if err := w.AppendRecord("event_msg", map[string]any{"type": "context_compacted", "turn_id": turn}); err != nil {
		t.Fatal(err)
	}
	waitDelivery(textCompacted)
	elapsed.Add(int64(3*time.Minute + time.Second))
	r.e.remindQuietNativeTurns()
	waitDelivery("Codex is still running.")
}
