package engine

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Put a message into the store after the runner's empty steer check but
// before its start transaction. No goroutine scheduling or sleeps are needed.
func TestInputAdmittedBetweenSteerAndStart(t *testing.T) {
	for _, tc := range []struct {
		name, native string
		queue        bool
		wantStart    bool
	}{
		{"ordinary-active", "native-active", false, false},
		{"queued-active", "native-active", true, true},
		{"ordinary-idle", "", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.Open(filepath.Join(t.TempDir(), "core.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
				t.Fatal(err)
			}
			defer st.Release()
			var id int64
			if err := st.Update(func(tx *store.Tx) error {
				var err error
				id, err = tx.CreateConversation(store.NewConversation{Label: "s001", Kind: store.KindWork, Channel: "teams", ExtChatID: "work"})
				if err != nil {
					return err
				}
				return tx.Bind(store.Thread{ConvID: id, ThreadID: "thread-1", Cwd: t.TempDir()})
			}); err != nil {
				t.Fatal(err)
			}
			a := &steerAgent{fakeAgent: newAgent(), current: tc.native, failure: errors.New("connection lost after write")}
			e, err := New(Config{Store: st, Channel: channeltest.New(), Agent: a})
			if err != nil {
				t.Fatal(err)
			}
			e.convs[id] = &convState{id: id, kind: store.KindWork, state: store.ConvActive, threadID: "thread-1"}
			ctx, cancel := context.WithCancel(t.Context())
			defer func() { cancel(); e.wg.Wait() }()
			if e.trySteer(ctx, id) {
				t.Fatal("empty queue handled as input")
			}
			if err := st.Update(func(tx *store.Tx) error {
				_, err := tx.AdmitWindow("work", id, []store.InboundMsg{{ExtMsgID: "input", ExtTime: 1, Sender: store.SenderOwner, Kind: store.InPrompt, Body: "late input", Queue: tc.queue}}, "cursor")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			sem := make(chan struct{}, 1)
			sem <- struct{}{}
			if started := e.tryStart(ctx, id, sem); started != tc.wantStart {
				t.Fatalf("started=%v, want %v: ordinary input bypassed native delivery", started, tc.wantStart)
			}
			if tc.wantStart {
				e.wg.Wait()
				if len(a.runLog()) != 1 || a.runLog()[0].Queue != tc.queue || a.count() != 0 {
					t.Fatal("independent execution changed queue mode")
				}
				return
			}
			turns, err := st.OpenTurns()
			if err != nil || len(turns) != 1 || turns[0].State != store.TurnQueued || turns[0].Attempt != 0 {
				t.Fatalf("deferred input changed execution state: %v, %v", turns, err)
			}
			if !e.trySteer(ctx, id) {
				t.Fatal("deferred input was not steered on the next pass")
			}
			e.wg.Wait()
			if a.count() != 1 || len(a.runLog()) != 0 || !e.convs[id].paused {
				t.Fatal("uncertain input was replayed or failed to pause")
			}
			if err := st.Update(func(tx *store.Tx) error {
				rows, err := tx.PendingOutbox(id)
				if err != nil {
					return err
				}
				n := 0
				for _, row := range rows {
					if strings.Contains(row.Body, "may have reached Codex") {
						n++
					}
				}
				if n != 1 {
					t.Fatalf("uncertain notice count=%d, want 1", n)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
