package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// Owner request 2026-11-07: what happens now is not held behind the
// catch-up backlog. A local session in use while 480 caught-up messages
// wait to be mirrored gets its chat, its prompt and its answer ahead of
// them (send class sendLive), as a Teams answer does (sendReply).
func TestLiveMirrorNotHeldByCatchUpBacklog(t *testing.T) {
	skipWallClockPacingUnderRace(t)
	const backlogChats, backlogRows = 12, 40
	var (
		mu    sync.Mutex
		order []string
	)
	sessions := t.TempDir()
	r := &rig{t: t, ch: channeltest.New(), ag: newAgent(), path: filepath.Join(t.TempDir(), "core.db"), log: &strings.Builder{}, logMu: &sync.Mutex{}}
	r.tweak = func(c *Config) {
		c.SessionsDir = sessions
		c.WriteRate = 4
		c.Channel.(*channeltest.Fake).SendFault = func(_ string, p channel.Part) (bool, error) {
			mu.Lock()
			order = append(order, p.Body)
			mu.Unlock()
			return false, nil
		}
	}
	t.Cleanup(r.stop)
	ctx := context.Background()
	st, err := store.Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	err = st.Import(func(im *store.Importer) error {
		for i := 0; i < backlogChats; i++ {
			ci, _ := r.ch.CreateChat(ctx, "w", fmt.Sprintf("k%02d", i))
			id, err := im.Conversation(store.Conversation{Label: fmt.Sprintf("b%02d", i+1), Kind: store.KindWork, Channel: "teams", ExtChatID: ci.ID, State: store.ConvActive})
			if err != nil {
				return err
			}
			if err := im.Stream(store.Stream{Key: store.ChatStreamKey("teams", ci.ID), ConvID: id, Cursor: "0", Seeded: true}); err != nil {
				return err
			}
			for k := 0; k < backlogRows; k++ {
				key := fmt.Sprintf("bl:%s:%03d", ci.ID, k)
				if _, _, err := im.Outbox(store.Outbox{ConvID: id, DeliveryKey: key, Seq: int64(k + 1), Kind: store.OutMirror,
					Body: "backlog " + key, State: store.OutQueued, CreatedAt: im.Now()}, nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	r.start()
	r.control()
	r.mirrorInitialised()
	mu.Lock()
	before := len(order)
	mu.Unlock()
	w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "live prompt now", Answer: "live answer now"}); err != nil {
		t.Fatal(err)
	}
	r.wait("the live answer", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, b := range order {
			if strings.Contains(b, "live answer now") {
				return true
			}
		}
		return false
	})
	mu.Lock()
	defer mu.Unlock()
	backlog := 0
	livePrompt := false
	for _, b := range order[before:] {
		switch {
		case strings.HasPrefix(b, labelUser+":\nlive prompt now"):
			livePrompt = true
		case strings.Contains(b, "backlog bl:"):
			if livePrompt {
				backlog++
			}
		case strings.Contains(b, "live answer now"):
			// Backlog sends while the helper discovers the new session do
			// not show that a live answer waited behind the backlog. Measure
			// from the observable prompt instead.
			if !livePrompt {
				t.Fatal("the live answer was sent before its prompt")
			}
			if backlog > 8 {
				t.Fatalf("%d backlog messages went between the live prompt and its answer; order:\n%s", backlog, strings.Join(order[before:], "\n"))
			}
			if left := backlogChats*backlogRows - strings.Count(strings.Join(order, "\n"), "backlog bl:"); left < 100 {
				t.Fatalf("the backlog was nearly sent (%d left): the test proves nothing", left)
			}
			return
		}
	}
}
