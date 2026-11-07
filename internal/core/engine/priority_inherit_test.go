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
)

// Real machine, rc.66 (s746): a Teams-run answer waited behind a progress
// post queued before it, and the sender ranked that chat by its progress
// head, after a 5,400-message catch-up backlog: the six answer parts would
// have waited more than an hour. A chat with an answer pending ranks as a
// reply; its older progress post goes first, in that rank.
func TestAnswerBehindProgressNotHeldByMirrorBacklog(t *testing.T) {
	skipWallClockPacingUnderRace(t)
	const backlogChats, backlogRows = 12, 40
	var (
		mu    sync.Mutex
		order []string
	)
	r := &rig{t: t, ch: channeltest.New(), ag: newAgent(), path: filepath.Join(t.TempDir(), "core.db"), log: &strings.Builder{}, logMu: &sync.Mutex{}}
	r.tweak = func(c *Config) {
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
		for i := 0; i <= backlogChats; i++ {
			ci, _ := r.ch.CreateChat(ctx, "w", fmt.Sprintf("k%02d", i))
			id, err := im.Conversation(store.Conversation{Label: fmt.Sprintf("s%03d", i+1), Kind: store.KindWork, Channel: "teams", ExtChatID: ci.ID, State: store.ConvActive})
			if err != nil {
				return err
			}
			if err := im.Stream(store.Stream{Key: store.ChatStreamKey("teams", ci.ID), ConvID: id, Cursor: "0", Seeded: true}); err != nil {
				return err
			}
			kinds := []string{store.OutProgress, store.OutFinal} // the answering chat (last)
			if i < backlogChats {
				kinds = make([]string, backlogRows)
				for k := range kinds {
					kinds[k] = store.OutMirror
				}
			}
			for k, kind := range kinds {
				key := fmt.Sprintf("bl:%s:%03d", ci.ID, k)
				if _, _, err := im.Outbox(store.Outbox{ConvID: id, DeliveryKey: key, Seq: int64(k + 1), Kind: kind, Body: kind + " backlog " + key, State: store.OutQueued, CreatedAt: im.Now()}, nil); err != nil {
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
	r.wait("the answer", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, b := range order {
			if strings.HasPrefix(b, store.OutFinal+" backlog") {
				return true
			}
		}
		return false
	})
	mu.Lock()
	defer mu.Unlock()
	mirrored, progress := 0, false
	for _, b := range order {
		switch {
		case strings.HasPrefix(b, store.OutMirror+" backlog"):
			mirrored++
		case strings.HasPrefix(b, store.OutProgress+" backlog"):
			progress = true
		case strings.HasPrefix(b, store.OutFinal+" backlog"):
			if !progress {
				t.Fatalf("the answer went before the progress post queued ahead of it; order:\n%s", strings.Join(order, "\n"))
			}
			// The first pass may start a few backlog sends before the
			// answer's chat is ranked; never the backlog itself.
			if mirrored > 6 {
				t.Fatalf("%d of %d mirrored messages went before the answer; order:\n%s", mirrored, backlogChats*backlogRows, strings.Join(order, "\n"))
			}
			return
		}
	}
}
