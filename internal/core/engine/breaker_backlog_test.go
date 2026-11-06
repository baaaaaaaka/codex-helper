package engine

import (
	"context"
	"errors"
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

// Finding 8 of the cluster drills: after a long gap the first read of a
// chat holds a burst of messages. The self-echo breaker must stop a live
// loop but never pause a chat for what queued while no helper read it.

// ourTexts returns the texts of the helper's last n posts in chat.
func ourTexts(r *rig, chat string, n int) []string {
	var ours []string
	for _, m := range r.ch.Messages(chat) {
		if m.Marker != "" {
			ours = append(ours, m.Text)
		}
	}
	if len(ours) > n {
		ours = ours[len(ours)-n:]
	}
	return ours
}

// The helper is down while marker-less copies of its posts (which count
// toward the breaker when they arrive live) and owner prompts queue in a
// work chat. The first read after the start does not trip the breaker, every
// queued prompt runs; the same copies arriving live afterwards still trip it.
func TestBreakerIgnoresTheBacklogOfTheFirstRead(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "first prompt")
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to first prompt")) == 1 })
	echoes := ourTexts(r, work, 3)
	if len(echoes) != 3 {
		t.Fatalf("helper posts to echo: %q", echoes)
	}
	r.stop()
	for _, text := range echoes {
		r.ch.PostUser(work, text)
	}
	for _, p := range []string{"queued one", "queued two", "queued three"} {
		r.ch.PostUser(work, p)
	}
	r.start()
	for _, p := range []string{"queued one", "queued two", "queued three"} {
		r.wait("answer to "+p, func() bool { return len(r.own(work, "ANSWER to "+p)) == 1 })
	}
	ctl := r.control()
	if n, m := len(r.own(work, "Paused auto-start")), len(r.own(ctl, "Paused auto-start")); n+m != 0 {
		t.Fatalf("the backlog tripped the breaker: %d notices in the work chat, %d in the control chat", n, m)
	}
	// A live loop still trips it.
	for _, text := range echoes {
		r.ch.PostUser(work, text)
	}
	r.wait("pause notice", func() bool { return len(r.own(work, "Paused auto-start")) == 1 })
	r.ch.PostUser(work, "held prompt")
	r.wait("held ack", func() bool { return len(r.own(work, "Request held")) == 1 })
	if r.ag.calls("held prompt") != 0 {
		t.Fatal("a prompt ran while paused")
	}
}

// Reads of a live chat stall (here: they fail) while three copies of the
// helper's posts arrive minutes apart, then one window holds them all. They
// count at the times they were posted, as in v0.1.20: no three within the
// window, no trip. Three copies posted together still trip it.
func TestBreakerCountsEchoesAtTheirPostTimes(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "first prompt")
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to first prompt")) == 1 })
	echoes := ourTexts(r, work, 3)
	if len(echoes) != 3 {
		t.Fatalf("helper posts to echo: %q", echoes)
	}
	var stalled atomic.Bool
	stalled.Store(true)
	r.ch.ReceiveFault = func(stream string) error {
		if stalled.Load() && strings.Contains(stream, work) {
			return errors.New("read stalled")
		}
		return nil
	}
	base := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	for i, text := range echoes {
		r.ch.SetClock(base.Add(time.Duration(i) * 2 * time.Minute))
		r.ch.PostUser(work, text)
	}
	r.ch.PostUser(work, "after the stall")
	stalled.Store(false)
	r.wait("answer after the stall", func() bool { return len(r.own(work, "ANSWER to after the stall")) == 1 })
	if n := len(r.own(work, "Paused auto-start")); n != 0 {
		t.Fatalf("copies posted minutes apart tripped the breaker (%d notices)", n)
	}
	for _, text := range echoes {
		r.ch.PostUser(work, text)
	}
	r.wait("pause notice", func() bool { return len(r.own(work, "Paused auto-start")) == 1 })
}

// The previous helper's posts on a converted store carry its own marker
// format with its outbox ids, and the conversion kept their message ids:
// they are this helper's posts, not a peer's (a peer's count toward the
// breaker until the marker self-test passes). A marker whose message id is
// not in the outbox stays a peer's.
func TestPreviousHelperMarkerWithKnownIDIsOwn(t *testing.T) {
	r := &rig{t: t, ch: channeltest.New(), ag: newAgent(), path: filepath.Join(t.TempDir(), "core.db"), log: &strings.Builder{}, logMu: &sync.Mutex{}}
	r.tweak = func(*Config) {}
	t.Cleanup(r.stop)
	ctx := context.Background()
	ctl, _ := r.ch.CreateChat(ctx, "control", "c")
	work, _ := r.ch.CreateChat(ctx, "w", "k")
	var old []channel.InboundMsg
	for i, text := range []string{"🔧 Helper: ⏳ Codex is working. Request accepted.", "🤖 ✅ Codex answer: done", "🔧 Helper: ✅ Codex finished responding."} {
		old = append(old, r.ch.PostOtherHelper(work.ID, text, "outbox:old-"+string(rune('a'+i))))
	}
	peer := r.ch.PostOtherHelper(work.ID, "🔧 Helper: ⏳ Codex is working. Request accepted.", "outbox:elsewhere")
	st, err := store.Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	err = st.Import(func(im *store.Importer) error {
		if _, err := im.Conversation(store.Conversation{Label: "control", Kind: store.KindControl, Channel: "teams", ExtChatID: ctl.ID, State: store.ConvActive}); err != nil {
			return err
		}
		id, err := im.Conversation(store.Conversation{Label: "s001", Kind: store.KindWork, Channel: "teams", ExtChatID: work.ID, State: store.ConvActive})
		if err != nil {
			return err
		}
		if err := im.Stream(store.Stream{Key: store.ChatStreamKey("teams", work.ID), ConvID: id, Cursor: "0", Seeded: true}); err != nil {
			return err
		}
		for i, m := range old {
			if _, _, err := im.Outbox(store.Outbox{ConvID: id, DeliveryKey: "legacy-outbox:" + m.Marker, Seq: int64(i + 1), Kind: store.OutNotice,
				Body: m.Text, State: store.OutSent, ExtMsgID: m.ID, CreatedAt: im.Now(), SentAt: im.Now()}, nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	r.start()
	sender := func(ext string) string {
		var s string
		_ = r.st.View(func(q store.Querier) error {
			return q.QueryRowContext(ctx, `SELECT sender FROM inbound WHERE ext_msg_id = ?`, ext).Scan(&s)
		})
		return s
	}
	r.wait("the chat is read", func() bool { return sender(peer.ID) != "" })
	for _, m := range old {
		if got := sender(m.ID); got != store.SenderSelf {
			t.Errorf("previous helper's post %q (%s): sender %q, want %q", m.Text, m.Marker, got, store.SenderSelf)
		}
	}
	if got := sender(peer.ID); got != store.SenderPeer {
		t.Errorf("unknown marker: sender %q, want %q", got, store.SenderPeer)
	}
}
