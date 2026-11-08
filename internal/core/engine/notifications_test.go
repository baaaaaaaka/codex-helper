package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

type noticeRecorder struct {
	mu      sync.Mutex
	notices []channel.OwnerNotice
	err     error
	before  func(channel.OwnerNotice)
}

func (n *noticeRecorder) Available() bool { return true }
func (n *noticeRecorder) Notify(_ context.Context, v channel.OwnerNotice) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.before != nil {
		n.before(v)
	}
	n.notices = append(n.notices, v)
	return n.err
}
func (n *noticeRecorder) count(heading string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	count := 0
	for _, v := range n.notices {
		if v.Heading == heading {
			count++
		}
	}
	return count
}

func TestReadyCardDurableAndOrdered(t *testing.T) {
	n := &noticeRecorder{}
	r := newRig(t, func(c *Config) { c.Notifier = n })
	n.mu.Lock()
	n.before = func(v channel.OwnerNotice) {
		if v.Heading == "💬 Codex chat ready" {
			if len(r.own(r.chatOf("s001"), "💬 Work chat is ready.")) != 1 {
				t.Error("card preceded ready message")
			}
			if v.Button != "Open chat" || v.ChatTitle == "" {
				t.Errorf("missing card fields: %+v", v)
			}
		}
	}
	n.mu.Unlock()
	r.newWork(t.TempDir())
	r.wait("ready card", func() bool { return n.count("💬 Codex chat ready") == 1 })
	r.stop()
	r.start()
	time.Sleep(150 * time.Millisecond)
	if got := n.count("💬 Codex chat ready"); got != 1 {
		t.Fatalf("%d cards after restart", got)
	}
}

func TestReadyCardFailureIsNotRetried(t *testing.T) {
	n := &noticeRecorder{err: errors.New("unknown delivery")}
	r := newRig(t, func(c *Config) { c.Notifier = n })
	r.newWork(t.TempDir())
	r.wait("fallback", func() bool { return len(r.own(r.chatOf("control"), "💬 Codex chat ready")) == 1 })
	r.stop()
	r.start()
	time.Sleep(150 * time.Millisecond)
	if n.count("💬 Codex chat ready") != 1 {
		t.Fatal("retried ambiguous ready card")
	}
}

func TestGeneratedTitleHonorsManualAndClosedChat(t *testing.T) {
	r := newRig(t, func(*Config) {})
	work := r.newWork(t.TempDir())
	var id int64
	convs, _ := r.st.Conversations()
	for _, c := range convs {
		if c.ExtChatID == work {
			id = c.ID
		}
	}
	cs := r.e.conv(id)
	calls := 0
	save := func(context.Context) (string, error) { calls++; return "Generated", nil }
	if err := r.e.generatedTitle(context.Background(), cs.id, save); err != nil {
		t.Fatal(err)
	}
	r.wait("generated title", func() bool { r.e.mu.Lock(); defer r.e.mu.Unlock(); return strings.Contains(cs.title, "Generated") })
	if err := r.e.update(func(tx *store.Tx) error { return tx.SetMeta("engine_user_title:"+fmt.Sprint(cs.id), "Mine") }); err != nil {
		t.Fatal(err)
	}
	if err := r.e.generatedTitle(context.Background(), cs.id, save); err != nil {
		t.Fatal(err)
	}
	if err := r.e.update(func(tx *store.Tx) error {
		if err := tx.SetMeta(fmt.Sprint(metaUserTitle, cs.id), ""); err != nil {
			return err
		}
		return tx.Close(cs.id)
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.e.generatedTitle(context.Background(), cs.id, save); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("saved over manual title: %d", calls)
	}
}

func TestGeneratedTitleSkipsBusyRename(t *testing.T) {
	r := newRig(t, func(*Config) {})
	r.e.titles.run.Lock()
	defer r.e.titles.run.Unlock()
	called := false
	if err := r.e.generatedTitle(context.Background(), 0, func(context.Context) (string, error) { called = true; return "", nil }); err != nil || called {
		t.Fatalf("busy rename: %v %v", called, err)
	}
}

func TestReadyMessageUsesSavedDefaults(t *testing.T) {
	r := newRig(t)
	if err := r.e.update(func(tx *store.Tx) error {
		if err := tx.SetMeta(metaDefaultModel, "gpt-5-codex"); err != nil {
			return err
		}
		return tx.SetMeta(metaDefaultEffort, "high")
	}); err != nil {
		t.Fatal(err)
	}
	work := r.newWork(t.TempDir())
	r.wait("ready with defaults", func() bool { return len(r.own(work, "Model: gpt-5-codex\nReasoning effort: high")) == 1 })
	if r.ag.calls("") != 0 {
		t.Fatal("chat creation started Codex work")
	}
}

func TestServiceNoticeCardUsesControlLink(t *testing.T) {
	n := &noticeRecorder{}
	r := newRig(t, func(c *Config) { c.Notifier = n; c.Version = "v2.0.0" })
	ctl := r.control()
	if err := r.e.update(func(tx *store.Tx) error {
		return setMetaJSON(tx, metaUpdatePending, updateRequest{Tag: "v2.0.0", From: "v1.0.0", Conv: r.e.controlID, Inbound: 17})
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.e.announceUpdate(); err != nil {
		t.Fatal(err)
	}
	r.e.wakeSender()
	r.e.wakeNotify()
	r.wait("update card", func() bool { return n.count("✅ Helper update completed") == 1 })
	n.mu.Lock()
	for _, notice := range n.notices {
		if notice.Heading == "✅ Helper update completed" && (notice.Button != "Open Control" || !strings.Contains(notice.URL, ctl)) {
			t.Errorf("wrong control action: %+v", notice)
		}
	}
	n.mu.Unlock()
	r.stop()
	r.start()
	time.Sleep(100 * time.Millisecond)
	if n.count("✅ Helper update completed") != 1 {
		t.Fatal("update card duplicated after restart")
	}
}

func TestNoticeEventHeadingsAndActions(t *testing.T) {
	n := &noticeRecorder{}
	r := newRig(t, func(c *Config) { c.Notifier = n })
	work := r.newWork(t.TempDir())
	r.wait("ready card", func() bool { return n.count("💬 Codex chat ready") == 1 })
	var conv int64
	convs, _ := r.st.Conversations()
	for _, c := range convs {
		if c.ExtChatID == work {
			conv = c.ID
		}
	}
	for _, tc := range []struct{ kind, heading, button string }{
		{"", "✅ Codex finished", "Open answer"},
		{notifyReady, "💬 Codex chat ready", "Open chat"},
		{notifyLocal, "💬 New local Codex chat detected", "Open chat"},
		{notifyAttention, "⚠️ Codex needs attention", "Open chat"},
		{notifyService, "✅ Helper update completed", "Open Control"},
		{notifyService, "⚠️ Helper update activation failed", "Open Control"},
		{notifyService, "✅ Helper restart completed", "Open Control"},
	} {
		t.Run(tc.heading, func(t *testing.T) {
			if !r.e.sendCard(context.Background(), pendingNotify{Conv: conv, Kind: tc.kind, Request: tc.heading}) {
				t.Fatal("card not handled")
			}
			n.mu.Lock()
			got := n.notices[len(n.notices)-1]
			n.mu.Unlock()
			if got.Heading != tc.heading || got.Button != tc.button || got.ChatTitle == "" || got.URL == "" {
				t.Fatalf("event card: %+v", got)
			}
		})
	}
}

func TestGeneratedTitleSaveFailureLeavesTitle(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	convs, _ := r.st.Conversations()
	for _, c := range convs {
		if c.ExtChatID != work {
			continue
		}
		want := errors.New("name storage unavailable")
		if err := r.e.generatedTitle(context.Background(), c.ID, func(context.Context) (string, error) { return "", want }); !errors.Is(err, want) {
			t.Fatalf("save failure: %v", err)
		}
		var after store.Conversation
		err := r.e.update(func(tx *store.Tx) error {
			var err error
			after, _, err = tx.Conversation(c.ID)
			return err
		})
		if err != nil || after.Title != c.Title {
			t.Fatalf("failed title changed conversation: %+v %v", after, err)
		}
		return
	}
	t.Fatal("work conversation not found")
}

type renameCounter struct {
	channel.Channel
	calls atomic.Int32
}

func (c *renameCounter) RenameChat(ctx context.Context, chat, title string) error {
	c.calls.Add(1)
	return c.Channel.RenameChat(ctx, chat, title)
}

func TestGeneratedSameTitleDoesNotPatch(t *testing.T) {
	var counter *renameCounter
	r := newRig(t, func(c *Config) {
		counter = &renameCounter{Channel: c.Channel}
		c.Channel = counter
	})
	work := r.newWork(t.TempDir())
	convs, _ := r.st.Conversations()
	for _, c := range convs {
		if c.ExtChatID != work {
			continue
		}
		initial := counter.calls.Load()
		if err := r.e.autoTitleName(context.Background(), c.ID, work, "Generated name"); err != nil {
			t.Fatal(err)
		}
		before := counter.calls.Load()
		if before != initial+1 {
			t.Fatal("changed title did not cause exactly one PATCH")
		}
		if err := r.e.autoTitleName(context.Background(), c.ID, work, "Generated name"); err != nil {
			t.Fatal(err)
		}
		if counter.calls.Load() != before {
			t.Fatal("equal title caused another PATCH")
		}
		return
	}
	t.Fatal("work conversation not found")
}
