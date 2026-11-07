package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Owner request 2026-11-07, on a store rc.66 left behind: at start the
// chats with the label titles rc.66 gave them ("s001 · name") are renamed
// to v0.1.20's titles (the owner's title, else the thread name or first
// prompt, else "New message in <workspace>"), and the discovered
// sessions' chats nobody announced are announced once, in one control-chat
// summary. Chats with any other title, chats the owner made, and chats
// already announced are left alone; a restart repeats nothing.
func TestStartupRepairsLabelTitlesAndAnnouncesUnannouncedChats(t *testing.T) {
	home := t.TempDir()
	sessions := filepath.Join(home, "sessions")
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "session_index.jsonl"),
		[]byte(`{"id":"thread-one","thread_name":"Thread name one","updated_at":"2026-11-07T00:00:00Z"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := &rig{t: t, ch: channeltest.New(), ag: newAgent(), path: filepath.Join(t.TempDir(), "core.db"), log: &strings.Builder{}, logMu: &sync.Mutex{}}
	r.tweak = func(c *Config) { c.SessionsDir = sessions }
	t.Cleanup(r.stop)
	type conv struct {
		label, title, source, cwd, thread string
		found, announced                  bool // a discovered session; its chat announced
		creating                          bool // its chat is not created yet
		want                              string
	}
	convs := []conv{
		// A caught-up session (rc.66): thread name, announced now.
		{label: "s001", title: "s001 · first prompt", source: "auto", cwd: "/w/proj", thread: "thread-one", found: true, want: "💬 m - Thread name one"},
		// `new <dir> -- My Title` (rc.66): the owner's title.
		{label: "s002", title: "s002 · My Title", source: "user", cwd: "/w/a", want: "💬 m - My Title"},
		// `new <dir>` without a prompt yet: v0.1.20's placeholder.
		{label: "s003", title: "s003 · proj2", source: "auto", cwd: "/w/proj2", want: "💬 m - New message in proj2"},
		// v0.1.20 titles, the owner's and migrated titles stay.
		{label: "s004", title: "💬 m - Standard", source: "auto", cwd: "/w/b", want: "💬 m - Standard"},
		{label: "s005", title: "💬 m - Announced live", source: "auto", cwd: "/w/c", found: true, announced: true, want: "💬 m - Announced live"},
		{label: "s006", title: "Old Title", source: "legacy", cwd: "/w/d", want: "Old Title"},
		// A second caught-up session with a v0.1.20 title: announced.
		{label: "s007", title: "💬 m - Caught up two", source: "auto", cwd: "/w/e", found: true, want: "💬 m - Caught up two"},
		// A caught-up session whose chat rc.66 had not created yet: created
		// with the repaired title; the summary waits for it.
		{label: "s008", title: "s008 · x", source: "auto", cwd: "/w/f", found: true, creating: true, want: "💬 m - New message in f"},
	}
	chats := map[string]string{}
	ctx := context.Background()
	st, err := store.Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	err = st.Import(func(im *store.Importer) error {
		for i, c := range convs {
			if c.creating {
				id, err := im.Conversation(store.Conversation{Label: c.label, Kind: store.KindWork, Channel: "teams",
					Title: c.title, TitleSource: c.source, State: store.ConvCreating})
				if err != nil {
					return err
				}
				if err := im.Thread(store.Thread{ConvID: id, Cwd: c.cwd}); err != nil {
					return err
				}
				if err := im.SetMeta(discoveredKey(id), discoveredReady); err != nil {
					return err
				}
				continue
			}
			ci, _ := r.ch.CreateChat(ctx, c.title, fmt.Sprintf("k%02d", i))
			chats[c.label] = ci.ID
			id, err := im.Conversation(store.Conversation{Label: c.label, Kind: store.KindWork, Channel: "teams", ExtChatID: ci.ID, ExtURL: ci.URL,
				Title: c.title, TitleSource: c.source, State: store.ConvActive})
			if err != nil {
				return err
			}
			if err := im.Stream(store.Stream{Key: store.ChatStreamKey("teams", ci.ID), ConvID: id, Cursor: "0", Seeded: true}); err != nil {
				return err
			}
			if err := im.Thread(store.Thread{ConvID: id, ThreadID: c.thread, Cwd: c.cwd}); err != nil {
				return err
			}
			if c.found {
				if err := im.SetMeta(discoveredKey(id), ""); err != nil {
					return err
				}
			}
			if c.announced {
				if err := im.SetMeta(announcedKey(id), "1"); err != nil {
					return err
				}
			}
		}
		return im.SetMeta(metaNextLabel, "8")
	})
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	r.start()
	ctl := r.control()
	title := func(label string) string {
		if chats[label] == "" {
			chats[label] = r.chatOf(label)
		}
		info, _ := r.ch.Chat(chats[label])
		return info.Title
	}
	r.wait("titles repaired", func() bool {
		for _, c := range convs {
			if title(c.label) != c.want {
				return false
			}
		}
		return true
	})
	r.wait("summary", func() bool { return len(r.own(ctl, "New local Codex chats (3)")) == 1 })
	if n := len(r.own(ctl, "New local Codex chats")); n != 1 {
		t.Fatalf("%d summaries, want 1 (it waits for the chat still to be created)", n)
	}
	m := r.own(ctl, "New local Codex chats")[0]
	if !m.MentionsOwner || !strings.Contains(m.Text, "s001") || !strings.Contains(m.Text, "s007") || !strings.Contains(m.Text, "s008") ||
		!strings.Contains(m.Text, "fake://chat/"+chats["s001"]) || !strings.Contains(m.Text, "Caught up two") {
		t.Fatalf("summary: %+v", m)
	}
	for _, l := range []string{"s002", "s003", "s004", "s005", "s006"} {
		if strings.Contains(m.Text, l) {
			t.Fatalf("summary lists %s: %q", l, m.Text)
		}
	}
	if v, _, _ := r.st.Meta(fmt.Sprint(metaUserTitle, convID(t, r, "s002"))); v != "My Title" {
		t.Fatalf("s002 user title %q", v)
	}
	// A restart renames and announces nothing again.
	r.stop()
	r.start()
	r.control()
	time.Sleep(time.Second)
	if n := len(r.own(ctl, "New local Codex chats")); n != 1 {
		t.Fatalf("%d summaries after a restart, want 1", n)
	}
	for _, c := range convs {
		if got := title(c.label); got != c.want {
			t.Fatalf("%s titled %q after a restart, want %q", c.label, got, c.want)
		}
	}
	r.logMu.Lock()
	repairs := strings.Count(r.log.String(), "title repair:")
	r.logMu.Unlock()
	if repairs != 1 {
		t.Fatalf("title repair logged %d times across a restart, want 1", repairs)
	}
}

func convID(t *testing.T, r *rig, label string) int64 {
	t.Helper()
	convs, err := r.st.Conversations()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range convs {
		if c.Label == label {
			return c.ID
		}
	}
	t.Fatalf("no conversation %s", label)
	return 0
}

// Many caught-up chats are announced in posts of announceBatch chats, the
// owner mentioned on the first only.
func TestManyUnannouncedChatsAnnouncedInBatches(t *testing.T) {
	const n = 45
	r := &rig{t: t, ch: channeltest.New(), ag: newAgent(), path: filepath.Join(t.TempDir(), "core.db"), log: &strings.Builder{}, logMu: &sync.Mutex{}}
	r.tweak = func(*Config) {}
	t.Cleanup(r.stop)
	ctx := context.Background()
	st, err := store.Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	err = st.Import(func(im *store.Importer) error {
		for i := 0; i < n; i++ {
			label := fmt.Sprintf("s%03d", i+1)
			ci, _ := r.ch.CreateChat(ctx, "💬 m - caught up "+label, label)
			id, err := im.Conversation(store.Conversation{Label: label, Kind: store.KindWork, Channel: "teams", ExtChatID: ci.ID, ExtURL: ci.URL,
				Title: "💬 m - caught up " + label, TitleSource: "auto", State: store.ConvActive})
			if err != nil {
				return err
			}
			if err := im.Stream(store.Stream{Key: store.ChatStreamKey("teams", ci.ID), ConvID: id, Cursor: "0", Seeded: true}); err != nil {
				return err
			}
			if err := im.SetMeta(discoveredKey(id), ""); err != nil {
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
	ctl := r.control()
	r.wait("summaries", func() bool { return len(r.own(ctl, "New local Codex chats (45, part")) == 3 })
	listed := map[string]int{}
	for i, m := range r.own(ctl, "New local Codex chats") {
		if m.MentionsOwner != (i == 0) {
			t.Fatalf("summary %d mentions the owner: %v", i+1, m.MentionsOwner)
		}
		if !strings.Contains(m.Text, fmt.Sprintf("part %d of 3", i+1)) {
			t.Fatalf("summary %d: %q", i+1, m.Text)
		}
		for i := 0; i < n; i++ {
			label := fmt.Sprintf("**s%03d**", i+1)
			listed[label] += strings.Count(m.Text, label)
		}
	}
	for label, c := range listed {
		if c != 1 {
			t.Fatalf("%s listed %d times", label, c)
		}
	}
	if len(listed) != n {
		t.Fatalf("%d chats listed, want %d", len(listed), n)
	}
}
