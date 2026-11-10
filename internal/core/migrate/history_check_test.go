package migrate

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Live canary 2026-11-05: s001 had no delivery proof behind its old
// checkpoint, so its catch-up started at byte 0 and re-posted at least 267
// records its chat already had. The converter lists such a rollout; at
// first start the engine reads the chat's history before its catch-up and
// resumes after the newest record found there (store.HistoryCheck). The
// old helper's posts carry other labels, a footer, a split; the old store's
// inbound rows carry no text.
func TestNoProofResumeCheckedAgainstHistory(t *testing.T) {
	const chat = "19:chat-s001@thread.v2"
	long := strings.Repeat("a long answer that the previous helper split in two parts ", 6)
	r := newRollout()
	r.ts = "2026-09-01T09:00:00Z"
	r.line("session_meta", map[string]any{"id": s001Thread, "originator": "codex_cli_rs", "source": "cli"})
	r.turn("ta")
	r.user("u1", "hello local")
	r.final("f1", "local answer")
	r.done("ta", "local answer")
	r.turn("tb")
	r.user("u2", "second local")
	r.final("f2", long)
	r.done("tb", long)
	r.turn("tc")
	r.user("u3", "ok") // short: found only as an equal text after the anchor's post
	r.final("f3", "third answer, posted before the checkpoint was written")
	r.done("tc", "third answer, posted before the checkpoint was written")
	ckpt := r.ends[13]
	r.turn("td")
	r.user("u4", "fourth local prompt")
	r.final("f4", "answer after the checkpoint")
	r.done("td", "answer after the checkpoint")
	sessions, path := writeRollout(t, r)

	// Old posts as v0.1.20 and rc.59 made them.
	user := func(s string) string { return "🧑‍💻 User:\n" + s }
	answer := func(s string) string {
		return "🤖 ✅ Codex answer:\n" + s + "\n🔧 Helper: ✅ Codex finished responding."
	}
	half := len(long) / 2
	all := []string{user("hello local"), answer("local answer"), user("second local"),
		"🤖 ✅ Codex answer [part 1/2]:\n" + long[:half], "🤖 ✅ Codex answer [part 2/2]:\n" + long[half:] + "\n🔧 Helper: ✅ Codex finished responding.",
		user("ok"), answer("third answer, posted before the checkpoint was written")}
	everything := []string{"hello local", "local answer", "second local", "split in two parts", "ok", "third answer", "fourth local prompt", "answer after the checkpoint"}
	for _, tc := range []struct {
		name    string
		history []string // old posts in the chat
		posted  []string // texts the catch-up posts
	}{
		{"every record before the checkpoint is in the chat", all, everything[6:]},
		{"the chat stops after the second turn's split answer", all[:5], everything[4:]},
		{"a short prompt alone does not anchor", []string{user("ok")}, everything},
		{"nothing in the chat: no record is lost", nil, everything},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src := fixture(t, "rc59", "json")
			editJSON(t, src, positions(path, ckpt, 13, ckpt, 13, nil))
			editJSON(t, src, func(m map[string]any) { delete(m, "chat_rate_limits") })
			out := t.TempDir()
			if _, err := Run(Options{Source: src, Out: out, DryRun: true, StaleUnsent: -1, CodexSessions: sessions}); err != nil {
				t.Fatal(err)
			}
			dbPath := filepath.Join(out, TargetName)
			st, err := store.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			convs, err := st.Conversations()
			if err != nil {
				t.Fatal(err)
			}
			ch := &cursorChannel{Fake: channeltest.New()}
			ch.SetClock(time.Date(2026, 9, 1, 9, 30, 0, 0, time.UTC))
			ch.HistoryPageSize = 2
			for _, c := range convs {
				if c.ExtChatID != "" {
					ch.PostSystem(c.ExtChatID, "chat exists")
				}
			}
			for i, h := range tc.history {
				ch.PostOtherHelper(chat, h, "old-"+string(rune('a'+i)))
			}
			if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
				t.Fatal(err)
			}
			var log strings.Builder
			var logMu sync.Mutex
			e, err := engine.New(engine.Config{Store: st, Channel: ch, ChannelName: "teams", Agent: idleAgent{}, Timing: engine.FastTiming(20 * time.Millisecond),
				Machine: "m", SessionsDir: sessions, Log: writerFunc(func(b []byte) { logMu.Lock(); log.Write(b); logMu.Unlock() })})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- e.Run(ctx) }()
			defer func() { cancel(); <-done; st.Release(); st.Close() }()
			mirrored := func() []string {
				var got []string
				for _, m := range ch.Messages(chat) {
					if m.Marker != "" && !strings.HasPrefix(m.Marker, "old-") && (strings.HasPrefix(m.Text, "🧑‍💻 User") || strings.HasPrefix(m.Text, "🤖 ✅ Codex answer")) {
						got = append(got, m.Text)
					}
				}
				return got
			}
			deadline := time.Now().Add(15 * time.Second)
			for {
				got := mirrored()
				if len(got) > 0 && strings.Contains(got[len(got)-1], "answer after the checkpoint") {
					break
				}
				if time.Now().After(deadline) {
					logMu.Lock()
					t.Fatalf("timeout waiting for the catch-up; posted %q\nengine log:\n%s", got, log.String())
				}
				time.Sleep(20 * time.Millisecond)
			}
			time.Sleep(200 * time.Millisecond)
			got := mirrored()
			for _, x := range everything {
				n := 0
				for _, m := range got {
					if m == "🧑‍💻 User:\n"+x || (strings.HasPrefix(m, "🤖") && strings.Contains(m, x)) {
						n++
					}
				}
				if want := slices.Contains(tc.posted, x); (n > 0) != want || n > 1 {
					logMu.Lock()
					t.Fatalf("%q posted %d times, want posted=%v\nposted: %q\nengine log:\n%s", x, n, want, got, log.String())
				}
			}
			// The decision is recorded: a restart does not check again.
			list, err := st.HistoryChecks()
			if err != nil {
				t.Fatal(err)
			}
			if len(list) != 1 || list[0].Path != path || list[0].From != 0 || list[0].To != ckpt || !list[0].Done {
				t.Fatalf("history checks: %+v", list)
			}
		})
	}
}
