package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chn "github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// cursorChannel records every Receive cursor. The fake channel only knows
// numeric cursors, so a migrated (timestamp) cursor reads from the start.
type cursorChannel struct {
	*channeltest.Fake
	mu    sync.Mutex
	reads []string // stream + "@" + cursor
}

func (c *cursorChannel) Receive(ctx context.Context, stream, cursor string) (chn.Window, error) {
	c.mu.Lock()
	c.reads = append(c.reads, stream+"@"+cursor)
	c.mu.Unlock()
	if cursor != "" && strings.ContainsAny(cursor, "-:TZ") {
		cursor = ""
	}
	return c.Fake.Receive(ctx, stream, cursor)
}

func (c *cursorChannel) readsWith(cursor string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.reads {
		if strings.HasSuffix(r, "@"+cursor) {
			n++
		}
	}
	return n
}

// idleAgent is a Codex that is never asked to run anything.
type idleAgent struct{}

func (idleAgent) Prepare(context.Context) error { return nil }
func (idleAgent) Run(context.Context, engine.TurnRequest, engine.TurnHooks) (engine.TurnOutcome, error) {
	return engine.TurnOutcome{Status: engine.StatusCompleted, Final: "ok"}, nil
}
func (idleAgent) Lookup(context.Context, int64, string, string) (engine.TurnOutcome, bool, error) {
	return engine.TurnOutcome{}, false, nil
}
func (idleAgent) Interrupt(context.Context, int64) error { return nil }
func (idleAgent) Kill(int64)                             {}
func (idleAgent) Fork(context.Context, int64, string, string, string) (engine.Forked, error) {
	return engine.Forked{}, errors.New("no fork")
}
func (idleAgent) Models(context.Context) ([]engine.Model, error) { return nil, nil }
func (idleAgent) Maintain()                                      {}
func (idleAgent) Close()                                         {}

// countingAgent is idleAgent counting the turns whose prompt contains sub.
type countingAgent struct {
	idleAgent
	sub  string
	runs atomic.Int32
}

func (a *countingAgent) Run(ctx context.Context, r engine.TurnRequest, h engine.TurnHooks) (engine.TurnOutcome, error) {
	if strings.Contains(r.Text, a.sub) {
		a.runs.Add(1)
	}
	return a.idleAgent.Run(ctx, r, h)
}

func writeStartRollout(t *testing.T, sessions, id string, at time.Time) string {
	t.Helper()
	dir := filepath.Join(sessions, at.Format("2006"), at.Format("01"), at.Format("02"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	ts := at.UTC().Format(time.RFC3339)
	p := filepath.Join(dir, "rollout-"+at.Format("2006-01-02T15-04-05")+"-"+id+".jsonl")
	body := `{"timestamp":"` + ts + `","type":"session_meta","payload":{"id":"` + id + `","timestamp":"` + ts + `","cwd":"/w","originator":"codex_cli_rs","source":"cli"}}` + "\n" +
		`{"timestamp":"` + ts + `","type":"event_msg","payload":{"type":"user_message","message":"offline prompt","images":[]}}` + "\n" +
		`{"timestamp":"` + ts + `","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"offline prompt"}]}}` + "\n" +
		`{"timestamp":"` + ts + `","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"offline answer"}]}}` + "\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, at, at); err != nil {
		t.Fatal(err)
	}
	return p
}

// A converted store started by the engine (review H1, H2):
//   - the receive cursors the converter wrote are the engine's: after a
//     restart no chat is read from its migration-time cursor again;
//   - the first start is not a fresh install: an interactive rollout left
//     for discovery gets its work chat, however old (no window, owner
//     decision 2026-11-05).
func TestConvertedStoreKeepsCursorsAndCatchesUpAcrossRestart(t *testing.T) {
	out := t.TempDir()
	if _, err := Run(Options{Source: fixture(t, "v0120", "json"), Out: out, DryRun: true, Now: func() time.Time { return testNow }}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(out, TargetName)
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	streams, err := st.Streams()
	st.Close()
	if err != nil || len(streams) == 0 {
		t.Fatalf("converted streams: %d, %v", len(streams), err)
	}
	migrated := map[string]bool{}
	for _, s := range streams {
		if s.Cursor != "" {
			migrated[s.Cursor] = true
		}
	}

	sessions := t.TempDir()
	recent := writeStartRollout(t, sessions, "thr-recent", time.Now().Add(-24*time.Hour))
	old := writeStartRollout(t, sessions, "thr-old", time.Now().Add(-200*24*time.Hour))
	ch := &cursorChannel{Fake: channeltest.New()}

	var log strings.Builder
	var logMu sync.Mutex
	start := func() (*store.Store, func()) {
		st, err := store.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
			t.Fatal(err)
		}
		e, err := engine.New(engine.Config{Store: st, Channel: ch, Agent: idleAgent{}, Timing: engine.FastTiming(20 * time.Millisecond),
			Machine: "m", SessionsDir: sessions, Log: writerFunc(func(b []byte) { logMu.Lock(); log.Write(b); logMu.Unlock() })})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- e.Run(ctx) }()
		return st, func() { cancel(); <-done; st.Release(); st.Close() }
	}
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				logMu.Lock()
				t.Fatalf("timeout waiting for %s\nengine log:\n%s", what, log.String())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	sourceState := func(st *store.Store, p string) string {
		srcs, _ := st.Sources()
		for _, s := range srcs {
			if s.Path == p {
				return s.State
			}
		}
		return ""
	}
	migratedReads := func() int {
		n := 0
		for c := range migrated {
			n += ch.readsWith(c)
		}
		return n
	}

	st, stop := start()
	waitFor("migrated chats read and the recent and 200-day-old rollouts tracked", func() bool {
		return migratedReads() >= 1 && sourceState(st, recent) == store.SrcTracking && sourceState(st, old) == store.SrcTracking
	})
	time.Sleep(time.Second) // every live chat read once, cursors written
	before := migratedReads()
	stop()

	st, stop = start()
	defer stop()
	ch.mu.Lock()
	total := len(ch.reads)
	ch.mu.Unlock()
	waitFor("reads after restart", func() bool {
		ch.mu.Lock()
		defer ch.mu.Unlock()
		return len(ch.reads) > total
	})
	time.Sleep(time.Second)
	if after := migratedReads(); after != before {
		ch.mu.Lock()
		t.Fatalf("after restart %d reads used a migration-time cursor again: %v", after-before, ch.reads[total:])
	}
	if got := sourceState(st, recent); got != store.SrcTracking {
		t.Errorf("recent rollout after restart: %q", got)
	}
}

type writerFunc func([]byte)

func (f writerFunc) Write(b []byte) (int, error) { f(b); return len(b), nil }

// The first start renders the unsent rows the converter kept (their body
// is the legacy Markdown text) into channel payloads before sending them,
// with the label of their kind, and posts the summary once.
func TestFirstStartRendersImportedUnsentRows(t *testing.T) {
	out := t.TempDir()
	// Real time: the fixture's leases are long expired; its unsent rows are
	// kept (StaleUnsent -1) so they are sent.
	if _, err := Run(Options{Source: fixture(t, "v0120", "json"), Out: out, DryRun: true, StaleUnsent: -1}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(out, TargetName)
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	convs, err := st.Conversations()
	if err != nil {
		t.Fatal(err)
	}
	ch := &cursorChannel{Fake: channeltest.New()}
	var control string
	for _, c := range convs {
		if c.ExtChatID != "" {
			ch.PostSystem(c.ExtChatID, "chat exists")
		}
		if c.Kind == store.KindControl {
			control = c.ExtChatID
		}
	}
	if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	var log strings.Builder
	var logMu sync.Mutex
	e, err := engine.New(engine.Config{Store: st, Channel: ch, ChannelName: "teams", Agent: idleAgent{}, Timing: engine.FastTiming(20 * time.Millisecond),
		Machine: "m", Log: writerFunc(func(b []byte) { logMu.Lock(); log.Write(b); logMu.Unlock() })})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	defer func() { cancel(); <-done; st.Release(); st.Close() }()

	summaries := func() int {
		n := 0
		for _, m := range ch.Messages(control) {
			if m.ByOwner && strings.Contains(m.Text, "Teams helper data converted") {
				n++
			}
		}
		return n
	}
	// Every imported row rendered, and one of them sent to a work chat.
	importedSent := func() bool {
		var left int
		if err := st.Update(func(tx *store.Tx) error {
			rows, err := tx.LegacyUnsentOutbox()
			left = len(rows)
			return err
		}); err != nil || left > 0 {
			return false
		}
		for _, c := range convs {
			if c.Kind == store.KindControl {
				continue
			}
			for _, m := range ch.Messages(c.ExtChatID) {
				if m.ByOwner {
					return true
				}
			}
		}
		return false
	}
	deadline := time.Now().Add(15 * time.Second)
	for summaries() < 1 || !importedSent() {
		if time.Now().After(deadline) {
			logMu.Lock()
			t.Fatalf("summary or an imported row not sent (control %s)\nengine log:\n%s", control, log.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if n := summaries(); n != 1 {
		t.Errorf("summary sent %d times", n)
	}
	for _, c := range convs {
		for _, m := range ch.Messages(c.ExtChatID) {
			// A legacy body sent without rendering has no label line.
			if m.ByOwner && !strings.HasPrefix(m.Text, "🔧 Helper") && !strings.HasPrefix(m.Text, "🤖") {
				t.Errorf("unrendered message in %s: %q", c.Label, m.Text)
			}
		}
	}
	logMu.Lock()
	defer logMu.Unlock()
	if !strings.Contains(log.String(), "rendered ") {
		t.Errorf("no render at start:\n%s", log.String())
	}
}

// Owner ruling 2026-11-05 (Q1): at first start the engine reads the chat's
// history of an answer whose send outcome was unknown (fixture o2, "maybe
// posted", s001) and resends it only when it is not there, once: a restart
// neither reads that history again nor resends. An identical text already
// in the chat counts as present even when the old store recorded that post
// as another row (its message id on o1, or another old marker): owner
// ruling after the live canary, where a 32nd identical copy was resent.
func TestUnknownAnswerCheckedAgainstHistoryOnce(t *testing.T) {
	const body = "maybe posted, the answer of the previous helper"
	for _, tc := range []struct {
		name   string
		seed   func(ch *channeltest.Fake, chat string) // before the first start
		resent bool
	}{
		{"absent", func(*channeltest.Fake, string) {}, true},
		{"found by the old marker", func(ch *channeltest.Fake, chat string) {
			ch.PostOtherHelper(chat, "🤖 ✅ Codex answer:\n"+body, "o2")
		}, false},
		{"found by its text", func(ch *channeltest.Fake, chat string) {
			ch.PostUser(chat, "🤖 ✅ Codex answer:\n"+body+"\n\n🔧 Helper: ✅ Codex finished responding. @Owner")
		}, false},
		{"found on a later page", func(ch *channeltest.Fake, chat string) {
			ch.PostOtherHelper(chat, "🤖 ✅ Codex answer:\n"+body, "o2")
			for i := 0; i < 7; i++ {
				ch.PostSystem(chat, fmt.Sprintf("later event %d", i))
			}
		}, false},
		{"found: the same text is another row's post", nil, false},
		{"found: the same text under another old marker", func(ch *channeltest.Fake, chat string) {
			ch.PostOtherHelper(chat, "🤖 ✅ Codex answer:\n"+body, "o9")
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Each case owns its fixture copy, store, fake channel and
			// engine; most of its time is the engine's per-chat send pace.
			t.Parallel()
			src := fixture(t, "v0120", "json")
			const chat = "19:chat-s001@thread.v2"
			ch := &cursorChannel{Fake: channeltest.New()}
			// The posts land after o2 was created (10:00:20) and after s001's
			// stream cursor (10:00:05), as an answer posted while the old
			// helper stopped does: the receiver's first read sees them as new.
			ch.SetClock(time.Date(2026, 9, 1, 10, 0, 21, 0, time.UTC))
			ch.HistoryPageSize = 3
			seed := tc.seed
			if seed == nil {
				// o1 (sent) recorded the message id of this post.
				decoy := ch.PostUser(chat, "🤖 ✅ Codex answer:\n"+body)
				editJSON(t, src, func(m map[string]any) {
					for _, o := range m["outbox_messages"].(map[string]any) {
						if o := o.(map[string]any); o["id"] == "o1" {
							o["teams_message_id"], o["body"] = decoy.ID, body
						}
					}
				})
				seed = func(*channeltest.Fake, string) {}
			}
			editJSON(t, src, func(m map[string]any) {
				for _, o := range m["outbox_messages"].(map[string]any) {
					if o := o.(map[string]any); o["id"] == "o2" {
						o["body"] = body
					}
				}
				// The fixture's chat gate until 2099 would hold s001's sends.
				delete(m, "chat_rate_limits")
			})
			out := t.TempDir()
			if _, err := Run(Options{Source: src, Out: out, DryRun: true, StaleUnsent: -1}); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(out, TargetName)
			st, err := store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			convs, err := st.Conversations()
			st.Close()
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range convs {
				if c.ExtChatID != "" {
					ch.PostSystem(c.ExtChatID, "chat exists")
				}
			}
			seed(ch.Fake, chat)

			var log strings.Builder
			var logMu sync.Mutex
			agent := &countingAgent{sub: body}
			start := func() (*store.Store, func()) {
				st, err := store.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
					t.Fatal(err)
				}
				e, err := engine.New(engine.Config{Store: st, Channel: ch, ChannelName: "teams", Agent: agent, Timing: engine.FastTiming(20 * time.Millisecond),
					Machine: "m", Log: writerFunc(func(b []byte) { logMu.Lock(); log.Write(b); logMu.Unlock() })})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- e.Run(ctx) }()
				return st, func() { cancel(); <-done; st.Release(); st.Close() }
			}
			// checks lists the converter's entries; undecided counts those
			// neither resent nor found.
			checks := func(st *store.Store) []store.LegacyFinalCheck {
				v, _, _ := st.Meta(store.MetaLegacyFinalChecks)
				var list []store.LegacyFinalCheck
				if v != "" {
					if err := json.Unmarshal([]byte(v), &list); err != nil {
						t.Fatal(err)
					}
				}
				return list
			}
			undecided := func(st *store.Store) int {
				n := 0
				for _, c := range checks(st) {
					if c.Found == "" {
						n++
					}
				}
				return n
			}
			resends := func() int {
				n := 0
				for _, m := range ch.Messages(chat) {
					if m.Marker != "" && m.Marker != "o2" && m.Marker != "o9" && strings.HasPrefix(m.Text, "🤖") && strings.Contains(m.Text, body) {
						n++
					}
				}
				return n
			}
			wait := func(what string, cond func() bool) {
				t.Helper()
				deadline := time.Now().Add(15 * time.Second)
				for !cond() {
					if time.Now().After(deadline) {
						logMu.Lock()
						t.Fatalf("timeout waiting for %s\nengine log:\n%s", what, log.String())
					}
					time.Sleep(20 * time.Millisecond)
				}
			}

			st, stop := start()
			wait("the check decided", func() bool { return undecided(st) == 0 })
			want := 0
			if tc.resent {
				want = 1
				wait("the answer resent", func() bool { return resends() == 1 })
			}
			time.Sleep(300 * time.Millisecond)
			list := checks(st)
			stop()
			if n := resends(); n != want {
				t.Fatalf("resent %d times, want %d", n, want)
			}
			// Resent: the entry is gone. Found: it stays with the post's id
			// (the receiver's first read may have found it before any page
			// of the history was read).
			if tc.resent != (len(list) == 0) || (!tc.resent && list[0].Found == "") {
				t.Fatalf("entries after the decision: %+v", list)
			}
			pages := ch.HistoryPages()
			if tc.resent && pages == 0 {
				t.Fatal("the chat history was not read")
			}
			// The old post is the helper's own, never the owner's prompt.
			if n := agent.runs.Load(); n != 0 {
				t.Fatalf("%d Codex turns ran on the old answer's text, want 0 (taken for a prompt)", n)
			}

			_, stop = start()
			time.Sleep(time.Second)
			stop()
			if n := resends(); n != want {
				t.Fatalf("after a restart: resent %d times, want %d", n, want)
			}
			if n := ch.HistoryPages(); n != pages {
				t.Fatalf("a restart read the history again: %d pages, then %d", pages, n)
			}
			if n := agent.runs.Load(); n != 0 {
				t.Fatalf("after a restart: %d Codex turns ran on the old answer's text, want 0", n)
			}
			logMu.Lock()
			defer logMu.Unlock()
			outcome := map[bool]string{true: "absent from the chat's history: sent now", false: "found in the chat's history"}[tc.resent]
			if !strings.Contains(log.String(), outcome) {
				t.Errorf("log lacks %q:\n%s", outcome, log.String())
			}
		})
	}
}
