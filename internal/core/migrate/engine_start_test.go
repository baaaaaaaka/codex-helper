package migrate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
func (idleAgent) Fork(context.Context, int64, string, string, string) (string, error) {
	return "", errors.New("no fork")
}
func (idleAgent) Models(context.Context) ([]engine.Model, error) { return nil, nil }
func (idleAgent) Maintain()                                      {}
func (idleAgent) Close()                                         {}

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
//   - the first start is not a fresh install: a recent interactive rollout
//     left for discovery gets its work chat, one older than the 120-day
//     window is never caught up.
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
	old := writeStartRollout(t, sessions, "thr-old", time.Now().Add(-130*24*time.Hour))
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
	waitFor("migrated chats read and the recent rollout tracked", func() bool {
		return migratedReads() >= 1 && sourceState(st, recent) == store.SrcTracking
	})
	time.Sleep(time.Second) // every live chat read once, cursors written
	if got := sourceState(st, old); got != "" && got != store.SrcBaseline {
		t.Errorf("rollout older than 120 days: state %q, want not caught up", got)
	}
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
