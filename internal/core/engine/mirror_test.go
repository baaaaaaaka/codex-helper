package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// mirrorRig is a rig whose engine mirrors rollouts under sessions.
func mirrorRig(t *testing.T, tweak ...func(*Config)) (*rig, string) {
	sessions := t.TempDir()
	r := newRig(t, append([]func(*Config){func(c *Config) { c.SessionsDir = sessions }}, tweak...)...)
	r.control()
	return r, sessions
}

// localChat waits for the work chat the mirror created for a local session.
func (r *rig) localChat() string {
	var chat string
	r.wait("work chat of the local session", func() bool {
		chat = r.chatOf("s001")
		return chat != ""
	})
	return chat
}

// stripUserIDs makes the rollout's prompts id-less (older Codex): their
// mirror keys then depend on the line number.
func stripUserIDs(t *testing.T, data []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	for _, ln := range bytes.Split(bytes.TrimRight(data, "\n"), []byte("\n")) {
		var rec map[string]any
		if err := json.Unmarshal(ln, &rec); err != nil {
			t.Fatal(err)
		}
		if p, ok := rec["payload"].(map[string]any); ok && p["type"] == "message" && p["role"] == "user" {
			delete(p, "id")
			ln, _ = json.Marshal(rec)
		}
		out.Write(ln)
		out.WriteByte('\n')
	}
	return out.Bytes()
}

// filler is n bytes of tool output records the mirror reads but never posts.
func filler(n int) []byte {
	var b bytes.Buffer
	chunk := strings.Repeat("x", 1<<20)
	for b.Len() < n {
		rec, _ := json.Marshal(map[string]any{"timestamp": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), "type": "response_item",
			"payload": map[string]any{"type": "function_call_output", "call_id": "c", "output": chunk}})
		b.Write(rec)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

func appendFile(t *testing.T, path string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
}

// Review M8: a rewritten rollout larger than one mirror pass is realigned
// over every pass, not only the first: records of the old file are not
// posted again even when their keys changed (id-less prompts after a line
// shift); only what is new is posted.
func TestMirrorRealignsRewrittenRolloutAcrossPasses(t *testing.T) {
	r, sessions := mirrorRig(t)
	cwd, stage := t.TempDir(), t.TempDir()
	// Built outside the sessions directory, then moved in whole.
	b, err := rollout.New(stage, cwd, rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	pre, _ := os.ReadFile(b.Path)
	np := bytes.Count(pre, []byte("\n"))
	if _, err := b.AppendTurn(rollout.Turn{Prompt: "first local prompt", Answer: "first local answer"}); err != nil {
		t.Fatal(err)
	}
	appendFile(t, b.Path, filler(10<<20))
	if _, err := b.AppendTurn(rollout.Turn{Prompt: "second local prompt", Answer: "second local answer"}); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(b.Path)
	data = stripUserIDs(t, data)
	rel, _ := filepath.Rel(stage, b.Path)
	dst := filepath.Join(sessions, rel)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b.Path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(b.Path, dst); err != nil {
		t.Fatal(err)
	}
	chat := r.localChat()
	r.wait("second answer mirrored", func() bool { return len(r.own(chat, "second local answer")) == 1 })

	// Rewrite: one extra line after the preamble shifts every line number,
	// and a new turn is added; the file is replaced by rename.
	time.Sleep(50 * time.Millisecond)
	lines := bytes.SplitAfter(data, []byte("\n"))
	extra, _ := json.Marshal(map[string]any{"timestamp": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), "type": "event_msg",
		"payload": map[string]any{"type": "token_count", "info": nil}})
	var nb bytes.Buffer
	nb.Write(bytes.Join(lines[:np], nil))
	nb.Write(append(extra, '\n'))
	nb.Write(bytes.Join(lines[np:], nil))
	tmp := filepath.Join(stage, "rewrite.jsonl")
	if err := os.WriteFile(tmp, nb.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	w := rollout.Open(tmp, b.SessionID, cwd)
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "third local prompt", Answer: "third local answer"}); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		t.Fatal(err)
	}
	r.wait("third answer mirrored", func() bool { return len(r.own(chat, "third local answer")) == 1 })
	time.Sleep(500 * time.Millisecond)
	for _, s := range []string{"first local prompt", "first local answer", "second local prompt", "second local answer", "third local prompt"} {
		if n := len(r.own(chat, s)); n != 1 {
			t.Errorf("%q mirrored %d times, want 1", s, n)
		}
	}
}

// flakyRender fails Render of mirrored prompts containing a marker a few
// times.
type flakyRender struct {
	*channeltest.Fake
	mu    sync.Mutex
	fails int
}

func (f *flakyRender) Render(ctx context.Context, doc channel.RichDoc) ([]channel.Part, error) {
	f.mu.Lock()
	// Only the mirrored prompt ("User: …"), not the chat's intro notice.
	fail := strings.HasPrefix(doc.Markdown, "**"+labelUser) && strings.Contains(doc.Markdown, "flaky") && f.fails > 0
	if fail {
		f.fails--
	}
	f.mu.Unlock()
	if fail {
		return nil, errors.New("test: render failed")
	}
	return f.Fake.Render(ctx, doc)
}

// Review M7: records whose rendering failed are re-read and posted after a
// backoff, not lost until a restart.
func TestMirrorRenderFailureIsRetried(t *testing.T) {
	r, sessions := mirrorRig(t, func(c *Config) {
		c.Channel = &flakyRender{Fake: c.Channel.(*channeltest.Fake), fails: 3}
	})
	w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "flaky local prompt", Answer: "flaky local answer"}); err != nil {
		t.Fatal(err)
	}
	chat := r.localChat()
	r.wait("flaky records mirrored", func() bool {
		return len(r.own(chat, "flaky local prompt")) == 1 && len(r.own(chat, "flaky local answer")) == 1
	})
}

// A rollout baselined at the first start is skipped by later discovery
// passes while unchanged (review L11), and still mirrored from where it
// was once it grows.
func TestBaselinedRolloutMirroredOnceItGrows(t *testing.T) {
	sessions := t.TempDir()
	w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "before the helper", Answer: "old answer"}); err != nil {
		t.Fatal(err)
	}
	r := newRig(t, func(c *Config) { c.SessionsDir = sessions })
	r.control()
	time.Sleep(time.Second) // many discovery passes over the unchanged file
	if chat := r.chatOf("s001"); chat != "" {
		t.Fatalf("an unchanged baselined rollout got a work chat")
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "after the helper", Answer: "new answer"}); err != nil {
		t.Fatal(err)
	}
	chat := r.localChat()
	r.wait("new turn mirrored", func() bool {
		return len(r.own(chat, "after the helper")) == 1 && len(r.own(chat, "new answer")) == 1
	})
	if n := len(r.own(chat, "before the helper")); n != 0 {
		t.Fatalf("the baselined turn was posted %d times", n)
	}
}

// B1: the rollout stat is tiered. A rollout that changed within
// MirrorHotFor, or whose conversation runs a turn, is stat'ed every
// MirrorStat; other tracked rollouts every MirrorIdleStat, and those of
// parked conversations every MirrorParkedStat, which bounds how late a
// local session wakes its parked chat.
func TestMirrorStatIsTiered(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	tm := ProductionTiming()
	stats := map[string][]time.Time{}
	infos := map[string]transcript.FileInfo{}
	e := &Engine{t: tm, now: func() time.Time { return now }, log: io.Discard, convs: map[int64]*convState{}}
	e.statFile = func(p string) (transcript.FileInfo, error) {
		stats[p] = append(stats[p], now)
		return infos[p], nil
	}
	e.mirror = &mirrorState{files: map[string]*mirrorFile{}, known: map[string]string{}}
	add := func(path string, conv int64, cs *convState, mtime time.Time) {
		e.convs[conv] = cs
		info := transcript.FileInfo{Size: 100, MTime: mtime, Inode: 1}
		infos[path] = info
		e.mirror.files[path] = &mirrorFile{src: store.TranscriptSource{Path: path, ConvID: conv},
			tracker: transcript.NewTracker("t", 100, 1, nil), info: info}
	}
	missing := filepath.Join(t.TempDir(), "missing")
	hot, running, idle, parked := missing+"-hot", missing+"-running", missing+"-idle", missing+"-parked"
	add(hot, 1, &convState{kind: store.KindWork, state: store.ConvActive}, now.Add(-time.Minute))
	add(running, 2, &convState{kind: store.KindWork, state: store.ConvActive, running: &runState{}}, now.Add(-time.Hour))
	add(idle, 3, &convState{kind: store.KindWork, state: store.ConvActive}, now.Add(-time.Hour))
	add(parked, 4, &convState{kind: store.KindWork, state: store.ConvParked}, now.Add(-time.Hour))
	ctx := context.Background()
	start := now
	for now.Sub(start) < 2*time.Minute { // the mirror loop's stat tick
		e.mirrorPoll(ctx)
		now = now.Add(tm.MirrorStat)
	}
	for _, c := range []struct {
		path     string
		min, max int
	}{{hot, 60, 60}, {running, 60, 60}, {idle, 4, 5}, {parked, 2, 3}} {
		if n := len(stats[c.path]); n < c.min || n > c.max {
			t.Errorf("%s stat'ed %d times in 2 min, want %d..%d", filepath.Base(c.path), n, c.min, c.max)
		}
	}

	// A local session appends to the parked conversation's rollout: the
	// stat sees it within MirrorParkedStat (plus one tick).
	changed := now
	infos[parked] = transcript.FileInfo{Size: 200, MTime: now, Inode: 1}
	for now.Sub(changed) <= tm.MirrorParkedStat+tm.MirrorStat {
		e.mirrorPoll(ctx)
		now = now.Add(tm.MirrorStat)
	}
	seen := false
	for _, at := range stats[parked] {
		seen = seen || !at.Before(changed)
	}
	if !seen {
		t.Fatalf("parked rollout change not stat'ed within %s", tm.MirrorParkedStat+tm.MirrorStat)
	}
}
