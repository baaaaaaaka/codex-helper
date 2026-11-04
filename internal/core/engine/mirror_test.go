package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	// Only the mirrored prompt ("🧑‍💻 User: …"), not the chat's intro notice.
	fail := doc.Label == labelUser && strings.Contains(doc.Markdown, "flaky") && f.fails > 0
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

// B3: the recent walk does not list a day directory again while its mtime
// is unchanged (a new rollout changes it); it only re-stats the rollouts
// still pending there (baselined, unreadable). The test restores the
// directory's mtime after adding a rollout to observe the skip.
func TestRecentWalkSkipsAnUnchangedDayDirectory(t *testing.T) {
	sessions := t.TempDir()
	now := time.Now()
	day := filepath.Join(sessions, now.UTC().Format("2006"), now.UTC().Format("01"), now.UTC().Format("02"))
	if err := os.MkdirAll(day, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name string) string {
		p := filepath.Join(day, name)
		if err := os.WriteFile(p, []byte("not a record\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	a := write("rollout-a.jsonl") // unreadable: pending
	old := now.Add(-time.Minute)
	if err := os.Chtimes(day, old, old); err != nil {
		t.Fatal(err)
	}
	stats := map[string]int{}
	e := &Engine{cfg: Config{SessionsDir: sessions}, t: ProductionTiming(), now: time.Now, log: io.Discard, readMeta: transcript.ReadMeta}
	e.statFile = func(p string) (transcript.FileInfo, error) {
		stats[p]++
		return transcript.Stat(p)
	}
	e.mirror = &mirrorState{files: map[string]*mirrorFile{}, known: map[string]string{}, checked: map[string]transcript.FileInfo{}, unreadable: map[string]transcript.FileInfo{}}
	ctx := context.Background()
	e.discover(ctx)
	if _, ok := e.mirror.unreadable[a]; !ok {
		t.Fatal("first walk did not see the rollout")
	}

	b := write("rollout-b.jsonl")
	if err := os.Chtimes(day, old, old); err != nil {
		t.Fatal(err)
	}
	e.discover(ctx)
	if _, ok := e.mirror.unreadable[b]; ok {
		t.Fatal("a day directory with an unchanged mtime was listed again")
	}
	if stats[a] != 1 {
		t.Fatalf("pending rollout stat'ed %d times by the skipped walk, want 1", stats[a])
	}

	changed := now.Add(-10 * time.Second)
	if err := os.Chtimes(day, changed, changed); err != nil {
		t.Fatal(err)
	}
	e.discover(ctx)
	if _, ok := e.mirror.unreadable[b]; !ok {
		t.Fatal("a day directory whose mtime changed was not listed again")
	}
}

// startupRig writes 3 day directories of 100 old rollouts each and returns
// an engine on them (readMeta reads a rollout's first record) and the
// rollouts newest first.
func startupRig(t *testing.T, readMeta func(string) (transcript.SessionMeta, error)) (*Engine, []string) {
	t.Helper()
	sessions := t.TempDir()
	base := time.Now().Add(-30 * 24 * time.Hour)
	var all []string
	n := 0
	for d := 0; d < 3; d++ {
		day := base.AddDate(0, 0, d)
		dir := filepath.Join(sessions, day.Format("2006"), day.Format("01"), day.Format("02"))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 100; i++ {
			n++
			id := fmt.Sprintf("00000000-0000-0000-0000-%012d", n)
			at := day.Add(time.Duration(i) * time.Minute)
			rec := fmt.Sprintf(`{"timestamp":%q,"type":"session_meta","payload":{"id":%q,"timestamp":%q,"cwd":"/w","originator":"codex_cli_rs","source":"cli"}}`+"\n",
				at.UTC().Format(time.RFC3339Nano), id, at.UTC().Format(time.RFC3339Nano))
			p := filepath.Join(dir, "rollout-"+id+".jsonl")
			if err := os.WriteFile(p, []byte(rec), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(p, at, at); err != nil {
				t.Fatal(err)
			}
			all = append(all, p)
		}
	}
	slices.Reverse(all)
	st, err := store.Open(filepath.Join(t.TempDir(), "core.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	e := &Engine{cfg: Config{SessionsDir: sessions}, st: st, t: ProductionTiming(), now: time.Now, log: io.Discard,
		statFile: transcript.Stat, readMeta: readMeta, convs: map[int64]*convState{}}
	return e, all
}

// firstBatch starts the mirror as mirrorLoop does and runs one batch of
// the start-up walk; it returns the rollouts classified.
func firstBatch(t *testing.T, e *Engine) map[string]string {
	t.Helper()
	if err := e.mirrorInit(); err != nil {
		t.Fatal(err)
	}
	e.startFirstWalk()
	e.discoverFirst(context.Background())
	return e.mirror.known
}

// checkNewestFirst fails unless known is the first len(known) of newest.
func checkNewestFirst(t *testing.T, known map[string]string, newest []string) {
	t.Helper()
	for _, p := range newest[:len(known)] {
		if _, ok := known[p]; !ok {
			t.Fatalf("%s not classified although %d rollouts were, newest first", filepath.Base(p), len(known))
		}
	}
}

// B4: the start-up walk classifies the rollouts a batch per stat tick,
// newest first, instead of reading every rollout's first record before the
// mirror loop starts (1.1 s in one second on a fresh install of 7,500).
func TestStartupWalkClassifiesABatchPerTick(t *testing.T) {
	e, newest := startupRig(t, transcript.ReadMeta)
	known := firstBatch(t, e)
	if len(known) == 0 || len(known) > firstWalkFiles {
		t.Fatalf("%d of 300 rollouts classified in the first batch, want 1..%d", len(known), firstWalkFiles)
	}
	checkNewestFirst(t, known, newest)
}

// W1b: the start-up walk runs one batch per stat tick and nothing else:
// not at start (the listing is the start's cost) and not on a mirror wake
// or a discovery tick, which made two batches in the seconds where the
// 10 s discovery tick met the 2 s stat tick.
func TestStartupWalkBatchesOnlyOnTheStatTick(t *testing.T) {
	var reads atomic.Int64
	counted := func(p string) (transcript.SessionMeta, error) {
		reads.Add(1)
		return transcript.ReadMeta(p)
	}
	run := func(e *Engine, until func() bool) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			e.mirrorLoop(ctx)
		}()
		until()
		cancel()
		<-done
	}
	e, _ := startupRig(t, counted)
	e.t.MirrorStat, e.t.Discovery = time.Hour, 20*time.Millisecond
	e.mirrorWake = make(chan struct{}, 1)
	run(e, func() bool {
		for i := 0; i < 10; i++ {
			e.mirrorWake <- struct{}{}
			time.Sleep(20 * time.Millisecond)
		}
		return true
	})
	if n := reads.Load(); n != 0 {
		t.Fatalf("%d first records read at start, on wakes and on discovery ticks, want 0 (stat tick only)", n)
	}

	e, newest := startupRig(t, counted)
	e.t.MirrorStat = 50 * time.Millisecond
	run(e, func() bool {
		deadline := time.Now().Add(10 * time.Second)
		for reads.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(150 * time.Millisecond) // a batch or two complete with their write
		return true
	})
	if len(e.mirror.known) == 0 {
		t.Fatal("no batch on the stat tick")
	}
	checkNewestFirst(t, e.mirror.known, newest)
}

// W1, W1b: a start-up batch also stops after about firstWalkBudget, so
// slow first-record reads (a cold or network disk) never make one long
// tick; classification stops at half of it, leaving the rest for the
// batch's store write.
func TestStartupWalkBatchStopsAtItsTimeBudget(t *testing.T) {
	const perFile = time.Millisecond
	e, newest := startupRig(t, func(p string) (transcript.SessionMeta, error) {
		time.Sleep(perFile)
		return transcript.ReadMeta(p)
	})
	known := firstBatch(t, e)
	// At least perFile each: the classification of a batch stops after
	// at most firstWalkClassify/perFile+1 reads.
	if max := int(firstWalkClassify/perFile) + 2; len(known) == 0 || len(known) > max {
		t.Fatalf("%d rollouts classified in the first batch at %s each, want 1..%d (classification budget %s of %s)",
			len(known), perFile, max, firstWalkClassify, firstWalkBudget)
	}
	checkNewestFirst(t, known, newest)
}

// No discovery or catch-up window (owner decision 2026-11-05): with no age
// cut the older-directory cycle covers the whole sessions tree, so a tick
// considers what it listed for at most firstWalkClassify and leaves the
// rest for the next tick, which lists nothing new before it is done.
func TestOldWalkTickStopsAtItsTimeBudget(t *testing.T) {
	const perFile = time.Millisecond
	var reads atomic.Int64
	e, newest := startupRig(t, func(p string) (transcript.SessionMeta, error) {
		reads.Add(1)
		time.Sleep(perFile)
		return transcript.ReadMeta(p)
	})
	if err := e.mirrorInit(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	e.discoverOld(ctx)
	if max := int64(firstWalkClassify/perFile) + 2; reads.Load() == 0 || reads.Load() > max {
		t.Fatalf("the first old-walk tick read %d first records at %s each, want 1..%d", reads.Load(), perFile, max)
	}
	dirs := len(e.mirror.oldDirs)
	e.discoverOld(ctx)
	if len(e.mirror.oldFound) > 0 && len(e.mirror.oldDirs) != dirs {
		t.Fatalf("a tick listed more directories (%d left, was %d) while %d listed rollouts waited", len(e.mirror.oldDirs), dirs, len(e.mirror.oldFound))
	}
	for i := 0; i < 1000 && (len(e.mirror.oldFound) > 0 || len(e.mirror.oldDirs) > 0); i++ {
		e.discoverOld(ctx)
	}
	if len(e.mirror.known) != len(newest) || reads.Load() != int64(len(newest)) {
		t.Fatalf("the cycle classified %d of %d rollouts with %d reads", len(e.mirror.known), len(newest), reads.Load())
	}
}

// No discovery or catch-up window (owner decision 2026-11-05): a local
// session last updated 200 days ago, while the helper was down, is
// discovered after a restart and its whole history is caught up.
func TestSessionFrom200DaysAgoIsDiscoveredAndCaughtUp(t *testing.T) {
	r, sessions := mirrorRig(t)
	r.wait("mirror initialised", func() bool {
		v, _, err := r.st.Meta(metaMirrorInit)
		return err == nil && v != ""
	})
	r.stop()
	at := time.Now().Add(-200 * 24 * time.Hour)
	w, err := rollout.New(sessions, t.TempDir(), rollout.Options{Start: at})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "a prompt from 200 days ago", Answer: "an answer from 200 days ago"}); err != nil {
		t.Fatal(err)
	}
	if err := w.SetModTime(at.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	r.start()
	chat := r.localChat()
	r.wait("the 200-day-old turn caught up", func() bool {
		return len(r.own(chat, "a prompt from 200 days ago")) == 1 && len(r.own(chat, "an answer from 200 days ago")) == 1
	})
}

// W1c: the store keeps a rollout's mtime in milliseconds. A tracked rollout
// restored at start-up, unchanged since, must compare equal to its
// nanosecond stat and not be read again: before the fix every tracked
// rollout was read, and committed its own transaction, on each start.
// mirrorRead drops the file of a closed conversation, which shows here
// whether it was called.
func TestRestoredRolloutWithMillisecondMTimeIsNotReadAgain(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	e := &Engine{t: ProductionTiming(), now: func() time.Time { return now }, log: io.Discard, convs: map[int64]*convState{}}
	e.mirror = &mirrorState{files: map[string]*mirrorFile{}, known: map[string]string{}}
	e.convs[1] = &convState{kind: store.KindWork, state: store.ConvClosed}
	mtime := now.Add(-time.Hour).Add(123_456_789 * time.Nanosecond) // sub-millisecond part
	stat := transcript.FileInfo{Size: 100, MTime: mtime, Inode: 7}
	stats := 0
	e.statFile = func(string) (transcript.FileInfo, error) { stats++; return stat, nil }
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	restore := func() {
		e.mirror.files[path] = &mirrorFile{src: store.TranscriptSource{Path: path, ConvID: 1, Size: 100, MTime: ms(mtime), Inode: 7},
			tracker: transcript.NewTracker("t", 100, 1, nil),
			info:    transcript.FileInfo{Size: 100, MTime: fromMS(ms(mtime)), Inode: 7}} // as mirrorInit restores it
	}
	restore()
	e.mirrorPoll(context.Background())
	if stats != 1 {
		t.Fatalf("stat'ed %d times, want 1", stats)
	}
	if _, ok := e.mirror.files[path]; !ok {
		t.Fatalf("an unchanged restored rollout was read again")
	}

	// A real change (a later mtime, same size) is still read.
	stat.MTime = mtime.Add(time.Second)
	restore()
	e.mirrorPoll(context.Background())
	if _, ok := e.mirror.files[path]; ok {
		t.Fatalf("a rollout with a newer mtime was not read")
	}
}
