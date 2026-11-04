package transcript

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/internal/leakcheck"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

func TestMain(m *testing.M) { leakcheck.Main(m) }

func readAll(t *testing.T, path string, tr *Tracker) []Output {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	lines, next, err := Read(f, tr.Offset(), 0)
	if err != nil {
		t.Fatal(err)
	}
	out := tr.Feed(lines)
	tr.Advance(next)
	return out
}

func keys(out []Output) string {
	var s []string
	for _, o := range out {
		kind := "F"
		if o.User {
			kind = "U"
		}
		s = append(s, kind+":"+o.Text)
	}
	return strings.Join(s, " | ")
}

func newRollout(t *testing.T, opts rollout.Options) *rollout.Writer {
	t.Helper()
	w, err := rollout.New(filepath.Join(t.TempDir(), "sessions"), "/work/x", opts)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestTrackerMirrorsUserAndFinalOfTUITurns(t *testing.T) {
	w := newRollout(t, rollout.Options{})
	mustTurn(t, w, rollout.Turn{Prompt: "first question", Commentary: "looking", Answer: "first answer"})
	tr := NewTracker("", 0, 0, nil)
	out := readAll(t, w.Path, tr)
	if got := keys(out); got != "U:first question | F:first answer" {
		t.Fatalf("outputs %q", got)
	}
	if tr.ThreadID() != w.SessionID {
		t.Fatalf("thread %q want %q", tr.ThreadID(), w.SessionID)
	}
	mustTurn(t, w, rollout.Turn{Prompt: "second", Answer: "second answer"})
	out2 := readAll(t, w.Path, tr)
	if got := keys(out2); got != "U:second | F:second answer" {
		t.Fatalf("appended outputs %q", got)
	}
	// Keys are stable: a fresh tracker from 0 produces the same keys.
	again := readAll(t, w.Path, NewTracker("", 0, 0, nil))
	all := append(out, out2...)
	for i := range all {
		if again[i].Key != all[i].Key {
			t.Fatalf("key %d %q != %q", i, again[i].Key, all[i].Key)
		}
	}
	if off, _ := tr.Resume(); off != tr.Offset() {
		t.Fatalf("resume %d offset %d with no open turn", off, tr.Offset())
	}
}

func mustTurn(t *testing.T, w *rollout.Writer, turn rollout.Turn) {
	t.Helper()
	if _, err := w.AppendTurn(turn); err != nil {
		t.Fatal(err)
	}
}

func TestTrackerSkipsHelperTurnsByClientID(t *testing.T) {
	w := newRollout(t, rollout.Options{Source: "vscode", Originator: "codex_vscode"})
	mustTurn(t, w, rollout.Turn{ClientID: "cxp-0badc0de-7", Prompt: "from teams", Answer: "teams answer"})
	mustTurn(t, w, rollout.Turn{Prompt: "typed in the TUI", Answer: "tui answer"})
	skip := func(id string) bool { return strings.HasPrefix(id, "cxp-0badc0de-") }
	out := readAll(t, w.Path, NewTracker("", 0, 0, skip))
	if got := keys(out); got != "U:typed in the TUI | F:tui answer" {
		t.Fatalf("outputs %q", got)
	}
}

func TestTrackerResumesFromOpenTurnStart(t *testing.T) {
	w := newRollout(t, rollout.Options{})
	open, err := w.BeginTurn(rollout.Turn{Prompt: "long task"})
	if err != nil {
		t.Fatal(err)
	}
	tr := NewTracker("", 0, 0, nil)
	out := readAll(t, w.Path, tr)
	if got := keys(out); got != "U:long task" {
		t.Fatalf("outputs %q", got)
	}
	resume, line := tr.Resume()
	if resume >= tr.Offset() || resume == 0 {
		t.Fatalf("resume %d should be the open turn start (< %d)", resume, tr.Offset())
	}
	if err := w.FinishTurn(open, rollout.Turn{Answer: "done"}); err != nil {
		t.Fatal(err)
	}
	// After a restart: a new tracker from the persisted position.
	tr2 := NewTracker(w.SessionID, resume, line, nil)
	out2 := readAll(t, w.Path, tr2)
	if got := keys(out2); got != "U:long task | F:done" {
		t.Fatalf("resumed outputs %q", got)
	}
	if out2[0].Key != out[0].Key {
		t.Fatalf("user key changed after resume: %q vs %q", out2[0].Key, out[0].Key)
	}
}

func TestTrackerAbortedTurnHasNoFinal(t *testing.T) {
	w := newRollout(t, rollout.Options{})
	mustTurn(t, w, rollout.Turn{Prompt: "stop me", Aborted: true})
	mustTurn(t, w, rollout.Turn{Prompt: "next", Answer: "ok"})
	out := readAll(t, w.Path, NewTracker("", 0, 0, nil))
	if got := keys(out); got != "U:stop me | U:next | F:ok" {
		t.Fatalf("outputs %q", got)
	}
}

func writeLines(t *testing.T, lines ...string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "rollout-x.jsonl")
	if err := os.WriteFile(p, []byte(strings.Join(lines, "")), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReadLeavesIncompleteTailAndAlignsMidLine(t *testing.T) {
	p := writeLines(t, "aaa\n", "bbbb\n", "cc")
	f, _ := os.Open(p)
	defer f.Close()
	lines, next, err := Read(f, 0, 0)
	if err != nil || len(lines) != 2 || next != 9 {
		t.Fatalf("lines %d next %d err %v", len(lines), next, err)
	}
	if string(lines[1].Data) != "bbbb" || lines[1].Off != 4 || lines[1].End != 9 {
		t.Fatalf("line %+v", lines[1])
	}
	// Offset 2 is inside "aaa": skip to the next newline.
	lines, next, _ = Read(f, 2, 0)
	if len(lines) != 1 || string(lines[0].Data) != "bbbb" || next != 9 {
		t.Fatalf("aligned read: %d lines next %d", len(lines), next)
	}
	// Offset exactly at a line start is kept.
	lines, _, _ = Read(f, 4, 0)
	if len(lines) != 1 || string(lines[0].Data) != "bbbb" {
		t.Fatalf("boundary read: %+v", lines)
	}
	// Past the end: nothing, offset unchanged.
	lines, next, err = Read(f, 100, 0)
	if len(lines) != 0 || next != 100 || err != nil {
		t.Fatalf("past end: %d %d %v", len(lines), next, err)
	}
}

func TestReadSkipsOversizeLines(t *testing.T) {
	big := strings.Repeat("x", MaxLine+10)
	p := writeLines(t, big+"\n", "ok\n")
	f, _ := os.Open(p)
	defer f.Close()
	lines, next, err := Read(f, 0, 0)
	if err != nil || len(lines) != 2 || !lines[0].Oversize || lines[0].Data != nil || string(lines[1].Data) != "ok" {
		t.Fatalf("lines %d err %v", len(lines), err)
	}
	if next != int64(len(big)+4) {
		t.Fatalf("next %d", next)
	}
}

func TestRewriteDetectionAndRealign(t *testing.T) {
	w := newRollout(t, rollout.Options{})
	mustTurn(t, w, rollout.Turn{Prompt: "one", Answer: "a1"})
	mustTurn(t, w, rollout.Turn{Prompt: "two", Answer: "a2"})
	tr := NewTracker("", 0, 0, nil)
	if got := keys(readAll(t, w.Path, tr)); got != "U:one | F:a1 | U:two | F:a2" {
		t.Fatalf("first read %q", got)
	}
	st, _ := Stat(w.Path)
	if Rewritten(st, tr.Offset(), st.Inode) {
		t.Fatal("unchanged file reported as rewritten")
	}
	// Codex rewrites the file (compaction): same records minus the first
	// turn, then a new turn. Replace the file (new inode) and shrink it.
	data, _ := os.ReadFile(w.Path)
	lines := strings.SplitAfter(string(data), "\n")
	var kept []string
	for _, l := range lines {
		if strings.Contains(l, "\"one\"") || strings.Contains(l, "a1") {
			continue
		}
		kept = append(kept, l)
	}
	tmp := w.Path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strings.Join(kept, "")), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, w.Path); err != nil {
		t.Fatal(err)
	}
	upTo := tr.LastTime()
	time.Sleep(5 * time.Millisecond)
	mustTurn(t, w, rollout.Turn{Prompt: "three", Answer: "a3"})
	st2, _ := Stat(w.Path)
	if !Rewritten(st2, tr.Offset(), st.Inode) {
		t.Fatal("inode change not reported as a rewrite")
	}
	tr.Realign(upTo)
	if got := keys(readAll(t, w.Path, tr)); got != "U:three | F:a3" {
		t.Fatalf("after realign %q", got)
	}
	// A shrink alone (same inode) is a rewrite too.
	if !Rewritten(FileInfo{Size: 10, Inode: st2.Inode}, 100, st2.Inode) {
		t.Fatal("shrink not reported")
	}
	if Rewritten(FileInfo{Size: 200, Inode: st2.Inode}, 100, st2.Inode) {
		t.Fatal("growth reported as rewrite")
	}
}

func TestLegacyEventsAndSubagentItems(t *testing.T) {
	ts := "2026-01-02T03:04:05.000Z"
	p := writeLines(t,
		`{"timestamp":"`+ts+`","type":"session_meta","payload":{"id":"thr-1","timestamp":"`+ts+`","cwd":"/w","originator":"codex_cli_rs","source":"cli"}}`+"\n",
		`{"timestamp":"`+ts+`","type":"event_msg","payload":{"type":"task_started","turn_id":"t1"}}`+"\n",
		`{"timestamp":"`+ts+`","type":"event_msg","payload":{"type":"user_message","message":"legacy prompt","images":[]}}`+"\n",
		`{"timestamp":"`+ts+`","type":"event_msg","payload":{"type":"item_completed","thread_id":"thr-1","turn_id":"t1","item":{"type":"UserMessage","client_id":"c1","content":[{"type":"text","text":"legacy prompt"}]}}}`+"\n",
		`{"timestamp":"`+ts+`","type":"event_msg","payload":{"type":"item_completed","thread_id":"sub-9","turn_id":"t1","item":{"type":"AgentMessage","phase":"final_answer","content":[{"type":"Text","text":"subagent says"}]}}}`+"\n",
		`{"timestamp":"`+ts+`","type":"event_msg","payload":{"type":"agent_message","message":"legacy answer"}}`+"\n",
		`{"timestamp":"`+ts+`","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ignored"}]}}`+"\n",
		`{"timestamp":"`+ts+`","type":"event_msg","payload":{"type":"task_complete","turn_id":"t1","last_agent_message":null}}`+"\n",
	)
	out := readAll(t, p, NewTracker("", 0, 0, nil))
	if got := keys(out); got != "U:legacy prompt | F:legacy answer" {
		t.Fatalf("outputs %q", got)
	}
	if out[0].Key != "thr-1:t1:u1" || out[1].Key != "thr-1:t1:f" {
		t.Fatalf("keys %q %q", out[0].Key, out[1].Key)
	}
}

func TestInteractiveClassification(t *testing.T) {
	cases := []struct {
		meta string
		want bool
	}{
		{`"source":"cli","originator":"codex_cli_rs"`, true},
		{`"source":"vscode","originator":"codex_vscode"`, true},
		{`"source":"exec","originator":"codex_exec"`, false},
		{`"source":{"subagent":{"thread_spawn":{}}},"originator":"codex_cli_rs"`, false},
		{`"source":"vscode","originator":"codex-helper"`, false},
	}
	for i, c := range cases {
		p := writeLines(t, fmt.Sprintf(`{"timestamp":"2026-01-02T03:04:05Z","type":"session_meta","payload":{"id":"s%d","cwd":"/w",%s}}`+"\n", i, c.meta))
		m, err := ReadMeta(p)
		if err != nil {
			t.Fatal(err)
		}
		if m.Interactive() != c.want {
			t.Errorf("%s: interactive=%v want %v", c.meta, m.Interactive(), c.want)
		}
	}
}

func TestWalkWindowAndRecentDirs(t *testing.T) {
	root := filepath.Join(t.TempDir(), "sessions")
	now := time.Now()
	mk := func(age time.Duration) string {
		at := now.Add(-age)
		w, err := rollout.New(root, "/w", rollout.Options{Start: at})
		if err != nil {
			t.Fatal(err)
		}
		mustTurn(t, w, rollout.Turn{Prompt: "p", Answer: "a"})
		if err := os.Chtimes(w.Path, at, at); err != nil {
			t.Fatal(err)
		}
		return w.Path
	}
	old := mk(121 * 24 * time.Hour)
	mid := mk(119 * 24 * time.Hour)
	recent := mk(time.Hour)
	if err := os.WriteFile(filepath.Join(filepath.Dir(recent), "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	found, err := Walk(root, now.Add(-Window), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range found {
		got = append(got, f.Path)
	}
	if strings.Join(got, ",") != mid+","+recent {
		t.Fatalf("window walk %v (old %s)", got, old)
	}
	found, _ = Walk(root, time.Time{}, now.Add(-48*time.Hour))
	if len(found) != 1 || found[0].Path != recent {
		t.Fatalf("recent-dir walk %v", found)
	}
	if fp := FirstPrompt(recent, 1<<20); fp != "p" {
		t.Fatalf("first prompt %q", fp)
	}
	if found, err := Walk(filepath.Join(root, "missing"), time.Time{}, time.Time{}); err != nil || len(found) != 0 {
		t.Fatalf("missing root: %v %v", found, err)
	}
}

func TestParseSkipsLargeNonEventRecordsCheaply(t *testing.T) {
	line := []byte(`{"timestamp":"2026-01-02T03:04:05Z","ordinal":7,"type":"response_item","payload":{"type":"function_call_output","output":"` + strings.Repeat("y", 1000) + `"}}`)
	if typ, ok := topType(line); !ok || typ != "response_item" {
		t.Fatalf("topType %q %v", typ, ok)
	}
	if Parse(line).Kind != KindOther {
		t.Fatal("response_item parsed as an event")
	}
	if _, ok := topType([]byte(`{"payload":{"type":"x"},"type":"event_msg"}`)); ok {
		t.Fatal("nested type key accepted as top-level")
	}
}
