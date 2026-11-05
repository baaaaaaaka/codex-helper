package transcript

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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
		kind := map[OutputKind]string{OutputPrompt: "U", OutputFinal: "F", OutputStatus: "S", OutputCompact: "C"}[o.Kind]
		if o.User != (o.Kind == OutputPrompt) {
			kind += "!user"
		}
		text := o.Text
		if o.Kind == OutputCompact && text == CompactText {
			text = "compacted"
		}
		s = append(s, kind+":"+text)
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
	if got := keys(out); got != "U:first question | S:looking | F:first answer" {
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
		if again[i].RecordID != all[i].RecordID || !strings.HasPrefix(all[i].RecordID, "msg_") {
			t.Fatalf("record id %d %q != %q", i, again[i].RecordID, all[i].RecordID)
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
	skip := func(id, _ string) bool { return strings.HasPrefix(id, "cxp-0badc0de-") }
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
	if out2[0].RecordID != out[0].RecordID {
		t.Fatalf("user record id changed after resume: %q vs %q", out2[0].RecordID, out[0].RecordID)
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
	first := readAll(t, w.Path, NewTracker("", 0, 0, nil))
	time.Sleep(5 * time.Millisecond)
	mustTurn(t, w, rollout.Turn{Prompt: "three", Answer: "a3"})
	st2, _ := Stat(w.Path)
	if !Rewritten(st2, tr.Offset(), st.Inode) {
		t.Fatal("inode change not reported as a rewrite")
	}
	tr.Restart()
	got := readAll(t, w.Path, tr)
	if keys(got) != "U:two | F:a2 | U:three | F:a3" {
		t.Fatalf("after restart %q", keys(got))
	}
	// Record ids survive the rewrite, so the caller can realign on them.
	if got[0].RecordID != first[0].RecordID || got[1].RecordID != first[1].RecordID {
		t.Fatalf("record ids moved: %q %q vs %q %q", got[0].RecordID, got[1].RecordID, first[0].RecordID, first[1].RecordID)
	}
	// A shrink alone (same inode) is a rewrite too.
	if !Rewritten(FileInfo{Size: 10, Inode: st2.Inode}, 100, st2.Inode) {
		t.Fatal("shrink not reported")
	}
	if Rewritten(FileInfo{Size: 200, Inode: st2.Inode}, 100, st2.Inode) {
		t.Fatal("growth reported as rewrite")
	}
}

func TestLegacyRecordsAndSubagentItems(t *testing.T) {
	ts := "2026-01-02T03:04:05.000Z"
	p := writeLines(t,
		`{"timestamp":"`+ts+`","type":"session_meta","payload":{"id":"thr-1","timestamp":"`+ts+`","cwd":"/w","originator":"codex_cli_rs","source":"cli"}}`+"\n",
		`{"timestamp":"`+ts+`","type":"event_msg","payload":{"type":"task_started","turn_id":"t1"}}`+"\n",
		`{"timestamp":"`+ts+`","type":"event_msg","payload":{"type":"user_message","message":"legacy prompt","images":[]}}`+"\n",
		`{"timestamp":"`+ts+`","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"legacy prompt"}]}}`+"\n",
		`{"timestamp":"`+ts+`","type":"event_msg","payload":{"type":"item_completed","thread_id":"sub-9","turn_id":"t1","item":{"type":"AgentMessage","phase":"final_answer","content":[{"type":"Text","text":"subagent says"}]}}}`+"\n",
		`{"timestamp":"`+ts+`","type":"event_msg","payload":{"type":"agent_message","message":"legacy answer"}}`+"\n",
		`{"timestamp":"`+ts+`","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"legacy answer"}]}}`+"\n",
		`{"timestamp":"`+ts+`","type":"event_msg","payload":{"type":"task_complete","turn_id":"t1","last_agent_message":"legacy answer"}}`+"\n",
	)
	out := readAll(t, p, NewTracker("", 0, 0, nil))
	if got := keys(out); got != "U:legacy prompt | F:legacy answer" {
		t.Fatalf("outputs %q", got)
	}
	if out[0].RecordID != "t:t1:prompt:4" || out[1].RecordID != "t:t1:final:8" {
		t.Fatalf("record ids %q %q", out[0].RecordID, out[1].RecordID)
	}
	if k := out[1].Key("thr-1", 1); k != "mirror:thr-1:t:t1:final:8:1" {
		t.Fatalf("key %q", k)
	}
}

// A turn can write several final_answer items without ids (fidelity run,
// s698): each is a record of its own, with an id that does not depend on
// where the scan began.
func TestSeveralIDlessFinalsInOneTurn(t *testing.T) {
	ts := "2026-01-02T03:04:05.000Z"
	final := func(text string) string {
		return `{"timestamp":"` + ts + `","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"` + text + `"}]}}` + "\n"
	}
	lines := []string{
		`{"timestamp":"` + ts + `","type":"session_meta","payload":{"id":"thr-1","timestamp":"` + ts + `","cwd":"/w","originator":"codex_cli_rs","source":"cli"}}` + "\n",
		`{"timestamp":"` + ts + `","type":"event_msg","payload":{"type":"task_started","turn_id":"t1"}}` + "\n",
		`{"timestamp":"` + ts + `","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"prompt"}]}}` + "\n",
		final("first"),
		final("second"),
		final("third"),
		`{"timestamp":"` + ts + `","type":"event_msg","payload":{"type":"task_complete","turn_id":"t1","last_agent_message":"third"}}` + "\n",
	}
	p := writeLines(t, lines...)
	out := readAll(t, p, NewTracker("", 0, 0, nil))
	if got := keys(out); got != "U:prompt | F:first | F:second | F:third" {
		t.Fatalf("outputs %q", got)
	}
	want := []string{"t:t1:prompt:3", "t:t1:final:4", "t:t1:final:5", "t:t1:final:6"}
	for i, o := range out {
		if o.RecordID != want[i] {
			t.Fatalf("record %d id %q, want %q", i, o.RecordID, want[i])
		}
	}
	// Resumed after the first final (a mid-turn scan): the same ids.
	var off int64
	for _, l := range lines[:4] {
		off += int64(len(l))
	}
	mid := readAll(t, p, NewTracker("thr-1", off, 4, nil))
	if got := keys(mid); got != "F:second | F:third" {
		t.Fatalf("resumed outputs %q", got)
	}
	if mid[0].RecordID != want[2] || mid[1].RecordID != want[3] {
		t.Fatalf("resumed ids %q %q", mid[0].RecordID, mid[1].RecordID)
	}
}

// An open turn of an older Codex (no user item) is held until a record
// decides it; Settle decides it when none will (fidelity run, s671), unless
// the helper ran it.
func TestSettleReleasesAnAbandonedTurn(t *testing.T) {
	ts := "2026-01-02T03:04:05.000Z"
	write := func(turn, prompt string) string {
		return writeLines(t,
			`{"timestamp":"`+ts+`","type":"session_meta","payload":{"id":"thr-1","timestamp":"`+ts+`","cwd":"/w","originator":"codex_cli_rs","source":"cli"}}`+"\n",
			`{"timestamp":"`+ts+`","type":"event_msg","payload":{"type":"task_started","turn_id":"`+turn+`"}}`+"\n",
			`{"timestamp":"`+ts+`","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"`+prompt+`"}]}}`+"\n",
		)
	}
	tr := NewTracker("", 0, 0, nil)
	if out := readAll(t, write("t1", "abandoned prompt"), tr); len(out) != 0 {
		t.Fatalf("undecided turn released early: %q", keys(out))
	}
	out := tr.Settle()
	if got := keys(out); got != "U:abandoned prompt" || out[0].RecordID != "t:t1:prompt:3" {
		t.Fatalf("settled %q %+v", got, out)
	}
	if again := tr.Settle(); len(again) != 0 {
		t.Fatalf("settled twice: %q", keys(again))
	}
	if off, _ := tr.Resume(); off == 0 {
		t.Fatal("resume moved to the file start")
	}
	// A helper-run turn (by its Codex turn id) stays unmirrored.
	skip := func(_, turn string) bool { return turn == "t2" }
	tr2 := NewTracker("", 0, 0, skip)
	readAll(t, write("t2", "from teams"), tr2)
	if out := tr2.Settle(); len(out) != 0 {
		t.Fatalf("helper turn settled as local: %q", keys(out))
	}
}

func TestLegacyTeamsWrapperAndTurnIDSkip(t *testing.T) {
	w := newRollout(t, rollout.Options{})
	wrapped := "do it\n\nTeams helper safety:\n- You are running inside a Codex turn launched by the Teams helper.\n- more"
	mustTurn(t, w, rollout.Turn{Prompt: wrapped, Answer: "legacy teams answer"})
	id, err := w.AppendTurn(rollout.Turn{Prompt: "run by an old helper", Answer: "known turn"})
	if err != nil {
		t.Fatal(err)
	}
	mustTurn(t, w, rollout.Turn{Prompt: "quotes Teams helper safety: inline", Answer: "local"})
	skip := func(_, turn string) bool { return turn == id }
	if got := keys(readAll(t, w.Path, NewTracker("", 0, 0, skip))); got != "U:quotes Teams helper safety: inline | F:local" {
		t.Fatalf("outputs %q", got)
	}
}

func TestScanStartingMidTurnTakesNoSecondFinal(t *testing.T) {
	w := newRollout(t, rollout.Options{})
	mustTurn(t, w, rollout.Turn{Prompt: "q", Answer: "the answer"})
	// Find the end of the final_answer record: a converted position right
	// after the delivered final.
	data, _ := os.ReadFile(w.Path)
	var off int64
	for _, l := range strings.SplitAfter(string(data), "\n") {
		off += int64(len(l))
		if strings.Contains(l, `"response_item"`) && strings.Contains(l, "final_answer") {
			break
		}
	}
	tr := NewTracker(w.SessionID, off, 0, nil)
	if got := keys(readAll(t, w.Path, tr)); got != "" {
		t.Fatalf("mid-turn scan produced %q", got)
	}
	// Starting between the prompt and the final mirrors only the final.
	off = 0
	for _, l := range strings.SplitAfter(string(data), "\n") {
		off += int64(len(l))
		if strings.Contains(l, `"UserMessage"`) {
			break
		}
	}
	if got := keys(readAll(t, w.Path, NewTracker(w.SessionID, off, 0, nil))); got != "F:the answer" {
		t.Fatalf("scan after prompt produced %q", got)
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

// Walk lists every rollout, however old (no discovery window, owner
// decision 2026-11-05), oldest first, and nothing else.
func TestWalkListsEveryRolloutHoweverOld(t *testing.T) {
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
	ancient := mk(400 * 24 * time.Hour)
	old := mk(200 * 24 * time.Hour)
	recent := mk(time.Hour)
	if err := os.WriteFile(filepath.Join(filepath.Dir(recent), "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	found, err := Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range found {
		got = append(got, f.Path)
	}
	if want := ancient + "," + old + "," + recent; strings.Join(got, ",") != want {
		t.Fatalf("walk %v, want %s", got, want)
	}
	if fp := FirstPrompt(recent, 1<<20); fp != "p" {
		t.Fatalf("first prompt %q", fp)
	}
	if found, err := Walk(filepath.Join(root, "missing")); err != nil || len(found) != 0 {
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

// Review M8: Stat reports a file identity that changes when the rollout is
// replaced by rename (inode on Unix, file index on Windows), so a rewrite
// is detected even when the new file is not smaller.
func TestStatFileIDChangesWhenReplaced(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "r.jsonl")
	if err := os.WriteFile(p, []byte("a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	a, err := Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, "r.tmp")
	if err := os.WriteFile(tmp, []byte("a\nb\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, p); err != nil {
		t.Fatal(err)
	}
	b, err := Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if a.Inode == 0 || b.Inode == 0 || a.Inode == b.Inode {
		t.Fatalf("file ids %d, %d: want two different non-zero ids", a.Inode, b.Inode)
	}
	if !Rewritten(b, a.Size, a.Inode) {
		t.Fatal("replacement not detected as a rewrite")
	}
}

// Large records the mirror does not use are classified from their first
// bytes, never decoded: the truncated (invalid) JSON below would be
// "other" without a turn if it were decoded. The records it uses are still
// decoded whatever their size.
func TestParseLargeRecordsCheaply(t *testing.T) {
	ts := "2026-11-04T10:00:00.000Z"
	big := strings.Repeat("x", 64<<10)
	out := `{"timestamp":"` + ts + `","type":"event_msg","payload":{"type":"exec_command_end","call_id":"c1","turn_id":"t7","aggregated_output":"` + big
	ev := Parse([]byte(out))
	if ev.Kind != KindTurnRef || ev.TurnID != "t7" || ev.Time.IsZero() {
		t.Fatalf("large tool output: %+v", ev)
	}
	agent := `{"timestamp":"` + ts + `","type":"event_msg","payload":{"type":"item_completed","thread_id":"th","turn_id":"t7","item":{"type":"AgentMessage","text":"` + big
	if ev := Parse([]byte(agent)); ev.Kind != KindTurnRef || ev.ThreadID != "th" {
		t.Fatalf("large agent item: %+v", ev)
	}
	dev := `{"timestamp":"` + ts + `","type":"response_item","payload":{"type":"message","role":"developer","content":[{"type":"input_text","text":"` + big + `"}]}}`
	if ev := Parse([]byte(dev)); ev.Kind != KindOther {
		t.Fatalf("developer message: %+v", ev)
	}
	done := `{"timestamp":"` + ts + `","type":"event_msg","payload":{"type":"task_complete","turn_id":"t7","last_agent_message":"` + big + `"}}`
	if ev := Parse([]byte(done)); ev.Kind != KindTurnComplete || len(ev.Text) != len(big) {
		t.Fatalf("large final answer not decoded: kind %v, %d bytes", ev.Kind, len(ev.Text))
	}
	user := `{"timestamp":"` + ts + `","type":"event_msg","payload":{"type":"item_completed","turn_id":"t7","item":{"type":"UserMessage","content":"` + big + `","client_id":"cid"}}}`
	if ev := Parse([]byte(user)); ev.Kind != KindUserItem || ev.ClientID != "cid" {
		t.Fatalf("large user item not decoded: %+v", ev)
	}
}

func BenchmarkParseLargeToolOutput(b *testing.B) {
	line := []byte(`{"timestamp":"2026-11-04T10:00:00.000Z","type":"event_msg","payload":{"type":"exec_command_end","call_id":"c1","turn_id":"t7","aggregated_output":"` + strings.Repeat("x", 4<<20) + `"}}`)
	b.SetBytes(int64(len(line)))
	for i := 0; i < b.N; i++ {
		Parse(line)
	}
}

// B6: Read keeps only the head of a long record that is classified from
// its head (tool outputs, reasoning, developer messages), so a 2 MB tool
// output does not allocate about 5 MB; records that are decoded (finals,
// user items) are read whole, and every record classifies as before.
func TestReadCutsLongRecordsClassifiedFromTheirHead(t *testing.T) {
	ts := "2026-11-04T10:00:00.000Z"
	big := strings.Repeat("x", 2<<20)
	recs := []string{
		`{"timestamp":"` + ts + `","type":"response_item","payload":{"type":"function_call_output","call_id":"c","output":"` + big + `"}}`,
		`{"timestamp":"` + ts + `","type":"event_msg","payload":{"type":"exec_command_end","call_id":"c1","turn_id":"t7","aggregated_output":"` + big + `"}}`,
		`{"timestamp":"` + ts + `","type":"event_msg","payload":{"type":"item_completed","thread_id":"th","turn_id":"t7","item":{"type":"AgentMessage","text":"` + big + `"}}}`,
		`{"timestamp":"` + ts + `","type":"response_item","payload":{"type":"message","role":"developer","content":[{"type":"input_text","text":"` + big + `"}]}}`,
		`{"timestamp":"` + ts + `","type":"event_msg","payload":{"type":"task_complete","turn_id":"t7","last_agent_message":"` + big + `"}}`,
		`{"timestamp":"` + ts + `","type":"event_msg","payload":{"type":"item_completed","turn_id":"t7","item":{"type":"UserMessage","content":"` + big + `","client_id":"cid"}}}`,
		`{"timestamp":"` + ts + `","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"short"}]}}`,
	}
	cut := []bool{true, true, true, true, false, false, false}
	p := writeLines(t, strings.Join(recs, "\n")+"\n")
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	lines, _, err := Read(f, 0, 0)
	if err != nil || len(lines) != len(recs) {
		t.Fatalf("%d lines, %v", len(lines), err)
	}
	for i, l := range lines {
		if l.Cut != cut[i] {
			t.Errorf("record %d: cut %v, want %v", i, l.Cut, cut[i])
		}
		got, want := ParseLine(l), Parse([]byte(recs[i]))
		if got.Kind != want.Kind || got.TurnID != want.TurnID || got.ThreadID != want.ThreadID || !got.Time.Equal(want.Time) ||
			got.ClientID != want.ClientID || got.Text != want.Text {
			t.Errorf("record %d classified %v %q, want %v %q", i, got.Kind, got.TurnID, want.Kind, want.TurnID)
		}
	}

	// Allocation: 8 skipped tool outputs of 2 MB each.
	var b strings.Builder
	for i := 0; i < 8; i++ {
		b.WriteString(recs[i%2])
		b.WriteByte('\n')
	}
	p2 := writeLines(t, b.String())
	f2, err := os.Open(p2)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	lines, _, err = Read(f2, 0, 0)
	runtime.ReadMemStats(&after)
	if err != nil || len(lines) != 8 {
		t.Fatalf("%d lines, %v", len(lines), err)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 2<<20 {
		t.Fatalf("reading 16 MB of skipped records allocated %d KB, want under 2 MB", alloc>>10)
	}
}

// Fidelity run, s523/s526: a resume position inside a turn of an older
// rollout, whose records name the turn only on task_complete. The final
// read before the turn is known is held, not dropped, and still skipped
// when the helper ran the turn.
func TestScanMidTurnHoldsFinalUntilTurnKnown(t *testing.T) {
	const ts = `{"timestamp":"2026-09-01T00:00:00Z",`
	lines := []string{
		ts + `"type":"response_item","payload":{"type":"reasoning","summary":[]}}` + "\n",
		ts + `"type":"event_msg","payload":{"type":"agent_message","message":"done here","phase":"final_answer"}}` + "\n",
		ts + `"type":"response_item","payload":{"type":"message","id":"msg_f","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"done here"}]}}` + "\n",
		ts + `"type":"event_msg","payload":{"type":"token_count"}}` + "\n",
		ts + `"type":"event_msg","payload":{"type":"task_complete","turn_id":"tA","last_agent_message":"done here"}}` + "\n",
	}
	p := writeLines(t, lines...)
	read := func(skip Skip) ([]Output, *Tracker) {
		tr := NewTracker("th", 0, 10, skip)
		return readAll(t, p, tr), tr
	}
	out, _ := read(nil)
	if got := keys(out); got != "F:done here" {
		t.Fatalf("outputs %q", got)
	}
	if out[0].RecordID != "msg_f" || out[0].TurnID != "tA" {
		t.Fatalf("output %+v", out[0])
	}
	if out, _ := read(func(_, turn string) bool { return turn == "tA" }); len(out) != 0 {
		t.Fatalf("helper turn mirrored: %q", keys(out))
	}
	// Held until task_complete: a restart re-reads from the final.
	held := writeLines(t, lines[:4]...)
	tr := NewTracker("th", 0, 10, nil)
	if out := readAll(t, held, tr); len(out) != 0 {
		t.Fatalf("released before the turn was known: %q", keys(out))
	}
	if off, _ := tr.Resume(); off != int64(len(lines[0])+len(lines[1])) {
		t.Fatalf("resume %d", off)
	}
	// The next turn's start ends the unnamed turn; it is not taken for it.
	next := writeLines(t, append(append([]string{}, lines[:4]...),
		ts+`"type":"turn_context","payload":{"turn_id":"tB"}}`+"\n",
		ts+`"type":"response_item","payload":{"type":"message","id":"msg_u","role":"user","content":[{"type":"input_text","text":"next question"}]}}`+"\n",
		ts+`"type":"response_item","payload":{"type":"message","id":"msg_g","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"next answer"}]}}`+"\n")...)
	if got := keys(readAll(t, next, NewTracker("th", 0, 10, nil))); got != "F:done here | U:next question | F:next answer" {
		t.Fatalf("outputs %q", got)
	}
}
