package teams

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHistoryTieredStatDetectsOnlyChangedFiles(t *testing.T) {
	dir := t.TempDir()
	states := make(map[string]historyTieredFileState)
	var paths []string
	for i := 0; i < 4; i++ {
		path := filepath.Join(dir, fmt.Sprintf("session-%d.jsonl", i))
		if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat fixture: %v", err)
		}
		paths = append(paths, path)
		states[path] = historyTieredFileState{Path: path, Size: info.Size(), ModTime: info.ModTime()}
	}
	if err := os.WriteFile(paths[2], []byte("{}\n{}\n"), 0o600); err != nil {
		t.Fatalf("modify fixture: %v", err)
	}

	changes, err := historyTieredDetectStatChanges(paths, states)
	if err != nil {
		t.Fatalf("historyTieredDetectStatChanges: %v", err)
	}
	if len(changes) != 1 || changes[0].Path != paths[2] {
		t.Fatalf("changes = %#v, want only %s", changes, paths[2])
	}
}

func TestHistoryTieredListSessionFilesInDirs(t *testing.T) {
	dir := t.TempDir()
	day1 := filepath.Join(dir, "2026", "05", "11")
	day2 := filepath.Join(dir, "2026", "05", "10")
	if err := os.MkdirAll(filepath.Join(day1, "nested"), 0o700); err != nil {
		t.Fatalf("mkdir day1: %v", err)
	}
	if err := os.MkdirAll(day2, 0o700); err != nil {
		t.Fatalf("mkdir day2: %v", err)
	}
	writeSmallFile(t, filepath.Join(day1, "b.jsonl"))
	writeSmallFile(t, filepath.Join(day1, "ignored.txt"))
	writeSmallFile(t, filepath.Join(day2, "a.jsonl"))

	files, err := historyTieredListSessionFilesInDirs([]string{day1, filepath.Join(dir, "missing"), day2})
	if err != nil {
		t.Fatalf("historyTieredListSessionFilesInDirs: %v", err)
	}
	want := []string{filepath.Join(day2, "a.jsonl"), filepath.Join(day1, "b.jsonl")}
	if strings.Join(files, "\n") != strings.Join(want, "\n") {
		t.Fatalf("files = %#v, want %#v", files, want)
	}
}

func TestHistoryTieredScanTailWaitsForCompletedTurn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"turn.started","turn_id":"turn-1"}`,
		`{"type":"item.completed","turn_id":"turn-1","item":{"id":"assistant-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"answer pending terminal"}]}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if len(result.Finals) != 0 {
		t.Fatalf("finals before turn.completed = %#v, want none", result.Finals)
	}

	appendLine(t, path, `{"type":"turn.completed","turn_id":"turn-1"}`)
	result, err = historyTieredScanTail(path, result.State, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail after completion: %v", err)
	}
	if len(result.Finals) != 1 {
		t.Fatalf("finals after turn.completed = %#v, want one", result.Finals)
	}
	if got := result.Finals[0].Key; !strings.Contains(got, "thread-1:turn-1:assistant-1") {
		t.Fatalf("completion key = %q, want thread/turn/item key", got)
	}

	repeat, err := historyTieredScanTail(path, result.State, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail repeat: %v", err)
	}
	if len(repeat.Finals) != 0 {
		t.Fatalf("repeat finals = %#v, want none", repeat.Finals)
	}
}

func TestHistoryTieredScanTailMethodTurnCompletedWithItems(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"method":"turn/completed","params":{"turnId":"turn-1","turn":{"items":[{"id":"user-1","type":"message","role":"user","content":[{"type":"input_text","text":"question"}]},{"id":"assistant-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"answer in completed turn"}]}]}}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if len(result.Finals) != 1 {
		t.Fatalf("finals = %#v, want one method turn/completed final", result.Finals)
	}
	if result.Finals[0].Record.Text != "answer in completed turn" {
		t.Fatalf("final text = %q", result.Finals[0].Record.Text)
	}
	if result.Finals[0].TerminalKind != "turn/completed" {
		t.Fatalf("terminal kind = %q, want turn/completed", result.Finals[0].TerminalKind)
	}
}

func TestHistoryTieredScanTailMultipleFinalAnswersInOneTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"event_msg","payload":{"id":"final-1","type":"agent_message","phase":"final_answer","message":"first final"}}`,
		`{"type":"event_msg","payload":{"id":"final-2","type":"agent_message","phase":"final_answer","message":"second final"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if len(result.Finals) != 2 {
		t.Fatalf("finals = %#v, want two final_answer completions", result.Finals)
	}
	if result.Finals[0].Record.Text != "first final" || result.Finals[1].Record.Text != "second final" {
		t.Fatalf("final texts = %#v", []string{result.Finals[0].Record.Text, result.Finals[1].Record.Text})
	}
}

func TestHistoryTieredScanTailUsesLastAssistantBeforeTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"turn.started","turn_id":"turn-1"}`,
		`{"type":"item.completed","turn_id":"turn-1","item":{"id":"assistant-draft","type":"message","role":"assistant","content":[{"type":"output_text","text":"draft"}]}}`,
		`{"type":"item.completed","turn_id":"turn-1","item":{"id":"assistant-final","type":"message","role":"assistant","content":[{"type":"output_text","text":"final"}]}}`,
		`{"type":"turn.completed","turn_id":"turn-1"}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if len(result.Finals) != 1 {
		t.Fatalf("finals = %#v, want one final", result.Finals)
	}
	if result.Finals[0].Record.Text != "final" || !strings.Contains(result.Finals[0].Key, "assistant-final") {
		t.Fatalf("final = %#v, want last assistant", result.Finals[0])
	}
}

func TestHistoryTieredScanTailDoesNotCompleteDifferentTurn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"turn.started","turn_id":"turn-1"}`,
		`{"type":"item.completed","turn_id":"turn-1","item":{"id":"assistant-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"answer for turn 1"}]}}`,
		`{"type":"turn.completed","turn_id":"turn-2"}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if len(result.Finals) != 0 {
		t.Fatalf("finals = %#v, want none for mismatched terminal turn", result.Finals)
	}
}

func TestHistoryTieredScanTailTreatsFinalAnswerPhaseAsComplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"event_msg","payload":{"id":"final-1","type":"agent_message","phase":"final_answer","message":"final answer from tui"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if len(result.Finals) != 1 {
		t.Fatalf("finals = %#v, want one final_answer completion", result.Finals)
	}
	if result.Finals[0].Record.Text != "final answer from tui" {
		t.Fatalf("final text = %q", result.Finals[0].Record.Text)
	}
}

func TestHistoryTieredScanTailFallbackIDsMatchFullParserWithSessionID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"role":"assistant","text":"fallback id answer"}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	full, err := ReadSessionTranscript(path)
	if err != nil {
		t.Fatalf("ReadSessionTranscript: %v", err)
	}
	if len(full.Records) != 1 {
		t.Fatalf("full records = %#v, want one", full.Records)
	}
	offset := int64(len(lines[0]) + 1)
	tail, err := historyTieredScanTail(path, historyTieredFileState{
		Path:      path,
		Offset:    offset,
		Line:      1,
		SessionID: "thread-1",
		ThreadID:  "thread-1",
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if len(tail.Records) != 1 {
		t.Fatalf("tail records = %#v, want one", tail.Records)
	}
	if tail.Records[0].ItemID != full.Records[0].ItemID || tail.Records[0].DedupeKey != full.Records[0].DedupeKey {
		t.Fatalf("tail fallback IDs = %q/%q, full = %q/%q", tail.Records[0].ItemID, tail.Records[0].DedupeKey, full.Records[0].ItemID, full.Records[0].DedupeKey)
	}
}

func TestHistoryTieredScanTailDoesNotTreatFinalAnswerTextAsPhase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"response_item","payload":{"id":"assistant-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"the literal word final_answer is not a completion signal"}]}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if len(result.Finals) != 0 {
		t.Fatalf("finals = %#v, want none without final_answer phase or turn.completed", result.Finals)
	}
}

func TestHistoryTieredScanTailDoesNotConsumeIncompleteTrailingLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	complete := `{"type":"session_meta","payload":{"id":"thread-1"}}` + "\n"
	partial := `{"type":"event_msg","payload":{"id":"final-1","type":"agent_message","phase":"final_answer","message":"partial`
	if err := os.WriteFile(path, []byte(complete+partial), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.Incomplete {
		t.Fatalf("incomplete = false, want true")
	}
	if len(result.Finals) != 0 {
		t.Fatalf("finals = %#v, want none while trailing line is incomplete", result.Finals)
	}
	offsetBefore := result.State.Offset

	appendLine(t, path, ` done"}}`)
	result, err = historyTieredScanTail(path, result.State, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail after completion: %v", err)
	}
	if result.State.Offset <= offsetBefore {
		t.Fatalf("offset = %d, want to advance past previous offset %d", result.State.Offset, offsetBefore)
	}
	if len(result.Finals) != 1 {
		t.Fatalf("finals after completing trailing line = %#v, want one", result.Finals)
	}
}

func TestHistoryTieredScanTailCapsLargeTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, []byte(strings.Repeat("{}\n", 128)), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 32)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.TooLarge || result.BytesRead != 0 {
		t.Fatalf("large tail result = %#v, want capped without read", result)
	}
	if result.State.Size != 0 || result.State.Offset != 0 {
		t.Fatalf("large tail state = %#v, want previous state preserved", result.State)
	}
}

func TestHistoryTieredScanTailCanRecoverAfterLargeTailCap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"event_msg","payload":{"id":"final-1","type":"agent_message","phase":"final_answer","message":"large tail final"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	capped, err := historyTieredScanTail(path, historyTieredFileState{}, 16)
	if err != nil {
		t.Fatalf("historyTieredScanTail capped: %v", err)
	}
	if !capped.TooLarge || len(capped.Finals) != 0 {
		t.Fatalf("capped result = %#v, want too-large without finals", capped)
	}

	result, err := historyTieredScanTail(path, capped.State, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail recovered: %v", err)
	}
	if len(result.Finals) != 1 {
		t.Fatalf("recovered finals = %#v, want one", result.Finals)
	}
}

func TestHistoryTieredScanTailTruncateDoesNotSkipRewrittenContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	oldLines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"event_msg","payload":{"id":"old-final","type":"agent_message","phase":"final_answer","message":"old final with much longer content to make the file longer"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(oldLines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write old fixture: %v", err)
	}
	initial, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail initial: %v", err)
	}
	if len(initial.Finals) != 1 {
		t.Fatalf("initial finals = %#v, want one", initial.Finals)
	}

	newLines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"event_msg","payload":{"id":"new-final","type":"agent_message","phase":"final_answer","message":"new"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(newLines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write rewritten fixture: %v", err)
	}
	truncated, err := historyTieredScanTail(path, initial.State, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail truncated: %v", err)
	}
	if !truncated.Truncated {
		t.Fatalf("truncated = false, want true")
	}
	if truncated.State.Offset != 0 || truncated.State.Size != 0 {
		t.Fatalf("truncated state = %#v, want reset state", truncated.State)
	}

	recovered, err := historyTieredScanTail(path, truncated.State, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail recovered: %v", err)
	}
	if len(recovered.Finals) != 1 || recovered.Finals[0].Record.Text != "new" {
		t.Fatalf("recovered finals = %#v, want rewritten final", recovered.Finals)
	}
}

func BenchmarkHistoryTieredStatHotSetUnchanged(b *testing.B) {
	dir := b.TempDir()
	const files = 5000
	paths := make([]string, 0, files)
	states := make(map[string]historyTieredFileState, files)
	for i := 0; i < files; i++ {
		path := filepath.Join(dir, fmt.Sprintf("session-%05d.jsonl", i))
		if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
			b.Fatalf("write fixture: %v", err)
		}
		info, err := os.Stat(path)
		if err != nil {
			b.Fatalf("stat fixture: %v", err)
		}
		paths = append(paths, path)
		states[path] = historyTieredFileState{Path: path, Size: info.Size(), ModTime: info.ModTime()}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		changes, err := historyTieredDetectStatChanges(paths, states)
		if err != nil {
			b.Fatalf("historyTieredDetectStatChanges: %v", err)
		}
		if len(changes) != 0 {
			b.Fatalf("changes = %d, want none", len(changes))
		}
	}
}

func BenchmarkHistoryTieredRecentDirRead(b *testing.B) {
	root := b.TempDir()
	const dirs = 4
	const filesPerDir = 250
	var scanDirs []string
	for d := 0; d < dirs; d++ {
		dir := filepath.Join(root, fmt.Sprintf("day-%02d", d))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			b.Fatalf("mkdir fixture: %v", err)
		}
		scanDirs = append(scanDirs, dir)
		for i := 0; i < filesPerDir; i++ {
			writeSmallFile(b, filepath.Join(dir, fmt.Sprintf("session-%05d.jsonl", i)))
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		files, err := historyTieredListSessionFilesInDirs(scanDirs)
		if err != nil {
			b.Fatalf("historyTieredListSessionFilesInDirs: %v", err)
		}
		if len(files) != dirs*filesPerDir {
			b.Fatalf("files = %d, want %d", len(files), dirs*filesPerDir)
		}
	}
}

func BenchmarkHistoryTieredTailScanSmallDelta(b *testing.B) {
	path := filepath.Join(b.TempDir(), "session.jsonl")
	base := make([]string, 0, 4002)
	base = append(base, `{"type":"session_meta","payload":{"id":"bench-thread"}}`)
	for i := 0; i < 4000; i++ {
		base = append(base,
			fmt.Sprintf(`{"type":"event_msg","payload":{"id":"status-%05d","type":"agent_message","phase":"commentary","message":"working %05d"}}`, i, i),
		)
	}
	if err := os.WriteFile(path, []byte(strings.Join(base, "\n")+"\n"), 0o600); err != nil {
		b.Fatalf("write base: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		b.Fatalf("stat base: %v", err)
	}
	state := historyTieredFileState{Path: path, Size: info.Size(), ModTime: info.ModTime(), Offset: info.Size(), Line: len(base), ThreadID: "bench-thread"}
	appendLine(b, path, `{"type":"event_msg","payload":{"id":"final-1","type":"agent_message","phase":"final_answer","message":"done"}}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := historyTieredScanTail(path, state, 1<<20)
		if err != nil {
			b.Fatalf("historyTieredScanTail: %v", err)
		}
		if len(result.Finals) != 1 {
			b.Fatalf("finals = %d, want one", len(result.Finals))
		}
	}
}

func writeSmallFile(tb testing.TB, path string) {
	tb.Helper()
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		tb.Fatalf("write file: %v", err)
	}
}

func appendLine(tb testing.TB, path string, line string) {
	tb.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		tb.Fatalf("open append: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteString(line + "\n"); err != nil {
		tb.Fatalf("append line: %v", err)
	}
}
