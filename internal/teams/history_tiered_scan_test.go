package teams

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestHistoryTieredFrontierSelectsEarliestAndAcceptsZeroOffset(t *testing.T) {
	state := historyTieredFileState{
		Path:                         "/tmp/session.jsonl",
		SourceGeneration:             "generation-1",
		Offset:                       900,
		UnresolvedContinuation:       true,
		UnresolvedContinuationOffset: 700,
		TranscriptQuarantine: &teamstore.TranscriptQuarantine{
			Kind:               "mixed_id_final_mirror",
			SourcePath:         "/tmp/session.jsonl",
			SourceGeneration:   "generation-1",
			FrontierRecordID:   "ignored:mirror",
			FrontierLine:       4,
			FrontierOffset:     400,
			ExclusiveEndOffset: 500,
			RangeFingerprint:   "sha256:range",
		},
	}
	frontier := historyTieredFrontierForState(state)
	if !frontier.Present || !frontier.Usable || !frontier.Crossed || frontier.Offset != 400 || frontier.Kind != "mixed_id_final_mirror" {
		t.Fatalf("earliest frontier = %#v, want crossed quarantine at offset 400", frontier)
	}
	state.TranscriptQuarantine = nil
	state.PendingRootTaskStarted = true
	state.PendingRootTaskStartedOffset = 0
	state.PendingRootTaskStartedRecordID = "ignored:pending-root"
	state.PendingRootTaskStartedThreadID = "thread-1"
	state.PendingRootTaskStartedTurnID = "turn-1"
	frontier = historyTieredFrontierForState(state)
	if !frontier.Present || !frontier.Usable || frontier.Offset != 0 || frontier.Kind != "pending_root_task_started" || frontier.OwnerTurnID != "turn-1" {
		t.Fatalf("zero-offset frontier = %#v, want present pending-root frontier at zero", frontier)
	}
	if got := historyTieredBoundaryOffset(state); got != 0 {
		t.Fatalf("zero-offset boundary = %d, want 0 while remaining present", got)
	}
	if got := filterTranscriptRecordsBeforeHistoryTieredFrontier([]TranscriptRecord{{SourceStartOffset: 0, Text: "must stay hidden"}}, state); len(got) != 0 {
		t.Fatalf("zero-offset frontier exposed %d records", len(got))
	}
}

func TestHistoryTieredMetadataOnlyQuarantineCannotAdvanceFrontier(t *testing.T) {
	state := historyTieredFileState{
		Path:             "/tmp/session.jsonl",
		SourceGeneration: "generation-1",
		Offset:           512,
		TranscriptQuarantine: &teamstore.TranscriptQuarantine{
			Kind:                "mixed_id_final_mirror",
			SourcePath:          "/tmp/session.jsonl",
			SourceGeneration:    "generation-1",
			CandidateTextHashes: []string{"sha256:candidate"},
		},
	}
	frontier := historyTieredFrontierForState(state)
	if frontier.Present {
		t.Fatalf("metadata-only quarantine = %#v, want no cursor frontier", frontier)
	}
}

func TestHistoryTieredScanCapturesReadRangeProofForSameSizeTailRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := []byte(`{"type":"prefix"}` + "\n")
	tail := []byte(`{"a":1}` + "\n")
	if err := os.WriteFile(path, append(prefix, tail...), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{Path: path, Offset: int64(len(prefix)), Size: int64(len(prefix)), ModTime: info.ModTime()}, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("scan transcript: %v", err)
	}
	if !result.ReadProofRangeKnown || result.ReadProofStartOffset != int64(len(prefix)) || result.ReadProofEndOffset != info.Size() {
		t.Skip("platform does not expose a stable source identity for range proofs")
	}
	if err := os.WriteFile(path, append(prefix, []byte(`{"b":1}`+"\n")...), 0o600); err != nil {
		t.Fatalf("rewrite transcript tail: %v", err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("restore transcript mtime: %v", err)
	}
	proof := outboxQueueOptions{
		ExpectedSourcePath:            path,
		ExpectedSourceReadFingerprint: result.ReadProofFingerprint,
		ExpectedSourceReadStartOffset: result.ReadProofStartOffset,
		ExpectedSourceReadEndOffset:   result.ReadProofEndOffset,
		ExpectedSourceReadRangeKnown:  result.ReadProofRangeKnown,
	}
	if transcriptSourceReadProofMatches(proof) {
		t.Fatal("same-size in-place tail rewrite passed the scan read-range proof")
	}
}

func TestTranscriptSourceReadProofRejectsOversizedPersistedRange(t *testing.T) {
	proof := outboxQueueOptions{
		ExpectedSourcePath:            filepath.Join(t.TempDir(), "transcript.jsonl"),
		ExpectedSourceReadFingerprint: "persisted-proof",
		ExpectedSourceReadStartOffset: 0,
		ExpectedSourceReadEndOffset:   historyTieredMaxReadProofBytes + 1,
		ExpectedSourceReadRangeKnown:  true,
	}
	if transcriptSourceReadProofMatches(proof) {
		t.Fatalf("oversized persisted read proof was accepted: %#v", proof)
	}
}

func TestTranscriptSourceReadProofAcceptsCompleteOversizedRecordRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transcript.jsonl")
	body := []byte(strings.Repeat("x", int(historyTieredMaxRecordBytes)+1024) + "\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write oversized proof fixture: %v", err)
	}
	fingerprint := transcriptSourceRangeFingerprint(path, 0, int64(len(body)))
	if fingerprint == "" {
		t.Skip("platform does not expose a stable source identity for range proofs")
	}
	proof := outboxQueueOptions{
		ExpectedSourcePath:            path,
		ExpectedSourceReadFingerprint: fingerprint,
		ExpectedSourceReadStartOffset: 0,
		ExpectedSourceReadEndOffset:   int64(len(body)),
		ExpectedSourceReadRangeKnown:  true,
	}
	if !transcriptSourceReadProofMatches(proof) {
		t.Fatalf("complete oversized record proof was rejected: range=%d limit=%d", len(body), historyTieredMaxReadProofBytes)
	}
}

func TestHistoryTieredScanPersistsTerminalBoundaryWithoutVisibleFinal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "terminal-only.jsonl")
	body := []byte(`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-terminal-only"}}` + "\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write terminal-only transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if len(result.Finals) != 0 {
		t.Fatalf("terminal-only scan produced visible finals: %#v", result.Finals)
	}
	boundary := result.State.TerminalBoundary
	if boundary == nil || boundary.RecordID == "" || boundary.StartOffset != 0 || boundary.ExclusiveEndOffset != int64(len(body)) || boundary.RangeFingerprint == "" {
		t.Fatalf("terminal-only boundary = %#v, want exact source proof", boundary)
	}
	transcript := withHistoryTieredReadProof(Transcript{}, result)
	if transcript.TerminalBoundary == nil || transcript.TerminalBoundary.RecordID != boundary.RecordID {
		t.Fatalf("terminal-only boundary was not propagated to Transcript: %#v", transcript.TerminalBoundary)
	}
}

func TestHistoryTieredOpaqueGapRemainsReachableAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "opaque-gap-restart.jsonl")
	body := []byte(`{"type":"session_meta","payload":{"id":"opaque-gap-restart"}}` + "\n" +
		`{"broken":` + "\n" +
		`{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"after opaque gap"}}` + "\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write opaque-gap transcript: %v", err)
	}
	first, err := historyTieredScanTail(path, historyTieredFileState{}, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if first.State.ContextGap == nil || first.State.Offset <= 0 || first.State.TranscriptQuarantine == nil {
		t.Fatalf("first scan = %#v, want committed opaque gap", first.State)
	}
	second, err := historyTieredScanTail(path, first.State, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("post-restart scan: %v", err)
	}
	if len(second.Finals) != 1 || second.Finals[0].Record.Text != "after opaque gap" {
		t.Fatalf("post-restart finals = %#v, want the suffix final", second.Finals)
	}
	third, err := historyTieredScanTail(path, second.State, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("repeat post-restart scan: %v", err)
	}
	if len(third.Finals) != 0 {
		t.Fatalf("repeat post-restart finals = %#v, want no duplicate", third.Finals)
	}
}

func TestTranscriptAutomaticImportProofLargeColdSourceUsesZeroOffsetFingerprint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large-cold.jsonl")
	body := []byte(`{"type":"session_meta","payload":{"id":"large-cold"}}` + "\n" +
		`{"type":"item.completed","item":{"type":"message","role":"assistant","metadata":"` +
		strings.Repeat("x", int(historyTieredMaxTailBytes)+1024) + `"}}` + "\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write large cold source: %v", err)
	}
	proof := transcriptAutomaticImportProofQueueOptions(path, teamstore.ImportCheckpoint{}, int64(len(body)))
	if proof.ExpectedSourceOffset != 0 || !proof.ExpectedSourceOffsetKnown {
		t.Fatalf("large cold proof offset = %d known=%v, want zero boundary", proof.ExpectedSourceOffset, proof.ExpectedSourceOffsetKnown)
	}
	want := transcriptCheckpointSourceFingerprint(path, 0)
	if proof.ExpectedSourceFingerprint != want || want == "" {
		t.Fatalf("large cold proof fingerprint = %q, want zero-prefix fingerprint %q", proof.ExpectedSourceFingerprint, want)
	}
	if !transcriptAutomaticImportSourceProofMatches(proof) {
		t.Fatalf("large cold proof did not validate against its zero-prefix boundary: %#v", proof)
	}
}

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

func TestHistoryTieredStatReconcileDetectsSameSizeSameMtimeRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	original := []byte("abcdef\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat original: %v", err)
	}
	state := historyTieredFileState{
		Path:              path,
		Size:              info.Size(),
		ModTime:           info.ModTime(),
		Offset:            info.Size(),
		SourceFingerprint: transcriptCheckpointSourceFingerprint(path, info.Size()),
	}
	if state.SourceFingerprint == "" {
		t.Fatal("missing original source fingerprint")
	}
	if err := os.WriteFile(path, []byte("uvwxyz\n"), 0o600); err != nil {
		t.Fatalf("rewrite same-size file: %v", err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("restore mtime: %v", err)
	}
	changes, err := historyTieredDetectStatChanges([]string{path}, map[string]historyTieredFileState{path: state}, true)
	if err != nil {
		t.Fatalf("historyTieredDetectStatChanges: %v", err)
	}
	if len(changes) != 1 || changes[0].Path != path {
		t.Fatalf("changes = %#v, want rewritten path", changes)
	}
}

func TestHistoryTieredStatFastModeDetectsSameSizeRewriteUsingChangeTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	original := []byte("abcdef\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat original: %v", err)
	}
	changeTime := teamstore.SourceFileChangeTimeFromFileInfo(info)
	if changeTime == 0 {
		t.Skip("platform does not expose a usable file change time")
	}
	state := historyTieredFileState{
		Path: path, Size: info.Size(), ModTime: info.ModTime(),
		SourceChangeTime: changeTime,
	}
	if err := os.WriteFile(path, []byte("uvwxyz\n"), 0o600); err != nil {
		t.Fatalf("rewrite same-size file: %v", err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("restore mtime: %v", err)
	}
	newInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat rewritten file: %v", err)
	}
	if got := teamstore.SourceFileChangeTimeFromFileInfo(newInfo); got == changeTime {
		t.Skip("filesystem did not expose a changed ctime for the rewrite")
	}
	changes, err := historyTieredDetectStatChanges([]string{path}, map[string]historyTieredFileState{path: state})
	if err != nil {
		t.Fatalf("historyTieredDetectStatChanges: %v", err)
	}
	if len(changes) != 1 || changes[0].Path != path {
		t.Fatalf("fast-mode changes = %#v, want rewritten path", changes)
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
	if result.State.UnresolvedContinuation || result.State.TranscriptQuarantine == nil || len(result.Finals) != 0 {
		t.Fatalf("anonymous final pair = state=%#v finals=%#v, want transcript quarantine with no publishable finals", result.State, result.Finals)
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

func TestHistoryTieredScanTailDropsPrefixFinalAnswerFragmentWhenFullResponseFollows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	partial := "final answer prefix from streamed event_msg before the response_item completes `"
	full := partial + "<oai-mem-citation> literal tag explanation continues with the rest of the real answer"
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"turn.started","turn_id":"turn-1"}`,
		`{"type":"event_msg","payload":{"id":"final-fragment","type":"agent_message","turn_id":"turn-1","phase":"final_answer","message":` + strconv.Quote(partial) + `}}`,
		`{"type":"response_item","payload":{"type":"message","role":"assistant","turn_id":"turn-1","phase":"final_answer","content":[{"type":"output_text","text":` + strconv.Quote(full) + `}]}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1","last_agent_message":` + strconv.Quote(partial) + `}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	var assistantRecords []TranscriptRecord
	for _, record := range result.Records {
		if record.Kind == TranscriptKindAssistant {
			assistantRecords = append(assistantRecords, record)
		}
	}
	if len(assistantRecords) != 1 || assistantRecords[0].Text != full {
		t.Fatalf("assistant records = %#v, want only full response item", assistantRecords)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != full {
		t.Fatalf("finals = %#v, want only full response item final", result.Finals)
	}
}

func TestHistoryTieredScanTailDropsPrefixFinalAnswerFragmentAfterIntermediateAssistant(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	partial := "final answer prefix from streamed event_msg before a later response_item `"
	intermediate := "different assistant record in the same turn that is not a prefix and should stay"
	full := partial + "<oai-mem-citation> literal tag explanation continues with the rest of the real answer"
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"turn.started","turn_id":"turn-1"}`,
		`{"type":"event_msg","payload":{"id":"final-fragment","type":"agent_message","turn_id":"turn-1","phase":"final_answer","message":` + strconv.Quote(partial) + `}}`,
		`{"type":"event_msg","payload":{"id":"other-final","type":"agent_message","turn_id":"turn-1","phase":"final_answer","message":` + strconv.Quote(intermediate) + `}}`,
		`{"type":"response_item","payload":{"type":"message","role":"assistant","turn_id":"turn-1","phase":"final_answer","content":[{"type":"output_text","text":` + strconv.Quote(full) + `}]}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	var gotRecords []string
	for _, record := range result.Records {
		if record.Kind == TranscriptKindAssistant {
			gotRecords = append(gotRecords, record.Text)
		}
	}
	wantRecords := []string{intermediate, full}
	if !reflect.DeepEqual(gotRecords, wantRecords) {
		t.Fatalf("assistant records = %#v, want %#v", gotRecords, wantRecords)
	}
	var gotFinals []string
	for _, final := range result.Finals {
		gotFinals = append(gotFinals, final.Record.Text)
	}
	if !reflect.DeepEqual(gotFinals, wantRecords) {
		t.Fatalf("finals = %#v, want %#v", gotFinals, wantRecords)
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
	if !result.TooLarge || !result.BudgetExhausted || result.BytesRead <= 0 {
		t.Fatalf("large tail result = %#v, want a bounded resumable prefix", result)
	}
	if result.State.Size <= 0 || result.State.Offset <= 0 || result.State.Offset >= 128*3 {
		t.Fatalf("large tail state = %#v, want a cursor inside the source", result.State)
	}
}

func TestHistoryTieredScanTailDoesNotAcceptJSONPrefixAtBudgetBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "budget-malformed-record.jsonl")
	line := `{"a":1}not-json` + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatalf("write malformed budget fixture: %v", err)
	}
	// The first seven bytes are a valid JSON object, but they are only a
	// prefix of the physical JSONL record. A bounded reader must drain to the
	// newline and quarantine the whole malformed range rather than committing
	// a cursor in the middle of a record.
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 7)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.Offset != int64(len(line)) || result.State.Offset <= 7 {
		t.Fatalf("state = %#v, want full malformed line consumed past budget boundary", result.State)
	}
	if len(result.Records) != 0 || result.State.TranscriptQuarantine == nil || result.State.TranscriptQuarantine.Kind != "malformed_record" {
		t.Fatalf("result = %#v, want one row-local malformed quarantine without visible records", result)
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

func TestHistoryTieredScanTailAdvancesPastOversizedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	line := `{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"` + strings.Repeat("x", historyTieredMaxRecordBytes+1) + `"}}` + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatalf("write oversized record: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.TooLarge || !result.OversizedRecord || !result.BudgetExhausted || result.Incomplete {
		t.Fatalf("oversized record result = %#v, want a bounded opaque disposition", result)
	}
	if result.State.Offset != int64(len(line)) || result.State.Line != 1 || len(result.Records) != 1 {
		t.Fatalf("oversized record state = %#v, want one complete disposition at EOF", result)
	}
	if result.Records[0].Kind != TranscriptKindUnknown || result.Records[0].SourceType != "oversized_record" {
		t.Fatalf("oversized record disposition = %#v", result.Records[0])
	}
	if result.Records[0].Text != "" {
		t.Fatalf("oversized record guessed visible text from bounded prefix: %#v", result.Records[0])
	}
}

func TestHistoryTieredScanTailPersistsPartialReadHintAndResumesAfterNewline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := `{"type":"session_meta","payload":{"id":"thread-partial"}}` + "\n"
	partial := `{"type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"still writing`
	if err := os.WriteFile(path, []byte(prefix+partial), 0o600); err != nil {
		t.Fatalf("write partial fixture: %v", err)
	}
	first, err := historyTieredScanTail(path, historyTieredFileState{}, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("first partial scan: %v", err)
	}
	if !first.Incomplete || first.State.Offset != int64(len(prefix)) || first.State.PartialReadOffset <= first.State.PartialLineStartOffset {
		t.Fatalf("first partial result = %#v", first)
	}
	unchanged, err := historyTieredScanTail(path, first.State, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("unchanged partial scan: %v", err)
	}
	if !unchanged.Incomplete || unchanged.State.PartialReadOffset != first.State.PartialReadOffset {
		t.Fatalf("unchanged partial result = %#v", unchanged)
	}
	appendLine(t, path, ` done"}}`)
	resumed, err := historyTieredScanTail(path, unchanged.State, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("resumed partial scan: %v", err)
	}
	if resumed.Incomplete || resumed.State.Offset <= first.State.Offset || resumed.State.PartialLineStartOffset != 0 {
		t.Fatalf("resumed partial result = %#v", resumed)
	}
}

func TestHistoryTieredScanTailReplaysSameSizePartialRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial-rewrite.jsonl")
	prefix := `{"type":"session_meta","payload":{"id":"partial-rewrite"}}` + "\n"
	partialA := `{"type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"` + strings.Repeat("a", 64) + `"}`
	if err := os.WriteFile(path, []byte(prefix+partialA), 0o600); err != nil {
		t.Fatalf("write partial rewrite fixture: %v", err)
	}
	first, err := historyTieredScanTail(path, historyTieredFileState{}, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("initial partial rewrite scan: %v", err)
	}
	if !first.Incomplete || first.State.PartialReadOffset <= first.State.PartialLineStartOffset {
		t.Fatalf("initial partial rewrite result = %#v, want resumable partial state", first)
	}
	oldInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat partial rewrite fixture: %v", err)
	}
	partialB := `{"type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"` + strings.Repeat("b", 64) + `"}`
	if len(partialA) != len(partialB) {
		t.Fatalf("same-size partial fixtures differ: %d != %d", len(partialA), len(partialB))
	}
	if err := os.WriteFile(path, []byte(prefix+partialB), 0o600); err != nil {
		t.Fatalf("rewrite partial fixture: %v", err)
	}
	if err := os.Chtimes(path, oldInfo.ModTime(), oldInfo.ModTime()); err != nil {
		t.Fatalf("restore partial fixture mtime: %v", err)
	}
	// Make the test deterministic on filesystems that do not expose ctime: a
	// nonzero saved marker versus the current zero value is still an unknown
	// revision and must force a replay from the last complete newline.
	if first.State.PartialSourceChangeTime == 0 {
		first.State.PartialSourceChangeTime = 1
	}
	rewritten, err := historyTieredScanTail(path, first.State, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("same-size partial rewrite scan: %v", err)
	}
	if rewritten.Truncated || !rewritten.Incomplete || rewritten.State.PartialReadOffset <= rewritten.State.PartialLineStartOffset {
		t.Fatalf("same-size partial rewrite result = %#v, want replayed partial state", rewritten)
	}
	if rewritten.State.PartialSourceChangeTime == first.State.PartialSourceChangeTime && first.State.PartialSourceChangeTime != 1 {
		t.Fatalf("partial source revision did not change: first=%d rewritten=%d", first.State.PartialSourceChangeTime, rewritten.State.PartialSourceChangeTime)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open rewritten partial fixture: %v", err)
	}
	if _, err := f.WriteString("}\n"); err != nil {
		_ = f.Close()
		t.Fatalf("complete rewritten partial record: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close rewritten partial fixture: %v", err)
	}
	appendLine(t, path, `{"type":"event_msg","payload":{"id":"partial-rewrite-final","type":"agent_message","phase":"final_answer","message":"partial rewrite final"}}`)
	completed, err := historyTieredScanTail(path, rewritten.State, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("completed partial rewrite scan: %v", err)
	}
	if len(completed.Finals) != 1 || completed.Finals[0].Record.Text != "partial rewrite final" {
		t.Fatalf("completed partial rewrite finals = %#v, want rewritten suffix final", completed.Finals)
	}
}

func TestHistoryTieredScanTailDrainsOversizedPartialRecordAcrossPolls(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversized-partial.jsonl")
	prefix := []byte(`{"type":"session_meta","payload":{"id":"oversized-partial"}}` + "\n")
	if err := os.WriteFile(path, prefix, 0o600); err != nil {
		t.Fatalf("write oversized-partial prefix: %v", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open oversized-partial payload: %v", err)
	}
	_, writeErr := f.Write(bytes.Repeat([]byte{'x'}, int(historyTieredMaxRecordReadBytes)+1024))
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("write oversized-partial payload: write=%v close=%v", writeErr, closeErr)
	}

	first, err := historyTieredScanTail(path, historyTieredFileState{}, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("initial oversized-partial scan: %v", err)
	}
	if !first.Incomplete || first.State.PartialReadOffset <= first.State.PartialLineStartOffset {
		t.Fatalf("initial oversized-partial result = %#v, want resumable partial state", first)
	}
	appendLine(t, path, "")
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","phase":"final_answer","message":"after huge opaque record"}}`)

	second, err := historyTieredScanTail(path, first.State, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("completed oversized-partial scan: %v", err)
	}
	if second.Incomplete || !second.OversizedRecord || second.State.Offset <= first.State.Offset || len(second.Records) != 1 {
		t.Fatalf("completed oversized-partial result = %#v, want one opaque disposition and a durable cursor", second)
	}
	if second.Records[0].SourceType != "oversized_record" || second.State.ContextGap == nil {
		t.Fatalf("oversized-partial disposition/state = record=%#v state=%#v", second.Records[0], second.State)
	}

	third, err := historyTieredScanTail(path, second.State, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("post-oversized-partial scan: %v", err)
	}
	if len(third.Finals) != 1 || third.Finals[0].Record.Text != "after huge opaque record" {
		t.Fatalf("post-oversized-partial finals = %#v, want the following final", third.Finals)
	}
}

func TestHistoryTieredScanTailConsumesCompleteMalformedLineAndResetsContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transcript.jsonl")
	malformed := `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"old-turn","message":"broken"}`
	later := `{"type":"event_msg","payload":{"id":"later-final","type":"agent_message","thread_id":"thread-1","turn_id":"new-turn","phase":"final_answer","message":"later final"}}`
	contents := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"event_msg","payload":{"id":"old-final","type":"agent_message","thread_id":"thread-1","turn_id":"old-turn","phase":"final_answer","message":"old final"}}`,
		malformed,
		later,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write malformed transcript: %v", err)
	}

	first, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if first.Incomplete || first.State.Offset <= 0 || first.State.Offset >= int64(len(contents)) {
		t.Fatalf("first scan state = %#v, want a complete cursor ending at malformed line", first)
	}
	if first.State.TranscriptQuarantine == nil || first.State.TranscriptQuarantine.Kind != "malformed_record" {
		t.Fatalf("first scan quarantine = %#v, want malformed_record", first.State.TranscriptQuarantine)
	}
	if first.LastConsumedRecordID == "" || first.LastConsumedOffset != first.State.Offset {
		t.Fatalf("first scan consumed = %q/%d, state offset %d", first.LastConsumedRecordID, first.LastConsumedOffset, first.State.Offset)
	}
	if strings.Contains(string(first.State.TranscriptQuarantine.CandidateTextHashes[0]), malformed) {
		t.Fatal("malformed raw payload was stored instead of a bounded hash")
	}
	for _, record := range first.Records {
		if strings.Contains(record.Text, "later final") {
			t.Fatalf("later final was read past malformed boundary: %#v", first.Records)
		}
	}

	second, err := historyTieredScanTail(path, first.State, 1<<20)
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	foundLater := false
	for _, record := range second.Records {
		if record.ItemID == "later-final" || strings.Contains(record.Text, "later final") {
			foundLater = true
			if record.TurnID != "new-turn" || !record.TurnIDExplicit {
				t.Fatalf("later record inherited malformed context: %#v", record)
			}
		}
	}
	if !foundLater {
		t.Fatalf("later final was not reachable after malformed line: %#v", second)
	}
}

func TestHistoryTieredScanTailDoesNotRetryStableMalformedLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transcript.jsonl")
	contents := `{"type":"session_meta","payload":{"id":"thread-1"}}` + "\n" +
		`{"broken":` + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write malformed transcript: %v", err)
	}
	first, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	second, err := historyTieredScanTail(path, first.State, 1<<20)
	if err != nil {
		t.Fatalf("repeat scan: %v", err)
	}
	if second.State.Offset != first.State.Offset || second.State.Line != first.State.Line {
		t.Fatalf("stable malformed cursor moved unexpectedly: first=%#v second=%#v", first.State, second.State)
	}
	if second.Incomplete || second.State.TranscriptQuarantine == nil || second.State.TranscriptQuarantine.Kind != "malformed_record" {
		t.Fatalf("repeat scan = %#v, want stable malformed disposition", second)
	}
}

func TestHistoryTieredScanTailPersistsDrainedPartialReadOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large-partial.jsonl")
	partial := `{"type":"item.completed","item":{"type":"message","role":"assistant","metadata":"` +
		strings.Repeat("x", int(historyTieredMaxTailBytes)+256*1024) + `"}}`
	if err := os.WriteFile(path, []byte(partial), 0o600); err != nil {
		t.Fatalf("write large partial fixture: %v", err)
	}
	first, err := historyTieredScanTail(path, historyTieredFileState{}, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("large partial scan: %v", err)
	}
	if !first.Incomplete || first.State.Offset != 0 || first.State.PartialReadOffset != int64(len(partial)) {
		t.Fatalf("large partial state = %#v, want drained read offset %d at logical offset 0", first.State, len(partial))
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

func TestHistoryTieredScanTailDoesNotMarkOrdinaryNextTurnAsContinuation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"response_item","payload":{"id":"new-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"ordinary next request"}]}}`,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"next-turn","started_at":"2026-08-08T08:00:00Z","model_context_window":128000,"collaboration_mode_kind":"default"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"next-turn","phase":"final_answer","message":"ordinary next answer"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:        path,
		Offset:      0,
		LastFinalID: "previous-final-is-only-a-dedupe-key",
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation {
		t.Fatalf("ordinary next turn was marked unresolved continuation: %#v", result.State)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "ordinary next answer" {
		t.Fatalf("ordinary next turn finals = %#v, want one final", result.Finals)
	}
}

func TestHistoryTieredScanTailDoesNotMarkLegacyOrdinaryNextTurnAsContinuation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"response_item","payload":{"id":"new-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"legacy ordinary next request"}]}}`,
		// Older Codex history emits normal root task_started events without
		// started_at; model_context_window + collaboration_mode_kind are still
		// the root-turn markers and must not be confused with a child continuation.
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"next-turn","model_context_window":128000,"collaboration_mode_kind":"plan"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"next-turn","phase":"final_answer","message":"legacy ordinary next answer"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:        path,
		Offset:      0,
		LastFinalID: "previous-final-is-only-a-dedupe-key",
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation {
		t.Fatalf("legacy ordinary next turn was marked unresolved continuation: %#v", result.State)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "legacy ordinary next answer" {
		t.Fatalf("legacy ordinary next turn finals = %#v, want one final", result.Finals)
	}
}

func TestHistoryTieredScanTailDoesNotMarkLegacyCursorOrdinaryNextTurnAsContinuation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"old-turn","phase":"final_answer","message":"old final"}}` + "\n"
	if err := os.WriteFile(path, []byte(prefix), 0o600); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat prefix: %v", err)
	}
	appendLine(t, path, `{"type":"response_item","payload":{"id":"next-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"ordinary request after legacy cursor"}]}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","turn_id":"next-turn","model_context_window":128000,"collaboration_mode_kind":"plan"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"next-turn","phase":"final_answer","message":"ordinary root after legacy cursor"}}`)
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:   path,
		Size:   info.Size(),
		Offset: info.Size(),
		Line:   1,
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation {
		t.Fatalf("ordinary root after legacy cursor was marked unresolved: %#v", result.State)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "ordinary root after legacy cursor" {
		t.Fatalf("ordinary root after legacy cursor finals = %#v", result.Finals)
	}
}

func TestHistoryTieredScanTailMarksContinuationAcrossIncrementalScan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer final"}}` + "\n"
	if err := os.WriteFile(path, []byte(prefix), 0o600); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	first, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("initial scan: %v", err)
	}
	if first.State.LastFinalID == "" || first.State.Offset <= 0 {
		t.Fatalf("initial scan state = %#v, want final and offset", first.State)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	_, writeErr := f.WriteString(
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"outer-turn"}}` + "\n" +
			`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"continuation final"}}` + "\n",
	)
	closeErr := f.Close()
	if writeErr != nil {
		t.Fatalf("append continuation: %v", writeErr)
	}
	if closeErr != nil {
		t.Fatalf("close append: %v", closeErr)
	}
	second, err := historyTieredScanTail(path, first.State, 1<<20)
	if err != nil {
		t.Fatalf("incremental scan: %v", err)
	}
	if !second.State.UnresolvedContinuation {
		t.Fatalf("incremental continuation was not marked: %#v finals=%#v", second.State, second.Finals)
	}
}

func TestHistoryTieredLineSignalsRecognizesTaskTerminalVariants(t *testing.T) {
	cases := []string{"task_complete", "task.completed", "task_failed", "task.failed"}
	for _, terminal := range cases {
		t.Run(terminal, func(t *testing.T) {
			line := []byte(`{"type":"event_msg","payload":{"type":"` + terminal + `","task_id":"task-1"}}`)
			signal := historyTieredLineSignals(line)
			wantCompleted := strings.Contains(terminal, "complete")
			if signal.TurnCompleted != wantCompleted || signal.TerminalFailed != !wantCompleted || signal.TurnID != "task-1" {
				t.Fatalf("signal = %#v, want completed=%t failed=%t task-1", signal, wantCompleted, !wantCompleted)
			}
		})
	}
	method := historyTieredLineSignals([]byte(`{"method":"turn/completed","params":{"turn_id":"method-turn"}}`))
	if !method.TurnCompleted || method.TurnID != "method-turn" || method.TerminalKind != "turn/completed" {
		t.Fatalf("method terminal signal = %#v, want completed method-turn", method)
	}
}

func TestHistoryTieredLineHintsCoverProtocolMarkerFamilies(t *testing.T) {
	for _, marker := range historyTieredContinuationMarkers {
		name := strings.Trim(string(marker), `"`)
		t.Run("continuation/"+name, func(t *testing.T) {
			hint := historyTieredLineHints([]byte(`{"type":"` + name + `"}`))
			if !hint.MayContinuation {
				t.Fatalf("hint = %#v, want continuation for %q", hint, name)
			}
		})
	}
	for _, marker := range historyTieredTerminalMarkers {
		name := strings.Trim(string(marker), `"`)
		t.Run("terminal/"+name, func(t *testing.T) {
			line := `{"type":"` + name + `"}`
			if name == "final_answer" {
				line = `{"phase":"final_answer"}`
			}
			hint := historyTieredLineHints([]byte(line))
			if !hint.MayTerminal {
				t.Fatalf("hint = %#v, want terminal for %q", hint, name)
			}
		})
	}

	for _, line := range []string{
		`{"turn_id":"turn-1"}`,
		`{"turnId":"turn-2"}`,
		`{"turn":{"id":"turn-3"}}`,
	} {
		hint := historyTieredLineHints([]byte(line))
		if !hint.MayTurnField || hint.MayContinuation || hint.MayTerminal {
			t.Fatalf("hint = %#v for provenance-only line %s, want turn field only", hint, line)
		}
	}
}

func TestHistoryTieredLineHintsDoNotChangeTextMarkerSemantics(t *testing.T) {
	line := []byte(`{"type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"literal \"task_started\" and \"final_answer\" text"}}`)
	hint := historyTieredLineHints(line)
	if !hint.MayContinuation || !hint.MayTerminal {
		t.Fatalf("hint = %#v, want conservative marker candidates", hint)
	}
	signal := historyTieredLineSignals(line)
	if signal.FinalAnswer || signal.TurnCompleted || signal.TerminalFailed {
		t.Fatalf("signal = %#v, text-only markers changed semantics", signal)
	}
	continuationType, _, continuation, _ := historyTieredContinuationSignal(line)
	if continuation || continuationType != "" {
		t.Fatalf("continuation signal = (%q, %t), text-only marker changed semantics", continuationType, continuation)
	}
}

func TestHistoryTieredScanTailLegacyCheckpointWithoutFinalBoundaryDoesNotAcceptContinuation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-legacy-watch"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"old-turn","phase":"final_answer","message":"old final"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"old-turn"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(prefix), 0o600); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat prefix: %v", err)
	}
	// This models a pre-anchor HistoryWatchCheckpoint: it resumes after a
	// completed final but has no LastFinalID/continuation metadata yet.
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","turn_id":"child-turn"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"child-turn","phase":"final_answer","message":"child continuation must not be accepted"}}`)
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:   path,
		Size:   info.Size(),
		Offset: info.Size(),
		Line:   3,
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation {
		t.Fatalf("legacy checkpoint accepted continuation: state=%#v finals=%#v", result.State, result.Finals)
	}
	if len(result.Finals) != 0 {
		t.Fatalf("legacy checkpoint emitted continuation finals: %#v", result.Finals)
	}
}

func TestHistoryTieredScanTailResponseItemOnlyChildFinalIsQuarantined(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := `{"type":"session_meta","payload":{"id":"thread-response-child"}}` + "\n" +
		`{"type":"response_item","payload":{"id":"old-final","type":"message","role":"assistant","turn_id":"outer-turn","phase":"final_answer","content":[{"type":"output_text","text":"old final"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(prefix), 0o600); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat prefix: %v", err)
	}
	appendLine(t, path, `{"type":"response_item","payload":{"id":"child-final","type":"message","role":"assistant","turn_id":"child-turn","phase":"final_answer","content":[{"type":"output_text","text":"child response-only final must not be accepted"}]}}`)
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:        path,
		Size:        info.Size(),
		Offset:      info.Size(),
		Line:        2,
		TurnID:      "outer-turn",
		LastFinalID: "old-final",
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation {
		t.Fatalf("response_item-only child was not marked unresolved: state=%#v finals=%#v", result.State, result.Finals)
	}
	if len(result.Finals) != 0 {
		t.Fatalf("response_item-only child emitted finals: %#v", result.Finals)
	}
}

func TestHistoryTieredScanTailResponseItemWithoutTurnIDAfterTerminalIsQuarantined(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, []byte(`{"type":"session_meta","payload":{"id":"thread-response-no-id"}}`+"\n"), 0o600); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat prefix: %v", err)
	}
	appendLine(t, path, `{"type":"response_item","payload":{"id":"orphan-no-turn","type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"response-only answer without turn provenance"}]}}`)
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:        path,
		Size:        info.Size(),
		Offset:      info.Size(),
		Line:        1,
		TurnID:      "outer-turn",
		LastFinalID: "old-final",
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation {
		t.Fatalf("response_item without turn provenance was accepted: state=%#v finals=%#v", result.State, result.Finals)
	}
	if len(result.Finals) != 0 {
		t.Fatalf("response_item without turn provenance emitted finals: %#v", result.Finals)
	}
}

func TestHistoryTieredScanTailLegacyFinalAndTurnIdentityStillQuarantinesRootShapedContinuation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer final"}}` + "\n"
	if err := os.WriteFile(path, []byte(prefix), 0o600); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat prefix: %v", err)
	}
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","turn_id":"child-turn","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"child-turn","phase":"final_answer","message":"legacy root-shaped child must not publish"}}`)
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:        path,
		Size:        info.Size(),
		Offset:      info.Size(),
		Line:        1,
		TurnID:      "outer-turn",
		LastFinalID: "outer-final",
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation || len(result.Finals) != 0 {
		t.Fatalf("legacy root-shaped continuation was accepted: state=%#v finals=%#v", result.State, result.Finals)
	}
}

func TestHistoryTieredScanTailLegacyTurnIDRootMarkerIsQuarantined(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer final"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(prefix), 0o600); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat prefix: %v", err)
	}
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","turn_id":"child-turn","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"child-turn","phase":"final_answer","message":"legacy turn-id child answer must not publish"}}`)
	// This is the direct/completion-recovery legacy shape: the old checkpoint
	// has a final dedupe key and inherited Codex turn ID, but no explicit
	// TerminalBoundarySeen bit. A root-shaped S462 continuation must still be
	// fail-closed until a visible external prompt proves a new owner.
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:                 path,
		Size:                 info.Size(),
		Offset:               info.Size(),
		Line:                 2,
		TurnID:               "outer-turn",
		LastFinalID:          "outer-final",
		TerminalBoundarySeen: false,
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation {
		t.Fatalf("legacy turn-id root marker accepted child continuation: state=%#v finals=%#v", result.State, result.Finals)
	}
	if len(result.Finals) != 0 {
		t.Fatalf("legacy turn-id root marker emitted child final: %#v", result.Finals)
	}
}

func TestHistoryTieredScanTailFailedTerminalQuarantinesChildResponse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"commentary","message":"work"}}`,
		`{"type":"event_msg","payload":{"type":"task_failed","turn_id":"outer-turn"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(prefix), 0o600); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat prefix: %v", err)
	}
	appendLine(t, path, `{"type":"response_item","payload":{"id":"child-final","type":"message","role":"assistant","turn_id":"child-turn","phase":"final_answer","content":[{"type":"output_text","text":"child after failed task must not publish"}]}}`)
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:   path,
		Size:   info.Size(),
		Offset: info.Size(),
		Line:   2,
		TurnID: "outer-turn",
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation || len(result.Finals) != 0 {
		t.Fatalf("failed terminal child was accepted: state=%#v finals=%#v", result.State, result.Finals)
	}
}

func TestHistoryTieredScanTailEventMessageChildFinalIsQuarantined(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer final"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(prefix), 0o600); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat prefix: %v", err)
	}
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"child-turn","phase":"final_answer","message":"event child final must not publish"}}`)
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path: path, Size: info.Size(), Offset: info.Size(), Line: 2,
		TurnID: "outer-turn", LastFinalID: "outer-final", TerminalBoundarySeen: true,
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation || len(result.Finals) != 0 {
		t.Fatalf("event child final was accepted: state=%#v finals=%#v", result.State, result.Finals)
	}
}

func TestHistoryTieredScanTailNormalizedTaskStartedMarksContinuation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer final"}}` + "\n"
	if err := os.WriteFile(path, []byte(prefix), 0o600); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	first, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil || len(first.Finals) != 1 {
		t.Fatalf("initial scan = state=%#v finals=%#v err=%v", first.State, first.Finals, err)
	}
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task.started","turn_id":"child-turn"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"child-turn","phase":"final_answer","message":"normalized child final must not publish"}}`)
	second, err := historyTieredScanTail(path, first.State, 1<<20)
	if err != nil {
		t.Fatalf("incremental scan: %v", err)
	}
	if !second.State.UnresolvedContinuation || len(second.Finals) != 0 {
		t.Fatalf("normalized child continuation was accepted: state=%#v finals=%#v", second.State, second.Finals)
	}
}

func TestHistoryTieredScanTailS462GoalContinuationUsesRootShapedTaskStarted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
		// The real s462 continuation has the same fields as a normal root
		// task_started.  Those fields are not proof of a new Teams owner.
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"child-task","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}`,
		`{"type":"response_item","payload":{"id":"goal-context","type":"message","role":"user","content":[{"type":"input_text","text":"<codex_internal_context source=\"goal\">Continue working toward the active thread goal.</codex_internal_context>"}]}}`,
		`{"type":"response_item","payload":{"id":"child-final","type":"message","role":"assistant","turn_id":"child-task","phase":"final_answer","content":[{"type":"output_text","text":"child goal answer must remain quarantined"}]}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation {
		t.Fatalf("root-shaped goal continuation was not quarantined: state=%#v finals=%#v", result.State, result.Finals)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "outer answer" {
		t.Fatalf("finals=%#v, want only the outer answer", result.Finals)
	}
}

func TestHistoryTieredScanTailTurnContextContinuationIsQuarantined(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
		// Some app-server revisions use turn_context instead of task_started
		// for the child execution boundary.
		`{"type":"event_msg","payload":{"type":"turn_context","turn_id":"child-turn"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"child-turn","phase":"final_answer","message":"turn_context child answer must remain quarantined"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation || len(result.Finals) != 1 || result.Finals[0].Record.Text != "outer answer" {
		t.Fatalf("turn_context continuation was accepted: state=%#v finals=%#v", result.State, result.Finals)
	}
}

func TestHistoryTieredScanTailTurnContextAfterPromptStartsFreshRoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
		`{"type":"response_item","payload":{"id":"new-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"new explicit request"}]}}`,
		`{"type":"event_msg","payload":{"type":"turn_context","turn_id":"new-turn"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"new-turn","phase":"final_answer","message":"new root answer"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation || len(result.Finals) != 2 || result.Finals[1].Record.Text != "new root answer" {
		t.Fatalf("turn_context root was not released by prompt: state=%#v finals=%#v", result.State, result.Finals)
	}
}

func TestHistoryTieredScanTailAcceptsRootShapedTaskStartedAfterExternalPrompt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
		`{"type":"response_item","payload":{"id":"new-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"new explicit Teams request"}]}}`,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"new-root","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"new-root","phase":"final_answer","message":"new root answer"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation {
		t.Fatalf("explicit new prompt was not accepted as a new root: state=%#v finals=%#v", result.State, result.Finals)
	}
	if len(result.Finals) != 2 || result.Finals[1].Record.Text != "new root answer" {
		t.Fatalf("finals=%#v, want outer and new-root finals", result.Finals)
	}
}

func TestHistoryTieredScanTailAcceptsRootShapedTaskStartedBeforeExternalPrompt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	// app-server normally writes task_started before the user response_item for
	// a fresh root turn.  The ownership hint therefore arrives after the marker
	// that resembles an S462 goal continuation.
	lines := []string{
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"new-root","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}`,
		`{"type":"response_item","payload":{"id":"new-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"new explicit Teams request"}]}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"new-root","phase":"final_answer","message":"new root answer"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation {
		t.Fatalf("external prompt after root marker was not accepted: state=%#v finals=%#v", result.State, result.Finals)
	}
	if len(result.Finals) != 2 || result.Finals[1].Record.Text != "new root answer" {
		t.Fatalf("finals=%#v, want outer and new-root finals", result.Finals)
	}
}

func TestHistoryTieredScanTailDoesNotReuseOuterPromptForLaterContinuation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		// The prompt belongs to the outer Teams turn and must not prove a
		// root-shaped task_started emitted after that turn completed.
		`{"type":"response_item","payload":{"id":"outer-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"outer Teams request"}]}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"child-turn","started_at":1786181089,"model_context_window":128000,"collaboration_mode_kind":"default"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"child-turn","phase":"final_answer","message":"child continuation must remain quarantined"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.State.UnresolvedContinuation {
		t.Fatalf("outer prompt was reused to accept continuation: state=%#v finals=%#v", result.State, result.Finals)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "outer answer" {
		t.Fatalf("finals=%#v, want only outer answer", result.Finals)
	}
}

func TestHistoryTieredScanTailReleasesRootMarkerWhenPromptArrivesNextScan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"new-root","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(prefix), 0o600); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	first, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if first.State.UnresolvedContinuation || !first.State.PendingRootTaskStarted {
		t.Fatalf("first scan state=%#v, want pending root without unresolved anchor", first.State)
	}
	appendLine(t, path, `{"type":"response_item","payload":{"id":"new-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"new explicit Teams request"}]}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"new-root","phase":"final_answer","message":"new root answer"}}`)
	second, err := historyTieredScanTail(path, first.State, 1<<20)
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if second.State.UnresolvedContinuation || second.State.PendingRootTaskStarted {
		t.Fatalf("second scan state=%#v, want released root", second.State)
	}
	if len(second.Finals) != 1 || second.Finals[0].Record.Text != "new root answer" {
		t.Fatalf("second scan finals=%#v, want new root final", second.Finals)
	}
}

func TestReadLinkedTranscriptDeltaBlocksPendingRootContinuationRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-linked-s462"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"outer-turn","phase":"final_answer","message":"outer answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat initial transcript: %v", err)
	}
	checkpoint := teamstore.ImportCheckpoint{
		SourcePath:     path,
		LastRecordID:   "outer-final",
		LastSourceLine: 3,
		LastOffset:     info.Size(),
		SourceSize:     info.Size(),
		SourceModTime:  info.ModTime(),
	}
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","turn_id":"child-task","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}`)
	appendLine(t, path, `{"type":"response_item","payload":{"id":"goal-context","type":"message","role":"user","content":[{"type":"input_text","text":"<codex_internal_context source=\"goal\">Continue.</codex_internal_context>"}]}}`)
	appendLine(t, path, `{"type":"response_item","payload":{"id":"child-final","type":"message","role":"assistant","turn_id":"child-task","phase":"final_answer","content":[{"type":"output_text","text":"orphan S462 answer"}]}}`)
	transcript, err := (&Bridge{}).readLinkedTranscriptDelta(path, checkpoint, "thread-linked-s462", "thread-linked-s462")
	if err != nil {
		t.Fatalf("readLinkedTranscriptDelta: %v", err)
	}
	if !transcript.UnresolvedContinuation && !transcript.PendingContinuation {
		t.Fatalf("pending S462 continuation was not blocked: records=%#v", transcript.Records)
	}
	if transcript.PendingContinuation {
		for _, record := range transcript.Records {
			if strings.Contains(record.Text, "orphan S462 answer") {
				t.Fatalf("pending S462 answer leaked in linked records: %#v", transcript.Records)
			}
		}
	}
}

func TestReadLinkedTranscriptDeltaLegacyCheckpointUsesFailClosedScanner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-linked-legacy"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","id":"outer-final","turn_id":"outer-turn","phase":"final_answer","message":"outer answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"outer-turn"}}`,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"child-turn","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}`,
		`{"type":"response_item","payload":{"id":"goal-context","type":"message","role":"user","content":[{"type":"input_text","text":"<codex_internal_context source=\"goal\">Continue.</codex_internal_context>"}]}}`,
		`{"type":"response_item","payload":{"id":"child-final","type":"message","role":"assistant","turn_id":"child-turn","phase":"final_answer","content":[{"type":"output_text","text":"legacy checkpoint child answer"}]}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	transcript, err := (&Bridge{}).readLinkedTranscriptDelta(path, teamstore.ImportCheckpoint{
		SourcePath:   path,
		LastRecordID: "outer-final",
	}, "thread-linked-legacy", "thread-linked-legacy")
	if err != nil {
		t.Fatalf("readLinkedTranscriptDelta: %v", err)
	}
	if !transcript.UnresolvedContinuation && !transcript.PendingContinuation {
		t.Fatalf("legacy checkpoint child was not blocked: %#v", transcript)
	}
	for _, record := range transcript.Records {
		if strings.Contains(record.Text, "legacy checkpoint child answer") {
			t.Fatalf("legacy checkpoint child leaked through linked delta: %#v", transcript.Records)
		}
	}
}

func TestReadLinkedTranscriptDeltaUnusableRecoveryProofIsSilent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := []string{
		`{"type":"session_meta","payload":{"id":"thread-invalid-proof"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","id":"old-final","turn_id":"old-turn","phase":"final_answer","message":"old answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"old-turn"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","id":"new-final","turn_id":"new-turn","phase":"final_answer","message":"new answer"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	transcript, err := (&Bridge{}).readLinkedTranscriptDelta(path, teamstore.ImportCheckpoint{
		SourcePath:            path,
		LastRecordID:          "old-final",
		RecoveryProofUnusable: true,
	}, "thread-invalid-proof", "thread-invalid-proof")
	if err != nil {
		t.Fatalf("readLinkedTranscriptDelta: %v", err)
	}
	if !transcriptHasDiagnostic(transcript, "legacy_source_unverified") {
		t.Fatalf("diagnostics = %#v, want silent legacy_source_unverified", transcript.Diagnostics)
	}
	if transcript.UnresolvedContinuation || transcript.PendingContinuation || len(transcript.Records) != 0 {
		t.Fatalf("unusable-proof transcript = %#v, want no records or execution gate", transcript)
	}
}

func TestHistoryTieredScanTailLegacyCursorAcceptsFirstExplicitResponseItem(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := `{"type":"session_meta","payload":{"id":"thread-baseline"}}` + "\n" +
		`{"type":"response_item","payload":{"id":"old-user","type":"message","role":"user","content":[{"type":"input_text","text":"old prompt"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(prefix), 0o600); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat prefix: %v", err)
	}
	appendLine(t, path, `{"type":"response_item","payload":{"id":"new-final","type":"message","role":"assistant","turn_id":"new-root-turn","phase":"final_answer","content":[{"type":"output_text","text":"first final after baseline"}]}}`)
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:   path,
		Size:   info.Size(),
		Offset: info.Size(),
		Line:   2,
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if result.State.UnresolvedContinuation {
		t.Fatalf("first explicit response_item after baseline was blocked: %#v", result.State)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "first final after baseline" {
		t.Fatalf("first explicit response_item finals = %#v", result.Finals)
	}
}

func TestHistoryTieredScanTailDedupesSameTurnFinalMirrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	lines := strings.Join([]string{
		`{"type":"event_msg","payload":{"id":"event-final","type":"agent_message","turn_id":"turn-1","phase":"final_answer","message":"same final mirrored by two transcript surfaces"}}`,
		`{"type":"response_item","payload":{"id":"response-final","type":"message","role":"assistant","turn_id":"turn-1","phase":"final_answer","content":[{"type":"output_text","text":"same final mirrored by two transcript surfaces"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(lines), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if len(result.Finals) != 1 {
		t.Fatalf("same-turn mirrored finals = %#v, want one final", result.Finals)
	}
}

func TestHistoryTieredScanTailDedupesIncrementalSameTurnFinalMirror(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	eventFinal := `{"type":"event_msg","payload":{"id":"event-final","type":"agent_message","turn_id":"turn-1","phase":"final_answer","message":"same final mirrored in a later response item"}}` + "\n"
	if err := os.WriteFile(path, []byte(eventFinal), 0o600); err != nil {
		t.Fatalf("write event final: %v", err)
	}
	first, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil || len(first.Finals) != 1 {
		t.Fatalf("initial scan = state=%#v finals=%#v err=%v", first.State, first.Finals, err)
	}
	appendLine(t, path, `{"type":"response_item","payload":{"id":"response-final","type":"message","role":"assistant","turn_id":"turn-1","phase":"final_answer","content":[{"type":"output_text","text":"same final mirrored in a later response item"}]}}`)
	second, err := historyTieredScanTail(path, first.State, 1<<20)
	if err != nil {
		t.Fatalf("incremental scan: %v", err)
	}
	if len(second.Finals) != 0 {
		t.Fatalf("incremental same-turn mirror emitted duplicate final: %#v", second.Finals)
	}
}

func TestHistoryTieredScanTailTruncateClearsOldContinuationMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, []byte(`{"type":"session_meta","payload":{"id":"thread-new"}}`+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{
		Path:                   path,
		Size:                   100,
		Offset:                 100,
		Line:                   3,
		LastFinalID:            "old-final",
		UnresolvedContinuation: true,
	}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if !result.Truncated || result.State.Offset != 0 || result.State.UnresolvedContinuation || result.State.LastFinalID != "" {
		t.Fatalf("truncated state retained old boundary: %#v", result.State)
	}
}

func TestHistoryTieredScanTailMarksNestedTurnIDExplicit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	line := `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","turn":{"id":"nested-turn"},"content":[{"type":"output_text","text":"nested turn final"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, 1<<20)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if len(result.Finals) != 1 {
		t.Fatalf("finals = %#v, want one final", result.Finals)
	}
	if !result.Finals[0].Record.TurnIDExplicit || result.Finals[0].Record.TurnID != "nested-turn" {
		t.Fatalf("nested turn provenance = explicit=%t id=%q, want explicit nested-turn", result.Finals[0].Record.TurnIDExplicit, result.Finals[0].Record.TurnID)
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
