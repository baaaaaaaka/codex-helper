package teams

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseCodexTranscriptPreservesUserAssistantOrder(t *testing.T) {
	input := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"session-1","cwd":"/work","source":"cli"}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"id":"user-1","type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}`,
		`{"timestamp":"2026-01-01T00:02:00Z","type":"response_item","payload":{"id":"assistant-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"hi"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if got.ThreadID != "session-1" {
		t.Fatalf("ThreadID = %q, want session-1", got.ThreadID)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want 2", got.Records)
	}
	if got.Records[0].ThreadID != "session-1" || got.Records[1].ThreadID != "session-1" {
		t.Fatalf("record thread ids = %q/%q, want session-1", got.Records[0].ThreadID, got.Records[1].ThreadID)
	}
	assertTranscriptRecord(t, got.Records[0], "user-1", "source:user-1", TranscriptKindUser, "hello", 2)
	assertTranscriptRecord(t, got.Records[1], "assistant-1", "source:assistant-1", TranscriptKindAssistant, "hi", 3)
}

func TestParseCodexTranscriptSupportsExecJSONLItemCompleted(t *testing.T) {
	input := strings.Join([]string{
		"Reading additional input from stdin...",
		`{"type":"thread.started","thread_id":"thread-123"}`,
		`{"type":"turn.started","turn_id":"turn-1"}`,
		`{"type":"item.completed","item":{"id":"item-1","type":"agent_message","content":[{"type":"output_text","text":"done"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "exec.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Diagnostics) != 0 {
		t.Fatalf("diagnostics = %#v, want non-JSON prelude skipped", got.Diagnostics)
	}
	if len(got.Records) != 1 {
		t.Fatalf("records = %#v, want 1", got.Records)
	}
	record := got.Records[0]
	if record.ThreadID != "thread-123" || record.TurnID != "turn-1" {
		t.Fatalf("record ids = thread %q turn %q", record.ThreadID, record.TurnID)
	}
	assertTranscriptRecord(t, record, "item-1", "source:item-1", TranscriptKindAssistant, "done", 4)
}

func TestParseCodexTranscriptClassifiesEventCommentaryAsStatus(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"agent_message","id":"commentary-1","message":"working","phase":"commentary"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","id":"final-1","message":"done","phase":"final_answer"}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want 2", got.Records)
	}
	assertTranscriptRecord(t, got.Records[0], "commentary-1", "source:commentary-1", TranscriptKindStatus, "working", 1)
	assertTranscriptRecord(t, got.Records[1], "final-1", "source:final-1", TranscriptKindAssistant, "done", 2)
}

func TestParseCodexTranscriptBadTailIsDiagnosticOnly(t *testing.T) {
	input := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"session-1","cwd":"/work","source":"cli"}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"id":"user-1","type":"message","role":"user","content":[{"type":"input_text","text":"complete"}]}}`,
		`{"timestamp":"2026-01-01T00:02:00Z","type":"response_item","payload":{"id":"assistant-tail","type":"message","role":"assistant","content":[{"type":"output_text","text":"partial"}]}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 1 {
		t.Fatalf("records = %#v, want only the complete record", got.Records)
	}
	assertTranscriptRecord(t, got.Records[0], "user-1", "source:user-1", TranscriptKindUser, "complete", 2)
	if len(got.Diagnostics) != 1 {
		t.Fatalf("diagnostics = %#v, want 1", got.Diagnostics)
	}
	if got.Diagnostics[0].Kind != "invalid_json" || got.Diagnostics[0].SourceLine != 3 {
		t.Fatalf("diagnostic = %#v, want invalid_json on line 3", got.Diagnostics[0])
	}
}

func TestParseCodexTranscriptMissingIDUsesStableFallback(t *testing.T) {
	input := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"session-1","cwd":"/work","source":"cli"}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"no id"}]}}`,
	}, "\n")

	got1, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript #1 error: %v", err)
	}
	got2, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript #2 error: %v", err)
	}
	if len(got1.Records) != 1 || len(got2.Records) != 1 {
		t.Fatalf("records got1=%#v got2=%#v", got1.Records, got2.Records)
	}
	wantID := "fallback:session:session-1:line:2:kind:user"
	if got1.FileFingerprint != "session:session-1" {
		t.Fatalf("FileFingerprint = %q, want session:session-1", got1.FileFingerprint)
	}
	if got1.Records[0].ItemID != wantID || got1.Records[0].DedupeKey != wantID {
		t.Fatalf("fallback identity = item %q dedupe %q, want %q", got1.Records[0].ItemID, got1.Records[0].DedupeKey, wantID)
	}
	if got2.Records[0].ItemID != got1.Records[0].ItemID {
		t.Fatalf("fallback ItemID changed across parses: %q vs %q", got1.Records[0].ItemID, got2.Records[0].ItemID)
	}
}

func TestParseCodexTranscriptDuplicateSourceIDKeepsOrderedRecordsAndReportsDiagnostic(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"session-1"}}`,
		`{"type":"response_item","payload":{"id":"dup","type":"message","role":"user","content":[{"type":"input_text","text":"first"}]}}`,
		`{"type":"response_item","payload":{"id":"dup","type":"message","role":"assistant","content":[{"type":"output_text","text":"second"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want 2", got.Records)
	}
	assertTranscriptRecord(t, got.Records[0], "dup", "source:dup", TranscriptKindUser, "first", 2)
	assertTranscriptRecord(t, got.Records[1], "dup#line:3", "source:dup", TranscriptKindAssistant, "second", 3)
	if len(got.Diagnostics) != 1 || got.Diagnostics[0].Kind != "duplicate_item_id" || got.Diagnostics[0].SourceLine != 3 {
		t.Fatalf("diagnostics = %#v, want duplicate_item_id on line 3", got.Diagnostics)
	}
}

func TestParseCodexTranscriptUsesJSONLOrderBeforeTimestamp(t *testing.T) {
	input := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:10:00Z","type":"response_item","payload":{"id":"first","type":"message","role":"user","content":[{"type":"input_text","text":"first in file"}]}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"id":"second","type":"message","role":"assistant","content":[{"type":"output_text","text":"second in file"}]}}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records = %#v, want 2", got.Records)
	}
	if got.Records[0].Text != "first in file" || got.Records[1].Text != "second in file" {
		t.Fatalf("records reordered by timestamp: %#v", got.Records)
	}
	if !got.Records[0].CreatedAt.After(got.Records[1].CreatedAt) {
		t.Fatalf("test fixture should have reversed timestamps: %#v", got.Records)
	}
}

func TestReadSessionTranscriptSinceRefusesMissingCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	input := `{"type":"session_meta","payload":{"id":"session-1"}}` + "\n" +
		`{"type":"response_item","payload":{"id":"first","type":"message","role":"user","content":[{"type":"input_text","text":"first"}]}}` + "\n" +
		`{"type":"response_item","payload":{"id":"second","type":"message","role":"assistant","content":[{"type":"output_text","text":"second"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := ReadSessionTranscriptSince(path, "source:first")
	if err != nil {
		t.Fatalf("ReadSessionTranscriptSince error: %v", err)
	}
	if len(got.Records) != 1 || got.Records[0].ItemID != "second" {
		t.Fatalf("records after checkpoint = %#v", got.Records)
	}

	got, err = ReadSessionTranscriptSince(path, "missing")
	if err != nil {
		t.Fatalf("ReadSessionTranscriptSince missing error: %v", err)
	}
	if len(got.Records) != 0 {
		t.Fatalf("records for missing checkpoint = %#v, want none", got.Records)
	}
	if len(got.Diagnostics) == 0 || got.Diagnostics[len(got.Diagnostics)-1].Kind != "checkpoint_not_found" {
		t.Fatalf("diagnostics = %#v, want checkpoint_not_found", got.Diagnostics)
	}
}

func assertTranscriptRecord(t *testing.T, record TranscriptRecord, wantItemID string, wantDedupeKey string, wantKind TranscriptKind, wantText string, wantLine int) {
	t.Helper()
	if record.ItemID != wantItemID {
		t.Fatalf("ItemID = %q, want %q in %#v", record.ItemID, wantItemID, record)
	}
	if record.DedupeKey != wantDedupeKey {
		t.Fatalf("DedupeKey = %q, want %q in %#v", record.DedupeKey, wantDedupeKey, record)
	}
	if record.Kind != wantKind {
		t.Fatalf("Kind = %q, want %q in %#v", record.Kind, wantKind, record)
	}
	if record.Text != wantText {
		t.Fatalf("Text = %q, want %q in %#v", record.Text, wantText, record)
	}
	if record.SourceLine != wantLine {
		t.Fatalf("SourceLine = %d, want %d in %#v", record.SourceLine, wantLine, record)
	}
}
