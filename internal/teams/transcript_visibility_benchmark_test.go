package teams

// These are workload probes, not comparisons between the named variants.
// Compare a change by running the same benchmark command at the base and
// candidate revisions; compare only identical benchmark names and flags.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func BenchmarkParseChatGPTAppTextInternalEvents(b *testing.B) {
	data := benchmarkChatGPTAppInternalTranscript(256, false)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transcript, err := ParseCodexTranscript(bytes.NewReader(data), TranscriptParseOptions{SourceName: "chatgpt-app-benchmark.jsonl"})
		if err != nil {
			b.Fatalf("ParseCodexTranscript: %v", err)
		}
		if len(transcript.Records) == 0 {
			b.Fatal("expected parsed transcript records")
		}
	}
}

func BenchmarkParseChatGPTAppCheckpointEvents(b *testing.B) {
	data := benchmarkChatGPTAppInternalTranscript(256, true)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transcript, err := ParseCodexTranscript(bytes.NewReader(data), TranscriptParseOptions{SourceName: "chatgpt-app-checkpoint-benchmark.jsonl"})
		if err != nil {
			b.Fatalf("ParseCodexTranscript: %v", err)
		}
		if len(transcript.Records) == 0 {
			b.Fatal("expected parsed transcript records")
		}
	}
}

func BenchmarkReadLinkedTranscriptDeltaChatGPTAppCheckpointEvents(b *testing.B) {
	transcriptPath := filepath.Join(b.TempDir(), "session.jsonl")
	initial := `{"type":"session_meta","payload":{"id":"thread-chatgpt-app-benchmark"}}` + "\n" +
		`{"id":"old","thread_id":"thread-chatgpt-app-benchmark","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		b.Fatalf("write initial transcript: %v", err)
	}
	info, err := os.Stat(transcriptPath)
	if err != nil {
		b.Fatalf("stat initial transcript: %v", err)
	}
	tail := benchmarkChatGPTAppInternalTranscript(256, true)
	newline := bytes.IndexByte(tail, '\n')
	if newline < 0 {
		b.Fatal("ChatGPT app benchmark transcript has no session metadata line")
	}
	updated := initial + string(tail[newline+1:])
	if err := os.WriteFile(transcriptPath, []byte(updated), 0o600); err != nil {
		b.Fatalf("write updated transcript: %v", err)
	}
	checkpoint := teamstore.ImportCheckpoint{
		SourcePath:     transcriptPath,
		SourceSize:     info.Size(),
		SourceModTime:  info.ModTime(),
		LastOffset:     int64(len(initial)),
		LastSourceLine: 2,
		LastRecordID:   "old",
	}
	bridge := &Bridge{}
	b.SetBytes(int64(len(updated) - len(initial)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transcript, err := bridge.readLinkedTranscriptDelta(transcriptPath, checkpoint, "thread-chatgpt-app-benchmark", "thread-chatgpt-app-benchmark")
		if err != nil {
			b.Fatalf("readLinkedTranscriptDelta: %v", err)
		}
		if len(transcript.Records) == 0 {
			b.Fatal("expected checkpoint tail records")
		}
	}
}

func BenchmarkImportChatGPTAppCheckpointEvents(b *testing.B) {
	data := benchmarkChatGPTAppInternalTranscript(256, true)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	var totalSQLiteBytes int64
	var totalSQLiteWALBytes int64
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store, bridge, session, transcriptPath := newCXPPerfBackgroundImportCheckpointOnlyFixture(b, 1, i)
		dataForSession := bytes.Replace(data, []byte("chatgpt-app-benchmark"), []byte(session.CodexThreadID), 1)
		if err := os.WriteFile(transcriptPath, dataForSession, 0o600); err != nil {
			b.Fatalf("write ChatGPT app transcript: %v", err)
		}
		beforeBytes := cxpPerfSQLiteStoreBytes(b, store)
		beforeWALBytes := cxpPerfSQLiteWALBytes(b, store)
		b.StartTimer()
		result, err := bridge.importTranscriptRecordsToTeams(ctx, session, transcriptPath, "import-chatgpt:"+session.ID, "import-chatgpt", transcriptCheckpointID(session.ID), transcriptImportRunOptions{QueueOnly: true, MaxBatches: 1})
		b.StopTimer()
		if err != nil {
			b.Fatalf("import ChatGPT app transcript: %v", err)
		}
		if result.LastRecordID == "" || result.LastOffset <= 0 {
			b.Fatalf("import ChatGPT app transcript made no checkpoint progress: %#v", result)
		}
		totalSQLiteBytes += cxpPerfSQLiteStoreBytes(b, store) - beforeBytes
		totalSQLiteWALBytes += cxpPerfSQLiteWALBytes(b, store) - beforeWALBytes
		b.StartTimer()
	}
	if b.N > 0 {
		b.ReportMetric(float64(totalSQLiteBytes)/float64(b.N), "sqlite_bytes_delta/op")
		b.ReportMetric(float64(totalSQLiteWALBytes)/float64(b.N), "sqlite_wal_bytes_delta/op")
	}
}

func BenchmarkHistoryTieredScanChatGPTAppCheckpointEvents(b *testing.B) {
	path := filepath.Join(b.TempDir(), "session.jsonl")
	data := benchmarkChatGPTAppInternalTranscript(256, true)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		b.Fatalf("write transcript: %v", err)
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := historyTieredScanTail(path, historyTieredFileState{}, 0)
		if err != nil {
			b.Fatalf("historyTieredScanTail: %v", err)
		}
		if len(result.Records) == 0 {
			b.Fatal("expected history tail records")
		}
	}
}

func BenchmarkHistoryTieredScanChatGPTAppWarmNoChange(b *testing.B) {
	path := filepath.Join(b.TempDir(), "session.jsonl")
	data := benchmarkChatGPTAppInternalTranscript(256, true)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		b.Fatalf("write transcript: %v", err)
	}
	initial, err := historyTieredScanTail(path, historyTieredFileState{}, 0)
	if err != nil {
		b.Fatalf("initial historyTieredScanTail: %v", err)
	}
	state := initial.State
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := historyTieredScanTail(path, state, 0)
		if err != nil {
			b.Fatalf("warm historyTieredScanTail: %v", err)
		}
		if result.BytesRead != 0 || len(result.Records) != 0 {
			b.Fatalf("warm history scan reread unchanged transcript: bytes=%d records=%d", result.BytesRead, len(result.Records))
		}
		state = result.State
	}
}

func BenchmarkPlanChatGPTAppUnknownEvents(b *testing.B) {
	records := make([]TranscriptRecord, 0, 4096)
	for i := 0; i < 4096; i++ {
		records = append(records, TranscriptRecord{
			ItemID: "unknown-" + strconv.Itoa(i),
			Kind:   TranscriptKindUnknown,
			Text:   "private future event text",
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dedupe := newTranscriptDedupeState()
		for index, record := range records {
			_, _, _, _ = planTranscriptImportRecord(record, index+1, int64(index+1), "sync", index+1, dedupe, transcriptPlanOptions{})
		}
	}
}

func benchmarkChatGPTAppInternalTranscript(events int, includeCheckpointOnly bool) []byte {
	var transcript strings.Builder
	transcript.WriteString(`{"type":"session_meta","payload":{"id":"chatgpt-app-benchmark"}}` + "\n")
	for i := 0; i < events; i++ {
		id := strconv.Itoa(i)
		fmt.Fprintf(&transcript, `{"type":"event_msg","payload":{"type":"agent_reasoning","id":"agent-reasoning-%s","message":%s}}`+"\n", id, strconv.Quote("Planning internal step "+id))
		fmt.Fprintf(&transcript, `{"type":"response_item","payload":{"id":"reasoning-%s","type":"reasoning","summary":[{"type":"summary_text","text":%s}]}}`+"\n", id, strconv.Quote("Internal reasoning summary "+id))
		fmt.Fprintf(&transcript, `{"type":"event_msg","payload":{"type":"token_count","id":"token-count-%s","message":%s,"info":{"total_token_usage":{"input_tokens":12,"output_tokens":3}}}}`+"\n", id, strconv.Quote("private token accounting "+id))
		fmt.Fprintf(&transcript, `{"type":"patch_apply_end","id":"patch-%s","message":%s}`+"\n", id, strconv.Quote("Applied private patch "+id))
		fmt.Fprintf(&transcript, `{"type":"item.completed","item":{"id":"completed-patch-%s","type":"patch_apply_end","text":%s}}`+"\n", id, strconv.Quote("Completed private patch "+id))
		if includeCheckpointOnly {
			fmt.Fprintf(&transcript, `{"type":"response_item","payload":{"id":"raw-reasoning-%s","type":"reasoning","encrypted_content":"private","summary":[]}}`+"\n", id)
			fmt.Fprintf(&transcript, `{"type":"event_msg","payload":{"type":"token_count","id":"empty-token-count-%s","info":{"total_token_usage":{"input_tokens":12,"output_tokens":3}}}}`+"\n", id)
		}
	}
	return []byte(transcript.String())
}
