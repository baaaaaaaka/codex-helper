package teams

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkTeamsTranscriptReadLongMixed(b *testing.B) {
	path, _ := writeBenchmarkTranscript(b, 4000, 24)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transcript, err := ReadSessionTranscript(path)
		if err != nil {
			b.Fatalf("ReadSessionTranscript: %v", err)
		}
		if len(transcript.Records) == 0 {
			b.Fatal("expected transcript records")
		}
	}
}

func BenchmarkTeamsTranscriptReadSinceTail(b *testing.B) {
	path, checkpoints := writeBenchmarkTranscript(b, 4000, 24)
	afterKey := checkpoints[len(checkpoints)-25]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transcript, err := ReadSessionTranscriptSince(path, afterKey)
		if err != nil {
			b.Fatalf("ReadSessionTranscriptSince: %v", err)
		}
		if len(transcript.Records) == 0 || len(transcript.Records) > 40 {
			b.Fatalf("tail records = %d, want small non-empty tail", len(transcript.Records))
		}
	}
}

func BenchmarkTeamsTranscriptVisibleFilterLongMixed(b *testing.B) {
	path, _ := writeBenchmarkTranscript(b, 4000, 24)
	transcript, err := ReadSessionTranscript(path)
	if err != nil {
		b.Fatalf("ReadSessionTranscript: %v", err)
	}
	teamsOriginHashes := map[string]bool{}
	knownHashes := map[TranscriptKind]map[string]bool{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := countVisibleTranscriptSyncRecords(transcript.Records, teamsOriginHashes, knownHashes, recentCompletedTeamsTranscriptMirrorSkipper{}); got == 0 {
			b.Fatal("expected visible records")
		}
	}
}

func BenchmarkTeamsTranscriptFormatLongMixed(b *testing.B) {
	path, _ := writeBenchmarkTranscript(b, 4000, 24)
	transcript, err := ReadSessionTranscript(path)
	if err != nil {
		b.Fatalf("ReadSessionTranscript: %v", err)
	}
	records := transcript.Records
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, record := range records {
			_ = formatTranscriptRecordForTeams(record)
		}
	}
}

func writeBenchmarkTranscript(tb testing.TB, turns int, longEvery int) (string, []string) {
	tb.Helper()
	dir := tb.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	var lines []string
	lines = append(lines, `{"type":"session_meta","payload":{"id":"bench-thread-1"}}`)
	checkpoints := make([]string, 0, turns*3)
	for i := 0; i < turns; i++ {
		userID := fmt.Sprintf("user-%05d", i)
		assistantID := fmt.Sprintf("assistant-%05d", i)
		statusID := fmt.Sprintf("status-%05d", i)
		userText := fmt.Sprintf("please inspect case %05d and update the notes", i)
		answerText := fmt.Sprintf("completed case %05d\n\n- checked input\n- wrote summary\n\n```text\ncase-%05d ok\n```", i, i)
		if longEvery > 0 && i%longEvery == 0 {
			answerText += "\n\n" + strings.Repeat("| col | value |\n| --- | --- |\n| key | value |\n", 12)
		}
		lines = append(lines,
			fmt.Sprintf(`{"type":"response_item","payload":{"id":%q,"type":"message","role":"user","content":[{"type":"input_text","text":%q}]}}`, userID, userText),
			fmt.Sprintf(`{"type":"event_msg","payload":{"id":%q,"type":"agent_message","phase":"commentary","message":%q}}`, statusID, "working on case "+fmt.Sprintf("%05d", i)),
			fmt.Sprintf(`{"type":"response_item","payload":{"id":"tool-%05d","type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"rg case\"}"}}`, i),
			fmt.Sprintf(`{"type":"response_item","payload":{"id":%q,"type":"message","role":"assistant","content":[{"type":"output_text","text":%q}]}}`, assistantID, answerText),
		)
		checkpoints = append(checkpoints, "source:"+userID, "source:"+statusID, "source:"+fmt.Sprintf("tool-%05d", i), "source:"+assistantID)
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		tb.Fatalf("write benchmark transcript: %v", err)
	}
	return path, checkpoints
}
