package teams

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func BenchmarkReadLinkedTranscriptDeltaLargeTailFromOffset(b *testing.B) {
	transcriptPath := filepath.Join(b.TempDir(), "session.jsonl")
	initial := `{"type":"session_meta","payload":{"id":"thread-linked-bench"}}` + "\n" +
		`{"id":"old","thread_id":"thread-linked-bench","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		b.Fatalf("write initial transcript: %v", err)
	}
	info, err := os.Stat(transcriptPath)
	if err != nil {
		b.Fatalf("stat initial transcript: %v", err)
	}
	checkpoint := teamstore.ImportCheckpoint{
		SourcePath:     transcriptPath,
		SourceSize:     info.Size(),
		SourceModTime:  info.ModTime(),
		LastOffset:     int64(len(initial)),
		LastSourceLine: 2,
		LastRecordID:   "old",
	}
	var updated strings.Builder
	updated.WriteString(initial)
	updated.WriteString(`{"type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":`)
	updated.WriteString(strconv.Quote(strings.Repeat("x", int(historyTieredMaxTailBytes)+1024)))
	updated.WriteString(`}}` + "\n")
	updated.WriteString(`{"type":"event_msg","payload":{"type":"agent_message","id":"large-linked-final","turn_id":"turn-large","phase":"final_answer","message":"large linked final"}}` + "\n")
	updatedBody := updated.String()
	if err := os.WriteFile(transcriptPath, []byte(updatedBody), 0o600); err != nil {
		b.Fatalf("write updated transcript: %v", err)
	}

	bridge := &Bridge{}
	b.ReportAllocs()
	b.SetBytes(int64(len(updatedBody) - len(initial)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		delta, err := bridge.readLinkedTranscriptDelta(transcriptPath, checkpoint, "thread-linked-bench", "thread-linked-bench")
		if err != nil {
			b.Fatalf("read linked transcript delta: %v", err)
		}
		if !transcriptRecordsContainText(delta.Records, "large linked final") {
			b.Fatalf("delta records = %#v, want large linked final", delta.Records)
		}
	}
}
