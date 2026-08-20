package teams

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHistoryWatchReadProofRejectsSameSizeTailRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	original := []byte(`{"id":"record-1","role":"assistant","text":"old tail"}` + "\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("write original transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("historyTieredScanTail: %v", err)
	}
	if len(result.Records) == 0 || !result.ReadProofRangeKnown {
		t.Fatalf("scan result lacks records/read proof: %#v", result)
	}
	if !historyWatchReadProofMatches(path, result) {
		t.Fatal("unchanged transcript did not satisfy its scan read proof")
	}
	recordless := result
	recordless.Records = nil
	recordless.Finals = nil
	if !historyWatchReadProofMatches(path, recordless) {
		t.Fatal("recordless scan did not satisfy its read proof before rewrite")
	}

	replacement := []byte(`{"id":"record-1","role":"assistant","text":"new tail"}` + "\n")
	if len(replacement) != len(original) {
		t.Fatalf("test replacement changed size: %d versus %d", len(replacement), len(original))
	}
	if err := os.WriteFile(path, replacement, 0o600); err != nil {
		t.Fatalf("rewrite transcript: %v", err)
	}
	if historyWatchReadProofMatches(path, result) {
		t.Fatal("same-size tail rewrite passed the history-watch read proof")
	}
	if historyWatchReadProofMatches(path, recordless) {
		t.Fatal("same-size recordless tail rewrite passed the history-watch read proof")
	}
}
