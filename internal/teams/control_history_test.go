package teams

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestControlChatHistoryReadsLongEscapedLegacyEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), controlChatHistoryFileName)
	var body strings.Builder
	enc := json.NewEncoder(&body)
	if err := enc.Encode(controlChatHistoryEntry{
		Version:   1,
		ChatID:    "control-chat",
		MessageID: "message-1",
		Direction: "user",
		Kind:      "control_inbound",
		CreatedAt: time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC),
		Text:      strings.Repeat("<", maxControlChatHistoryEntryBytes),
	}); err != nil {
		t.Fatalf("encode legacy history entry: %v", err)
	}
	if err := os.WriteFile(path, []byte(body.String()), 0o600); err != nil {
		t.Fatalf("write legacy history entry: %v", err)
	}

	entries, err := readControlChatHistoryEntries(path)
	if err != nil {
		t.Fatalf("read legacy history entry: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if entries[0].Text == "" || !strings.Contains(entries[0].Text, "<") {
		t.Fatalf("entry text was not preserved after read: %#v", entries[0])
	}
}
