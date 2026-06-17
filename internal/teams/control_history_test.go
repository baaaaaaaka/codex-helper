package teams

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestControlChatHistoryMigratesLegacyJSONLWithoutRewrite(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), controlChatHistoryFileName)
	legacy := controlChatHistoryEntry{
		Version:   1,
		ChatID:    "control-chat",
		MessageID: "legacy-message",
		Direction: "user",
		Kind:      "control_inbound",
		CreatedAt: time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC),
		Text:      "legacy command",
	}
	var body strings.Builder
	enc := json.NewEncoder(&body)
	if err := enc.Encode(legacy); err != nil {
		t.Fatalf("encode legacy history entry: %v", err)
	}
	if err := os.WriteFile(path, []byte(body.String()), 0o600); err != nil {
		t.Fatalf("write legacy history entry: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read legacy history before: %v", err)
	}
	if err := appendControlChatHistoryEntry(ctx, path, controlChatHistoryEntry{
		Version:   1,
		ChatID:    "control-chat",
		MessageID: "new-message",
		Direction: "assistant",
		Kind:      "control_outbound",
		CreatedAt: time.Date(2026, 5, 15, 1, 3, 0, 0, time.UTC),
		Text:      "new response",
	}); err != nil {
		t.Fatalf("append migrated history entry: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read legacy history after: %v", err)
	}
	if string(after) != string(before) {
		t.Fatal("appending control history rewrote legacy JSONL")
	}
	if _, err := os.Stat(teamsLedgerSQLitePath(path)); err != nil {
		t.Fatalf("stat control history sqlite sidecar: %v", err)
	}
	entries, err := readControlChatHistoryEntries(path)
	if err != nil {
		t.Fatalf("read migrated control history: %v", err)
	}
	if !controlHistoryEntriesContain(entries, "user", "legacy-message", "legacy command") {
		t.Fatalf("migrated history missing legacy entry: %#v", entries)
	}
	if !controlHistoryEntriesContain(entries, "assistant", "new-message", "new response") {
		t.Fatalf("migrated history missing new entry: %#v", entries)
	}
}

func TestControlChatHistoryReadRecoversPartialSQLiteSidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), controlChatHistoryFileName)
	legacy := controlChatHistoryEntry{
		Version:   1,
		ChatID:    "control-chat",
		MessageID: "legacy-message",
		Direction: "user",
		Kind:      "control_inbound",
		CreatedAt: time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC),
		Text:      "legacy command",
	}
	var body strings.Builder
	enc := json.NewEncoder(&body)
	if err := enc.Encode(legacy); err != nil {
		t.Fatalf("encode legacy history entry: %v", err)
	}
	if err := os.WriteFile(path, []byte(body.String()), 0o600); err != nil {
		t.Fatalf("write legacy history entry: %v", err)
	}
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		t.Fatalf("open partial sqlite sidecar: %v", err)
	}
	if err := ensureControlChatHistorySQLite(context.Background(), db); err != nil {
		_ = db.Close()
		t.Fatalf("create partial sqlite sidecar: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close partial sqlite sidecar: %v", err)
	}

	entries, err := readControlChatHistoryEntries(path)
	if err != nil {
		t.Fatalf("read partial sqlite sidecar history: %v", err)
	}
	if !controlHistoryEntriesContain(entries, "user", "legacy-message", "legacy command") {
		t.Fatalf("partial sqlite read did not recover legacy entry: %#v", entries)
	}
}

func BenchmarkControlChatHistoryAppend(b *testing.B) {
	ctx := context.Background()
	path := filepath.Join(b.TempDir(), controlChatHistoryFileName)
	for i := 0; i < maxControlChatHistoryEntries; i++ {
		if err := appendControlChatHistoryEntry(ctx, path, controlChatHistoryEntry{
			ChatID:    "control-chat",
			MessageID: fmt.Sprintf("seed-%06d", i),
			Direction: "user",
			Kind:      "control_inbound",
			CreatedAt: time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC).Add(time.Duration(i) * time.Second),
			Text:      "seed command",
		}); err != nil {
			b.Fatalf("seed control history: %v", err)
		}
	}
	b.ReportAllocs()
	beforeIO, beforeIOOK := cxpPerfReadProcSelfIO()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := appendControlChatHistoryEntry(ctx, path, controlChatHistoryEntry{
			ChatID:    "control-chat",
			MessageID: fmt.Sprintf("new-%06d", i),
			Direction: "assistant",
			Kind:      "control_outbound",
			CreatedAt: time.Date(2026, 5, 15, 2, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second),
			Text:      "new response",
		}); err != nil {
			b.Fatalf("append control history: %v", err)
		}
	}
	b.StopTimer()
	cxpPerfReportProcIODelta(b, beforeIO, beforeIOOK, b.N)
}
