package teams

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestBridgeSyncLinkedTranscriptPersistsHistoryQuarantineWithLegacyGeneration(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			testBridgeSyncLinkedTranscriptPersistsHistoryQuarantineWithLegacyGeneration(t, useSQLite)
		})
	}
}

func testBridgeSyncLinkedTranscriptPersistsHistoryQuarantineWithLegacyGeneration(t *testing.T, useSQLite bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "legacy-generation.jsonl")
	threadID := "thread-legacy-generation"
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-legacy-generation"}}`,
		`{"type":"event_msg","payload":{"id":"old-final","type":"agent_message","phase":"final_answer","message":"old answer"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, threadID, path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, threadID)
	*sent = nil

	checkpointID := transcriptCheckpointID(session.ID)
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load seeded legacy checkpoint: %v", err)
	}
	seeded, found := state.ImportCheckpoints[checkpointID]
	if !found {
		t.Fatalf("legacy checkpoint %s was not found", checkpointID)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{
			ID:                checkpointID,
			SessionID:         session.ID,
			SourcePath:        seeded.SourcePath,
			SourceFingerprint: seeded.SourceFingerprint,
			LastRecordID:      seeded.LastRecordID,
			LastSourceLine:    seeded.LastSourceLine,
			LastOffset:        seeded.LastOffset,
			LastOffsetKnown:   seeded.LastOffsetKnown,
			SourceSize:        seeded.SourceSize,
			SourceModTime:     seeded.SourceModTime,
			Status:            importCheckpointStatusComplete,
			UpdatedAt:         time.Now(),
		}
		return nil
	}); err != nil {
		t.Fatalf("write legacy checkpoint shape: %v", err)
	}
	if useSQLite {
		if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
			t.Fatalf("migrate legacy-generation store to SQLite: %v", err)
		}
	}

	answer := "legacy generation mixed-id mirror"
	appendLine(t, path, `{"type":"event_msg","payload":{"id":"mirror-event","type":"agent_message","phase":"final_answer","message":"`+answer+`"}}`)
	appendLine(t, path, `{"type":"response_item","thread_id":"`+threadID+`","payload":{"type":"message","role":"assistant","phase":"final_answer","turn":{"id":"mirror-turn"},"content":[{"type":"output_text","text":"`+answer+`"}]}}`)

	err = bridge.syncLinkedTranscripts(ctx)
	if err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync legacy-generation mixed-ID mirror: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load legacy-generation state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[checkpointID]
	if checkpoint.TranscriptQuarantine == nil || checkpoint.PendingHistoryRange == nil {
		t.Fatalf("checkpoint = %#v, want history-only quarantine and bounded pending range", checkpoint)
	}
	if checkpoint.TranscriptQuarantine.Kind != "mixed_id_final_mirror" && checkpoint.TranscriptQuarantine.Kind != "anonymous_final_group" {
		t.Fatalf("checkpoint quarantine kind = %q, want a final-mirror history quarantine", checkpoint.TranscriptQuarantine.Kind)
	}
	if checkpoint.PendingHistoryRange.RangeFingerprint == "" || checkpoint.PendingHistoryRange.ExclusiveEnd <= checkpoint.PendingHistoryRange.StartOffset {
		t.Fatalf("pending history range = %#v, want a bounded fingerprinted range", checkpoint.PendingHistoryRange)
	}
	if checkpoint.Status == importCheckpointStatusBlocked || checkpoint.UnresolvedExecution != nil {
		t.Fatalf("checkpoint = %#v, want no execution/whole-chat block", checkpoint)
	}
	if strings.TrimSpace(checkpoint.SourceGeneration) == "" || checkpoint.PendingHistoryRange.SourceGeneration != checkpoint.SourceGeneration {
		t.Fatalf("checkpoint generation = %q, pending range generation = %q; want a consistent source proof", checkpoint.SourceGeneration, checkpoint.PendingHistoryRange.SourceGeneration)
	}
	if sentPlainContains(*sent, answer) {
		t.Fatalf("ambiguous mixed-ID mirror was published: %#v", *sent)
	}
	if sentPlainContains(*sent, "old answer") {
		t.Fatalf("legacy checkpoint caused the consumed prefix to be replayed: %#v", *sent)
	}
}
