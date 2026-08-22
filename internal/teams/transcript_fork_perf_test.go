package teams

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestBridgeSQLiteLinkedTranscriptChangedTailExercisesParentForkGuard(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		sqliteMode := sqliteMode
		t.Run(map[bool]string{false: "json", true: "sqlite"}[sqliteMode], func(t *testing.T) {
			ctx := context.Background()
			transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
			threadID := "thread-parent-fork-hot-path"
			initial := `{"type":"session_meta","payload":{"id":"thread-parent-fork-hot-path"}}` + "\n" +
				`{"id":"old","thread_id":"thread-parent-fork-hot-path","role":"assistant","text":"old answer"}` + "\n"
			if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
				t.Fatalf("write initial transcript: %v", err)
			}
			restoreDiscover := stubDiscoverCodexSession(t, threadID, transcriptPath)
			defer restoreDiscover()

			graph, sent := newBridgeTestGraph(t)
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			session := seedLinkedTranscriptForTest(t, bridge, transcriptPath, threadID)
			if sqliteMode {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			internalTail := `{"type":"response_item","payload":{"id":"internal-tail","type":"agent_message","author":"/root/child","recipient":"/root","content":[{"type":"input_text","text":"Message Type: MESSAGE\nPayload: private"}],"internal_chat_message_metadata_passthrough":{"turn_id":"child-turn"}}}` + "\n"
			if err := os.WriteFile(transcriptPath, []byte(initial+internalTail), 0o600); err != nil {
				t.Fatalf("append internal transcript tail: %v", err)
			}
			if err := bridge.syncLinkedTranscripts(ctx); err != nil {
				t.Fatalf("sync changed linked transcript tail: %v", err)
			}
			if len(*sent) != 0 {
				t.Fatalf("internal tail reached Teams: %#v", *sent)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, transcriptCheckpointID(session.ID))
			if err != nil {
				t.Fatalf("read changed-tail checkpoint: %v", err)
			}
			if !found || !strings.HasPrefix(checkpoint.LastRecordID, "ignored:") || checkpoint.Status != importCheckpointStatusComplete {
				t.Fatalf("changed-tail checkpoint = %#v found=%v, want ignored disposition at EOF", checkpoint, found)
			}
		})
	}
}

func TestBridgeSQLiteBackgroundTranscriptSyncStillRespectsParentForkFence(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		sqliteMode := sqliteMode
		t.Run(map[bool]string{false: "json", true: "sqlite"}[sqliteMode], func(t *testing.T) {
			ctx := context.Background()
			transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
			threadID := "thread-active-parent-fork"
			initial := `{"type":"session_meta","payload":{"id":"thread-active-parent-fork"}}` + "\n" +
				`{"id":"old","thread_id":"thread-active-parent-fork","role":"assistant","text":"old answer"}` + "\n"
			if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
				t.Fatalf("write initial transcript: %v", err)
			}
			restoreDiscover := stubDiscoverCodexSession(t, threadID, transcriptPath)
			defer restoreDiscover()

			graph, sent := newBridgeTestGraph(t)
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			parent := seedLinkedTranscriptForTest(t, bridge, transcriptPath, threadID)
			if err := store.Update(ctx, func(state *teamstore.State) error {
				cutoffAt := time.Now().Add(-time.Minute)
				state.Turns["active-fork-cutoff"] = teamstore.Turn{
					ID:          "active-fork-cutoff",
					SessionID:   parent.ID,
					CodexTurnID: "active-fork-codex-cutoff",
					Status:      teamstore.TurnStatusCompleted,
					CompletedAt: cutoffAt,
					CreatedAt:   cutoffAt,
					UpdatedAt:   cutoffAt,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed fork cutoff: %v", err)
			}
			if _, created, err := store.BeginFork(ctx, teamstore.ForkBeginRequest{
				OperationID:       "active-fork-operation",
				ParentSessionID:   parent.ID,
				ParentChatID:      parent.ChatID,
				ParentThreadID:    parent.CodexThreadID,
				ChildSession:      teamstore.SessionContext{ID: "active-fork-child", Status: teamstore.SessionStatusStaging},
				CutoffTurnID:      "active-fork-cutoff",
				CutoffCodexTurnID: "active-fork-codex-cutoff",
			}); err != nil || !created {
				t.Fatalf("BeginFork created=%v err=%v", created, err)
			}
			if sqliteMode {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			before, err := store.TranscriptImportStateSnapshot(ctx)
			if err != nil {
				t.Fatalf("read transcript snapshot before fenced sync: %v", err)
			}
			beforePublication := struct {
				Checkpoints map[string]teamstore.ImportCheckpoint
				Deliveries  map[string]teamstore.TranscriptDeliveryRecord
				Ledger      map[string]teamstore.TranscriptLedgerRecord
				Outbox      map[string]teamstore.OutboxMessage
				Helper      map[string]teamstore.HelperDeliveryRecord
			}{before.ImportCheckpoints, before.TranscriptDeliveries, before.TranscriptLedger, before.OutboxMessages, before.HelperDeliveries}

			updated := initial + `{"id":"visible-after-fork","thread_id":"thread-active-parent-fork","role":"assistant","text":"must not be published after fork fence"}` + "\n"
			if err := os.WriteFile(transcriptPath, []byte(updated), 0o600); err != nil {
				t.Fatalf("append visible transcript tail: %v", err)
			}
			if err := bridge.syncLinkedTranscripts(ctx); err != nil {
				t.Fatalf("fenced background sync: %v", err)
			}
			if len(*sent) != 0 {
				t.Fatalf("fenced sync sent visible transcript tail: %#v", *sent)
			}

			after, err := store.TranscriptImportStateSnapshot(ctx)
			if err != nil {
				t.Fatalf("read transcript snapshot after fenced sync: %v", err)
			}
			afterPublication := struct {
				Checkpoints map[string]teamstore.ImportCheckpoint
				Deliveries  map[string]teamstore.TranscriptDeliveryRecord
				Ledger      map[string]teamstore.TranscriptLedgerRecord
				Outbox      map[string]teamstore.OutboxMessage
				Helper      map[string]teamstore.HelperDeliveryRecord
			}{after.ImportCheckpoints, after.TranscriptDeliveries, after.TranscriptLedger, after.OutboxMessages, after.HelperDeliveries}
			if !reflect.DeepEqual(afterPublication, beforePublication) {
				t.Fatalf("fenced sync changed durable publication state:\nbefore=%#v\nafter=%#v", beforePublication, afterPublication)
			}
		})
	}
}

func BenchmarkCXPPerfModelSQLiteLinkedTranscriptChangedTailParentFork(b *testing.B) {
	for _, changed := range []struct {
		name  string
		count int
	}{
		{name: "one-changed-session", count: 1},
		{name: "all-32-changed-sessions", count: 32},
	} {
		changed := changed
		b.Run(changed.name, func(b *testing.B) {
			store, bridge, paths := newChangedLinkedTranscriptPerfFixture(b)
			if changed.count > len(paths) {
				b.Fatalf("changed session count %d exceeds fixture paths %d", changed.count, len(paths))
			}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for sessionIndex := 0; sessionIndex < changed.count; sessionIndex++ {
					appendLinkedTranscriptPerfTail(b, paths[sessionIndex], i, sessionIndex)
				}
				b.StartTimer()
				bridge.lastTranscriptSync = time.Time{}
				if err := bridge.syncLinkedTranscripts(ctx); err != nil {
					b.Fatalf("sync changed linked transcript tail: %v", err)
				}
			}
			_ = store
		})
	}
}

func newChangedLinkedTranscriptPerfFixture(tb testing.TB) (*teamstore.Store, *Bridge, []string) {
	tb.Helper()
	profile := cxpPerfProfile{
		Name:            "linked-transcript-parent-fork-hot-path",
		WorkChats:       32,
		TurnsPerChat:    4,
		MessageBytes:    96,
		HistoryFiles:    32,
		HistoryLines:    20,
		OutboxPerChat:   1,
		LookupPerCycle:  0,
		MessagesPerPoll: 0,
	}
	store := newCXPPerfStore(tb, profile)
	cxpPerfSeedColdRuntimeMetadata(tb, store, profile)
	if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		tb.Fatalf("migrate changed-tail perf store: %v", err)
	}
	bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
	cxpPerfSeedLinkedTranscriptFiles(tb, store, bridge, profile)
	state, err := store.TranscriptImportStateSnapshot(context.Background())
	if err != nil {
		tb.Fatalf("read changed-tail perf checkpoints: %v", err)
	}
	paths := make([]string, 0, profile.WorkChats)
	for i := 0; i < profile.WorkChats; i++ {
		checkpoint := state.ImportCheckpoints[transcriptCheckpointID(cxpPerfSessionID(i))]
		if checkpoint.SourcePath == "" {
			tb.Fatalf("missing changed-tail perf checkpoint for session %d: %#v", i, checkpoint)
		}
		paths = append(paths, checkpoint.SourcePath)
	}
	return store, bridge, paths
}

func appendLinkedTranscriptPerfTail(tb testing.TB, path string, iteration int, sessionIndex int) {
	tb.Helper()
	line := fmt.Sprintf(`{"id":"perf-linked-tail-%06d-%02d","role":"assistant","text":"changed linked transcript tail %06d/%02d"}`+"\n", iteration, sessionIndex, iteration, sessionIndex)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		tb.Fatalf("open changed-tail transcript %s: %v", path, err)
	}
	if _, err := file.WriteString(line); err != nil {
		_ = file.Close()
		tb.Fatalf("append changed-tail transcript %s: %v", path, err)
	}
	if err := file.Close(); err != nil {
		tb.Fatalf("close changed-tail transcript %s: %v", path, err)
	}
}
