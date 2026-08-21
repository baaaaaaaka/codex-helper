package teams

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestHistoryWatchRebasesPaginatedRolloutFromStableFinalWithoutDeliveryReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	content := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-rebase","history_mode":"paginated"}}`,
		`{"type":"response_item","payload":{"id":"final-1","type":"message","role":"assistant","turn_id":"turn-1","phase":"final_answer","content":[{"type":"output_text","text":"old answer"}]}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write paginated rollout: %v", err)
	}
	oldFinalID := "codex-final:v1:thread-rebase:turn-1:final-1"
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat rollout: %v", err)
	}
	store := newBridgeTestStore(t)
	id := historyWatchCheckpointID(path)
	if err := store.UpdateHistoryWatch(context.Background(), func(history map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
		history[id] = teamstore.HistoryWatchCheckpoint{
			ID:                   id,
			Path:                 path,
			Size:                 info.Size(),
			ModTime:              info.ModTime(),
			Offset:               info.Size(),
			SessionID:            "thread-rebase",
			ThreadID:             "thread-rebase",
			SourceRewriteBlocked: true,
			LastFinalID:          oldFinalID,
			LastFinalThreadID:    "thread-rebase",
			LastFinalTurnID:      "turn-1",
			LastFinalTextHash:    normalizedTextHash("old answer"),
			TerminalBoundarySeen: true,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed history checkpoint: %v", err)
	}

	bridge := &Bridge{store: store}
	if err := bridge.syncCodexHistoryWatchPath(context.Background(), path, time.Now()); err != nil {
		t.Fatalf("rebase history checkpoint: %v", err)
	}
	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("load rebased history checkpoint: %v", err)
	}
	checkpoint := state.HistoryWatch[id]
	anchorOffset := int64(strings.Index(content, `{"type":"event_msg"`))
	if anchorOffset <= 0 {
		t.Fatalf("find terminal suffix in fixture")
	}
	if checkpoint.SourceRewriteBlocked {
		t.Fatalf("rebased checkpoint remains blocked: %#v", checkpoint)
	}
	if checkpoint.LastFinalID != oldFinalID || checkpoint.LastFinalLine != 2 {
		t.Fatalf("rebased final boundary = %#v, want stable final at line 2", checkpoint)
	}
	if checkpoint.Offset != anchorOffset || checkpoint.Size != anchorOffset {
		t.Fatalf("rebased cursor = offset %d size %d, want %d", checkpoint.Offset, checkpoint.Size, anchorOffset)
	}
	if checkpoint.SourceFingerprint == "" {
		t.Fatalf("rebase did not establish source fingerprint")
	}
	if len(state.OutboxMessages) != 0 {
		t.Fatalf("rebase created delivery rows: %#v", state.OutboxMessages)
	}

	// The terminal suffix must be consumed on the next ordinary poll. In
	// particular, the rebase must not leave Size at the new file EOF while the
	// cursor is still at the old final.
	if err := bridge.syncCodexHistoryWatchPath(context.Background(), path, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("scan rebased suffix: %v", err)
	}
	state, err = store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("reload history checkpoint after suffix: %v", err)
	}
	checkpoint = state.HistoryWatch[id]
	if checkpoint.Offset != info.Size() || checkpoint.SourceRewriteBlocked {
		t.Fatalf("rebased suffix was not consumed: %#v", checkpoint)
	}
}

func TestHistoryWatchChangedPathsRechecksOnlyNewPaginatedIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "candidate-rollout.jsonl")
	content := `{"type":"session_meta","payload":{"id":"thread-candidate","history_mode":"paginated"}}` + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write candidate rollout: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat candidate rollout: %v", err)
	}
	id := historyWatchCheckpointID(path)
	state := teamstore.State{HistoryWatch: map[string]teamstore.HistoryWatchCheckpoint{
		id: {
			ID:                   id,
			Path:                 path,
			Size:                 info.Size(),
			ModTime:              info.ModTime(),
			ThreadID:             "thread-candidate",
			SourceRewriteBlocked: true,
		},
	}}
	changes, err := historyWatchChangedPaths([]string{path}, state, false)
	if err != nil {
		t.Fatalf("historyWatchChangedPaths: %v", err)
	}
	if len(changes) != 1 || cleanComparablePath(changes[0]) != cleanComparablePath(path) {
		t.Fatalf("new paginated identity changes = %#v, want %q", changes, path)
	}
	identity, ok := codexPaginatedHistoryIdentity(path, "thread-candidate")
	if !ok || identity == "" {
		t.Fatalf("candidate rollout has no stable identity: %q %v", identity, ok)
	}
	state.HistoryWatch[id] = teamstore.HistoryWatchCheckpoint{
		ID:                            id,
		Path:                          path,
		Size:                          info.Size(),
		ModTime:                       info.ModTime(),
		ThreadID:                      "thread-candidate",
		SourceRewriteBlocked:          true,
		SourceRewriteRecoveryIdentity: identity,
	}
	changes, err = historyWatchChangedPaths([]string{path}, state, false)
	if err != nil {
		t.Fatalf("historyWatchChangedPaths after attempted identity: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("same paginated identity changes = %#v, want no repeated rebase scan", changes)
	}
}

func TestLinkedTranscriptRebasePreservesCompletionAndExecutionState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "linked-rollout.jsonl")
	content := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-import","history_mode":"paginated"}}`,
		`{"type":"response_item","payload":{"id":"last-record","type":"message","role":"user","turn_id":"turn-1","content":[{"type":"input_text","text":"prompt"}]}}`,
		`{"type":"response_item","payload":{"id":"new-record","type":"message","role":"assistant","turn_id":"turn-1","phase":"final_answer","content":[{"type":"output_text","text":"answer"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write linked rollout: %v", err)
	}
	store := newBridgeTestStore(t)
	checkpointID := transcriptCheckpointID("session-import")
	anchor := &teamstore.ExecutionAnchor{
		SessionID:      "session-import",
		ThreadID:       "thread-import",
		OuterTurnID:    "teams-turn",
		CodexTurnID:    "turn-1",
		CutoffRecordID: "last-record",
		Reason:         "test-preserve",
	}
	checkpoint := teamstore.ImportCheckpoint{
		ID:                   checkpointID,
		SessionID:            "session-import",
		SourcePath:           path,
		LastRecordID:         "source:last-record",
		LastOffsetKnown:      true,
		SourceRewriteBlocked: true,
		Status:               importCheckpointStatusBlocked,
		CompletionPending:    true,
		UnresolvedExecution:  anchor,
	}
	if _, _, err := store.UpdateImportCheckpoint(context.Background(), checkpointID, func(_ teamstore.ImportCheckpoint, _ bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
		checkpoint.UpdatedAt = now
		return checkpoint, true, nil
	}); err != nil {
		t.Fatalf("seed linked checkpoint: %v", err)
	}
	bridge := &Bridge{store: store}
	session := Session{ID: "session-import", CodexThreadID: "thread-import"}
	local := codexhistory.Session{SessionID: "thread-import", FilePath: path}
	if rebased, err := bridge.rebaseLinkedTranscriptSourceRewrite(context.Background(), session, local, checkpoint); err != nil {
		t.Fatalf("rebase linked checkpoint: %v", err)
	} else if !rebased {
		t.Fatal("linked checkpoint was not rebased")
	}
	updated, found, err := store.ImportCheckpoint(context.Background(), checkpointID)
	if err != nil || !found {
		t.Fatalf("load linked checkpoint: found=%v err=%v", found, err)
	}
	if updated.SourceRewriteBlocked || updated.SourceFingerprint == "" || !updated.LastOffsetKnown {
		t.Fatalf("linked checkpoint did not regain source proof: %#v", updated)
	}
	if updated.Status != importCheckpointStatusImporting || !updated.CompletionPending {
		t.Fatalf("completion recovery phase changed unexpectedly: %#v", updated)
	}
	if !reflect.DeepEqual(updated.UnresolvedExecution, anchor) {
		t.Fatalf("execution anchor changed during rebase: got=%#v want=%#v", updated.UnresolvedExecution, anchor)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load linked state: %v", err)
	}
	if len(state.OutboxMessages) != 0 {
		t.Fatalf("linked rebase created delivery rows: %#v", state.OutboxMessages)
	}
	if rebased, err := bridge.rebaseLinkedTranscriptSourceRewrite(context.Background(), session, local, updated); err != nil {
		t.Fatalf("repeat linked rebase: %v", err)
	} else if rebased {
		t.Fatal("repeat linked rebase should be a no-op")
	}
}

func TestSourceRewriteRebaseRecordsMissingAnchorOncePerFileIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-anchor.jsonl")
	content := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-missing","history_mode":"paginated"}}`,
		`{"type":"response_item","payload":{"id":"different","type":"message","role":"assistant","turn_id":"turn-1","phase":"final_answer","content":[{"type":"output_text","text":"other"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write missing-anchor rollout: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat missing-anchor rollout: %v", err)
	}
	store := newBridgeTestStore(t)
	id := historyWatchCheckpointID(path)
	if err := store.UpdateHistoryWatch(context.Background(), func(history map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
		history[id] = teamstore.HistoryWatchCheckpoint{
			ID:                   id,
			Path:                 path,
			Size:                 info.Size(),
			ModTime:              info.ModTime(),
			Offset:               info.Size(),
			SessionID:            "thread-missing",
			ThreadID:             "thread-missing",
			SourceRewriteBlocked: true,
			LastFinalID:          "codex-final:v1:thread-missing:turn-old:gone",
		}
		return nil
	}); err != nil {
		t.Fatalf("seed missing-anchor checkpoint: %v", err)
	}
	bridge := &Bridge{store: store}
	if err := bridge.syncCodexHistoryWatchPath(context.Background(), path, time.Now()); err != nil {
		t.Fatalf("first missing-anchor recovery: %v", err)
	}
	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("load missing-anchor checkpoint: %v", err)
	}
	first := state.HistoryWatch[id]
	if !first.SourceRewriteBlocked || first.SourceRewriteRecoveryIdentity == "" {
		t.Fatalf("missing anchor did not leave a durable retry fence: %#v", first)
	}
	if err := bridge.syncCodexHistoryWatchPath(context.Background(), path, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("repeat missing-anchor recovery: %v", err)
	}
	state, err = store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("reload missing-anchor checkpoint: %v", err)
	}
	second := state.HistoryWatch[id]
	if second.SourceRewriteRecoveryIdentity != first.SourceRewriteRecoveryIdentity || !second.SourceRewriteBlocked {
		t.Fatalf("repeat recovery changed durable fence: first=%#v second=%#v", first, second)
	}
}

func TestSourceRewriteRecoveryIdentityPersistsInSQLiteProjection(t *testing.T) {
	ctx := context.Background()
	store, err := teamstore.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	path := filepath.Join(t.TempDir(), "blocked.jsonl")
	checkpointID := historyWatchCheckpointID(path)
	if err := store.UpdateHistoryWatch(ctx, func(history map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
		history[checkpointID] = teamstore.HistoryWatchCheckpoint{
			ID:                            checkpointID,
			Path:                          path,
			SourceRewriteBlocked:          true,
			SourceRewriteRecoveryIdentity: "file:recovery-test",
		}
		return nil
	}); err != nil {
		t.Fatalf("seed history projection: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate store to SQLite: %v", err)
	}
	state, err := store.HistoryWatchState(ctx)
	if err != nil {
		t.Fatalf("load SQLite history projection: %v", err)
	}
	if got := state.HistoryWatch[checkpointID].SourceRewriteRecoveryIdentity; got != "file:recovery-test" {
		t.Fatalf("SQLite recovery identity = %q, want persisted marker", got)
	}
	importID := transcriptCheckpointID("sqlite-recovery")
	if _, _, err := store.UpdateImportCheckpoint(ctx, importID, func(_ teamstore.ImportCheckpoint, _ bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
		return teamstore.ImportCheckpoint{
			ID:                            importID,
			SessionID:                     "sqlite-recovery",
			SourceRewriteBlocked:          true,
			SourceRewriteRecoveryIdentity: "file:import-recovery-test",
			UpdatedAt:                     now,
		}, true, nil
	}); err != nil {
		t.Fatalf("write SQLite import marker: %v", err)
	}
	checkpoint, found, err := store.ImportCheckpoint(ctx, importID)
	if err != nil || !found {
		t.Fatalf("load SQLite import marker: found=%v err=%v", found, err)
	}
	if checkpoint.SourceRewriteRecoveryIdentity != "file:import-recovery-test" {
		t.Fatalf("SQLite import recovery identity = %q, want persisted marker", checkpoint.SourceRewriteRecoveryIdentity)
	}
}
