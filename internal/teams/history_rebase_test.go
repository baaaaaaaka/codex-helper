package teams

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestHistoryWatchRebaseScanYieldsAcrossLargeSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large-rebase-rollout.jsonl")
	paddingLine := `{"type":"noop","padding":"` + strings.Repeat("x", 128) + `"}` + "\n"
	filler := strings.Repeat(paddingLine, int(historyRebaseMaxScanBytesPerPass/int64(len(paddingLine)))+1024)
	content := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-large-rebase","history_mode":"paginated"}}`,
		strings.TrimSuffix(filler, "\n"),
		`{"type":"response_item","payload":{"id":"large-rebase-final","type":"message","role":"assistant","turn_id":"turn-large-rebase","phase":"final_answer","content":[{"type":"output_text","text":"large rebase answer"}]}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-large-rebase"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write large paginated rollout: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat large paginated rollout: %v", err)
	}
	if info.Size() <= historyRebaseMaxScanBytesPerPass {
		t.Fatalf("large rebase fixture size = %d, want greater than per-pass budget %d", info.Size(), historyRebaseMaxScanBytesPerPass)
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
			SessionID:            "thread-large-rebase",
			ThreadID:             "thread-large-rebase",
			SourceRewriteBlocked: true,
			LastFinalID:          "codex-final:v1:thread-large-rebase:turn-large-rebase:large-rebase-final",
			LastFinalThreadID:    "thread-large-rebase",
			LastFinalTurnID:      "turn-large-rebase",
			LastFinalTextHash:    normalizedTextHash("large rebase answer"),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed large rebase checkpoint: %v", err)
	}
	for attempt := 0; attempt < 8; attempt++ {
		// Recreate the bridge on every pass to prove the bounded rebase cursor is
		// durable, rather than relying on the process-local scan map.
		bridge := &Bridge{store: store}
		if err := bridge.syncCodexHistoryWatchPath(context.Background(), path, time.Now().Add(time.Duration(attempt+1)*time.Second)); err != nil {
			t.Fatalf("large rebase scan attempt %d: %v", attempt+1, err)
		}
		state, err := store.HistoryWatchState(context.Background())
		if err != nil {
			t.Fatalf("load large rebase checkpoint after attempt %d: %v", attempt+1, err)
		}
		if !state.HistoryWatch[id].SourceRewriteBlocked {
			checkpoint := state.HistoryWatch[id]
			if checkpoint.Offset <= 0 || checkpoint.Offset >= info.Size() {
				t.Fatalf("large rebase cursor after attempt %d = %#v, want anchor before EOF", attempt+1, checkpoint)
			}
			return
		}
	}
	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("load final large rebase checkpoint: %v", err)
	}
	t.Fatalf("large rebase did not complete in bounded passes: %#v", state.HistoryWatch[id])
}

func TestHistoryWatchRebaseScanHonorsCanceledContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "canceled-rebase-rollout.jsonl")
	if err := os.WriteFile(path, []byte(`{"type":"session_meta","payload":{"id":"thread-canceled-rebase","history_mode":"paginated"}}`+"\n"), 0o600); err != nil {
		t.Fatalf("write canceled rebase rollout: %v", err)
	}
	source, ok, err := codexPaginatedHistoryFile(path, "thread-canceled-rebase")
	if err != nil || !ok {
		t.Fatalf("load canceled rebase source: ok=%v err=%v", ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = historyWatchRebaseAnchorScan(ctx, path, historyTieredFileState{SessionID: "thread-canceled-rebase", ThreadID: "thread-canceled-rebase"}, source, historyWatchRebaseScanProgress{SourceIdentity: source.Identity})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled rebase scan error = %v, want context.Canceled", err)
	}
}

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
			SourceGeneration:     "old-source-generation",
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
	if checkpoint.SourceGeneration == "" || checkpoint.SourceGeneration == "old-source-generation" {
		t.Fatalf("rebase did not establish the new source generation: %#v", checkpoint)
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
		ID:                              id,
		Path:                            path,
		Size:                            info.Size(),
		ModTime:                         info.ModTime(),
		ThreadID:                        "thread-candidate",
		SourceRewriteBlocked:            true,
		SourceRewriteRecoveryIdentity:   identity,
		SourceRewriteRecoverySize:       info.Size(),
		SourceRewriteRecoveryModTime:    info.ModTime(),
		SourceRewriteRecoveryChangeTime: teamstore.SourceFileChangeTimeFromFileInfo(info),
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
		SourceGeneration:     "old-source-generation",
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
	if updated.SourceGeneration == "" || updated.SourceGeneration == "old-source-generation" {
		t.Fatalf("linked rebase did not establish the new source generation: %#v", updated)
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

func TestSourceRewriteRebaseRetriesAfterSameInodeRepair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same-inode-repair.jsonl")
	content := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-same-inode","history_mode":"paginated"}}`,
		`{"type":"response_item","payload":{"id":"old-final","type":"message","role":"assistant","turn_id":"turn-1","phase":"final_answer","content":[{"type":"output_text","text":"old answer"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write same-inode rollout: %v", err)
	}
	infoBefore, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat same-inode rollout: %v", err)
	}
	id := historyWatchCheckpointID(path)
	store := newBridgeTestStore(t)
	if err := store.UpdateHistoryWatch(context.Background(), func(history map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
		history[id] = teamstore.HistoryWatchCheckpoint{
			ID:                   id,
			Path:                 path,
			Size:                 infoBefore.Size(),
			ModTime:              infoBefore.ModTime(),
			Offset:               infoBefore.Size(),
			SessionID:            "thread-same-inode",
			ThreadID:             "thread-same-inode",
			SourceRewriteBlocked: true,
			LastFinalThreadID:    "thread-same-inode",
			LastFinalTurnID:      "turn-1",
			LastFinalTextHash:    normalizedTextHash("new answer"),
			TerminalBoundarySeen: true,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed same-inode checkpoint: %v", err)
	}
	bridge := &Bridge{store: store}
	if err := bridge.syncCodexHistoryWatchPath(context.Background(), path, time.Now()); err != nil {
		t.Fatalf("first same-inode recovery: %v", err)
	}
	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("load first same-inode checkpoint: %v", err)
	}
	first := state.HistoryWatch[id]
	if !first.SourceRewriteBlocked || first.SourceRewriteRecoveryIdentity == "" || first.SourceRewriteRecoverySize != infoBefore.Size() {
		t.Fatalf("first same-inode recovery did not persist the retry snapshot: %#v", first)
	}

	oldText := []byte("old answer")
	newText := []byte("new answer")
	if len(oldText) != len(newText) {
		t.Fatalf("same-inode fixture text lengths differ")
	}
	answerOffset := int64(strings.Index(content, string(oldText)))
	if answerOffset < 0 {
		t.Fatalf("find same-inode answer")
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open same-inode rollout for repair: %v", err)
	}
	if _, err := file.WriteAt(newText, answerOffset); err != nil {
		_ = file.Close()
		t.Fatalf("repair same-inode rollout: %v", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatalf("sync same-inode rollout repair: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close same-inode rollout repair: %v", err)
	}
	if err := os.Chtimes(path, infoBefore.ModTime().Add(time.Second), infoBefore.ModTime().Add(time.Second)); err != nil {
		t.Fatalf("mark same-inode rollout changed: %v", err)
	}
	infoAfter, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat repaired same-inode rollout: %v", err)
	}
	if !os.SameFile(infoBefore, infoAfter) || infoAfter.Size() != infoBefore.Size() {
		t.Fatalf("repair changed source identity or size: before=%#v after=%#v", infoBefore, infoAfter)
	}

	changed, err := historyWatchChangedPaths([]string{path}, state, false)
	if err != nil {
		t.Fatalf("detect same-inode repair: %v", err)
	}
	if len(changed) != 1 || cleanComparablePath(changed[0]) != cleanComparablePath(path) {
		t.Fatalf("same-inode repair was not scheduled for rebase: %#v", changed)
	}
	if err := bridge.syncCodexHistoryWatchPath(context.Background(), path, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("retry same-inode recovery: %v", err)
	}
	state, err = store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("load repaired same-inode checkpoint: %v", err)
	}
	updated := state.HistoryWatch[id]
	if updated.SourceRewriteBlocked || updated.SourceRewriteRecoveryIdentity != "" {
		t.Fatalf("same-inode repair did not clear the rebase fence: %#v", updated)
	}
	if updated.LastFinalTextHash != normalizedTextHash("new answer") || updated.Offset <= 0 {
		t.Fatalf("same-inode repair did not recover the expected final boundary: %#v", updated)
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
