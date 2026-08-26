package teams

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// TestBridgeSyncLinkedTranscriptReleasesPendingRootAcrossPolls models a
// rollout-JSONL poll race in which task_started and turn_context become
// visible before the response_item containing the external prompt. The first
// poll must not create an execution fence or a user-visible history gate; the
// later poll must publish the new root final exactly once.
func TestBridgeSyncLinkedTranscriptReleasesPendingRootAcrossPolls(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	threadID := "thread-pending-root-across-polls"
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-pending-root-across-polls"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","phase":"final_answer","message":"outer answer"}}`,
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
	appendLine(t, path, `{"type":"response_item","thread_id":"thread-pending-root-across-polls","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"boundary Teams request"}]}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","thread_id":"thread-pending-root-across-polls","turn_id":"boundary-turn","started_at":1786181088,"model_context_window":258400,"collaboration_mode_kind":"default"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","id":"boundary-final","thread_id":"thread-pending-root-across-polls","turn_id":"boundary-turn","phase":"final_answer","message":"boundary answer"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_complete","thread_id":"thread-pending-root-across-polls","turn_id":"boundary-turn"}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync terminal boundary: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load terminal boundary state: %v", err)
	}
	boundaryCheckpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if !boundaryCheckpoint.TerminalBoundarySeen || boundaryCheckpoint.TerminalBoundary == nil || boundaryCheckpoint.TranscriptQuarantine != nil {
		t.Fatalf("seeded terminal state was not cleanly established: %#v", boundaryCheckpoint)
	}
	seedTranscriptTeamsTurnForTest(t, store, session, "new-root")
	*sent = nil

	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","thread_id":"thread-pending-root-across-polls","turn_id":"new-root","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"turn_context","thread_id":"thread-pending-root-across-polls","turn_id":"new-root"}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync after task-marker snapshot: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load pending-root state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.UnresolvedExecution != nil || checkpoint.Status == importCheckpointStatusBlocked {
		t.Fatalf("task-marker snapshot created an execution/history block: %#v", checkpoint)
	}
	if checkpoint.LastOffset <= boundaryCheckpoint.LastOffset || strings.TrimSpace(checkpoint.LastRecordID) == "" {
		t.Fatalf("task-marker snapshot did not persist its safe physical boundary: before=%#v after=%#v", boundaryCheckpoint, checkpoint)
	}
	if sentPlainContains(*sent, "previous Codex execution is still unconfirmed") || sentPlainContains(*sent, "helper publish-history") {
		t.Fatalf("task-marker snapshot emitted a user-visible gate: %#v", *sent)
	}
	beforeAutomaticImport := len(*sent)
	if err := bridge.importCodexTranscriptToTeamsWithOptions(context.Background(), *session, codexhistory.Session{SessionID: threadID, FilePath: path}, transcriptImportRunOptions{}); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("automatic import preflight at pending root: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if len(*sent) != beforeAutomaticImport {
		t.Fatalf("automatic import queued visible status at pending root: before=%d after=%#v", beforeAutomaticImport, *sent)
	}

	appendLine(t, path, `{"type":"response_item","thread_id":"thread-pending-root-across-polls","payload":{"id":"new-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"new explicit Teams request"}]}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync after prompt-only snapshot: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load prompt-only state: %v", err)
	}
	checkpoint = state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.UnresolvedExecution != nil || checkpoint.TranscriptQuarantine != nil {
		t.Fatalf("prompt-only snapshot retained an execution/history quarantine: %#v", checkpoint)
	}
	if sentPlainContains(*sent, "new root answer after prompt") {
		t.Fatalf("prompt-only snapshot published a final that was not written yet: %#v", *sent)
	}

	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","id":"new-final","turn_id":"new-root","phase":"final_answer","message":"new root answer after prompt"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"new-root"}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync after final: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if got := countSentPlainContaining(*sent, "new root answer after prompt"); got != 1 {
		t.Fatalf("new root final deliveries = %d, want exactly one: %#v", got, *sent)
	}
	if sentPlainContains(*sent, "previous Codex execution is still unconfirmed") || sentPlainContains(*sent, "helper publish-history") {
		t.Fatalf("normal root recovery emitted a history/ownership gate: %#v", *sent)
	}

	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load recovered root state: %v", err)
	}
	checkpoint = state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.TranscriptQuarantine != nil {
		t.Fatalf("recovered root retained transcript quarantine: %#v", checkpoint.TranscriptQuarantine)
	}
	if checkpoint.UnresolvedExecution != nil || checkpoint.TranscriptQuarantine != nil || checkpoint.PendingHistoryRange != nil || checkpoint.LastOffset != checkpoint.SourceSize {
		t.Fatalf("recovered root checkpoint = %#v, want clean EOF checkpoint", checkpoint)
	}
	assertSingleSentTranscriptFinal(t, state, session.ID, path, "new root answer after prompt", "new-root")

	*sent = nil
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("repeat recovered root sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if sentPlainContains(*sent, "new root answer after prompt") {
		t.Fatalf("recovered root final replayed on an unchanged scan: %#v", *sent)
	}
}

// TestBridgeSyncLinkedTranscriptReleasesPendingRootAcrossSQLiteStoreReopen
// repeats the same rollout-JSONL poll race across the JSON-to-SQLite/reopen
// boundary. A service restart must not turn a provisional task marker into a
// permanent history gate or lose the later root final.
func TestBridgeSyncLinkedTranscriptReleasesPendingRootAcrossSQLiteStoreReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	threadID := "thread-pending-root-after-restart"
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-pending-root-after-restart"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","phase":"final_answer","message":"outer answer"}}`,
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
	appendLine(t, path, `{"type":"response_item","thread_id":"thread-pending-root-after-restart","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"boundary Teams request"}]}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","thread_id":"thread-pending-root-after-restart","turn_id":"boundary-turn","started_at":1786181088,"model_context_window":258400,"collaboration_mode_kind":"default"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","id":"boundary-final","turn_id":"boundary-turn","phase":"final_answer","message":"boundary answer"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"boundary-turn"}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync terminal boundary before restart: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load terminal boundary before restart: %v", err)
	}
	boundaryCheckpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if !boundaryCheckpoint.TerminalBoundarySeen || boundaryCheckpoint.TerminalBoundary == nil || boundaryCheckpoint.TranscriptQuarantine != nil {
		t.Fatalf("seeded terminal state before restart was not cleanly established: %#v", boundaryCheckpoint)
	}
	seedTranscriptTeamsTurnForTest(t, store, session, "new-root")
	*sent = nil

	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","thread_id":"thread-pending-root-after-restart","turn_id":"new-root","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"turn_context","thread_id":"thread-pending-root-after-restart","turn_id":"new-root"}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync before restart: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load marker-only state before restart: %v", err)
	}
	markerCheckpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if markerCheckpoint.LastOffset <= boundaryCheckpoint.LastOffset || strings.TrimSpace(markerCheckpoint.LastRecordID) == "" {
		t.Fatalf("marker-only snapshot did not persist its safe physical boundary before restart: before=%#v after=%#v", boundaryCheckpoint, markerCheckpoint)
	}

	if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		t.Fatalf("migrate state before restart: %v", err)
	}
	storePath := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close pre-restart store: %v", err)
	}
	restartedStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("reopen state after restart: %v", err)
	}
	defer restartedStore.Close()
	restartedGraph, restartedSent := newBridgeTestGraph(t)
	restarted := newBridgeTestBridge(restartedGraph, restartedStore, &recordingExecutor{})
	restarted.registryPath = filepath.Join(t.TempDir(), "restarted-registry.json")
	if err := restarted.restoreRegistryFromStore(context.Background()); err != nil {
		t.Fatalf("restore registry after restart: %v", err)
	}
	resumedSession := restarted.reg.SessionByID(session.ID)
	if resumedSession == nil {
		t.Fatalf("restarted registry missing session %s", session.ID)
	}

	appendLine(t, path, `{"type":"response_item","thread_id":"thread-pending-root-after-restart","payload":{"id":"new-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"new explicit Teams request after restart"}]}}`)
	if err := restarted.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync after prompt after restart: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, restarted)
	state, err = restartedStore.Load(context.Background())
	if err != nil {
		t.Fatalf("load prompt-only post-restart state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.UnresolvedExecution != nil || checkpoint.TranscriptQuarantine != nil {
		t.Fatalf("prompt-only post-restart snapshot retained a quarantine: %#v", checkpoint)
	}
	if sentPlainContains(*restartedSent, "new root answer after restart") {
		t.Fatalf("prompt-only post-restart snapshot published an unwritten final: %#v", *restartedSent)
	}

	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","id":"new-final","turn_id":"new-root","phase":"final_answer","message":"new root answer after restart"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"new-root"}}`)
	if err := restarted.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync final after restart: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, restarted)
	if got := countSentPlainContaining(*restartedSent, "new root answer after restart"); got != 1 {
		state, _ := restartedStore.Load(context.Background())
		t.Fatalf("post-restart root final deliveries = %d, want exactly one; checkpoint=%#v sent=%#v", got, state.ImportCheckpoints[transcriptCheckpointID(session.ID)], *restartedSent)
	}
	state, err = restartedStore.Load(context.Background())
	if err != nil {
		t.Fatalf("load post-restart state: %v", err)
	}
	checkpoint = state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.UnresolvedExecution != nil || checkpoint.TranscriptQuarantine != nil || checkpoint.PendingHistoryRange != nil || checkpoint.LastOffset != checkpoint.SourceSize {
		t.Fatalf("post-restart checkpoint = %#v, want clean EOF checkpoint", checkpoint)
	}
	assertSingleSentTranscriptFinal(t, state, resumedSession.ID, path, "new root answer after restart", "new-root")
}

// TestBridgeSyncLinkedTranscriptReleasesPendingRootSeenInOnePoll covers the
// other writer timing: task_started, the external prompt, and the final are
// all present before the watcher opens the file.  The witness must not be
// mistaken for a previously persisted frontier (there is no pending-range
// row yet), and the next poll must deliver the final exactly once.
func TestBridgeSyncLinkedTranscriptReleasesPendingRootSeenInOnePoll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	threadID := "thread-pending-root-one-poll"
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-pending-root-one-poll"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","thread_id":"thread-pending-root-one-poll","turn_id":"outer-turn","type":"agent_message","phase":"final_answer","message":"outer answer"}}`,
		`{"type":"event_msg","payload":{"thread_id":"thread-pending-root-one-poll","turn_id":"outer-turn","type":"task_complete"}}`,
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
	seedTranscriptTeamsTurnForTest(t, store, session, "same-snapshot-root")
	*sent = nil

	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","thread_id":"thread-pending-root-one-poll","turn_id":"same-snapshot-root","started_at":1786181089,"model_context_window":258400,"collaboration_mode_kind":"default"}}`)
	appendLine(t, path, `{"type":"response_item","thread_id":"thread-pending-root-one-poll","payload":{"id":"same-snapshot-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"same snapshot Teams request"}]}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","id":"same-snapshot-final","thread_id":"thread-pending-root-one-poll","turn_id":"same-snapshot-root","phase":"final_answer","message":"same snapshot answer"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"task_complete","thread_id":"thread-pending-root-one-poll","turn_id":"same-snapshot-root"}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("same-snapshot sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if got := countSentPlainContaining(*sent, "same snapshot answer"); got != 0 {
		t.Fatalf("same-snapshot first poll delivered final = %d, want held for rescan: %#v", got, *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load same-snapshot release state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if !checkpoint.HistoryRootReleased || checkpoint.TranscriptQuarantine != nil || checkpoint.PendingHistoryRange != nil {
		t.Fatalf("same-snapshot release checkpoint = %#v, want semantic frontier cleared with rescan marker", checkpoint)
	}

	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("same-snapshot rescan: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if got := countSentPlainContaining(*sent, "same snapshot answer"); got != 1 {
		t.Fatalf("same-snapshot rescan deliveries = %d, want exactly one: %#v", got, *sent)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load same-snapshot final state: %v", err)
	}
	checkpoint = state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.TranscriptQuarantine != nil || checkpoint.PendingHistoryRange != nil || checkpoint.LastOffset != checkpoint.SourceSize {
		t.Fatalf("same-snapshot final checkpoint = %#v, want clean EOF checkpoint", checkpoint)
	}
	assertSingleSentTranscriptFinal(t, state, session.ID, path, "same snapshot answer", "same-snapshot-root")
}

func seedTranscriptTeamsTurnForTest(t *testing.T, store *teamstore.Store, session *Session, codexTurnID string) {
	t.Helper()
	inboundID := "inbound:transcript-root:" + session.ID + ":" + codexTurnID
	turnID := "turn:transcript-root:" + session.ID + ":" + codexTurnID
	now := time.Now()
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		if state.InboundEvents == nil {
			state.InboundEvents = map[string]teamstore.InboundEvent{}
		}
		if state.Turns == nil {
			state.Turns = map[string]teamstore.Turn{}
		}
		state.InboundEvents[inboundID] = teamstore.InboundEvent{
			ID:             inboundID,
			SessionID:      session.ID,
			TeamsChatID:    session.ChatID,
			TeamsMessageID: "teams-message:" + codexTurnID,
			Source:         "teams",
			Status:         teamstore.InboundStatusPersisted,
			TurnID:         turnID,
			CreatedAt:      now,
			ReceivedAt:     now,
		}
		state.Turns[turnID] = teamstore.Turn{
			ID:             turnID,
			SessionID:      session.ID,
			InboundEventID: inboundID,
			Status:         teamstore.TurnStatusCompleted,
			CodexThreadID:  session.CodexThreadID,
			CodexTurnID:    codexTurnID,
			CreatedAt:      now,
			StartedAt:      now,
			CompletedAt:    now,
			UpdatedAt:      now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed transcript Teams turn: %v", err)
	}
}

// TestExplicitHistoryRecoveryUsesStoredPendingRange verifies the crossed-
// frontier recovery contract directly. Automatic polling has left a synthetic
// ignored checkpoint at the old physical cursor and persisted a bounded range;
// publish-history must seek to that range, not look up the synthetic key and
// not replay the already-published prefix.
func TestExplicitHistoryRecoveryUsesStoredPendingRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-explicit-frontier"}}`,
		`{"id":"old-answer","role":"assistant","text":"already published prefix"}`,
	}, "\n") + "\n"
	marker := `{"type":"token_count","info":{"total_tokens":7}}` + "\n"
	suffix := strings.Join([]string{
		`{"id":"suffix-prompt","role":"user","text":"recovered prompt"}`,
		`{"id":"suffix-final","role":"assistant","text":"recovered suffix answer"}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(prefix+marker+suffix), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-explicit-frontier", path)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, "thread-explicit-frontier")
	*sent = nil

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load seeded state: %v", err)
	}
	checkpointID := transcriptCheckpointID(session.ID)
	checkpoint := state.ImportCheckpoints[checkpointID]
	markerStart := int64(len(prefix))
	markerEnd := markerStart + int64(len(marker))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat transcript: %v", err)
	}
	generation := historyTieredSourceIdentity(path, info)
	checkpoint.LastRecordID = "ignored:synthetic-frontier"
	checkpoint.LastSourceLine = 2
	checkpoint.LastOffset = markerStart
	checkpoint.LastOffsetKnown = true
	checkpoint.SourceSize = markerStart
	checkpoint.SourceModTime = info.ModTime()
	checkpoint.SourceGeneration = generation
	checkpoint.PendingHistoryRange = &teamstore.HistoryPendingRange{
		SourcePath:       path,
		SourceGeneration: generation,
		RangeID:          "gap:explicit-frontier",
		Kind:             "opaque_history_frontier",
		StartLine:        3,
		StartOffset:      markerStart,
		StartOffsetKnown: true,
		ExclusiveEnd:     markerEnd,
		RangeFingerprint: transcriptSourceRangeFingerprint(path, markerStart, markerEnd),
	}
	checkpoint.TranscriptQuarantine = &teamstore.TranscriptQuarantine{
		Kind:             "opaque_history_frontier",
		SourcePath:       path,
		SourceGeneration: generation,
		FrontierOffset:   markerStart,
	}
	checkpoint.RecoveryProofUnusable = true
	if err := store.UpdateSession(context.Background(), session.ID, func(state *teamstore.State) error {
		state.ImportCheckpoints[checkpointID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("seed pending frontier: %v", err)
	}

	if err := bridge.publishWorkSessionHistory(context.Background(), session); err != nil {
		t.Fatalf("explicit history recovery: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if strings.Count(joined, "recovered suffix answer") != 1 {
		t.Fatalf("explicit recovery output = %q, want one suffix answer", joined)
	}
	if strings.Contains(joined, "already published prefix") || strings.Contains(joined, "Local Codex history sync needs attention") {
		t.Fatalf("explicit recovery replayed the prefix or emitted a stale gate: %q", joined)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load recovered state: %v", err)
	}
	checkpoint = state.ImportCheckpoints[checkpointID]
	if checkpoint.PendingHistoryRange != nil || checkpoint.TranscriptQuarantine != nil || checkpoint.RecoveryProofUnusable || checkpoint.Status != importCheckpointStatusComplete || checkpoint.LastOffset != info.Size() {
		t.Fatalf("recovered checkpoint = %#v, want clean EOF state", checkpoint)
	}
}

// TestBridgeSyncLinkedTranscriptKeepsLiveTeamsTurnUsableAfterQuarantineRestart
// verifies the liveness contract for a history-only ambiguity: an ambiguous
// mirror remains hidden, but a normal Teams request after a service restart
// must still execute on the existing thread without a publish-history gate.
// The history frontier itself remains durable until the operator selects an
// explicit recovery boundary; live execution and automatic history import are
// intentionally separate state machines.
func TestBridgeSyncLinkedTranscriptKeepsLiveTeamsTurnUsableAfterQuarantineRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	threadID := "thread-quarantine-new-root"
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-quarantine-new-root"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","phase":"final_answer","message":"outer answer"}}`,
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
	if state, err := store.Load(context.Background()); err != nil {
		t.Fatalf("load seeded state before quarantine: %v", err)
	} else {
		boundaryCheckpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
		if boundaryCheckpoint.TranscriptQuarantine != nil {
			t.Fatalf("seeded state before quarantine was not clean: %#v", boundaryCheckpoint)
		}
	}

	appendLine(t, path, `{"type":"event_msg","payload":{"id":"mixed-event","type":"agent_message","phase":"final_answer","message":"ambiguous mirror answer must stay hidden"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"token_count","info":{"total_tokens":1}}}`)
	appendLine(t, path, `{"type":"response_item","thread_id":"thread-quarantine-new-root","payload":{"type":"message","role":"assistant","turn":{"id":"ambiguous-mirror-turn"},"phase":"final_answer","content":[{"type":"output_text","text":"ambiguous mirror answer must stay hidden"}]}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync ambiguous continuation: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load quarantined state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.TranscriptQuarantine == nil || checkpoint.UnresolvedExecution != nil {
		t.Fatalf("ambiguous continuation state = %#v, want history-only quarantine", checkpoint)
	}
	if sentPlainContains(*sent, "ambiguous mirror answer must stay hidden") || sentPlainContains(*sent, "helper publish-history") {
		t.Fatalf("ambiguous continuation leaked or emitted a gate: %#v", *sent)
	}

	if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		t.Fatalf("migrate quarantined state before restart: %v", err)
	}
	storePath := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close quarantined store before restart: %v", err)
	}
	restartedStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("reopen quarantined store: %v", err)
	}
	defer restartedStore.Close()
	restartedGraph, restartedSent := newBridgeTestGraph(t)
	restarted := newBridgeTestBridge(restartedGraph, restartedStore, &recordingExecutor{})
	resumedSession := restarted.reg.SessionByID(session.ID)
	if resumedSession == nil {
		t.Fatalf("restarted registry missing session %s", session.ID)
	}
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "live Teams answer after quarantine",
		CodexThreadID: threadID,
		CodexTurnID:   "live-turn",
	}}
	restarted.executor = executor
	if err := restarted.handleSessionMessage(context.Background(), resumedSession.ChatID, bridgePollMessage("new-normal-teams-request", "2026-08-26T03:00:00Z", "new normal Teams request"), "new normal Teams request"); err != nil {
		t.Fatalf("live Teams turn after quarantine restart: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, restarted)
	if len(executor.prompts) != 1 {
		t.Fatalf("live Teams prompts after quarantine restart = %#v, want one", executor.prompts)
	}
	if countSentPlainContaining(*restartedSent, "live Teams answer after quarantine") != 1 {
		t.Fatalf("live Teams answer after quarantine = %#v, want one", *restartedSent)
	}
	if sentPlainContains(*restartedSent, "ambiguous mirror answer must stay hidden") || sentPlainContains(*restartedSent, "previous Codex execution is still unconfirmed") || sentPlainContains(*restartedSent, "helper publish-history") {
		t.Fatalf("quarantine recovery leaked hidden history or emitted a gate: %#v", *restartedSent)
	}
	state, err = restartedStore.Load(context.Background())
	if err != nil {
		t.Fatalf("load post-quarantine state: %v", err)
	}
	checkpoint = state.ImportCheckpoints[transcriptCheckpointID(resumedSession.ID)]
	if checkpoint.TranscriptQuarantine == nil || checkpoint.UnresolvedExecution != nil || checkpoint.TranscriptQuarantine.LiveBranchThreadID != "" {
		t.Fatalf("live Teams turn changed history-only quarantine into a runtime/live branch state: %#v", checkpoint)
	}
	latestTurn := state.Turns[state.Sessions[resumedSession.ID].LatestTurnID]
	if latestTurn.Status != teamstore.TurnStatusCompleted {
		t.Fatalf("live Teams turn after quarantine status = %q, want completed: %#v", latestTurn.Status, latestTurn)
	}
}

// TestBridgeSyncLinkedTranscriptRecordsSafePrefixDeliveryBeforeHistoryQuarantine
// covers one poll that contains a deliverable final followed by an ambiguous
// mirror group. The safe prefix must be sent as a durable source-bound delivery,
// while the ambiguous suffix remains silent and quarantined; a later poll must
// not replay the safe prefix.
func TestBridgeSyncLinkedTranscriptRecordsSafePrefixDeliveryBeforeHistoryQuarantine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	threadID := "thread-safe-prefix-before-quarantine"
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-safe-prefix-before-quarantine"}}`,
		`{"type":"event_msg","payload":{"id":"outer-final","type":"agent_message","phase":"final_answer","message":"outer answer"}}`,
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

	appendLine(t, path, `{"type":"event_msg","payload":{"id":"safe-prefix-final","type":"agent_message","turn_id":"safe-turn","phase":"final_answer","message":"safe prefix answer"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"id":"ambiguous-event","type":"agent_message","phase":"final_answer","message":"ambiguous suffix must stay hidden"}}`)
	appendLine(t, path, `{"type":"event_msg","payload":{"type":"token_count","info":{"total_tokens":1}}}`)
	appendLine(t, path, `{"type":"response_item","thread_id":"thread-safe-prefix-before-quarantine","payload":{"type":"message","role":"assistant","turn":{"id":"ambiguous-turn"},"phase":"final_answer","content":[{"type":"output_text","text":"ambiguous suffix must stay hidden"}]}}`)
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) && !errors.Is(err, teamstore.ErrUnresolvedExecution) {
		t.Fatalf("sync safe prefix and ambiguous suffix: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if !sentPlainContains(*sent, "safe prefix answer") || sentPlainContains(*sent, "ambiguous suffix must stay hidden") || sentPlainContains(*sent, "helper publish-history") {
		t.Fatalf("safe-prefix/quarantine output = %#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load safe-prefix/quarantine state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.TranscriptQuarantine == nil || checkpoint.UnresolvedExecution != nil {
		t.Fatalf("safe-prefix/quarantine checkpoint = %#v, want silent history-only quarantine", checkpoint)
	}
	assertSingleSentTranscriptFinal(t, state, session.ID, path, "safe prefix answer", "safe-turn")
	if checkpoint.TranscriptQuarantine.SourcePath != path || checkpoint.TranscriptQuarantine.FrontierRecordID == "" {
		t.Fatalf("safe-prefix/quarantine frontier = %#v, want a source-bound suffix frontier", checkpoint.TranscriptQuarantine)
	}
	for _, delivery := range state.TranscriptDeliveries {
		if delivery.SessionID == session.ID && delivery.TextHash == normalizedTextHash("ambiguous suffix must stay hidden") {
			t.Fatalf("quarantined suffix created a delivery ledger row: %#v", delivery)
		}
	}
	for _, outbox := range state.OutboxMessages {
		if outbox.SessionID == session.ID && strings.Contains(outbox.Body, "ambiguous suffix must stay hidden") {
			t.Fatalf("quarantined suffix created an outbox row: %#v", outbox)
		}
	}

	*sent = nil
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("repeat safe-prefix/quarantine sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if sentPlainContains(*sent, "safe prefix answer") || sentPlainContains(*sent, "ambiguous suffix must stay hidden") {
		t.Fatalf("safe prefix or quarantined suffix replayed: %#v", *sent)
	}
}

func assertSingleSentTranscriptFinal(t *testing.T, state teamstore.State, sessionID, sourcePath, text, codexTurnID string) {
	t.Helper()
	hash := normalizedTextHash(text)
	var deliveries []teamstore.TranscriptDeliveryRecord
	for _, delivery := range state.TranscriptDeliveries {
		if delivery.SessionID == sessionID && delivery.TextHash == hash && delivery.Status == teamstore.TranscriptDeliveryStatusSent {
			deliveries = append(deliveries, delivery)
		}
	}
	if len(deliveries) != 1 {
		t.Fatalf("transcript final deliveries for %q = %#v, want exactly one sent record", text, deliveries)
	}
	delivery := deliveries[0]
	if delivery.CodexTurnID != codexTurnID || delivery.SourcePath != sourcePath || delivery.SourceLine <= 0 || delivery.SourceRecordID == "" || delivery.OutboxID == "" {
		t.Fatalf("transcript delivery provenance = %#v, want source=%q turn=%q", delivery, sourcePath, codexTurnID)
	}
	outbox, ok := state.OutboxMessages[delivery.OutboxID]
	if !ok || outbox.Status != teamstore.OutboxStatusSent || outbox.TranscriptSourcePath != sourcePath || !outbox.TranscriptSourceProofOffsetKnown || outbox.TranscriptSourceProofFingerprint == "" {
		t.Fatalf("transcript outbox provenance = %#v found=%v, want sent source-proofed row", outbox, ok)
	}
}
