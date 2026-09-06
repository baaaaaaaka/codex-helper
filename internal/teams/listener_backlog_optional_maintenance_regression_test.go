package teams

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// These vertical tests intentionally combine the two durable workloads that
// used to be tested in isolation: a queued Teams turn and a non-empty Codex
// history tail.  A listener must keep the optional history maintenance out of
// the cycle while the Teams queue is not drained.  Otherwise a large history
// reconciliation can consume the phase/SQLite budget and make the owner
// heartbeat lose its lease before the next Teams message is admitted.
func TestTeamsListenFalseBacklogSkipsOptionalHistoryMaintenanceJSON(t *testing.T) {
	runTeamsListenFalseBacklogSkipsOptionalHistoryMaintenance(t, false)
}

func TestTeamsListenFalseBacklogSkipsOptionalHistoryMaintenanceSQLite(t *testing.T) {
	runTeamsListenFalseBacklogSkipsOptionalHistoryMaintenance(t, true)
}

func runTeamsListenFalseBacklogSkipsOptionalHistoryMaintenance(t *testing.T, useSQLite bool) {
	t.Helper()
	ctx := context.Background()
	store := newBridgeTestStore(t)
	if err := store.Update(ctx, func(*teamstore.State) error { return nil }); err != nil {
		t.Fatalf("materialize listener backlog fixture: %v", err)
	}
	if useSQLite {
		if migration, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
			t.Fatalf("migrate listener backlog fixture to SQLite: %v", err)
		} else if !migration.Migrated && !migration.AlreadyDB {
			t.Fatalf("unexpected listener backlog migration result: %#v", migration)
		}
	}

	graphMessages := map[string][]ChatMessage{
		"chat-backlog-history": {
			backlogHistoryTestMessage("teams-backlog-1", "first durable backlog prompt"),
			backlogHistoryTestMessage("teams-backlog-2", "second durable backlog prompt"),
		},
	}
	graph, _ := newListenerRecoveryGraph(t, nil, graphMessages, 0)
	executor := &listenerBacklogCompletingExecutor{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.maxQueuedTurnStartsPerCycle = 1
	listenerOutput := &listenerBacklogOutput{buffer: &bytes.Buffer{}}
	bridge.out = listenerOutput

	// Keep the fixture's real-looking work chat separate from the control chat,
	// then leave a second turn queued behind the deliberately blocked first one.
	session := appendBridgeTestSession(t, bridge, store, "s-backlog-history", "chat-backlog-history")
	now := time.Now().UTC()
	queueBridgeTurnForTest(t, bridge, session, "teams-backlog-1", "first durable backlog prompt", now.Add(-2*time.Second))
	queueBridgeTurnForTest(t, bridge, session, "teams-backlog-2", "second durable backlog prompt", now.Add(-time.Second))
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now.Add(-time.Minute))
	listenerRecoverySeedDuePoll(t, store, session.ChatID, now.Add(-time.Minute))

	// A real Codex root with a checkpointed file plus a fresh tail makes the
	// history phase enter its worker instead of returning from an empty scan.
	root := newBridgeTestCodexRoot(t)
	path := filepath.Join(root, "sessions", now.Format("2006"), now.Format("01"), now.Format("02"), "backlog-history.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create history fixture directory: %v", err)
	}
	initial := listenerRecoveryTranscriptLine("backlog-history-initial", "initial history record")
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial history fixture: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat initial history fixture: %v", err)
	}
	checkpoint := listenerRecoveryHistoryCheckpoint(path, session.ID, "backlog-history-thread", info)
	if err := store.UpdateHistoryWatch(ctx, func(history map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		history[checkpoint.ID] = checkpoint
		*ready = now.Add(-time.Minute)
		return nil
	}); err != nil {
		t.Fatalf("seed history checkpoint: %v", err)
	}
	if err := appendListenerRecoveryTranscript(path, listenerRecoveryTranscriptFinalLine("backlog-history-tail", "history tail")); err != nil {
		t.Fatalf("append history fixture tail: %v", err)
	}
	bridge.scope.CodexHome = root
	bridge.lastHistoryWatchReconcile = now
	bridge.lastHistoryWatchSync = time.Time{}

	// A second linked session carries an explicit source-rewrite fence. It is
	// mandatory recovery and must remain eligible even while the Teams queue is
	// active; this keeps the test from proving the unsafe variant that simply
	// skips both maintenance phases.
	mandatorySession := appendBridgeTestSession(t, bridge, store, "s-backlog-mandatory-linked", "chat-backlog-mandatory-linked")
	mandatorySession.CodexThreadID = "thread-backlog-mandatory-linked"
	if err := store.UpdateSession(ctx, mandatorySession.ID, func(state *teamstore.State) error {
		current := state.Sessions[mandatorySession.ID]
		current.CodexThreadID = mandatorySession.CodexThreadID
		state.Sessions[mandatorySession.ID] = current
		return nil
	}); err != nil {
		t.Fatalf("persist mandatory linked session: %v", err)
	}
	mandatoryLinkedPath := filepath.Join(root, "sessions", now.Format("2006"), now.Format("01"), now.Format("02"), "backlog-mandatory-linked.jsonl")
	mandatoryLinkedContent := `{"type":"session_meta","payload":{"id":"thread-backlog-mandatory-linked","history_mode":"paginated"}}` + "\n" + listenerRecoveryTranscriptLine("mandatory-linked-root", "mandatory linked root")
	if err := os.WriteFile(mandatoryLinkedPath, []byte(mandatoryLinkedContent), 0o600); err != nil {
		t.Fatalf("write mandatory linked fixture: %v", err)
	}
	mandatoryLinkedInfo, err := os.Stat(mandatoryLinkedPath)
	if err != nil {
		t.Fatalf("stat mandatory linked fixture: %v", err)
	}
	mandatoryLinkedSource, ok, err := codexPaginatedHistoryFile(mandatoryLinkedPath, mandatorySession.CodexThreadID)
	if err != nil || !ok {
		t.Fatalf("read mandatory linked fixture identity: ok=%v err=%v", ok, err)
	}
	mandatoryCheckpointID := transcriptCheckpointID(mandatorySession.ID)
	if _, _, err := store.UpdateImportCheckpoint(ctx, mandatoryCheckpointID, func(_ teamstore.ImportCheckpoint, _ bool, updatedAt time.Time) (teamstore.ImportCheckpoint, bool, error) {
		return teamstore.ImportCheckpoint{
			ID:                              mandatoryCheckpointID,
			SessionID:                       mandatorySession.ID,
			SourcePath:                      mandatoryLinkedPath,
			SourceGeneration:                "old-mandatory-linked-source",
			SourceFingerprint:               transcriptCheckpointSourceFingerprint(mandatoryLinkedPath, mandatoryLinkedInfo.Size()),
			LastOffset:                      mandatoryLinkedInfo.Size(),
			LastRecordID:                    "source:mandatory-linked-root",
			LastOffsetKnown:                 true,
			SourceSize:                      mandatoryLinkedInfo.Size(),
			SourceModTime:                   mandatoryLinkedInfo.ModTime(),
			SourceRewriteBlocked:            true,
			SourceRewriteRecoveryIdentity:   mandatoryLinkedSource.Identity,
			SourceRewriteRecoverySize:       mandatoryLinkedInfo.Size(),
			SourceRewriteRecoveryModTime:    mandatoryLinkedInfo.ModTime(),
			SourceRewriteRecoveryChangeTime: teamstore.SourceFileChangeTimeFromFileInfo(mandatoryLinkedInfo),
			Status:                          importCheckpointStatusBlocked,
			UnresolvedExecution:             &teamstore.ExecutionAnchor{SessionID: mandatorySession.ID, ThreadID: mandatorySession.CodexThreadID, OuterTurnID: "turn-mandatory-linked"},
			UpdatedAt:                       updatedAt,
		}, true, nil
	}); err != nil {
		t.Fatalf("seed mandatory linked checkpoint: %v", err)
	}

	// A separate HistoryWatch row carries the same kind of explicit source
	// rewrite fence. Backlog mode must retain this recovery lane too; testing
	// only linked checkpoints would allow the history phase to be suppressed
	// wholesale without proving the safety boundary.
	mandatoryHistoryPath := filepath.Join(root, "sessions", now.Format("2006"), now.Format("01"), now.Format("02"), "backlog-mandatory-history.jsonl")
	mandatoryHistoryContent := `{"type":"session_meta","payload":{"id":"backlog-history-thread","history_mode":"paginated"}}` + "\n" + listenerRecoveryTranscriptLine("mandatory-history-root", "mandatory history root")
	if err := os.WriteFile(mandatoryHistoryPath, []byte(mandatoryHistoryContent), 0o600); err != nil {
		t.Fatalf("write mandatory history fixture: %v", err)
	}
	mandatoryHistoryInfo, err := os.Stat(mandatoryHistoryPath)
	if err != nil {
		t.Fatalf("stat mandatory history fixture: %v", err)
	}
	mandatoryHistoryCheckpoint := listenerRecoveryHistoryCheckpoint(mandatoryHistoryPath, session.ID, "backlog-history-thread", mandatoryHistoryInfo)
	mandatoryHistoryCheckpoint.SourceGeneration = "old-mandatory-history-source"
	mandatoryHistoryCheckpoint.SourceRewriteBlocked = true
	mandatoryHistoryCheckpoint.SourceRewriteRecoveryIdentity = ""
	if err := store.UpdateHistoryWatch(ctx, func(history map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
		history[mandatoryHistoryCheckpoint.ID] = mandatoryHistoryCheckpoint
		return nil
	}); err != nil {
		t.Fatalf("seed mandatory history checkpoint: %v", err)
	}

	historyEntered := make(chan struct{})
	var historyOnce sync.Once
	mandatoryHistoryEntered := make(chan struct{})
	var mandatoryHistoryOnce sync.Once
	bridge.historyWatchPathHook = func(ctx context.Context, path string) error {
		if cleanComparablePath(path) == cleanComparablePath(mandatoryHistoryPath) {
			mandatoryHistoryOnce.Do(func() { close(mandatoryHistoryEntered) })
			return nil
		}
		historyOnce.Do(func() { close(historyEntered) })
		<-ctx.Done()
		return ctx.Err()
	}
	linkedMandatoryEntered := make(chan struct{})
	var linkedMandatoryOnce sync.Once
	bridge.linkedTranscriptSessionHook = func(_ context.Context, session Session) error {
		if session.ID == mandatorySession.ID {
			linkedMandatoryOnce.Do(func() { close(linkedMandatoryEntered) })
		}
		return nil
	}

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), executor)
	options.PhaseBudget = 500 * time.Millisecond
	options.PollWorkerBudget = 100 * time.Millisecond
	options.OwnerStaleAfter = 2 * time.Second
	listener := startListenerRecovery(t, bridge, options)
	var releaseOnce sync.Once
	releaseExecutor := func() {
		releaseOnce.Do(func() { close(executor.release) })
	}
	t.Cleanup(releaseExecutor)

	select {
	case <-executor.started:
	case err := <-listener.done:
		t.Fatalf("listener exited before durable backlog reached executor: %v; output=%s", err, listenerOutput.String())
	case <-time.After(listenerRecoveryProgressTimeout):
		state, _ := store.Load(ctx)
		t.Fatalf("durable Teams backlog never reached the executor: turns=%#v phases={queued:%#v poll:%#v history:%#v} output=%s", state.Turns, bridge.mainLoopPhaseStatsSnapshot("queued-turns"), bridge.mainLoopPhaseStatsSnapshot("poll"), bridge.mainLoopPhaseStatsSnapshot("history-watch"), listenerOutput.String())
	}

	// The desired invariant is observable without waiting for a lease takeover:
	// while one turn is running and another remains queued, ordinary history
	// maintenance must not enter its worker. Mandatory linked recovery below is
	// still expected to run in the same backlog cycle.
	select {
	case <-historyEntered:
		t.Fatalf("history maintenance entered while Teams backlog was still durable; phase=%#v", bridge.mainLoopPhaseStatsSnapshot("history-watch"))
	case <-time.After(750 * time.Millisecond):
	}
	select {
	case <-mandatoryHistoryEntered:
		// Mandatory source-rewrite recovery remains eligible during backlog mode.
	case listenerErr := <-listener.done:
		t.Fatalf("listener exited before mandatory history recovery ran: %v; output=%s", listenerErr, listenerOutput.String())
	case <-time.After(listenerRecoveryProgressTimeout):
		t.Fatalf("mandatory history recovery did not run during backlog; phase=%#v output=%s", bridge.mainLoopPhaseStatsSnapshot("history-watch"), listenerOutput.String())
	}
	select {
	case <-linkedMandatoryEntered:
		// Mandatory source-rewrite recovery remains eligible during backlog mode.
	case listenerErr := <-listener.done:
		t.Fatalf("listener exited before mandatory linked recovery ran: %v; output=%s", listenerErr, listenerOutput.String())
	case <-time.After(listenerRecoveryProgressTimeout):
		t.Fatalf("mandatory linked recovery did not run during backlog; phase=%#v output=%s", bridge.mainLoopPhaseStatsSnapshot("linked-transcript"), listenerOutput.String())
	}

	queued, err := store.HasQueuedTurns(ctx)
	if err != nil {
		t.Fatalf("check durable Teams backlog after observation: %v", err)
	}
	if !queued {
		t.Fatal("Teams backlog disappeared while the first executor was deliberately blocked")
	}
	control, err := store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("read durable optional-maintenance deferral: %v", err)
	}
	if control.OptionalMaintenanceDeferredUntil.IsZero() || control.OptionalMaintenanceDeferredReason == "" {
		t.Fatalf("optional-maintenance deferral = %#v, want durable wake deadline/reason", control)
	}

	releaseExecutor()
	deadline := time.Now().Add(listenerRecoveryProgressTimeout)
	for time.Now().Before(deadline) {
		queued, err = store.HasQueuedTurns(ctx)
		if err != nil {
			t.Fatalf("check Teams backlog after releasing executor: %v", err)
		}
		if !queued {
			break
		}
		time.Sleep(listenerRecoveryPollInterval)
	}
	if queued {
		state, _ := store.Load(ctx)
		t.Fatalf("Teams backlog did not drain after executor release: turns=%#v output=%s", state.Turns, listenerOutput.String())
	}
	select {
	case <-historyEntered:
		// The durable wake edge must restore ordinary maintenance after the
		// Teams queue is no longer active.
	case err := <-listener.done:
		t.Fatalf("listener exited before optional maintenance woke: %v; output=%s", err, listenerOutput.String())
	case <-time.After(listenerRecoveryProgressTimeout):
		t.Fatalf("optional history maintenance did not wake after backlog drain; control=%#v phase=%#v output=%s", func() teamstore.ServiceControl {
			control, _ := store.ReadControl(ctx)
			return control
		}(), bridge.mainLoopPhaseStatsSnapshot("history-watch"), listenerOutput.String())
	}
	control, err = store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("read optional-maintenance control after backlog drain: %v", err)
	}
	if !control.OptionalMaintenanceDeferredUntil.IsZero() || control.OptionalMaintenanceDeferredReason != "" {
		t.Fatalf("optional-maintenance deferral remained after backlog drain: %#v", control)
	}
	listener.stop(t)
}

type listenerBacklogOutput struct {
	mu     sync.Mutex
	buffer *bytes.Buffer
}

func (w *listenerBacklogOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.Write(p)
}

func (w *listenerBacklogOutput) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.String()
}

func backlogHistoryTestMessage(id string, text string) ChatMessage {
	message := bridgeTestMessage(id)
	message.ChatID = "chat-backlog-history"
	message.Body.ContentType = "html"
	message.Body.Content = "<p>" + text + "</p>"
	return message
}

type listenerBacklogCompletingExecutor struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
	once    sync.Once
	calls   int
}

func (e *listenerBacklogCompletingExecutor) Run(ctx context.Context, _ *Session, _ string) (ExecutionResult, error) {
	e.once.Do(func() { close(e.started) })
	select {
	case <-e.release:
	case <-ctx.Done():
		return ExecutionResult{}, ctx.Err()
	}
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()
	return ExecutionResult{
		Text:          "durable backlog completion",
		CodexThreadID: "thread-backlog-history",
		CodexTurnID:   fmt.Sprintf("turn-backlog-history-%d", call),
	}, nil
}
