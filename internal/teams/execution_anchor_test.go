package teams

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type testExecutionFenceExecutor struct {
	confirmed bool
	calls     int
}

func (e *testExecutionFenceExecutor) Run(context.Context, *Session, string) (ExecutionResult, error) {
	return ExecutionResult{}, nil
}

func (e *testExecutionFenceExecutor) ReconcileExecutionFence(context.Context, *Session) (bool, error) {
	e.calls++
	return e.confirmed, nil
}

func TestExecutionFenceProbeThrottleIsMemoryScoped(t *testing.T) {
	bridge := &Bridge{}
	anchor := teamstore.ExecutionAnchor{OuterTurnID: "outer", CodexTurnID: "codex", Generation: 1}
	first := time.Unix(100, 0)
	if !bridge.claimExecutionFenceProbe("session", anchor, first) {
		t.Fatal("first fence probe was throttled")
	}
	if bridge.claimExecutionFenceProbe("session", anchor, first.Add(time.Second)) {
		t.Fatal("second fence probe inside interval was not throttled")
	}
	if !bridge.claimExecutionFenceProbe("session", anchor, first.Add(executionFenceReconcileInterval)) {
		t.Fatal("fence probe at interval was throttled")
	}
	if len(bridge.executionFenceProbeAt) != 1 {
		t.Fatalf("probe cache entries = %d, want one scoped entry", len(bridge.executionFenceProbeAt))
	}
}

func TestExecutionFenceProbeCacheIsBounded(t *testing.T) {
	bridge := &Bridge{}
	now := time.Unix(200, 0)
	for i := 0; i < executionFenceProbeCacheLimit+17; i++ {
		anchor := teamstore.ExecutionAnchor{
			OuterTurnID: "outer-" + strconv.Itoa(i),
			CodexTurnID: "codex-" + strconv.Itoa(i),
			Generation:  int64(i + 1),
		}
		if !bridge.claimExecutionFenceProbe("session-"+strconv.Itoa(i), anchor, now) {
			t.Fatalf("probe %d was unexpectedly throttled", i)
		}
	}
	if got := len(bridge.executionFenceProbeAt); got != executionFenceProbeCacheLimit {
		t.Fatalf("probe cache entries = %d, want bounded at %d", got, executionFenceProbeCacheLimit)
	}
}

func TestSessionExecutionOwnershipClearsOnlyAfterConfirmedFence(t *testing.T) {
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &testExecutionFenceExecutor{}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-1"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	now := time.Now()
	checkpointID := transcriptCheckpointID(session.ID)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		checkpoint := state.ImportCheckpoints[checkpointID]
		checkpoint.ID = checkpointID
		checkpoint.SessionID = session.ID
		checkpoint.UnresolvedExecution = &teamstore.ExecutionAnchor{
			SessionID:   session.ID,
			ThreadID:    "thread-1",
			OuterTurnID: "outer-1",
			CodexTurnID: "codex-outer-1",
			State:       executionAnchorStateUnresolved,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		state.Turns["outer-1"] = teamstore.Turn{
			ID:             "outer-1",
			SessionID:      session.ID,
			Status:         teamstore.TurnStatusInterrupted,
			CodexThreadID:  "thread-1",
			CodexTurnID:    "codex-outer-1",
			RecoveryReason: recoveryReasonAmbiguousCodexExecutionPrefix + " cancellation unconfirmed",
			InterruptedAt:  now,
		}
		state.ImportCheckpoints[checkpointID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("seed unresolved anchor: %v", err)
	}
	if unresolved, err := bridge.sessionExecutionOwnershipUnresolved(context.Background(), *session); err != nil {
		t.Fatalf("unconfirmed ownership check: %v", err)
	} else if !unresolved {
		t.Fatal("unconfirmed fence was treated as resolved")
	}
	if executor.calls != 1 {
		t.Fatalf("reconcile calls after unconfirmed fence = %d, want 1", executor.calls)
	}
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		checkpoint := state.ImportCheckpoints[checkpointID]
		if checkpoint.UnresolvedExecution != nil {
			checkpoint.UnresolvedExecution.LastFenceCheckAt = time.Time{}
		}
		state.ImportCheckpoints[checkpointID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("reset fence probe backoff: %v", err)
	}
	bridge.executionFenceProbeMu.Lock()
	bridge.executionFenceProbeAt = nil
	bridge.executionFenceProbeMu.Unlock()
	executor.confirmed = true
	if unresolved, err := bridge.sessionExecutionOwnershipUnresolved(context.Background(), *session); err != nil {
		t.Fatalf("confirmed ownership check: %v", err)
	} else if unresolved {
		t.Fatal("confirmed fence remained unresolved")
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state after confirmed fence: %v", err)
	}
	if state.ImportCheckpoints[checkpointID].UnresolvedExecution != nil {
		t.Fatalf("confirmed fence did not clear anchor: %#v", state.ImportCheckpoints[checkpointID].UnresolvedExecution)
	}
}

func TestSessionExecutionOwnershipClearsTypedOuterTurnProof(t *testing.T) {
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &testExecutionFenceExecutor{}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "typed-thread"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	now := time.Now()
	checkpointID := transcriptCheckpointID(session.ID)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{
			ID: checkpointID, SessionID: session.ID,
			UnresolvedExecution: &teamstore.ExecutionAnchor{
				SessionID: session.ID, ThreadID: "typed-thread", OuterTurnID: "typed-outer", CodexTurnID: "typed-codex",
				Generation: 11, State: executionAnchorStateUnresolved, CreatedAt: now, UpdatedAt: now,
			},
		}
		state.Turns["typed-outer"] = teamstore.Turn{
			ID: "typed-outer", SessionID: session.ID, Status: teamstore.TurnStatusInterrupted,
			CodexThreadID: "typed-thread", CodexTurnID: "typed-codex",
			RecoveryReason: recoveryReasonCodexExecutionConfirmed, InterruptedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed typed outer proof: %v", err)
	}
	unresolved, err := bridge.sessionExecutionOwnershipUnresolved(context.Background(), *session)
	if err != nil {
		t.Fatalf("typed outer ownership check: %v", err)
	}
	if unresolved {
		t.Fatal("typed outer proof remained unresolved")
	}
	if executor.calls != 0 {
		t.Fatalf("typed outer proof invoked fence probe %d times, want zero", executor.calls)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load typed outer state: %v", err)
	}
	if state.ImportCheckpoints[checkpointID].UnresolvedExecution != nil {
		t.Fatalf("typed outer proof did not clear anchor: %#v", state.ImportCheckpoints[checkpointID].UnresolvedExecution)
	}
}

func TestSessionExecutionOwnershipKeepsAnchorWithLaterDurableTerminal(t *testing.T) {
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &testExecutionFenceExecutor{}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-1"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	now := time.Now()
	checkpointID := transcriptCheckpointID(session.ID)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{
			ID: checkpointID, SessionID: session.ID,
			UnresolvedExecution: &teamstore.ExecutionAnchor{
				SessionID: session.ID, ThreadID: session.CodexThreadID,
				OuterTurnID: "outer-active", CodexTurnID: "codex-old",
				State: executionAnchorStateUnresolved, CreatedAt: now, UpdatedAt: now,
			},
		}
		state.Turns["outer-active"] = teamstore.Turn{
			ID: "outer-active", SessionID: session.ID, Status: teamstore.TurnStatusInterrupted,
			CodexThreadID: session.CodexThreadID, CodexTurnID: "codex-old",
			RecoveryReason: recoveryReasonAmbiguousCodexExecutionPrefix + " app-server still active",
			InterruptedAt:  now,
		}
		state.Turns["later-terminal"] = teamstore.Turn{
			ID: "later-terminal", SessionID: session.ID, Status: teamstore.TurnStatusCompleted,
			CodexThreadID: session.CodexThreadID, CodexTurnID: "codex-new",
			CompletedAt: now.Add(time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed active anchor and later terminal: %v", err)
	}
	unresolved, err := bridge.sessionExecutionOwnershipUnresolved(context.Background(), *session)
	if err != nil {
		t.Fatalf("ownership check: %v", err)
	}
	if !unresolved {
		t.Fatal("later durable terminal incorrectly cleared active app-server ownership")
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state.ImportCheckpoints[checkpointID].UnresolvedExecution == nil {
		t.Fatal("active execution anchor was cleared by unrelated durable terminal")
	}
}

func TestExecutionAnchorForRestartedRunningTurnDoesNotTrustNewerCheckpoint(t *testing.T) {
	now := time.Now()
	checkpoint := teamstore.ImportCheckpoint{
		SourcePath:     "/tmp/session.jsonl",
		LastRecordID:   "post-restart-cursor",
		LastSourceLine: 40,
		LastOffset:     4096,
		UpdatedAt:      now.Add(time.Minute),
	}
	session := Session{ID: "session-1", CodexThreadID: "thread-1"}
	turn := teamstore.Turn{
		ID:             "turn-1",
		SessionID:      session.ID,
		Status:         teamstore.TurnStatusRunning,
		CodexThreadID:  session.CodexThreadID,
		CodexTurnID:    "codex-turn-1",
		RecoveryReason: recoveryReasonAmbiguousAfterHelperRestart,
		StartedAt:      now.Add(-time.Minute),
	}
	anchor := executionAnchorForLegacyTurn(session, codexhistory.Session{}, checkpoint, turn)
	if anchor.CutoffOffset != 0 || anchor.CutoffLine != 0 || anchor.CutoffRecordID != "" {
		t.Fatalf("restart-recovered running turn trusted newer checkpoint: %#v", anchor)
	}
}

func TestObserveUnresolvedExecutionTailSkipsUnchangedSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := `{"type":"session_meta","payload":{"id":"thread-1"}}` + "\n"
	firstRecord := `{"type":"task_started","thread_id":"thread-1","task_id":"inner-1"}` + "\n"
	if err := os.WriteFile(path, []byte(prefix+firstRecord), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	anchor := teamstore.ExecutionAnchor{
		ThreadID:     "thread-1",
		SourcePath:   path,
		CutoffOffset: int64(len(prefix)),
	}
	first := observeUnresolvedExecutionTail(anchor)
	if !first.SourceObserved || !first.Scanned || !first.Continuation || len(first.TaskIDs) != 1 || first.TaskIDs[0] != "inner-1" {
		t.Fatalf("first observation = %#v, want scanned task continuation", first)
	}

	anchor.ObservedSourceSize = first.SourceSize
	anchor.ObservedSourceModTime = first.SourceModTime
	second := observeUnresolvedExecutionTail(anchor)
	if !second.SourceObserved || second.Scanned || len(second.TaskIDs) != 0 {
		t.Fatalf("unchanged-source observation = %#v, want stat-only fast path", second)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open transcript for append: %v", err)
	}
	if _, err := file.WriteString(`{"type":"task_started","thread_id":"thread-1","task_id":"inner-2"}` + "\n"); err != nil {
		_ = file.Close()
		t.Fatalf("append transcript: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close transcript: %v", err)
	}
	third := observeUnresolvedExecutionTail(anchor)
	if !third.SourceObserved || !third.Scanned || len(third.TaskIDs) != 2 {
		t.Fatalf("grown-source observation = %#v, want rescanned continuation tail", third)
	}
}

func TestTranscriptImportCompletionLeavesConcurrentAppendForNextPass(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	initialInfo, err := os.Stat(transcriptPath)
	if err != nil {
		t.Fatalf("stat initial transcript: %v", err)
	}
	if err := os.WriteFile(transcriptPath, []byte(initial+`{"id":"new","role":"assistant","text":"appended while importing"}`+"\n"), 0o600); err != nil {
		t.Fatalf("append transcript: %v", err)
	}
	finalInfo, err := os.Stat(transcriptPath)
	if err != nil {
		t.Fatalf("stat final transcript: %v", err)
	}
	if finalInfo.Size() <= initialInfo.Size() {
		t.Fatalf("transcript did not grow: initial=%d final=%d", initialInfo.Size(), finalInfo.Size())
	}

	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	checkpointID := transcriptCheckpointID(session.ID)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{
			ID:             checkpointID,
			SessionID:      session.ID,
			SourcePath:     transcriptPath,
			LastRecordID:   "old",
			LastSourceLine: 1,
			LastOffset:     initialInfo.Size(),
			SourceSize:     initialInfo.Size(),
			SourceModTime:  initialInfo.ModTime(),
			Status:         importCheckpointStatusImporting,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed importing checkpoint: %v", err)
	}
	result := transcriptImportResult{
		LastRecordID:        "old",
		LastLine:            1,
		LastOffset:          initialInfo.Size(),
		SourceSizeAtRead:    initialInfo.Size(),
		SourceModTimeAtRead: initialInfo.ModTime(),
		Complete:            true,
	}
	if err := bridge.markTranscriptImportCompleteFromResult(context.Background(), *session, transcriptPath, result, checkpointID); err != nil {
		t.Fatalf("mark import completion: %v", err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[checkpointID]
	if checkpoint.Status != importCheckpointStatusComplete || checkpoint.LastRecordID != "old" || checkpoint.LastOffset != initialInfo.Size() {
		t.Fatalf("checkpoint = %#v, want complete at imported boundary %d", checkpoint, initialInfo.Size())
	}
	if checkpoint.SourceSize != finalInfo.Size() {
		t.Fatalf("checkpoint source size = %d, want current %d while retaining boundary", checkpoint.SourceSize, finalInfo.Size())
	}
}

func TestTranscriptImportCompletionDoesNotReuseCursorAfterSameSizeRewrite(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	oldLine := `{"id":"old","role":"assistant","text":"old"}` + "\n"
	newLine := `{"id":"new","role":"assistant","text":"new"}` + "\n"
	if len(oldLine) != len(newLine) {
		t.Fatalf("rewrite fixtures must have equal size: old=%d new=%d", len(oldLine), len(newLine))
	}
	if err := os.WriteFile(transcriptPath, []byte(oldLine), 0o600); err != nil {
		t.Fatalf("write old transcript: %v", err)
	}
	oldModTime := time.Now().Add(-2 * time.Minute)
	if err := os.Chtimes(transcriptPath, oldModTime, oldModTime); err != nil {
		t.Fatalf("set old transcript time: %v", err)
	}
	oldInfo, err := os.Stat(transcriptPath)
	if err != nil {
		t.Fatalf("stat old transcript: %v", err)
	}
	if err := os.WriteFile(transcriptPath, []byte(newLine), 0o600); err != nil {
		t.Fatalf("rewrite transcript: %v", err)
	}
	// Preserve both metadata fields: a same-size, same-mtime in-place rewrite
	// must still invalidate the trusted cursor through SourceFingerprint.
	newModTime := oldModTime
	if err := os.Chtimes(transcriptPath, newModTime, newModTime); err != nil {
		t.Fatalf("set new transcript time: %v", err)
	}
	newInfo, err := os.Stat(transcriptPath)
	if err != nil {
		t.Fatalf("stat rewritten transcript: %v", err)
	}

	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	checkpointID := transcriptCheckpointID(session.ID)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{
			ID:             checkpointID,
			SessionID:      session.ID,
			SourcePath:     transcriptPath,
			LastRecordID:   "old",
			LastSourceLine: 1,
			LastOffset:     oldInfo.Size(),
			SourceSize:     oldInfo.Size(),
			SourceModTime:  oldInfo.ModTime(),
			Status:         importCheckpointStatusImporting,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed old checkpoint: %v", err)
	}
	result := transcriptImportResult{
		LastRecordID:            "new",
		LastLine:                1,
		LastOffset:              newInfo.Size(),
		SourceSizeBeforeRead:    newInfo.Size(),
		SourceSizeAtRead:        newInfo.Size(),
		SourceModTimeBeforeRead: newInfo.ModTime(),
		SourceModTimeAtRead:     newInfo.ModTime(),
		Complete:                true,
	}
	if err := bridge.markTranscriptImportCompleteFromResult(context.Background(), *session, transcriptPath, result, checkpointID); err != nil {
		t.Fatalf("mark rewritten completion: %v", err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load rewritten state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[checkpointID]
	if checkpoint.LastRecordID != "new" || checkpoint.SourceFingerprint == "" {
		t.Fatalf("rewritten checkpoint = %#v, want new record and content identity", checkpoint)
	}
}

func TestTeamsSourceProofDetectsSameSizeInPlaceRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	oldLine := `{"id":"old","role":"assistant","text":"old"}` + "\n"
	newLine := `{"id":"new","role":"assistant","text":"new"}` + "\n"
	if len(oldLine) != len(newLine) {
		t.Fatalf("rewrite fixtures must have equal size: old=%d new=%d", len(oldLine), len(newLine))
	}
	if err := os.WriteFile(path, []byte(oldLine), 0o600); err != nil {
		t.Fatalf("write old transcript: %v", err)
	}
	oldModTime := time.Now().Add(-time.Minute)
	if err := os.Chtimes(path, oldModTime, oldModTime); err != nil {
		t.Fatalf("set old transcript time: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat old transcript: %v", err)
	}
	checkpoint := teamstore.ImportCheckpoint{
		SourcePath:        path,
		LastOffset:        info.Size(),
		SourceSize:        info.Size(),
		SourceModTime:     info.ModTime(),
		SourceFingerprint: transcriptCheckpointSourceFingerprint(path, info.Size()),
		Status:            importCheckpointStatusComplete,
	}
	if !linkedCheckpointFileUnchanged(path, checkpoint) {
		t.Fatal("baseline checkpoint was not recognized as unchanged")
	}
	if err := os.WriteFile(path, []byte(newLine), 0o600); err != nil {
		t.Fatalf("rewrite transcript: %v", err)
	}
	if err := os.Chtimes(path, oldModTime, oldModTime); err != nil {
		t.Fatalf("restore rewritten transcript time: %v", err)
	}
	newInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat rewritten transcript: %v", err)
	}
	if newInfo.Size() != info.Size() || !newInfo.ModTime().Equal(info.ModTime()) {
		t.Fatalf("rewrite changed stat metadata: old=%v/%d new=%v/%d", info.ModTime(), info.Size(), newInfo.ModTime(), newInfo.Size())
	}
	if linkedCheckpointFileUnchanged(path, checkpoint) {
		t.Fatal("same-size, same-mtime rewrite reused the old checkpoint")
	}
}

func TestExecutionAnchorLineSignalRecognizesContinuationShapes(t *testing.T) {
	cases := []struct {
		name   string
		line   string
		wantID string
	}{
		{name: "task started", line: `{"type":"task_started","thread_id":"thread-1","turn_id":"inner-task"}`, wantID: "inner-task"},
		{name: "event task started", line: `{"type":"event_msg","thread_id":"thread-1","payload":{"type":"task_started","id":"inner-event","turn_id":"inner-event"}}`, wantID: "inner-event"},
		{name: "turn context", line: `{"type":"event_msg","thread_id":"thread-1","payload":{"type":"turn_context","id":"inner-context"}}`, wantID: "inner-context"},
		{name: "goal continuation", line: `{"type":"goal_continuation","thread_id":"thread-1","id":"inner-goal"}`, wantID: "inner-goal"},
		{name: "response item with turn", line: `{"type":"response_item","thread_id":"thread-1","payload":{"id":"answer-item","turn_id":"inner-answer","type":"message"}}`, wantID: "inner-answer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var obj map[string]json.RawMessage
			if err := json.Unmarshal([]byte(tc.line), &obj); err != nil {
				t.Fatalf("unmarshal fixture: %v", err)
			}
			gotID, gotThread, ok := executionAnchorLineSignal(obj)
			if !ok || gotID != tc.wantID || gotThread != "thread-1" {
				t.Fatalf("signal = (%q, %q, %t), want (%q, thread-1, true)", gotID, gotThread, ok, tc.wantID)
			}
		})
	}
}

func TestObserveUnresolvedExecutionTailFailsClosedOnTruncatedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	prefix := `{"type":"session_meta","payload":{"id":"thread-1"}}` + "\n"
	if err := os.WriteFile(path, []byte(prefix+`{"type":"task_started","thread_id":"thread-1"`), 0o600); err != nil {
		t.Fatalf("write truncated transcript: %v", err)
	}
	observation := observeUnresolvedExecutionTail(teamstore.ExecutionAnchor{
		ThreadID:     "thread-1",
		SourcePath:   path,
		CutoffOffset: int64(len(prefix)),
	})
	if !observation.SourceObserved || !observation.Scanned || !observation.Unknown {
		t.Fatalf("truncated observation = %#v, want source scanned as unknown", observation)
	}
}

func TestExecutionAnchorLaterDurableTerminalDoesNotResolveActiveExecution(t *testing.T) {
	now := time.Now()
	session := Session{ID: "s1", CodexThreadID: "thread-1"}
	base := teamstore.State{Turns: map[string]teamstore.Turn{
		"outer": {ID: "outer", SessionID: "s1", CodexThreadID: "thread-1", Status: teamstore.TurnStatusInterrupted, RecoveryReason: recoveryReasonAmbiguousCodexExecutionPrefix + " still running", InterruptedAt: now},
	}}
	tests := []struct {
		name  string
		later teamstore.Turn
	}{
		{name: "running is still ambiguous", later: teamstore.Turn{ID: "later", SessionID: "s1", CodexThreadID: "thread-1", CodexTurnID: "codex-later", Status: teamstore.TurnStatusRunning, StartedAt: now.Add(time.Second)}},
		{name: "missing thread is still ambiguous", later: teamstore.Turn{ID: "later", SessionID: "s1", CodexTurnID: "codex-later", Status: teamstore.TurnStatusCompleted, CompletedAt: now.Add(time.Second)}},
		{name: "matching later terminal is not ownership proof", later: teamstore.Turn{ID: "later", SessionID: "s1", CodexThreadID: "thread-1", CodexTurnID: "codex-later", Status: teamstore.TurnStatusCompleted, CompletedAt: now.Add(time.Second)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := base
			state.Turns = map[string]teamstore.Turn{"outer": base.Turns["outer"], "later": tc.later}
			if _, unresolved := unresolvedAmbiguousCodexTurn(state, session); !unresolved {
				t.Fatalf("later durable state incorrectly resolved the anchor: %#v", state.Turns)
			}
		})
	}
}

func TestExecutionAnchorConfirmedFenceDoesNotRecreateOnLaterOwnershipChecks(t *testing.T) {
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &testExecutionFenceExecutor{confirmed: true}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-1"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	checkpointID := transcriptCheckpointID(session.ID)
	turnID := "outer-confirmed"
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		now := time.Now()
		state.Turns[turnID] = teamstore.Turn{ID: turnID, SessionID: session.ID, CodexThreadID: session.CodexThreadID, Status: teamstore.TurnStatusInterrupted, RecoveryReason: recoveryReasonAmbiguousCodexExecutionPrefix + " fence", InterruptedAt: now}
		state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{ID: checkpointID, SessionID: session.ID, UnresolvedExecution: &teamstore.ExecutionAnchor{SessionID: session.ID, ThreadID: session.CodexThreadID, OuterTurnID: turnID, State: executionAnchorStateUnresolved}}
		return nil
	}); err != nil {
		t.Fatalf("seed confirmed anchor: %v", err)
	}
	for i := 0; i < 3; i++ {
		if unresolved, err := bridge.sessionExecutionOwnershipUnresolved(context.Background(), *session); err != nil {
			t.Fatalf("ownership check %d: %v", i, err)
		} else if unresolved {
			t.Fatalf("ownership check %d remained unresolved", i)
		}
	}
	if executor.calls != 1 {
		t.Fatalf("fence calls = %d, want one before clear", executor.calls)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load post-clear state: %v", err)
	}
	if state.ImportCheckpoints[checkpointID].UnresolvedExecution != nil {
		t.Fatalf("anchor recreated after confirmed fence: %#v", state.ImportCheckpoints[checkpointID].UnresolvedExecution)
	}
}

func TestExecutionAnchorProofRejectsMismatchedProvenance(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &testExecutionFenceExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-proof"
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	checkpointID := transcriptCheckpointID(session.ID)
	now := time.Now()
	anchor := teamstore.ExecutionAnchor{
		SessionID: session.ID, ThreadID: session.CodexThreadID, OuterTurnID: "outer-proof",
		CodexTurnID: "codex-proof", SourcePath: "/tmp/proof-session.jsonl", CutoffRecordID: "before",
		CutoffLine: 7, CutoffOffset: 700, State: executionAnchorStateUnresolved, Generation: 3,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{
			ID: checkpointID, SessionID: session.ID, ExecutionAnchorGeneration: anchor.Generation,
			UnresolvedExecution: &anchor,
		}
		state.Turns[anchor.OuterTurnID] = teamstore.Turn{
			ID: anchor.OuterTurnID, SessionID: session.ID, Status: teamstore.TurnStatusInterrupted,
			CodexThreadID: anchor.ThreadID, CodexTurnID: anchor.CodexTurnID,
			RecoveryReason: recoveryReasonAmbiguousCodexExecutionPrefix + " proof test", InterruptedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed proof anchor: %v", err)
	}
	for name, mutate := range map[string]func(*executionAnchorProof){
		"wrong thread":     func(proof *executionAnchorProof) { proof.ThreadID = "other-thread" },
		"wrong source":     func(proof *executionAnchorProof) { proof.SourcePath = "/tmp/other-session.jsonl" },
		"wrong generation": func(proof *executionAnchorProof) { proof.Generation = 2 },
		"transcript terminal is not ownership proof": func(proof *executionAnchorProof) {
			proof.Kind = "transcript_terminal"
		},
	} {
		t.Run(name, func(t *testing.T) {
			proof := executionAnchorProofFromAnchor(session.ID, anchor, executionAnchorProofFence)
			mutate(&proof)
			if err := bridge.clearUnresolvedExecutionAnchorWithProof(ctx, proof); err != nil {
				t.Fatalf("clear with mismatched proof: %v", err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
			if err != nil || !found || checkpoint.UnresolvedExecution == nil {
				t.Fatalf("mismatched proof cleared anchor: found=%v err=%v checkpoint=%#v", found, err, checkpoint)
			}
		})
	}
}

func TestExecutionAnchorGenerationIncreasesAfterClearAndRecreate(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &testExecutionFenceExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-generation"
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	checkpointID := transcriptCheckpointID(session.ID)
	now := time.Now()
	turn := teamstore.Turn{
		ID: "outer-generation-1", SessionID: session.ID, Status: teamstore.TurnStatusInterrupted,
		CodexThreadID: session.CodexThreadID, CodexTurnID: "codex-generation-1",
		RecoveryReason: recoveryReasonAmbiguousCodexExecutionPrefix + " generation test", InterruptedAt: now,
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Turns[turn.ID] = turn
		state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{
			ID: checkpointID, SessionID: session.ID, ExecutionAnchorGeneration: 1,
			UnresolvedExecution: &teamstore.ExecutionAnchor{
				SessionID: session.ID, ThreadID: session.CodexThreadID, OuterTurnID: turn.ID,
				CodexTurnID: turn.CodexTurnID, State: executionAnchorStateUnresolved, Generation: 1,
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed generation anchor: %v", err)
	}
	checkpoint, _, err := store.ImportCheckpoint(ctx, checkpointID)
	if err != nil {
		t.Fatalf("load generation anchor: %v", err)
	}
	if err := bridge.clearUnresolvedExecutionAnchorWithProof(ctx, executionAnchorProofFromAnchor(session.ID, *checkpoint.UnresolvedExecution, executionAnchorProofFence)); err != nil {
		t.Fatalf("clear generation one anchor: %v", err)
	}
	turn2 := turn
	turn2.ID = "outer-generation-2"
	turn2.CodexTurnID = "codex-generation-2"
	turn2.RecoveryReason = recoveryReasonAmbiguousCodexExecutionPrefix + " recreated generation"
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Turns[turn2.ID] = turn2
		return nil
	}); err != nil {
		t.Fatalf("seed recreated turn: %v", err)
	}
	checkpoint, _, err = store.ImportCheckpoint(ctx, checkpointID)
	if err != nil {
		t.Fatalf("load cleared checkpoint: %v", err)
	}
	updated, err := bridge.ensureUnresolvedExecutionAnchor(ctx, *session, codexhistory.Session{}, checkpoint, turn2)
	if err != nil {
		t.Fatalf("recreate execution anchor: %v", err)
	}
	if updated.UnresolvedExecution == nil || updated.UnresolvedExecution.Generation != 2 || updated.ExecutionAnchorGeneration != 2 {
		t.Fatalf("recreated anchor generation = %#v checkpoint_generation=%d, want 2", updated.UnresolvedExecution, updated.ExecutionAnchorGeneration)
	}
}
