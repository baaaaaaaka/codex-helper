package teams

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestSplitForkHistoryBodyUsesTeamsChunkLimit(t *testing.T) {
	text := strings.Repeat("<&> history line\n", 6000)
	chunks := splitForkHistoryBody("assistant", text)
	if len(chunks) < 2 {
		t.Fatalf("expected long fork history to be chunked, got %d chunk(s)", len(chunks))
	}
	var joined strings.Builder
	for i, chunk := range chunks {
		if chunk.PartIndex != i+1 || chunk.PartCount != len(chunks) {
			t.Fatalf("chunk %d metadata = %#v", i, chunk)
		}
		if chunk.ByteLength > safeTeamsHTMLContentBytes {
			t.Fatalf("chunk %d rendered bytes = %d, want <= %d", i, chunk.ByteLength, safeTeamsHTMLContentBytes)
		}
		joined.WriteString(chunk.Text)
	}
	want := normalizeTeamsRenderTextForKind(TeamsRenderAssistant, text)
	if joined.String() != want {
		t.Fatalf("fork history chunks did not preserve normalized source text: got %d bytes want %d", len(joined.String()), len(want))
	}
}

func TestForkWorkCommandMutatesParentGate(t *testing.T) {
	readOnly := []DashboardCommandName{
		DashboardCommandStatus,
		DashboardCommandStats,
		DashboardCommandDetails,
		DashboardCommandHelp,
		DashboardCommandDefault,
	}
	for _, command := range readOnly {
		if forkWorkCommandMutatesParent(command) {
			t.Fatalf("%q unexpectedly mutates a fenced parent", command)
		}
	}
	mutating := []DashboardCommandName{
		DashboardCommandClose,
		DashboardCommandPark,
		DashboardCommandResume,
		DashboardCommandRetry,
		DashboardCommandRestoreThread,
		DashboardCommandCancel,
		DashboardCommandSendFile,
		DashboardCommandRename,
		DashboardCommandPublishHistory,
		DashboardCommandFork,
		DashboardCommandSkills,
		DashboardCommandBeacon,
		DashboardCommandModel,
		DashboardCommandEffort,
	}
	for _, command := range mutating {
		if !forkWorkCommandMutatesParent(command) {
			t.Fatalf("%q unexpectedly bypasses the fenced parent gate", command)
		}
	}
}

func TestForkWorkSessionQueuesDeterministicPendingNoticeWithoutGraphFlush(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	parent := &Session{
		ID:            "async-fork-parent",
		Status:        string(teamstore.SessionStatusActive),
		ChatID:        "async-fork-parent-chat",
		CodexThreadID: "async-fork-parent-thread",
		Topic:         "Async fork parent",
	}
	if _, created, err := store.CreateSession(ctx, teamstore.SessionContext{
		ID:            parent.ID,
		Status:        teamstore.SessionStatusActive,
		TeamsChatID:   parent.ChatID,
		TeamsTopic:    parent.Topic,
		CodexThreadID: parent.CodexThreadID,
	}); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		now := time.Now().Add(-time.Minute)
		state.Turns["async-fork-cutoff"] = teamstore.Turn{
			ID:          "async-fork-cutoff",
			SessionID:   parent.ID,
			CodexTurnID: "async-fork-codex-cutoff",
			Status:      teamstore.TurnStatusCompleted,
			CompletedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed cutoff: %v", err)
	}
	bridge := &Bridge{store: store}
	first := ChatMessage{ID: "async-fork-command-1"}
	first.Body.Content = "fork"
	if err := bridge.forkWorkSession(ctx, parent, first); err != nil {
		t.Fatalf("first forkWorkSession: %v", err)
	}
	second := ChatMessage{ID: "async-fork-command-2"}
	second.Body.Content = "fork"
	if err := bridge.forkWorkSession(ctx, parent, second); err != nil {
		t.Fatalf("second forkWorkSession: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	progress, pending := 0, 0
	for _, message := range state.OutboxMessages {
		if message.TeamsChatID != parent.ChatID {
			continue
		}
		switch message.Kind {
		case "fork-progress":
			progress++
		case "fork-pending":
			pending++
		}
	}
	if progress != 1 || pending != 1 {
		t.Fatalf("fork notices progress/pending = %d/%d, want one each", progress, pending)
	}
}

func TestRecreateSessionChatDoesNotBypassParentForkFence(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	bridge.registryPath = t.TempDir() + "/registry.json"
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("test session is missing")
	}
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Turns["recreate-fork-cutoff"] = teamstore.Turn{
			ID:          "recreate-fork-cutoff",
			SessionID:   session.ID,
			CodexTurnID: "recreate-fork-codex-cutoff",
			Status:      teamstore.TurnStatusCompleted,
			CompletedAt: time.Now().Add(-time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed fork cutoff: %v", err)
	}
	if _, created, err := store.BeginFork(ctx, teamstore.ForkBeginRequest{
		OperationID:       "recreate-fork-operation",
		ParentSessionID:   session.ID,
		ParentChatID:      session.ChatID,
		ChildSession:      teamstore.SessionContext{ID: "recreate-fork-child", Status: teamstore.SessionStatusStaging},
		CutoffTurnID:      "recreate-fork-cutoff",
		CutoffCodexTurnID: "recreate-fork-codex-cutoff",
	}); err != nil || !created {
		t.Fatalf("BeginFork created=%v err=%v", created, err)
	}
	if _, err := bridge.RecreateSessionChat(ctx, session.ID, RecreateSessionChatOptions{}); err == nil || !strings.Contains(err.Error(), "recreate is temporarily gated") {
		t.Fatalf("recreate while parent is fenced error = %v", err)
	}
}

func TestTranscriptPublicationEntryPointsRespectParentForkFence(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	parent := &Session{
		ID:            "publication-fenced-parent",
		Status:        string(teamstore.SessionStatusActive),
		ChatID:        "publication-fenced-chat",
		CodexThreadID: "publication-fenced-thread",
		Cwd:           t.TempDir(),
	}
	if _, _, err := store.CreateSession(ctx, teamstore.SessionContext{
		ID:            parent.ID,
		Status:        teamstore.SessionStatusActive,
		TeamsChatID:   parent.ChatID,
		CodexThreadID: parent.CodexThreadID,
		Cwd:           parent.Cwd,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Turns["publication-fenced-cutoff"] = teamstore.Turn{
			ID:          "publication-fenced-cutoff",
			SessionID:   parent.ID,
			CodexTurnID: "publication-fenced-codex-cutoff",
			Status:      teamstore.TurnStatusCompleted,
			CompletedAt: time.Now().Add(-time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed cutoff: %v", err)
	}
	if _, created, err := store.BeginFork(ctx, teamstore.ForkBeginRequest{
		OperationID:       "publication-fenced-operation",
		ParentSessionID:   parent.ID,
		ParentChatID:      parent.ChatID,
		ParentThreadID:    parent.CodexThreadID,
		ChildSession:      teamstore.SessionContext{ID: "publication-fenced-child", Status: teamstore.SessionStatusStaging},
		CutoffTurnID:      "publication-fenced-cutoff",
		CutoffCodexTurnID: "publication-fenced-codex-cutoff",
	}); err != nil || !created {
		t.Fatalf("BeginFork created=%v err=%v", created, err)
	}
	bridge := &Bridge{store: store, maintenanceStorePinned: true}
	bridge.reg.Sessions = []Session{*parent}
	cases := []struct {
		name string
		call func() error
		want string
	}{
		{name: "incremental", call: func() error { return bridge.publishWorkSessionHistory(ctx, parent) }, want: "publish-history will resume"},
		{name: "full", call: func() error { return bridge.publishWorkSessionFullHistory(ctx, parent) }, want: "full history publish is temporarily gated"},
		{name: "generic import", call: func() error {
			return bridge.importCodexTranscriptToTeamsWithTarget(ctx, *parent, codexhistory.Session{FilePath: filepath.Join(t.TempDir(), "missing.jsonl")}, "publication-checkpoint", "publication-turn", "sync", transcriptImportRunOptions{})
		}, want: "transcript publication is temporarily gated"},
		{name: "public full", call: func() error {
			_, err := bridge.PublishSessionFullHistory(ctx, parent.ID, "")
			return err
		}, want: "full history publish is temporarily gated"},
	}
	for _, phase := range []teamstore.ForkOperationPhase{
		teamstore.ForkPhaseParentFenced,
		teamstore.ForkPhaseHistoryPublishing,
		teamstore.ForkPhaseActivated,
		teamstore.ForkPhaseBlockedAmbiguous,
	} {
		if _, err := store.UpdateForkOperation(ctx, "publication-fenced-operation", func(current *teamstore.ForkOperation) error {
			current.Phase = phase
			return nil
		}); err != nil {
			t.Fatalf("set phase %q: %v", phase, err)
		}
		for _, tc := range cases {
			t.Run(string(phase)+"/"+tc.name, func(t *testing.T) {
				err := tc.call()
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error = %v, want substring %q", err, tc.want)
				}
			})
		}
	}
	before, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load state before background sync: %v", err)
	}
	if err := bridge.syncSessionTranscriptFromSnapshot(ctx, *parent, codexhistory.Session{FilePath: filepath.Join(t.TempDir(), "missing.jsonl")}, teamstore.State{}, teamstore.ImportCheckpoint{}, false); err != nil {
		t.Fatalf("background sync while parent is fenced = %v, want nil", err)
	}
	after, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load state after background sync: %v", err)
	}
	if len(before.OutboxMessages) != len(after.OutboxMessages) || len(before.ImportCheckpoints) != len(after.ImportCheckpoints) {
		t.Fatalf("background sync changed durable publication state: before outbox/checkpoints=%d/%d after=%d/%d", len(before.OutboxMessages), len(before.ImportCheckpoints), len(after.OutboxMessages), len(after.ImportCheckpoints))
	}
	if _, err := store.UpdateForkOperation(ctx, "publication-fenced-operation", func(current *teamstore.ForkOperation) error {
		current.Phase = teamstore.ForkPhaseLinkSent
		return nil
	}); err != nil {
		t.Fatalf("release phase: %v", err)
	}
	if _, err := bridge.PublishSessionFullHistory(ctx, parent.ID, ""); err != nil && strings.Contains(err.Error(), "temporarily gated") {
		t.Fatalf("public full history remained fenced after link_sent: %v", err)
	}
}

type forkReconcileTestExecutor struct {
	result ForkReconcileResult
	calls  int
}

func (e *forkReconcileTestExecutor) Run(context.Context, *Session, string) (ExecutionResult, error) {
	return ExecutionResult{}, fmt.Errorf("not used by fork reconciliation test")
}

func (e *forkReconcileTestExecutor) ReconcileForkThread(context.Context, *Session, string, time.Time, time.Time) (ForkReconcileResult, error) {
	e.calls++
	return e.result, nil
}

func TestForkParentAndCutoffLoadsDurableSessionAndTurn(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[sqliteMode], func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			parent := teamstore.SessionContext{
				ID:            "fork-parent",
				Status:        teamstore.SessionStatusActive,
				TeamsChatID:   "fork-parent-chat",
				CodexThreadID: "fork-parent-thread",
				TeamsTopic:    "parent topic",
				CreatedAt:     time.Now().Add(-time.Hour),
				UpdatedAt:     time.Now(),
			}
			if _, created, err := store.CreateSession(ctx, parent); err != nil || !created {
				t.Fatalf("CreateSession created=%v err=%v", created, err)
			}
			cutoffAt := time.Now().Add(-time.Minute)
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.Turns["fork-cutoff"] = teamstore.Turn{
					ID:          "fork-cutoff",
					SessionID:   parent.ID,
					CodexTurnID: "codex-cutoff",
					Status:      teamstore.TurnStatusCompleted,
					CompletedAt: cutoffAt,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed cutoff: %v", err)
			}
			if sqliteMode {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			bridge := &Bridge{store: store}
			gotParent, gotCutoff, err := bridge.forkParentAndCutoff(ctx, teamstore.ForkOperation{
				ID:                "fork-operation",
				ParentSessionID:   parent.ID,
				ParentThreadID:    parent.CodexThreadID,
				CutoffTurnID:      "fork-cutoff",
				CutoffCodexTurnID: "codex-cutoff",
			})
			if err != nil {
				t.Fatalf("forkParentAndCutoff: %v", err)
			}
			if gotParent.ID != parent.ID || gotParent.CodexThreadID != parent.CodexThreadID || gotCutoff.ID != "fork-cutoff" {
				t.Fatalf("recovered parent=%#v cutoff=%#v", gotParent, gotCutoff)
			}
		})
	}
}

func TestForkHistoryPlanV1RetainsLegacyManifestShape(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	parent := &Session{ID: "v1-parent", Status: "active", CodexThreadID: "v1-parent-thread", Cwd: t.TempDir()}
	if _, _, err := store.CreateSession(ctx, teamstore.SessionContext{ID: parent.ID, Status: teamstore.SessionStatusActive, CodexThreadID: parent.CodexThreadID, Cwd: parent.Cwd}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	transcriptPath := filepath.Join(t.TempDir(), "v1-parent.jsonl")
	transcript := strings.Join([]string{
		`{"type":"thread.started","thread_id":"v1-parent-thread"}`,
		`{"type":"turn.started","turn_id":"v1-cutoff-turn"}`,
		`{"type":"event_msg","payload":{"type":"agent_message","id":"v1-status","message":"v1 status","phase":"commentary"}}`,
		`{"type":"response_item","payload":{"id":"v1-final","type":"message","role":"assistant","phase":"final_answer","internal_chat_message_metadata_passthrough":{"turn_id":"v1-cutoff-turn"},"content":[{"type":"output_text","text":"v1 final"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		now := time.Now().Add(-time.Minute)
		state.Turns["v1-cutoff"] = teamstore.Turn{ID: "v1-cutoff", SessionID: parent.ID, CodexTurnID: "v1-cutoff-turn", Status: teamstore.TurnStatusCompleted, CompletedAt: now}
		state.ImportCheckpoints[transcriptCheckpointID(parent.ID)] = teamstore.ImportCheckpoint{ID: transcriptCheckpointID(parent.ID), SessionID: parent.ID, SourcePath: transcriptPath}
		return nil
	}); err != nil {
		t.Fatalf("seed transcript checkpoint: %v", err)
	}
	bridge := &Bridge{store: store}
	cutoff := teamstore.Turn{ID: "v1-cutoff", SessionID: parent.ID, CodexTurnID: "v1-cutoff-turn", Status: teamstore.TurnStatusCompleted}
	v1, err := bridge.materializeForkHistoryForVersion(ctx, parent, cutoff, 1)
	if err != nil {
		t.Fatalf("materialize v1: %v", err)
	}
	if v1.Metadata.HistoryPlanVersion != 1 || len(v1.Items) == 0 {
		t.Fatalf("v1 snapshot metadata/items = %#v/%d", v1.Metadata, len(v1.Items))
	}
	for _, item := range v1.Items {
		if item.SourceEndRecordID != "" {
			t.Fatalf("legacy v1 item unexpectedly has source end id: %#v", item)
		}
	}
	if got, want := teamstore.ForkHistoryManifestHashForPlanVersion(v1.Items, 1), teamstore.ForkHistoryManifestHash(v1.Items); got != want {
		t.Fatalf("v1 manifest hash = %q, legacy hash = %q", got, want)
	}
	v2, err := bridge.materializeForkHistoryForVersion(ctx, parent, cutoff, 2)
	if err != nil {
		t.Fatalf("materialize v2: %v", err)
	}
	foundRange := false
	for _, item := range v2.Items {
		if item.SourceEndRecordID != "" {
			foundRange = true
			break
		}
	}
	if !foundRange {
		t.Fatalf("v2 snapshot did not record a source end range: %#v", v2.Items)
	}
}

func TestForkLegacyV1OperationRecoversAfterStoreReopen(t *testing.T) {
	ctx := context.Background()
	statePath := filepath.Join(t.TempDir(), "legacy-fork-state.json")
	store, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = store.Close() }()
	parent := &Session{
		ID:            "legacy-recovery-parent",
		Status:        string(teamstore.SessionStatusActive),
		ChatID:        "legacy-recovery-parent-chat",
		CodexThreadID: "legacy-recovery-parent-thread",
		Cwd:           t.TempDir(),
	}
	if _, created, err := store.CreateSession(ctx, teamstore.SessionContext{
		ID:            parent.ID,
		Status:        teamstore.SessionStatusActive,
		TeamsChatID:   parent.ChatID,
		CodexThreadID: parent.CodexThreadID,
		Cwd:           parent.Cwd,
	}); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	transcriptPath := filepath.Join(t.TempDir(), "legacy-recovery.jsonl")
	transcript := strings.Join([]string{
		`{"type":"thread.started","thread_id":"legacy-recovery-parent-thread"}`,
		`{"type":"turn.started","turn_id":"legacy-recovery-turn"}`,
		`{"type":"response_item","payload":{"id":"legacy-recovery-final","type":"message","role":"assistant","phase":"final_answer","internal_chat_message_metadata_passthrough":{"turn_id":"legacy-recovery-turn"},"content":[{"type":"output_text","text":"legacy history"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	cutoffAt := time.Now().Add(-time.Minute)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Turns["legacy-recovery-cutoff"] = teamstore.Turn{
			ID:          "legacy-recovery-cutoff",
			SessionID:   parent.ID,
			CodexTurnID: "legacy-recovery-turn",
			Status:      teamstore.TurnStatusCompleted,
			CompletedAt: cutoffAt,
		}
		state.ImportCheckpoints[transcriptCheckpointID(parent.ID)] = teamstore.ImportCheckpoint{
			ID:         transcriptCheckpointID(parent.ID),
			SessionID:  parent.ID,
			SourcePath: transcriptPath,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy transcript: %v", err)
	}
	op, created, err := store.BeginFork(ctx, teamstore.ForkBeginRequest{
		OperationID:       "legacy-recovery-operation",
		ParentSessionID:   parent.ID,
		ParentChatID:      parent.ChatID,
		ParentThreadID:    parent.CodexThreadID,
		ChildSession:      teamstore.SessionContext{ID: "legacy-recovery-child", Status: teamstore.SessionStatusStaging},
		CutoffTurnID:      "legacy-recovery-cutoff",
		CutoffCodexTurnID: "legacy-recovery-turn",
	})
	if err != nil || !created || op.HistoryPlanVersion != 1 {
		t.Fatalf("legacy BeginFork = %#v created=%v err=%v, want v1", op, created, err)
	}
	bridge := &Bridge{store: store}
	cutoff := teamstore.Turn{ID: "legacy-recovery-cutoff", SessionID: parent.ID, CodexTurnID: "legacy-recovery-turn", Status: teamstore.TurnStatusCompleted}
	snapshot, err := bridge.materializeForkHistoryForVersion(ctx, parent, cutoff, 1)
	if err != nil {
		t.Fatalf("materialize legacy v1: %v", err)
	}
	if _, err := bridge.saveForkManifest(ctx, op.ID, snapshot.Items, snapshot.Metadata); err != nil {
		t.Fatalf("save legacy manifest: %v", err)
	}
	if _, err := store.RecordForkCodexChild(ctx, op.ID, "legacy-recovery-child-thread"); err != nil {
		t.Fatalf("RecordForkCodexChild: %v", err)
	}
	if _, err := store.StageForkChat(ctx, op.ID, "legacy-recovery-child-chat", "https://teams.example/legacy-recovery", "legacy recovery", op.ID, time.Now(), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("StageForkChat: %v", err)
	}
	queued, err := store.QueueForkHistory(ctx, op.ID, "History import complete")
	if err != nil || len(queued) == 0 {
		t.Fatalf("QueueForkHistory = %#v err=%v", queued, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close before recovery: %v", err)
	}
	store, err = teamstore.Open(statePath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	bridge.store = store
	for _, message := range queued {
		if _, err := store.MarkOutboxSendAttempt(ctx, message.ID); err != nil {
			t.Fatalf("MarkOutboxSendAttempt(%s): %v", message.ID, err)
		}
		if _, err := store.MarkOutboxSent(ctx, message.ID, message.ID+":teams"); err != nil {
			t.Fatalf("MarkOutboxSent(%s): %v", message.ID, err)
		}
	}
	recovered, verified, err := store.RefreshForkHistory(ctx, op.ID)
	if err != nil || !verified || recovered.Phase != teamstore.ForkPhaseHistoryVerified {
		t.Fatalf("legacy recovery = %#v verified=%v err=%v", recovered, verified, err)
	}
	if recovered.HistoryPlanVersion != 1 {
		t.Fatalf("legacy recovery changed plan version to %d", recovered.HistoryPlanVersion)
	}
}

func TestForkReconcileAmbiguousNativeResponseAdoptsOnlyUniqueChild(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	parent := teamstore.SessionContext{
		ID:            "ambiguous-parent",
		Status:        teamstore.SessionStatusActive,
		TeamsChatID:   "ambiguous-parent-chat",
		CodexThreadID: "ambiguous-parent-thread",
	}
	if _, _, err := store.CreateSession(ctx, parent); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Turns["ambiguous-cutoff"] = teamstore.Turn{
			ID:          "ambiguous-cutoff",
			SessionID:   parent.ID,
			CodexTurnID: "ambiguous-codex-cutoff",
			Status:      teamstore.TurnStatusCompleted,
			CompletedAt: time.Now().Add(-time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed cutoff: %v", err)
	}
	op, _, err := store.BeginFork(ctx, teamstore.ForkBeginRequest{
		OperationID:       "ambiguous-operation",
		ParentSessionID:   parent.ID,
		ParentThreadID:    parent.CodexThreadID,
		ChildSession:      teamstore.SessionContext{ID: "ambiguous-child", Status: teamstore.SessionStatusStaging},
		CutoffTurnID:      "ambiguous-cutoff",
		CutoffCodexTurnID: "ambiguous-codex-cutoff",
		ForkWindowStart:   time.Now().Add(-time.Minute),
		ForkWindowEnd:     time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("BeginFork: %v", err)
	}
	if _, err := store.UpdateForkOperation(ctx, op.ID, func(current *teamstore.ForkOperation) error {
		current.Phase = teamstore.ForkPhaseBlockedAmbiguous
		return nil
	}); err != nil {
		t.Fatalf("mark ambiguous: %v", err)
	}
	executor := &forkReconcileTestExecutor{result: ForkReconcileResult{
		MatchCount: 1,
		Result:     ForkResult{CodexThreadID: "recovered-child-thread"},
	}}
	bridge := &Bridge{store: store, executor: executor}
	if err := bridge.reconcileForkOperation(ctx, teamstore.ForkOperation{
		ID:                    op.ID,
		ParentSessionID:       parent.ID,
		ParentThreadID:        parent.CodexThreadID,
		CutoffTurnID:          "ambiguous-cutoff",
		CutoffCodexTurnID:     "ambiguous-codex-cutoff",
		Phase:                 teamstore.ForkPhaseBlockedAmbiguous,
		NativeForkWindowStart: time.Now().Add(-time.Minute),
		NativeForkWindowEnd:   time.Now().Add(time.Minute),
	}); err == nil {
		t.Fatal("reconcileForkOperation unexpectedly completed without a Graph client")
	}
	if executor.calls != 1 {
		t.Fatalf("reconciler calls = %d, want one read-only reconciliation", executor.calls)
	}
	recovered, ok, err := store.ForkOperation(ctx, op.ID)
	if err != nil || !ok {
		t.Fatalf("ForkOperation = %#v ok=%v err=%v", recovered, ok, err)
	}
	if recovered.ChildThreadID != "recovered-child-thread" || recovered.Phase != teamstore.ForkPhaseBlockedAmbiguous {
		t.Fatalf("recovered operation = %#v", recovered)
	}
}

func TestForkRecoveryTreatsParentFencedIntentAsAmbiguous(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	parent := teamstore.SessionContext{
		ID:            "intent-parent",
		Status:        teamstore.SessionStatusActive,
		TeamsChatID:   "intent-parent-chat",
		CodexThreadID: "intent-parent-thread",
	}
	if _, _, err := store.CreateSession(ctx, parent); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Turns["intent-cutoff"] = teamstore.Turn{
			ID:          "intent-cutoff",
			SessionID:   parent.ID,
			CodexTurnID: "intent-codex-cutoff",
			Status:      teamstore.TurnStatusCompleted,
			CompletedAt: time.Now().Add(-time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed cutoff: %v", err)
	}
	op, _, err := store.BeginFork(ctx, teamstore.ForkBeginRequest{
		OperationID:       "intent-operation",
		ParentSessionID:   parent.ID,
		ParentThreadID:    parent.CodexThreadID,
		ChildSession:      teamstore.SessionContext{ID: "intent-child", Status: teamstore.SessionStatusStaging},
		CutoffTurnID:      "intent-cutoff",
		CutoffCodexTurnID: "intent-codex-cutoff",
		ForkWindowStart:   time.Now().Add(-time.Minute),
		ForkWindowEnd:     time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("BeginFork: %v", err)
	}
	if _, err := store.UpdateForkOperation(ctx, op.ID, func(current *teamstore.ForkOperation) error {
		current.Phase = teamstore.ForkPhaseParentFenced
		current.NativeForkIntentAt = time.Now().Add(-time.Second)
		return nil
	}); err != nil {
		t.Fatalf("persist native fork intent: %v", err)
	}
	executor := &forkReconcileTestExecutor{result: ForkReconcileResult{
		MatchCount: 1,
		Result:     ForkResult{CodexThreadID: "intent-recovered-child"},
	}}
	bridge := &Bridge{store: store, executor: executor}
	durable, ok, err := store.ForkOperation(ctx, op.ID)
	if err != nil || !ok {
		t.Fatalf("ForkOperation = %#v ok=%v err=%v", durable, ok, err)
	}
	if err := bridge.reconcileForkOperation(ctx, durable); err == nil {
		t.Fatal("reconcileForkOperation unexpectedly completed without a Graph client")
	}
	if executor.calls != 1 {
		t.Fatalf("reconciler calls = %d, want one read-only reconciliation", executor.calls)
	}
	recovered, ok, err := store.ForkOperation(ctx, op.ID)
	if err != nil || !ok {
		t.Fatalf("recovered ForkOperation = %#v ok=%v err=%v", recovered, ok, err)
	}
	if recovered.ChildThreadID != "intent-recovered-child" || recovered.Phase != teamstore.ForkPhaseBlockedAmbiguous {
		t.Fatalf("recovered operation = %#v", recovered)
	}
}
