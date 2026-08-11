package teams

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

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

type forkCountingExecutor struct {
	forkCalls int
}

func (e *forkCountingExecutor) Run(context.Context, *Session, string) (ExecutionResult, error) {
	return ExecutionResult{}, fmt.Errorf("not used by fork staging test")
}

func (e *forkCountingExecutor) ForkThread(context.Context, *Session, string) (ForkResult, error) {
	e.forkCalls++
	return ForkResult{CodexThreadID: "unexpected-child-thread"}, nil
}

func TestForkWorkSessionDoesNotCallNativeForkWhenParentExecutionIsUnresolved(t *testing.T) {
	ctx := context.Background()
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			graph, sent := newBridgeTestGraph(t)
			store := newBridgeTestStore(t)
			const (
				parentID      = "fork-staged-parent"
				parentChatID  = "fork-staged-chat"
				parentThread  = "fork-staged-thread"
				cutoffTurnID  = "fork-staged-cutoff"
				cutoffCodexID = "fork-staged-codex"
			)
			now := time.Now()
			machine := MachineRecordForUser(User{ID: "user-1", UserPrincipalName: "user@example.test"}, ScopeIdentityForUser(User{ID: "user-1", UserPrincipalName: "user@example.test"}))
			lease := teamstore.ControlLease{HolderMachineID: machine.ID, Generation: 1, LeaseUntil: now.Add(time.Hour), Status: teamstore.ControlLeaseStatusActive}
			if _, created, err := store.CreateSession(ctx, teamstore.SessionContext{
				ID: parentID, Status: teamstore.SessionStatusActive, TeamsChatID: parentChatID, CodexThreadID: parentThread,
				CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
			}); err != nil || !created {
				t.Fatalf("CreateSession created=%v err=%v", created, err)
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ControlLease = lease
				state.Turns[cutoffTurnID] = teamstore.Turn{
					ID: cutoffTurnID, SessionID: parentID, CodexTurnID: cutoffCodexID,
					Status: teamstore.TurnStatusCompleted, CompletedAt: now.Add(-time.Minute),
				}
				checkpointID := "transcript:" + parentID
				state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{
					ID: checkpointID, SessionID: parentID, Status: "blocked",
					UnresolvedExecution: &teamstore.ExecutionAnchor{
						SessionID: parentID, ThreadID: parentThread, OuterTurnID: "fork-staged-old-turn", CodexTurnID: "fork-staged-old-codex",
						State: "unresolved", Generation: 3,
					},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed parent fence: %v", err)
			}
			if sqliteMode {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			executor := &forkCountingExecutor{}
			bridge := newBridgeTestBridge(graph, store, executor)
			bridge.machine = machine
			bridge.setControlLease(lease)
			parent := &Session{ID: parentID, ChatID: parentChatID, Status: "active", CodexThreadID: parentThread}
			command := ChatMessage{ID: "fork-staged-command", ChatID: parentChatID}
			command.Body.Content = "fork"
			if err := bridge.forkWorkSession(ctx, parent, command); err != nil {
				t.Fatalf("forkWorkSession: %v", err)
			}
			if executor.forkCalls != 0 {
				t.Fatalf("ForkThread calls = %d, want zero while parent execution is unresolved", executor.forkCalls)
			}
			op, ok, err := store.ParentFork(ctx, parentID)
			if err != nil || !ok {
				t.Fatalf("ParentFork = %#v ok=%v err=%v", op, ok, err)
			}
			if op.NativeForkIntentAt.IsZero() == false {
				t.Fatalf("staged operation recorded native intent: %#v", op)
			}
			if len(*sent) == 0 {
				t.Fatal("fork staging notice was not sent")
			}
		})
	}
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
