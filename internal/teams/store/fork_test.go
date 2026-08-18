package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestForkParentFenceBlocksNewInputsAndClaims(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)

			parent := testSession()
			parent.ID = "parent"
			parent.Status = SessionStatusActive
			parent.TeamsChatID = "parent-chat"
			parent.CodexThreadID = "parent-thread"
			if _, _, err := store.CreateSession(ctx, parent); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			seedCompletedForkCutoff(t, store, parent.ID, "cutoff", "codex-cutoff", time.Now().Add(-time.Minute))
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}

			op, created, err := store.BeginFork(ctx, ForkBeginRequest{
				OperationID:       "fork-parent-fence",
				ParentSessionID:   parent.ID,
				ParentChatID:      parent.TeamsChatID,
				ParentThreadID:    parent.CodexThreadID,
				ChildSession:      SessionContext{ID: "child", Status: SessionStatusStaging},
				CutoffTurnID:      "cutoff",
				CutoffCodexTurnID: "codex-cutoff",
			})
			if err != nil {
				t.Fatalf("BeginFork: %v", err)
			}
			if !created || op.Phase != ForkPhaseParentFenced {
				t.Fatalf("BeginFork = %#v, created=%v", op, created)
			}
			if op.HistoryNamespace != "fork-history:"+op.ID || op.NativeForkWindowEnd.Before(op.NativeForkWindowStart) {
				t.Fatalf("fork durability metadata = %#v", op)
			}

			duplicate, created, err := store.BeginFork(ctx, ForkBeginRequest{
				OperationID:     op.ID,
				ParentSessionID: parent.ID,
				ChildSession:    SessionContext{ID: "child"},
			})
			if err != nil || created || duplicate.ID != op.ID {
				t.Fatalf("duplicate BeginFork = %#v, created=%v, err=%v", duplicate, created, err)
			}
			_, _, err = store.BeginFork(ctx, ForkBeginRequest{
				OperationID:     "fork-parent-fence-other",
				ParentSessionID: parent.ID,
				ChildSession:    SessionContext{ID: "child-other"},
			})
			if !errors.Is(err, ErrForkAlreadyInProgress) {
				t.Fatalf("second operation error = %v, want ErrForkAlreadyInProgress", err)
			}
			if err := store.Update(ctx, func(state *State) error {
				state.Turns["queued-before-fork"] = Turn{
					ID:        "queued-before-fork",
					SessionID: parent.ID,
					Status:    TurnStatusQueued,
					QueuedAt:  time.Now(),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed queued turn after fence: %v", err)
			}

			_, _, err = store.PersistInbound(ctx, InboundEvent{
				ID:             "blocked-inbound",
				SessionID:      parent.ID,
				TeamsChatID:    parent.TeamsChatID,
				TeamsMessageID: "blocked-message",
				Status:         InboundStatusPersisted,
			})
			if !errors.Is(err, ErrForkParentFenced) {
				t.Fatalf("blocked PersistInbound error = %v, want ErrForkParentFenced", err)
			}

			if _, _, err := store.PersistInbound(ctx, InboundEvent{
				ID:             "deferred-inbound",
				SessionID:      parent.ID,
				TeamsChatID:    parent.TeamsChatID,
				TeamsMessageID: "deferred-message",
				Status:         InboundStatusDeferred,
			}); err != nil {
				t.Fatalf("deferred PersistInbound: %v", err)
			}

			_, _, err = store.QueueTurn(ctx, Turn{ID: "blocked-turn", SessionID: parent.ID, Status: TurnStatusQueued})
			if !errors.Is(err, ErrForkParentFenced) {
				t.Fatalf("blocked QueueTurn error = %v, want ErrForkParentFenced", err)
			}
			claimed, ok, err := store.ClaimNextQueuedTurn(ctx, parent.ID)
			if err != nil {
				t.Fatalf("ClaimNextQueuedTurn: %v", err)
			}
			if ok || claimed.ID != "" {
				t.Fatalf("ClaimNextQueuedTurn = %#v, claimed=%v while fenced", claimed, ok)
			}
		})
	}
}

func TestBeginForkSelectsAndValidatesLatestCompletedCutoffInStoreCAS(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			parent := testSession()
			parent.ID = "atomic-cutoff-parent"
			parent.Status = SessionStatusActive
			if _, _, err := store.CreateSession(ctx, parent); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			first := time.Now().Add(-2 * time.Minute)
			seedCompletedForkCutoff(t, store, parent.ID, "cutoff-1", "codex-cutoff-1", first)
			seedCompletedForkCutoff(t, store, parent.ID, "cutoff-2", "codex-cutoff-2", first.Add(time.Minute))
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}

			op, created, err := store.BeginFork(ctx, ForkBeginRequest{
				OperationID:     "atomic-cutoff-operation",
				ParentSessionID: parent.ID,
				ChildSession:    SessionContext{ID: "atomic-cutoff-child", Status: SessionStatusStaging},
			})
			if err != nil || !created {
				t.Fatalf("BeginFork = %#v created=%v err=%v", op, created, err)
			}
			if op.CutoffTurnID != "cutoff-2" || op.CutoffCodexTurnID != "codex-cutoff-2" {
				t.Fatalf("selected cutoff = %#v, want latest completed turn", op)
			}
			cutoff, ok, err := store.ForkCutoff(ctx, op.ID)
			if err != nil || !ok || cutoff.ID != "cutoff-2" {
				t.Fatalf("ForkCutoff = %#v ok=%v err=%v", cutoff, ok, err)
			}

			if _, err := store.UpdateForkOperation(ctx, op.ID, func(current *ForkOperation) error {
				current.Phase = ForkPhaseFailed
				return nil
			}); err != nil {
				t.Fatalf("terminalize operation: %v", err)
			}
			_, _, err = store.BeginFork(ctx, ForkBeginRequest{
				OperationID:       "stale-cutoff-operation",
				ParentSessionID:   parent.ID,
				ChildSession:      SessionContext{ID: "stale-cutoff-child", Status: SessionStatusStaging},
				CutoffTurnID:      "cutoff-1",
				CutoffCodexTurnID: "codex-cutoff-1",
			})
			if err == nil || !strings.Contains(err.Error(), "not the latest completed turn") {
				t.Fatalf("stale cutoff error = %v, want latest-cutoff rejection", err)
			}
		})
	}
}

func TestMarkForkNativeIntentRequiresClearParentExecutionAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			store := newTestStore(t)
			parent := testSession()
			parent.ID = "fork-intent-parent"
			parent.Status = SessionStatusActive
			parent.TeamsChatID = "fork-intent-chat"
			parent.CodexThreadID = "fork-intent-thread"
			if _, _, err := store.CreateSession(ctx, parent); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			seedCompletedForkCutoff(t, store, parent.ID, "fork-intent-cutoff", "fork-intent-codex", time.Now().Add(-time.Minute))
			if sqliteMode {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			op, created, err := store.BeginFork(ctx, ForkBeginRequest{
				OperationID:       "fork-intent-operation",
				ParentSessionID:   parent.ID,
				ParentChatID:      parent.TeamsChatID,
				ParentThreadID:    parent.CodexThreadID,
				ChildSession:      SessionContext{ID: "fork-intent-child", Status: SessionStatusStaging},
				CutoffTurnID:      "fork-intent-cutoff",
				CutoffCodexTurnID: "fork-intent-codex",
			})
			if err != nil || !created {
				t.Fatalf("BeginFork = %#v created=%v err=%v", op, created, err)
			}
			checkpointID := sessionTranscriptCheckpointID(parent.ID)
			if err := store.Update(ctx, func(state *State) error {
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
					ID: checkpointID, SessionID: parent.ID,
					UnresolvedExecution: &ExecutionAnchor{SessionID: parent.ID, State: "unresolved", Generation: 4},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed unresolved execution: %v", err)
			}
			if _, err := store.MarkForkNativeIntentIfParentExecutionClear(ctx, op.ID); !errors.Is(err, ErrForkParentFenced) {
				t.Fatalf("blocked native intent error = %v, want ErrForkParentFenced", err)
			}
			blocked, ok, err := store.ForkOperation(ctx, op.ID)
			if err != nil || !ok {
				t.Fatalf("blocked ForkOperation = %#v ok=%v err=%v", blocked, ok, err)
			}
			if !blocked.NativeForkIntentAt.IsZero() {
				t.Fatalf("blocked fork recorded native intent at %s", blocked.NativeForkIntentAt)
			}
			if err := store.Update(ctx, func(state *State) error {
				checkpoint := state.ImportCheckpoints[checkpointID]
				checkpoint.UnresolvedExecution = nil
				state.ImportCheckpoints[checkpointID] = checkpoint
				return nil
			}); err != nil {
				t.Fatalf("clear unresolved execution: %v", err)
			}
			marked, err := store.MarkForkNativeIntentIfParentExecutionClear(ctx, op.ID)
			if err != nil || marked.NativeForkIntentAt.IsZero() {
				t.Fatalf("clear native intent = %#v err=%v", marked, err)
			}
		})
	}
}

func TestClaimForkOperationRequiresCurrentLeaseAndSupportsTakeover(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			parent := testSession()
			parent.ID = "claim-parent"
			parent.Status = SessionStatusActive
			if _, _, err := store.CreateSession(ctx, parent); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			seedCompletedForkCutoff(t, store, parent.ID, "claim-cutoff", "claim-codex-cutoff", time.Now().Add(-time.Minute))
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}
			now := time.Now()
			if err := store.Update(ctx, func(state *State) error {
				state.ControlLease = ControlLease{
					HolderMachineID: "machine-a",
					Generation:      7,
					LeaseUntil:      now.Add(time.Hour),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed control lease: %v", err)
			}
			op, _, err := store.BeginFork(ctx, ForkBeginRequest{
				OperationID:     "claim-operation",
				ParentSessionID: parent.ID,
				ChildSession:    SessionContext{ID: "claim-child", Status: SessionStatusStaging},
			})
			if err != nil {
				t.Fatalf("BeginFork: %v", err)
			}
			claimed, err := store.ClaimForkOperation(ctx, op.ID, "machine-a", 7, now)
			if err != nil || claimed.OwnerMachineID != "machine-a" || claimed.OwnerLeaseGeneration != 7 {
				t.Fatalf("initial ClaimForkOperation = %#v err=%v", claimed, err)
			}
			if _, err := store.ClaimForkOperation(ctx, op.ID, "machine-b", 8, now); !errors.Is(err, ErrForkOwnerLease) {
				t.Fatalf("stale ClaimForkOperation error = %v, want ErrForkOwnerLease", err)
			}
			if err := store.Update(ctx, func(state *State) error {
				state.ControlLease.HolderMachineID = "machine-b"
				state.ControlLease.Generation = 8
				return nil
			}); err != nil {
				t.Fatalf("take over control lease: %v", err)
			}
			takenOver, err := store.ClaimForkOperation(ctx, op.ID, "machine-b", 8, now)
			if err != nil || takenOver.OwnerMachineID != "machine-b" || takenOver.OwnerLeaseGeneration != 8 {
				t.Fatalf("takeover ClaimForkOperation = %#v err=%v", takenOver, err)
			}
		})
	}
}

func TestOwnedForkMutationStopsAfterLeaseTakeoverAndActivatedStaysFenced(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			parent := testSession()
			parent.ID = "owned-mutation-parent"
			parent.Status = SessionStatusActive
			if _, _, err := store.CreateSession(ctx, parent); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			seedCompletedForkCutoff(t, store, parent.ID, "owned-cutoff", "owned-codex-cutoff", time.Now().Add(-time.Minute))
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}
			now := time.Now()
			if err := store.Update(ctx, func(state *State) error {
				state.ControlLease = ControlLease{HolderMachineID: "machine-a", Generation: 7, LeaseUntil: now.Add(time.Hour)}
				return nil
			}); err != nil {
				t.Fatalf("seed lease: %v", err)
			}
			op, _, err := store.BeginFork(ctx, ForkBeginRequest{
				OperationID:          "owned-mutation-operation",
				ParentSessionID:      parent.ID,
				ChildSession:         SessionContext{ID: "owned-mutation-child", Status: SessionStatusStaging},
				OwnerMachineID:       "machine-a",
				OwnerLeaseGeneration: 7,
			})
			if err != nil {
				t.Fatalf("BeginFork: %v", err)
			}
			if _, err := store.ClaimForkOperation(ctx, op.ID, "machine-a", 7, now); err != nil {
				t.Fatalf("claim machine-a: %v", err)
			}
			if _, err := store.UpdateForkOperationOwned(ctx, op.ID, ForkOwnerLease{MachineID: "machine-a", Generation: 7}, func(current *ForkOperation) error {
				current.LastError = "machine-a update"
				return nil
			}); err != nil {
				t.Fatalf("owned update before takeover: %v", err)
			}
			if err := store.Update(ctx, func(state *State) error {
				state.ControlLease.HolderMachineID = "machine-b"
				state.ControlLease.Generation = 8
				state.ControlLease.LeaseUntil = now.Add(time.Hour)
				return nil
			}); err != nil {
				t.Fatalf("take over lease: %v", err)
			}
			if _, err := store.ClaimForkOperation(ctx, op.ID, "machine-b", 8, now); err != nil {
				t.Fatalf("claim machine-b: %v", err)
			}
			if _, err := store.UpdateForkOperationOwned(ctx, op.ID, ForkOwnerLease{MachineID: "machine-a", Generation: 7}, func(current *ForkOperation) error {
				current.LastError = "stale write"
				return nil
			}); !errors.Is(err, ErrForkOwnerLease) {
				t.Fatalf("stale owned update error = %v, want ErrForkOwnerLease", err)
			}
			if err := store.ValidateForkOperationOwner(ctx, op.ID, ForkOwnerLease{MachineID: "machine-a", Generation: 7}); !errors.Is(err, ErrForkOwnerLease) {
				t.Fatalf("stale owner validation error = %v, want ErrForkOwnerLease", err)
			}
			if ForkPhaseTerminal(ForkPhaseActivated) {
				t.Fatal("activated must remain recoverable until link-sent")
			}
			if _, err := store.UpdateForkOperationOwned(ctx, op.ID, ForkOwnerLease{MachineID: "machine-b", Generation: 8}, func(current *ForkOperation) error {
				current.Phase = ForkPhaseActivated
				return nil
			}); err != nil {
				t.Fatalf("activate phase update: %v", err)
			}
			if _, fenced, err := store.ParentFork(ctx, parent.ID); err != nil || !fenced {
				t.Fatalf("ParentFork after activated = fenced=%v err=%v, want fenced", fenced, err)
			}
		})
	}
}

func TestForkHistoryRequiresSentProofBeforeActivation(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)

			parent := testSession()
			parent.ID = "parent-history"
			parent.Status = SessionStatusActive
			parent.TeamsChatID = "parent-history-chat"
			parent.CodexThreadID = "parent-history-thread"
			if _, _, err := store.CreateSession(ctx, parent); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			seedCompletedForkCutoff(t, store, parent.ID, "cutoff", "codex-cutoff", time.Now().Add(-time.Minute))
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}
			op, created, err := store.BeginFork(ctx, ForkBeginRequest{
				OperationID:       "fork-history-proof",
				ParentSessionID:   parent.ID,
				ParentChatID:      parent.TeamsChatID,
				ParentThreadID:    parent.CodexThreadID,
				ChildSession:      SessionContext{ID: "history-child", Status: SessionStatusStaging},
				CutoffTurnID:      "cutoff",
				CutoffCodexTurnID: "codex-cutoff",
			})
			if err != nil || !created {
				t.Fatalf("BeginFork = %#v, created=%v, err=%v", op, created, err)
			}
			manifestOp, err := store.SaveForkManifestWithMetadata(ctx, op.ID, []ForkHistoryItem{{
				Ordinal:           0,
				SourceRecordID:    "record-1",
				SourceLine:        3,
				SourceStartOffset: 10,
				SourceOffset:      42,
				SourceTurnID:      "turn-1",
				Kind:              "user",
				RenderedBody:      "Please continue from here.",
			}}, ForkManifestMetadata{
				SourcePath:              "transcript.jsonl",
				SourceFingerprint:       "fingerprint",
				CutoffSourceRecordID:    "record-1",
				CutoffSourceLine:        3,
				CutoffSourceStartOffset: 10,
				CutoffSourceOffset:      42,
				SourcePrefixHash:        "prefix-hash",
			})
			if err != nil {
				t.Fatalf("SaveForkManifestWithMetadata: %v", err)
			}
			if manifestOp.CutoffSourceOffset != 42 || manifestOp.SourcePrefixHash != "prefix-hash" {
				t.Fatalf("manifest metadata = %#v", manifestOp)
			}
			sameManifest, err := store.SaveForkManifestWithMetadata(ctx, op.ID, []ForkHistoryItem{{
				Ordinal:           0,
				SourceRecordID:    "record-1",
				SourceLine:        3,
				SourceStartOffset: 10,
				SourceOffset:      42,
				SourceTurnID:      "turn-1",
				Kind:              "user",
				RenderedBody:      "Please continue from here.",
			}}, ForkManifestMetadata{
				SourcePath:              "transcript.jsonl",
				SourceFingerprint:       "fingerprint",
				CutoffSourceRecordID:    "record-1",
				CutoffSourceLine:        3,
				CutoffSourceStartOffset: 10,
				CutoffSourceOffset:      42,
				SourcePrefixHash:        "prefix-hash",
			})
			if err != nil || sameManifest.ManifestHash != manifestOp.ManifestHash {
				t.Fatalf("idempotent manifest save = %#v err=%v", sameManifest, err)
			}
			_, err = store.SaveForkManifestWithMetadata(ctx, op.ID, []ForkHistoryItem{{
				Ordinal:        0,
				SourceRecordID: "record-1",
				RenderedBody:   "mutated history",
			}}, ForkManifestMetadata{SourcePath: "transcript.jsonl", SourceFingerprint: "fingerprint"})
			if err == nil || !strings.Contains(err.Error(), "manifest is immutable") {
				t.Fatalf("mutated manifest save error = %v, want immutable rejection", err)
			}
			if _, err := store.RecordForkCodexChild(ctx, op.ID, "child-thread"); err != nil {
				t.Fatalf("RecordForkCodexChild: %v", err)
			}
			if _, err := store.StageForkChat(ctx, op.ID, "child-chat", "https://teams.example/child-chat", "child topic", "graph-window", time.Now(), time.Now().Add(time.Hour)); err != nil {
				t.Fatalf("StageForkChat: %v", err)
			}
			extra, _, err := store.QueueOutbox(ctx, OutboxMessage{
				ID:                   "fork-extra-history",
				SessionID:            "history-child",
				TeamsChatID:          "child-chat",
				Kind:                 "fork-history-extra",
				Body:                 "extra proof row",
				ForkOperationID:      op.ID,
				ForkHistoryNamespace: op.HistoryNamespace,
				ForkRole:             "history-extra",
			})
			if err != nil {
				t.Fatalf("QueueOutbox extra history: %v", err)
			}
			queued, err := store.QueueForkHistory(ctx, op.ID, "history complete")
			if err != nil || len(queued) != 2 {
				t.Fatalf("QueueForkHistory = %#v, err=%v", queued, err)
			}

			var historyMessageID string
			var markerID string
			for _, msg := range queued {
				if msg.ForkRole == "complete-marker" {
					markerID = msg.ID
				} else {
					historyMessageID = msg.ID
				}
			}
			if historyMessageID == "" || markerID == "" {
				t.Fatalf("fork outbox rows = %#v", queued)
			}
			if _, err := store.MarkOutboxSendAttempt(ctx, historyMessageID); err != nil {
				t.Fatalf("staged history MarkOutboxSendAttempt: %v", err)
			}
			if _, err := store.MarkOutboxSent(ctx, historyMessageID, "history-message"); err != nil {
				t.Fatalf("MarkOutboxSent history: %v", err)
			}
			if err := store.MarkForkHistoryDuplicateSettled(ctx, op.ID, historyMessageID, "history-message"); err != nil {
				t.Fatalf("MarkForkHistoryDuplicateSettled history: %v", err)
			}
			if err := store.Update(ctx, func(state *State) error {
				marker := state.OutboxMessages[markerID]
				marker.Status = OutboxStatusAccepted
				marker.TeamsMessageID = "marker-accepted"
				marker.ForkRole = "not-a-marker"
				state.OutboxMessages[markerID] = marker
				return nil
			}); err != nil {
				t.Fatalf("mark marker accepted: %v", err)
			}
			if _, verified, err := store.RefreshForkHistory(ctx, op.ID); err != nil || verified {
				t.Fatalf("RefreshForkHistory with accepted marker = verified=%v err=%v", verified, err)
			}
			if err := store.Update(ctx, func(state *State) error {
				marker := state.OutboxMessages[markerID]
				marker.ForkRole = "complete-marker"
				state.OutboxMessages[markerID] = marker
				return nil
			}); err != nil {
				t.Fatalf("restore marker role: %v", err)
			}
			if _, err := store.MarkOutboxSent(ctx, markerID, "marker-accepted"); err != nil {
				t.Fatalf("MarkOutboxSent marker: %v", err)
			}
			if _, verified, err := store.RefreshForkHistory(ctx, op.ID); err != nil || verified {
				t.Fatalf("RefreshForkHistory with unsent extra history = verified=%v err=%v", verified, err)
			}
			if _, err := store.MarkOutboxSendAttempt(ctx, extra.ID); err != nil {
				t.Fatalf("extra history send attempt: %v", err)
			}
			if _, err := store.MarkOutboxSent(ctx, extra.ID, "extra-history-message"); err != nil {
				t.Fatalf("extra history sent: %v", err)
			}
			verifiedOp, verified, err := store.RefreshForkHistory(ctx, op.ID)
			if err != nil || !verified || verifiedOp.Phase != ForkPhaseHistoryVerified {
				t.Fatalf("RefreshForkHistory = %#v, verified=%v, err=%v", verifiedOp, verified, err)
			}

			activated, err := store.ActivateFork(ctx, op.ID, OutboxMessage{
				ID:              "fork-link:fork-history-proof",
				SessionID:       parent.ID,
				TeamsChatID:     parent.TeamsChatID,
				Kind:            "fork-link",
				Body:            "open child",
				ForkOperationID: op.ID,
				ForkRole:        "link",
			})
			if err != nil {
				t.Fatalf("ActivateFork: %v", err)
			}
			if activated.Phase != ForkPhaseActivated {
				t.Fatalf("ActivateFork phase = %q", activated.Phase)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load after activation: %v", err)
			}
			if state.Sessions["history-child"].Status != SessionStatusActive {
				t.Fatalf("child status = %q, want active", state.Sessions["history-child"].Status)
			}
			if state.ForkOperations[op.ID].ManifestHash == "" || state.ForkOperations[op.ID].CutoffSourceOffset != 42 || state.ForkHistoryItems[ForkHistoryItemID(op.ID, 0)].DeliveryStatus != ForkHistoryDeliveryDuplicateSettled {
				t.Fatalf("fork proof state was not persisted: op=%#v item=%#v", state.ForkOperations[op.ID], state.ForkHistoryItems[ForkHistoryItemID(op.ID, 0)])
			}
		})
	}
}

func TestForkHistoryPlanV2PersistsSourceRangeAndProtectsIt(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			parent := testSession()
			parent.ID = "parent-v2-range"
			parent.Status = SessionStatusActive
			parent.TeamsChatID = "parent-v2-range-chat"
			parent.CodexThreadID = "parent-v2-range-thread"
			if _, _, err := store.CreateSession(ctx, parent); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			seedCompletedForkCutoff(t, store, parent.ID, "cutoff-v2-range", "codex-cutoff-v2-range", time.Now().Add(-time.Minute))
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}
			op, created, err := store.BeginFork(ctx, ForkBeginRequest{
				OperationID:        "fork-v2-range",
				ParentSessionID:    parent.ID,
				ParentChatID:       parent.TeamsChatID,
				ParentThreadID:     parent.CodexThreadID,
				ChildSession:       SessionContext{ID: "child-v2-range", Status: SessionStatusStaging},
				CutoffTurnID:       "cutoff-v2-range",
				CutoffCodexTurnID:  "codex-cutoff-v2-range",
				HistoryPlanVersion: 2,
			})
			if err != nil || !created {
				t.Fatalf("BeginFork = %#v, created=%v, err=%v", op, created, err)
			}
			item := ForkHistoryItem{
				Ordinal:           0,
				SourceRecordID:    "first..last",
				SourceEndRecordID: "last",
				SourceLine:        4,
				SourceStartOffset: 20,
				SourceOffset:      80,
				SourceTurnID:      "codex-cutoff-v2-range",
				Kind:              "batch",
				RenderedBody:      "<p>batch</p>",
			}
			metadata := ForkManifestMetadata{
				SourcePath:           "transcript.jsonl",
				SourceFingerprint:    "fingerprint-v2",
				HistoryPlanVersion:   2,
				CutoffSourceRecordID: "last",
				CutoffSourceLine:     4,
				CutoffSourceOffset:   80,
				SourcePrefixHash:     "prefix-v2",
			}
			saved, err := store.SaveForkManifestWithMetadata(ctx, op.ID, []ForkHistoryItem{item}, metadata)
			if err != nil {
				t.Fatalf("SaveForkManifestWithMetadata: %v", err)
			}
			if saved.HistoryPlanVersion != 2 {
				t.Fatalf("saved plan version = %d, want 2", saved.HistoryPlanVersion)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load after v2 manifest: %v", err)
			}
			persisted := state.ForkHistoryItems[ForkHistoryItemID(op.ID, 0)]
			if persisted.SourceEndRecordID != "last" {
				t.Fatalf("persisted source end = %q, want last", persisted.SourceEndRecordID)
			}

			// Omitting the new metadata field must preserve the operation's
			// durable plan version for legacy store callers.
			if _, err := store.SaveForkManifestWithMetadata(ctx, op.ID, []ForkHistoryItem{item}, ForkManifestMetadata{}); err != nil {
				t.Fatalf("idempotent v2 save without metadata version: %v", err)
			}
			mutated := item
			mutated.SourceEndRecordID = "different-end"
			if _, err := store.SaveForkManifestWithMetadata(ctx, op.ID, []ForkHistoryItem{mutated}, ForkManifestMetadata{}); err == nil || !strings.Contains(err.Error(), "manifest is immutable") {
				t.Fatalf("mutated source range error = %v, want immutable rejection", err)
			}
		})
	}
}

func TestRefreshForkHistoryNoopDoesNotRewriteState(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			parent := testSession()
			parent.ID = "refresh-noop-parent"
			parent.Status = SessionStatusActive
			parent.TeamsChatID = "refresh-noop-parent-chat"
			parent.CodexThreadID = "refresh-noop-parent-thread"
			if _, _, err := store.CreateSession(ctx, parent); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			seedCompletedForkCutoff(t, store, parent.ID, "refresh-noop-cutoff", "refresh-noop-codex-cutoff", time.Now().Add(-time.Minute))
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}
			op, created, err := store.BeginFork(ctx, ForkBeginRequest{
				OperationID:        "refresh-noop-operation",
				ParentSessionID:    parent.ID,
				ParentChatID:       parent.TeamsChatID,
				ParentThreadID:     parent.CodexThreadID,
				ChildSession:       SessionContext{ID: "refresh-noop-child", Status: SessionStatusStaging},
				CutoffTurnID:       "refresh-noop-cutoff",
				CutoffCodexTurnID:  "refresh-noop-codex-cutoff",
				HistoryPlanVersion: 2,
			})
			if err != nil || !created {
				t.Fatalf("BeginFork = %#v, created=%v, err=%v", op, created, err)
			}
			item := ForkHistoryItem{
				Ordinal:           0,
				SourceRecordID:    "refresh-record",
				SourceEndRecordID: "refresh-record",
				SourceLine:        3,
				SourceStartOffset: 10,
				SourceOffset:      42,
				SourceTurnID:      "refresh-noop-codex-cutoff",
				Kind:              "batch",
				RenderedBody:      "<p>refresh me</p>",
			}
			metadata := ForkManifestMetadata{
				SourcePath:              "refresh-noop-transcript.jsonl",
				SourceFingerprint:       "refresh-noop-fingerprint",
				HistoryPlanVersion:      2,
				CutoffSourceRecordID:    "refresh-record",
				CutoffSourceLine:        3,
				CutoffSourceStartOffset: 10,
				CutoffSourceOffset:      42,
				SourcePrefixHash:        "refresh-noop-prefix",
			}
			if _, err := store.SaveForkManifestWithMetadata(ctx, op.ID, []ForkHistoryItem{item}, metadata); err != nil {
				t.Fatalf("SaveForkManifestWithMetadata: %v", err)
			}
			if _, err := store.RecordForkCodexChild(ctx, op.ID, "refresh-noop-child-thread"); err != nil {
				t.Fatalf("RecordForkCodexChild: %v", err)
			}
			if _, err := store.StageForkChat(ctx, op.ID, "refresh-noop-child-chat", "https://teams.example/refresh-noop", "refresh noop", "refresh-noop-graph", time.Now(), time.Now().Add(time.Hour)); err != nil {
				t.Fatalf("StageForkChat: %v", err)
			}
			queued, err := store.QueueForkHistory(ctx, op.ID, "History import complete")
			if err != nil || len(queued) != 2 {
				t.Fatalf("QueueForkHistory = %#v, err=%v", queued, err)
			}
			for _, message := range queued {
				if _, err := store.MarkOutboxSendAttempt(ctx, message.ID); err != nil {
					t.Fatalf("MarkOutboxSendAttempt(%s): %v", message.ID, err)
				}
				if _, err := store.MarkOutboxSent(ctx, message.ID, message.ID+":teams"); err != nil {
					t.Fatalf("MarkOutboxSent(%s): %v", message.ID, err)
				}
			}
			if _, verified, err := store.RefreshForkHistory(ctx, op.ID); err != nil || !verified {
				t.Fatalf("initial RefreshForkHistory verified=%v err=%v", verified, err)
			}
			beforeFiles := snapshotRegularFilesForReadOnlyTest(t, filepath.Dir(store.Path()))
			var beforeSQLite []byte
			if sqliteMode {
				// SQLite may update its shared-memory bookkeeping while a read
				// transaction is opened. The durable state JSON is the write
				// amplification signal for this no-op assertion.
				delete(beforeFiles, "store.sqlite-shm")
				delete(beforeFiles, "store.sqlite-wal")
				beforeSQLite = sqliteRawStateJSONForTest(t, store)
			}
			refreshed, verified, err := store.RefreshForkHistory(ctx, op.ID)
			if err != nil || !verified || refreshed.Phase != ForkPhaseHistoryVerified {
				t.Fatalf("noop RefreshForkHistory = %#v verified=%v err=%v", refreshed, verified, err)
			}
			afterFiles := snapshotRegularFilesForReadOnlyTest(t, filepath.Dir(store.Path()))
			if sqliteMode {
				delete(afterFiles, "store.sqlite-shm")
				delete(afterFiles, "store.sqlite-wal")
			}
			if !reflect.DeepEqual(beforeFiles, afterFiles) {
				t.Fatalf("noop refresh rewrote store files:\nbefore=%#v\nafter=%#v", beforeFiles, afterFiles)
			}
			if sqliteMode && !reflect.DeepEqual(beforeSQLite, sqliteRawStateJSONForTest(t, store)) {
				t.Fatal("noop refresh rewrote SQLite state JSON")
			}
		})
	}
}

func TestBlockedAmbiguousForkRemainsParentFenced(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			parent := testSession()
			parent.ID = "parent-blocked"
			parent.Status = SessionStatusActive
			parent.TeamsChatID = "blocked-parent-chat"
			parent.CodexThreadID = "blocked-parent-thread"
			if _, _, err := store.CreateSession(ctx, parent); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			seedCompletedForkCutoff(t, store, parent.ID, "cutoff", "codex-cutoff", time.Now().Add(-time.Minute))
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}
			op, _, err := store.BeginFork(ctx, ForkBeginRequest{
				OperationID:       "fork-blocked",
				ParentSessionID:   parent.ID,
				ChildSession:      SessionContext{ID: "blocked-child", Status: SessionStatusStaging},
				CutoffCodexTurnID: "codex-cutoff",
			})
			if err != nil {
				t.Fatalf("BeginFork: %v", err)
			}
			if _, err := store.UpdateForkOperation(ctx, op.ID, func(current *ForkOperation) error {
				current.Phase = ForkPhaseBlockedAmbiguous
				return nil
			}); err != nil {
				t.Fatalf("mark blocked: %v", err)
			}
			if _, _, err := store.BeginFork(ctx, ForkBeginRequest{
				OperationID:     "fork-blocked-second",
				ParentSessionID: parent.ID,
				ChildSession:    SessionContext{ID: "blocked-child-second", Status: SessionStatusStaging},
			}); !errors.Is(err, ErrForkAlreadyInProgress) {
				t.Fatalf("second fork error = %v, want ErrForkAlreadyInProgress", err)
			}
			_, _, err = store.PersistInbound(ctx, InboundEvent{
				ID:             "blocked-normal-input",
				SessionID:      parent.ID,
				TeamsChatID:    parent.TeamsChatID,
				TeamsMessageID: "blocked-normal-message",
				Status:         InboundStatusPersisted,
			})
			if !errors.Is(err, ErrForkParentFenced) {
				t.Fatalf("normal input error = %v, want ErrForkParentFenced", err)
			}
		})
	}
}

func seedCompletedForkCutoff(t *testing.T, store *Store, sessionID string, turnID string, codexTurnID string, completedAt time.Time) {
	t.Helper()
	if err := store.Update(context.Background(), func(state *State) error {
		state.Turns[turnID] = Turn{
			ID:          turnID,
			SessionID:   sessionID,
			CodexTurnID: codexTurnID,
			Status:      TurnStatusCompleted,
			CompletedAt: completedAt,
			CreatedAt:   completedAt,
			UpdatedAt:   completedAt,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed completed fork cutoff: %v", err)
	}
}
