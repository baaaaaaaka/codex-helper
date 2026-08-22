package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMarkTurnRunningRespectsUnresolvedExecutionAcrossBackends(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seed := func() error {
		return store.Update(ctx, func(state *State) error {
			now := time.Now()
			state.Sessions["mark-session"] = SessionContext{ID: "mark-session", Status: SessionStatusActive}
			state.Turns["mark-turn"] = Turn{ID: "mark-turn", SessionID: "mark-session", Status: TurnStatusQueued, QueuedAt: now, CreatedAt: now}
			state.ImportCheckpoints["transcript:mark-session"] = ImportCheckpoint{ID: "transcript:mark-session", SessionID: "mark-session", UnresolvedExecution: &ExecutionAnchor{State: "unresolved", OuterTurnID: "old"}}
			return nil
		})
	}
	if err := seed(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for _, backend := range []string{"json", "sqlite"} {
		if backend == "sqlite" {
			if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
				t.Fatalf("MigrateLargeStateToSQLite: %v", err)
			}
		}
		if _, err := store.MarkTurnRunning(ctx, "mark-turn", "thread-1", "turn-1"); !errors.Is(err, ErrUnresolvedExecution) {
			t.Fatalf("%s MarkTurnRunning error = %v, want ErrUnresolvedExecution", backend, err)
		}
		turn, ok, err := store.TurnByID(ctx, "mark-turn")
		if err != nil || !ok {
			t.Fatalf("%s load turn: ok=%v err=%v", backend, ok, err)
		}
		if turn.Status != TurnStatusQueued {
			t.Fatalf("%s turn status = %q, want queued", backend, turn.Status)
		}
	}
}

func TestIsolatedLiveBranchSurvivesUnresolvedHistoryFenceAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "isolated-live-session"
			const oldTurnID = "isolated-old-turn"
			const turnID = "isolated-live-turn"
			const checkpointID = "transcript:" + sessionID
			now := time.Now().UTC()
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, CodexThreadID: "thread-old", TeamsChatID: "isolated-live-chat"}
				state.Turns[oldTurnID] = Turn{ID: oldTurnID, SessionID: sessionID, Status: TurnStatusInterrupted, CodexThreadID: "thread-old", RecoveryReason: "ambiguous Codex execution: helper restart", InterruptedAt: now}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusQueued, CodexThreadID: "thread-old", QueuedAt: now, CreatedAt: now}
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{ID: checkpointID, SessionID: sessionID, Status: importCheckpointStatusBlocked, UnresolvedExecution: &ExecutionAnchor{
					SessionID: sessionID, ThreadID: "thread-old", OuterTurnID: oldTurnID, CodexTurnID: "codex-old", State: "unresolved", Generation: 4,
				}}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, store)
			}
			prepared, err := store.MarkTurnForIsolatedCodexThread(ctx, turnID)
			if err != nil || !prepared.StartNewCodexThread {
				t.Fatalf("prepare isolated turn = %#v err=%v", prepared, err)
			}
			claimed, ok, err := store.ClaimNextQueuedTurn(ctx, sessionID)
			if err != nil || !ok || claimed.Status != TurnStatusRunning || claimed.CodexThreadID != "" {
				t.Fatalf("claim isolated turn = %#v ok=%v err=%v", claimed, ok, err)
			}
			bound, err := store.BindCodexThreadForRunningTurn(ctx, CodexThreadStartBindingRequest{
				SessionID: sessionID, TurnID: turnID, ThreadID: "thread-new", ModelGeneration: 0,
			})
			if err != nil || bound.Turn.CodexThreadID != "thread-new" {
				t.Fatalf("bind isolated thread = %#v err=%v", bound, err)
			}
			boundCheckpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
			if err != nil || !found || boundCheckpoint.UnresolvedExecution == nil || boundCheckpoint.UnresolvedExecution.LiveBranchThreadID != "thread-new" {
				t.Fatalf("live branch was not durable at bind = %#v found=%v err=%v", boundCheckpoint, found, err)
			}
			final := OutboxMessage{ID: "outbox:" + turnID + ":final", SessionID: sessionID, TurnID: turnID, TeamsChatID: "isolated-live-chat", Kind: "final", NotificationKind: "turn_completed", Body: "new branch final", SourceTextHash: "isolated-live-hash", PartIndex: 1, PartCount: 1}
			completed, err := store.CompleteTurnWithFinal(ctx, CompleteTurnWithFinalRequest{
				SessionID: sessionID, TurnID: turnID, CodexThreadID: "thread-new", CodexTurnID: "codex-new",
				Progress: TranscriptCheckpointProgress{ID: checkpointID, SessionID: sessionID}, FinalOutbox: []OutboxMessage{final},
			})
			if err != nil || completed.Status != TurnStatusCompleted {
				t.Fatalf("complete isolated turn = %#v err=%v", completed, err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
			if err != nil || !found || checkpoint.UnresolvedExecution == nil || checkpoint.UnresolvedExecution.LiveBranchThreadID != "thread-new" {
				t.Fatalf("live branch checkpoint = %#v found=%v err=%v", checkpoint, found, err)
			}
			claimedOutbox, err := store.MarkOutboxSendAttempt(ctx, final.ID)
			if err != nil || claimedOutbox.Status != OutboxStatusSending {
				t.Fatalf("claim isolated final = %#v err=%v", claimedOutbox, err)
			}
			if _, err := store.MarkOutboxSent(ctx, final.ID, "teams-isolated-message"); err != nil {
				t.Fatalf("send isolated final: %v", err)
			}
			const nextTurnID = "isolated-next-turn"
			if _, _, err := store.QueueTurn(ctx, Turn{ID: nextTurnID, SessionID: sessionID, Status: TurnStatusQueued, CodexThreadID: "thread-new", QueuedAt: now.Add(time.Second), CreatedAt: now.Add(time.Second)}); err != nil {
				t.Fatalf("queue next branch turn: %v", err)
			}
			next, ok, err := store.ClaimNextQueuedTurn(ctx, sessionID)
			if err != nil || !ok || next.ID != nextTurnID || next.Status != TurnStatusRunning || next.CodexThreadID != "thread-new" {
				t.Fatalf("claim next live branch turn = %#v ok=%v err=%v", next, ok, err)
			}
		})
	}
}

func TestTranscriptQuarantineAdmitsOnlyAnIsolatedLiveBranchAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "transcript-quarantine-session"
			const turnID = "transcript-quarantine-turn"
			const checkpointID = "transcript:" + sessionID
			now := time.Now().UTC()
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, CodexThreadID: "thread-old"}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusQueued, CodexThreadID: "thread-old", QueuedAt: now, CreatedAt: now}
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
					ID: checkpointID, SessionID: sessionID, Status: importCheckpointStatusBlocked,
					TranscriptQuarantine: &TranscriptQuarantine{Kind: "mixed_id_final_mirror", SourcePath: "/old/session.jsonl"},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed transcript quarantine: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, store)
			}
			if unresolved, err := store.SessionExecutionOwnershipProbe(ctx, sessionID, checkpointID); err != nil || unresolved {
				t.Fatalf("transcript quarantine became strict execution ownership: unresolved=%v err=%v", unresolved, err)
			}
			prepared, err := store.MarkTurnForIsolatedCodexThread(ctx, turnID)
			if err != nil || !prepared.StartNewCodexThread {
				t.Fatalf("prepare isolated transcript-quarantine turn = %#v err=%v", prepared, err)
			}
			claimed, ok, err := store.ClaimNextQueuedTurn(ctx, sessionID)
			if err != nil || !ok || !claimed.StartNewCodexThread || claimed.Status != TurnStatusRunning {
				t.Fatalf("claim isolated transcript-quarantine turn = %#v ok=%v err=%v", claimed, ok, err)
			}
			bound, err := store.BindCodexThreadForRunningTurn(ctx, CodexThreadStartBindingRequest{
				SessionID: sessionID, TurnID: turnID, ThreadID: "thread-new", ModelGeneration: 0,
			})
			if err != nil || bound.Turn.CodexThreadID != "thread-new" {
				t.Fatalf("bind isolated transcript-quarantine thread = %#v err=%v", bound, err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
			if err != nil || !found || checkpoint.TranscriptQuarantine == nil || checkpoint.TranscriptQuarantine.LiveBranchThreadID != "thread-new" {
				t.Fatalf("transcript quarantine live branch = %#v found=%v err=%v", checkpoint, found, err)
			}
		})
	}
}

func TestQueuedTurnCapturedBeforeLiveBranchIsRetargetedAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "retarget-live-session"
			const turnID = "retarget-live-turn"
			now := time.Now().UTC()
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, CodexThreadID: "thread-live"}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusQueued, CodexThreadID: "thread-old", StartNewCodexThread: true, QueuedAt: now, CreatedAt: now}
				state.ImportCheckpoints["transcript:"+sessionID] = ImportCheckpoint{
					ID: "transcript:" + sessionID, SessionID: sessionID,
					UnresolvedExecution: &ExecutionAnchor{SessionID: sessionID, ThreadID: "thread-old", LiveBranchThreadID: "thread-live", State: "unresolved", Generation: 5},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, store)
			}
			prepared, err := store.MarkTurnForIsolatedCodexThread(ctx, turnID)
			if err != nil || prepared.CodexThreadID != "thread-live" || prepared.StartNewCodexThread {
				t.Fatalf("retarget queued turn = %#v err=%v, want live branch without fresh-thread flag", prepared, err)
			}
			claimed, ok, err := store.ClaimNextQueuedTurn(ctx, sessionID)
			if err != nil || !ok || claimed.CodexThreadID != "thread-live" || claimed.StartNewCodexThread {
				t.Fatalf("claim retargeted turn = %#v ok=%v err=%v", claimed, ok, err)
			}
		})
	}
}

func TestActiveExecutionAnchorOverridesOlderTranscriptQuarantineBranchAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "anchor-over-quarantine-session"
			const turnID = "anchor-over-quarantine-turn"
			const checkpointID = "transcript:" + sessionID
			now := time.Now().UTC()
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, CodexThreadID: "thread-old"}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusQueued, CodexThreadID: "thread-old", QueuedAt: now, CreatedAt: now}
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
					ID: checkpointID, SessionID: sessionID, Status: importCheckpointStatusBlocked,
					UnresolvedExecution:  &ExecutionAnchor{SessionID: sessionID, ThreadID: "thread-old", OuterTurnID: "old-turn", CodexTurnID: "old-codex-turn", State: "unresolved", Generation: 3},
					TranscriptQuarantine: &TranscriptQuarantine{Kind: "mixed_id_final_mirror", LiveBranchThreadID: "stale-quarantine-branch"},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, store)
			}
			prepared, err := store.MarkTurnForIsolatedCodexThread(ctx, turnID)
			if err != nil || !prepared.StartNewCodexThread || prepared.CodexThreadID != "" {
				t.Fatalf("prepared turn = %#v err=%v, want a fresh branch rather than stale quarantine branch", prepared, err)
			}
			claimed, ok, err := store.ClaimNextQueuedTurn(ctx, sessionID)
			if err != nil || !ok || !claimed.StartNewCodexThread {
				t.Fatalf("claimed turn = %#v ok=%v err=%v", claimed, ok, err)
			}
			if _, err := store.BindCodexThreadForRunningTurn(ctx, CodexThreadStartBindingRequest{
				SessionID: sessionID, TurnID: turnID, ThreadID: "thread-anchor-live", ModelGeneration: 0,
			}); err != nil {
				t.Fatalf("bind fresh branch: %v", err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
			if err != nil || !found || checkpoint.UnresolvedExecution == nil || checkpoint.UnresolvedExecution.LiveBranchThreadID != "thread-anchor-live" || checkpoint.TranscriptQuarantine == nil || checkpoint.TranscriptQuarantine.LiveBranchThreadID != "stale-quarantine-branch" {
				t.Fatalf("strong anchor/quarantine branch state = %#v found=%v err=%v", checkpoint, found, err)
			}
		})
	}
}

func TestTranscriptQuarantineConcurrentQueuedAdmissionsShareLiveBranchAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "quarantine-concurrent-session"
			const checkpointID = "transcript:" + sessionID
			now := time.Now().UTC()
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, CodexThreadID: "thread-old"}
				state.Turns["quarantine-turn-a"] = Turn{ID: "quarantine-turn-a", SessionID: sessionID, Status: TurnStatusQueued, CodexThreadID: "thread-old", QueuedAt: now, CreatedAt: now}
				state.Turns["quarantine-turn-b"] = Turn{ID: "quarantine-turn-b", SessionID: sessionID, Status: TurnStatusQueued, CodexThreadID: "thread-old", QueuedAt: now.Add(time.Millisecond), CreatedAt: now.Add(time.Millisecond)}
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
					ID: checkpointID, SessionID: sessionID, Status: importCheckpointStatusBlocked,
					TranscriptQuarantine: &TranscriptQuarantine{Kind: "mixed_id_final_mirror", SourcePath: "/sessions/quarantine.jsonl"},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, store)
			}

			start := make(chan struct{})
			errs := make(chan error, 2)
			for _, turnID := range []string{"quarantine-turn-a", "quarantine-turn-b"} {
				go func(turnID string) {
					<-start
					_, err := store.MarkTurnForIsolatedCodexThread(ctx, turnID)
					errs <- err
				}(turnID)
			}
			close(start)
			for i := 0; i < 2; i++ {
				if err := <-errs; err != nil {
					t.Fatalf("concurrent admission %d: %v", i, err)
				}
			}

			first, ok, err := store.ClaimNextQueuedTurn(ctx, sessionID)
			if err != nil || !ok || !first.StartNewCodexThread {
				t.Fatalf("first isolated claim = %#v ok=%v err=%v", first, ok, err)
			}
			bound, err := store.BindCodexThreadForRunningTurn(ctx, CodexThreadStartBindingRequest{
				SessionID: sessionID, TurnID: first.ID, ThreadID: "thread-live", ModelGeneration: 0,
			})
			if err != nil || bound.Turn.CodexThreadID != "thread-live" {
				t.Fatalf("bind first isolated branch = %#v err=%v", bound, err)
			}
			if _, err := store.MarkTurnCompleted(ctx, first.ID, "thread-live", "codex-first"); err != nil {
				t.Fatalf("complete first isolated branch: %v", err)
			}

			second, ok, err := store.ClaimNextQueuedTurn(ctx, sessionID)
			if err != nil || !ok || second.StartNewCodexThread || second.CodexThreadID != "thread-live" {
				t.Fatalf("second isolated claim = %#v ok=%v err=%v, want the admitted live branch", second, ok, err)
			}
		})
	}
}

func TestCompleteTurnWithFinalCommitsOwnerAndFinalAtomicallyAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC()
			const sessionID = "complete-final-session"
			const turnID = "complete-final-turn"
			const checkpointID = "transcript:complete-final-session"
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, TeamsChatID: "complete-final-chat", CodexThreadID: "complete-final-thread"}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusRunning, CodexThreadID: "complete-final-thread", CodexTurnID: "complete-final-codex", StartedAt: now}
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
					ID: checkpointID, SessionID: sessionID, Status: importCheckpointStatusBlocked,
					UnresolvedExecution: &ExecutionAnchor{SessionID: sessionID, ThreadID: "complete-final-thread", OuterTurnID: turnID, CodexTurnID: "complete-final-codex", State: "unresolved", Generation: 9},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			final := OutboxMessage{
				ID: "outbox:" + turnID + ":final", SessionID: sessionID, TurnID: turnID,
				TeamsChatID: "complete-final-chat", Kind: "final", NotificationKind: "turn_completed",
				Body: "atomic final", SourceTextHash: "atomic-final-hash", PartIndex: 1, PartCount: 1,
			}
			continuation := OutboxMessage{
				ID: "outbox:" + turnID + ":final-002", SessionID: sessionID, TurnID: turnID,
				TeamsChatID: "complete-final-chat", Kind: "final-002",
				Body: "atomic final continuation", SourceTextHash: "atomic-final-hash", PartIndex: 2, PartCount: 2,
			}
			req := CompleteTurnWithFinalRequest{
				SessionID: sessionID, TurnID: turnID, CodexThreadID: "complete-final-thread", CodexTurnID: "complete-final-codex",
				AnchorGeneration: 9, Progress: TranscriptCheckpointProgress{ID: checkpointID, SessionID: sessionID}, FinalOutbox: []OutboxMessage{final, continuation},
			}
			completed, err := store.CompleteTurnWithFinal(ctx, req)
			if err != nil || completed.Status != TurnStatusCompleted {
				t.Fatalf("CompleteTurnWithFinal = %#v, err=%v", completed, err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
			if err != nil || !found || checkpoint.UnresolvedExecution != nil || checkpoint.Status != importCheckpointStatusComplete {
				t.Fatalf("checkpoint = %#v found=%v err=%v, want completed without anchor", checkpoint, found, err)
			}
			queued, err := store.OutboxMessageByID(ctx, final.ID)
			if err != nil || queued.Status != OutboxStatusQueued || queued.Body != final.Body {
				t.Fatalf("final outbox = %#v err=%v, want one queued final", queued, err)
			}
			queuedContinuation, err := store.OutboxMessageByID(ctx, continuation.ID)
			if err != nil || queuedContinuation.Status != OutboxStatusQueued || queuedContinuation.Body != continuation.Body {
				t.Fatalf("continuation outbox = %#v err=%v, want queued continuation", queuedContinuation, err)
			}
			if _, err := store.CompleteTurnWithFinal(ctx, req); err != nil {
				t.Fatalf("idempotent CompleteTurnWithFinal: %v", err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			count := 0
			for _, msg := range state.OutboxMessages {
				if msg.ID == final.ID || msg.ID == continuation.ID {
					count++
				}
			}
			if count != 2 {
				t.Fatalf("final outbox count = %d, want 2", count)
			}
		})
	}
}

func TestCompleteTurnWithFinalRejectsTerminalLoserBeforeCreatingFinalAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, winner := range []string{"failed", "completed"} {
		for _, backend := range []string{"json", "sqlite"} {
			t.Run(winner+"/"+backend, func(t *testing.T) {
				store := newTestStore(t)
				now := time.Now().UTC()
				const sessionID = "terminal-loser-session"
				const turnID = "terminal-loser-turn"
				if err := store.Update(ctx, func(state *State) error {
					state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, TeamsChatID: "terminal-loser-chat"}
					state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusRunning, CodexThreadID: "terminal-loser-thread", CodexTurnID: "terminal-loser-codex", StartedAt: now}
					return nil
				}); err != nil {
					t.Fatalf("seed: %v", err)
				}
				if backend == "sqlite" {
					if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
						t.Fatalf("MigrateLargeStateToSQLite: %v", err)
					}
				}
				if winner == "failed" {
					if _, err := store.MarkTurnFailedForExecution(ctx, ExecutionFailureIdentity{SessionID: sessionID, TurnID: turnID, ThreadID: "terminal-loser-thread", CodexTurnID: "terminal-loser-codex"}, "winner failed"); err != nil {
						t.Fatalf("MarkTurnFailedForExecution: %v", err)
					}
				} else if _, err := store.MarkTurnCompleted(ctx, turnID, "terminal-loser-thread", "terminal-loser-codex"); err != nil {
					t.Fatalf("MarkTurnCompleted: %v", err)
				}
				_, err := store.CompleteTurnWithFinal(ctx, CompleteTurnWithFinalRequest{
					SessionID: sessionID, TurnID: turnID, CodexThreadID: "terminal-loser-thread", CodexTurnID: "terminal-loser-codex",
					FinalOutbox: []OutboxMessage{{ID: "outbox:terminal-loser-final", SessionID: sessionID, TurnID: turnID, TeamsChatID: "terminal-loser-chat", Kind: "final", NotificationKind: "turn_completed", Body: "stale final", PartIndex: 1, PartCount: 1}},
				})
				if !errors.Is(err, ErrCompletionOwnerLost) {
					t.Fatalf("losing completion error = %v, want ErrCompletionOwnerLost", err)
				}
				state, err := store.Load(ctx)
				if err != nil {
					t.Fatalf("Load: %v", err)
				}
				for _, msg := range state.OutboxMessages {
					if msg.TurnID == turnID && strings.EqualFold(msg.NotificationKind, "turn_completed") {
						t.Fatalf("stale final was created after %s winner: %#v", winner, msg)
					}
				}
			})
		}
	}
}

func TestCompleteAndFailureBarrierOrderingAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			const (
				sessionID = "barrier-order-session"
				threadID  = "barrier-order-thread"
				codexID   = "barrier-order-codex"
			)
			newFixture := func(t *testing.T, turnID string) *Store {
				t.Helper()
				store := newTestStore(t)
				now := time.Now().UTC()
				if err := store.Update(ctx, func(state *State) error {
					state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, CodexThreadID: threadID}
					state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusRunning, CodexThreadID: threadID, CodexTurnID: codexID, StartedAt: now}
					return nil
				}); err != nil {
					t.Fatalf("seed %s: %v", turnID, err)
				}
				if backend == "sqlite" {
					if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
						t.Fatalf("MigrateLargeStateToSQLite: %v", err)
					}
				}
				return store
			}
			completion := func(store *Store, turnID string) error {
				_, err := store.CompleteTurnWithFinal(ctx, CompleteTurnWithFinalRequest{
					SessionID: sessionID, TurnID: turnID, CodexThreadID: threadID, CodexTurnID: codexID,
					FinalOutbox: []OutboxMessage{{
						ID: "outbox:" + turnID + ":final", SessionID: sessionID, TurnID: turnID,
						TeamsChatID: "barrier-order-chat", Kind: "final", NotificationKind: "turn_completed",
						Body: "barrier final", PartIndex: 1, PartCount: 1,
					}},
				})
				return err
			}
			failure := func(store *Store, turnID string) error {
				_, err := store.MarkTurnFailedForExecution(ctx, ExecutionFailureIdentity{SessionID: sessionID, TurnID: turnID, ThreadID: threadID, CodexTurnID: codexID}, "barrier failure")
				return err
			}

			// Failure wins while completion is held behind a barrier. The loser must
			// not create a final after the terminal owner has committed.
			failureFirst := newFixture(t, "barrier-failure-first")
			completionReady := make(chan struct{})
			completionDone := make(chan error, 1)
			go func() {
				<-completionReady
				completionDone <- completion(failureFirst, "barrier-failure-first")
			}()
			if err := failure(failureFirst, "barrier-failure-first"); err != nil {
				t.Fatalf("failure-first failure: %v", err)
			}
			close(completionReady)
			if err := <-completionDone; !errors.Is(err, ErrCompletionOwnerLost) {
				t.Fatalf("failure-first completion error = %v, want ErrCompletionOwnerLost", err)
			}
			state, err := failureFirst.Load(ctx)
			if err != nil {
				t.Fatalf("failure-first load: %v", err)
			}
			if state.Turns["barrier-failure-first"].Status != TurnStatusFailed {
				t.Fatalf("failure-first turn = %#v, want failed", state.Turns["barrier-failure-first"])
			}
			for _, msg := range state.OutboxMessages {
				if msg.TurnID == "barrier-failure-first" && strings.EqualFold(msg.NotificationKind, "turn_completed") {
					t.Fatalf("failure-first created stale final: %#v", msg)
				}
			}

			// Completion wins before the stale failure callback is released. The
			// callback is idempotent and cannot replace the completed owner.
			completionFirst := newFixture(t, "barrier-completion-first")
			failureReady := make(chan struct{})
			failureDone := make(chan error, 1)
			go func() {
				<-failureReady
				failureDone <- failure(completionFirst, "barrier-completion-first")
			}()
			if err := completion(completionFirst, "barrier-completion-first"); err != nil {
				t.Fatalf("completion-first completion: %v", err)
			}
			close(failureReady)
			if err := <-failureDone; err != nil && !errors.Is(err, ErrStaleExecutionCallback) {
				t.Fatalf("completion-first failure error = %v, want nil or stale callback", err)
			}
			state, err = completionFirst.Load(ctx)
			if err != nil {
				t.Fatalf("completion-first load: %v", err)
			}
			if state.Turns["barrier-completion-first"].Status != TurnStatusCompleted {
				t.Fatalf("completion-first turn = %#v, want completed", state.Turns["barrier-completion-first"])
			}
			finals := 0
			for _, msg := range state.OutboxMessages {
				if msg.TurnID == "barrier-completion-first" && strings.EqualFold(msg.NotificationKind, "turn_completed") {
					finals++
				}
			}
			if finals != 1 {
				t.Fatalf("completion-first final count = %d, want one", finals)
			}
		})
	}
}

func TestCompleteAndFailureConcurrentExactlyOneOwnerAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			for iteration := 0; iteration < 16; iteration++ {
				store := newTestStore(t)
				now := time.Now().UTC()
				sessionID := fmt.Sprintf("concurrent-owner-session-%d", iteration)
				turnID := fmt.Sprintf("concurrent-owner-turn-%d", iteration)
				checkpointID := sessionTranscriptCheckpointID(sessionID)
				const (
					threadID = "concurrent-owner-thread"
					codexID  = "concurrent-owner-codex"
				)
				if err := store.Update(ctx, func(state *State) error {
					state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, CodexThreadID: threadID}
					state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusRunning, CodexThreadID: threadID, CodexTurnID: codexID, StartedAt: now}
					state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
						ID: checkpointID, SessionID: sessionID, Status: "blocked",
						UnresolvedExecution: &ExecutionAnchor{
							SessionID: sessionID, ThreadID: threadID, OuterTurnID: turnID, CodexTurnID: codexID,
							State: "unresolved", Generation: 3, CutoffRecordID: "before-owner", CutoffLine: 4, CutoffOffset: 96,
						},
					}
					return nil
				}); err != nil {
					t.Fatalf("seed: %v", err)
				}
				if backend == "sqlite" {
					if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
						t.Fatalf("MigrateLargeStateToSQLite: %v", err)
					}
				}

				start := make(chan struct{})
				completionErr := make(chan error, 1)
				failureErr := make(chan error, 1)
				go func() {
					<-start
					_, err := store.CompleteTurnWithFinal(ctx, CompleteTurnWithFinalRequest{
						SessionID: sessionID, TurnID: turnID, CodexThreadID: threadID, CodexTurnID: codexID, AnchorGeneration: 3,
						Progress: TranscriptCheckpointProgress{ID: checkpointID, SessionID: sessionID, LastOffsetKnown: true},
						FinalOutbox: []OutboxMessage{{
							ID: "outbox:" + turnID + ":final", SessionID: sessionID, TurnID: turnID,
							TeamsChatID: "concurrent-owner-chat", Kind: "final", NotificationKind: "turn_completed",
							Body: "concurrent final", PartIndex: 1, PartCount: 1,
						}},
					})
					completionErr <- err
				}()
				go func() {
					<-start
					_, err := store.MarkTurnFailedForExecution(ctx, ExecutionFailureIdentity{
						SessionID: sessionID, TurnID: turnID, ThreadID: threadID, CodexTurnID: codexID, AnchorGeneration: 3,
					}, "concurrent failure")
					failureErr <- err
				}()
				close(start)
				completionResult := <-completionErr
				failureResult := <-failureErr
				if completionResult != nil && !errors.Is(completionResult, ErrCompletionOwnerLost) && !errors.Is(completionResult, ErrStaleExecutionCallback) && !errors.Is(completionResult, ErrUnresolvedExecution) {
					t.Fatalf("completion iteration %d error = %v", iteration, completionResult)
				}
				if failureResult != nil && !errors.Is(failureResult, ErrCompletionOwnerLost) && !errors.Is(failureResult, ErrStaleExecutionCallback) && !errors.Is(failureResult, ErrUnresolvedExecution) {
					t.Fatalf("failure iteration %d error = %v", iteration, failureResult)
				}

				state, err := store.Load(ctx)
				if err != nil {
					t.Fatalf("Load iteration %d: %v", iteration, err)
				}
				turn := state.Turns[turnID]
				if turn.Status != TurnStatusCompleted && turn.Status != TurnStatusFailed {
					t.Fatalf("iteration %d turn = %#v, want exactly one terminal owner", iteration, turn)
				}
				checkpoint := state.ImportCheckpoints[checkpointID]
				if checkpoint.UnresolvedExecution != nil && checkpoint.UnresolvedExecution.State != "resolved" {
					t.Fatalf("iteration %d active anchor remains: %#v", iteration, checkpoint.UnresolvedExecution)
				}
				finals := 0
				for _, msg := range state.OutboxMessages {
					if msg.TurnID == turnID && strings.EqualFold(msg.NotificationKind, "turn_completed") {
						finals++
						if turn.Status == TurnStatusFailed && msg.Status != OutboxStatusSkipped && !msg.BlockedByTerminalFailure {
							t.Fatalf("iteration %d failed owner left sendable final: %#v", iteration, msg)
						}
					}
				}
				if turn.Status == TurnStatusCompleted && finals != 1 {
					t.Fatalf("iteration %d completed owner final count = %d, want one", iteration, finals)
				}
				if turn.Status == TurnStatusFailed && finals != 0 {
					t.Fatalf("iteration %d failed owner final count = %d, want zero", iteration, finals)
				}
			}
		})
	}
}

func TestOutboxExecutionFenceAllowsOnlyTrustedPrefixAcrossBackends(t *testing.T) {
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			const sessionID = "outbox-prefix-session"
			sourcePath := "/tmp/outbox-prefix-session.jsonl"
			anchor := ExecutionAnchor{
				SessionID: sessionID, ThreadID: "outbox-prefix-thread", OuterTurnID: "ambiguous-turn",
				SourcePath: sourcePath, CutoffRecordID: "trusted", CutoffOffset: 100,
				State: "unresolved", Generation: 3,
			}
			if err := store.Update(ctx, func(state *State) error {
				state.ImportCheckpoints[sessionTranscriptCheckpointID(sessionID)] = ImportCheckpoint{
					ID: sessionTranscriptCheckpointID(sessionID), SessionID: sessionID,
					UnresolvedExecution: &anchor, ExecutionAnchorGeneration: anchor.Generation,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed anchor: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			prior := OutboxMessage{
				SessionID: sessionID, Kind: "sync-final", TurnID: "sync:" + sessionID,
				TranscriptCheckpointID: sessionTranscriptCheckpointID(sessionID), TranscriptSourcePath: sourcePath,
				TranscriptSourceOffset: 80, TranscriptSourceOffsetKnown: true,
			}
			tail := prior
			tail.TranscriptSourceOffset = 120
			legacy := OutboxMessage{SessionID: sessionID, Kind: "sync-final", TurnID: "sync:" + sessionID}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load state: %v", err)
			}
			if outboxSendBlockedByUnresolvedExecution(&state, prior) {
				t.Fatal("trusted prefix outbox was blocked")
			}
			if !outboxSendBlockedByUnresolvedExecution(&state, tail) {
				t.Fatal("untrusted tail outbox was allowed")
			}
			if !outboxSendBlockedByUnresolvedExecution(&state, legacy) {
				t.Fatal("legacy outbox without provenance was allowed")
			}
		})
	}
}

func TestImportCheckpointKnownZeroOffsetRoundTripsAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			checkpointID := "transcript:known-zero-" + backend
			if err := store.Update(ctx, func(state *State) error {
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
					ID: checkpointID, SessionID: "known-zero-session", LastRecordID: "zero-boundary",
					LastOffset: 0, LastOffsetKnown: true, Status: "complete",
				}
				return nil
			}); err != nil {
				t.Fatalf("seed checkpoint: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
			if err != nil || !found || !checkpoint.LastOffsetKnown || checkpoint.LastOffset != 0 {
				t.Fatalf("checkpoint = %#v found=%v err=%v, want known zero offset", checkpoint, found, err)
			}
		})
	}
}

func TestCompleteTurnWithFinalRejectsOutboxIDCollisionAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC()
			const (
				sessionID = "outbox-collision-session"
				turnID    = "outbox-collision-turn"
				otherTurn = "outbox-collision-other-turn"
				outboxID  = "outbox:outbox-collision-turn:final"
			)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.Turns[turnID] = Turn{
					ID: turnID, SessionID: sessionID, Status: TurnStatusRunning,
					CodexThreadID: "collision-thread", CodexTurnID: "collision-codex", StartedAt: now,
				}
				state.Turns[otherTurn] = Turn{ID: otherTurn, SessionID: sessionID, Status: TurnStatusCompleted, CompletedAt: now}
				state.OutboxMessages[outboxID] = OutboxMessage{
					ID: outboxID, SessionID: sessionID, TurnID: otherTurn,
					TeamsChatID: "collision-chat", Kind: "final", NotificationKind: "turn_completed",
					Status: OutboxStatusSent, TeamsMessageID: "teams-existing-collision", Body: "authoritative other final",
					CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			_, err := store.CompleteTurnWithFinal(ctx, CompleteTurnWithFinalRequest{
				SessionID: sessionID, TurnID: turnID,
				CodexThreadID: "collision-thread", CodexTurnID: "collision-codex",
				FinalOutbox: []OutboxMessage{{
					ID: outboxID, SessionID: sessionID, TurnID: turnID,
					TeamsChatID: "collision-chat", Kind: "final", NotificationKind: "turn_completed",
					Body: "wrong replacement", PartIndex: 1, PartCount: 1,
				}},
			})
			if !errors.Is(err, ErrTerminalOutboxConflict) {
				t.Fatalf("completion error = %v, want ErrTerminalOutboxConflict", err)
			}
			turn, found, err := store.TurnByID(ctx, turnID)
			if err != nil || !found || turn.Status != TurnStatusRunning {
				t.Fatalf("current turn = %#v found=%v err=%v, want unchanged running", turn, found, err)
			}
			existing, err := store.OutboxMessageByID(ctx, outboxID)
			if err != nil || existing.TurnID != otherTurn || existing.Body != "authoritative other final" || existing.TeamsMessageID != "teams-existing-collision" {
				t.Fatalf("existing outbox = %#v err=%v, want unchanged owner", existing, err)
			}
		})
	}
}

func TestSessionExecutionStateSnapshotIsSessionScopedAndPersistsAnchor(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 8, 8, 8, 0, 0, 0, time.UTC)
	anchor := &ExecutionAnchor{
		SessionID:   "s1",
		ThreadID:    "thread-1",
		OuterTurnID: "outer-1",
		State:       "unresolved",
		Generation:  1,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["s1"] = SessionContext{ID: "s1", Status: SessionStatusActive, CodexThreadID: "thread-1"}
		state.Sessions["s2"] = SessionContext{ID: "s2", Status: SessionStatusActive, CodexThreadID: "thread-2"}
		state.Turns["outer-1"] = Turn{ID: "outer-1", SessionID: "s1", Status: TurnStatusInterrupted, CodexThreadID: "thread-1", RecoveryReason: "ambiguous Codex execution: still running", InterruptedAt: now, UpdatedAt: now}
		state.Turns["outer-2"] = Turn{ID: "outer-2", SessionID: "s2", Status: TurnStatusInterrupted, CodexThreadID: "thread-2", RecoveryReason: "ambiguous Codex execution: other session", InterruptedAt: now, UpdatedAt: now}
		state.ImportCheckpoints["transcript:s1"] = ImportCheckpoint{ID: "transcript:s1", SessionID: "s1", Status: "blocked", UnresolvedExecution: anchor, UpdatedAt: now}
		state.ImportCheckpoints["transcript:s2"] = ImportCheckpoint{ID: "transcript:s2", SessionID: "s2", Status: "blocked", UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	assertSnapshot := func(t *testing.T) {
		t.Helper()
		got, err := store.SessionExecutionStateSnapshot(ctx, "s1", "transcript:s1")
		if err != nil {
			t.Fatalf("SessionExecutionStateSnapshot: %v", err)
		}
		if len(got.Turns) != 1 || got.Turns["outer-1"].ID != "outer-1" {
			t.Fatalf("session turns = %#v, want only s1", got.Turns)
		}
		if len(got.ImportCheckpoints) != 1 || got.ImportCheckpoints["transcript:s1"].UnresolvedExecution == nil {
			t.Fatalf("session checkpoints = %#v, want persisted anchor", got.ImportCheckpoints)
		}
		if got.ImportCheckpoints["transcript:s1"].UnresolvedExecution.OuterTurnID != "outer-1" {
			t.Fatalf("anchor = %#v, want outer-1", got.ImportCheckpoints["transcript:s1"].UnresolvedExecution)
		}
	}
	assertSnapshot(t)
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	assertSnapshot(t)
}

func TestSessionExecutionOwnershipProbesAreScopedAndInvalidateAfterStateChange(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["probe-anchor"] = SessionContext{ID: "probe-anchor", Status: SessionStatusActive}
				state.Sessions["probe-legacy"] = SessionContext{ID: "probe-legacy", Status: SessionStatusActive}
				state.Sessions["probe-clean"] = SessionContext{ID: "probe-clean", Status: SessionStatusActive}
				state.ImportCheckpoints[sessionTranscriptCheckpointID("probe-anchor")] = ImportCheckpoint{
					ID: sessionTranscriptCheckpointID("probe-anchor"), SessionID: "probe-anchor",
					UnresolvedExecution: &ExecutionAnchor{SessionID: "probe-anchor", State: "unresolved", Generation: 2},
				}
				state.Turns["probe-legacy-turn"] = Turn{
					ID: "probe-legacy-turn", SessionID: "probe-legacy", Status: TurnStatusInterrupted,
					RecoveryReason: "ambiguous Codex execution: cancellation unconfirmed", InterruptedAt: time.Now(),
				}
				state.Turns["probe-clean-turn"] = Turn{ID: "probe-clean-turn", SessionID: "probe-clean", Status: TurnStatusCompleted}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			ids := []string{"probe-anchor", "probe-legacy", "probe-clean"}
			probes, err := store.SessionExecutionOwnershipProbes(ctx, ids)
			if err != nil {
				t.Fatalf("SessionExecutionOwnershipProbes: %v", err)
			}
			if !probes["probe-anchor"] || !probes["probe-legacy"] || probes["probe-clean"] {
				t.Fatalf("ownership probes = %#v, want anchor/legacy true and clean false", probes)
			}
			if err := store.Update(ctx, func(state *State) error {
				checkpoint := state.ImportCheckpoints[sessionTranscriptCheckpointID("probe-anchor")]
				checkpoint.UnresolvedExecution = nil
				state.ImportCheckpoints[checkpoint.ID] = checkpoint
				turn := state.Turns["probe-legacy-turn"]
				turn.Status = TurnStatusCompleted
				state.Turns[turn.ID] = turn
				return nil
			}); err != nil {
				t.Fatalf("clear ownership state: %v", err)
			}
			probes, err = store.SessionExecutionOwnershipProbes(ctx, ids)
			if err != nil {
				t.Fatalf("SessionExecutionOwnershipProbes after clear: %v", err)
			}
			if probes["probe-anchor"] || probes["probe-legacy"] || probes["probe-clean"] {
				t.Fatalf("stale ownership probes after clear = %#v, want all false", probes)
			}
		})
	}
}

func TestLinkedTranscriptSessionSnapshotCombinesRunningCheckpointAndOwnershipAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.Turns["snapshot-running"] = Turn{ID: "snapshot-running", SessionID: "snapshot-running-session", Status: TurnStatusRunning}
				state.Turns["snapshot-legacy"] = Turn{ID: "snapshot-legacy", SessionID: "snapshot-legacy-session", Status: TurnStatusInterrupted, RecoveryReason: "ambiguous Codex execution: cancellation unconfirmed"}
				state.ImportCheckpoints[sessionTranscriptCheckpointID("snapshot-anchor-session")] = ImportCheckpoint{
					ID: sessionTranscriptCheckpointID("snapshot-anchor-session"), SessionID: "snapshot-anchor-session", LastRecordID: "anchor-record",
					UnresolvedExecution: &ExecutionAnchor{SessionID: "snapshot-anchor-session", State: "unresolved", Generation: 4},
				}
				state.ImportCheckpoints[sessionTranscriptCheckpointID("snapshot-clean-session")] = ImportCheckpoint{
					ID: sessionTranscriptCheckpointID("snapshot-clean-session"), SessionID: "snapshot-clean-session", LastRecordID: "clean-record",
				}
				return nil
			}); err != nil {
				t.Fatalf("seed snapshot state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			got, err := store.LinkedTranscriptSessionSnapshot(ctx, []string{
				"snapshot-running-session", "snapshot-legacy-session", "snapshot-anchor-session", "snapshot-clean-session",
			})
			if err != nil {
				t.Fatalf("LinkedTranscriptSessionSnapshot: %v", err)
			}
			if !got.Running["snapshot-running-session"] || got.Running["snapshot-legacy-session"] || got.Running["snapshot-anchor-session"] {
				t.Fatalf("running snapshot = %#v", got.Running)
			}
			if !got.Ownership["snapshot-legacy-session"] || !got.Ownership["snapshot-anchor-session"] || got.Ownership["snapshot-clean-session"] {
				t.Fatalf("ownership snapshot = %#v", got.Ownership)
			}
			if got.Checkpoints[sessionTranscriptCheckpointID("snapshot-anchor-session")].LastRecordID != "anchor-record" || got.Checkpoints[sessionTranscriptCheckpointID("snapshot-clean-session")].LastRecordID != "clean-record" {
				t.Fatalf("checkpoint snapshot = %#v", got.Checkpoints)
			}
			execution, err := store.LinkedTranscriptExecutionSnapshot(ctx, []string{"snapshot-running-session", "snapshot-legacy-session", "snapshot-clean-session"})
			if err != nil {
				t.Fatalf("LinkedTranscriptExecutionSnapshot: %v", err)
			}
			if !execution.Running["snapshot-running-session"] || execution.Running["snapshot-clean-session"] || !execution.Ownership["snapshot-legacy-session"] || execution.Ownership["snapshot-clean-session"] {
				t.Fatalf("execution snapshot = %#v", execution)
			}
		})
	}
}

func TestImportCheckpointsForSessionsIsScopedAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				for _, sessionID := range []string{"checkpoint-s1", "checkpoint-s2", "checkpoint-other"} {
					id := sessionTranscriptCheckpointID(sessionID)
					state.ImportCheckpoints[id] = ImportCheckpoint{ID: id, SessionID: sessionID, LastRecordID: sessionID + "-record"}
				}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			got, err := store.ImportCheckpointsForSessions(ctx, []string{"checkpoint-s1", "checkpoint-missing"})
			if err != nil {
				t.Fatalf("ImportCheckpointsForSessions: %v", err)
			}
			if len(got) != 1 || got[sessionTranscriptCheckpointID("checkpoint-s1")].LastRecordID != "checkpoint-s1-record" {
				t.Fatalf("scoped checkpoints = %#v, want only checkpoint-s1", got)
			}
			if _, found := got[sessionTranscriptCheckpointID("checkpoint-other")]; found {
				t.Fatalf("scoped checkpoint query returned unrelated session: %#v", got)
			}
		})
	}
}

func TestImportCheckpointsForSessionsRejectsSessionProvenanceMismatchAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.ImportCheckpoints[sessionTranscriptCheckpointID("owner-s2")] = ImportCheckpoint{
					ID:        sessionTranscriptCheckpointID("owner-s1"),
					SessionID: "owner-s2",
					Status:    importCheckpointStatusComplete,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed mismatched checkpoint: %v", err)
			}
			// Store the row under the canonical ID requested for owner-s1 while
			// retaining owner-s2 as its durable owner.
			if err := store.Update(ctx, func(state *State) error {
				checkpoint := state.ImportCheckpoints[sessionTranscriptCheckpointID("owner-s2")]
				delete(state.ImportCheckpoints, sessionTranscriptCheckpointID("owner-s2"))
				checkpoint.ID = sessionTranscriptCheckpointID("owner-s1")
				state.ImportCheckpoints[checkpoint.ID] = checkpoint
				return nil
			}); err != nil {
				t.Fatalf("move mismatched checkpoint: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			_, err := store.ImportCheckpointsForSessions(ctx, []string{"owner-s1"})
			if !errors.Is(err, ErrSessionStateProvenanceMismatch) {
				t.Fatalf("ImportCheckpointsForSessions error = %v, want ErrSessionStateProvenanceMismatch", err)
			}
			if _, err := store.SessionExecutionStateSnapshot(ctx, "owner-s1", sessionTranscriptCheckpointID("owner-s1")); !errors.Is(err, ErrSessionStateProvenanceMismatch) {
				t.Fatalf("SessionExecutionStateSnapshot error = %v, want ErrSessionStateProvenanceMismatch", err)
			}
			if _, err := store.SessionExecutionOwnershipProbes(ctx, []string{"owner-s1"}); !errors.Is(err, ErrSessionStateProvenanceMismatch) {
				t.Fatalf("SessionExecutionOwnershipProbes error = %v, want ErrSessionStateProvenanceMismatch", err)
			}
		})
	}
}

func TestImportCheckpointsForSessionsRejectsSameSessionWrongEmbeddedIDAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			requestedID := sessionTranscriptCheckpointID("same-session-id")
			if err := store.Update(ctx, func(state *State) error {
				state.ImportCheckpoints[requestedID] = ImportCheckpoint{
					ID:        requestedID,
					SessionID: "same-session-id",
					Status:    importCheckpointStatusComplete,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed same-session wrong-ID checkpoint: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
				if err := store.withStateLock(ctx, func() error {
					pointer, ok, err := store.currentSQLitePointerUnlocked()
					if err != nil || !ok {
						if err == nil {
							err = fmt.Errorf("SQLite pointer is missing")
						}
						return err
					}
					db, err := store.sqliteDBUnlocked(pointer)
					if err != nil {
						return err
					}
					raw, err := json.Marshal(ImportCheckpoint{
						ID:        sessionTranscriptCheckpointID("different-checkpoint"),
						SessionID: "same-session-id",
						Status:    importCheckpointStatusComplete,
					})
					if err != nil {
						return err
					}
					_, err = db.ExecContext(ctx, `UPDATE import_checkpoints SET json = ? WHERE id = ?`, raw, requestedID)
					return err
				}); err != nil {
					t.Fatalf("corrupt SQLite checkpoint row: %v", err)
				}
			} else {
				if err := store.Update(ctx, func(state *State) error {
					checkpoint := state.ImportCheckpoints[requestedID]
					checkpoint.ID = sessionTranscriptCheckpointID("different-checkpoint")
					state.ImportCheckpoints[requestedID] = checkpoint
					return nil
				}); err != nil {
					t.Fatalf("corrupt JSON checkpoint row: %v", err)
				}
			}
			_, err := store.ImportCheckpointsForSessions(ctx, []string{"same-session-id"})
			if !errors.Is(err, ErrSessionStateProvenanceMismatch) {
				t.Fatalf("ImportCheckpointsForSessions error = %v, want ErrSessionStateProvenanceMismatch", err)
			}
		})
	}
}

func TestSQLiteScopedOwnershipQueriesBatchLargeSessionSet(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	const sessionCount = 1200
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < sessionCount; i++ {
			sessionID := fmt.Sprintf("batch-session-%04d", i)
			state.ImportCheckpoints[sessionTranscriptCheckpointID(sessionID)] = ImportCheckpoint{
				ID:        sessionTranscriptCheckpointID(sessionID),
				SessionID: sessionID,
				Status:    importCheckpointStatusComplete,
			}
			state.Turns[fmt.Sprintf("batch-turn-%04d", i)] = Turn{
				ID:             fmt.Sprintf("batch-turn-%04d", i),
				SessionID:      sessionID,
				Status:         TurnStatusInterrupted,
				RecoveryReason: "ordinary cancellation",
			}
		}
		anchorID := sessionTranscriptCheckpointID("batch-session-0999")
		checkpoint := state.ImportCheckpoints[anchorID]
		checkpoint.UnresolvedExecution = &ExecutionAnchor{State: "unresolved", SessionID: "batch-session-0999"}
		state.ImportCheckpoints[anchorID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("seed large session set: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	sessionIDs := make([]string, 0, sessionCount)
	for i := 0; i < sessionCount; i++ {
		sessionIDs = append(sessionIDs, fmt.Sprintf("batch-session-%04d", i))
	}
	checkpoints, err := store.ImportCheckpointsForSessions(ctx, sessionIDs)
	if err != nil {
		t.Fatalf("ImportCheckpointsForSessions large set: %v", err)
	}
	if len(checkpoints) != sessionCount {
		t.Fatalf("large checkpoint result count = %d, want %d", len(checkpoints), sessionCount)
	}
	probes, err := store.SessionExecutionOwnershipProbes(ctx, sessionIDs)
	if err != nil {
		t.Fatalf("SessionExecutionOwnershipProbes large set: %v", err)
	}
	if len(probes) != sessionCount || !probes["batch-session-0999"] {
		t.Fatalf("large ownership probes = count %d target=%v, want %d/true", len(probes), probes["batch-session-0999"], sessionCount)
	}
}

func TestSessionExecutionOwnershipCacheInvalidatesOnSQLitePointerSwitch(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.ImportCheckpoints[sessionTranscriptCheckpointID("pointer-cache")] = ImportCheckpoint{
			ID:        sessionTranscriptCheckpointID("pointer-cache"),
			SessionID: "pointer-cache",
			Status:    importCheckpointStatusComplete,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed pointer cache state: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	pointer, ok, err := store.currentSQLitePointerUnlocked()
	if err != nil || !ok {
		t.Fatalf("current SQLite pointer: ok=%v err=%v", ok, err)
	}
	stamp := store.sessionExecutionOwnershipCacheStampUnlocked(pointer, true)
	if stamp == "" {
		t.Fatal("SQLite ownership cache stamp is empty for a trusted pointer")
	}
	// Simulate a stale negative/positive cache from the previous pointer. A
	// pointer replacement must invalidate it even when the DB/WAL metadata is
	// unchanged.
	store.ownershipProbeStamp = stamp
	store.ownershipProbeCache = map[string]bool{"pointer-cache": true}
	pointer.MigrationID += "-replacement"
	if err := store.writeSQLitePointerUnlocked(pointer); err != nil {
		t.Fatalf("replace SQLite pointer: %v", err)
	}
	probes, err := store.SessionExecutionOwnershipProbes(ctx, []string{"pointer-cache"})
	if err != nil {
		t.Fatalf("SessionExecutionOwnershipProbes after pointer switch: %v", err)
	}
	if probes["pointer-cache"] {
		t.Fatalf("stale ownership cache survived SQLite pointer switch: %#v", probes)
	}
}

func TestClaimNextQueuedTurnRespectsUnresolvedExecutionAcrossBackends(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now()
	seed := func(id string) error {
		return store.Update(ctx, func(state *State) error {
			state.Sessions["claim-session"] = SessionContext{ID: "claim-session", Status: SessionStatusActive}
			state.Turns[id] = Turn{ID: id, SessionID: "claim-session", Status: TurnStatusQueued, QueuedAt: now, CreatedAt: now}
			state.ImportCheckpoints["transcript:claim-session"] = ImportCheckpoint{
				ID: "transcript:claim-session", SessionID: "claim-session", UnresolvedExecution: &ExecutionAnchor{State: "unresolved", OuterTurnID: "old"},
			}
			return nil
		})
	}
	if err := seed("queued-json"); err != nil {
		t.Fatalf("seed JSON claim: %v", err)
	}
	if _, claimed, err := store.ClaimNextQueuedTurn(ctx, "claim-session"); err != nil {
		t.Fatalf("JSON claim: %v", err)
	} else if claimed {
		t.Fatal("JSON claim bypassed unresolved execution")
	}
	if err := store.Update(ctx, func(state *State) error {
		checkpoint := state.ImportCheckpoints["transcript:claim-session"]
		checkpoint.UnresolvedExecution = nil
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("clear JSON anchor: %v", err)
	}
	if _, claimed, err := store.ClaimNextQueuedTurn(ctx, "claim-session"); err != nil || !claimed {
		t.Fatalf("JSON claim after clear = claimed=%v err=%v, want true", claimed, err)
	}
	if _, err := store.MarkTurnCompleted(ctx, "queued-json", "thread-json", "turn-json"); err != nil {
		t.Fatalf("complete JSON claim before SQLite phase: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate SQLite claim: %v", err)
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Turns["queued-sqlite"] = Turn{ID: "queued-sqlite", SessionID: "claim-session", Status: TurnStatusQueued, QueuedAt: now.Add(time.Second), CreatedAt: now.Add(time.Second)}
		checkpoint := state.ImportCheckpoints["transcript:claim-session"]
		checkpoint.UnresolvedExecution = &ExecutionAnchor{State: "unresolved", OuterTurnID: "old-sqlite"}
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("seed SQLite claim: %v", err)
	}
	if _, claimed, err := store.ClaimNextQueuedTurn(ctx, "claim-session"); err != nil {
		t.Fatalf("SQLite claim: %v", err)
	} else if claimed {
		t.Fatal("SQLite claim bypassed unresolved execution")
	}
	if err := store.Update(ctx, func(state *State) error {
		checkpoint := state.ImportCheckpoints["transcript:claim-session"]
		checkpoint.UnresolvedExecution = nil
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("clear SQLite anchor: %v", err)
	}
	if _, claimed, err := store.ClaimNextQueuedTurn(ctx, "claim-session"); err != nil || !claimed {
		t.Fatalf("SQLite claim after clear = claimed=%v err=%v, want true", claimed, err)
	}
}

func TestClaimNextQueuedTurnLegacyAmbiguityIgnoresLaterDurableTerminal(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	base := time.Date(2026, 8, 8, 9, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["legacy-session"] = SessionContext{ID: "legacy-session", Status: SessionStatusActive, CodexThreadID: "thread-legacy"}
		state.Turns["legacy-old"] = Turn{
			ID: "legacy-old", SessionID: "legacy-session", Status: TurnStatusInterrupted,
			CodexThreadID: "thread-legacy", CodexTurnID: "old-codex-turn",
			RecoveryReason: "ambiguous Codex execution: cancellation unconfirmed",
			InterruptedAt:  base, UpdatedAt: base,
		}
		state.Turns["legacy-later"] = Turn{
			ID: "legacy-later", SessionID: "legacy-session", Status: TurnStatusCompleted,
			CodexThreadID: "thread-legacy", CodexTurnID: "later-codex-turn",
			CompletedAt: base.Add(time.Minute), UpdatedAt: base.Add(time.Minute),
		}
		state.Turns["legacy-queued"] = Turn{
			ID: "legacy-queued", SessionID: "legacy-session", Status: TurnStatusQueued,
			QueuedAt: base.Add(2 * time.Minute), CreatedAt: base.Add(2 * time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy state: %v", err)
	}
	if _, claimed, err := store.ClaimNextQueuedTurn(ctx, "legacy-session"); err != nil {
		t.Fatalf("legacy claim with later durable terminal: %v", err)
	} else if claimed {
		t.Fatal("legacy claim bypassed unresolved execution because of a later durable terminal")
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	if _, claimed, err := store.ClaimNextQueuedTurn(ctx, "legacy-session"); err != nil {
		t.Fatalf("legacy SQLite claim with later durable terminal: %v", err)
	} else if claimed {
		t.Fatal("legacy SQLite claim bypassed unresolved execution")
	}
	checkpoint, found, err := store.ImportCheckpoint(ctx, "transcript:legacy-session")
	if err != nil || !found || checkpoint.UnresolvedExecution == nil {
		t.Fatalf("legacy ambiguity was not materialized as a canonical anchor: found=%v err=%v checkpoint=%#v", found, err, checkpoint)
	}
	if checkpoint.UnresolvedExecution.OuterTurnID != "legacy-old" || checkpoint.UnresolvedExecution.Generation != 1 {
		t.Fatalf("materialized legacy anchor = %#v, want outer legacy-old generation 1", checkpoint.UnresolvedExecution)
	}
}

func TestSQLiteLegacyAmbiguityMaterializesLatestInterruptedTurn(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	base := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["legacy-order-session"] = SessionContext{ID: "legacy-order-session", Status: SessionStatusActive, CodexThreadID: "thread-order"}
		state.Turns["interrupted-old"] = Turn{ID: "interrupted-old", SessionID: "legacy-order-session", Status: TurnStatusInterrupted, CodexThreadID: "thread-order", CodexTurnID: "codex-old", RecoveryReason: "ambiguous Codex execution: old", InterruptedAt: base, UpdatedAt: base}
		state.Turns["interrupted-new"] = Turn{ID: "interrupted-new", SessionID: "legacy-order-session", Status: TurnStatusInterrupted, CodexThreadID: "thread-order", CodexTurnID: "codex-new", RecoveryReason: "ambiguous Codex execution: new", InterruptedAt: base.Add(time.Minute), UpdatedAt: base.Add(time.Minute)}
		state.Turns["queued-order"] = Turn{ID: "queued-order", SessionID: "legacy-order-session", Status: TurnStatusQueued, QueuedAt: base.Add(2 * time.Minute), CreatedAt: base.Add(2 * time.Minute)}
		return nil
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	if _, claimed, err := store.ClaimNextQueuedTurn(ctx, "legacy-order-session"); err != nil {
		t.Fatalf("ClaimNextQueuedTurn: %v", err)
	} else if claimed {
		t.Fatal("queued turn bypassed legacy ambiguity")
	}
	checkpoint, found, err := store.ImportCheckpoint(ctx, "transcript:legacy-order-session")
	if err != nil || !found || checkpoint.UnresolvedExecution == nil {
		t.Fatalf("materialized checkpoint = found=%v err=%v checkpoint=%#v", found, err, checkpoint)
	}
	if checkpoint.UnresolvedExecution.OuterTurnID != "interrupted-new" || checkpoint.UnresolvedExecution.CodexTurnID != "codex-new" {
		t.Fatalf("legacy candidate ordering chose %#v, want interrupted-new", checkpoint.UnresolvedExecution)
	}
}

func TestMarkOutboxSendAttemptBlocksLegacyAmbiguousTranscriptWithoutAnchor(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	base := time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC)
	seed := func(outboxID string) error {
		return store.Update(ctx, func(state *State) error {
			state.Sessions["legacy-outbox-session"] = SessionContext{
				ID: "legacy-outbox-session", Status: SessionStatusActive, TeamsChatID: "legacy-chat", CodexThreadID: "legacy-thread",
			}
			state.Turns["legacy-outbox-turn"] = Turn{
				ID: "legacy-outbox-turn", SessionID: "legacy-outbox-session", Status: TurnStatusInterrupted,
				CodexThreadID: "legacy-thread", CodexTurnID: "legacy-codex-turn",
				RecoveryReason: "ambiguous Codex execution: cancellation unconfirmed", InterruptedAt: base, UpdatedAt: base,
			}
			state.OutboxMessages[outboxID] = OutboxMessage{
				ID: outboxID, SessionID: "legacy-outbox-session", TurnID: "legacy-outbox-turn", TeamsChatID: "legacy-chat",
				Kind: "final", NotificationKind: "turn_completed", Body: "legacy answer must remain quarantined", Status: OutboxStatusQueued,
				CreatedAt: base, UpdatedAt: base,
			}
			return nil
		})
	}
	if err := seed("legacy-outbox-json"); err != nil {
		t.Fatalf("seed JSON state: %v", err)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, "legacy-outbox-json"); !errors.Is(err, ErrOutboxSendNotClaimed) {
		t.Fatalf("JSON MarkOutboxSendAttempt error = %v, want ErrOutboxSendNotClaimed", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	if err := seed("legacy-outbox-sqlite"); err != nil {
		t.Fatalf("seed SQLite state: %v", err)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, "legacy-outbox-sqlite"); !errors.Is(err, ErrOutboxSendNotClaimed) {
		t.Fatalf("SQLite MarkOutboxSendAttempt error = %v, want ErrOutboxSendNotClaimed", err)
	}
}

func TestFinalOutboxDeliveryCASRetainsStableGraphIdentityWhenAnchorAppears(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				now := time.Now()
				state.Sessions["final-cas-session"] = SessionContext{ID: "final-cas-session", Status: SessionStatusActive, TeamsChatID: "final-cas-chat"}
				state.ImportCheckpoints["transcript:final-cas-session"] = ImportCheckpoint{
					ID: "transcript:final-cas-session", SessionID: "final-cas-session", Status: "blocked",
					UnresolvedExecution: &ExecutionAnchor{SessionID: "final-cas-session", ThreadID: "thread-cas", OuterTurnID: "outer-cas", CodexTurnID: "codex-cas", State: "unresolved", Generation: 3, UpdatedAt: now},
				}
				state.OutboxMessages["outbox:final-cas"] = OutboxMessage{
					ID: "outbox:final-cas", SessionID: "final-cas-session", TurnID: "outer-cas", TeamsChatID: "final-cas-chat",
					Kind: "final", NotificationKind: "turn_completed", Status: OutboxStatusQueued, Body: "already accepted by Graph", CreatedAt: now, UpdatedAt: now,
				}
				state.OutboxMessages["outbox:final-cas-no-id"] = OutboxMessage{
					ID: "outbox:final-cas-no-id", SessionID: "final-cas-session", TurnID: "outer-cas", TeamsChatID: "final-cas-chat",
					Kind: "final", NotificationKind: "turn_completed", Status: OutboxStatusQueued, Body: "missing Graph identity", CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			got, err := store.MarkOutboxSent(ctx, "outbox:final-cas", "teams-message-cas")
			if err != nil {
				t.Fatalf("MarkOutboxSent: %v", err)
			}
			if got.Status != OutboxStatusAccepted || got.TeamsMessageID != "teams-message-cas" || !got.BlockedByUnresolvedExecution {
				t.Fatalf("final delivery projection = %#v, want accepted with unresolved marker", got)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, "transcript:final-cas-session")
			if err != nil || !found || checkpoint.UnresolvedExecution == nil {
				t.Fatalf("final delivery cleared anchor: found=%v err=%v checkpoint=%#v", found, err, checkpoint)
			}
			if _, err := store.MarkOutboxAccepted(ctx, "outbox:final-cas-no-id", ""); !errors.Is(err, ErrOutboxSendNotClaimed) {
				t.Fatalf("MarkOutboxAccepted without Graph identity = %v, want ErrOutboxSendNotClaimed", err)
			}
			if err := store.Update(ctx, func(state *State) error {
				checkpoint := state.ImportCheckpoints["transcript:final-cas-session"]
				checkpoint.UnresolvedExecution = nil
				state.ImportCheckpoints[checkpoint.ID] = checkpoint
				return nil
			}); err != nil {
				t.Fatalf("clear unresolved anchor: %v", err)
			}
			promoted, err := store.MarkOutboxSent(ctx, "outbox:final-cas", "teams-message-cas")
			if err != nil {
				t.Fatalf("promote accepted delivery: %v", err)
			}
			if promoted.Status != OutboxStatusSent || promoted.TeamsMessageID != "teams-message-cas" || promoted.BlockedByUnresolvedExecution {
				t.Fatalf("promoted delivery = %#v, want sent without unresolved marker", promoted)
			}
		})
	}
}

func TestMarkTurnCompletedRequiresExactExecutionAnchorOwnershipAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now()
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["completion-fence-session"] = SessionContext{ID: "completion-fence-session", Status: SessionStatusActive}
				state.Turns["completion-fence-turn"] = Turn{
					ID: "completion-fence-turn", SessionID: "completion-fence-session", Status: TurnStatusRunning,
					CodexThreadID: "thread-completion-fence", CodexTurnID: "codex-completion-fence", StartedAt: now,
				}
				state.ImportCheckpoints["transcript:completion-fence-session"] = ImportCheckpoint{
					ID: "transcript:completion-fence-session", SessionID: "completion-fence-session",
					UnresolvedExecution: &ExecutionAnchor{
						SessionID: "completion-fence-session", ThreadID: "thread-completion-fence", OuterTurnID: "completion-fence-turn",
						CodexTurnID: "codex-completion-fence", State: "unresolved", Generation: 4,
					},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed completion fence: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			if _, err := store.MarkTurnCompleted(ctx, "completion-fence-turn", "thread-completion-fence", "different-codex-turn"); !errors.Is(err, ErrUnresolvedExecution) {
				t.Fatalf("mismatched completion = %v, want ErrUnresolvedExecution", err)
			}
			turn, found, err := store.TurnByID(ctx, "completion-fence-turn")
			if err != nil || !found || turn.Status != TurnStatusRunning {
				t.Fatalf("mismatched completion changed turn: found=%v err=%v turn=%#v", found, err, turn)
			}
			if _, err := store.MarkTurnCompleted(ctx, "completion-fence-turn", "", ""); !errors.Is(err, ErrUnresolvedExecution) {
				t.Fatalf("no-ID completion = %v, want ErrUnresolvedExecution", err)
			}
			completed, err := store.MarkTurnCompleted(ctx, "completion-fence-turn", "thread-completion-fence", "codex-completion-fence")
			if err != nil {
				t.Fatalf("exact completion: %v", err)
			}
			if completed.Status != TurnStatusCompleted {
				t.Fatalf("exact completion status = %q, want completed", completed.Status)
			}
		})
	}
}

func TestMarkTurnCompletedWithTranscriptCheckpointIsAtomicAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		for _, tc := range []struct {
			name       string
			codexTurn  string
			wantError  bool
			wantAnchor bool
		}{
			{name: "mismatched", codexTurn: "codex-other", wantError: true, wantAnchor: true},
			{name: "exact", codexTurn: "codex-atomic", wantAnchor: false},
		} {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				store := newTestStore(t)
				now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
				if err := store.Update(ctx, func(state *State) error {
					state.Sessions["atomic-session"] = SessionContext{ID: "atomic-session", Status: SessionStatusActive, CodexThreadID: "thread-atomic"}
					state.Turns["atomic-turn"] = Turn{ID: "atomic-turn", SessionID: "atomic-session", Status: TurnStatusRunning, CodexThreadID: "thread-atomic", CodexTurnID: "codex-atomic", StartedAt: now}
					state.ImportCheckpoints["transcript:atomic-session"] = ImportCheckpoint{
						ID: "transcript:atomic-session", SessionID: "atomic-session", SourcePath: "/tmp/atomic.jsonl", LastRecordID: "old-record", LastSourceLine: 4, LastOffset: 96, Status: "complete",
						UnresolvedExecution: &ExecutionAnchor{SessionID: "atomic-session", ThreadID: "thread-atomic", OuterTurnID: "atomic-turn", CodexTurnID: "codex-atomic", State: "unresolved", Generation: 9},
					}
					return nil
				}); err != nil {
					t.Fatalf("seed atomic completion: %v", err)
				}
				if backend == "sqlite" {
					if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
						t.Fatalf("MigrateLargeStateToSQLite: %v", err)
					}
				}
				completed, err := store.MarkTurnCompletedWithTranscriptCheckpoint(ctx, "atomic-turn", "thread-atomic", tc.codexTurn, TranscriptCheckpointProgress{
					ID: "transcript:atomic-session", SessionID: "atomic-session", SourcePath: "/tmp/atomic.jsonl", SourceFingerprint: "fingerprint-final", LastRecordID: "final-record", LastSourceLine: 5, LastOffset: 128,
					SourceSize: 128, SourceModTime: now.Add(time.Second),
				})
				if tc.wantError {
					if !errors.Is(err, ErrUnresolvedExecution) {
						t.Fatalf("mismatched completion error = %v, want ErrUnresolvedExecution", err)
					}
				} else if err != nil {
					t.Fatalf("exact completion error: %v", err)
				}
				if !tc.wantError && completed.Status != TurnStatusCompleted {
					t.Fatalf("completed turn = %#v, want completed", completed)
				}
				turn, found, err := store.TurnByID(ctx, "atomic-turn")
				if err != nil || !found {
					t.Fatalf("load atomic turn: found=%v err=%v", found, err)
				}
				checkpoint, found, err := store.ImportCheckpoint(ctx, "transcript:atomic-session")
				if err != nil || !found {
					t.Fatalf("load atomic checkpoint: found=%v err=%v", found, err)
				}
				if tc.wantError {
					if turn.Status != TurnStatusRunning || checkpoint.LastRecordID != "old-record" || checkpoint.UnresolvedExecution == nil {
						t.Fatalf("mismatched completion partially committed: turn=%#v checkpoint=%#v", turn, checkpoint)
					}
				} else {
					if turn.Status != TurnStatusCompleted || checkpoint.LastRecordID != "final-record" || checkpoint.UnresolvedExecution != nil || checkpoint.ExecutionAnchorGeneration != 9 {
						t.Fatalf("exact completion did not commit atomically: turn=%#v checkpoint=%#v", turn, checkpoint)
					}
				}
				if (checkpoint.UnresolvedExecution != nil) != tc.wantAnchor {
					t.Fatalf("anchor presence = %v, want %v", checkpoint.UnresolvedExecution != nil, tc.wantAnchor)
				}
			})
		}
	}
}

func TestMarkTurnCompletedWithTranscriptCheckpointRejectsCrossSessionCheckpoint(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			defer store.Close()
			now := time.Date(2026, 8, 10, 8, 0, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["provenance-session"] = SessionContext{ID: "provenance-session", Status: SessionStatusActive}
				state.Sessions["other-session"] = SessionContext{ID: "other-session", Status: SessionStatusActive}
				state.Turns["provenance-turn"] = Turn{
					ID: "provenance-turn", SessionID: "provenance-session", Status: TurnStatusRunning,
					CodexThreadID: "provenance-thread", CodexTurnID: "provenance-codex", StartedAt: now,
				}
				// The row is deliberately stored under the requested ID while its
				// embedded owner points at another session. Completion must fail
				// closed before changing either the Turn or this checkpoint.
				state.ImportCheckpoints["transcript:provenance-session"] = ImportCheckpoint{
					ID: "transcript:provenance-session", SessionID: "other-session", LastRecordID: "old-record", LastOffset: 64, Status: "blocked",
				}
				return nil
			}); err != nil {
				t.Fatalf("seed provenance state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			_, err := store.MarkTurnCompletedWithTranscriptCheckpoint(ctx, "provenance-turn", "provenance-thread", "provenance-codex", TranscriptCheckpointProgress{
				ID: "transcript:provenance-session", SessionID: "provenance-session", LastRecordID: "new-record", LastOffset: 128,
			})
			if !errors.Is(err, ErrSessionStateProvenanceMismatch) {
				t.Fatalf("cross-session completion error = %v, want ErrSessionStateProvenanceMismatch", err)
			}
			turn, found, err := store.TurnByID(ctx, "provenance-turn")
			if err != nil || !found || turn.Status != TurnStatusRunning {
				t.Fatalf("cross-session completion changed turn: found=%v err=%v turn=%#v", found, err, turn)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, "transcript:provenance-session")
			if err != nil || !found {
				t.Fatalf("load provenance checkpoint: found=%v err=%v", found, err)
			}
			if checkpoint.SessionID != "other-session" || checkpoint.LastRecordID != "old-record" || checkpoint.LastOffset != 64 {
				t.Fatalf("cross-session completion changed checkpoint: %#v", checkpoint)
			}
		})
	}
}

func TestUpdateImportCheckpointRejectsForeignRebindAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			defer store.Close()
			const checkpointID = "transcript:rebind-session"
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["rebind-session"] = SessionContext{ID: "rebind-session", Status: SessionStatusActive}
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{ID: checkpointID, SessionID: "rebind-session", LastRecordID: "old", Status: "complete"}
				return nil
			}); err != nil {
				t.Fatalf("seed checkpoint: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			_, _, err := store.UpdateImportCheckpoint(ctx, checkpointID, func(current ImportCheckpoint, found bool, now time.Time) (ImportCheckpoint, bool, error) {
				if !found {
					t.Fatal("checkpoint unexpectedly missing")
				}
				current.SessionID = "other-session"
				current.LastRecordID = "foreign-write"
				return current, true, nil
			})
			if !errors.Is(err, ErrSessionStateProvenanceMismatch) {
				t.Fatalf("foreign checkpoint rebind error = %v, want ErrSessionStateProvenanceMismatch", err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
			if err != nil || !found || checkpoint.SessionID != "rebind-session" || checkpoint.LastRecordID != "old" {
				t.Fatalf("foreign rebind changed checkpoint: found=%v err=%v checkpoint=%#v", found, err, checkpoint)
			}
		})
	}
}

func TestUpdateImportCheckpointRejectsForeignCreateInDeterministicNamespaceAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			defer store.Close()
			const checkpointID = "transcript:create-owner"
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			_, changed, err := store.UpdateImportCheckpoint(ctx, checkpointID, func(current ImportCheckpoint, found bool, now time.Time) (ImportCheckpoint, bool, error) {
				if found {
					t.Fatal("checkpoint unexpectedly exists")
				}
				return ImportCheckpoint{SessionID: "foreign-owner", LastRecordID: "foreign-record"}, true, nil
			})
			if !errors.Is(err, ErrSessionStateProvenanceMismatch) || changed {
				t.Fatalf("foreign checkpoint create changed=%v err=%v, want mismatch without mutation", changed, err)
			}
			if _, found, loadErr := store.ImportCheckpoint(ctx, checkpointID); loadErr != nil || found {
				t.Fatalf("foreign checkpoint create left row: found=%v err=%v", found, loadErr)
			}
		})
	}
}

func TestUpdateImportCheckpointRejectsForeignCreateForColonSessionNamespaceAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			defer store.Close()
			const checkpointID = "transcript:session:with:colon"
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			_, changed, err := store.UpdateImportCheckpoint(ctx, checkpointID, func(current ImportCheckpoint, found bool, now time.Time) (ImportCheckpoint, bool, error) {
				if found {
					t.Fatal("checkpoint unexpectedly exists")
				}
				return ImportCheckpoint{SessionID: "foreign-owner", LastRecordID: "foreign-record"}, true, nil
			})
			if !errors.Is(err, ErrSessionStateProvenanceMismatch) || changed {
				t.Fatalf("colon-session checkpoint create changed=%v err=%v, want mismatch without mutation", changed, err)
			}
			if _, found, loadErr := store.ImportCheckpoint(ctx, checkpointID); loadErr != nil || found {
				t.Fatalf("colon-session checkpoint create left row: found=%v err=%v", found, loadErr)
			}
		})
	}
}

func TestRecordTranscriptCheckpointPreservesExistingSessionBindingAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			defer store.Close()
			const checkpointID = "checkpoint:binding-preserved"
			if err := store.Update(ctx, func(state *State) error {
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{ID: checkpointID, SessionID: "bound-session", LastRecordID: "old", Status: importCheckpointStatusComplete}
				return nil
			}); err != nil {
				t.Fatalf("seed checkpoint: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			if err := store.RecordTranscriptCheckpoint(ctx, ImportCheckpoint{
				ID: checkpointID, LastRecordID: "new",
			}, TranscriptLedgerRecord{ID: "ledger:binding-preserved"}); err != nil {
				t.Fatalf("RecordTranscriptCheckpoint: %v", err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
			if err != nil || !found || checkpoint.SessionID != "bound-session" || checkpoint.LastRecordID != "new" {
				t.Fatalf("checkpoint binding = %#v found=%v err=%v, want bound-session/new", checkpoint, found, err)
			}
		})
	}
}

func TestQueueTranscriptDeliveryRejectsCheckpointSwapAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "queue-checkpoint-session"
			const checkpointID = "transcript:queue-checkpoint-session"
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
					ID: checkpointID, SessionID: sessionID, SourcePath: "/tmp/original-transcript.jsonl",
					SourceFingerprint: "original-proof", LastOffset: 128, LastOffsetKnown: true,
					Status: importCheckpointStatusComplete,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			msg := OutboxMessage{
				ID: "outbox:queue-checkpoint-swap", SessionID: sessionID, TurnID: "sync:" + sessionID,
				TeamsChatID: "queue-checkpoint-chat", Kind: "sync-assistant", Body: "stale candidate",
				Status: OutboxStatusQueued, TranscriptCheckpointID: checkpointID,
			}
			_, _, _, err := store.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
				Message:    msg,
				Delivery:   TranscriptDeliveryRecord{ID: "delivery:queue-checkpoint-swap", SessionID: sessionID, SourcePath: "/tmp/replaced-transcript.jsonl", SourceOffset: 128},
				Checkpoint: ImportCheckpoint{ID: checkpointID, SessionID: sessionID, SourcePath: "/tmp/replaced-transcript.jsonl", SourceFingerprint: "replacement-proof", LastOffset: 128, LastOffsetKnown: true},
			})
			if !errors.Is(err, ErrSessionStateProvenanceMismatch) {
				t.Fatalf("QueueTranscriptDeliveryOutbox error = %v, want ErrSessionStateProvenanceMismatch", err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if _, ok := state.OutboxMessages[msg.ID]; ok {
				t.Fatalf("checkpoint swap queued stale outbox: %#v", state.OutboxMessages[msg.ID])
			}
		})
	}
}

func TestMarkTurnFailedRequiresExactExecutionAnchorOwnershipAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now()
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["failure-fence-session"] = SessionContext{ID: "failure-fence-session", Status: SessionStatusActive}
				state.Turns["failure-fence-turn"] = Turn{
					ID: "failure-fence-turn", SessionID: "failure-fence-session", Status: TurnStatusRunning,
					CodexThreadID: "thread-failure-fence", CodexTurnID: "codex-failure-fence", StartedAt: now,
				}
				state.ImportCheckpoints["transcript:failure-fence-session"] = ImportCheckpoint{
					ID: "transcript:failure-fence-session", SessionID: "failure-fence-session",
					UnresolvedExecution: &ExecutionAnchor{
						SessionID: "failure-fence-session", ThreadID: "thread-failure-fence", OuterTurnID: "failure-fence-turn",
						CodexTurnID: "codex-failure-fence", State: "unresolved", Generation: 2,
					},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed failure fence: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			if _, err := store.MarkTurnFailedWithCodexIDs(ctx, "failure-fence-turn", "stale failure", "thread-failure-fence", "different-codex-turn"); !errors.Is(err, ErrUnresolvedExecution) {
				t.Fatalf("mismatched failure = %v, want ErrUnresolvedExecution", err)
			}
			turn, found, err := store.TurnByID(ctx, "failure-fence-turn")
			if err != nil || !found || turn.Status != TurnStatusRunning {
				t.Fatalf("mismatched failure changed turn: found=%v err=%v turn=%#v", found, err, turn)
			}
			if _, err := store.MarkTurnFailedWithCodexIDs(ctx, "failure-fence-turn", "confirmed failure", "thread-failure-fence", "codex-failure-fence"); !errors.Is(err, ErrUnresolvedExecution) {
				t.Fatalf("legacy exact failure = %v, want ErrUnresolvedExecution", err)
			}
			failed, err := store.MarkTurnFailedWithExecutionProof(ctx, ExecutionAnchorClearRequest{
				CheckpointID: sessionTranscriptCheckpointID("failure-fence-session"), SessionID: "failure-fence-session",
				ThreadID: "thread-failure-fence", OuterTurnID: "failure-fence-turn", CodexTurnID: "codex-failure-fence", Generation: 2,
			}, "confirmed failure")
			if err != nil {
				t.Fatalf("exact failure proof: %v", err)
			}
			if failed.Status != TurnStatusFailed || failed.FailureMessage != "confirmed failure" {
				t.Fatalf("exact failure result = %#v", failed)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, "transcript:failure-fence-session")
			if err != nil || !found {
				t.Fatalf("load failure checkpoint: found=%v err=%v", found, err)
			}
			if checkpoint.UnresolvedExecution != nil {
				t.Fatalf("exact failure left unresolved anchor: %#v", checkpoint.UnresolvedExecution)
			}
		})
	}
}

func TestClearFailedExecutionAnchorInstallsTerminalFailureFenceAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			defer store.Close()
			const sessionID = "clear-failed-fence-session"
			const turnID = "clear-failed-fence-turn"
			const checkpointID = "transcript:clear-failed-fence-session"
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusFailed, CodexThreadID: "clear-failed-thread", CodexTurnID: "clear-failed-codex"}
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{ID: checkpointID, SessionID: sessionID, UnresolvedExecution: &ExecutionAnchor{
					SessionID: sessionID, ThreadID: "clear-failed-thread", OuterTurnID: turnID, CodexTurnID: "clear-failed-codex", Generation: 7, State: "unresolved",
				}}
				state.OutboxMessages["outbox:clear-failed-final"] = OutboxMessage{ID: "outbox:clear-failed-final", SessionID: sessionID, TurnID: turnID, Kind: "final", Status: OutboxStatusQueued}
				return nil
			}); err != nil {
				t.Fatalf("seed failed anchor: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			err := store.ClearExecutionAnchorAndConfirmTurn(ctx, ExecutionAnchorClearRequest{
				CheckpointID: checkpointID, SessionID: sessionID, ThreadID: "clear-failed-thread", OuterTurnID: turnID,
				CodexTurnID: "clear-failed-codex", Generation: 7,
			})
			if err != nil {
				t.Fatalf("ClearExecutionAnchorAndConfirmTurn: %v", err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
			if err != nil || !found || checkpoint.UnresolvedExecution != nil {
				t.Fatalf("anchor after clear = %#v found=%v err=%v", checkpoint, found, err)
			}
			final, err := store.OutboxMessageByID(ctx, "outbox:clear-failed-final")
			if err != nil || final.Status != OutboxStatusSkipped {
				t.Fatalf("failed final after clear = %#v err=%v, want skipped terminal final", final, err)
			}
		})
	}
}

func TestMarkTurnFailedWithCodexIDsResolvesInterruptedAnchorAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			defer store.Close()
			now := time.Now()
			anchor := ExecutionAnchor{
				SessionID: "interrupted-failure-session", ThreadID: "interrupted-failure-thread",
				OuterTurnID: "interrupted-failure-turn", CodexTurnID: "interrupted-failure-codex",
				State: "unresolved", Generation: 4,
			}
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[anchor.SessionID] = SessionContext{ID: anchor.SessionID, Status: SessionStatusActive}
				state.Turns[anchor.OuterTurnID] = Turn{
					ID: anchor.OuterTurnID, SessionID: anchor.SessionID, Status: TurnStatusInterrupted,
					CodexThreadID: anchor.ThreadID, CodexTurnID: anchor.CodexTurnID,
					RecoveryReason: "ambiguous Codex execution: cancellation unconfirmed", InterruptedAt: now,
				}
				state.ImportCheckpoints[sessionTranscriptCheckpointID(anchor.SessionID)] = ImportCheckpoint{
					ID: sessionTranscriptCheckpointID(anchor.SessionID), SessionID: anchor.SessionID,
					ExecutionAnchorGeneration: anchor.Generation, UnresolvedExecution: &anchor,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed interrupted failure state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			if _, err := store.MarkTurnFailedWithCodexIDs(ctx, anchor.OuterTurnID, "confirmed interrupted failure", anchor.ThreadID, anchor.CodexTurnID); !errors.Is(err, ErrUnresolvedExecution) {
				t.Fatalf("legacy interrupted failure = %v, want ErrUnresolvedExecution", err)
			}
			failed, err := store.MarkTurnFailedWithExecutionProof(ctx, ExecutionAnchorClearRequest{
				CheckpointID: sessionTranscriptCheckpointID(anchor.SessionID), SessionID: anchor.SessionID,
				ThreadID: anchor.ThreadID, OuterTurnID: anchor.OuterTurnID, CodexTurnID: anchor.CodexTurnID, Generation: anchor.Generation,
			}, "confirmed interrupted failure")
			if err != nil {
				t.Fatalf("MarkTurnFailedWithExecutionProof: %v", err)
			}
			if failed.Status != TurnStatusFailed || failed.FailureMessage != "confirmed interrupted failure" {
				t.Fatalf("failed turn = %#v, want failed with callback message", failed)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, sessionTranscriptCheckpointID(anchor.SessionID))
			if err != nil || !found || checkpoint.UnresolvedExecution != nil {
				t.Fatalf("checkpoint = %#v found=%v err=%v, want anchor cleared", checkpoint, found, err)
			}
		})
	}
}

func TestClearExecutionAnchorAndConfirmTurnIsAtomicAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		for _, generation := range []int64{7, 8} {
			t.Run(backend+"/generation-"+fmt.Sprint(generation), func(t *testing.T) {
				store := newTestStore(t)
				now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
				anchor := ExecutionAnchor{SessionID: "clear-session", ThreadID: "thread-clear", OuterTurnID: "turn-clear", CodexTurnID: "codex-clear", SourcePath: "/tmp/clear.jsonl", SourceFingerprint: "fingerprint-clear", CutoffRecordID: "cutoff", CutoffLine: 4, CutoffOffset: 96, State: "unresolved", Generation: 8}
				if err := store.Update(ctx, func(state *State) error {
					state.Sessions[anchor.SessionID] = SessionContext{ID: anchor.SessionID, Status: SessionStatusActive}
					state.Turns[anchor.OuterTurnID] = Turn{ID: anchor.OuterTurnID, SessionID: anchor.SessionID, Status: TurnStatusInterrupted, CodexThreadID: anchor.ThreadID, CodexTurnID: anchor.CodexTurnID, RecoveryReason: "ambiguous Codex execution: cancellation unconfirmed", InterruptedAt: now}
					state.ImportCheckpoints["transcript:"+anchor.SessionID] = ImportCheckpoint{ID: "transcript:" + anchor.SessionID, SessionID: anchor.SessionID, ExecutionAnchorGeneration: anchor.Generation, UnresolvedExecution: &anchor}
					return nil
				}); err != nil {
					t.Fatalf("seed clear anchor: %v", err)
				}
				if backend == "sqlite" {
					if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
						t.Fatalf("MigrateLargeStateToSQLite: %v", err)
					}
				}
				err := store.ClearExecutionAnchorAndConfirmTurn(ctx, ExecutionAnchorClearRequest{
					CheckpointID: "transcript:" + anchor.SessionID, SessionID: anchor.SessionID, ThreadID: anchor.ThreadID, SourcePath: anchor.SourcePath, SourceFingerprint: anchor.SourceFingerprint,
					OuterTurnID: anchor.OuterTurnID, CodexTurnID: anchor.CodexTurnID, Generation: generation, CutoffRecordID: anchor.CutoffRecordID, CutoffLine: anchor.CutoffLine, CutoffOffset: anchor.CutoffOffset,
					RecoveryReasonFrom: "ambiguous Codex execution: cancellation unconfirmed", RecoveryReasonTo: "Codex execution ownership confirmed",
				})
				if err != nil {
					t.Fatalf("clear anchor: %v", err)
				}
				checkpoint, found, err := store.ImportCheckpoint(ctx, "transcript:"+anchor.SessionID)
				if err != nil || !found {
					t.Fatalf("load cleared checkpoint: found=%v err=%v", found, err)
				}
				turn, found, err := store.TurnByID(ctx, anchor.OuterTurnID)
				if err != nil || !found {
					t.Fatalf("load cleared turn: found=%v err=%v", found, err)
				}
				if generation != anchor.Generation {
					if checkpoint.UnresolvedExecution == nil || turn.RecoveryReason != "ambiguous Codex execution: cancellation unconfirmed" {
						t.Fatalf("stale clear partially committed: checkpoint=%#v turn=%#v", checkpoint, turn)
					}
				} else if checkpoint.UnresolvedExecution != nil || turn.RecoveryReason != "Codex execution ownership confirmed" {
					t.Fatalf("exact clear did not commit atomically: checkpoint=%#v turn=%#v", checkpoint, turn)
				}
			})
		}
	}
}

func TestQueueTranscriptDeliveryRequiresExplicitCodexTurnProofAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now()
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["delivery-proof-session"] = SessionContext{ID: "delivery-proof-session", Status: SessionStatusActive}
				state.Turns["delivery-proof-turn"] = Turn{ID: "delivery-proof-turn", SessionID: "delivery-proof-session", Status: TurnStatusRunning, CodexThreadID: "delivery-proof-thread", CodexTurnID: "delivery-proof-codex", StartedAt: now}
				state.ImportCheckpoints["transcript:delivery-proof-session"] = ImportCheckpoint{
					ID: "transcript:delivery-proof-session", SessionID: "delivery-proof-session",
					UnresolvedExecution: &ExecutionAnchor{SessionID: "delivery-proof-session", ThreadID: "delivery-proof-thread", OuterTurnID: "delivery-proof-turn", CodexTurnID: "delivery-proof-codex", State: "unresolved", Generation: 1},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed delivery proof: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			request := func(id string, codexTurnID string) error {
				_, _, _, err := store.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
					Message:    OutboxMessage{ID: id, SessionID: "delivery-proof-session", TurnID: "delivery-proof-turn", CodexThreadID: "delivery-proof-thread", TeamsChatID: "delivery-proof-chat", Kind: "final", NotificationKind: "turn_completed", Body: "proof final"},
					Delivery:   TranscriptDeliveryRecord{ID: "delivery:" + id, SessionID: "delivery-proof-session", CodexThreadID: "delivery-proof-thread", CodexTurnID: codexTurnID, Kind: "final", Status: TranscriptDeliveryStatusQueued},
					Checkpoint: ImportCheckpoint{ID: "transcript:delivery-proof-session", SessionID: "delivery-proof-session"},
				})
				return err
			}
			if err := request("outbox:delivery-proof-no-id", ""); !errors.Is(err, ErrUnresolvedExecution) {
				t.Fatalf("no-ID transcript delivery = %v, want ErrUnresolvedExecution", err)
			}
			if err := request("outbox:delivery-proof-exact", "delivery-proof-codex"); err != nil {
				t.Fatalf("exact transcript delivery = %v", err)
			}
		})
	}
}

func TestBackgroundImportDoesNotBypassUnresolvedExecutionAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now()
			const sessionID = "background-import-fence-session"
			const checkpointID = "transcript:" + sessionID
			const threadID = "background-import-fence-thread"
			const codexTurnID = "background-import-fence-codex"
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.Turns["background-import-fence-turn"] = Turn{
					ID: "background-import-fence-turn", SessionID: sessionID, Status: TurnStatusRunning,
					CodexThreadID: threadID, CodexTurnID: codexTurnID, StartedAt: now,
				}
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
					ID: checkpointID, SessionID: sessionID,
					UnresolvedExecution: &ExecutionAnchor{
						SessionID: sessionID, ThreadID: threadID, OuterTurnID: "background-import-fence-turn",
						CodexTurnID: codexTurnID, State: "unresolved", Generation: 1,
					},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed background import fence: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			_, _, _, err := store.QueueTranscriptDeliveryOutbox(ctx, TranscriptDeliveryQueueRequest{
				Message: OutboxMessage{
					ID: "outbox:background-import-fence", SessionID: sessionID,
					TurnID: "import-bg:background-import-fence", CodexThreadID: threadID,
					TeamsChatID: "background-import-fence-chat", Kind: "import-bg-batch-assistant",
					Body: "background history must remain fenced",
				},
				Delivery: TranscriptDeliveryRecord{
					ID: "delivery:background-import-fence", SessionID: sessionID,
					CodexThreadID: threadID, Kind: "import-bg-batch-assistant",
					Status: TranscriptDeliveryStatusQueued,
				},
				Checkpoint: ImportCheckpoint{ID: checkpointID, SessionID: sessionID},
			})
			if !errors.Is(err, ErrUnresolvedExecution) {
				t.Fatalf("background import queue error = %v, want ErrUnresolvedExecution", err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load background import fence state: %v", err)
			}
			if _, found := state.OutboxMessages["outbox:background-import-fence"]; found {
				t.Fatal("background import queued transcript delivery across unresolved execution")
			}
		})
	}
}

func TestQuarantineQueuedTerminalAnswerOutboxPreservesAcceptedIdentityAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now()
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["quarantine-session"] = SessionContext{ID: "quarantine-session", Status: SessionStatusActive}
				state.Turns["quarantine-turn"] = Turn{ID: "quarantine-turn", SessionID: "quarantine-session", Status: TurnStatusRunning}
				state.OutboxMessages["outbox:quarantine-queued"] = OutboxMessage{
					ID: "outbox:quarantine-queued", SessionID: "quarantine-session", TurnID: "quarantine-turn",
					TeamsChatID: "chat-quarantine", Kind: "final", NotificationKind: "turn_completed", Status: OutboxStatusQueued,
					Body: "unsent final", CreatedAt: now, UpdatedAt: now,
				}
				state.OutboxMessages["outbox:quarantine-accepted"] = OutboxMessage{
					ID: "outbox:quarantine-accepted", SessionID: "quarantine-session", TurnID: "quarantine-turn",
					TeamsChatID: "chat-quarantine", Kind: "final", NotificationKind: "turn_completed", Status: OutboxStatusAccepted,
					TeamsMessageID: "teams-accepted", BlockedByUnresolvedExecution: true, Body: "accepted final", CreatedAt: now, UpdatedAt: now,
				}
				state.OutboxMessages["outbox:quarantine-sending"] = OutboxMessage{
					ID: "outbox:quarantine-sending", SessionID: "quarantine-session", TurnID: "quarantine-turn",
					TeamsChatID: "chat-quarantine", Kind: "final", NotificationKind: "turn_completed", Status: OutboxStatusSending,
					SendAttemptToken: "attempt-quarantine", Body: "in-flight final", CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed quarantine state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			changed, err := store.QuarantineQueuedTerminalAnswerOutbox(ctx, "quarantine-turn", "superseded by unresolved execution")
			if err != nil || changed != 1 {
				t.Fatalf("quarantine changed=%d err=%v, want one", changed, err)
			}
			queued, err := store.OutboxMessageByID(ctx, "outbox:quarantine-queued")
			if err != nil || queued.Status != OutboxStatusSkipped {
				t.Fatalf("queued final = %#v err=%v, want skipped", queued, err)
			}
			accepted, err := store.OutboxMessageByID(ctx, "outbox:quarantine-accepted")
			if err != nil || accepted.Status != OutboxStatusAccepted || accepted.TeamsMessageID != "teams-accepted" || !accepted.BlockedByUnresolvedExecution {
				t.Fatalf("accepted final = %#v err=%v, want unchanged accepted identity", accepted, err)
			}
			sending, err := store.OutboxMessageByID(ctx, "outbox:quarantine-sending")
			if err != nil || sending.Status != OutboxStatusSending || sending.SendAttemptToken != "attempt-quarantine" {
				t.Fatalf("sending final = %#v err=%v, want in-flight attempt preserved", sending, err)
			}
			changed, err = store.QuarantineQueuedTerminalAnswerOutbox(ctx, "quarantine-turn", "superseded by unresolved execution")
			if err != nil || changed != 0 {
				t.Fatalf("repeat quarantine changed=%d err=%v, want idempotent zero", changed, err)
			}
		})
	}
}

func TestInFlightTerminalOutboxCanRecordAcceptedIDAfterQuarantineAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now()
			anchor := ExecutionAnchor{
				SessionID: "inflight-session", ThreadID: "inflight-thread", OuterTurnID: "inflight-turn", CodexTurnID: "inflight-codex",
				State: "unresolved", Generation: 4, UpdatedAt: now,
			}
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[anchor.SessionID] = SessionContext{ID: anchor.SessionID, Status: SessionStatusActive}
				state.Turns[anchor.OuterTurnID] = Turn{ID: anchor.OuterTurnID, SessionID: anchor.SessionID, Status: TurnStatusRunning, CodexThreadID: anchor.ThreadID, CodexTurnID: anchor.CodexTurnID}
				state.ImportCheckpoints[sessionTranscriptCheckpointID(anchor.SessionID)] = ImportCheckpoint{
					ID: sessionTranscriptCheckpointID(anchor.SessionID), SessionID: anchor.SessionID,
					ExecutionAnchorGeneration: anchor.Generation, UnresolvedExecution: &anchor,
				}
				state.OutboxMessages["outbox:inflight-final"] = OutboxMessage{
					ID: "outbox:inflight-final", SessionID: anchor.SessionID, TurnID: anchor.OuterTurnID,
					TeamsChatID: "inflight-chat", Kind: "final", NotificationKind: "turn_completed", Status: OutboxStatusSending,
					SendAttemptToken: "attempt-inflight", Body: "already posted", CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			changed, err := store.QuarantineQueuedTerminalAnswerOutbox(ctx, anchor.OuterTurnID, "superseded by unresolved execution")
			if err != nil || changed != 0 {
				t.Fatalf("quarantine changed=%d err=%v, want no in-flight mutation", changed, err)
			}
			current, err := store.OutboxMessageByID(ctx, "outbox:inflight-final")
			if err != nil || current.Status != OutboxStatusSending {
				t.Fatalf("after quarantine outbox=%#v err=%v, want sending", current, err)
			}
			accepted, err := store.MarkOutboxAcceptedForAttempt(ctx, current.ID, current.SendAttemptToken, "teams-inflight")
			if err != nil {
				t.Fatalf("MarkOutboxAcceptedForAttempt: %v", err)
			}
			if accepted.Status != OutboxStatusAccepted || accepted.TeamsMessageID != "teams-inflight" || !accepted.BlockedByUnresolvedExecution {
				t.Fatalf("accepted outbox=%#v, want stable blocked identity", accepted)
			}
			promoted, err := store.MarkOutboxSentForAttempt(ctx, current.ID, current.SendAttemptToken, "teams-inflight")
			if err != nil {
				t.Fatalf("MarkOutboxSentForAttempt: %v", err)
			}
			if promoted.Status != OutboxStatusAccepted || promoted.TeamsMessageID != "teams-inflight" || !promoted.BlockedByUnresolvedExecution {
				t.Fatalf("promoted outbox=%#v, want accepted blocked identity retained", promoted)
			}
		})
	}
}

func TestMarkTurnFailedWithExecutionProofClearsAnchorAtomicallyAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		for _, status := range []TurnStatus{TurnStatusRunning, TurnStatusInterrupted} {
			t.Run(backend+"/"+string(status), func(t *testing.T) {
				store := newTestStore(t)
				now := time.Now()
				anchor := ExecutionAnchor{
					SessionID: "failure-proof-session", ThreadID: "failure-proof-thread", OuterTurnID: "failure-proof-turn", CodexTurnID: "failure-proof-codex",
					SourcePath: "/tmp/failure-proof.jsonl", SourceFingerprint: "failure-proof-fingerprint", CutoffRecordID: "before-failure", CutoffLine: 3, CutoffOffset: 72,
					State: "unresolved", Generation: 9, UpdatedAt: now,
				}
				recoveryReason := ""
				if status == TurnStatusInterrupted {
					recoveryReason = "ambiguous Codex execution: failure callback was delayed"
				}
				if err := store.Update(ctx, func(state *State) error {
					state.Sessions[anchor.SessionID] = SessionContext{ID: anchor.SessionID, Status: SessionStatusActive}
					state.Turns[anchor.OuterTurnID] = Turn{
						ID: anchor.OuterTurnID, SessionID: anchor.SessionID, Status: status,
						CodexThreadID: anchor.ThreadID, CodexTurnID: anchor.CodexTurnID, RecoveryReason: recoveryReason,
						StartedAt: now, InterruptedAt: func() time.Time {
							if status == TurnStatusInterrupted {
								return now
							}
							return time.Time{}
						}(),
					}
					state.ImportCheckpoints[sessionTranscriptCheckpointID(anchor.SessionID)] = ImportCheckpoint{
						ID: sessionTranscriptCheckpointID(anchor.SessionID), SessionID: anchor.SessionID,
						ExecutionAnchorGeneration: anchor.Generation, UnresolvedExecution: &anchor,
					}
					state.OutboxMessages["outbox:failure-queued"] = OutboxMessage{
						ID: "outbox:failure-queued", SessionID: anchor.SessionID, TurnID: anchor.OuterTurnID,
						TeamsChatID: "failure-proof-chat", Kind: "final", NotificationKind: "turn_completed", Status: OutboxStatusQueued,
						Body: "losing final", CreatedAt: now, UpdatedAt: now,
					}
					state.OutboxMessages["outbox:failure-sending"] = OutboxMessage{
						ID: "outbox:failure-sending", SessionID: anchor.SessionID, TurnID: anchor.OuterTurnID,
						TeamsChatID: "failure-proof-chat", Kind: "final", NotificationKind: "turn_completed", Status: OutboxStatusSending,
						SendAttemptToken: "failure-attempt", Body: "in-flight final", CreatedAt: now, UpdatedAt: now,
					}
					return nil
				}); err != nil {
					t.Fatalf("seed state: %v", err)
				}
				if backend == "sqlite" {
					if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
						t.Fatalf("MigrateLargeStateToSQLite: %v", err)
					}
				}
				proof := ExecutionAnchorClearRequest{
					CheckpointID: sessionTranscriptCheckpointID(anchor.SessionID), SessionID: anchor.SessionID,
					ThreadID: anchor.ThreadID, SourcePath: anchor.SourcePath, SourceFingerprint: anchor.SourceFingerprint,
					OuterTurnID: anchor.OuterTurnID, CodexTurnID: anchor.CodexTurnID, Generation: anchor.Generation,
					CutoffRecordID: anchor.CutoffRecordID, CutoffLine: anchor.CutoffLine, CutoffOffset: anchor.CutoffOffset,
				}
				stale := proof
				stale.Generation++
				if _, err := store.MarkTurnFailedWithExecutionProof(ctx, stale, "stale failure"); !errors.Is(err, ErrUnresolvedExecution) {
					t.Fatalf("stale failure proof error=%v, want ErrUnresolvedExecution", err)
				}
				before, found, err := store.TurnByID(ctx, anchor.OuterTurnID)
				if err != nil || !found || before.Status != status {
					t.Fatalf("stale failure changed turn=%#v found=%v err=%v", before, found, err)
				}
				if _, err := store.MarkTurnFailedWithExecutionProof(ctx, proof, "confirmed app-server failure"); err != nil {
					t.Fatalf("MarkTurnFailedWithExecutionProof: %v", err)
				}
				failed, found, err := store.TurnByID(ctx, anchor.OuterTurnID)
				if err != nil || !found || failed.Status != TurnStatusFailed || failed.FailureMessage != "confirmed app-server failure" {
					t.Fatalf("failed turn=%#v found=%v err=%v", failed, found, err)
				}
				checkpoint, found, err := store.ImportCheckpoint(ctx, proof.CheckpointID)
				if err != nil || !found || checkpoint.UnresolvedExecution != nil || checkpoint.ExecutionAnchorGeneration != anchor.Generation {
					t.Fatalf("checkpoint=%#v found=%v err=%v, want cleared anchor", checkpoint, found, err)
				}
				queued, err := store.OutboxMessageByID(ctx, "outbox:failure-queued")
				if err != nil || queued.Status != OutboxStatusSkipped {
					t.Fatalf("queued final=%#v err=%v, want skipped", queued, err)
				}
				sending, err := store.OutboxMessageByID(ctx, "outbox:failure-sending")
				if err != nil || sending.Status != OutboxStatusSending || sending.SendAttemptToken != "failure-attempt" || !sending.BlockedByTerminalFailure {
					t.Fatalf("sending final=%#v err=%v, want preserved attempt", sending, err)
				}
				accepted, err := store.MarkOutboxAcceptedForAttempt(ctx, sending.ID, sending.SendAttemptToken, "teams-failure-late")
				if err != nil || accepted.Status != OutboxStatusAccepted || accepted.TeamsMessageID != "teams-failure-late" || !accepted.BlockedByTerminalFailure {
					t.Fatalf("late accepted final=%#v err=%v, want stable blocked identity", accepted, err)
				}
				promoted, err := store.MarkOutboxSentForAttempt(ctx, sending.ID, sending.SendAttemptToken, "teams-failure-late")
				if err != nil || promoted.Status != OutboxStatusAccepted || promoted.TeamsMessageID != "teams-failure-late" || !promoted.BlockedByTerminalFailure {
					t.Fatalf("late sent final=%#v err=%v, want terminal-failure fence retained", promoted, err)
				}
				// A duplicate exact failure callback is harmless after the anchor has
				// been consumed and must not rewrite the terminal result.
				duplicate, err := store.MarkTurnFailedWithExecutionProof(ctx, proof, "duplicate failure")
				if err != nil || duplicate.Status != TurnStatusFailed || duplicate.FailureMessage != "confirmed app-server failure" {
					t.Fatalf("duplicate failure=%#v err=%v, want original terminal result", duplicate, err)
				}
			})
		}
	}
}

func TestMarkTurnFailedWithExecutionProofFencesEveryTerminalChunkAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC()
			const (
				sessionID = "terminal-chunks-session"
				turnID    = "terminal-chunks-turn"
				threadID  = "terminal-chunks-thread"
				codexID   = "terminal-chunks-codex"
			)
			anchor := ExecutionAnchor{
				SessionID: sessionID, ThreadID: threadID, OuterTurnID: turnID, CodexTurnID: codexID,
				SourcePath: "/tmp/terminal-chunks.jsonl", SourceFingerprint: "terminal-chunks-source",
				CutoffRecordID: "before-chunks", CutoffLine: 4, CutoffOffset: 128,
				State: "unresolved", Generation: 12, UpdatedAt: now,
			}
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.Turns[turnID] = Turn{
					ID: turnID, SessionID: sessionID, Status: TurnStatusRunning,
					CodexThreadID: threadID, CodexTurnID: codexID, StartedAt: now,
				}
				checkpointID := sessionTranscriptCheckpointID(sessionID)
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
					ID: checkpointID, SessionID: sessionID, ExecutionAnchorGeneration: anchor.Generation,
					UnresolvedExecution: &anchor,
				}
				state.OutboxMessages["outbox:terminal-chunks-final"] = OutboxMessage{
					ID: "outbox:terminal-chunks-final", SessionID: sessionID, TurnID: turnID,
					TeamsChatID: "terminal-chunks-chat", Kind: "final", NotificationKind: "turn_completed",
					Status: OutboxStatusQueued, Body: "first chunk", PartIndex: 1, PartCount: 3,
					TerminalGroupID: terminalOutboxGroupID(turnID), CreatedAt: now, UpdatedAt: now,
				}
				// Continuation chunks intentionally have no notification kind.  They
				// must still be fenced by the same terminal group.
				state.OutboxMessages["outbox:terminal-chunks-final-002"] = OutboxMessage{
					ID: "outbox:terminal-chunks-final-002", SessionID: sessionID, TurnID: turnID,
					TeamsChatID: "terminal-chunks-chat", Kind: "final-002",
					Status: OutboxStatusQueued, Body: "second chunk", PartIndex: 2, PartCount: 3,
					TerminalGroupID: terminalOutboxGroupID(turnID), CreatedAt: now, UpdatedAt: now,
				}
				state.OutboxMessages["outbox:terminal-chunks-final-003"] = OutboxMessage{
					ID: "outbox:terminal-chunks-final-003", SessionID: sessionID, TurnID: turnID,
					TeamsChatID: "terminal-chunks-chat", Kind: "final-003",
					Status: OutboxStatusSending, SendAttemptToken: "terminal-chunks-attempt",
					Body: "third chunk", PartIndex: 3, PartCount: 3,
					TerminalGroupID: terminalOutboxGroupID(turnID), CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			proof := ExecutionAnchorClearRequest{
				CheckpointID: sessionTranscriptCheckpointID(sessionID), SessionID: sessionID,
				ThreadID: threadID, SourcePath: anchor.SourcePath, SourceFingerprint: anchor.SourceFingerprint,
				OuterTurnID: turnID, CodexTurnID: codexID, Generation: anchor.Generation,
				CutoffRecordID: anchor.CutoffRecordID, CutoffLine: anchor.CutoffLine, CutoffOffset: anchor.CutoffOffset,
			}
			if _, err := store.MarkTurnFailedWithExecutionProof(ctx, proof, "terminal chunk failure"); err != nil {
				t.Fatalf("MarkTurnFailedWithExecutionProof: %v", err)
			}
			for _, id := range []string{"outbox:terminal-chunks-final", "outbox:terminal-chunks-final-002"} {
				msg, err := store.OutboxMessageByID(ctx, id)
				if err != nil || msg.Status != OutboxStatusSkipped {
					t.Fatalf("queued chunk %q = %#v err=%v, want skipped", id, msg, err)
				}
			}
			inflight, err := store.OutboxMessageByID(ctx, "outbox:terminal-chunks-final-003")
			if err != nil || inflight.Status != OutboxStatusSending || !inflight.BlockedByTerminalFailure {
				t.Fatalf("in-flight continuation = %#v err=%v, want Sending+terminal fence", inflight, err)
			}
			accepted, err := store.MarkOutboxAcceptedForAttempt(ctx, inflight.ID, inflight.SendAttemptToken, "teams-terminal-chunk-late")
			if err != nil || accepted.Status != OutboxStatusAccepted || !accepted.BlockedByTerminalFailure {
				t.Fatalf("late accepted continuation = %#v err=%v, want Accepted+terminal fence", accepted, err)
			}
			settled, err := store.MarkOutboxSentForAttempt(ctx, inflight.ID, inflight.SendAttemptToken, "teams-terminal-chunk-late")
			if err != nil || settled.Status != OutboxStatusAccepted || !settled.BlockedByTerminalFailure {
				t.Fatalf("late sent continuation = %#v err=%v, want Accepted+terminal fence", settled, err)
			}
		})
	}
}

func TestMarkTurnFailedForExecutionResolvesCurrentAnchorAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now()
			identity := ExecutionFailureIdentity{
				SessionID: "execution-failure-session", TurnID: "execution-failure-turn",
				ThreadID: "execution-failure-thread", CodexTurnID: "execution-failure-codex",
			}
			anchor := ExecutionAnchor{
				SessionID: identity.SessionID, ThreadID: identity.ThreadID, OuterTurnID: identity.TurnID,
				CodexTurnID: identity.CodexTurnID, SourcePath: "/tmp/execution-failure.jsonl",
				SourceFingerprint: "execution-failure-source", CutoffRecordID: "before-failure",
				CutoffLine: 4, CutoffOffset: 96, State: "unresolved", Generation: 7, UpdatedAt: now,
			}
			identity.AnchorGeneration = anchor.Generation
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[identity.SessionID] = SessionContext{ID: identity.SessionID, Status: SessionStatusActive}
				state.Turns[identity.TurnID] = Turn{ID: identity.TurnID, SessionID: identity.SessionID, Status: TurnStatusRunning, StartedAt: now}
				state.ImportCheckpoints[sessionTranscriptCheckpointID(identity.SessionID)] = ImportCheckpoint{
					ID: sessionTranscriptCheckpointID(identity.SessionID), SessionID: identity.SessionID,
					ExecutionAnchorGeneration: anchor.Generation, UnresolvedExecution: &anchor,
				}
				state.OutboxMessages["outbox:execution-failure-sending"] = OutboxMessage{
					ID: "outbox:execution-failure-sending", SessionID: identity.SessionID, TurnID: identity.TurnID,
					TeamsChatID: "execution-failure-chat", Kind: "final", NotificationKind: "turn_completed",
					Status: OutboxStatusSending, SendAttemptToken: "execution-failure-attempt", CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			stale := identity
			stale.CodexTurnID = "execution-failure-other-codex"
			if _, err := store.MarkTurnFailedForExecution(ctx, stale, "stale failure"); !errors.Is(err, ErrStaleExecutionCallback) {
				t.Fatalf("stale execution failure error=%v, want ErrStaleExecutionCallback", err)
			}
			noID := identity
			noID.ThreadID = ""
			noID.CodexTurnID = ""
			if _, err := store.MarkTurnFailedForExecution(ctx, noID, "unverified failure"); !errors.Is(err, ErrStaleExecutionCallback) {
				t.Fatalf("no-ID execution failure error=%v, want ErrStaleExecutionCallback", err)
			}
			before, found, err := store.TurnByID(ctx, identity.TurnID)
			if err != nil || !found || before.Status != TurnStatusRunning {
				t.Fatalf("stale callback changed turn=%#v found=%v err=%v", before, found, err)
			}

			failed, err := store.MarkTurnFailedForExecution(ctx, identity, "confirmed execution failure")
			if err != nil || failed.Status != TurnStatusFailed || failed.FailureMessage != "confirmed execution failure" {
				t.Fatalf("MarkTurnFailedForExecution=%#v err=%v", failed, err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, sessionTranscriptCheckpointID(identity.SessionID))
			if err != nil || !found || checkpoint.UnresolvedExecution != nil || checkpoint.ExecutionAnchorGeneration != anchor.Generation {
				t.Fatalf("checkpoint=%#v found=%v err=%v, want exact anchor cleared", checkpoint, found, err)
			}
			pending, err := store.OutboxMessageByID(ctx, "outbox:execution-failure-sending")
			if err != nil || pending.Status != OutboxStatusSending || pending.SendAttemptToken != "execution-failure-attempt" || !pending.BlockedByTerminalFailure {
				t.Fatalf("pending final=%#v err=%v, want fenced in-flight attempt", pending, err)
			}
			accepted, err := store.MarkOutboxAcceptedForAttempt(ctx, pending.ID, pending.SendAttemptToken, "teams-execution-failure")
			if err != nil || accepted.Status != OutboxStatusAccepted || !accepted.BlockedByTerminalFailure {
				t.Fatalf("accepted late final=%#v err=%v, want blocked", accepted, err)
			}
			promoted, err := store.MarkOutboxSentForAttempt(ctx, pending.ID, pending.SendAttemptToken, "teams-execution-failure")
			if err != nil || promoted.Status != OutboxStatusAccepted || !promoted.BlockedByTerminalFailure {
				t.Fatalf("sent late final=%#v err=%v, want no promotion", promoted, err)
			}
		})
	}
}

func TestMarkTurnFailedForExecutionRejectsReusedIDsFromOlderAnchorGenerationAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const (
				sessionID = "generation-reuse-session"
				turnID    = "generation-reuse-turn"
				threadID  = "generation-reuse-thread"
				codexID   = "generation-reuse-codex"
			)
			now := time.Now().UTC()
			anchor := ExecutionAnchor{
				SessionID: sessionID, ThreadID: threadID, OuterTurnID: turnID, CodexTurnID: codexID,
				SourcePath: "/tmp/generation-reuse.jsonl", SourceFingerprint: "generation-reuse-source",
				CutoffRecordID: "generation-reuse-cutoff", CutoffLine: 2, CutoffOffset: 64,
				State: "unresolved", Generation: 8, UpdatedAt: now,
			}
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusRunning, CodexThreadID: threadID, CodexTurnID: codexID, StartedAt: now}
				checkpointID := sessionTranscriptCheckpointID(sessionID)
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{ID: checkpointID, SessionID: sessionID, ExecutionAnchorGeneration: anchor.Generation, UnresolvedExecution: &anchor}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			old := ExecutionFailureIdentity{SessionID: sessionID, TurnID: turnID, ThreadID: threadID, CodexTurnID: codexID, AnchorGeneration: 7}
			if _, err := store.MarkTurnFailedForExecution(ctx, old, "old generation callback"); !errors.Is(err, ErrStaleExecutionCallback) {
				t.Fatalf("old generation error=%v, want ErrStaleExecutionCallback", err)
			}
			turn, found, err := store.TurnByID(ctx, turnID)
			if err != nil || !found || turn.Status != TurnStatusRunning {
				t.Fatalf("old generation changed turn=%#v found=%v err=%v", turn, found, err)
			}
			current := old
			current.AnchorGeneration = anchor.Generation
			failed, err := store.MarkTurnFailedForExecution(ctx, current, "current generation failure")
			if err != nil || failed.Status != TurnStatusFailed {
				t.Fatalf("current generation result=%#v err=%v", failed, err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, sessionTranscriptCheckpointID(sessionID))
			if err != nil || !found || checkpoint.UnresolvedExecution != nil {
				t.Fatalf("checkpoint=%#v found=%v err=%v, want anchor cleared", checkpoint, found, err)
			}
		})
	}
}

func TestExecutionAnchorAndInFlightOutboxSurviveCloseOpenAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const (
				sessionID = "restart-fence-session"
				turnID    = "restart-fence-turn"
				threadID  = "restart-fence-thread"
				codexID   = "restart-fence-codex"
			)
			now := time.Now().UTC()
			anchor := ExecutionAnchor{
				SessionID: sessionID, ThreadID: threadID, OuterTurnID: turnID, CodexTurnID: codexID,
				SourcePath: "/tmp/restart-fence.jsonl", SourceFingerprint: "restart-fence-source",
				CutoffRecordID: "restart-cutoff", CutoffLine: 3, CutoffOffset: 96,
				State: "unresolved", Generation: 11, UpdatedAt: now,
			}
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusInterrupted, CodexThreadID: threadID, CodexTurnID: codexID, RecoveryReason: "ambiguous Codex execution: restart", InterruptedAt: now}
				checkpointID := sessionTranscriptCheckpointID(sessionID)
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{ID: checkpointID, SessionID: sessionID, ExecutionAnchorGeneration: anchor.Generation, UnresolvedExecution: &anchor}
				state.OutboxMessages["restart-sending"] = OutboxMessage{ID: "restart-sending", SessionID: sessionID, TurnID: turnID, TeamsChatID: "restart-chat", Kind: "final", NotificationKind: "turn_completed", Status: OutboxStatusSending, SendAttemptToken: "restart-attempt", CreatedAt: now, UpdatedAt: now}
				state.OutboxMessages["restart-accepted"] = OutboxMessage{ID: "restart-accepted", SessionID: sessionID, TurnID: turnID, TeamsChatID: "restart-chat", Kind: "final-002", NotificationKind: "turn_completed", Status: OutboxStatusAccepted, SendAttemptToken: "restart-accepted-attempt", TeamsMessageID: "teams-restart-stable", BlockedByTerminalFailure: true, CreatedAt: now, UpdatedAt: now}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			path := store.Path()
			if err := store.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			reopened, err := Open(path)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopened.Close()
			checkpoint, found, err := reopened.ImportCheckpoint(ctx, sessionTranscriptCheckpointID(sessionID))
			if err != nil || !found || checkpoint.UnresolvedExecution == nil || checkpoint.UnresolvedExecution.Generation != anchor.Generation {
				t.Fatalf("reopened checkpoint=%#v found=%v err=%v", checkpoint, found, err)
			}
			sending, err := reopened.OutboxMessageByID(ctx, "restart-sending")
			if err != nil || sending.Status != OutboxStatusSending || sending.SendAttemptToken != "restart-attempt" {
				t.Fatalf("reopened sending=%#v err=%v", sending, err)
			}
			accepted, err := reopened.OutboxMessageByID(ctx, "restart-accepted")
			if err != nil || accepted.Status != OutboxStatusAccepted || accepted.TeamsMessageID != "teams-restart-stable" || !accepted.BlockedByTerminalFailure {
				t.Fatalf("reopened accepted=%#v err=%v", accepted, err)
			}
			if _, err := reopened.MarkOutboxSendAttempt(ctx, "restart-sending"); !errors.Is(err, ErrOutboxSendNotClaimed) {
				t.Fatalf("reopened sending reclaim error=%v, want ErrOutboxSendNotClaimed", err)
			}
			if _, err := reopened.MarkOutboxSendAttempt(ctx, "restart-accepted"); !errors.Is(err, ErrOutboxSendNotClaimed) {
				t.Fatalf("reopened accepted reclaim error=%v, want ErrOutboxSendNotClaimed", err)
			}
		})
	}
}

func TestMarkTurnFailedWithCodexIDsFencesInFlightFinalAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now()
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["ordinary-failure-session"] = SessionContext{ID: "ordinary-failure-session", Status: SessionStatusActive}
				state.Turns["ordinary-failure-turn"] = Turn{
					ID: "ordinary-failure-turn", SessionID: "ordinary-failure-session", Status: TurnStatusRunning,
					CodexThreadID: "ordinary-failure-thread", CodexTurnID: "ordinary-failure-codex", StartedAt: now,
				}
				state.OutboxMessages["outbox:ordinary-failure"] = OutboxMessage{
					ID: "outbox:ordinary-failure", SessionID: "ordinary-failure-session", TurnID: "ordinary-failure-turn",
					TeamsChatID: "ordinary-failure-chat", Kind: "final", NotificationKind: "turn_completed",
					Status: OutboxStatusSending, SendAttemptToken: "ordinary-failure-attempt", CreatedAt: now, UpdatedAt: now,
				}
				state.OutboxMessages["outbox:ordinary-failure-error"] = OutboxMessage{
					ID: "outbox:ordinary-failure-error", SessionID: "ordinary-failure-session", TurnID: "ordinary-failure-turn",
					TeamsChatID: "ordinary-failure-chat", Kind: "final", NotificationKind: "turn_completed",
					Status: OutboxStatusSending, SendAttemptToken: "ordinary-failure-error-attempt", CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			if _, err := store.MarkTurnFailedWithCodexIDs(ctx, "ordinary-failure-turn", "ordinary failure", "ordinary-failure-thread", "ordinary-failure-codex"); err != nil {
				t.Fatalf("MarkTurnFailedWithCodexIDs: %v", err)
			}
			created, _, err := store.QueueOutbox(ctx, OutboxMessage{
				ID: "outbox:ordinary-failure-late", SessionID: "ordinary-failure-session", TurnID: "ordinary-failure-turn",
				TeamsChatID: "ordinary-failure-chat", Kind: "final", NotificationKind: "turn_completed", Body: "late stale final",
			})
			if err != nil || created.Status != OutboxStatusSkipped || !created.BlockedByTerminalFailure {
				t.Fatalf("late queued final=%#v err=%v, want skipped terminal-failure fence", created, err)
			}
			fenced, err := store.OutboxMessageByID(ctx, "outbox:ordinary-failure")
			if err != nil || fenced.Status != OutboxStatusSending || !fenced.BlockedByTerminalFailure {
				t.Fatalf("ordinary failure final=%#v err=%v, want fenced sending", fenced, err)
			}
			if _, err := store.MarkOutboxSendAttempt(ctx, fenced.ID); !errors.Is(err, ErrOutboxSendNotClaimed) {
				t.Fatalf("fenced retry error=%v, want ErrOutboxSendNotClaimed", err)
			}
			errorOutbox, err := store.MarkOutboxSendErrorForAttempt(ctx, "outbox:ordinary-failure-error", "ordinary-failure-error-attempt", "definitive Graph failure")
			if err != nil || errorOutbox.Status != OutboxStatusSkipped || !errorOutbox.BlockedByTerminalFailure {
				t.Fatalf("ordinary definitive failure outbox=%#v err=%v, want skipped fenced row", errorOutbox, err)
			}
			accepted, err := store.MarkOutboxAcceptedForAttempt(ctx, fenced.ID, fenced.SendAttemptToken, "teams-ordinary-failure")
			if err != nil || accepted.Status != OutboxStatusAccepted || !accepted.BlockedByTerminalFailure {
				t.Fatalf("ordinary late accepted=%#v err=%v, want blocked", accepted, err)
			}
			promoted, err := store.MarkOutboxSentForAttempt(ctx, fenced.ID, fenced.SendAttemptToken, "teams-ordinary-failure")
			if err != nil || promoted.Status != OutboxStatusAccepted || !promoted.BlockedByTerminalFailure {
				t.Fatalf("ordinary late sent=%#v err=%v, want no promotion", promoted, err)
			}
		})
	}
}

func TestTerminalFailureFenceOrderMatrixAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		for _, order := range []string{"failure-before-graph", "graph-before-failure", "graph-fully-sent-before-failure"} {
			t.Run(backend+"/"+order, func(t *testing.T) {
				store := newTestStore(t)
				const (
					sessionID = "terminal-order-session"
					turnID    = "terminal-order-turn"
					outboxID  = "outbox:terminal-order"
					token     = "terminal-order-attempt"
				)
				identity := ExecutionFailureIdentity{
					SessionID: sessionID, TurnID: turnID,
					ThreadID: "terminal-order-thread", CodexTurnID: "terminal-order-codex",
				}
				now := time.Now()
				if err := store.Update(ctx, func(state *State) error {
					state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
					state.Turns[turnID] = Turn{
						ID: turnID, SessionID: sessionID, Status: TurnStatusRunning,
						CodexThreadID: identity.ThreadID, CodexTurnID: identity.CodexTurnID, StartedAt: now,
					}
					state.OutboxMessages[outboxID] = OutboxMessage{
						ID: outboxID, SessionID: sessionID, TurnID: turnID,
						TeamsChatID: "terminal-order-chat", Kind: "final", NotificationKind: "turn_completed",
						Status: OutboxStatusSending, SendAttemptToken: token, CreatedAt: now, UpdatedAt: now,
					}
					return nil
				}); err != nil {
					t.Fatalf("seed state: %v", err)
				}
				if backend == "sqlite" {
					if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
						t.Fatalf("MigrateLargeStateToSQLite: %v", err)
					}
				}

				markFailure := func() {
					t.Helper()
					if _, err := store.MarkTurnFailedForExecution(ctx, identity, "terminal order failure"); err != nil {
						t.Fatalf("MarkTurnFailedForExecution: %v", err)
					}
				}
				markAccepted := func() OutboxMessage {
					t.Helper()
					accepted, err := store.MarkOutboxAcceptedForAttempt(ctx, outboxID, token, "teams-terminal-order")
					if err != nil {
						t.Fatalf("MarkOutboxAcceptedForAttempt: %v", err)
					}
					return accepted
				}
				markSent := func() OutboxMessage {
					t.Helper()
					sent, err := store.MarkOutboxSentForAttempt(ctx, outboxID, token, "teams-terminal-order")
					if err != nil {
						t.Fatalf("MarkOutboxSentForAttempt: %v", err)
					}
					return sent
				}

				switch order {
				case "failure-before-graph":
					markFailure()
					accepted := markAccepted()
					if accepted.Status != OutboxStatusAccepted || !accepted.BlockedByTerminalFailure {
						t.Fatalf("late accepted row = %#v, want Accepted+terminal fence", accepted)
					}
					sent := markSent()
					if sent.Status != OutboxStatusAccepted || !sent.BlockedByTerminalFailure {
						t.Fatalf("late sent row = %#v, want Accepted+terminal fence", sent)
					}
				case "graph-before-failure":
					accepted := markAccepted()
					if accepted.Status != OutboxStatusAccepted || accepted.BlockedByTerminalFailure {
						t.Fatalf("pre-failure accepted row = %#v, want ordinary Accepted", accepted)
					}
					markFailure()
					sent := markSent()
					if sent.Status != OutboxStatusAccepted || !sent.BlockedByTerminalFailure {
						t.Fatalf("failure-after-Graph row = %#v, want Accepted+terminal fence", sent)
					}
				case "graph-fully-sent-before-failure":
					accepted := markAccepted()
					if accepted.Status != OutboxStatusAccepted || accepted.BlockedByTerminalFailure {
						t.Fatalf("pre-failure accepted row = %#v, want ordinary Accepted", accepted)
					}
					sent := markSent()
					if sent.Status != OutboxStatusSent || sent.BlockedByTerminalFailure {
						t.Fatalf("pre-failure sent row = %#v, want Sent", sent)
					}
					markFailure()
					settled, err := store.OutboxMessageByID(ctx, outboxID)
					if err != nil || settled.Status != OutboxStatusSent || settled.BlockedByTerminalFailure {
						t.Fatalf("already-sent row after failure = %#v err=%v, want unchanged Sent", settled, err)
					}
				}
				failed, found, err := store.TurnByID(ctx, turnID)
				if err != nil || !found || failed.Status != TurnStatusFailed {
					t.Fatalf("turn after %s = %#v found=%v err=%v, want Failed", order, failed, found, err)
				}
			})
		}
	}
}

func TestMarkTurnFailedForExecutionRejectsIncompleteOwnedIdentityAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const (
				sessionID = "owned-identity-session"
				turnID    = "owned-identity-turn"
			)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.Turns[turnID] = Turn{
					ID: turnID, SessionID: sessionID, Status: TurnStatusRunning,
					CodexThreadID: "owned-identity-thread", CodexTurnID: "owned-identity-codex",
				}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			for name, identity := range map[string]ExecutionFailureIdentity{
				"missing-thread":     {SessionID: sessionID, TurnID: turnID, CodexTurnID: "owned-identity-codex"},
				"missing-codex-turn": {SessionID: sessionID, TurnID: turnID, ThreadID: "owned-identity-thread"},
			} {
				t.Run(name, func(t *testing.T) {
					if _, err := store.MarkTurnFailedForExecution(ctx, identity, "incomplete callback"); !errors.Is(err, ErrStaleExecutionCallback) {
						t.Fatalf("incomplete identity error = %v, want ErrStaleExecutionCallback", err)
					}
					turn, found, err := store.TurnByID(ctx, turnID)
					if err != nil || !found || turn.Status != TurnStatusRunning {
						t.Fatalf("incomplete identity changed turn = %#v found=%v err=%v", turn, found, err)
					}
				})
			}
		})
	}
}

func TestMarkTurnFailedForExecutionPreservesThreadOnlyLegacyCompatibilityAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const (
				sessionID = "thread-only-legacy-session"
				turnID    = "thread-only-legacy-turn"
				threadID  = "thread-only-legacy-thread"
			)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				// Older app-server runners persisted the thread owner but did not
				// provide a Codex turn ID on failure.  Keep this narrow compatibility
				// path when no unresolved anchor exists; an active anchor still
				// requires the complete typed proof.
				state.Turns[turnID] = Turn{
					ID: turnID, SessionID: sessionID, Status: TurnStatusRunning,
					CodexThreadID: threadID,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			failed, err := store.MarkTurnFailedForExecution(ctx, ExecutionFailureIdentity{
				SessionID: sessionID, TurnID: turnID, ThreadID: threadID,
			}, "legacy thread-only failure")
			if err != nil || failed.Status != TurnStatusFailed || failed.CodexThreadID != threadID || failed.CodexTurnID != "" {
				t.Fatalf("thread-only legacy failure = %#v err=%v, want Failed with thread owner", failed, err)
			}
		})
	}
}

func BenchmarkSessionExecutionStateSnapshotLargeState(b *testing.B) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		b.Run(backend, func(b *testing.B) {
			store := newBenchmarkStore(b)
			defer store.Close()
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["benchmark-session"] = SessionContext{ID: "benchmark-session", Status: SessionStatusActive, CodexThreadID: "benchmark-thread"}
				state.ImportCheckpoints["transcript:benchmark-session"] = ImportCheckpoint{ID: "transcript:benchmark-session", SessionID: "benchmark-session", Status: "complete"}
				for i := 0; i < 1500; i++ {
					id := "benchmark-turn-" + time.Unix(int64(i), 0).Format("150405")
					state.Turns[id] = Turn{ID: id, SessionID: "benchmark-session", Status: TurnStatusCompleted, CodexThreadID: "benchmark-thread", CompletedAt: time.Now()}
					state.OutboxMessages["benchmark-outbox-"+id] = OutboxMessage{
						ID: "benchmark-outbox-" + id, SessionID: "benchmark-session", TurnID: id,
						TeamsChatID: "benchmark-chat", Kind: "final", NotificationKind: "turn_completed", Status: OutboxStatusSent,
						Body: strings.Repeat("large unrelated outbox payload ", 64), CreatedAt: time.Now(), UpdatedAt: time.Now(),
					}
				}
				return nil
			}); err != nil {
				b.Fatalf("seed benchmark state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					b.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				state, err := store.SessionExecutionStateSnapshot(ctx, "benchmark-session", "transcript:benchmark-session")
				if err != nil {
					b.Fatalf("SessionExecutionStateSnapshot: %v", err)
				}
				if len(state.Turns) != 1500 {
					b.Fatalf("snapshot turns = %d, want 1500", len(state.Turns))
				}
			}
		})
	}
}

func BenchmarkSessionExecutionOwnershipProbesCached(b *testing.B) {
	ctx := context.Background()
	store := newBenchmarkStore(b)
	defer store.Close()
	const sessionCount = 40
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < sessionCount; i++ {
			sessionID := fmt.Sprintf("probe-session-%03d", i)
			state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
			state.ImportCheckpoints[sessionTranscriptCheckpointID(sessionID)] = ImportCheckpoint{ID: sessionTranscriptCheckpointID(sessionID), SessionID: sessionID, Status: "complete"}
			state.Turns[fmt.Sprintf("probe-turn-%03d", i)] = Turn{ID: fmt.Sprintf("probe-turn-%03d", i), SessionID: sessionID, Status: TurnStatusCompleted}
		}
		return nil
	}); err != nil {
		b.Fatalf("seed probe state: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		b.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	ids := make([]string, 0, sessionCount)
	for i := 0; i < sessionCount; i++ {
		ids = append(ids, fmt.Sprintf("probe-session-%03d", i))
	}
	if _, err := store.SessionExecutionOwnershipProbes(ctx, ids); err != nil {
		b.Fatalf("warm ownership probes: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.SessionExecutionOwnershipProbes(ctx, ids); err != nil {
			b.Fatalf("ownership probes: %v", err)
		}
	}
}

func BenchmarkPersistInterruptedTurnWithAnchor(b *testing.B) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		b.Run(backend, func(b *testing.B) {
			store := newBenchmarkStore(b)
			defer store.Close()
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["benchmark-recovery-session"] = SessionContext{ID: "benchmark-recovery-session", Status: SessionStatusActive}
				return nil
			}); err != nil {
				b.Fatalf("seed recovery benchmark session: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					b.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				turnID := "benchmark-recovery-turn"
				b.StopTimer()
				if err := store.Update(ctx, func(state *State) error {
					now := time.Now()
					state.Turns[turnID] = Turn{ID: turnID, SessionID: "benchmark-recovery-session", Status: TurnStatusRunning, CodexThreadID: "benchmark-recovery-thread", CodexTurnID: turnID, StartedAt: now}
					state.ImportCheckpoints["transcript:benchmark-recovery-session"] = ImportCheckpoint{ID: "transcript:benchmark-recovery-session", SessionID: "benchmark-recovery-session", LastRecordID: "trusted", LastOffset: 128, UpdatedAt: now}
					return nil
				}); err != nil {
					b.Fatalf("seed recovery iteration %d: %v", i, err)
				}
				b.StartTimer()
				_, err := store.PersistInterruptedTurnWithAnchor(ctx, PersistInterruptedTurnWithAnchorRequest{
					SessionID: "benchmark-recovery-session", TurnID: turnID, CheckpointID: "transcript:benchmark-recovery-session",
					CodexThreadID: "benchmark-recovery-thread", CodexTurnID: turnID, RecoveryReason: "ambiguous Codex execution: benchmark",
				})
				if err != nil {
					b.Fatalf("persist recovery iteration %d: %v", i, err)
				}
			}
		})
	}
}

func TestMarkTurnInterruptedCannotOverwriteTerminalOwner(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seed := func(turnID string, status TurnStatus) error {
		return store.Update(ctx, func(state *State) error {
			now := time.Now()
			state.Sessions["terminal-owner-session"] = SessionContext{ID: "terminal-owner-session", Status: SessionStatusActive}
			turn := Turn{ID: turnID, SessionID: "terminal-owner-session", Status: status, CodexThreadID: "thread-owner", CodexTurnID: "codex-owner", UpdatedAt: now}
			if status == TurnStatusCompleted {
				turn.CompletedAt = now
			} else {
				turn.FailedAt = now
			}
			state.Turns[turnID] = turn
			return nil
		})
	}
	for _, status := range []TurnStatus{TurnStatusCompleted, TurnStatusFailed} {
		turnID := "terminal-owner-" + string(status) + "-json"
		if err := seed(turnID, status); err != nil {
			t.Fatalf("seed %s: %v", status, err)
		}
		got, err := store.MarkTurnInterrupted(ctx, turnID, "stale executor callback")
		if err != nil {
			t.Fatalf("JSON MarkTurnInterrupted(%s): %v", status, err)
		}
		if got.Status != status || got.RecoveryReason != "" {
			t.Fatalf("JSON stale interruption changed %s turn: %#v", status, got)
		}
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	for _, status := range []TurnStatus{TurnStatusCompleted, TurnStatusFailed} {
		turnID := "terminal-owner-" + string(status) + "-sqlite"
		if err := seed(turnID, status); err != nil {
			t.Fatalf("seed SQLite %s: %v", status, err)
		}
		got, err := store.MarkTurnInterrupted(ctx, turnID, "stale executor callback")
		if err != nil {
			t.Fatalf("SQLite MarkTurnInterrupted(%s): %v", status, err)
		}
		if got.Status != status || got.RecoveryReason != "" {
			t.Fatalf("SQLite stale interruption changed %s turn: %#v", status, got)
		}
	}
}

func TestPersistInterruptedTurnWithAnchorDoesNotFenceTerminalOwner(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Date(2026, 8, 9, 15, 0, 0, 0, time.UTC)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["atomic-recovery-session"] = SessionContext{ID: "atomic-recovery-session", Status: SessionStatusActive, CodexThreadID: "thread-atomic-recovery"}
				state.Turns["atomic-recovery-turn"] = Turn{
					ID: "atomic-recovery-turn", SessionID: "atomic-recovery-session", Status: TurnStatusRunning,
					CodexThreadID: "thread-atomic-recovery", CodexTurnID: "codex-atomic-recovery", StartedAt: now,
				}
				state.ImportCheckpoints["transcript:atomic-recovery-session"] = ImportCheckpoint{
					ID: "transcript:atomic-recovery-session", SessionID: "atomic-recovery-session", SourcePath: "/tmp/atomic-recovery.jsonl",
					LastRecordID: "trusted", LastSourceLine: 8, LastOffset: 256, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			if _, err := store.MarkTurnCompleted(ctx, "atomic-recovery-turn", "thread-atomic-recovery", "codex-atomic-recovery"); err != nil {
				t.Fatalf("commit terminal owner: %v", err)
			}
			result, err := store.PersistInterruptedTurnWithAnchor(ctx, PersistInterruptedTurnWithAnchorRequest{
				SessionID: "atomic-recovery-session", TurnID: "atomic-recovery-turn", CheckpointID: "transcript:atomic-recovery-session",
				CodexThreadID: "thread-atomic-recovery", CodexTurnID: "codex-atomic-recovery", RecoveryReason: "ambiguous Codex execution: stale callback",
				Anchor: ExecutionAnchor{SourcePath: "/tmp/atomic-recovery.jsonl", State: "unresolved"},
			})
			if err != nil {
				t.Fatalf("stale recovery transition: %v", err)
			}
			if !result.Terminal || result.Changed {
				t.Fatalf("stale recovery result = %#v, want terminal no-op", result)
			}
			turn, found, err := store.TurnByID(ctx, "atomic-recovery-turn")
			if err != nil || !found || turn.Status != TurnStatusCompleted {
				t.Fatalf("terminal turn = %#v found=%v err=%v", turn, found, err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, "transcript:atomic-recovery-session")
			if err != nil || !found {
				t.Fatalf("load checkpoint: found=%v err=%v", found, err)
			}
			if checkpoint.UnresolvedExecution != nil {
				t.Fatalf("stale recovery created anchor: %#v", checkpoint.UnresolvedExecution)
			}
		})
	}
}

func TestPersistInterruptedTurnWithAnchorRejectsStaleIdentityAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["stale-recovery-session"] = SessionContext{ID: "stale-recovery-session", Status: SessionStatusActive}
				state.Turns["stale-recovery-turn"] = Turn{ID: "stale-recovery-turn", SessionID: "stale-recovery-session", Status: TurnStatusRunning, CodexThreadID: "thread-current", CodexTurnID: "codex-current"}
				state.ImportCheckpoints["transcript:stale-recovery-session"] = ImportCheckpoint{ID: "transcript:stale-recovery-session", SessionID: "stale-recovery-session"}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			_, err := store.PersistInterruptedTurnWithAnchor(ctx, PersistInterruptedTurnWithAnchorRequest{
				SessionID: "stale-recovery-session", TurnID: "stale-recovery-turn", CheckpointID: "transcript:stale-recovery-session",
				CodexThreadID: "thread-old", CodexTurnID: "codex-old", RecoveryReason: "ambiguous Codex execution: stale callback",
			})
			if !errors.Is(err, ErrStaleExecutionCallback) {
				t.Fatalf("stale identity error = %v, want ErrStaleExecutionCallback", err)
			}
			turn, found, err := store.TurnByID(ctx, "stale-recovery-turn")
			if err != nil || !found || turn.Status != TurnStatusRunning {
				t.Fatalf("stale identity changed turn = %#v found=%v err=%v", turn, found, err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, "transcript:stale-recovery-session")
			if err != nil || !found || checkpoint.UnresolvedExecution != nil {
				t.Fatalf("stale identity changed checkpoint = %#v found=%v err=%v", checkpoint, found, err)
			}
		})
	}
}

func TestPersistInterruptedTurnWithAnchorRejectsIdentityDifferentFromExistingAnchor(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			sessionID := "anchor-merge-session"
			turnID := "anchor-merge-turn"
			checkpointID := "transcript:" + sessionID
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusRunning}
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
					ID: checkpointID, SessionID: sessionID,
					UnresolvedExecution:       &ExecutionAnchor{SessionID: sessionID, OuterTurnID: turnID, ThreadID: "thread-owner", CodexTurnID: "codex-owner", State: "unresolved", Generation: 4},
					ExecutionAnchorGeneration: 4,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			_, err := store.PersistInterruptedTurnWithAnchor(ctx, PersistInterruptedTurnWithAnchorRequest{
				SessionID: sessionID, TurnID: turnID, CheckpointID: checkpointID,
				CodexThreadID: "thread-stale", CodexTurnID: "codex-stale", RecoveryReason: "stale anchor merge",
			})
			if !errors.Is(err, ErrStaleExecutionCallback) {
				t.Fatalf("identity mismatch error = %v, want ErrStaleExecutionCallback", err)
			}
			turn, found, err := store.TurnByID(ctx, turnID)
			if err != nil || !found || turn.Status != TurnStatusRunning || turn.CodexThreadID != "" || turn.CodexTurnID != "" {
				t.Fatalf("turn changed after stale anchor merge: %#v found=%v err=%v", turn, found, err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
			if err != nil || !found || checkpoint.UnresolvedExecution == nil || checkpoint.UnresolvedExecution.ThreadID != "thread-owner" || checkpoint.UnresolvedExecution.CodexTurnID != "codex-owner" {
				t.Fatalf("anchor changed after stale merge: %#v found=%v err=%v", checkpoint, found, err)
			}
		})
	}
}

func TestPersistInterruptedTurnWithAnchorAndCompletionNeverLeaveCompletedAnchor(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["race-session"] = SessionContext{ID: "race-session", Status: SessionStatusActive}
				return nil
			}); err != nil {
				t.Fatalf("seed session: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			for i := 0; i < 12; i++ {
				turnID := fmt.Sprintf("race-turn-%d", i)
				checkpointID := "transcript:race-session"
				if err := store.Update(ctx, func(state *State) error {
					now := time.Now()
					state.Turns[turnID] = Turn{ID: turnID, SessionID: "race-session", Status: TurnStatusRunning, CodexThreadID: "race-thread", CodexTurnID: turnID, StartedAt: now}
					state.ImportCheckpoints[checkpointID] = ImportCheckpoint{ID: checkpointID, SessionID: "race-session", LastRecordID: "trusted", LastOffset: 64, UpdatedAt: now}
					return nil
				}); err != nil {
					t.Fatalf("seed iteration %d: %v", i, err)
				}
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					_, _ = store.MarkTurnCompleted(ctx, turnID, "race-thread", turnID)
				}()
				go func() {
					defer wg.Done()
					_, _ = store.PersistInterruptedTurnWithAnchor(ctx, PersistInterruptedTurnWithAnchorRequest{
						SessionID: "race-session", TurnID: turnID, CheckpointID: checkpointID,
						CodexThreadID: "race-thread", CodexTurnID: turnID, RecoveryReason: "ambiguous Codex execution: race",
					})
				}()
				wg.Wait()
				turn, found, err := store.TurnByID(ctx, turnID)
				if err != nil || !found {
					t.Fatalf("load race turn %d: found=%v err=%v", i, found, err)
				}
				checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
				if err != nil || !found {
					t.Fatalf("load race checkpoint %d: found=%v err=%v", i, found, err)
				}
				if turn.Status == TurnStatusCompleted && checkpoint.UnresolvedExecution != nil {
					t.Fatalf("iteration %d left Completed turn with active anchor: turn=%#v checkpoint=%#v", i, turn, checkpoint)
				}
			}
		})
	}
}

func TestTerminalTurnCallbacksCannotReplaceOppositeTerminalState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seed := func(turnID string, status TurnStatus) error {
		return store.Update(ctx, func(state *State) error {
			now := time.Now()
			state.Sessions["terminal-cas-session"] = SessionContext{ID: "terminal-cas-session", Status: SessionStatusActive}
			turn := Turn{ID: turnID, SessionID: "terminal-cas-session", Status: status, CodexThreadID: "thread-cas", CodexTurnID: "codex-cas", UpdatedAt: now}
			if status == TurnStatusCompleted {
				turn.CompletedAt = now
			} else {
				turn.FailedAt = now
				turn.FailureMessage = "original failure"
			}
			state.Turns[turnID] = turn
			return nil
		})
	}
	assertBackend := func(backend string) {
		t.Helper()
		completedID := "terminal-cas-completed-" + backend
		if err := seed(completedID, TurnStatusCompleted); err != nil {
			t.Fatalf("seed completed %s: %v", backend, err)
		}
		got, err := store.MarkTurnFailed(ctx, completedID, "stale failure")
		if err != nil || got.Status != TurnStatusCompleted || got.FailureMessage != "" {
			t.Fatalf("%s stale failure changed completed turn: got=%#v err=%v", backend, got, err)
		}
		failedID := "terminal-cas-failed-" + backend
		if err := seed(failedID, TurnStatusFailed); err != nil {
			t.Fatalf("seed failed %s: %v", backend, err)
		}
		got, err = store.MarkTurnCompleted(ctx, failedID, "thread-stale", "codex-stale")
		if err != nil || got.Status != TurnStatusFailed || got.FailureMessage != "original failure" || got.CodexTurnID != "codex-cas" {
			t.Fatalf("%s stale completion changed failed turn: got=%#v err=%v", backend, got, err)
		}
	}
	assertBackend("json")
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	assertBackend("sqlite")
}

func TestCompleteTurnWithFinalRequiresSourceProofForAnchoredExecutionAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "source-proof-session"
			const turnID = "source-proof-turn"
			const checkpointID = "transcript:" + sessionID
			now := time.Now().UTC()
			sourcePath := filepath.Join(t.TempDir(), "source-proof.jsonl")
			sourceBytes := []byte(strings.Repeat("source-proof-", 16))
			if err := os.WriteFile(sourcePath, sourceBytes, 0o600); err != nil {
				t.Fatalf("write source proof fixture: %v", err)
			}
			prefixProof, err := sourceCheckpointFingerprintAtOffset(sourcePath, 64)
			if err != nil {
				t.Fatalf("source prefix proof: %v", err)
			}
			afterProof, err := sourceCheckpointFingerprintAtOffset(sourcePath, 96)
			if err != nil {
				t.Fatalf("source progress proof: %v", err)
			}
			anchor := ExecutionAnchor{SessionID: sessionID, ThreadID: "source-proof-thread", OuterTurnID: turnID, CodexTurnID: "source-proof-codex", SourcePath: sourcePath, SourceFingerprint: prefixProof, CutoffRecordID: "before", CutoffLine: 4, CutoffOffset: 64, State: "unresolved", Generation: 3}
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, TeamsChatID: "source-proof-chat"}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusRunning, CodexThreadID: anchor.ThreadID, CodexTurnID: anchor.CodexTurnID, StartedAt: now}
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{ID: checkpointID, SessionID: sessionID, ExecutionAnchorGeneration: anchor.Generation, UnresolvedExecution: &anchor}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			final := OutboxMessage{ID: "outbox:" + turnID + ":final", SessionID: sessionID, TurnID: turnID, TeamsChatID: "source-proof-chat", Kind: "final", NotificationKind: "turn_completed", Body: "source proof final", PartIndex: 1, PartCount: 1}
			_, err = store.CompleteTurnWithFinal(ctx, CompleteTurnWithFinalRequest{SessionID: sessionID, TurnID: turnID, CodexThreadID: anchor.ThreadID, CodexTurnID: anchor.CodexTurnID, AnchorGeneration: anchor.Generation, Progress: TranscriptCheckpointProgress{ID: checkpointID, SessionID: sessionID, LastRecordID: "after"}, FinalOutbox: []OutboxMessage{final}})
			if !errors.Is(err, ErrUnresolvedExecution) {
				t.Fatalf("missing source proof error=%v, want ErrUnresolvedExecution", err)
			}
			turn, _, loadErr := store.TurnByID(ctx, turnID)
			if loadErr != nil || turn.Status != TurnStatusRunning {
				t.Fatalf("missing source proof changed turn=%#v err=%v", turn, loadErr)
			}
			_, err = store.CompleteTurnWithFinal(ctx, CompleteTurnWithFinalRequest{SessionID: sessionID, TurnID: turnID, CodexThreadID: anchor.ThreadID, CodexTurnID: anchor.CodexTurnID, AnchorGeneration: anchor.Generation, Progress: TranscriptCheckpointProgress{ID: checkpointID, SessionID: sessionID, SourcePath: anchor.SourcePath, SourceFingerprint: afterProof, AnchorSourceFingerprint: "wrong-prefix-proof", LastRecordID: "after", LastOffset: 96, LastOffsetKnown: true}, FinalOutbox: []OutboxMessage{final}})
			if !errors.Is(err, ErrUnresolvedExecution) {
				t.Fatalf("mismatched source proof error=%v, want ErrUnresolvedExecution", err)
			}
			turnAfterMismatch, _, loadErr := store.TurnByID(ctx, turnID)
			if loadErr != nil || turnAfterMismatch.Status != TurnStatusRunning {
				t.Fatalf("mismatched source proof changed turn=%#v err=%v", turnAfterMismatch, loadErr)
			}
			rewrittenBytes := []byte(strings.Repeat("rewrite-abcde", 16))
			if len(rewrittenBytes) != len(sourceBytes) {
				t.Fatalf("rewrite fixture length changed: %d != %d", len(rewrittenBytes), len(sourceBytes))
			}
			if err := os.WriteFile(sourcePath, rewrittenBytes, 0o600); err != nil {
				t.Fatalf("rewrite source proof fixture: %v", err)
			}
			_, err = store.CompleteTurnWithFinal(ctx, CompleteTurnWithFinalRequest{SessionID: sessionID, TurnID: turnID, CodexThreadID: anchor.ThreadID, CodexTurnID: anchor.CodexTurnID, AnchorGeneration: anchor.Generation, Progress: TranscriptCheckpointProgress{ID: checkpointID, SessionID: sessionID, SourcePath: anchor.SourcePath, SourceFingerprint: afterProof, AnchorSourceFingerprint: anchor.SourceFingerprint, LastRecordID: "after", LastOffset: 96, LastOffsetKnown: true}, FinalOutbox: []OutboxMessage{final}})
			if !errors.Is(err, ErrUnresolvedExecution) {
				t.Fatalf("rewritten source proof error=%v, want ErrUnresolvedExecution", err)
			}
			if err := os.WriteFile(sourcePath, sourceBytes, 0o600); err != nil {
				t.Fatalf("restore source proof fixture: %v", err)
			}
			completed, err := store.CompleteTurnWithFinal(ctx, CompleteTurnWithFinalRequest{SessionID: sessionID, TurnID: turnID, CodexThreadID: anchor.ThreadID, CodexTurnID: anchor.CodexTurnID, AnchorGeneration: anchor.Generation, Progress: TranscriptCheckpointProgress{ID: checkpointID, SessionID: sessionID, SourcePath: anchor.SourcePath, SourceFingerprint: afterProof, AnchorSourceFingerprint: anchor.SourceFingerprint, LastRecordID: "after", LastOffset: 96, LastOffsetKnown: true}, FinalOutbox: []OutboxMessage{final}})
			if err != nil || completed.Status != TurnStatusCompleted {
				t.Fatalf("exact source proof completion=%#v err=%v", completed, err)
			}
		})
	}
}

func TestLegacyExecutionAnchorGenerationMigratesOnExactCallbackAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "legacy-generation-session"
			const turnID = "legacy-generation-turn"
			const checkpointID = "transcript:" + sessionID
			now := time.Now().UTC()
			anchor := ExecutionAnchor{SessionID: sessionID, ThreadID: "legacy-generation-thread", OuterTurnID: turnID, CodexTurnID: "legacy-generation-codex", State: "unresolved"}
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusInterrupted, CodexThreadID: anchor.ThreadID, CodexTurnID: anchor.CodexTurnID, RecoveryReason: "ambiguous Codex execution: legacy generation", InterruptedAt: now}
				state.ImportCheckpoints[checkpointID] = ImportCheckpoint{ID: checkpointID, SessionID: sessionID, ExecutionAnchorGeneration: 5, UnresolvedExecution: &anchor}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			failed, err := store.MarkTurnFailedForExecution(ctx, ExecutionFailureIdentity{SessionID: sessionID, TurnID: turnID, ThreadID: anchor.ThreadID, CodexTurnID: anchor.CodexTurnID}, "legacy callback failure")
			if err != nil || failed.Status != TurnStatusFailed {
				t.Fatalf("legacy exact failure=%#v err=%v", failed, err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
			if err != nil || !found || checkpoint.UnresolvedExecution != nil || checkpoint.ExecutionAnchorGeneration != 6 {
				t.Fatalf("legacy generation migration checkpoint=%#v found=%v err=%v", checkpoint, found, err)
			}
		})
	}
}

func TestTerminalFailureFencesLegacyTerminalOutboxKindsAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "legacy-terminal-kind-session"
			const turnID = "legacy-terminal-kind-turn"
			kinds := []struct{ id, kind, notification string }{
				{"answer", "answer", ""},
				{"codex-final", "codex-final", ""},
				{"final-answer", "final-answer", ""},
				{"turn-completed", "status", "turn_completed"},
			}
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusFailed, CodexThreadID: "legacy-terminal-thread", CodexTurnID: "legacy-terminal-codex", FailedAt: time.Now()}
				for i, item := range kinds {
					status := OutboxStatusSending
					if i%2 == 1 {
						status = OutboxStatusAccepted
					}
					state.OutboxMessages["outbox:legacy-terminal:"+item.id] = OutboxMessage{ID: "outbox:legacy-terminal:" + item.id, SessionID: sessionID, TurnID: turnID, TeamsChatID: "legacy-terminal-chat", Kind: item.kind, NotificationKind: item.notification, Status: status, TeamsMessageID: "teams:" + item.id}
				}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if backend == "sqlite" {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			if _, err := store.MarkTurnFailedWithCodexIDs(ctx, turnID, "duplicate failure", "legacy-terminal-thread", "legacy-terminal-codex"); err != nil {
				t.Fatalf("duplicate failure: %v", err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			for _, item := range kinds {
				msg := state.OutboxMessages["outbox:legacy-terminal:"+item.id]
				if !msg.BlockedByTerminalFailure {
					t.Fatalf("legacy terminal kind %q was not fenced: %#v", item.kind, msg)
				}
			}
		})
	}
}
