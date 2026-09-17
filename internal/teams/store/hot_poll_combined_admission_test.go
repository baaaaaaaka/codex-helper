package store

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestSQLiteHotPollScheduleAndWorkCandidatesMatchesSeparateAdmission(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	const (
		controlChatID = "control-chat"
		workChatID    = "work-chat-combined-admission"
		sessionID     = "session-combined-admission"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[sessionID] = SessionContext{
			ID: sessionID, Status: SessionStatusActive, TeamsChatID: workChatID,
			UpdatedAt: now,
		}
		state.ChatPolls[controlChatID] = ChatPollState{
			ChatID: controlChatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(time.Hour), UpdatedAt: now,
		}
		state.ChatPolls[workChatID] = ChatPollState{
			ChatID: workChatID, Seeded: true, PollState: chatPollStateHot,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now,
			PollRevision: 9, ScheduleRevision: 11, FrontierEpoch: 3, UpdatedAt: now,
			PendingPage: &ChatPollPendingPage{
				ReceiptID: "receipt-combined-admission", ChatID: workChatID,
				RequestPath: "/chats/" + workChatID + "/messages?$top=1", Frontier: "head",
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed combined admission fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	var combinedLocks atomic.Int32
	store.SetTimingObserver(func(event StoreTimingEvent) {
		if event.Stage == "state-lock-hold" {
			combinedLocks.Add(1)
		}
	})
	combinedState, combinedCandidates, handled, err := store.HotPollScheduleAndWorkCandidatesExcludingIdleAt(ctx, controlChatID, time.Time{}, now)
	store.SetTimingObserver(nil)
	if err != nil || !handled {
		t.Fatalf("combined admission handled=%v err=%v", handled, err)
	}
	if got := combinedLocks.Load(); got != 1 {
		t.Fatalf("combined admission state-lock holds=%d, want exactly one", got)
	}

	separateState, err := store.HotPollReadyScheduleState(ctx, controlChatID, now)
	if err != nil {
		t.Fatalf("separate schedule admission: %v", err)
	}
	separateCandidates, separateHandled, err := store.HotPollWorkCandidatesExcludingIdleAt(ctx, controlChatID, time.Time{}, now)
	if err != nil || !separateHandled {
		t.Fatalf("separate candidate admission handled=%v err=%v", separateHandled, err)
	}
	if len(combinedCandidates) != len(separateCandidates) || len(combinedCandidates) != 1 {
		t.Fatalf("combined candidates=%#v separate candidates=%#v", combinedCandidates, separateCandidates)
	}
	if combinedCandidates[0].ID != separateCandidates[0].ID || combinedCandidates[0].TeamsChatID != separateCandidates[0].TeamsChatID {
		t.Fatalf("combined candidate=%#v separate candidate=%#v", combinedCandidates[0], separateCandidates[0])
	}
	combinedPoll, combinedOK := combinedState.ChatPolls[workChatID]
	separatePoll, separateOK := separateState.ChatPolls[workChatID]
	if !combinedOK || !separateOK {
		t.Fatalf("combined/separate schedule omitted work chat: combined=%v separate=%v", combinedOK, separateOK)
	}
	if combinedPoll.PollRevision != separatePoll.PollRevision ||
		combinedPoll.ScheduleRevision != separatePoll.ScheduleRevision ||
		combinedPoll.FrontierEpoch != separatePoll.FrontierEpoch ||
		combinedPoll.PendingPage == nil || separatePoll.PendingPage == nil ||
		combinedPoll.PendingPage.ReceiptID != separatePoll.PendingPage.ReceiptID {
		t.Fatalf("combined poll=%#v separate poll=%#v", combinedPoll, separatePoll)
	}
}

func TestSQLiteHotPollScalarHydrationDoesNotOverwriteControlPoll(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 9, 12, 30, 0, 0, time.UTC)
	store := newTestStore(t)
	const (
		controlChatID = "control-chat-preserve"
		workChatID    = "work-chat-preserve-control"
		sessionID     = "session-preserve-control"
	)
	controlPage := &ChatPollPendingPage{
		ChatID:      controlChatID,
		RequestPath: "/chats/" + controlChatID + "/messages?$top=1",
		ReceiptID:   "control-receipt-must-survive",
		Frontier:    "head",
		PollRole:    "control",
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[sessionID] = SessionContext{
			ID: sessionID, Status: SessionStatusActive, TeamsChatID: workChatID, UpdatedAt: now,
		}
		state.ChatPolls[controlChatID] = ChatPollState{
			ChatID: controlChatID, Seeded: true, PollState: chatPollStateWarm,
			PendingPage: controlPage, NextPollAt: now, UpdatedAt: now,
		}
		state.ChatPolls[workChatID] = ChatPollState{
			ChatID: workChatID, Seeded: true, PollState: chatPollStateHot,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed control-preservation fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	state, candidates, handled, err := store.HotPollScheduleAndWorkCandidatesOptimizedExcludingIdleAt(ctx, controlChatID, time.Time{}, now)
	if err != nil || !handled {
		t.Fatalf("optimized admission handled=%v err=%v", handled, err)
	}
	if len(candidates) != 1 || candidates[0].ID != sessionID {
		t.Fatalf("optimized candidates=%#v, want %q", candidates, sessionID)
	}
	poll, ok := state.ChatPolls[controlChatID]
	if !ok || poll.PendingPage == nil || poll.PendingPage.ReceiptID != controlPage.ReceiptID || poll.PendingPage.PollRole != "control" {
		t.Fatalf("control poll was overwritten by scalar work hint: ok=%v poll=%#v", ok, poll)
	}
}

func TestSQLiteHotPollOptimizedAdmissionUsesScalarTurnHintsBeforeSelectedHydration(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	const (
		controlChatID = "control-chat"
		workChatID    = "work-chat-turn-hint"
		sessionID     = "session-turn-hint"
		turnID        = "turn-turn-hint"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[sessionID] = SessionContext{
			ID: sessionID, Status: SessionStatusActive, TeamsChatID: workChatID,
			CodexThreadID: "canonical-thread-must-not-be-preselected", UpdatedAt: now,
		}
		state.ChatPolls[controlChatID] = ChatPollState{
			ChatID: controlChatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(time.Hour), UpdatedAt: now,
		}
		state.ChatPolls[workChatID] = ChatPollState{
			ChatID: workChatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		state.Turns[turnID] = Turn{
			ID: turnID, SessionID: sessionID, Status: TurnStatusRunning,
			CodexThreadID: "turn-canonical-thread", CodexTurnID: "turn-canonical-id",
			CreatedAt: now, UpdatedAt: now,
		}
		state.ImportCheckpoints["checkpoint-turn-hint"] = ImportCheckpoint{
			ID: "checkpoint-turn-hint", SessionID: sessionID,
			Status: importCheckpointStatusImporting, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed scalar turn hint fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	state, candidates, handled, err := store.HotPollScheduleAndWorkCandidatesOptimizedExcludingIdleAt(
		ctx, controlChatID, time.Time{}, now)
	if err != nil || !handled {
		t.Fatalf("optimized admission handled=%v err=%v", handled, err)
	}
	if len(candidates) != 1 || candidates[0].ID != sessionID {
		t.Fatalf("optimized candidates=%#v, want only %q", candidates, sessionID)
	}
	hint, ok := state.Turns[turnID]
	if !ok {
		t.Fatalf("optimized scalar state omitted active turn hint: %#v", state.Turns)
	}
	if hint.Status != TurnStatusRunning || hint.SessionID != sessionID {
		t.Fatalf("optimized turn hint=%#v, want running turn/session", hint)
	}
	if hint.CodexThreadID != "" || hint.CodexTurnID != "" {
		t.Fatalf("optimized admission hydrated turn JSON before selection: %#v", hint)
	}
	if len(state.ImportCheckpoints) != 0 {
		t.Fatalf("optimized admission hydrated checkpoints before selection: %#v", state.ImportCheckpoints)
	}

	selected, selectedHandled, err := store.HotPollSelectedStateForChatsAndSessions(
		ctx, []string{workChatID}, []string{sessionID})
	if err != nil || !selectedHandled {
		t.Fatalf("selected hydration handled=%v err=%v", selectedHandled, err)
	}
	if got := selected.Turns[turnID].CodexThreadID; got != "turn-canonical-thread" {
		t.Fatalf("selected hydration turn thread=%q, want canonical value", got)
	}
	if _, ok := selected.ImportCheckpoints["checkpoint-turn-hint"]; !ok {
		t.Fatalf("selected hydration omitted importing checkpoint: %#v", selected.ImportCheckpoints)
	}
}
