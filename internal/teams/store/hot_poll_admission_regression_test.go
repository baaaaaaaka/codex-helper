package store

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestSQLiteHotPollAdmissionFiltersDeferredChatsAndPreservesDueFences(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	const excludedPerClass = sqliteHotPollReadyLimit/2 + 1

	if err := store.Update(ctx, func(state *State) error {
		for _, class := range []string{"blocked", "parked", "future"} {
			for i := 0; i < excludedPerClass; i++ {
				chatID := fmt.Sprintf("chat-hot-poll-%s-%02d", class, i)
				sessionID := fmt.Sprintf("session-hot-poll-%s-%02d", class, i)
				updatedAt := now.Add(-time.Duration(1000-i) * time.Second)
				state.Sessions[sessionID] = SessionContext{
					ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: updatedAt,
				}
				poll := ChatPollState{
					ChatID: chatID, Seeded: true, UpdatedAt: updatedAt,
				}
				switch class {
				case "blocked":
					poll.PollState = chatPollStateBlocked
					poll.NextPollAt = now.Add(time.Hour)
					poll.BlockedUntil = now.Add(time.Hour)
					poll.ContinuationPath = "/chats/" + chatID + "/messages?$skiptoken=blocked"
				case "parked":
					poll.PollState = chatPollStateParked
					poll.NextPollAt = now.Add(time.Hour)
					poll.ParkedAt = now.Add(-48 * time.Hour)
					poll.ParkNoticeSentAt = now.Add(-47 * time.Hour)
				case "future":
					poll.PollState = chatPollStateWarm
					poll.NextPollAt = now.Add(time.Hour)
					poll.ContinuationPath = "/chats/" + chatID + "/messages?$skiptoken=future"
				}
				state.ChatPolls[chatID] = poll
			}
		}

		const chatID = "chat-hot-poll-due"
		state.Sessions["session-hot-poll-due"] = SessionContext{
			ID: "session-hot-poll-due", Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now,
		}
		state.ChatPolls[chatID] = ChatPollState{
			ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now,
			PollRevision: 17, ScheduleRevision: 23, FrontierEpoch: 5, UpdatedAt: now,
			PendingPage: &ChatPollPendingPage{
				ReceiptID: "receipt-hot-poll-due", ChatID: chatID,
				RequestPath: "/chats/" + chatID + "/messages?$top=1", Frontier: "head",
			},
			Gap: &ChatPollGap{
				Epoch: 5, Kind: "test-gap", SafeCursor: now.Add(-time.Hour),
				RecoveryCursor: now.Add(-30 * time.Minute),
			},
			Attempt: &ChatPollAttempt{
				ID: "attempt-hot-poll-due", Owner: "owner-a", ProcessIncarnation: "process-a",
				LeaseGeneration: 9, ExpectedPollRevision: 17, ExpectedScheduleRevision: 23,
				ExpectedFrontier: "head:/chats/" + chatID + "/messages?$top=1", ExpectedReceiptID: "receipt-hot-poll-due",
				StartedAt: now.Add(-time.Second), ExpiresAt: now.Add(time.Minute),
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed hot-poll admission fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	candidates, handled, err := store.HotPollWorkCandidatesExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
	if err != nil || !handled {
		t.Fatalf("hot-poll candidates handled=%v err=%v", handled, err)
	}
	if len(candidates) != 1 || candidates[0].ID != "session-hot-poll-due" {
		t.Fatalf("hot-poll candidates = %#v, want only due healthy chat", candidates)
	}

	schedule, err := store.HotPollReadyScheduleState(ctx, "control-chat", now)
	if err != nil {
		t.Fatalf("hot-poll ready schedule: %v", err)
	}
	for _, class := range []string{"blocked", "parked", "future"} {
		for i := 0; i < excludedPerClass; i++ {
			chatID := fmt.Sprintf("chat-hot-poll-%s-%02d", class, i)
			if _, ok := schedule.ChatPolls[chatID]; ok {
				t.Fatalf("deferred %s chat %q consumed ready schedule quota", class, chatID)
			}
		}
	}
	poll, ok := schedule.ChatPolls["chat-hot-poll-due"]
	if !ok {
		t.Fatal("due healthy chat was omitted from the narrow schedule projection")
	}
	if poll.PollRevision != 17 || poll.ScheduleRevision != 23 || poll.FrontierEpoch != 5 {
		t.Fatalf("due poll revisions changed in projection: %#v", poll)
	}
	if poll.PendingPage == nil || poll.PendingPage.ReceiptID != "receipt-hot-poll-due" {
		t.Fatalf("due poll pending page fence was not preserved: %#v", poll.PendingPage)
	}
	if poll.Gap == nil || poll.Gap.Epoch != 5 || poll.Gap.SafeCursor.IsZero() {
		t.Fatalf("due poll gap fence was not preserved: %#v", poll.Gap)
	}
	if poll.Attempt == nil || poll.Attempt.ID != "attempt-hot-poll-due" || poll.Attempt.Owner != "owner-a" || poll.Attempt.ExpectedPollRevision != 17 {
		t.Fatalf("due poll attempt fence was not preserved: %#v", poll.Attempt)
	}
}

func TestSQLiteHotPollCandidatesKeepDueIdlePollRetryVisible(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	old := now.Add(-2 * time.Hour)

	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["session-idle-poll-retry"] = SessionContext{
			ID: "session-idle-poll-retry", Status: SessionStatusActive,
			TeamsChatID: "chat-idle-poll-retry", UpdatedAt: old,
		}
		state.ChatPolls["chat-idle-poll-retry"] = ChatPollState{
			ChatID: "chat-idle-poll-retry", Seeded: true,
			PollState: chatPollStateCold, NextPollAt: now.Add(-time.Minute),
			LastActivityAt: old, LastSuccessfulPollAt: old.Add(-time.Hour),
			FailureCount: 1, LastError: "Graph HTTP 429 Too Many Requests",
			LastErrorAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute),
		}
		state.Sessions["session-idle-poll-clean"] = SessionContext{
			ID: "session-idle-poll-clean", Status: SessionStatusActive,
			TeamsChatID: "chat-idle-poll-clean", UpdatedAt: old,
		}
		state.ChatPolls["chat-idle-poll-clean"] = ChatPollState{
			ChatID: "chat-idle-poll-clean", Seeded: true,
			PollState: chatPollStateCold, NextPollAt: now.Add(-time.Minute),
			LastActivityAt: old, LastSuccessfulPollAt: old.Add(-time.Hour),
			UpdatedAt: now.Add(-time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed idle retry admission fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	assertCandidates := func(name string, load func() ([]SessionContext, error)) {
		t.Helper()
		candidates, err := load()
		if err != nil {
			t.Fatalf("%s idle retry candidates: %v", name, err)
		}
		if len(candidates) != 1 || candidates[0].ID != "session-idle-poll-retry" {
			t.Fatalf("%s idle retry candidates = %#v, want only the due failed chat", name, candidates)
		}
	}
	assertCandidates("optimized", func() ([]SessionContext, error) {
		candidates, handled, err := store.HotPollWorkCandidatesExcludingIdleAt(ctx, "control-chat", now.Add(-time.Hour), now)
		if err == nil && !handled {
			return nil, fmt.Errorf("optimized SQLite admission was not handled")
		}
		return candidates, err
	})
	assertCandidates("legacy", func() ([]SessionContext, error) {
		var candidates []SessionContext
		err := store.withStateLock(ctx, func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("SQLite pointer missing")
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			candidates, err = loadSQLiteHotPollWorkCandidatesLegacy(ctx, db, "control-chat", now.Add(-time.Hour), now, sqliteHotPollReadyLimit)
			return err
		})
		return candidates, err
	})
}

func TestSQLiteHotPollRetryLanePrecedesLargeOrdinaryBacklog(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 9, 13, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	const retryChat = "chat-retry-behind-backlog"
	_, ordinaryLimit := sqliteHotPollLaneLimits(sqliteHotPollReadyLimit)

	if err := store.Update(ctx, func(state *State) error {
		// Make the failed row sort after the ordinary rows. Without a dedicated
		// retry lane it would fall behind the ordinary LIMIT and never reach the
		// bridge when a large fresh backlog is due at the same time.
		state.Sessions["session-retry-behind-backlog"] = SessionContext{
			ID: "session-retry-behind-backlog", Status: SessionStatusActive,
			TeamsChatID: retryChat, UpdatedAt: now,
		}
		state.ChatPolls[retryChat] = ChatPollState{
			ChatID: retryChat, Seeded: true, PollState: chatPollStateCold,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now,
			LastSuccessfulPollAt: now.Add(-time.Hour), FailureCount: 1,
			LastError: "Graph HTTP 429 Too Many Requests", LastErrorAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		for i := 0; i < ordinaryLimit+4; i++ {
			chatID := fmt.Sprintf("chat-ordinary-backlog-%02d", i)
			sessionID := fmt.Sprintf("session-ordinary-backlog-%02d", i)
			updatedAt := now.Add(-time.Duration(100+i) * time.Second)
			state.Sessions[sessionID] = SessionContext{
				ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: updatedAt,
			}
			state.ChatPolls[chatID] = ChatPollState{
				ChatID: chatID, Seeded: true, PollState: chatPollStateCold,
				NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: updatedAt,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed retry lane fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	assertRetryVisible := func(name string, load func() ([]SessionContext, error)) {
		t.Helper()
		candidates, err := load()
		if err != nil {
			t.Fatalf("%s retry lane candidates: %v", name, err)
		}
		foundRetry := false
		ordinaryCount := 0
		for _, candidate := range candidates {
			if candidate.TeamsChatID == retryChat {
				foundRetry = true
			}
			if strings.HasPrefix(candidate.TeamsChatID, "chat-ordinary-backlog-") {
				ordinaryCount++
			}
		}
		if !foundRetry {
			t.Fatalf("%s omitted due failed chat behind ordinary backlog: %#v", name, candidates)
		}
		if ordinaryCount == 0 {
			t.Fatalf("%s dropped the healthy ordinary lane while admitting retry: %#v", name, candidates)
		}
	}

	assertRetryVisible("optimized", func() ([]SessionContext, error) {
		candidates, handled, err := store.HotPollWorkCandidatesExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
		if err == nil && !handled {
			return nil, fmt.Errorf("optimized SQLite admission was not handled")
		}
		return candidates, err
	})
	assertRetryVisible("legacy", func() ([]SessionContext, error) {
		var candidates []SessionContext
		err := store.withStateLock(ctx, func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("SQLite pointer missing")
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			candidates, err = loadSQLiteHotPollWorkCandidatesLegacy(ctx, db, "control-chat", time.Time{}, now, sqliteHotPollReadyLimit)
			return err
		})
		return candidates, err
	})

	schedule, err := store.HotPollReadyScheduleState(ctx, "control-chat", now)
	if err != nil {
		t.Fatalf("retry lane ready schedule: %v", err)
	}
	if _, ok := schedule.ChatPolls[retryChat]; !ok {
		t.Fatalf("ready schedule omitted due failed chat behind ordinary backlog")
	}
}

func TestSQLiteChatPollScheduleNoopPreservesHotPollFencesAndDoesNotRewrite(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 5, 12, 30, 0, 0, time.UTC)
	store := newTestStore(t)
	const chatID = "chat-hot-poll-noop"
	want := ChatPollState{
		ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
		NextPollAt: now.Add(time.Minute), LastActivityAt: now,
		PollRevision: 31, ScheduleRevision: 37, FrontierEpoch: 11, UpdatedAt: now,
		PendingPage: &ChatPollPendingPage{
			ReceiptID: "receipt-hot-poll-noop", ChatID: chatID,
			RequestPath: "/chats/" + chatID + "/messages?$top=1", Frontier: "head",
		},
		Gap: &ChatPollGap{Epoch: 11, Kind: "test-gap", SafeCursor: now.Add(-time.Hour)},
		Attempt: &ChatPollAttempt{
			ID: "attempt-hot-poll-noop", Owner: "owner-b", ProcessIncarnation: "process-b",
			LeaseGeneration: 12, ExpectedPollRevision: 31, ExpectedScheduleRevision: 37,
			ExpectedFrontier: "head:/chats/" + chatID + "/messages?$top=1", ExpectedReceiptID: "receipt-hot-poll-noop",
			StartedAt: now.Add(-time.Second), ExpiresAt: now.Add(time.Minute),
		},
	}
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls[chatID] = want
		return nil
	}); err != nil {
		t.Fatalf("seed hot-poll no-op fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	beforePoll := sqliteRawChatPollJSONForTest(t, store, chatID)
	beforeState := sqliteRawStateJSONForTest(t, store)
	got, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID: chatID, PollState: want.PollState,
		NextPollAt: want.NextPollAt, LastActivityAt: want.LastActivityAt,
	})
	if err != nil {
		t.Fatalf("SQLite no-op schedule update: %v", err)
	}
	if !bytes.Equal(beforePoll, sqliteRawChatPollJSONForTest(t, store, chatID)) {
		t.Fatal("SQLite no-op schedule update rewrote chat poll projection")
	}
	if !bytes.Equal(beforeState, sqliteRawStateJSONForTest(t, store)) {
		t.Fatal("SQLite no-op schedule update rewrote compatibility state projection")
	}
	if got.PollRevision != want.PollRevision || got.ScheduleRevision != want.ScheduleRevision || got.FrontierEpoch != want.FrontierEpoch {
		t.Fatalf("SQLite no-op schedule update changed revisions: got=%#v want=%#v", got, want)
	}
	if got.PendingPage == nil || got.PendingPage.ReceiptID != want.PendingPage.ReceiptID {
		t.Fatalf("SQLite no-op schedule update dropped pending page fence: %#v", got.PendingPage)
	}
	if got.Gap == nil || got.Gap.Epoch != want.Gap.Epoch {
		t.Fatalf("SQLite no-op schedule update dropped gap fence: %#v", got.Gap)
	}
	if got.Attempt == nil || got.Attempt.ID != want.Attempt.ID || got.Attempt.Owner != want.Attempt.Owner || got.Attempt.ExpectedPollRevision != want.Attempt.ExpectedPollRevision {
		t.Fatalf("SQLite no-op schedule update dropped attempt fence: %#v", got.Attempt)
	}
}
