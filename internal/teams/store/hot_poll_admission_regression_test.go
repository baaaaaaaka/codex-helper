package store

import (
	"bytes"
	"context"
	"fmt"
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
				RequestPath: "/chats/" + chatID + "/messages?$top=1",
			},
			Gap: &ChatPollGap{
				Epoch: 5, Kind: "test-gap", SafeCursor: now.Add(-time.Hour),
				RecoveryCursor: now.Add(-30 * time.Minute),
			},
			Attempt: &ChatPollAttempt{
				ID: "attempt-hot-poll-due", Owner: "owner-a", ProcessIncarnation: "process-a",
				LeaseGeneration: 9, ExpectedPollRevision: 17, ExpectedScheduleRevision: 23,
				ExpectedFrontier: "frontier-a", ExpectedReceiptID: "receipt-hot-poll-due",
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
			RequestPath: "/chats/" + chatID + "/messages?$top=1",
		},
		Gap: &ChatPollGap{Epoch: 11, Kind: "test-gap", SafeCursor: now.Add(-time.Hour)},
		Attempt: &ChatPollAttempt{
			ID: "attempt-hot-poll-noop", Owner: "owner-b", ProcessIncarnation: "process-b",
			LeaseGeneration: 12, ExpectedPollRevision: 31, ExpectedScheduleRevision: 37,
			ExpectedFrontier: "frontier-b", ExpectedReceiptID: "receipt-hot-poll-noop",
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
