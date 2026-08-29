package store

import (
	"context"
	"sort"
	"testing"
	"time"
)

func TestStoreJSONSQLiteDispositionParity(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			queuedAt := now.Add(-2 * time.Minute)
			for _, msg := range []OutboxMessage{
				{ID: "outbox:parity:due", TeamsChatID: "parity-chat", Kind: "helper", Body: "due", CreatedAt: queuedAt},
				{ID: "outbox:parity:future", TeamsChatID: "parity-chat", Kind: "helper", Body: "future", CreatedAt: queuedAt.Add(time.Second)},
			} {
				if _, created, err := store.QueueOutbox(ctx, msg); err != nil || !created {
					t.Fatalf("QueueOutbox %s created=%v err=%v", msg.ID, created, err)
				}
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			future := now.Add(time.Hour)
			if _, err := store.DeferOutboxDeliveryUntil(ctx, "outbox:parity:future", future); err != nil {
				t.Fatalf("DeferOutboxDeliveryUntil: %v", err)
			}

			page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 10})
			if err != nil {
				t.Fatalf("PendingOutboxPageAt due: %v", err)
			}
			if got := backlogOutboxIDs(page.Messages); len(got) != 1 || got[0] != "outbox:parity:due" {
				t.Fatalf("due pending ids = %#v, want only due row", got)
			}
			all, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 10, IgnoreRetryGate: true})
			if err != nil {
				t.Fatalf("PendingOutboxPageAt ignore gate: %v", err)
			}
			if got := backlogOutboxIDs(all.Messages); len(got) != 2 {
				t.Fatalf("ignore-gate pending ids = %#v, want both rows", got)
			}

			if _, err := store.SetChatRateLimit(ctx, "parity-chat", now.Add(2*time.Hour), "test rate limit"); err != nil {
				t.Fatalf("SetChatRateLimit: %v", err)
			}
			limited, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 10})
			if err != nil {
				t.Fatalf("PendingOutboxPageAt rate limit: %v", err)
			}
			if len(limited.Messages) != 0 {
				t.Fatalf("rate-limited pending rows = %#v, want none", backlogOutboxIDs(limited.Messages))
			}
			if err := store.ClearChatRateLimit(ctx, "parity-chat"); err != nil {
				t.Fatalf("ClearChatRateLimit: %v", err)
			}
			woken, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 10})
			if err != nil {
				t.Fatalf("PendingOutboxPageAt after clear: %v", err)
			}
			if got := backlogOutboxIDs(woken.Messages); len(got) != 2 {
				t.Fatalf("woken pending ids = %#v, want both rows", got)
			}

			futureRow, err := store.OutboxMessageByID(ctx, "outbox:parity:future")
			if err != nil {
				t.Fatalf("OutboxMessageByID future row: %v", err)
			}
			if !futureRow.NextAttemptAt.IsZero() {
				t.Fatalf("future row retry gate = %s, want cleared by chat wake-up", futureRow.NextAttemptAt)
			}
		})
	}
}

func TestStorePollFrontierAndScheduleRevisionRace(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			old, acquired, err := store.BeginChatPollAttempt(ctx, ChatPollAttemptRequest{
				ChatID:             "poll-race-chat",
				Owner:              "old-owner",
				ProcessIncarnation: "old-process",
				LeaseGeneration:    11,
				Now:                now,
				TTL:                time.Minute,
			})
			if err != nil || !acquired || old.Attempt == nil {
				t.Fatalf("begin old attempt acquired=%v attempt=%#v err=%v", acquired, old.Attempt, err)
			}
			oldCapability := ChatPollAttemptCapability{
				ID:                 old.Attempt.ID,
				Owner:              old.Attempt.Owner,
				ProcessIncarnation: old.Attempt.ProcessIncarnation,
				LeaseGeneration:    old.Attempt.LeaseGeneration,
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			// Model an owner handoff that preserves the attempt ID but changes the
			// durable identity. Attempt-ID-only CAS would accept a delayed callback
			// if it learned the new revision; the capability CAS must reject it.
			if _, changed, err := store.UpdateChatPoll(ctx, "poll-race-chat", func(poll *ChatPollState) error {
				if poll.Attempt == nil {
					t.Fatalf("poll attempt disappeared during handoff")
				}
				poll.Attempt.Owner = "new-owner"
				poll.Attempt.ProcessIncarnation = "new-process"
				poll.Attempt.LeaseGeneration = 12
				poll.Attempt.ExpectedPollRevision = poll.PollRevision + 1
				return nil
			}); err != nil || !changed {
				t.Fatalf("seed replacement capability changed=%v err=%v", changed, err)
			}
			current, found, err := store.ChatPoll(ctx, "poll-race-chat")
			if err != nil || !found || current.Attempt == nil {
				t.Fatalf("current poll after handoff found=%v attempt=%#v err=%v", found, current.Attempt, err)
			}
			if _, committed, err := store.CommitChatPollAttemptWithCapability(ctx, "poll-race-chat", oldCapability, current.PollRevision, func(poll *ChatPollState) error {
				poll.LastError = "stale callback must not win"
				return nil
			}); err != nil || committed {
				t.Fatalf("old capability commit committed=%v err=%v, want rejected", committed, err)
			}
			current, found, err = store.ChatPoll(ctx, "poll-race-chat")
			if err != nil || !found || current.Attempt == nil {
				t.Fatalf("current poll found=%v attempt=%#v err=%v", found, current.Attempt, err)
			}
			newCapability := oldCapability
			newCapability.Owner = "new-owner"
			newCapability.ProcessIncarnation = "new-process"
			newCapability.LeaseGeneration = 12
			if _, committed, err := store.CommitChatPollAttemptWithCapability(ctx, "poll-race-chat", newCapability, current.PollRevision, func(poll *ChatPollState) error {
				poll.LastError = "replacement callback committed"
				return nil
			}); err != nil || !committed {
				t.Fatalf("new capability commit committed=%v err=%v, want committed", committed, err)
			}
			final, found, err := store.ChatPoll(ctx, "poll-race-chat")
			if err != nil || !found {
				t.Fatalf("final poll found=%v err=%v", found, err)
			}
			if final.LastError != "replacement callback committed" || final.Attempt != nil {
				t.Fatalf("final poll = %#v, want replacement result without attempt", final)
			}
		})
	}
}

func backlogOutboxIDs(messages []OutboxMessage) []string {
	ids := make([]string, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.ID)
	}
	sort.Strings(ids)
	return ids
}
