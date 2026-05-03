package teams

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSendSchedulerRetryAfterBlocksOnlyOneChat(t *testing.T) {
	now := time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	scheduler := NewSendScheduler(SendSchedulerOptions{LeaseDuration: time.Minute})
	mustEnqueueSend(t, scheduler, now, ScheduledSendItem{ID: "a-1", ChatID: "chat-a", Sequence: 1})
	mustEnqueueSend(t, scheduler, now.Add(time.Second), ScheduledSendItem{ID: "b-1", ChatID: "chat-b", Sequence: 1})

	lease, ok := scheduler.Next(now)
	if !ok || lease.ItemID != "a-1" {
		t.Fatalf("first lease = %#v, ok=%v; want a-1", lease, ok)
	}
	if err := scheduler.Fail(lease, SendFailure{Reason: "429", RetryAfter: 30 * time.Second}, now); err != nil {
		t.Fatalf("Fail retry-after: %v", err)
	}

	lease, ok = scheduler.Next(now.Add(time.Second))
	if !ok || lease.ItemID != "b-1" {
		t.Fatalf("retry-after should not block chat-b, lease = %#v ok=%v", lease, ok)
	}
	if err := scheduler.Complete(lease, "teams-b-1", now.Add(2*time.Second)); err != nil {
		t.Fatalf("Complete chat-b: %v", err)
	}
	if lease, ok := scheduler.Next(now.Add(10 * time.Second)); ok {
		t.Fatalf("chat-a retry-after should still hold, got lease %#v", lease)
	}

	lease, ok = scheduler.Next(now.Add(31 * time.Second))
	if !ok || lease.ItemID != "a-1" || lease.Item.Attempts != 2 {
		t.Fatalf("chat-a should retry after block expires, lease = %#v ok=%v", lease, ok)
	}
}

func TestSendSchedulerPreservesSameChatFIFO(t *testing.T) {
	now := time.Date(2026, 4, 30, 10, 5, 0, 0, time.UTC)
	scheduler := NewSendScheduler(SendSchedulerOptions{LeaseDuration: time.Minute})
	mustEnqueueSend(t, scheduler, now, ScheduledSendItem{ID: "later", ChatID: "chat-a", Sequence: 2, CreatedAt: now})
	mustEnqueueSend(t, scheduler, now, ScheduledSendItem{ID: "earlier", ChatID: "chat-a", Sequence: 1, CreatedAt: now.Add(time.Second)})

	lease, ok := scheduler.Next(now)
	if !ok || lease.ItemID != "earlier" {
		t.Fatalf("first same-chat lease = %#v ok=%v; want earlier sequence", lease, ok)
	}
	if next, ok := scheduler.Next(now.Add(10 * time.Second)); ok {
		t.Fatalf("later same-chat item overtook active earlier item: %#v", next)
	}
	if err := scheduler.Complete(lease, "teams-earlier", now.Add(11*time.Second)); err != nil {
		t.Fatalf("Complete earlier: %v", err)
	}
	lease, ok = scheduler.Next(now.Add(12 * time.Second))
	if !ok || lease.ItemID != "later" {
		t.Fatalf("second same-chat lease = %#v ok=%v; want later sequence", lease, ok)
	}
}

func TestSendSchedulerLeaseTimeoutAllowsRetry(t *testing.T) {
	now := time.Date(2026, 4, 30, 10, 10, 0, 0, time.UTC)
	scheduler := NewSendScheduler(SendSchedulerOptions{LeaseDuration: time.Minute})
	mustEnqueueSend(t, scheduler, now, ScheduledSendItem{ID: "a-1", ChatID: "chat-a", Sequence: 1})

	first, ok := scheduler.Next(now)
	if !ok || first.ItemID != "a-1" {
		t.Fatalf("first lease = %#v ok=%v; want a-1", first, ok)
	}
	if next, ok := scheduler.Next(now.Add(30 * time.Second)); ok {
		t.Fatalf("lease should still be active, got %#v", next)
	}
	second, ok := scheduler.Next(first.ExpiresAt.Add(time.Nanosecond))
	if !ok || second.ItemID != "a-1" {
		t.Fatalf("expired lease should be retryable, lease = %#v ok=%v", second, ok)
	}
	if second.LeaseID == first.LeaseID || second.Item.Attempts != 2 {
		t.Fatalf("retry should get a new lease attempt, first=%#v second=%#v", first, second)
	}
}

func TestSendSchedulerPoisonItemDoesNotBlockOtherChats(t *testing.T) {
	now := time.Date(2026, 4, 30, 10, 15, 0, 0, time.UTC)
	scheduler := NewSendScheduler(SendSchedulerOptions{LeaseDuration: time.Minute})
	mustEnqueueSend(t, scheduler, now, ScheduledSendItem{ID: "a-1", ChatID: "chat-a", Sequence: 1, CreatedAt: now})
	mustEnqueueSend(t, scheduler, now, ScheduledSendItem{ID: "b-1", ChatID: "chat-b", Sequence: 1, CreatedAt: now.Add(time.Second)})
	mustEnqueueSend(t, scheduler, now, ScheduledSendItem{ID: "a-2", ChatID: "chat-a", Sequence: 2, CreatedAt: now.Add(2 * time.Second)})

	first, ok := scheduler.Next(now)
	if !ok || first.ItemID != "a-1" {
		t.Fatalf("first lease = %#v ok=%v; want a-1", first, ok)
	}
	if err := scheduler.Fail(first, SendFailure{Reason: "invalid payload", Poison: true}, now.Add(time.Second)); err != nil {
		t.Fatalf("poison a-1: %v", err)
	}
	item, ok := scheduler.Item("a-1")
	if !ok || item.Status != SendStatusPoisoned {
		t.Fatalf("a-1 status = %#v ok=%v; want poisoned", item, ok)
	}

	lease, ok := scheduler.Next(now.Add(2 * time.Second))
	if !ok || lease.ItemID != "b-1" {
		t.Fatalf("poisoned chat-a item should not block chat-b, lease=%#v ok=%v", lease, ok)
	}
	if err := scheduler.Complete(lease, "teams-b-1", now.Add(3*time.Second)); err != nil {
		t.Fatalf("Complete b-1: %v", err)
	}
	lease, ok = scheduler.Next(now.Add(4 * time.Second))
	if !ok || lease.ItemID != "a-2" {
		t.Fatalf("terminal poison should allow next same-chat sequence, lease=%#v ok=%v", lease, ok)
	}
}

func TestSendSchedulerPersistedPlanDefaultsChatAndBatch(t *testing.T) {
	now := time.Date(2026, 4, 30, 10, 20, 0, 0, time.UTC)
	scheduler := NewSendScheduler(SendSchedulerOptions{})
	plan := PersistedSendPlan{
		BatchID: "batch-1",
		ChatID:  "chat-a",
		Items: []ScheduledSendItem{
			{ID: "a-1", Sequence: 1},
			{ID: "b-1", ChatID: "chat-b", Sequence: 1, SourceBatchID: "custom-batch"},
		},
	}
	if err := scheduler.EnqueuePersistedSendPlan(context.Background(), plan); err != nil {
		t.Fatalf("EnqueuePersistedSendPlan error: %v", err)
	}
	item, ok := scheduler.Item("a-1")
	if !ok || item.ChatID != "chat-a" || item.SourceBatchID != "batch-1" {
		t.Fatalf("defaulted persisted item = %#v ok=%v", item, ok)
	}
	item, ok = scheduler.Item("b-1")
	if !ok || item.ChatID != "chat-b" || item.SourceBatchID != "custom-batch" {
		t.Fatalf("explicit persisted item = %#v ok=%v", item, ok)
	}
	duplicatePlan := PersistedSendPlan{ChatID: "chat-a", Items: []ScheduledSendItem{{ID: "a-1", Sequence: 99}}}
	if err := scheduler.EnqueuePersistedSendPlanAt(now.Add(time.Second), duplicatePlan); err != nil {
		t.Fatalf("duplicate EnqueuePersistedSendPlanAt error: %v", err)
	}
	item, ok = scheduler.Item("a-1")
	if !ok || item.Sequence != 1 {
		t.Fatalf("duplicate persisted item should not replace existing item: %#v ok=%v", item, ok)
	}
}

func TestSendSchedulerManualUnblockAndPoisonItem(t *testing.T) {
	now := time.Date(2026, 4, 30, 10, 25, 0, 0, time.UTC)
	scheduler := NewSendScheduler(SendSchedulerOptions{LeaseDuration: time.Minute})
	if _, created, err := scheduler.Enqueue(ScheduledSendItem{ID: "a-1", ChatID: "chat-a", Sequence: 1, CreatedAt: now}); err != nil {
		t.Fatalf("Enqueue error: %v", err)
	} else if !created {
		t.Fatal("Enqueue created=false")
	}
	blockedUntil := now.Add(time.Minute)
	scheduler.BlockChatUntil("chat-a", blockedUntil)
	if got, ok := scheduler.ChatBlockedUntil(" chat-a "); !ok || !got.Equal(blockedUntil) {
		t.Fatalf("ChatBlockedUntil = %v ok=%v, want %v", got, ok, blockedUntil)
	}
	if lease, ok := scheduler.Next(now.Add(time.Second)); ok {
		t.Fatalf("blocked chat should not lease, got %#v", lease)
	}
	scheduler.UnblockChat(" chat-a ")
	lease, ok := scheduler.Next(now.Add(2 * time.Second))
	if !ok || lease.ItemID != "a-1" {
		t.Fatalf("unblocked chat lease = %#v ok=%v, want a-1", lease, ok)
	}
	if err := scheduler.PoisonItem("a-1", "bad payload", now.Add(3*time.Second)); err != nil {
		t.Fatalf("PoisonItem error: %v", err)
	}
	item, ok := scheduler.Item("a-1")
	if !ok || item.Status != SendStatusPoisoned || item.PoisonReason != "bad payload" || item.LeaseID != "" || !item.LeaseUntil.IsZero() {
		t.Fatalf("poisoned item = %#v ok=%v", item, ok)
	}
	if err := scheduler.PoisonItem("missing", "bad", now); !errors.Is(err, ErrScheduledSendItemNotFound) {
		t.Fatalf("missing PoisonItem error = %v, want ErrScheduledSendItemNotFound", err)
	}
}

func mustEnqueueSend(t *testing.T, scheduler *SendScheduler, now time.Time, item ScheduledSendItem) {
	t.Helper()
	if _, created, err := scheduler.EnqueueAt(now, item); err != nil {
		t.Fatalf("EnqueueAt(%s): %v", item.ID, err)
	} else if !created {
		t.Fatalf("EnqueueAt(%s) created=false", item.ID)
	}
}
