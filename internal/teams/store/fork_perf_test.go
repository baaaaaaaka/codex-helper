package store

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// parentForkPerfScale mirrors the shape of the live store observed while
// investigating the Teams service stall. The benchmark intentionally keeps
// setup outside the timed region so the per-call read amplification is
// visible rather than hidden by fixture construction.
type parentForkPerfScale struct {
	name                 string
	sessions             int
	inboundEvents        int
	turns                int
	outboxMessages       int
	transcriptDeliveries int
	helperDeliveries     int
	artifacts            int
	notifications        int
	forkHistoryItems     int
}

var parentForkPerfScales = []parentForkPerfScale{
	{
		name:                 "small",
		sessions:             8,
		inboundEvents:        64,
		turns:                32,
		outboxMessages:       64,
		transcriptDeliveries: 128,
		helperDeliveries:     128,
		artifacts:            16,
		notifications:        16,
		forkHistoryItems:     8,
	},
	{
		// These counts are the read-only snapshot observed in the affected
		// installed scope, rounded only for the less relevant side tables.
		name:                 "live-shape",
		sessions:             477,
		inboundEvents:        93226,
		turns:                8127,
		outboxMessages:       14137,
		transcriptDeliveries: 32383,
		helperDeliveries:     32771,
		artifacts:            1000,
		notifications:        1000,
		forkHistoryItems:     280,
	},
}

// TestParentForkSQLiteLoadProfile is a diagnostic regression test. It
// records whether ParentFork takes the unbounded Store.Load path for the
// common no-fork, active-fork, and terminal-fork cases, and compares it with
// ForkOperations, which already uses the bounded fork-operation projection.
// The test deliberately logs the current behavior so it remains useful while
// the production fix is being evaluated without encoding a machine-specific
// latency threshold.
func TestParentForkSQLiteLoadProfile(t *testing.T) {
	cases := []struct {
		name        string
		sqlite      bool
		operation   bool
		phase       ForkOperationPhase
		wantBlocked bool
	}{
		{name: "json-active", operation: true, phase: ForkPhaseParentFenced, wantBlocked: true},
		{name: "sqlite-no-fork", sqlite: true},
		{name: "sqlite-active", sqlite: true, operation: true, phase: ForkPhaseParentFenced, wantBlocked: true},
		{name: "sqlite-terminal", sqlite: true, operation: true, phase: ForkPhaseLinkSent, wantBlocked: false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			seedParentForkPerfState(t, store, parentForkPerfScales[0], tc.operation, tc.phase)
			if tc.sqlite {
				migrateStoreToSQLiteForTest(t, store)
			}

			var fullLoads int64
			previousHook := loadUnlockedTestHook
			loadUnlockedTestHook = func() {
				atomic.AddInt64(&fullLoads, 1)
			}
			t.Cleanup(func() { loadUnlockedTestHook = previousHook })

			op, blocked, err := store.ParentFork(ctx, "parent")
			if err != nil {
				t.Fatalf("ParentFork: %v", err)
			}
			if blocked != tc.wantBlocked {
				t.Fatalf("ParentFork blocked=%v op=%#v, want %v", blocked, op, tc.wantBlocked)
			}
			parentLoads := atomic.LoadInt64(&fullLoads)

			atomic.StoreInt64(&fullLoads, 0)
			operations, err := store.ForkOperations(ctx)
			if err != nil {
				t.Fatalf("ForkOperations: %v", err)
			}
			operationLoads := atomic.LoadInt64(&fullLoads)

			atomic.StoreInt64(&fullLoads, 0)
			operationID := "fork-perf"
			if !tc.operation {
				operationID = "missing-fork"
			}
			_, found, err := store.ForkOperation(ctx, operationID)
			if err != nil {
				t.Fatalf("ForkOperation: %v", err)
			}
			singularOperationLoads := atomic.LoadInt64(&fullLoads)
			if found != tc.operation {
				t.Fatalf("ForkOperation found=%v, want %v", found, tc.operation)
			}
			t.Logf("sqlite=%v operation=%v phase=%q parent_fork_full_loads=%d fork_operations_full_loads=%d fork_operation_full_loads=%d fork_operations=%d", tc.sqlite, tc.operation, tc.phase, parentLoads, operationLoads, singularOperationLoads, len(operations))
			if tc.sqlite && parentLoads != 0 {
				t.Fatalf("ParentFork loaded full state %d times", parentLoads)
			}
			if tc.sqlite && operationLoads != 0 {
				t.Fatalf("bounded ForkOperations unexpectedly loaded full state %d times", operationLoads)
			}
			if tc.sqlite && singularOperationLoads != 0 {
				t.Fatalf("bounded ForkOperation loaded full state %d times", singularOperationLoads)
			}
		})
	}
}

func BenchmarkParentForkSQLiteScale(b *testing.B) {
	for _, scale := range parentForkPerfScales {
		scale := scale
		b.Run(scale.name+"/ParentFork", func(b *testing.B) {
			store := newParentForkPerfStore(b, scale)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, blocked, err := store.ParentFork(ctx, "parent")
				if err != nil {
					b.Fatalf("ParentFork: %v", err)
				}
				if !blocked {
					b.Fatal("ParentFork did not find the active fixture operation")
				}
			}
		})
		b.Run(scale.name+"/ForkOperations", func(b *testing.B) {
			store := newParentForkPerfStore(b, scale)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				operations, err := store.ForkOperations(ctx)
				if err != nil {
					b.Fatalf("ForkOperations: %v", err)
				}
				if len(operations) != 1 {
					b.Fatalf("ForkOperations returned %d operations, want one", len(operations))
				}
			}
		})
		b.Run(scale.name+"/ForkOperation", func(b *testing.B) {
			store := newParentForkPerfStore(b, scale)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				operation, found, err := store.ForkOperation(ctx, "fork-perf")
				if err != nil {
					b.Fatalf("ForkOperation: %v", err)
				}
				if !found || operation.ID != "fork-perf" {
					b.Fatalf("ForkOperation = %#v found=%v, want fork-perf", operation, found)
				}
			}
		})
	}
}

func newParentForkPerfStore(tb testing.TB, scale parentForkPerfScale) *Store {
	tb.Helper()
	store, err := Open(tb.TempDir() + "/state.json")
	if err != nil {
		tb.Fatalf("open ParentFork perf store: %v", err)
	}
	tb.Cleanup(func() {
		if err := store.Close(); err != nil {
			tb.Errorf("close ParentFork perf store: %v", err)
		}
	})
	seedParentForkPerfState(tb, store, scale, true, ForkPhaseParentFenced)
	if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		tb.Fatalf("migrate ParentFork perf store to SQLite: %v", err)
	}
	return store
}

func seedParentForkPerfState(tb testing.TB, store *Store, scale parentForkPerfScale, operation bool, phase ForkOperationPhase) {
	tb.Helper()
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	if err := store.Update(context.Background(), func(state *State) error {
		for i := 0; i < max(1, scale.sessions); i++ {
			id := fmt.Sprintf("session-%06d", i)
			state.Sessions[id] = SessionContext{
				ID:            id,
				Status:        SessionStatusActive,
				TeamsChatID:   fmt.Sprintf("chat-%06d", i),
				CodexThreadID: fmt.Sprintf("thread-%06d", i),
				CreatedAt:     now,
				UpdatedAt:     now,
			}
		}
		state.Sessions["parent"] = SessionContext{
			ID:            "parent",
			Status:        SessionStatusActive,
			TeamsChatID:   "parent-chat",
			CodexThreadID: "parent-thread",
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		for i := 0; i < scale.inboundEvents; i++ {
			id := fmt.Sprintf("inbound-%06d", i)
			state.InboundEvents[id] = InboundEvent{ID: id, SessionID: "parent", TeamsChatID: "parent-chat", TeamsMessageID: id, Source: "teams", Status: InboundStatusPersisted, CreatedAt: now, UpdatedAt: now}
		}
		for i := 0; i < scale.turns; i++ {
			id := fmt.Sprintf("turn-%06d", i)
			state.Turns[id] = Turn{ID: id, SessionID: "parent", InboundEventID: fmt.Sprintf("inbound-%06d", i%max(1, scale.inboundEvents)), Status: TurnStatusCompleted, CodexThreadID: "parent-thread", CreatedAt: now, UpdatedAt: now, CompletedAt: now}
		}
		for i := 0; i < scale.outboxMessages; i++ {
			id := fmt.Sprintf("outbox-%06d", i)
			state.OutboxMessages[id] = OutboxMessage{ID: id, SessionID: "parent", TurnID: fmt.Sprintf("turn-%06d", i%max(1, scale.turns)), TeamsChatID: "parent-chat", Kind: "final", Body: "perf outbox body", Status: OutboxStatusSent, CreatedAt: now, UpdatedAt: now, SentAt: now}
		}
		for i := 0; i < scale.transcriptDeliveries; i++ {
			id := fmt.Sprintf("transcript-delivery-%06d", i)
			state.TranscriptDeliveries[id] = TranscriptDeliveryRecord{ID: id, SessionID: "parent", CodexThreadID: "parent-thread", SourceRecordID: id, OutboxID: fmt.Sprintf("outbox-%06d", i%max(1, scale.outboxMessages)), Status: TranscriptDeliveryStatusSent, CreatedAt: now, UpdatedAt: now, SentAt: now}
		}
		for i := 0; i < scale.helperDeliveries; i++ {
			id := fmt.Sprintf("helper-delivery-%06d", i)
			state.HelperDeliveries[id] = HelperDeliveryRecord{ID: id, SessionID: "parent", TurnID: fmt.Sprintf("turn-%06d", i%max(1, scale.turns)), TeamsChatID: "parent-chat", Kind: "final", Status: HelperDeliveryStatusSent, CreatedAt: now, UpdatedAt: now, SentAt: now}
		}
		for i := 0; i < scale.artifacts; i++ {
			id := fmt.Sprintf("artifact-%06d", i)
			state.ArtifactRecords[id] = ArtifactRecord{ID: id, SessionID: "parent", TurnID: fmt.Sprintf("turn-%06d", i%max(1, scale.turns)), Path: "artifact.txt", Status: "uploaded", CreatedAt: now, UpdatedAt: now, UploadedAt: now}
		}
		for i := 0; i < scale.notifications; i++ {
			id := fmt.Sprintf("notification-%06d", i)
			state.Notifications[id] = NotificationRecord{ID: id, SessionID: "parent", TurnID: fmt.Sprintf("turn-%06d", i%max(1, scale.turns)), Kind: "turn_completed", Status: NotificationStatusSent, Title: "perf notification", CreatedAt: now, UpdatedAt: now, SentAt: now}
		}
		for i := 0; i < scale.forkHistoryItems; i++ {
			id := fmt.Sprintf("fork-history:perf:%06d", i)
			state.ForkHistoryItems[id] = ForkHistoryItem{ID: id, OperationID: "fork-perf", Ordinal: i, SourceRecordID: id, Kind: "assistant", DeliveryStatus: ForkHistoryDeliverySent, CreatedAt: now, UpdatedAt: now}
		}
		if operation {
			state.ForkOperations["fork-perf"] = ForkOperation{ID: "fork-perf", ParentSessionID: "parent", ParentChatID: "parent-chat", ParentThreadID: "parent-thread", ChildSessionID: "child", Phase: phase, HistoryNamespace: "fork-history:fork-perf", CreatedAt: now, UpdatedAt: now}
		}
		return nil
	}); err != nil {
		tb.Fatalf("seed ParentFork perf state: %v", err)
	}
}
