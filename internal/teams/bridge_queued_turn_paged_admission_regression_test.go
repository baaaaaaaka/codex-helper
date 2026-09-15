package teams

import (
	"context"
	"testing"
	"time"
)

func TestBridgeSQLiteQueuedTurnAdmissionUsesPagedSelectedSessions(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeQueuedTurnGraph(t, map[string]string{
		"message-s001": "paged queued prompt one",
		"message-s002": "paged queued prompt two",
	})
	teamStore := newBridgeTestStore(t)
	executor := &parallelBlockingExecutor{
		started: make(chan parallelSessionStart, 2),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, teamStore, executor)
	bridge.asyncTurns = true
	bridge.maxQueuedTurnStartsPerCycle = 1

	first := bridge.reg.SessionByID("s001")
	if first == nil {
		t.Fatal("missing base session s001")
	}
	if err := bridge.ensureDurableSession(ctx, first); err != nil {
		t.Fatalf("ensure first session: %v", err)
	}
	second := appendBridgeTestSession(t, bridge, teamStore, "s002", "chat-2")
	queueBridgeTurnForTest(t, bridge, first, "message-s001", "paged queued prompt one", time.Now())
	queueBridgeTurnForTest(t, bridge, second, "message-s002", "paged queued prompt two", time.Now().Add(time.Second))
	if _, err := teamStore.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate queued admission fixture: %v", err)
	}

	started, err := bridge.processQueuedTurnsWithStartBudget(ctx, 1, true)
	if err != nil || started != 1 {
		close(executor.release)
		t.Fatalf("first paged admission started=%d err=%v, want one", started, err)
	}
	select {
	case got := <-executor.started:
		if got.SessionID != first.ID {
			close(executor.release)
			t.Fatalf("first paged admission session=%q, want %q", got.SessionID, first.ID)
		}
	case <-time.After(bridgeAsyncTestTimeout):
		close(executor.release)
		t.Fatal("first paged queued turn did not start")
	}
	select {
	case got := <-executor.started:
		close(executor.release)
		t.Fatalf("second session bypassed per-cycle admission bound: %#v", got)
	case <-time.After(50 * time.Millisecond):
	}

	close(executor.release)
	waitForCompletedTurnCount(t, teamStore, first.ID, 1)
	waitForBridgeAsyncTurns(t, bridge)
	started, err = bridge.processQueuedTurnsWithStartBudget(ctx, 1, true)
	if err != nil || started != 1 {
		t.Fatalf("second paged admission started=%d err=%v, want one", started, err)
	}
	select {
	case got := <-executor.started:
		if got.SessionID != second.ID {
			t.Fatalf("second paged admission session=%q, want %q", got.SessionID, second.ID)
		}
	case <-time.After(bridgeAsyncTestTimeout):
		t.Fatal("second paged queued turn did not start after cursor advance")
	}
	waitForCompletedTurnCount(t, teamStore, second.ID, 1)
	waitForBridgeAsyncTurns(t, bridge)

}
