package store

import (
	"context"
	"testing"
	"time"
)

func TestSQLiteQueueOutboxAvoidsColdHistoryWatchHydration(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:queue-hot-path-seed",
		TeamsChatID: "chat:queue-hot-path",
		Kind:        "helper-status",
		Body:        "seed",
		Status:      OutboxStatusQueued,
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed legacy outbox row: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	loads := 0
	previousHook := sqliteHistoryWatchProjectionLoadTestHook
	sqliteHistoryWatchProjectionLoadTestHook = func() { loads++ }
	t.Cleanup(func() { sqliteHistoryWatchProjectionLoadTestHook = previousHook })

	queued, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:queue-hot-path-target",
		TeamsChatID: "chat:queue-hot-path",
		Kind:        "helper-status",
		Body:        "target",
		Status:      OutboxStatusQueued,
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("queue target outbox row: %v", err)
	}
	if !created || queued.ID != "outbox:queue-hot-path-target" {
		t.Fatalf("queued target = %#v created=%t, want a new target row", queued, created)
	}
	if loads != 0 {
		t.Fatalf("ordinary SQLite outbox queue hydrated the cold history-watch projection %d time(s)", loads)
	}
}
