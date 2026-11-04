package delegation

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestReserveOutboxAllowsOnlyOneConcurrentNonIdempotentOwner(t *testing.T) {
	record, err := NewClaimRecord("delegation-reservation", "machine-a", "worker-a", 1, time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("new claim: %v", err)
	}
	for _, suffix := range []string{"json", "sqlite"} {
		t.Run(suffix, func(t *testing.T) {
			path := t.TempDir() + "/worker-state." + suffix
			if suffix == "sqlite" {
				db, err := openDelegationSQLiteStore(path, true)
				if err != nil {
					t.Fatalf("open sqlite reservation fixture: %v", err)
				}
				if err := ensureDelegationSQLiteSchema(context.Background(), db); err != nil {
					_ = db.Close()
					t.Fatalf("prepare sqlite reservation fixture: %v", err)
				}
				if err := db.Close(); err != nil {
					t.Fatalf("close sqlite reservation fixture: %v", err)
				}
			}
			const callers = 16
			results := make(chan bool, callers)
			errs := make(chan error, callers)
			var start sync.WaitGroup
			start.Add(callers)
			var done sync.WaitGroup
			done.Add(callers)
			for i := 0; i < callers; i++ {
				go func() {
					defer done.Done()
					start.Done()
					start.Wait()
					_, reserved, reserveErr := ReserveOutbox(context.Background(), path, record, "chat-inbox", time.Date(2026, 9, 14, 12, 0, 1, 0, time.UTC))
					if reserveErr != nil {
						errs <- reserveErr
						return
					}
					results <- reserved
				}()
			}
			done.Wait()
			close(results)
			close(errs)
			for reserveErr := range errs {
				t.Fatalf("concurrent reservation: %v", reserveErr)
			}
			reservedCount := 0
			for reserved := range results {
				if reserved {
					reservedCount++
				}
			}
			if reservedCount != 1 {
				t.Fatalf("reserved callers = %d, want exactly one", reservedCount)
			}

			store, err := LoadStore(path)
			if err != nil {
				t.Fatalf("load reserved store: %v", err)
			}
			outbox, ok := store.OutboxForRecordID(record.RecordID)
			if !ok {
				t.Fatalf("outbox %q is missing", record.RecordID)
			}
			if outbox.Attempts != 1 || outbox.Status != OutboxPending {
				t.Fatalf("reserved outbox = %#v, want one pending attempt", outbox)
			}
		})
	}
}

func TestJSONWorkerStoreAuxiliaryWritesPreserveDurableSafetyState(t *testing.T) {
	path := t.TempDir() + "/worker-state.json"
	record, err := NewClaimRecord("delegation-auxiliary-writes", "machine-a", "worker-a", 1, time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("new claim: %v", err)
	}
	if _, reserved, err := ReserveOutbox(context.Background(), path, record, "chat-inbox", time.Date(2026, 9, 14, 12, 0, 1, 0, time.UTC)); err != nil || !reserved {
		t.Fatalf("initial reservation reserved=%v err=%v", reserved, err)
	}

	const writers = 24
	errs := make(chan error, writers*2)
	var done sync.WaitGroup
	done.Add(writers * 2)
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer done.Done()
			err := UpsertInboxBackoffJSON(path, InboxBackoff{
				ChatID:       "chat-inbox",
				BlockedUntil: time.Date(2026, 9, 14, 12, 1, i, 0, time.UTC).Format(time.RFC3339Nano),
				Reason:       "429 writer",
				UpdatedAt:    time.Date(2026, 9, 14, 12, 0, i, 0, time.UTC).Format(time.RFC3339Nano),
			})
			if err != nil {
				errs <- err
			}
		}(i)
		go func(i int) {
			defer done.Done()
			err := UpsertInboxCursorJSON(path, InboxCursor{
				ChatID:            "chat-inbox",
				LastHeadMessageID: "head-" + time.Date(2026, 9, 14, 12, 0, i, 0, time.UTC).Format("150405"),
				UpdatedAt:         time.Date(2026, 9, 14, 12, 2, i, 0, time.UTC).Format(time.RFC3339Nano),
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	done.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent auxiliary write: %v", err)
	}

	store, err := LoadStore(path)
	if err != nil {
		t.Fatalf("load worker store: %v", err)
	}
	outbox, ok := store.OutboxForRecordID(record.RecordID)
	if !ok || outbox.Status != OutboxPending || outbox.Attempts != 1 {
		t.Fatalf("reservation after auxiliary writes = %#v, want pending/one attempt", outbox)
	}
	if cursor, ok := store.InboxCursorForChat("chat-inbox"); !ok || cursor.LastHeadMessageID == "" {
		t.Fatalf("cursor after auxiliary writes = %#v found=%v, want durable cursor", cursor, ok)
	}
	if backoff, ok := store.InboxBackoffForChat("chat-inbox"); !ok || backoff.BlockedUntil == "" {
		t.Fatalf("backoff after auxiliary writes = %#v found=%v, want durable retry gate", backoff, ok)
	}
}

func TestJSONWorkerStoreExecutionUpdatesAreAtomicWithAuxiliaryWrites(t *testing.T) {
	path := t.TempDir() + "/worker-state.json"
	record, err := NewClaimRecord("delegation-execution-update", "machine-a", "worker-a", 1, time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("new claim: %v", err)
	}
	if _, reserved, err := ReserveOutbox(context.Background(), path, record, "chat-inbox", time.Date(2026, 9, 14, 12, 0, 1, 0, time.UTC)); err != nil || !reserved {
		t.Fatalf("initial reservation reserved=%v err=%v", reserved, err)
	}

	const writers = 16
	errs := make(chan error, writers*3)
	var done sync.WaitGroup
	done.Add(writers * 3)
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer done.Done()
			err := UpdateStoreJSON(context.Background(), path, func(store *Store) error {
				store.UpsertExecution(ExecutionFence{
					DelegationID: record.DelegationID,
					ClaimID:      record.RecordID,
					Status:       StateRunning,
					UpdatedAt:    time.Date(2026, 9, 14, 12, 3, i, 0, time.UTC).Format(time.RFC3339Nano),
				})
				return nil
			})
			if err != nil {
				errs <- err
			}
		}(i)
		go func(i int) {
			defer done.Done()
			err := UpsertInboxCursorJSON(path, InboxCursor{
				ChatID:            "chat-inbox",
				LastHeadMessageID: "head-execution-" + time.Date(2026, 9, 14, 12, 0, i, 0, time.UTC).Format("150405"),
				UpdatedAt:         time.Date(2026, 9, 14, 12, 4, i, 0, time.UTC).Format(time.RFC3339Nano),
			})
			if err != nil {
				errs <- err
			}
		}(i)
		go func(i int) {
			defer done.Done()
			err := UpsertInboxBackoffJSON(path, InboxBackoff{
				ChatID:       "chat-inbox",
				BlockedUntil: time.Date(2026, 9, 14, 12, 5, i, 0, time.UTC).Format(time.RFC3339Nano),
				Reason:       "execution update race",
				UpdatedAt:    time.Date(2026, 9, 14, 12, 6, i, 0, time.UTC).Format(time.RFC3339Nano),
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	done.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent execution/auxiliary update: %v", err)
	}

	store, err := LoadStore(path)
	if err != nil {
		t.Fatalf("load worker store: %v", err)
	}
	outbox, ok := store.OutboxForRecordID(record.RecordID)
	if !ok || outbox.Status != OutboxPending || outbox.Attempts != 1 {
		t.Fatalf("reservation after execution updates = %#v, want pending/one attempt", outbox)
	}
	if execution, ok := store.ExecutionForID(record.DelegationID); !ok || execution.Status != StateRunning {
		t.Fatalf("execution after concurrent updates = %#v found=%v, want running fence", execution, ok)
	}
}
