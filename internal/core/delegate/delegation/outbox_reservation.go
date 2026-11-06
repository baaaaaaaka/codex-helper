package delegation

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

// Auxiliary worker-store updates can serialize several full JSON
// read/modify/write operations behind one cross-process lock. Keep the wait
// finite, but allow slow hosted filesystems and concurrent safety updates to
// drain instead of turning ordinary contention into a lost retry/backoff.
const delegationOutboxReservationLockTimeout = 30 * time.Second

// ReserveOutbox durably reserves the one non-idempotent POST associated with a
// delegation record. The reservation is made before the Graph call and is
// atomic across processes for both the legacy JSON and SQLite worker stores.
// A false return means that an earlier reservation or POST witness already
// exists; callers must reconcile visibility and must not POST again.
func ReserveOutbox(ctx context.Context, path string, record Record, chatID string, now time.Time) (OutboxRecord, bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return OutboxRecord{}, false, fmt.Errorf("durable delegation state path is required before reserving a Graph POST")
	}
	if StorePathUsesSQLite(path) {
		return reserveOutboxSQLite(ctx, path, record, chatID, now)
	}
	return reserveOutboxJSON(ctx, path, record, chatID, now)
}

// UpsertWorkerOutboxJSON applies a worker outbox transition while holding the
// same cross-process lock used by ReserveOutbox. Without this lock, a status
// update for another record could overwrite an Unknown witness and reopen a
// duplicate-POST window on the next poll.
func UpsertWorkerOutboxJSON(path string, record Record, status string, chatID string, messageID string, errText string, now time.Time) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	return withDelegationJSONStoreLock(context.Background(), path, func() error {
		store, err := LoadStore(path)
		if err != nil {
			return err
		}
		upsertWorkerOutbox(&store, record, status, chatID, messageID, errText, now)
		_, err = SaveStore(path, store)
		return err
	})
}

// UpsertInboxCursorJSON persists an observation boundary without allowing a
// concurrent worker-store read/modify/write to overwrite a reservation,
// execution fence, or another cursor. Cursor advancement is deliberately a
// separate durable step from record admission; callers invoke it only after
// the corresponding inbox window has been handled successfully.
func UpsertInboxCursorJSON(path string, cursor InboxCursor) error {
	path = strings.TrimSpace(path)
	if path == "" || strings.TrimSpace(cursor.ChatID) == "" {
		return nil
	}
	return withDelegationJSONStoreLock(context.Background(), path, func() error {
		store, err := LoadStore(path)
		if err != nil {
			return err
		}
		store.UpsertInboxCursor(cursor)
		store.Prune(parseDelegationTime(cursor.UpdatedAt), DefaultStoreRetention)
		_, err = SaveStore(path, store)
		return err
	})
}

// UpsertInboxBackoffJSON applies a local Graph retry gate while holding the
// same cross-process lock as the other worker-store read/modify/write paths.
// A backoff is advisory, but losing a durable outbox or cursor alongside it
// would turn a transient 429 into a duplicate-send or skipped-message window.
func UpsertInboxBackoffJSON(path string, backoff InboxBackoff) error {
	path = strings.TrimSpace(path)
	if path == "" || strings.TrimSpace(backoff.ChatID) == "" {
		return nil
	}
	return withDelegationJSONStoreLock(context.Background(), path, func() error {
		store, err := LoadStore(path)
		if err != nil {
			return err
		}
		store.UpsertInboxBackoff(backoff)
		store.Prune(parseDelegationTime(backoff.UpdatedAt), DefaultStoreRetention)
		_, err = SaveStore(path, store)
		return err
	})
}

// UpdateStoreJSON executes one JSON worker-store read/modify/write while
// holding the same cross-process lock as the other durable auxiliary updates.
// The callback must only mutate the supplied store; Graph calls must remain
// outside this critical section. Keeping this primitive in the delegation
// package prevents a JSON-only caller from accidentally reintroducing a stale
// full-store overwrite around an execution fence or POST witness.
func UpdateStoreJSON(ctx context.Context, path string, update func(*Store) error) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	if StorePathUsesSQLite(path) {
		return fmt.Errorf("delegation store %q is sqlite; use an atomic sqlite worker operation", path)
	}
	if update == nil {
		return nil
	}
	return withDelegationJSONStoreLock(ctx, path, func() error {
		store, err := LoadStore(path)
		if err != nil {
			return err
		}
		if err := update(&store); err != nil {
			return err
		}
		_, err = SaveStore(path, store)
		return err
	})
}

func parseDelegationTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return time.Now().UTC()
	}
	return parsed
}

func reserveOutboxJSON(ctx context.Context, path string, record Record, chatID string, now time.Time) (OutboxRecord, bool, error) {
	var reserved OutboxRecord
	claimed := false
	err := withDelegationJSONStoreLock(ctx, path, func() error {
		store, err := LoadStore(path)
		if err != nil {
			return err
		}
		existing, exists := store.OutboxForRecordID(record.RecordID)
		if exists && outboxHasDeliveryWitness(existing) {
			reserved = existing
			return nil
		}
		reserved = nextWorkerOutbox(existing, record, OutboxPending, chatID, "", "", now)
		store.UpsertOutbox(reserved)
		if _, err := SaveStore(path, store); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	return reserved, claimed, err
}

func withDelegationJSONStoreLock(ctx context.Context, path string, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	lock := flock.New(path + ".lock")
	lockCtx, cancel := context.WithTimeout(ctx, delegationOutboxReservationLockTimeout)
	defer cancel()
	ok, err := lock.TryLockContext(lockCtx, 10*time.Millisecond)
	if err != nil {
		return err
	}
	if !ok {
		if err := lockCtx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("delegation worker store is locked: %s", path)
	}
	defer func() { _ = lock.Unlock() }()
	return fn()
}

func outboxHasDeliveryWitness(outbox OutboxRecord) bool {
	return outbox.Attempts > 0 || strings.TrimSpace(outbox.MessageID) != "" ||
		strings.TrimSpace(outbox.Status) == OutboxUnknown ||
		strings.TrimSpace(outbox.Status) == OutboxSent ||
		strings.TrimSpace(outbox.Status) == OutboxVisible
}

func nextWorkerOutbox(existing OutboxRecord, record Record, status, chatID, messageID, errText string, now time.Time) OutboxRecord {
	nowText := now.UTC().Format(time.RFC3339Nano)
	createdAt := strings.TrimSpace(existing.CreatedAt)
	if createdAt == "" {
		createdAt = nowText
	}
	if strings.TrimSpace(messageID) == "" {
		messageID = existing.MessageID
	}
	attempts := existing.Attempts
	if status == OutboxPending {
		attempts++
	}
	return OutboxRecord{
		RecordID:     record.RecordID,
		DelegationID: record.DelegationID,
		ChatID:       strings.TrimSpace(chatID),
		InboxRef:     strings.TrimSpace(record.InboxRef),
		Status:       strings.TrimSpace(status),
		MessageID:    strings.TrimSpace(messageID),
		Attempts:     attempts,
		Error:        strings.TrimSpace(errText),
		CreatedAt:    createdAt,
		UpdatedAt:    nowText,
	}
}

func upsertWorkerOutbox(store *Store, record Record, status, chatID, messageID, errText string, now time.Time) {
	if store == nil {
		return
	}
	existing, _ := store.OutboxForRecordID(record.RecordID)
	store.UpsertOutbox(nextWorkerOutbox(existing, record, status, chatID, messageID, errText, now))
	store.Prune(now.UTC(), DefaultStoreRetention)
}
