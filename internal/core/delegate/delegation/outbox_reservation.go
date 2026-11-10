package delegation

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ReserveOutbox commits the durable fence before a non-idempotent Graph POST.
func ReserveOutbox(ctx context.Context, path string, record Record, chatID string, now time.Time) (OutboxRecord, bool, error) {
	if strings.TrimSpace(path) == "" {
		return OutboxRecord{}, false, fmt.Errorf("durable delegation state path is required before reserving a Graph POST")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return reserveOutboxSQLite(ctx, path, record, chatID, now)
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
