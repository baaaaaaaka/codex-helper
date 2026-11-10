package delegation

import (
	"strings"
	"time"
)

// Frozen in-memory retention oracle; never used by the new runtime.
func (s *Store) Prune(now time.Time, retention time.Duration) {
	if s == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if retention <= 0 {
		retention = DefaultStoreRetention
	}
	cutoff := now.Add(-retention)
	prunedDelegations := map[string]bool{}
	if len(s.Records) > 0 {
		byID := map[string][]Record{}
		for _, record := range s.Records {
			if id := strings.TrimSpace(record.DelegationID); id != "" {
				byID[id] = append(byID[id], record)
			}
		}
		for id, records := range byID {
			state := Reduce(records, now)
			if !terminalOrExpiredState(state) {
				continue
			}
			if t := stateTime(state); !t.IsZero() && t.Before(cutoff) {
				prunedDelegations[id] = true
			}
		}
		if len(prunedDelegations) > 0 {
			next := s.Records[:0]
			for _, record := range s.Records {
				if !prunedDelegations[record.DelegationID] {
					next = append(next, record)
				}
			}
			s.Records = next
		}
	}
	for id, outbox := range s.Outbox {
		if prunedDelegations[outbox.DelegationID] || pruneOutboxRecord(outbox, cutoff) {
			delete(s.Outbox, id)
		}
	}
	for id := range s.Routes {
		if prunedDelegations[id] {
			delete(s.Routes, id)
		}
	}
	for id, thread := range s.RemoteThreads {
		expiresAt := parseRecordTime(thread.ExpiresAt)
		updatedAt := parseRecordTime(firstNonEmptyString(thread.LastUsedAt, thread.UpdatedAt, thread.CreatedAt))
		if thread.State != RemoteThreadStateActive && ((!expiresAt.IsZero() && expiresAt.Before(cutoff)) || (!updatedAt.IsZero() && updatedAt.Before(cutoff))) {
			delete(s.RemoteThreads, id)
		}
	}
	for id, execution := range s.Executions {
		if prunedDelegations[id] || (terminalExecutionStatus(execution.Status) && parseRecordTime(execution.UpdatedAt).Before(cutoff)) {
			delete(s.Executions, id)
		}
	}
	for chatID, cursor := range s.InboxCursors {
		updatedAt := parseRecordTime(cursor.UpdatedAt)
		if !updatedAt.IsZero() && updatedAt.Before(cutoff) {
			delete(s.InboxCursors, chatID)
		}
	}
	for chatID, backoff := range s.InboxBackoffs {
		blockedUntil := parseRecordTime(backoff.BlockedUntil)
		updatedAt := parseRecordTime(backoff.UpdatedAt)
		if (!blockedUntil.IsZero() && blockedUntil.Before(cutoff)) || (!updatedAt.IsZero() && updatedAt.Before(cutoff)) {
			delete(s.InboxBackoffs, chatID)
		}
	}
}

func newStore() Store {
	return Store{
		SchemaVersion: 1,
		Routes:        map[string]Route{},
		RemoteThreads: map[string]RemoteThread{},
		Executions:    map[string]ExecutionFence{},
		Outbox:        map[string]OutboxRecord{},
		InboxCursors:  map[string]InboxCursor{},
		InboxBackoffs: map[string]InboxBackoff{},
	}
}
