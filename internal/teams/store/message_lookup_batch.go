package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
)

// MessageLookupBatch resolves a bounded poll window from one store view.
// Missing IDs are returned as zero-value lookups. The caller must retain the
// registry and marker checks for those IDs; this API is only a read-side
// batching primitive and does not change dedupe authority.
func (s *Store) MessageLookupBatch(ctx context.Context, chatID string, teamsMessageIDs []string) (map[string]MessageLookup, error) {
	chatID = strings.TrimSpace(chatID)
	uniqueIDs := make([]string, 0, len(teamsMessageIDs))
	seen := make(map[string]struct{}, len(teamsMessageIDs))
	for _, messageID := range teamsMessageIDs {
		messageID = strings.TrimSpace(messageID)
		if messageID == "" {
			continue
		}
		if _, ok := seen[messageID]; ok {
			continue
		}
		seen[messageID] = struct{}{}
		uniqueIDs = append(uniqueIDs, messageID)
	}
	out := make(map[string]MessageLookup, len(uniqueIDs))
	if s == nil || chatID == "" || len(uniqueIDs) == 0 {
		return out, nil
	}

	if sqliteOut, handled, err := s.messageLookupSQLiteBatch(ctx, chatID, uniqueIDs); handled || err != nil {
		return sqliteOut, err
	}

	err := s.withStateLock(ctx, func() error {
		stamp, err := stateFileStampForPath(s.path)
		if err != nil {
			return err
		}
		cached := true
		for _, messageID := range uniqueIDs {
			lookup, ok := s.messageLookup.lookup(stamp, chatID, messageID)
			if !ok {
				cached = false
				break
			}
			out[messageID] = lookup
		}
		if cached {
			return nil
		}
		state, err := s.loadUnlocked(ctx)
		if err != nil {
			s.invalidateMessageLookupCacheLocked()
			return err
		}
		// Keep the legacy JSON path on the same warm-cache behavior as the
		// single-message lookup. A poll window should not decode the complete
		// state once per message after the first cache miss.
		s.replaceMessageLookupCacheFromStateLocked(state)
		for _, messageID := range uniqueIDs {
			lookup, ok := s.messageLookup.lookup(s.messageLookup.Stamp, chatID, messageID)
			if ok {
				out[messageID] = lookup
				continue
			}
			out[messageID] = messageLookupLocked(&state, chatID, messageID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) messageLookupSQLiteBatch(ctx context.Context, chatID string, messageIDs []string) (map[string]MessageLookup, bool, error) {
	var out map[string]MessageLookup
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		out, err = messageLookupSQLiteBatchDirectCompat(ctx, tx, chatID, messageIDs)
		if err != nil {
			return err
		}
		handled = true
		return tx.Commit()
	})
	return out, handled, err
}

type messageLookupBatchQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func messageLookupSQLiteBatchDirectCompat(ctx context.Context, db messageLookupBatchQueryer, chatID string, messageIDs []string) (map[string]MessageLookup, error) {
	out := make(map[string]MessageLookup, len(messageIDs))
	for _, messageID := range messageIDs {
		messageID = strings.TrimSpace(messageID)
		if messageID != "" {
			out[messageID] = MessageLookup{}
		}
	}
	if strings.TrimSpace(chatID) == "" || len(out) == 0 {
		return out, nil
	}

	selectedProvenance := make(map[string]MessageProvenanceRecord, len(out))
	selectedCanonical := make(map[string]bool, len(out))
	ids := make([]string, 0, len(out))
	for messageID := range out {
		ids = append(ids, messageID)
	}

	for start := 0; start < len(ids); start += sqliteQueryParameterBatchSize {
		end := start + sqliteQueryParameterBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		windowIDs := ids[start:end]
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(windowIDs)), ",")

		canonicalIDs := make(map[string]string, len(windowIDs))
		canonicalArgs := make([]any, 0, len(windowIDs))
		for _, messageID := range windowIDs {
			canonicalID := messageProvenanceID(chatID, messageID)
			canonicalIDs[canonicalID] = messageID
			canonicalArgs = append(canonicalArgs, canonicalID)
		}
		rows, err := db.QueryContext(ctx, `SELECT id, json FROM message_provenance WHERE id IN (`+placeholders+`) ORDER BY id`, canonicalArgs...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			var raw []byte
			if err := rows.Scan(&id, &raw); err != nil {
				_ = rows.Close()
				return nil, err
			}
			messageID, ok := canonicalIDs[id]
			if !ok {
				continue
			}
			var record MessageProvenanceRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				_ = rows.Close()
				return nil, err
			}
			selectedProvenance[messageID] = record
			selectedCanonical[messageID] = true
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}

		fallbackArgs := make([]any, 0, len(windowIDs)+1)
		fallbackArgs = append(fallbackArgs, chatID)
		for _, messageID := range windowIDs {
			fallbackArgs = append(fallbackArgs, messageID)
		}
		rows, err = db.QueryContext(ctx, `SELECT id, teams_message_id, json FROM message_provenance WHERE teams_chat_id = ? AND teams_message_id IN (`+placeholders+`) ORDER BY id`, fallbackArgs...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, messageID string
			var raw []byte
			if err := rows.Scan(&id, &messageID, &raw); err != nil {
				_ = rows.Close()
				return nil, err
			}
			if _, ok := out[messageID]; !ok {
				continue
			}
			var record MessageProvenanceRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				_ = rows.Close()
				return nil, err
			}
			canonical := id == messageProvenanceID(chatID, messageID)
			if _, exists := selectedProvenance[messageID]; !exists || (canonical && !selectedCanonical[messageID]) {
				selectedProvenance[messageID] = record
				selectedCanonical[messageID] = canonical
			}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}

		inboundSeen := make(map[string]bool, len(windowIDs))
		inboundArgs := append([]any{chatID}, fallbackArgs[1:]...)
		rows, err = db.QueryContext(ctx, `SELECT teams_message_id, json FROM inbound_events WHERE teams_chat_id = ? AND teams_message_id IN (`+placeholders+`)`, inboundArgs...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var messageID string
			var raw []byte
			if err := rows.Scan(&messageID, &raw); err != nil {
				_ = rows.Close()
				return nil, err
			}
			if _, ok := out[messageID]; !ok || inboundSeen[messageID] {
				continue
			}
			var event InboundEvent
			if err := json.Unmarshal(raw, &event); err != nil {
				_ = rows.Close()
				return nil, err
			}
			lookup := out[messageID]
			lookup.HasInbound = true
			lookup.InboundNeedsQueue = inboundEventNeedsQueue(event)
			out[messageID] = lookup
			inboundSeen[messageID] = true
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}

		outboxArgs := append([]any{}, fallbackArgs...)
		outboxArgs = append(outboxArgs, string(OutboxStatusAccepted), string(OutboxStatusSent))
		rows, err = db.QueryContext(ctx, `SELECT teams_message_id FROM outbox_messages WHERE teams_chat_id = ? AND teams_message_id IN (`+placeholders+`) AND status IN (?, ?)`, outboxArgs...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var messageID string
			if err := rows.Scan(&messageID); err != nil {
				_ = rows.Close()
				return nil, err
			}
			if _, ok := out[messageID]; !ok {
				continue
			}
			lookup := out[messageID]
			lookup.HasDeliveredOutbox = true
			out[messageID] = lookup
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	for messageID, record := range selectedProvenance {
		lookup := out[messageID]
		lookup.Provenance = record
		lookup.HasProvenance = true
		switch strings.TrimSpace(record.Origin) {
		case MessageOriginUserInbound:
			lookup.HasInbound = true
		case MessageOriginHelperOutbox:
			lookup.HasDeliveredOutbox = true
		}
		out[messageID] = lookup
	}
	return out, nil
}
