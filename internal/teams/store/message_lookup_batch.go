package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
		nativeOutboxReady, err := s.sqliteOutboxProjectionNativeMarkerReady(ctx, db, sqliteOutboxProjectionTrustKey)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		out, err = messageLookupSQLiteBatchDirectCompat(ctx, tx, chatID, messageIDs, nativeOutboxReady)
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

func messageLookupSQLiteBatchDirectCompat(ctx context.Context, db messageLookupBatchQueryer, chatID string, messageIDs []string, nativeOutboxReady bool) (map[string]MessageLookup, error) {
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

		delivered, err := sqliteMessageLookupDeliveredOutbox(ctx, db, chatID, windowIDs, nativeOutboxReady)
		if err != nil {
			return nil, err
		}
		for messageID := range delivered {
			lookup := out[messageID]
			lookup.HasDeliveredOutbox = true
			out[messageID] = lookup
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

// sqliteMessageLookupDeliveredOutbox is the dedupe read boundary for helper
// messages already present in Teams.  A trusted projection can use the
// indexed scalar identity, but every selected row is still checked against
// the canonical JSON before it authorizes a delivered result.  If the marker
// is not trusted, the scalar probe is retained only as a corruption detector
// and a canonical JSON query supplies the complete answer, including legacy
// rows whose compatibility columns are blank or stale.
func sqliteMessageLookupDeliveredOutbox(ctx context.Context, db messageLookupBatchQueryer, chatID string, messageIDs []string, nativeOutboxReady bool) (map[string]bool, error) {
	chatID = strings.TrimSpace(chatID)
	wanted := make(map[string]struct{}, len(messageIDs))
	for _, messageID := range messageIDs {
		messageID = strings.TrimSpace(messageID)
		if messageID != "" {
			wanted[messageID] = struct{}{}
		}
	}
	delivered := make(map[string]bool, len(wanted))
	if chatID == "" || len(wanted) == 0 {
		return delivered, nil
	}
	ids := make([]string, 0, len(wanted))
	for messageID := range wanted {
		ids = append(ids, messageID)
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, len(ids)+3)
	args = append(args, chatID)
	for _, messageID := range ids {
		args = append(args, messageID)
	}
	args = append(args, string(OutboxStatusAccepted), string(OutboxStatusSent))
	rows, err := db.QueryContext(ctx, `SELECT `+sqliteOutboxProjectionSelect("o")+` FROM outbox_messages o
WHERE o.teams_chat_id = ? AND o.teams_message_id IN (`+placeholders+`) AND o.status IN (?, ?)`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var row sqliteOutboxProjectionRow
		if err := scanSQLiteOutboxProjectionRow(rows, &row); err != nil {
			_ = rows.Close()
			return nil, err
		}
		messageID := strings.TrimSpace(row.teamsMessageID.String)
		if _, ok := wanted[messageID]; !ok {
			continue
		}
		matched, valid := sqliteOutboxLookupRowMatches(row, chatID, messageID)
		if !valid {
			_ = rows.Close()
			return nil, fmt.Errorf("%w: delivered outbox row %q", ErrSQLiteOutboxProjectionUntrusted, strings.TrimSpace(row.id.String))
		}
		if !matched {
			_ = rows.Close()
			return nil, fmt.Errorf("%w: delivered outbox row %q scalar identity contradicts canonical JSON", ErrSQLiteOutboxProjectionUntrusted, strings.TrimSpace(row.id.String))
		}
		delivered[messageID] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if nativeOutboxReady {
		return delivered, nil
	}

	canonicalChatID := sqliteOutboxCanonicalTextSQL("o.json", "$.teams_chat_id", "o.teams_chat_id")
	canonicalMessageID := sqliteOutboxCanonicalTextSQL("o.json", "$.teams_message_id", "o.teams_message_id")
	canonicalStatus := sqliteOutboxCanonicalTextSQL("o.json", "$.status", "o.status")
	canonicalArgs := make([]any, 0, len(ids)+3)
	canonicalArgs = append(canonicalArgs, chatID)
	for _, messageID := range ids {
		canonicalArgs = append(canonicalArgs, messageID)
	}
	canonicalArgs = append(canonicalArgs, string(OutboxStatusAccepted), string(OutboxStatusSent))
	rows, err = db.QueryContext(ctx, `SELECT `+sqliteOutboxProjectionSelect("o")+` FROM outbox_messages o
WHERE json_valid(o.json)
  AND json_type(o.json, '$') = 'object'
  AND `+sqliteOutboxTopLevelKeysUniqueSQL("o")+`
  AND `+canonicalChatID+` = ?
  AND `+canonicalMessageID+` IN (`+placeholders+`)
  AND `+canonicalStatus+` IN (?, ?)`, canonicalArgs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var row sqliteOutboxProjectionRow
		if err := scanSQLiteOutboxProjectionRow(rows, &row); err != nil {
			_ = rows.Close()
			return nil, err
		}
		messageID, valid := sqliteOutboxLookupCanonicalMessageID(row)
		if !valid {
			_ = rows.Close()
			return nil, fmt.Errorf("%w: canonical delivered outbox row %q", ErrSQLiteOutboxProjectionUntrusted, strings.TrimSpace(row.id.String))
		}
		if _, ok := wanted[messageID]; !ok {
			continue
		}
		matched, valid := sqliteOutboxLookupRowMatches(row, chatID, messageID)
		if !valid || !matched {
			_ = rows.Close()
			return nil, fmt.Errorf("%w: canonical delivered outbox row %q does not satisfy lookup", ErrSQLiteOutboxProjectionUntrusted, strings.TrimSpace(row.id.String))
		}
		delivered[messageID] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return delivered, nil
}

func sqliteOutboxLookupCanonicalString(object map[string]json.RawMessage, field string, scalar sql.NullString) (string, bool, bool) {
	raw, present := object[field]
	if !present {
		if !scalar.Valid {
			return "", false, true
		}
		return strings.TrimSpace(scalar.String), false, true
	}
	if string(raw) == "null" {
		return "", true, true
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", true, false
	}
	return strings.TrimSpace(value), true, true
}

func sqliteOutboxLookupCanonicalMessageID(row sqliteOutboxProjectionRow) (string, bool) {
	// The SQLite primary-key value is part of the durable row identity.  A
	// canonical JSON id alone is not enough: a row with a NULL physical id can
	// otherwise be relabeled as an already-delivered message during the
	// compatibility lookup and suppress a future send.
	if !row.id.Valid || strings.TrimSpace(row.id.String) == "" {
		return "", false
	}
	object, ok := decodeSQLiteOutboxObjectWithoutDuplicateKeys(row.raw)
	if !ok {
		return "", false
	}
	messageID, _, ok := sqliteOutboxLookupCanonicalString(object, "teams_message_id", row.teamsMessageID)
	if !ok || messageID == "" {
		return "", false
	}
	return messageID, true
}

func sqliteOutboxLookupRowMatches(row sqliteOutboxProjectionRow, chatID string, messageID string) (bool, bool) {
	if !row.id.Valid || strings.TrimSpace(row.id.String) == "" {
		return false, false
	}
	object, ok := decodeSQLiteOutboxObjectWithoutDuplicateKeys(row.raw)
	if !ok {
		return false, false
	}
	id, idPresent, ok := sqliteOutboxLookupCanonicalString(object, "id", row.id)
	if !ok || id == "" {
		return false, false
	}
	if idPresent && strings.TrimSpace(row.id.String) != id {
		return false, false
	}
	canonicalChatID, _, ok := sqliteOutboxLookupCanonicalString(object, "teams_chat_id", row.teamsChatID)
	if !ok {
		return false, false
	}
	canonicalMessageID, _, ok := sqliteOutboxLookupCanonicalString(object, "teams_message_id", row.teamsMessageID)
	if !ok {
		return false, false
	}
	status, _, ok := sqliteOutboxLookupCanonicalString(object, "status", row.status)
	if !ok {
		return false, false
	}
	return canonicalChatID == strings.TrimSpace(chatID) &&
		canonicalMessageID == strings.TrimSpace(messageID) &&
		(status == string(OutboxStatusAccepted) || status == string(OutboxStatusSent)), true
}
