package codexhistory

import (
	"database/sql"
	"os"
	"strings"
)

// Parse once before consulting dedupe state. A hidden record needs no lookup;
// candidates must retain their order, including locally repeated records.
func (store *sessionPreviewSQLiteStore) readAppendCandidates(f *os.File, entry sessionPreviewSQLiteEntry, offset, size int64) ([]Message, sessionPreviewSeenDelta, error) {
	var candidates []Message
	err := visitSessionMessagesWindowFrom(f, offset, size, projectPreviewMessage, func(msg Message) { candidates = append(candidates, msg) })
	if err != nil || len(candidates) == 0 {
		return nil, sessionPreviewSeenDelta{}, err
	}
	seen, err := store.loadCandidateSeen(entry, candidates)
	if err != nil {
		return nil, sessionPreviewSeenDelta{}, err
	}
	// Only uncommitted state belongs to the in-process append base. It wins
	// over committed values, including the latest fallback timestamp/kind.
	for _, key := range entry.pendingDelta.sourceKeys {
		seen.sourceKeys[key] = true
	}
	for _, value := range entry.pendingDelta.fallback {
		seen.fallbackTimes[value.Key] = value.Timestamp
		seen.fallbackSourceKind[value.Key] = value.SourceKind
	}
	var delta sessionPreviewSeenDelta
	seen.delta = &delta
	out := candidates[:0]
	for _, msg := range candidates {
		if markMessageSeen(msg, seen, true) {
			out = append(out, msg)
		}
	}
	return out, delta, nil
}

func (store *sessionPreviewSQLiteStore) loadCandidateSeen(entry sessionPreviewSQLiteEntry, messages []Message) (*messageSeenState, error) {
	seen := newMessageSeenState()
	sources, fallback := map[string]bool{}, map[string]bool{}
	for _, msg := range messages {
		if msg.sourceID != "" {
			sources[msg.sourceID] = true
		}
		if key := fallbackMessageDedupKey(msg, true); key != "" {
			fallback[key] = true
		}
	}
	tx, err := store.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	queries := []struct {
		query string
		keys  map[string]bool
		kind  int
	}{
		{`SELECT source_id FROM preview_message WHERE session_id=? AND generation=? AND source_id<>'' AND source_id IN (`, sources, 0},
		{`SELECT source_key FROM preview_seen_source WHERE session_id=? AND generation=? AND source_key IN (`, nil, 1},
		{`SELECT fallback_key,timestamp_ns,source_kind FROM preview_seen_fallback WHERE session_id=? AND generation=? AND fallback_key IN (`, fallback, 2},
	}
	queries[1].keys = make(map[string]bool, len(sources))
	for source := range sources {
		queries[1].keys[sourceMessageDedupKey(source)] = true
	}
	for _, query := range queries {
		keys := make([]string, 0, len(query.keys))
		for key := range query.keys {
			keys = append(keys, key)
		}
		for start := 0; start < len(keys); start += 256 {
			end := min(start+256, len(keys))
			args := make([]any, 2, end-start+2)
			args[0], args[1] = entry.sessionID, entry.generation
			for _, key := range keys[start:end] {
				args = append(args, key)
			}
			rows, err := tx.Query(query.query+strings.TrimSuffix(strings.Repeat("?,", end-start), ",")+")", args...)
			if err != nil {
				return nil, err
			}
			if err := scanPreviewCandidateSeen(rows, query.kind, seen); err != nil {
				return nil, err
			}
		}
	}
	if err := checkPreviewRevision(tx, entry); err != nil {
		return nil, err
	}
	return seen, nil
}

func scanPreviewCandidateSeen(rows *sql.Rows, kind int, seen *messageSeenState) error {
	defer rows.Close()
	for rows.Next() {
		var key, sourceKind string
		var timestamp int64
		if kind == 2 {
			if err := rows.Scan(&key, &timestamp, &sourceKind); err != nil {
				return err
			}
			seen.fallbackTimes[key] = sessionPreviewTimeFromUnixNano(timestamp)
			seen.fallbackSourceKind[key] = sourceKind
		} else {
			if err := rows.Scan(&key); err != nil {
				return err
			}
			if kind == 0 {
				key = sourceMessageDedupKey(key)
			}
			seen.sourceKeys[key] = true
		}
	}
	return rows.Err()
}
