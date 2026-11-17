package codexhistory

import "errors"

// Full state inspection is test-only; the runtime queries candidate keys.
type previewLoadedEntry struct {
	sessionPreviewSQLiteEntry
	seen *messageSeenState
}

func (store *sessionPreviewSQLiteStore) load(path string, wantMessages bool, wantSeen bool) (previewLoadedEntry, bool, error) {
	header, ok, err := store.loadHeader(path)
	entry := previewLoadedEntry{sessionPreviewSQLiteEntry: header}
	if err != nil || !ok {
		return entry, ok, err
	}
	if err := store.loadMessages(&entry.sessionPreviewSQLiteEntry, wantMessages); err != nil {
		return previewLoadedEntry{}, false, err
	}
	if wantSeen {
		if err := store.loadSeenState(&entry); err != nil {
			return previewLoadedEntry{}, false, err
		}
	}
	return entry, true, nil
}

func (store *sessionPreviewSQLiteStore) loadSeenState(entry *previewLoadedEntry) error {
	if entry == nil {
		return errors.New("nil session preview sqlite entry")
	}
	entry.seen = newMessageSeenState()
	rows, err := store.db.Query(`SELECT source_id FROM preview_message
		WHERE session_id = ? AND generation = ? AND source_id <> ''`, entry.sessionID, entry.generation)
	if err != nil {
		return err
	}
	for rows.Next() {
		var sourceID string
		if err := rows.Scan(&sourceID); err != nil {
			_ = rows.Close()
			return err
		}
		if key := sourceMessageDedupKey(sourceID); key != "" {
			entry.seen.sourceKeys[key] = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	rows, err = store.db.Query(`SELECT source_key FROM preview_seen_source WHERE session_id = ? AND generation = ?`, entry.sessionID, entry.generation)
	if err != nil {
		return err
	}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			_ = rows.Close()
			return err
		}
		entry.seen.sourceKeys[key] = true
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	rows, err = store.db.Query(`SELECT fallback_key, timestamp_ns, source_kind
		FROM preview_seen_fallback WHERE session_id = ? AND generation = ?`, entry.sessionID, entry.generation)
	if err != nil {
		return err
	}
	for rows.Next() {
		var key, sourceKind string
		var timestamp int64
		if err := rows.Scan(&key, &timestamp, &sourceKind); err != nil {
			_ = rows.Close()
			return err
		}
		entry.seen.fallbackTimes[key] = sessionPreviewTimeFromUnixNano(timestamp)
		entry.seen.fallbackSourceKind[key] = sourceKind
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for key := range entry.seen.sourceKeys {
		entry.seen.bytes += int64(len(key) + 64)
	}
	for key, kind := range entry.seen.fallbackSourceKind {
		entry.seen.bytes += int64(len(key) + len(kind) + 128)
	}
	return store.checkRevision(entry.sessionPreviewSQLiteEntry)
}
