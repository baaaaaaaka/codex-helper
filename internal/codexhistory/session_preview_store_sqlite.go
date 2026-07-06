package codexhistory

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	sqlite "modernc.org/sqlite"
)

// The source session JSONL remains authoritative. This database stores only
// rebuildable preview acceleration: normalized visible messages plus the small
// amount of dedupe state that cannot be recovered from those messages.

const (
	sessionPreviewSQLiteSchemaVersion = 1
	sessionPreviewSQLiteMaxSessions   = 1024
	sessionPreviewSQLiteBusyMillis    = 100
)

var (
	errSessionPreviewSQLiteConflict      = errors.New("session preview sqlite write conflict")
	errSessionPreviewSQLiteSchemaVersion = errors.New("unsupported session preview sqlite schema version")

	sessionPreviewAppendFlushBytes int64            = 64 * 1024
	sessionPreviewAppendFlushDelay                  = 30 * time.Second
	sessionPreviewNow              func() time.Time = time.Now
	sessionPreviewSQLiteCommitHook func(string)
)

type sessionPreviewSQLiteStore struct {
	path string
	db   *sql.DB
}

type sessionPreviewSQLiteEntry struct {
	sessionID   int64
	generation  int64
	nextOrdinal int64

	fileKey        fileCacheKey
	filterVersion  string
	offset         int64
	prefixTailHash string
	prefixTailSize int64
	messages       []Message
	text           string
	seen           *messageSeenState
}

type sessionPreviewSeenDelta struct {
	sourceKeys []string
	fallback   []persistentSessionPreviewFallbackMessage
}

var sessionPreviewSQLiteState struct {
	mu   sync.Mutex
	path string
	db   *sql.DB
}

var sessionPreviewAppendState struct {
	mu         sync.Mutex
	dirtySince map[string]time.Time
}

func readSessionPreviewCacheValueSQLite(filePath string, wantMessages bool) ([]Message, string, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		if store, storeErr := currentSessionPreviewSQLiteStore(); storeErr == nil {
			_ = store.delete(filePath)
		}
		return nil, "", err
	}

	store, err := currentSessionPreviewSQLiteStore()
	if err != nil {
		return nil, "", err
	}

	for attempt := 0; attempt < 3; attempt++ {
		entry, ok, err := store.load(filePath, wantMessages, false)
		if err != nil {
			return nil, "", err
		}
		if ok && entry.filterVersion == sessionPreviewFilterVersion {
			if matchesFileInfo(filePath, info, entry.fileKey) {
				return sessionPreviewResult(entry.messages, entry.text, wantMessages)
			}
			if canAppendSessionPreviewSQLite(filePath, info, entry) {
				completeOffset, complete := sessionPreviewCompleteOffset(filePath, info)
				if !complete || completeOffset < info.Size() || completeOffset < entry.offset {
					return readSessionPreviewUncached(filePath)
				}

				if err := store.loadSeenState(&entry); err != nil {
					return nil, "", err
				}
				seenBefore := cloneMessageSeenState(entry.seen)
				tail, readErr := readSessionMessagesWindow(
					filePath,
					entry.offset,
					completeOffset-entry.offset,
					0,
					isPreviewMessage,
					entry.seen,
				)
				if readErr != nil {
					return nil, "", readErr
				}
				messages := append(append([]Message(nil), entry.messages...), tail...)
				text := appendPreviewText(entry.text, FormatPreviewMessages(tail, 0))
				deltaBytes := completeOffset - entry.offset
				if shouldFlushSessionPreviewAppend(filePath, deltaBytes) {
					hash, hashSize, _ := sessionPreviewPrefixTailHash(filePath, completeOffset)
					delta := diffMessageSeenState(seenBefore, entry.seen)
					appendErr := store.append(entry, newFileCacheKey(filePath, info), completeOffset, hash, hashSize, tail, delta)
					switch {
					case errors.Is(appendErr, errSessionPreviewSQLiteConflict):
						continue
					case appendErr == nil:
						markSessionPreviewAppendFlushed(filePath)
					}
				}
				return sessionPreviewResult(messages, text, wantMessages)
			}
		}

		completeOffset, complete := sessionPreviewCompleteOffset(filePath, info)
		if !complete || completeOffset < info.Size() {
			return readSessionPreviewUncached(filePath)
		}
		seen := newMessageSeenState()
		messages, readErr := readSessionMessagesWindow(filePath, 0, completeOffset, 0, isPreviewMessage, seen)
		if readErr != nil {
			return nil, "", readErr
		}
		hash, hashSize, _ := sessionPreviewPrefixTailHash(filePath, completeOffset)
		if err := store.replace(filePath, newFileCacheKey(filePath, info), completeOffset, hash, hashSize, messages, seen); err == nil {
			markSessionPreviewAppendFlushed(filePath)
		}
		return sessionPreviewResult(messages, "", wantMessages)
	}

	return readSessionPreviewUncached(filePath)
}

func sessionPreviewResult(messages []Message, text string, wantMessages bool) ([]Message, string, error) {
	if text == "" && len(messages) > 0 {
		text = FormatPreviewMessages(messages, 0)
	}
	if wantMessages {
		return messages, text, nil
	}
	return nil, text, nil
}

func canAppendSessionPreviewSQLite(path string, info os.FileInfo, entry sessionPreviewSQLiteEntry) bool {
	return canAppendPersistentSessionPreview(path, info, persistentSessionPreviewEntry{
		FileCacheKey:   entry.fileKey,
		FilterVersion:  entry.filterVersion,
		Offset:         entry.offset,
		PrefixTailHash: entry.prefixTailHash,
		PrefixTailSize: entry.prefixTailSize,
	})
}

func shouldFlushSessionPreviewAppend(path string, deltaBytes int64) bool {
	if deltaBytes <= 0 {
		return false
	}
	path = filepath.Clean(path)
	now := sessionPreviewNow()
	sessionPreviewAppendState.mu.Lock()
	defer sessionPreviewAppendState.mu.Unlock()
	if sessionPreviewAppendState.dirtySince == nil {
		sessionPreviewAppendState.dirtySince = make(map[string]time.Time)
	}
	first, ok := sessionPreviewAppendState.dirtySince[path]
	if !ok {
		first = now
		sessionPreviewAppendState.dirtySince[path] = first
	}
	return sessionPreviewAppendFlushBytes <= 0 || deltaBytes >= sessionPreviewAppendFlushBytes ||
		sessionPreviewAppendFlushDelay <= 0 || now.Sub(first) >= sessionPreviewAppendFlushDelay
}

func markSessionPreviewAppendFlushed(path string) {
	sessionPreviewAppendState.mu.Lock()
	delete(sessionPreviewAppendState.dirtySince, filepath.Clean(path))
	sessionPreviewAppendState.mu.Unlock()
}

func cloneMessageSeenState(in *messageSeenState) *messageSeenState {
	out := newMessageSeenState()
	if in == nil {
		return out
	}
	for key, value := range in.sourceKeys {
		out.sourceKeys[key] = value
	}
	for key, value := range in.fallbackTimes {
		out.fallbackTimes[key] = value
	}
	for key, value := range in.fallbackSourceKind {
		out.fallbackSourceKind[key] = value
	}
	return out
}

func diffMessageSeenState(before *messageSeenState, after *messageSeenState) sessionPreviewSeenDelta {
	delta := sessionPreviewSeenDelta{}
	if after == nil {
		return delta
	}
	for key := range after.sourceKeys {
		if before == nil || !before.sourceKeys[key] {
			delta.sourceKeys = append(delta.sourceKeys, key)
		}
	}
	sort.Strings(delta.sourceKeys)
	for key, timestamp := range after.fallbackTimes {
		previous, ok := time.Time{}, false
		if before != nil {
			previous, ok = before.fallbackTimes[key]
		}
		sourceKind := after.fallbackSourceKind[key]
		if ok && previous.Equal(timestamp) && before.fallbackSourceKind[key] == sourceKind {
			continue
		}
		delta.fallback = append(delta.fallback, persistentSessionPreviewFallbackMessage{
			Key:        key,
			Timestamp:  timestamp,
			SourceKind: sourceKind,
		})
	}
	sort.Slice(delta.fallback, func(i, j int) bool { return delta.fallback[i].Key < delta.fallback[j].Key })
	return delta
}

func currentSessionPreviewSQLiteStore() (*sessionPreviewSQLiteStore, error) {
	path, err := sessionPreviewSQLiteFile()
	if err != nil {
		return nil, err
	}

	sessionPreviewSQLiteState.mu.Lock()
	defer sessionPreviewSQLiteState.mu.Unlock()
	if sessionPreviewSQLiteState.db != nil && sessionPreviewSQLiteState.path == path {
		return &sessionPreviewSQLiteStore{path: path, db: sessionPreviewSQLiteState.db}, nil
	}
	if sessionPreviewSQLiteState.db != nil {
		_ = sessionPreviewSQLiteState.db.Close()
		sessionPreviewSQLiteState.db = nil
		sessionPreviewSQLiteState.path = ""
	}

	db, err := openSessionPreviewSQLite(path)
	if err != nil && isRebuildableSessionPreviewSQLiteError(err) {
		_ = quarantineSessionPreviewSQLite(path)
		db, err = openSessionPreviewSQLite(path)
	}
	if err != nil {
		return nil, err
	}
	sessionPreviewSQLiteState.path = path
	sessionPreviewSQLiteState.db = db
	return &sessionPreviewSQLiteStore{path: path, db: db}, nil
}

func sessionPreviewSQLiteFile() (string, error) {
	dir, err := persistentCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "session_preview_cache.sqlite3"), nil
}

func openSessionPreviewSQLite(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", sessionPreviewSQLiteFileURI(path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, statement := range []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA synchronous = NORMAL`,
		fmt.Sprintf(`PRAGMA busy_timeout = %d`, sessionPreviewSQLiteBusyMillis),
		`PRAGMA temp_store = MEMORY`,
		`PRAGMA foreign_keys = ON`,
		`PRAGMA wal_autocheckpoint = 1000`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if err := initializeSessionPreviewSQLiteSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	_ = os.Chmod(path, 0o600)
	for _, suffix := range []string{"-wal", "-shm"} {
		_ = os.Chmod(path+suffix, 0o600)
	}
	return db, nil
}

func initializeSessionPreviewSQLiteSchema(db *sql.DB) error {
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version != 0 && version != sessionPreviewSQLiteSchemaVersion {
		return fmt.Errorf("%w: got %d want %d", errSessionPreviewSQLiteSchemaVersion, version, sessionPreviewSQLiteSchemaVersion)
	}
	if version == 0 {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, statement := range []string{
			`CREATE TABLE IF NOT EXISTS preview_session (
				id INTEGER PRIMARY KEY,
				path TEXT NOT NULL UNIQUE,
				filter_version TEXT NOT NULL,
				generation INTEGER NOT NULL,
				parsed_offset INTEGER NOT NULL,
				prefix_tail_hash TEXT NOT NULL,
				prefix_tail_size INTEGER NOT NULL,
				source_size INTEGER NOT NULL,
				source_mtime_ns INTEGER NOT NULL,
				source_mode INTEGER NOT NULL,
				has_file_id INTEGER NOT NULL,
				source_dev TEXT NOT NULL,
				source_ino TEXT NOT NULL,
				has_ctime INTEGER NOT NULL,
				source_ctime_ns INTEGER NOT NULL,
				next_ordinal INTEGER NOT NULL,
				last_write_ns INTEGER NOT NULL
			) STRICT`,
			`CREATE TABLE IF NOT EXISTS preview_message (
				session_id INTEGER NOT NULL REFERENCES preview_session(id) ON DELETE CASCADE,
				generation INTEGER NOT NULL,
				ordinal INTEGER NOT NULL,
				role TEXT NOT NULL,
				content TEXT NOT NULL,
				timestamp_ns INTEGER NOT NULL,
				source_id TEXT NOT NULL,
				PRIMARY KEY (session_id, generation, ordinal)
			) STRICT, WITHOUT ROWID`,
			`CREATE TABLE IF NOT EXISTS preview_seen_source (
				session_id INTEGER NOT NULL REFERENCES preview_session(id) ON DELETE CASCADE,
				generation INTEGER NOT NULL,
				source_key TEXT NOT NULL,
				PRIMARY KEY (session_id, generation, source_key)
			) STRICT, WITHOUT ROWID`,
			`CREATE TABLE IF NOT EXISTS preview_seen_fallback (
				session_id INTEGER NOT NULL REFERENCES preview_session(id) ON DELETE CASCADE,
				generation INTEGER NOT NULL,
				fallback_key TEXT NOT NULL,
				timestamp_ns INTEGER NOT NULL,
				source_kind TEXT NOT NULL,
				PRIMARY KEY (session_id, generation, fallback_key)
			) STRICT, WITHOUT ROWID`,
			fmt.Sprintf(`PRAGMA user_version = %d`, sessionPreviewSQLiteSchemaVersion),
		} {
			if _, err := tx.Exec(statement); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	var table string
	return db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'preview_session'`).Scan(&table)
}

func sessionPreviewSQLiteFileURI(path string) string {
	slash := filepath.ToSlash(path)
	u := &url.URL{Scheme: "file", Path: slash}
	if runtime.GOOS == "windows" {
		if strings.HasPrefix(slash, "//") {
			trimmed := strings.TrimLeft(slash, "/")
			host, rest, ok := strings.Cut(trimmed, "/")
			if ok {
				u.Host = host
				u.Path = "/" + rest
			}
		}
		if len(slash) >= 2 && slash[1] == ':' {
			u.Path = "/" + slash
		}
	}
	query := u.Query()
	for _, pragma := range []string{
		"synchronous(NORMAL)",
		fmt.Sprintf("busy_timeout(%d)", sessionPreviewSQLiteBusyMillis),
		"temp_store(MEMORY)",
		"foreign_keys(ON)",
		"wal_autocheckpoint(1000)",
	} {
		query.Add("_pragma", pragma)
	}
	u.RawQuery = query.Encode()
	return u.String()
}

func isRebuildableSessionPreviewSQLiteError(err error) bool {
	if errors.Is(err, errSessionPreviewSQLiteSchemaVersion) {
		return true
	}
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	switch sqliteErr.Code() & 0xff {
	case 11, 26: // SQLITE_CORRUPT, SQLITE_NOTADB.
		return true
	default:
		return false
	}
}

func quarantineSessionPreviewSQLite(path string) error {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	quarantine := fmt.Sprintf("%s.invalid-%d", path, time.Now().UnixNano())
	if previous, _ := filepath.Glob(path + ".invalid-*"); len(previous) > 0 {
		for _, candidate := range previous {
			_ = os.Remove(candidate)
		}
	}
	if err := os.Rename(path, quarantine); err != nil {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}
	return nil
}

func (store *sessionPreviewSQLiteStore) load(path string, wantMessages bool, wantSeen bool) (sessionPreviewSQLiteEntry, bool, error) {
	var entry sessionPreviewSQLiteEntry
	var hasFileID, hasCtime int
	var dev, ino string
	err := store.db.QueryRow(`SELECT
		id, generation, filter_version, parsed_offset, prefix_tail_hash, prefix_tail_size,
		source_size, source_mtime_ns, source_mode, has_file_id, source_dev, source_ino,
		has_ctime, source_ctime_ns, next_ordinal
		FROM preview_session WHERE path = ?`, filepath.Clean(path)).Scan(
		&entry.sessionID,
		&entry.generation,
		&entry.filterVersion,
		&entry.offset,
		&entry.prefixTailHash,
		&entry.prefixTailSize,
		&entry.fileKey.Size,
		&entry.fileKey.MtimeUnixNano,
		&entry.fileKey.Mode,
		&hasFileID,
		&dev,
		&ino,
		&hasCtime,
		&entry.fileKey.CtimeUnixNano,
		&entry.nextOrdinal,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sessionPreviewSQLiteEntry{}, false, nil
	}
	if err != nil {
		return sessionPreviewSQLiteEntry{}, false, err
	}
	entry.fileKey.HasFileID = hasFileID != 0
	entry.fileKey.HasCtime = hasCtime != 0
	entry.fileKey.Dev, _ = strconv.ParseUint(dev, 10, 64)
	entry.fileKey.Ino, _ = strconv.ParseUint(ino, 10, 64)

	if err := store.loadMessages(&entry, wantMessages); err != nil {
		return sessionPreviewSQLiteEntry{}, false, err
	}
	if wantSeen {
		if err := store.loadSeenState(&entry); err != nil {
			return sessionPreviewSQLiteEntry{}, false, err
		}
	}
	return entry, true, nil
}

func (store *sessionPreviewSQLiteStore) loadMessages(entry *sessionPreviewSQLiteEntry, wantMessages bool) error {
	if entry == nil {
		return errors.New("nil session preview sqlite entry")
	}
	if !wantMessages {
		rows, err := store.db.Query(`SELECT role, content
			FROM preview_message WHERE session_id = ? AND generation = ? ORDER BY ordinal`, entry.sessionID, entry.generation)
		if err != nil {
			return err
		}
		var builder strings.Builder
		wrote := false
		for rows.Next() {
			var role, content string
			if err := rows.Scan(&role, &content); err != nil {
				_ = rows.Close()
				return err
			}
			label := previewRoleLabel(role)
			content = strings.TrimSpace(content)
			if label == "" || content == "" {
				continue
			}
			if wrote {
				builder.WriteString("\n\n")
			}
			builder.WriteString(label)
			builder.WriteString(":\n")
			builder.WriteString(content)
			wrote = true
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
		entry.text = strings.TrimSpace(builder.String())
		return nil
	}

	rows, err := store.db.Query(`SELECT role, content, timestamp_ns, source_id
		FROM preview_message WHERE session_id = ? AND generation = ? ORDER BY ordinal`, entry.sessionID, entry.generation)
	if err != nil {
		return err
	}
	for rows.Next() {
		var msg Message
		var timestamp int64
		if err := rows.Scan(&msg.Role, &msg.Content, &timestamp, &msg.sourceID); err != nil {
			_ = rows.Close()
			return err
		}
		msg.Timestamp = sessionPreviewTimeFromUnixNano(timestamp)
		entry.messages = append(entry.messages, msg)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return rows.Err()
}

func (store *sessionPreviewSQLiteStore) loadSeenState(entry *sessionPreviewSQLiteEntry) error {
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
	return nil
}

func (store *sessionPreviewSQLiteStore) replace(
	path string,
	key fileCacheKey,
	offset int64,
	prefixHash string,
	prefixSize int64,
	messages []Message,
	seen *messageSeenState,
) error {
	tx, err := store.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var sessionID, generation int64
	err = tx.QueryRow(`INSERT INTO preview_session (
		path, filter_version, generation, parsed_offset, prefix_tail_hash, prefix_tail_size,
		source_size, source_mtime_ns, source_mode, has_file_id, source_dev, source_ino,
		has_ctime, source_ctime_ns, next_ordinal, last_write_ns
	) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(path) DO UPDATE SET
		filter_version = excluded.filter_version,
		generation = preview_session.generation + 1,
		parsed_offset = excluded.parsed_offset,
		prefix_tail_hash = excluded.prefix_tail_hash,
		prefix_tail_size = excluded.prefix_tail_size,
		source_size = excluded.source_size,
		source_mtime_ns = excluded.source_mtime_ns,
		source_mode = excluded.source_mode,
		has_file_id = excluded.has_file_id,
		source_dev = excluded.source_dev,
		source_ino = excluded.source_ino,
		has_ctime = excluded.has_ctime,
		source_ctime_ns = excluded.source_ctime_ns,
		next_ordinal = excluded.next_ordinal,
		last_write_ns = excluded.last_write_ns
	RETURNING id, generation`,
		filepath.Clean(path), sessionPreviewFilterVersion, offset, prefixHash, prefixSize,
		key.Size, key.MtimeUnixNano, key.Mode, boolInt(key.HasFileID), strconv.FormatUint(key.Dev, 10), strconv.FormatUint(key.Ino, 10),
		boolInt(key.HasCtime), key.CtimeUnixNano, len(messages), sessionPreviewNow().UnixNano(),
	).Scan(&sessionID, &generation)
	if err != nil {
		return err
	}
	if err := insertSessionPreviewMessages(tx, sessionID, generation, 0, messages); err != nil {
		return err
	}
	if err := insertSessionPreviewSeenState(tx, sessionID, generation, messages, seen); err != nil {
		return err
	}
	for _, table := range []string{"preview_message", "preview_seen_source", "preview_seen_fallback"} {
		if _, err := tx.Exec(`DELETE FROM `+table+` WHERE session_id = ? AND generation <> ?`, sessionID, generation); err != nil {
			return err
		}
	}
	if err := pruneSessionPreviewSQLite(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	notifySessionPreviewSQLiteCommit(store.path)
	return nil
}

func (store *sessionPreviewSQLiteStore) append(
	entry sessionPreviewSQLiteEntry,
	key fileCacheKey,
	offset int64,
	prefixHash string,
	prefixSize int64,
	messages []Message,
	seenDelta sessionPreviewSeenDelta,
) error {
	tx, err := store.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// The offset/ordinal compare-and-swap makes two TUI processes idempotent:
	// only the writer that observed the current tail can advance it.
	result, err := tx.Exec(`UPDATE preview_session SET
		parsed_offset = ?, prefix_tail_hash = ?, prefix_tail_size = ?, source_size = ?,
		source_mtime_ns = ?, source_mode = ?, has_file_id = ?, source_dev = ?, source_ino = ?,
		has_ctime = ?, source_ctime_ns = ?, next_ordinal = next_ordinal + ?, last_write_ns = ?
		WHERE id = ? AND generation = ? AND filter_version = ? AND parsed_offset = ? AND next_ordinal = ?`,
		offset, prefixHash, prefixSize, key.Size, key.MtimeUnixNano, key.Mode,
		boolInt(key.HasFileID), strconv.FormatUint(key.Dev, 10), strconv.FormatUint(key.Ino, 10),
		boolInt(key.HasCtime), key.CtimeUnixNano, len(messages), sessionPreviewNow().UnixNano(),
		entry.sessionID, entry.generation, sessionPreviewFilterVersion, entry.offset, entry.nextOrdinal,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return errSessionPreviewSQLiteConflict
	}
	if err := insertSessionPreviewMessages(tx, entry.sessionID, entry.generation, entry.nextOrdinal, messages); err != nil {
		return err
	}
	if err := insertSessionPreviewSeenDelta(tx, entry.sessionID, entry.generation, messages, seenDelta); err != nil {
		return err
	}
	if err := pruneSessionPreviewSQLite(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	notifySessionPreviewSQLiteCommit(store.path)
	return nil
}

func (store *sessionPreviewSQLiteStore) delete(path string) error {
	result, err := store.db.Exec(`DELETE FROM preview_session WHERE path = ?`, filepath.Clean(path))
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err == nil && rows > 0 {
		notifySessionPreviewSQLiteCommit(store.path)
	}
	return err
}

func insertSessionPreviewMessages(tx *sql.Tx, sessionID int64, generation int64, startOrdinal int64, messages []Message) error {
	statement, err := tx.Prepare(`INSERT INTO preview_message (
		session_id, generation, ordinal, role, content, timestamp_ns, source_id
	) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()
	for index, message := range messages {
		if _, err := statement.Exec(
			sessionID,
			generation,
			startOrdinal+int64(index),
			message.Role,
			message.Content,
			sessionPreviewUnixNano(message.Timestamp),
			message.sourceID,
		); err != nil {
			return err
		}
	}
	return nil
}

func insertSessionPreviewSeenState(tx *sql.Tx, sessionID int64, generation int64, messages []Message, seen *messageSeenState) error {
	if seen == nil {
		return nil
	}
	// Retained messages already persist their source IDs. Store only IDs from
	// mirrored records that dedupe intentionally skipped.
	retained := retainedSessionPreviewSourceKeys(messages)
	keys := make([]string, 0, len(seen.sourceKeys))
	for key := range seen.sourceKeys {
		if retained[key] {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := tx.Exec(`INSERT INTO preview_seen_source (session_id, generation, source_key) VALUES (?, ?, ?)`, sessionID, generation, key); err != nil {
			return err
		}
	}
	fallback := make([]persistentSessionPreviewFallbackMessage, 0, len(seen.fallbackTimes))
	for key, timestamp := range seen.fallbackTimes {
		fallback = append(fallback, persistentSessionPreviewFallbackMessage{
			Key:        key,
			Timestamp:  timestamp,
			SourceKind: seen.fallbackSourceKind[key],
		})
	}
	sort.Slice(fallback, func(i, j int) bool { return fallback[i].Key < fallback[j].Key })
	return insertSessionPreviewFallback(tx, sessionID, generation, fallback)
}

func insertSessionPreviewSeenDelta(tx *sql.Tx, sessionID int64, generation int64, messages []Message, delta sessionPreviewSeenDelta) error {
	retained := retainedSessionPreviewSourceKeys(messages)
	for _, key := range delta.sourceKeys {
		if retained[key] {
			continue
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO preview_seen_source (session_id, generation, source_key) VALUES (?, ?, ?)`, sessionID, generation, key); err != nil {
			return err
		}
	}
	return insertSessionPreviewFallback(tx, sessionID, generation, delta.fallback)
}

func retainedSessionPreviewSourceKeys(messages []Message) map[string]bool {
	retained := make(map[string]bool, len(messages))
	for _, message := range messages {
		if key := sourceMessageDedupKey(message.sourceID); key != "" {
			retained[key] = true
		}
	}
	return retained
}

func insertSessionPreviewFallback(tx *sql.Tx, sessionID int64, generation int64, fallback []persistentSessionPreviewFallbackMessage) error {
	for _, message := range fallback {
		if _, err := tx.Exec(`INSERT INTO preview_seen_fallback (
			session_id, generation, fallback_key, timestamp_ns, source_kind
		) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(session_id, generation, fallback_key) DO UPDATE SET
			timestamp_ns = excluded.timestamp_ns,
			source_kind = excluded.source_kind`,
			sessionID, generation, message.Key, sessionPreviewUnixNano(message.Timestamp), message.SourceKind,
		); err != nil {
			return err
		}
	}
	return nil
}

func pruneSessionPreviewSQLite(tx *sql.Tx) error {
	_, err := tx.Exec(`DELETE FROM preview_session WHERE id IN (
		SELECT id FROM preview_session ORDER BY last_write_ns DESC, id DESC LIMIT -1 OFFSET ?
	)`, sessionPreviewSQLiteMaxSessions)
	return err
}

func sessionPreviewUnixNano(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}

func sessionPreviewTimeFromUnixNano(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func notifySessionPreviewSQLiteCommit(path string) {
	if sessionPreviewSQLiteCommitHook != nil {
		sessionPreviewSQLiteCommitHook(path)
	}
}

func resetSessionPreviewSQLiteForTest() {
	sessionPreviewSQLiteState.mu.Lock()
	if sessionPreviewSQLiteState.db != nil {
		_ = sessionPreviewSQLiteState.db.Close()
	}
	sessionPreviewSQLiteState.path = ""
	sessionPreviewSQLiteState.db = nil
	sessionPreviewSQLiteState.mu.Unlock()

	sessionPreviewAppendState.mu.Lock()
	sessionPreviewAppendState.dirtySince = nil
	sessionPreviewAppendState.mu.Unlock()
}
