package codexhistory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	sqlite "modernc.org/sqlite"
)

const (
	catalogSQLiteApplicationID = 0x43585043 // "CXPC"
	catalogSQLiteBusyMillis    = 100
)

var (
	errCatalogSQLiteConflict = errors.New("catalog sqlite write conflict")
	errCatalogSQLiteSchema   = errors.New("invalid catalog sqlite schema")
	catalogSQLiteCommitHook  func(string)
)

type catalogSQLiteStore struct {
	path string
	db   *sql.DB
}

type catalogSessionMetaEntry struct {
	path           string
	fileKey        fileCacheKey
	parsedOffset   int64
	prefixTailHash string
	prefixTailSize int64
	meta           sessionFileMeta
}

type catalogHistorySourceEntry struct {
	path           string
	generation     int64
	fileKey        fileCacheKey
	parsedOffset   int64
	prefixTailHash string
	prefixTailSize int64
	sessions       map[string]*historySessionInfo
}

type catalogSessionMetaBatch struct {
	mu      sync.Mutex
	updates map[string]catalogSessionMetaEntry
	deletes map[string]struct{}
}

type catalogSessionMetaBatchKey struct{}

var catalogSQLiteState = struct {
	sync.Mutex
	stores map[string]*catalogSQLiteStore
}{stores: map[string]*catalogSQLiteStore{}}

func currentCatalogSQLiteStore(sourcePath string) (*catalogSQLiteStore, error) {
	path, err := cacheV2DatabasePath(sourcePath, cacheV2CatalogFile)
	if err != nil {
		return nil, err
	}
	root, err := cacheV2RootForSource(sourcePath)
	if err != nil {
		return nil, err
	}

	catalogSQLiteState.Lock()
	defer catalogSQLiteState.Unlock()
	if store := catalogSQLiteState.stores[path]; store != nil {
		return store, nil
	}
	db, err := openCatalogSQLite(path)
	if err != nil && isRebuildableCatalogSQLiteError(err) {
		_ = quarantineCatalogSQLite(path)
		db, err = openCatalogSQLite(path)
	}
	if err != nil {
		return nil, err
	}
	store := &catalogSQLiteStore{path: path, db: db}
	catalogSQLiteState.stores[path] = store
	cleanupLegacyCaches(root)
	return store, nil
}

func catalogSQLiteFileForSource(sourcePath string) (string, error) {
	return cacheV2DatabasePath(sourcePath, cacheV2CatalogFile)
}

func openCatalogSQLite(path string) (*sql.DB, error) {
	if err := secureCacheV2Database(path, true); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", cacheV2SQLiteFileURI(path, catalogSQLiteBusyMillis))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var journalMode string
	if err := db.QueryRow(`PRAGMA journal_mode = DELETE`).Scan(&journalMode); err != nil {
		_ = db.Close()
		return nil, err
	}
	if journalMode != "delete" {
		_ = db.Close()
		return nil, fmt.Errorf("catalog sqlite journal mode = %q, want delete", journalMode)
	}
	for _, statement := range []string{
		`PRAGMA synchronous = NORMAL`,
		fmt.Sprintf(`PRAGMA busy_timeout = %d`, catalogSQLiteBusyMillis),
		`PRAGMA temp_store = MEMORY`,
		`PRAGMA foreign_keys = ON`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if err := initializeCatalogSQLiteSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := secureCacheV2Database(path, false); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func initializeCatalogSQLiteSchema(db *sql.DB) error {
	var version, applicationID int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if err := db.QueryRow(`PRAGMA application_id`).Scan(&applicationID); err != nil {
		return err
	}
	if version != 0 || applicationID != 0 {
		if version != cacheVersion || applicationID != catalogSQLiteApplicationID {
			return fmt.Errorf("%w: application_id=%d version=%d", errCatalogSQLiteSchema, applicationID, version)
		}
		return validateCatalogSQLiteSchema(db)
	}
	var existingObjects int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema
		WHERE name NOT LIKE 'sqlite_%'`).Scan(&existingObjects); err != nil {
		return err
	}
	if existingObjects != 0 {
		return fmt.Errorf("%w: uninitialized database contains %d schema objects", errCatalogSQLiteSchema, existingObjects)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range []string{
		`CREATE TABLE session_meta (
			path TEXT PRIMARY KEY,
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
			session_id TEXT NOT NULL,
			project_path TEXT NOT NULL,
			first_prompt TEXT NOT NULL,
			message_count INTEGER NOT NULL,
			created_at_ns INTEGER NOT NULL,
			modified_at_ns INTEGER NOT NULL,
			is_subagent INTEGER NOT NULL,
			subagent_type TEXT NOT NULL,
			parent_thread_id TEXT NOT NULL
		) STRICT`,
		`CREATE TABLE history_source (
			path TEXT PRIMARY KEY,
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
			source_ctime_ns INTEGER NOT NULL
		) STRICT`,
		`CREATE TABLE history_session (
			source_path TEXT NOT NULL REFERENCES history_source(path) ON DELETE CASCADE,
			session_id TEXT NOT NULL,
			first_prompt TEXT NOT NULL,
			first_prompt_time_ns INTEGER NOT NULL,
			PRIMARY KEY (source_path, session_id)
		) STRICT, WITHOUT ROWID`,
		fmt.Sprintf(`PRAGMA application_id = %d`, catalogSQLiteApplicationID),
		fmt.Sprintf(`PRAGMA user_version = %d`, cacheVersion),
	} {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return validateCatalogSQLiteSchema(db)
}

func validateCatalogSQLiteSchema(db *sql.DB) error {
	queries := []string{
		`SELECT path, parsed_offset, prefix_tail_hash, prefix_tail_size,
			source_size, source_mtime_ns, source_mode, has_file_id, source_dev,
			source_ino, has_ctime, source_ctime_ns, session_id, project_path,
			first_prompt, message_count, created_at_ns, modified_at_ns,
			is_subagent, subagent_type, parent_thread_id FROM session_meta LIMIT 0`,
		`SELECT path, generation, parsed_offset, prefix_tail_hash, prefix_tail_size,
			source_size, source_mtime_ns, source_mode, has_file_id, source_dev,
			source_ino, has_ctime, source_ctime_ns FROM history_source LIMIT 0`,
		`SELECT source_path, session_id, first_prompt, first_prompt_time_ns
			FROM history_session LIMIT 0`,
	}
	for _, query := range queries {
		rows, err := db.Query(query)
		if err != nil {
			return fmt.Errorf("%w: %v", errCatalogSQLiteSchema, err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("%w: %v", errCatalogSQLiteSchema, err)
		}
	}
	return nil
}

func isRebuildableCatalogSQLiteError(err error) bool {
	if errors.Is(err, errCatalogSQLiteSchema) {
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

func quarantineCatalogSQLite(path string) error {
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
	for _, suffix := range []string{"-journal", "-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}
	return nil
}

func (store *catalogSQLiteStore) loadSessionMeta(ctx context.Context, path string) (catalogSessionMetaEntry, bool, error) {
	var entry catalogSessionMetaEntry
	entry.path = filepath.Clean(path)
	var hasFileID, hasCtime, isSubagent int
	var dev, ino string
	var createdNS, modifiedNS int64
	err := store.db.QueryRowContext(ctx, `SELECT parsed_offset, prefix_tail_hash,
		prefix_tail_size, source_size, source_mtime_ns, source_mode, has_file_id,
		source_dev, source_ino, has_ctime, source_ctime_ns, session_id,
		project_path, first_prompt, message_count, created_at_ns, modified_at_ns,
		is_subagent, subagent_type, parent_thread_id
		FROM session_meta WHERE path = ?`, entry.path).Scan(
		&entry.parsedOffset, &entry.prefixTailHash, &entry.prefixTailSize,
		&entry.fileKey.Size, &entry.fileKey.MtimeUnixNano, &entry.fileKey.Mode,
		&hasFileID, &dev, &ino, &hasCtime, &entry.fileKey.CtimeUnixNano,
		&entry.meta.SessionID, &entry.meta.ProjectPath, &entry.meta.FirstPrompt,
		&entry.meta.MessageCount, &createdNS, &modifiedNS, &isSubagent,
		&entry.meta.SubagentType, &entry.meta.ParentThreadID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return catalogSessionMetaEntry{}, false, nil
	}
	if err != nil {
		return catalogSessionMetaEntry{}, false, err
	}
	entry.fileKey.HasFileID = hasFileID != 0
	entry.fileKey.HasCtime = hasCtime != 0
	entry.fileKey.Dev, _ = strconv.ParseUint(dev, 10, 64)
	entry.fileKey.Ino, _ = strconv.ParseUint(ino, 10, 64)
	entry.meta.CreatedAt = cacheTimeFromUnixNano(createdNS)
	entry.meta.ModifiedAt = cacheTimeFromUnixNano(modifiedNS)
	entry.meta.IsSubagent = isSubagent != 0
	return entry, true, nil
}

func (store *catalogSQLiteStore) writeSessionMetaBatch(ctx context.Context, updates map[string]catalogSessionMetaEntry, deletes map[string]struct{}) error {
	if len(updates) == 0 && len(deletes) == 0 {
		return nil
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	changed := false

	deletePaths := make([]string, 0, len(deletes))
	for path := range deletes {
		deletePaths = append(deletePaths, path)
	}
	sort.Strings(deletePaths)
	for _, path := range deletePaths {
		result, err := tx.ExecContext(ctx, `DELETE FROM session_meta WHERE path = ?`, filepath.Clean(path))
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		changed = changed || rows > 0
	}

	updatePaths := make([]string, 0, len(updates))
	for path := range updates {
		updatePaths = append(updatePaths, path)
	}
	sort.Strings(updatePaths)
	for _, path := range updatePaths {
		entry := updates[path]
		entry.path = filepath.Clean(path)
		rows, err := upsertCatalogSessionMeta(ctx, tx, entry)
		if err != nil {
			return err
		}
		changed = changed || rows > 0
	}
	if !changed {
		return nil
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if catalogSQLiteCommitHook != nil {
		catalogSQLiteCommitHook(store.path)
	}
	return nil
}

func upsertCatalogSessionMeta(ctx context.Context, tx *sql.Tx, entry catalogSessionMetaEntry) (int64, error) {
	result, err := tx.ExecContext(ctx, `INSERT INTO session_meta (
		path, parsed_offset, prefix_tail_hash, prefix_tail_size, source_size,
		source_mtime_ns, source_mode, has_file_id, source_dev, source_ino,
		has_ctime, source_ctime_ns, session_id, project_path, first_prompt,
		message_count, created_at_ns, modified_at_ns, is_subagent, subagent_type,
		parent_thread_id
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(path) DO UPDATE SET
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
		session_id = excluded.session_id,
		project_path = excluded.project_path,
		first_prompt = excluded.first_prompt,
		message_count = excluded.message_count,
		created_at_ns = excluded.created_at_ns,
		modified_at_ns = excluded.modified_at_ns,
		is_subagent = excluded.is_subagent,
		subagent_type = excluded.subagent_type,
		parent_thread_id = excluded.parent_thread_id
	WHERE parsed_offset <> excluded.parsed_offset
		OR prefix_tail_hash <> excluded.prefix_tail_hash
		OR prefix_tail_size <> excluded.prefix_tail_size
		OR source_size <> excluded.source_size
		OR source_mtime_ns <> excluded.source_mtime_ns
		OR source_mode <> excluded.source_mode
		OR has_file_id <> excluded.has_file_id
		OR source_dev <> excluded.source_dev
		OR source_ino <> excluded.source_ino
		OR has_ctime <> excluded.has_ctime
		OR source_ctime_ns <> excluded.source_ctime_ns
		OR session_id <> excluded.session_id
		OR project_path <> excluded.project_path
		OR first_prompt <> excluded.first_prompt
		OR message_count <> excluded.message_count
		OR created_at_ns <> excluded.created_at_ns
		OR modified_at_ns <> excluded.modified_at_ns
		OR is_subagent <> excluded.is_subagent
		OR subagent_type <> excluded.subagent_type
		OR parent_thread_id <> excluded.parent_thread_id`,
		entry.path, entry.parsedOffset, entry.prefixTailHash, entry.prefixTailSize,
		entry.fileKey.Size, entry.fileKey.MtimeUnixNano, entry.fileKey.Mode,
		boolInt(entry.fileKey.HasFileID), strconv.FormatUint(entry.fileKey.Dev, 10),
		strconv.FormatUint(entry.fileKey.Ino, 10), boolInt(entry.fileKey.HasCtime),
		entry.fileKey.CtimeUnixNano, entry.meta.SessionID, entry.meta.ProjectPath,
		entry.meta.FirstPrompt, entry.meta.MessageCount, cacheTimeUnixNano(entry.meta.CreatedAt),
		cacheTimeUnixNano(entry.meta.ModifiedAt), boolInt(entry.meta.IsSubagent),
		entry.meta.SubagentType, entry.meta.ParentThreadID,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (store *catalogSQLiteStore) loadHistory(ctx context.Context, path string) (catalogHistorySourceEntry, bool, error) {
	entry := catalogHistorySourceEntry{path: filepath.Clean(path), sessions: map[string]*historySessionInfo{}}
	var hasFileID, hasCtime int
	var dev, ino string
	err := store.db.QueryRowContext(ctx, `SELECT generation, parsed_offset,
		prefix_tail_hash, prefix_tail_size, source_size, source_mtime_ns,
		source_mode, has_file_id, source_dev, source_ino, has_ctime,
		source_ctime_ns FROM history_source WHERE path = ?`, entry.path).Scan(
		&entry.generation, &entry.parsedOffset, &entry.prefixTailHash,
		&entry.prefixTailSize, &entry.fileKey.Size, &entry.fileKey.MtimeUnixNano,
		&entry.fileKey.Mode, &hasFileID, &dev, &ino, &hasCtime,
		&entry.fileKey.CtimeUnixNano,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return catalogHistorySourceEntry{}, false, nil
	}
	if err != nil {
		return catalogHistorySourceEntry{}, false, err
	}
	entry.fileKey.HasFileID = hasFileID != 0
	entry.fileKey.HasCtime = hasCtime != 0
	entry.fileKey.Dev, _ = strconv.ParseUint(dev, 10, 64)
	entry.fileKey.Ino, _ = strconv.ParseUint(ino, 10, 64)

	rows, err := store.db.QueryContext(ctx, `SELECT session_id, first_prompt,
		first_prompt_time_ns FROM history_session WHERE source_path = ?`, entry.path)
	if err != nil {
		return catalogHistorySourceEntry{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var sessionID string
		var info historySessionInfo
		var timestamp int64
		if err := rows.Scan(&sessionID, &info.FirstPrompt, &timestamp); err != nil {
			return catalogHistorySourceEntry{}, false, err
		}
		info.FirstPromptTime = cacheTimeFromUnixNano(timestamp)
		entry.sessions[sessionID] = &info
	}
	if err := rows.Err(); err != nil {
		return catalogHistorySourceEntry{}, false, err
	}
	return entry, true, nil
}

func (store *catalogSQLiteStore) replaceHistory(ctx context.Context, expected catalogHistorySourceEntry, expectedFound bool, key fileCacheKey, offset int64, prefixHash string, prefixSize int64, sessions map[string]*historySessionInfo) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var result sql.Result
	if expectedFound {
		result, err = tx.ExecContext(ctx, `UPDATE history_source SET
			generation = generation + 1, parsed_offset = ?, prefix_tail_hash = ?,
			prefix_tail_size = ?, source_size = ?, source_mtime_ns = ?, source_mode = ?,
			has_file_id = ?, source_dev = ?, source_ino = ?, has_ctime = ?, source_ctime_ns = ?
			WHERE path = ? AND generation = ?`, offset, prefixHash, prefixSize,
			key.Size, key.MtimeUnixNano, key.Mode, boolInt(key.HasFileID),
			strconv.FormatUint(key.Dev, 10), strconv.FormatUint(key.Ino, 10),
			boolInt(key.HasCtime), key.CtimeUnixNano, expected.path, expected.generation)
	} else {
		result, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO history_source (
			path, generation, parsed_offset, prefix_tail_hash, prefix_tail_size,
			source_size, source_mtime_ns, source_mode, has_file_id, source_dev,
			source_ino, has_ctime, source_ctime_ns
		) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, expected.path, offset,
			prefixHash, prefixSize, key.Size, key.MtimeUnixNano, key.Mode,
			boolInt(key.HasFileID), strconv.FormatUint(key.Dev, 10),
			strconv.FormatUint(key.Ino, 10), boolInt(key.HasCtime), key.CtimeUnixNano)
	}
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return errCatalogSQLiteConflict
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM history_session WHERE source_path = ?`, expected.path); err != nil {
		return err
	}
	if err := insertCatalogHistorySessions(ctx, tx, expected.path, sessions); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if catalogSQLiteCommitHook != nil {
		catalogSQLiteCommitHook(store.path)
	}
	return nil
}

func (store *catalogSQLiteStore) appendHistory(ctx context.Context, expected catalogHistorySourceEntry, key fileCacheKey, offset int64, prefixHash string, prefixSize int64, changed map[string]*historySessionInfo) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE history_source SET
		parsed_offset = ?, prefix_tail_hash = ?, prefix_tail_size = ?, source_size = ?,
		source_mtime_ns = ?, source_mode = ?, has_file_id = ?, source_dev = ?,
		source_ino = ?, has_ctime = ?, source_ctime_ns = ?
		WHERE path = ? AND generation = ? AND parsed_offset = ?`, offset, prefixHash,
		prefixSize, key.Size, key.MtimeUnixNano, key.Mode, boolInt(key.HasFileID),
		strconv.FormatUint(key.Dev, 10), strconv.FormatUint(key.Ino, 10),
		boolInt(key.HasCtime), key.CtimeUnixNano, expected.path, expected.generation,
		expected.parsedOffset)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return errCatalogSQLiteConflict
	}
	if err := insertCatalogHistorySessions(ctx, tx, expected.path, changed); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if catalogSQLiteCommitHook != nil {
		catalogSQLiteCommitHook(store.path)
	}
	return nil
}

func insertCatalogHistorySessions(ctx context.Context, tx *sql.Tx, sourcePath string, sessions map[string]*historySessionInfo) error {
	ids := make([]string, 0, len(sessions))
	for id := range sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		info := sessions[id]
		if info == nil {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO history_session (
			source_path, session_id, first_prompt, first_prompt_time_ns
		) VALUES (?, ?, ?, ?)
		ON CONFLICT(source_path, session_id) DO UPDATE SET
			first_prompt = excluded.first_prompt,
			first_prompt_time_ns = excluded.first_prompt_time_ns
		WHERE first_prompt <> excluded.first_prompt
			OR first_prompt_time_ns <> excluded.first_prompt_time_ns`,
			sourcePath, id, info.FirstPrompt, cacheTimeUnixNano(info.FirstPromptTime)); err != nil {
			return err
		}
	}
	return nil
}

func (store *catalogSQLiteStore) deleteHistory(ctx context.Context, path string) error {
	result, err := store.db.ExecContext(ctx, `DELETE FROM history_source WHERE path = ?`, filepath.Clean(path))
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows > 0 && catalogSQLiteCommitHook != nil {
		catalogSQLiteCommitHook(store.path)
	}
	return nil
}

func withCatalogSessionMetaBatch(ctx context.Context) (context.Context, *catalogSessionMetaBatch) {
	if ctx == nil {
		ctx = context.Background()
	}
	batch := &catalogSessionMetaBatch{
		updates: map[string]catalogSessionMetaEntry{},
		deletes: map[string]struct{}{},
	}
	return context.WithValue(ctx, catalogSessionMetaBatchKey{}, batch), batch
}

func stageCatalogSessionMeta(ctx context.Context, entry catalogSessionMetaEntry) bool {
	if ctx == nil {
		return false
	}
	batch, _ := ctx.Value(catalogSessionMetaBatchKey{}).(*catalogSessionMetaBatch)
	if batch == nil {
		return false
	}
	entry.path = filepath.Clean(entry.path)
	batch.mu.Lock()
	delete(batch.deletes, entry.path)
	batch.updates[entry.path] = entry
	batch.mu.Unlock()
	return true
}

func stageCatalogSessionMetaDelete(ctx context.Context, path string) bool {
	if ctx == nil {
		return false
	}
	batch, _ := ctx.Value(catalogSessionMetaBatchKey{}).(*catalogSessionMetaBatch)
	if batch == nil {
		return false
	}
	path = filepath.Clean(path)
	batch.mu.Lock()
	delete(batch.updates, path)
	batch.deletes[path] = struct{}{}
	batch.mu.Unlock()
	return true
}

func flushCatalogSessionMetaBatch(ctx context.Context, batch *catalogSessionMetaBatch) error {
	if batch == nil {
		return nil
	}
	batch.mu.Lock()
	updates := batch.updates
	deletes := batch.deletes
	batch.updates = map[string]catalogSessionMetaEntry{}
	batch.deletes = map[string]struct{}{}
	batch.mu.Unlock()
	if len(updates) == 0 && len(deletes) == 0 {
		return nil
	}

	type group struct {
		store   *catalogSQLiteStore
		updates map[string]catalogSessionMetaEntry
		deletes map[string]struct{}
	}
	groups := map[string]*group{}
	for path, entry := range updates {
		store, err := currentCatalogSQLiteStore(path)
		if err != nil {
			if isContextError(err) {
				return err
			}
			continue
		}
		g := groups[store.path]
		if g == nil {
			g = &group{store: store, updates: map[string]catalogSessionMetaEntry{}, deletes: map[string]struct{}{}}
			groups[store.path] = g
		}
		g.updates[path] = entry
	}
	for path := range deletes {
		store, err := currentCatalogSQLiteStore(path)
		if err != nil {
			if isContextError(err) {
				return err
			}
			continue
		}
		g := groups[store.path]
		if g == nil {
			g = &group{store: store, updates: map[string]catalogSessionMetaEntry{}, deletes: map[string]struct{}{}}
			groups[store.path] = g
		}
		g.deletes[path] = struct{}{}
	}
	for _, g := range groups {
		if err := g.store.writeSessionMetaBatch(ctx, g.updates, g.deletes); err != nil {
			if isContextError(err) {
				return err
			}
		}
	}
	return nil
}

func writeCatalogSessionMeta(ctx context.Context, entry catalogSessionMetaEntry) error {
	store, err := currentCatalogSQLiteStore(entry.path)
	if err != nil {
		if isContextError(err) {
			return err
		}
		return nil
	}
	return store.writeSessionMetaBatch(ctx, map[string]catalogSessionMetaEntry{entry.path: entry}, nil)
}

func deleteCatalogSessionMeta(ctx context.Context, path string) error {
	databasePath, exists, err := existingCacheV2DatabasePath(path, cacheV2CatalogFile)
	if err != nil {
		if isContextError(err) {
			return err
		}
		return nil
	}
	catalogSQLiteState.Lock()
	store := catalogSQLiteState.stores[databasePath]
	catalogSQLiteState.Unlock()
	if !exists && store == nil {
		return nil
	}
	if store == nil {
		store, err = currentCatalogSQLiteStore(path)
	}
	if err != nil {
		if isContextError(err) {
			return err
		}
		return nil
	}
	return store.writeSessionMetaBatch(ctx, nil, map[string]struct{}{filepath.Clean(path): {}})
}

func cacheTimeUnixNano(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}

func cacheTimeFromUnixNano(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}

func resetCatalogSQLiteForTest() {
	_ = closeCatalogSQLiteCaches()
}
