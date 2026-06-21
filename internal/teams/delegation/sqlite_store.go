package delegation

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

const delegationSQLiteStoreVersion = 1

func storePathUsesSQLite(path string) bool {
	return strings.EqualFold(filepath.Ext(strings.TrimSpace(path)), ".sqlite")
}

func legacyJSONStorePathForSQLite(path string) string {
	path = strings.TrimSpace(path)
	if !storePathUsesSQLite(path) {
		return ""
	}
	return strings.TrimSuffix(path, filepath.Ext(path)) + ".json"
}

func materializeSQLiteStoreFromLegacy(path string) error {
	path = strings.TrimSpace(path)
	if path == "" || !storePathUsesSQLite(path) {
		return nil
	}
	exists, err := osStatSQLite(path)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	legacy := legacyJSONStorePathForSQLite(path)
	if legacy == "" {
		return nil
	}
	if _, err := os.Stat(legacy); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	store, err := LoadStore(legacy)
	if err != nil {
		return err
	}
	_, err = saveSQLiteStore(path, store)
	return err
}

func loadSQLiteStore(path string) (Store, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return newStore(), nil
	}
	if _, err := os.Stat(path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return Store{}, err
		}
		if legacy := legacyJSONStorePathForSQLite(path); legacy != "" {
			if _, legacyErr := os.Stat(legacy); legacyErr == nil {
				return LoadStore(legacy)
			} else if legacyErr != nil && !errors.Is(legacyErr, os.ErrNotExist) {
				return Store{}, legacyErr
			}
		}
		return newStore(), nil
	}
	db, err := openDelegationSQLiteStore(path, false)
	if err != nil {
		return Store{}, err
	}
	defer db.Close()
	if err := ensureDelegationSQLiteSchema(context.Background(), db); err != nil {
		return Store{}, err
	}
	return readDelegationSQLiteStore(context.Background(), db)
}

func saveSQLiteStore(path string, store Store) (bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return false, nil
	}
	store.SchemaVersion = 1
	store.EnsureRoutes()
	store.EnsureRemoteThreads()
	store.EnsureExecutions()
	store.EnsureOutbox()
	store.EnsureInboxCursors()
	store.EnsureInboxBackoffs()
	db, err := openDelegationSQLiteStore(path, true)
	if err != nil {
		return false, err
	}
	defer db.Close()
	ctx := context.Background()
	if err := ensureDelegationSQLiteSchema(ctx, db); err != nil {
		return false, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	changed := false
	for _, sync := range []func(context.Context, *sql.Tx, Store) (bool, error){
		syncDelegationSQLiteRecords,
		syncDelegationSQLiteRoutes,
		syncDelegationSQLiteRemoteThreads,
		syncDelegationSQLiteExecutions,
		syncDelegationSQLiteOutbox,
		syncDelegationSQLiteInboxCursors,
		syncDelegationSQLiteInboxBackoffs,
	} {
		tableChanged, err := sync(ctx, tx, store)
		if err != nil {
			return false, err
		}
		changed = changed || tableChanged
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO meta(key, value) VALUES ('schema_version', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, fmt.Sprint(delegationSQLiteStoreVersion)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	chmodDelegationSQLiteFiles(path)
	return changed, nil
}

func openDelegationSQLiteStore(path string, create bool) (*sql.DB, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("delegation sqlite path is required")
	}
	if create {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, err
		}
	}
	query := url.Values{}
	if create {
		query.Set("mode", "rwc")
	} else {
		query.Set("mode", "rw")
	}
	db, err := sql.Open("sqlite", delegationSQLiteFileURI(path, query))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, stmt := range []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA synchronous = NORMAL`,
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA temp_store = MEMORY`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	chmodDelegationSQLiteFiles(path)
	return db, nil
}

func delegationSQLiteFileURI(path string, query url.Values) string {
	u := url.URL{Scheme: "file"}
	if runtime.GOOS == "windows" {
		slash := strings.ReplaceAll(path, `\`, `/`)
		if strings.HasPrefix(slash, "//") {
			trimmed := strings.TrimLeft(slash, "/")
			host, rest, ok := strings.Cut(trimmed, "/")
			if ok {
				u.Host = host
				u.Path = "/" + rest
			} else {
				u.Path = slash
			}
		} else {
			if len(slash) >= 2 && slash[1] == ':' {
				slash = "/" + slash
			}
			u.Path = slash
		}
	} else {
		u.Path = path
	}
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	return u.String()
}

func chmodDelegationSQLiteFiles(path string) {
	_ = os.Chmod(path, 0o600)
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); err == nil {
			_ = os.Chmod(path+suffix, 0o600)
		}
	}
}

func ensureDelegationSQLiteSchema(ctx context.Context, db *sql.DB) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS records (id TEXT PRIMARY KEY, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS routes (id TEXT PRIMARY KEY, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS remote_threads (id TEXT PRIMARY KEY, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS executions (id TEXT PRIMARY KEY, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS inbox_cursors (id TEXT PRIMARY KEY, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS inbox_backoffs (id TEXT PRIMARY KEY, json BLOB NOT NULL)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func readDelegationSQLiteStore(ctx context.Context, db *sql.DB) (Store, error) {
	store := newStore()
	if err := loadDelegationSQLiteSlice(ctx, db, `SELECT json FROM records`, &store.Records); err != nil {
		return Store{}, err
	}
	sort.SliceStable(store.Records, func(i, j int) bool {
		at := parseRecordTime(store.Records[i].CreatedAt)
		bt := parseRecordTime(store.Records[j].CreatedAt)
		if at.Equal(bt) {
			return store.Records[i].RecordID < store.Records[j].RecordID
		}
		return at.Before(bt)
	})
	if err := loadDelegationSQLiteMap(ctx, db, `SELECT json FROM routes`, store.Routes, func(v Route) string { return v.DelegationID }); err != nil {
		return Store{}, err
	}
	if err := loadDelegationSQLiteMap(ctx, db, `SELECT json FROM remote_threads`, store.RemoteThreads, func(v RemoteThread) string { return v.ThreadID }); err != nil {
		return Store{}, err
	}
	if err := loadDelegationSQLiteMap(ctx, db, `SELECT json FROM executions`, store.Executions, func(v ExecutionFence) string { return v.DelegationID }); err != nil {
		return Store{}, err
	}
	if err := loadDelegationSQLiteMap(ctx, db, `SELECT json FROM outbox`, store.Outbox, func(v OutboxRecord) string { return v.RecordID }); err != nil {
		return Store{}, err
	}
	if err := loadDelegationSQLiteMap(ctx, db, `SELECT json FROM inbox_cursors`, store.InboxCursors, func(v InboxCursor) string { return v.ChatID }); err != nil {
		return Store{}, err
	}
	if err := loadDelegationSQLiteMap(ctx, db, `SELECT json FROM inbox_backoffs`, store.InboxBackoffs, func(v InboxBackoff) string { return v.ChatID }); err != nil {
		return Store{}, err
	}
	return store, nil
}

func loadDelegationSQLiteSlice[T any](ctx context.Context, db *sql.DB, query string, out *[]T) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var value T
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		*out = append(*out, value)
	}
	return rows.Err()
}

func loadDelegationSQLiteMap[T any](ctx context.Context, db *sql.DB, query string, out map[string]T, key func(T) string) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var value T
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		id := strings.TrimSpace(key(value))
		if id != "" {
			out[id] = value
		}
	}
	return rows.Err()
}

func syncDelegationSQLiteRecords(ctx context.Context, tx *sql.Tx, store Store) (bool, error) {
	desired := map[string][]byte{}
	for _, record := range store.Records {
		id := strings.TrimSpace(record.RecordID)
		if id == "" {
			continue
		}
		raw, err := json.Marshal(record)
		if err != nil {
			return false, err
		}
		desired[id] = raw
	}
	return syncDelegationSQLiteJSONTable(ctx, tx, "records", desired)
}

func syncDelegationSQLiteRoutes(ctx context.Context, tx *sql.Tx, store Store) (bool, error) {
	return syncDelegationSQLiteTypedMap(ctx, tx, "routes", store.Routes, func(v Route) string { return v.DelegationID })
}

func syncDelegationSQLiteRemoteThreads(ctx context.Context, tx *sql.Tx, store Store) (bool, error) {
	return syncDelegationSQLiteTypedMap(ctx, tx, "remote_threads", store.RemoteThreads, func(v RemoteThread) string { return v.ThreadID })
}

func syncDelegationSQLiteExecutions(ctx context.Context, tx *sql.Tx, store Store) (bool, error) {
	return syncDelegationSQLiteTypedMap(ctx, tx, "executions", store.Executions, func(v ExecutionFence) string { return v.DelegationID })
}

func syncDelegationSQLiteOutbox(ctx context.Context, tx *sql.Tx, store Store) (bool, error) {
	return syncDelegationSQLiteTypedMap(ctx, tx, "outbox", store.Outbox, func(v OutboxRecord) string { return v.RecordID })
}

func syncDelegationSQLiteInboxCursors(ctx context.Context, tx *sql.Tx, store Store) (bool, error) {
	return syncDelegationSQLiteTypedMap(ctx, tx, "inbox_cursors", store.InboxCursors, func(v InboxCursor) string { return v.ChatID })
}

func syncDelegationSQLiteInboxBackoffs(ctx context.Context, tx *sql.Tx, store Store) (bool, error) {
	return syncDelegationSQLiteTypedMap(ctx, tx, "inbox_backoffs", store.InboxBackoffs, func(v InboxBackoff) string { return v.ChatID })
}

func syncDelegationSQLiteTypedMap[T any](ctx context.Context, tx *sql.Tx, table string, values map[string]T, key func(T) string) (bool, error) {
	desired := make(map[string][]byte, len(values))
	for _, value := range values {
		id := strings.TrimSpace(key(value))
		if id == "" {
			continue
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return false, err
		}
		desired[id] = raw
	}
	return syncDelegationSQLiteJSONTable(ctx, tx, table, desired)
}

func syncDelegationSQLiteJSONTable(ctx context.Context, tx *sql.Tx, table string, desired map[string][]byte) (bool, error) {
	existing := map[string][]byte{}
	rows, err := tx.QueryContext(ctx, "SELECT id, json FROM "+table)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return false, err
		}
		existing[id] = raw
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	changed := false
	for id := range existing {
		if _, ok := desired[id]; ok {
			continue
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE id = ?", id); err != nil {
			return false, err
		}
		changed = true
	}
	for id, raw := range desired {
		if bytes.Equal(existing[id], raw) {
			continue
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+table+"(id, json) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET json = excluded.json", id, raw); err != nil {
			return false, err
		}
		changed = true
	}
	return changed, nil
}
