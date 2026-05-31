package responsesadapter

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type SQLiteStoreOptions struct {
	TTL        time.Duration
	MaxRecords int
	Now        func() time.Time
}

type SQLiteStore struct {
	db         *sql.DB
	now        func() time.Time
	ttl        time.Duration
	maxRecords int
}

const sqliteActiveTurnStaleAge = 6 * time.Hour

func NewSQLiteStore(path string, opts SQLiteStoreOptions) (*SQLiteStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("sqlite store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &SQLiteStore{
		db:         db,
		now:        opts.Now,
		ttl:        opts.TTL,
		maxRecords: opts.MaxRecords,
	}
	if store.now == nil {
		store.now = time.Now
	}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *SQLiteStore) init() error {
	stmts := []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA busy_timeout = 5000`,
		`CREATE TABLE IF NOT EXISTS responses_adapter_responses (
			id TEXT PRIMARY KEY,
			previous_response_id TEXT,
			scope_key TEXT NOT NULL,
			tenant TEXT NOT NULL,
			user_id TEXT NOT NULL,
			provider TEXT NOT NULL,
			model TEXT NOT NULL,
			thread TEXT NOT NULL,
			branch TEXT NOT NULL,
			key_fingerprint TEXT NOT NULL,
			base_url_hash TEXT NOT NULL,
			profile_version TEXT NOT NULL,
			status TEXT NOT NULL,
			cached_tokens INTEGER,
			record_json TEXT NOT NULL,
			stored_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_responses_adapter_responses_scope ON responses_adapter_responses(scope_key, updated_at)`,
		`CREATE INDEX IF NOT EXISTS idx_responses_adapter_responses_previous ON responses_adapter_responses(previous_response_id)`,
		`CREATE TABLE IF NOT EXISTS responses_adapter_active_turns (
				scope_key TEXT PRIMARY KEY,
				turn_id TEXT NOT NULL,
				created_at INTEGER NOT NULL
			)`,
		`CREATE TABLE IF NOT EXISTS responses_adapter_reasoning_cache (
			scope_key TEXT NOT NULL,
			cache_key TEXT NOT NULL,
				reasoning TEXT NOT NULL,
				updated_at INTEGER NOT NULL,
				PRIMARY KEY(scope_key, cache_key)
			)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}
	return s.deleteStaleActiveTurns(sqliteMillis(s.now()))
}

func (s *SQLiteStore) BeginTurn(scope Scope, turnID string) (func(), error) {
	scope = scope.withDefaults()
	key := scope.key()
	now := sqliteMillis(s.now())
	if err := s.deleteStaleActiveTurns(now); err != nil {
		return nil, err
	}
	result, err := s.db.Exec(
		`INSERT OR IGNORE INTO responses_adapter_active_turns(scope_key, turn_id, created_at) VALUES (?, ?, ?)`,
		key, turnID, now,
	)
	if err != nil {
		return nil, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if affected == 0 {
		return nil, fmt.Errorf("%w: %s", ErrActiveTurn, key)
	}
	return func() {
		_, _ = s.db.Exec(
			`DELETE FROM responses_adapter_active_turns WHERE scope_key = ? AND turn_id = ?`,
			key, turnID,
		)
	}, nil
}

func (s *SQLiteStore) Store(record ResponseRecord) error {
	record.Scope = record.Scope.withDefaults()
	if strings.TrimSpace(record.ID) == "" {
		return fmt.Errorf("response id is required")
	}
	if err := s.evict(); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	now := sqliteMillis(s.now())
	cachedTokens := sql.NullInt64{}
	if record.Usage != nil {
		cachedTokens = sql.NullInt64{Int64: int64(record.Usage.CachedTokens), Valid: true}
	}
	_, err = s.db.Exec(
		`INSERT INTO responses_adapter_responses (
			id, previous_response_id, scope_key, tenant, user_id, provider, model, thread, branch,
			key_fingerprint, base_url_hash, profile_version, status, cached_tokens, record_json, stored_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			previous_response_id = excluded.previous_response_id,
			scope_key = excluded.scope_key,
			tenant = excluded.tenant,
			user_id = excluded.user_id,
			provider = excluded.provider,
			model = excluded.model,
			thread = excluded.thread,
			branch = excluded.branch,
			key_fingerprint = excluded.key_fingerprint,
			base_url_hash = excluded.base_url_hash,
			profile_version = excluded.profile_version,
			status = excluded.status,
			cached_tokens = excluded.cached_tokens,
			record_json = excluded.record_json,
			updated_at = excluded.updated_at`,
		record.ID,
		record.PreviousResponseID,
		record.Scope.key(),
		record.Scope.Tenant,
		record.Scope.User,
		record.Scope.Provider,
		record.Scope.Model,
		record.Scope.Thread,
		record.Scope.Branch,
		record.Scope.KeyFingerprint,
		record.Scope.BaseURLHash,
		record.Scope.ProfileVersion,
		string(record.Status),
		cachedTokens,
		string(raw),
		now,
		now,
	)
	if err != nil {
		return err
	}
	if err := s.rememberReasoning(record, now); err != nil {
		return err
	}
	return s.evict()
}

func (s *SQLiteStore) Get(id string, scope Scope) (ResponseRecord, error) {
	scope = scope.withDefaults()
	var scopeKey string
	var raw string
	err := s.db.QueryRow(
		`SELECT scope_key, record_json FROM responses_adapter_responses WHERE id = ?`,
		id,
	).Scan(&scopeKey, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return ResponseRecord{}, ErrResponseNotFound
	}
	if err != nil {
		return ResponseRecord{}, err
	}
	if scopeKey != scope.key() {
		return ResponseRecord{}, ErrScopeMismatch
	}
	var record ResponseRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return ResponseRecord{}, err
	}
	record.Scope = record.Scope.withDefaults()
	return record, nil
}

func (s *SQLiteStore) ResolveChain(previousResponseID string, scope Scope) ([]ResponseRecord, error) {
	scope = scope.withDefaults()
	var reversed []ResponseRecord
	seen := map[string]bool{}
	current := previousResponseID
	for current != "" {
		if seen[current] {
			break
		}
		seen[current] = true
		record, err := s.Get(current, scope)
		if err != nil {
			return nil, err
		}
		reversed = append(reversed, record)
		current = record.PreviousResponseID
	}
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	return reversed, nil
}

func (s *SQLiteStore) LookupReasoning(scope Scope, message ProviderMessage) (string, error) {
	keys := reasoningCacheKeys(scope, message)
	if len(keys) == 0 {
		return "", nil
	}
	scopeKey := scope.withDefaults().key()
	for _, key := range keys {
		var reasoning string
		err := s.db.QueryRow(
			`SELECT reasoning FROM responses_adapter_reasoning_cache WHERE scope_key = ? AND cache_key = ?`,
			scopeKey, key,
		).Scan(&reasoning)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return "", err
		}
		return reasoning, nil
	}
	return "", nil
}

func (s *SQLiteStore) rememberReasoning(record ResponseRecord, now int64) error {
	if strings.TrimSpace(record.ReasoningText) == "" {
		return nil
	}
	scope := record.Scope.withDefaults()
	for _, key := range reasoningKeysForRecord(scope, record) {
		if _, err := s.db.Exec(
			`INSERT INTO responses_adapter_reasoning_cache(scope_key, cache_key, reasoning, updated_at)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(scope_key, cache_key) DO UPDATE SET
				reasoning = excluded.reasoning,
				updated_at = excluded.updated_at`,
			scope.key(), key, record.ReasoningText, now,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStore) deleteStaleActiveTurns(now int64) error {
	ttl := sqliteActiveTurnStaleAge
	if ttl <= 0 {
		return nil
	}
	_, err := s.db.Exec(`DELETE FROM responses_adapter_active_turns WHERE created_at < ?`, now-ttl.Milliseconds())
	return err
}

func (s *SQLiteStore) evict() error {
	now := sqliteMillis(s.now())
	if s.ttl > 0 {
		cutoff := now - s.ttl.Milliseconds()
		if _, err := s.db.Exec(`DELETE FROM responses_adapter_responses WHERE stored_at < ?`, cutoff); err != nil {
			return err
		}
		if _, err := s.db.Exec(`DELETE FROM responses_adapter_reasoning_cache WHERE updated_at < ?`, cutoff); err != nil {
			return err
		}
	}
	if s.maxRecords > 0 {
		_, err := s.db.Exec(
			`DELETE FROM responses_adapter_responses
			 WHERE id IN (
				SELECT id FROM responses_adapter_responses
				ORDER BY stored_at DESC, id DESC
				LIMIT -1 OFFSET ?
			 )`,
			s.maxRecords,
		)
		return err
	}
	return nil
}

func sqliteMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}
