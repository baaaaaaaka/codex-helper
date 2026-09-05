package teams

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

const (
	globalInboundClaimTTL     = 5 * time.Minute
	globalInboundLockTimeout  = 500 * time.Millisecond
	maxGlobalInboundLedgerIDs = 2000
)

type globalInboundLedger struct {
	Version int                          `json:"version"`
	Items   map[string]globalInboundItem `json:"items,omitempty"`
}

type globalInboundItem struct {
	ChatID     string    `json:"chat_id"`
	MessageID  string    `json:"message_id"`
	Owner      string    `json:"owner,omitempty"`
	ClaimToken string    `json:"claim_token,omitempty"`
	Status     string    `json:"status,omitempty"`
	ClaimedAt  time.Time `json:"claimed_at,omitempty"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
}

type globalInboundClaim struct {
	Path       string
	Key        string
	ChatID     string
	MessageID  string
	Owner      string
	ClaimToken string
	// ExistingStatus is populated when another durable owner already holds the
	// message.  Callers must not mark such a message as locally seen: the
	// claim may be released after that poll and the message remains retryable.
	ExistingStatus string
	writer         *globalInboundSQLiteWriter
}

// globalInboundSQLiteWriter is scoped to one poll window. It reuses the
// already-open sidecar and its schema setup, while retaining the existing
// per-message transaction, file lock, claim token, and owner fencing rules.
// A physical identity check prevents a replaced sidecar from being mistaken
// for the file that the previous connection opened.
type globalInboundSQLiteWriter struct {
	mu            sync.Mutex
	path          string
	db            *sql.DB
	identity      os.FileInfo
	schemaReady   bool
	schemaVersion int64
}

func (w *globalInboundSQLiteWriter) open(path string) (*sql.DB, error) {
	if w == nil {
		return openTeamsLedgerSQLite(path)
	}
	path = filepath.Clean(strings.TrimSpace(path))
	info, statErr := os.Stat(path)
	if statErr != nil && !os.IsNotExist(statErr) {
		return nil, statErr
	}
	if w.db != nil && w.path == path && w.identity != nil && info != nil && os.SameFile(w.identity, info) {
		return w.db, nil
	}
	if w.db != nil {
		_ = w.db.Close()
	}
	w.db = nil
	w.path = ""
	w.identity = nil
	w.schemaReady = false
	w.schemaVersion = 0
	db, err := openTeamsLedgerSQLite(path)
	if err != nil {
		return nil, err
	}
	info, err = os.Stat(path)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	w.path = path
	w.db = db
	w.identity = info
	return db, nil
}

func (w *globalInboundSQLiteWriter) close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.db == nil {
		return nil
	}
	err := w.db.Close()
	w.db = nil
	w.path = ""
	w.identity = nil
	w.schemaReady = false
	w.schemaVersion = 0
	return err
}

func globalInboundLedgerPathForRegistry(registryPath string) (string, bool) {
	registryPath = strings.TrimSpace(registryPath)
	if registryPath == "" {
		return "", false
	}
	clean := filepath.Clean(registryPath)
	dir := filepath.Dir(clean)
	if filepath.Base(clean) == "registry.json" && filepath.Base(filepath.Dir(dir)) == "scopes" {
		return filepath.Join(filepath.Dir(filepath.Dir(dir)), "global-inbound-ledger.json"), true
	}
	return filepath.Join(dir, "teams-global-inbound-ledger.json"), true
}

func (b *Bridge) tryClaimGlobalInbound(ctx context.Context, chatID string, messageID string) (globalInboundClaim, bool, error) {
	return b.tryClaimGlobalInboundWithWriter(ctx, chatID, messageID, nil)
}

func (b *Bridge) tryClaimGlobalInboundWithWriter(ctx context.Context, chatID string, messageID string, writer *globalInboundSQLiteWriter) (globalInboundClaim, bool, error) {
	if b == nil || strings.TrimSpace(chatID) == "" || strings.TrimSpace(messageID) == "" {
		return globalInboundClaim{}, true, nil
	}
	path, ok := globalInboundLedgerPathForRegistry(b.registryPath)
	if !ok {
		return globalInboundClaim{}, true, nil
	}
	owner := strings.TrimSpace(b.machine.ID)
	if owner == "" {
		owner = strings.TrimSpace(b.scope.ID)
	}
	if owner == "" {
		owner = "unknown"
	}
	return claimGlobalInboundWithWriter(ctx, path, chatID, messageID, owner, time.Now(), writer)
}

func completeGlobalInbound(ctx context.Context, claim globalInboundClaim) error {
	_, err := completeGlobalInboundClaim(ctx, claim)
	return err
}

func completeGlobalInboundClaim(ctx context.Context, claim globalInboundClaim) (bool, error) {
	if strings.TrimSpace(claim.Path) == "" || strings.TrimSpace(claim.Key) == "" {
		return true, nil
	}
	completed := false
	err := updateGlobalInboundSQLiteWithWriter(ctx, claim.Path, claim.writer, func(tx *sql.Tx, now time.Time) error {
		item, ok, err := loadGlobalInboundSQLiteItem(ctx, tx, claim.Key)
		if err != nil {
			return err
		}
		// Completion is a compare-and-swap on the immutable claim token.  An
		// old poll goroutine may finish after a lease takeover (including an
		// ABA takeover by the same machine), and must never complete the new
		// owner's claim or recreate a row that was deliberately released.
		if !ok || item.Status != "claimed" || item.Owner != claim.Owner ||
			strings.TrimSpace(claim.ClaimToken) == "" || item.ClaimToken != claim.ClaimToken {
			return nil
		}
		item.ChatID = claim.ChatID
		item.MessageID = claim.MessageID
		item.Status = "done"
		item.UpdatedAt = now
		if err := upsertGlobalInboundSQLiteTx(ctx, tx, claim.Key, item); err != nil {
			return err
		}
		completed = true
		return nil
	})
	return completed, err
}

func releaseGlobalInbound(ctx context.Context, claim globalInboundClaim) {
	if strings.TrimSpace(claim.Path) == "" || strings.TrimSpace(claim.Key) == "" {
		return
	}
	_ = updateGlobalInboundSQLiteWithWriter(ctx, claim.Path, claim.writer, func(tx *sql.Tx, _ time.Time) error {
		item, ok, err := loadGlobalInboundSQLiteItem(ctx, tx, claim.Key)
		if err != nil {
			return err
		}
		if !ok || item.Owner != claim.Owner || item.Status != "claimed" ||
			strings.TrimSpace(claim.ClaimToken) == "" || item.ClaimToken != claim.ClaimToken {
			return nil
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM inbound_ledger WHERE key = ?`, claim.Key)
		return err
	})
}

func claimGlobalInbound(ctx context.Context, path string, chatID string, messageID string, owner string, now time.Time) (globalInboundClaim, bool, error) {
	return claimGlobalInboundWithWriter(ctx, path, chatID, messageID, owner, now, nil)
}

func claimGlobalInboundWithWriter(ctx context.Context, path string, chatID string, messageID string, owner string, now time.Time, writer *globalInboundSQLiteWriter) (globalInboundClaim, bool, error) {
	claim := globalInboundClaim{
		Path:      path,
		Key:       globalInboundKey(chatID, messageID),
		ChatID:    chatID,
		MessageID: messageID,
		Owner:     owner,
		writer:    writer,
	}
	claimed := false
	err := updateGlobalInboundSQLiteWithWriter(ctx, path, writer, func(tx *sql.Tx, _ time.Time) error {
		item, ok, err := loadGlobalInboundSQLiteItem(ctx, tx, claim.Key)
		if err != nil {
			return err
		}
		if ok {
			claim.ExistingStatus = item.Status
			switch item.Status {
			case "done":
				return nil
			case "claimed":
				if !item.UpdatedAt.IsZero() && now.Sub(item.UpdatedAt) < globalInboundClaimTTL {
					return nil
				}
			}
		}
		claim.ClaimToken = globalInboundClaimToken(claim.Key, owner, now, item.ClaimToken)
		claim.ExistingStatus = "claimed"
		if err := upsertGlobalInboundSQLiteTx(ctx, tx, claim.Key, globalInboundItem{
			ChatID:     chatID,
			MessageID:  messageID,
			Owner:      owner,
			ClaimToken: claim.ClaimToken,
			Status:     "claimed",
			ClaimedAt:  now,
			UpdatedAt:  now,
		}); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	return claim, claimed, err
}

func globalInboundClaimToken(key string, owner string, now time.Time, previous string) string {
	payload := strings.TrimSpace(key) + "\x00" + strings.TrimSpace(owner) + "\x00" + now.UTC().Format(time.RFC3339Nano) + "\x00" + strings.TrimSpace(previous)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:16])
}

func updateGlobalInboundSQLite(ctx context.Context, path string, fn func(*sql.Tx, time.Time) error) error {
	return updateGlobalInboundSQLiteWithWriter(ctx, path, nil, fn)
}

func updateGlobalInboundSQLiteWithWriter(ctx context.Context, path string, writer *globalInboundSQLiteWriter, fn func(*sql.Tx, time.Time) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	lock := flock.New(path + ".lock")
	// TryLockContext's duration is only its retry interval; it does not bound
	// how long an otherwise-live context may wait. Use a child deadline so a
	// stuck or slow sibling owner cannot pin a poll worker until the outer
	// listener phase expires.
	lockCtx, cancelLock := context.WithTimeout(ctx, globalInboundLockTimeout)
	defer cancelLock()
	ok, err := lock.TryLockContext(lockCtx, globalInboundLockTimeout)
	if err != nil {
		return err
	}
	if !ok {
		if ctxErr := lockCtx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("global Teams inbound ledger is locked: %s", path)
	}
	defer func() { _ = lock.Unlock() }()
	if writer != nil {
		writer.mu.Lock()
		defer writer.mu.Unlock()
	}
	var db *sql.DB
	if writer != nil {
		db, err = writer.open(teamsLedgerSQLitePath(path))
	} else {
		db, err = openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	}
	if err != nil {
		return err
	}
	if writer == nil {
		defer func() { _ = db.Close() }()
		if err := ensureGlobalInboundSQLite(ctx, db); err != nil {
			return err
		}
	} else {
		var schemaVersion int64
		if err := db.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&schemaVersion); err != nil {
			return err
		}
		if !writer.schemaReady || writer.schemaVersion != schemaVersion {
			if err := ensureGlobalInboundSQLite(ctx, db); err != nil {
				return err
			}
			if err := db.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&schemaVersion); err != nil {
				return err
			}
			writer.schemaReady = true
			writer.schemaVersion = schemaVersion
		}
	}
	if err := importLegacyGlobalInboundJSON(ctx, db, path, time.Now()); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now()
	if err := fn(tx, now); err != nil {
		return err
	}
	if err := pruneGlobalInboundSQLiteTx(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func readGlobalInboundLedger(path string) (globalInboundLedger, error) {
	if ledger, ok, err := readGlobalInboundSQLite(path); ok || err != nil {
		return ledger, err
	}
	var ledger globalInboundLedger
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		ledger.Version = 1
		ledger.Items = map[string]globalInboundItem{}
		return ledger, nil
	}
	if err != nil {
		return ledger, err
	}
	if len(strings.TrimSpace(string(data))) > 0 {
		if err := json.Unmarshal(data, &ledger); err != nil {
			return ledger, err
		}
	}
	if ledger.Version == 0 {
		ledger.Version = 1
	}
	if ledger.Items == nil {
		ledger.Items = map[string]globalInboundItem{}
	}
	return ledger, nil
}

func readGlobalInboundSQLite(path string) (globalInboundLedger, bool, error) {
	var ledger globalInboundLedger
	sqlitePath := teamsLedgerSQLitePath(path)
	if sqlitePath == "" {
		return ledger, false, nil
	}
	if _, err := os.Stat(sqlitePath); os.IsNotExist(err) {
		return ledger, false, nil
	} else if err != nil {
		return ledger, false, err
	}
	db, err := openTeamsLedgerSQLite(sqlitePath)
	if err != nil {
		return ledger, false, err
	}
	defer func() { _ = db.Close() }()
	if err := ensureGlobalInboundSQLite(context.Background(), db); err != nil {
		return ledger, false, err
	}
	if err := importLegacyGlobalInboundJSON(context.Background(), db, path, time.Now()); err != nil {
		return ledger, false, err
	}
	rows, err := db.Query(`SELECT json FROM inbound_ledger`)
	if err != nil {
		return ledger, false, err
	}
	defer rows.Close()
	ledger.Version = 1
	ledger.Items = map[string]globalInboundItem{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return ledger, false, err
		}
		var item globalInboundItem
		if err := json.Unmarshal(raw, &item); err != nil {
			return ledger, false, err
		}
		if strings.TrimSpace(item.ChatID) == "" || strings.TrimSpace(item.MessageID) == "" {
			continue
		}
		ledger.Items[globalInboundKey(item.ChatID, item.MessageID)] = item
	}
	if err := rows.Err(); err != nil {
		return ledger, false, err
	}
	return ledger, true, nil
}

func ensureGlobalInboundSQLite(ctx context.Context, db *sql.DB) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS inbound_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS inbound_ledger (key TEXT PRIMARY KEY, chat_id TEXT NOT NULL, message_id TEXT NOT NULL, owner TEXT NOT NULL, status TEXT NOT NULL, claimed_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS inbound_ledger_prune_idx ON inbound_ledger(updated_at, claimed_at, key)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func legacyGlobalInboundItemWins(existing, incoming globalInboundItem) bool {
	if incoming.UpdatedAt.After(existing.UpdatedAt) {
		return true
	}
	if incoming.UpdatedAt.Before(existing.UpdatedAt) {
		return false
	}
	// Equal timestamps are possible when a legacy writer copies a row without
	// preserving subsecond precision. A terminal durable disposition must not
	// be regressed to a live claim at that boundary.
	return incoming.Status == "done" && existing.Status != "done"
}

func importLegacyGlobalInboundJSON(ctx context.Context, db *sql.DB, path string, now time.Time) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		_, err = db.ExecContext(ctx, `INSERT INTO inbound_meta(key, value) VALUES ('legacy_json_token', '') ON CONFLICT(key) DO UPDATE SET value = excluded.value`)
		return err
	}
	if err != nil {
		return err
	}
	token := fmt.Sprintf("%d:%d", info.Size(), info.ModTime().UnixNano())
	var existing string
	err = db.QueryRowContext(ctx, `SELECT value FROM inbound_meta WHERE key = 'legacy_json_token'`).Scan(&existing)
	if err == nil && existing == token {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	legacy, err := readGlobalInboundJSON(path)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for key, item := range legacy.Items {
		existing, exists, err := loadGlobalInboundSQLiteItem(ctx, tx, key)
		if err != nil {
			return err
		}
		if exists && !legacyGlobalInboundItemWins(existing, item) {
			continue
		}
		if err := upsertGlobalInboundSQLiteTx(ctx, tx, key, item); err != nil {
			return err
		}
	}
	if err := pruneGlobalInboundSQLiteTx(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO inbound_meta(key, value) VALUES ('legacy_json_token', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, token); err != nil {
		return err
	}
	return tx.Commit()
}

func readGlobalInboundJSON(path string) (globalInboundLedger, error) {
	var ledger globalInboundLedger
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		ledger.Version = 1
		ledger.Items = map[string]globalInboundItem{}
		return ledger, nil
	}
	if err != nil {
		return ledger, err
	}
	if len(strings.TrimSpace(string(data))) > 0 {
		if err := json.Unmarshal(data, &ledger); err != nil {
			return ledger, err
		}
	}
	if ledger.Version == 0 {
		ledger.Version = 1
	}
	if ledger.Items == nil {
		ledger.Items = map[string]globalInboundItem{}
	}
	return ledger, nil
}

func loadGlobalInboundSQLiteItem(ctx context.Context, tx *sql.Tx, key string) (globalInboundItem, bool, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT json FROM inbound_ledger WHERE key = ?`, key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return globalInboundItem{}, false, nil
	}
	if err != nil {
		return globalInboundItem{}, false, err
	}
	var item globalInboundItem
	if err := json.Unmarshal(raw, &item); err != nil {
		return globalInboundItem{}, false, err
	}
	return item, true, nil
}

func upsertGlobalInboundSQLiteTx(ctx context.Context, tx *sql.Tx, key string, item globalInboundItem) error {
	item.ChatID = strings.TrimSpace(item.ChatID)
	item.MessageID = strings.TrimSpace(item.MessageID)
	if item.ChatID == "" || item.MessageID == "" {
		return nil
	}
	if strings.TrimSpace(key) == "" {
		key = globalInboundKey(item.ChatID, item.MessageID)
	}
	raw, err := json.Marshal(item)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO inbound_ledger(key, chat_id, message_id, owner, status, claimed_at, updated_at, json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(key) DO UPDATE SET chat_id = excluded.chat_id, message_id = excluded.message_id, owner = excluded.owner, status = excluded.status, claimed_at = excluded.claimed_at, updated_at = excluded.updated_at, json = excluded.json`,
		key, item.ChatID, item.MessageID, item.Owner, item.Status, item.ClaimedAt.UnixNano(), item.UpdatedAt.UnixNano(), raw)
	return err
}

func insertGlobalInboundSQLiteTxIfMissing(ctx context.Context, tx *sql.Tx, key string, item globalInboundItem) error {
	item.ChatID = strings.TrimSpace(item.ChatID)
	item.MessageID = strings.TrimSpace(item.MessageID)
	if item.ChatID == "" || item.MessageID == "" {
		return nil
	}
	if strings.TrimSpace(key) == "" {
		key = globalInboundKey(item.ChatID, item.MessageID)
	}
	raw, err := json.Marshal(item)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO inbound_ledger(key, chat_id, message_id, owner, status, claimed_at, updated_at, json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		key,
		item.ChatID,
		item.MessageID,
		item.Owner,
		item.Status,
		item.ClaimedAt.UnixNano(),
		item.UpdatedAt.UnixNano(),
		raw,
	)
	return err
}

func pruneGlobalInboundSQLiteTx(ctx context.Context, tx *sql.Tx) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger`).Scan(&count); err != nil {
		return err
	}
	over := count - maxGlobalInboundLedgerIDs
	if over <= 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM inbound_ledger WHERE key IN (
SELECT key FROM inbound_ledger ORDER BY updated_at ASC, claimed_at ASC, key ASC LIMIT ?
)`, over)
	return err
}

func pruneGlobalInboundLedger(ledger *globalInboundLedger, now time.Time) {
	if ledger == nil || len(ledger.Items) <= maxGlobalInboundLedgerIDs {
		return
	}
	type entry struct {
		key string
		at  time.Time
	}
	entries := make([]entry, 0, len(ledger.Items))
	for key, item := range ledger.Items {
		at := item.UpdatedAt
		if at.IsZero() {
			at = item.ClaimedAt
		}
		if at.IsZero() {
			at = now
		}
		entries = append(entries, entry{key: key, at: at})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].at.Before(entries[j].at) })
	for len(entries) > maxGlobalInboundLedgerIDs {
		delete(ledger.Items, entries[0].key)
		entries = entries[1:]
	}
}

func globalInboundKey(chatID string, messageID string) string {
	return strings.TrimSpace(chatID) + "\x00" + strings.TrimSpace(messageID)
}
