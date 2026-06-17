package teams

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	ChatID    string    `json:"chat_id"`
	MessageID string    `json:"message_id"`
	Owner     string    `json:"owner,omitempty"`
	Status    string    `json:"status,omitempty"`
	ClaimedAt time.Time `json:"claimed_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type globalInboundClaim struct {
	Path      string
	Key       string
	ChatID    string
	MessageID string
	Owner     string
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
	return claimGlobalInbound(ctx, path, chatID, messageID, owner, time.Now())
}

func completeGlobalInbound(ctx context.Context, claim globalInboundClaim) error {
	if strings.TrimSpace(claim.Path) == "" || strings.TrimSpace(claim.Key) == "" {
		return nil
	}
	return updateGlobalInboundSQLite(ctx, claim.Path, func(tx *sql.Tx, now time.Time) error {
		item, _, err := loadGlobalInboundSQLiteItem(ctx, tx, claim.Key)
		if err != nil {
			return err
		}
		item.ChatID = claim.ChatID
		item.MessageID = claim.MessageID
		item.Owner = claim.Owner
		item.Status = "done"
		item.UpdatedAt = now
		return upsertGlobalInboundSQLiteTx(ctx, tx, claim.Key, item)
	})
}

func releaseGlobalInbound(ctx context.Context, claim globalInboundClaim) {
	if strings.TrimSpace(claim.Path) == "" || strings.TrimSpace(claim.Key) == "" {
		return
	}
	_ = updateGlobalInboundSQLite(ctx, claim.Path, func(tx *sql.Tx, _ time.Time) error {
		item, ok, err := loadGlobalInboundSQLiteItem(ctx, tx, claim.Key)
		if err != nil {
			return err
		}
		if !ok || item.Owner != claim.Owner || item.Status != "claimed" {
			return nil
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM inbound_ledger WHERE key = ?`, claim.Key)
		return err
	})
}

func claimGlobalInbound(ctx context.Context, path string, chatID string, messageID string, owner string, now time.Time) (globalInboundClaim, bool, error) {
	claim := globalInboundClaim{
		Path:      path,
		Key:       globalInboundKey(chatID, messageID),
		ChatID:    chatID,
		MessageID: messageID,
		Owner:     owner,
	}
	claimed := false
	err := updateGlobalInboundSQLite(ctx, path, func(tx *sql.Tx, _ time.Time) error {
		item, ok, err := loadGlobalInboundSQLiteItem(ctx, tx, claim.Key)
		if err != nil {
			return err
		}
		if ok {
			switch item.Status {
			case "done":
				return nil
			case "claimed":
				if !item.UpdatedAt.IsZero() && now.Sub(item.UpdatedAt) < globalInboundClaimTTL {
					return nil
				}
			}
		}
		if err := upsertGlobalInboundSQLiteTx(ctx, tx, claim.Key, globalInboundItem{
			ChatID:    chatID,
			MessageID: messageID,
			Owner:     owner,
			Status:    "claimed",
			ClaimedAt: now,
			UpdatedAt: now,
		}); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	return claim, claimed, err
}

func updateGlobalInboundSQLite(ctx context.Context, path string, fn func(*sql.Tx, time.Time) error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	lock := flock.New(path + ".lock")
	ok, err := lock.TryLockContext(ctx, globalInboundLockTimeout)
	if err != nil {
		return err
	}
	if !ok {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("global Teams inbound ledger is locked: %s", path)
	}
	defer func() { _ = lock.Unlock() }()
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if err := ensureGlobalInboundSQLite(ctx, db); err != nil {
		return err
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
