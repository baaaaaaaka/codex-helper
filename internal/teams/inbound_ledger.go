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

var ErrInboundLedgerProjectionUntrusted = errors.New("Teams inbound ledger projection is untrusted")

type globalInboundLedger struct {
	Version int                          `json:"version"`
	Items   map[string]globalInboundItem `json:"items,omitempty"`
}

type globalInboundItem struct {
	ChatID             string    `json:"chat_id"`
	MessageID          string    `json:"message_id"`
	Owner              string    `json:"owner,omitempty"`
	ScopeID            string    `json:"scope_id,omitempty"`
	ProcessIncarnation string    `json:"process_incarnation,omitempty"`
	LeaseGeneration    int64     `json:"lease_generation,omitempty"`
	ClaimToken         string    `json:"claim_token,omitempty"`
	Status             string    `json:"status,omitempty"`
	ClaimedAt          time.Time `json:"claimed_at,omitempty"`
	UpdatedAt          time.Time `json:"updated_at,omitempty"`
}

type globalInboundClaim struct {
	Path               string
	Key                string
	ChatID             string
	MessageID          string
	Owner              string
	ScopeID            string
	ProcessIncarnation string
	LeaseGeneration    int64
	ClaimToken         string
	// ExistingStatus is populated when another durable owner already holds the
	// message.  Callers must not mark such a message as locally seen: the
	// claim may be released after that poll and the message remains retryable.
	ExistingStatus string
	writer         *globalInboundSQLiteWriter
}

type globalInboundClaimIdentity struct {
	Owner              string
	ScopeID            string
	ProcessIncarnation string
	LeaseGeneration    int64
	// AllowUnexpiredReclaim is set only after the active control lease for this
	// generation has been validated by the bridge.  It is never persisted and
	// is not a substitute for the claim-token CAS on completion/release.
	AllowUnexpiredReclaim bool
}

// globalInboundSQLiteWriter is scoped to one poll window. It reuses the
// already-open sidecar and its schema setup, while retaining the existing
// per-message transaction, file lock, claim token, and owner fencing rules.
// A physical identity check prevents a replaced sidecar from being mistaken
// for the file that the previous connection opened.
type globalInboundSQLiteWriter struct {
	// gate serializes the whole per-operation use of the shared connection.
	// It is a channel rather than sync.Mutex so a poll worker waiting behind a
	// sibling can stop as soon as its phase context is canceled.
	gateInit      sync.Mutex
	gate          chan struct{}
	stateMu       sync.Mutex
	path          string
	db            *sql.DB
	identity      os.FileInfo
	schemaReady   bool
	schemaVersion int64
}

func (w *globalInboundSQLiteWriter) gateChannel() chan struct{} {
	w.gateInit.Lock()
	defer w.gateInit.Unlock()
	if w.gate == nil {
		w.gate = make(chan struct{}, 1)
		w.gate <- struct{}{}
	}
	return w.gate
}

func (w *globalInboundSQLiteWriter) acquire(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.gateChannel():
		return nil
	}
}

func (w *globalInboundSQLiteWriter) release() {
	w.gateChannel() <- struct{}{}
}

func (w *globalInboundSQLiteWriter) open(path string) (*sql.DB, error) {
	if w == nil {
		return openTeamsLedgerSQLite(path)
	}
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
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
	if err := w.acquire(context.Background()); err != nil {
		return err
	}
	defer w.release()
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
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
	scopeID, owner, leaseGeneration := b.ownerFieldsForContext(ctx)
	if owner == "" {
		owner = strings.TrimSpace(b.machine.ID)
	}
	if owner == "" {
		owner = strings.TrimSpace(b.scope.ID)
	}
	if owner == "" {
		owner = "unknown"
	}
	identity := globalInboundClaimIdentity{
		Owner:              owner,
		ScopeID:            strings.TrimSpace(scopeID),
		ProcessIncarnation: b.pollProcessIncarnation(),
		LeaseGeneration:    leaseGeneration,
		// A control-lease generation change does not by itself prove that the
		// previous poll worker has stopped.  Keep the per-message claim until
		// its TTL expires unless a caller has an explicit, independently
		// serialized reclaim capability.  This bridge path has no such atomic
		// capability, so taking over here could let the old worker and the new
		// worker both process the same inbound message.
		AllowUnexpiredReclaim: false,
	}
	return claimGlobalInboundWithIdentity(ctx, path, chatID, messageID, identity, time.Now(), writer)
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

func releaseGlobalInbound(ctx context.Context, claim globalInboundClaim) error {
	if strings.TrimSpace(claim.Path) == "" || strings.TrimSpace(claim.Key) == "" {
		return nil
	}
	return updateGlobalInboundSQLiteWithWriter(ctx, claim.Path, claim.writer, func(tx *sql.Tx, _ time.Time) error {
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
	return claimGlobalInboundWithIdentity(ctx, path, chatID, messageID, globalInboundClaimIdentity{Owner: owner}, now, writer)
}

func claimGlobalInboundWithIdentity(ctx context.Context, path string, chatID string, messageID string, identity globalInboundClaimIdentity, now time.Time, writer *globalInboundSQLiteWriter) (globalInboundClaim, bool, error) {
	claim := globalInboundClaim{
		Path:               path,
		Key:                globalInboundKey(chatID, messageID),
		ChatID:             chatID,
		MessageID:          messageID,
		Owner:              identity.Owner,
		ScopeID:            identity.ScopeID,
		ProcessIncarnation: identity.ProcessIncarnation,
		LeaseGeneration:    identity.LeaseGeneration,
		writer:             writer,
	}
	claimed := false
	err := updateGlobalInboundSQLiteWithWriter(ctx, path, writer, func(tx *sql.Tx, _ time.Time) error {
		item, ok, err := loadGlobalInboundSQLiteItem(ctx, tx, claim.Key)
		if err != nil {
			return err
		}
		if !ok {
			quarantined, err := globalInboundQuarantineExists(ctx, tx, claim.Key)
			if err != nil {
				return err
			}
			if quarantined {
				return fmt.Errorf("%w: legacy inbound key %q is quarantined", ErrInboundLedgerProjectionUntrusted, claim.Key)
			}
		}
		if ok {
			claim.ExistingStatus = item.Status
			switch item.Status {
			case "done":
				return nil
			case "claimed":
				fresh := !item.UpdatedAt.IsZero() && now.Sub(item.UpdatedAt) < globalInboundClaimTTL
				canTakeover := identity.AllowUnexpiredReclaim && identity.LeaseGeneration > 0 &&
					item.LeaseGeneration > 0 && item.LeaseGeneration < identity.LeaseGeneration &&
					strings.TrimSpace(item.ScopeID) != "" && strings.TrimSpace(item.ScopeID) == strings.TrimSpace(identity.ScopeID)
				if fresh && !canTakeover {
					return nil
				}
			}
		}
		claim.ClaimToken = globalInboundClaimToken(claim.Key, identity.Owner, now, item.ClaimToken)
		claim.ExistingStatus = "claimed"
		if err := upsertGlobalInboundSQLiteTx(ctx, tx, claim.Key, globalInboundItem{
			ChatID:             chatID,
			MessageID:          messageID,
			Owner:              identity.Owner,
			ScopeID:            identity.ScopeID,
			ProcessIncarnation: identity.ProcessIncarnation,
			LeaseGeneration:    identity.LeaseGeneration,
			ClaimToken:         claim.ClaimToken,
			Status:             "claimed",
			ClaimedAt:          now,
			UpdatedAt:          now,
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
	// Serialize operations using the same process-local writer before taking the
	// cross-process file lock. Taking flock first lets several sibling poll
	// workers all wait on the same ledger lock while only one can make progress,
	// consuming the short lock timeout without providing any extra safety.
	// Distinct writers/processes still rendezvous at flock, and every operation
	// below remains its own transaction with the existing claim/CAS checks.
	if writer != nil {
		if err := writer.acquire(ctx); err != nil {
			return err
		}
		defer writer.release()
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
	// Commit the claim/CAS before doing any replay-ledger maintenance. Pruning
	// is a bounded best-effort cleanup; if a malformed row, a busy database, or
	// cancellation prevents it from running, the newly durable claim must not
	// be rolled back and become invisible to the poller.
	if err := tx.Commit(); err != nil {
		return err
	}
	_ = pruneGlobalInboundSQLiteDB(ctx, db, now)
	return nil
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
	rows, err := db.Query(`SELECT key, chat_id, message_id, owner, status, claimed_at, updated_at, json FROM inbound_ledger`)
	if err != nil {
		return ledger, false, err
	}
	defer rows.Close()
	ledger.Version = 1
	ledger.Items = map[string]globalInboundItem{}
	for rows.Next() {
		var row globalInboundSQLiteRow
		if err := rows.Scan(&row.key, &row.chatID, &row.messageID, &row.owner, &row.status, &row.claimedAt, &row.updatedAt, &row.raw); err != nil {
			return ledger, false, err
		}
		var item globalInboundItem
		if err := json.Unmarshal(row.raw, &item); err != nil {
			return ledger, false, err
		}
		if err := validateGlobalInboundSQLiteProjection(row, item); err != nil {
			return ledger, false, err
		}
		ledger.Items[row.key] = item
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
		// Legacy rows that cannot be proved safe are retained outside the active
		// claim table. This prevents migration from creating a permanently
		// unreadable active row while preserving dedupe evidence for an explicit
		// repair or operator decision.
		`CREATE TABLE IF NOT EXISTS inbound_ledger_quarantine (key TEXT PRIMARY KEY, source_key TEXT NOT NULL, reason TEXT NOT NULL, imported_at INTEGER NOT NULL, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS inbound_ledger_quarantine_source_idx ON inbound_ledger_quarantine(source_key)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func validateLegacyGlobalInboundItem(item globalInboundItem) error {
	if strings.TrimSpace(item.ChatID) == "" || strings.TrimSpace(item.MessageID) == "" {
		return errors.New("missing chat or message identity")
	}
	if item.Status != "claimed" && item.Status != "done" {
		return fmt.Errorf("unsupported status %q", item.Status)
	}
	if item.Status == "claimed" && (item.ClaimedAt.IsZero() || item.UpdatedAt.IsZero()) {
		return errors.New("claimed item has no complete timestamp")
	}
	return nil
}

func quarantineGlobalInboundRawSQLiteTx(ctx context.Context, tx *sql.Tx, sourceKey string, raw []byte, reason string, now time.Time) error {
	if strings.TrimSpace(sourceKey) == "" {
		sourceKey = "legacy-unknown"
	}
	payload := strings.TrimSpace(sourceKey) + "\x00" + string(raw)
	sum := sha256.Sum256([]byte(payload))
	quarantineKey := "legacy:" + hex.EncodeToString(sum[:])
	_, err := tx.ExecContext(ctx, `INSERT INTO inbound_ledger_quarantine(key, source_key, reason, imported_at, json)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(key) DO UPDATE SET source_key = excluded.source_key, reason = excluded.reason, imported_at = excluded.imported_at, json = excluded.json`,
		quarantineKey, sourceKey, reason, globalInboundSQLiteTime(now), raw)
	return err
}

func quarantineLegacyGlobalInboundItem(ctx context.Context, tx *sql.Tx, sourceKey string, item globalInboundItem, reason string, now time.Time) error {
	raw, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return quarantineGlobalInboundRawSQLiteTx(ctx, tx, sourceKey, raw, reason, now)
}

func globalInboundQuarantineExists(ctx context.Context, tx *sql.Tx, sourceKey string) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM inbound_ledger_quarantine WHERE source_key = ? LIMIT 1`, sourceKey).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil && exists == 1, err
}

// quarantineInvalidGlobalInboundSQLiteRows migrates rows written by older
// versions before strict projection validation existed. It runs once per
// sidecar, preserving every raw row in the quarantine table before removing
// it from the active claim table. A later claim therefore fails closed with a
// repair-visible error instead of repeatedly poisoning every normal claim
// transaction.
func quarantineInvalidGlobalInboundSQLiteRows(ctx context.Context, db *sql.DB, now time.Time) error {
	var migrated string
	err := db.QueryRowContext(ctx, `SELECT value FROM inbound_meta WHERE key = 'projection_quarantine_v1'`).Scan(&migrated)
	if err == nil && migrated == "1" {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT key, chat_id, message_id, owner, status, claimed_at, updated_at, json FROM inbound_ledger`)
	if err != nil {
		return err
	}
	type invalidRow struct {
		key, chatID, messageID string
		owner, status          string
		claimedAt, updatedAt   int64
		raw                    []byte
	}
	var invalid []invalidRow
	for rows.Next() {
		var row invalidRow
		if err := rows.Scan(&row.key, &row.chatID, &row.messageID, &row.owner, &row.status, &row.claimedAt, &row.updatedAt, &row.raw); err != nil {
			_ = rows.Close()
			return err
		}
		var item globalInboundItem
		valid := json.Unmarshal(row.raw, &item) == nil && validateGlobalInboundSQLiteProjection(globalInboundSQLiteRow{
			key: row.key, chatID: row.chatID, messageID: row.messageID, owner: row.owner, status: row.status,
			claimedAt: row.claimedAt, updatedAt: row.updatedAt, raw: row.raw,
		}, item) == nil
		if !valid {
			invalid = append(invalid, row)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, row := range invalid {
		sourceKey := globalInboundKey(strings.TrimSpace(row.chatID), strings.TrimSpace(row.messageID))
		if strings.TrimSpace(row.chatID) == "" || strings.TrimSpace(row.messageID) == "" {
			sourceKey = strings.TrimSpace(row.key)
		}
		if err := quarantineGlobalInboundRawSQLiteTx(ctx, tx, sourceKey, row.raw, "pre-existing active inbound projection is untrusted", now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM inbound_ledger WHERE key = ?`, row.key); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO inbound_meta(key, value) VALUES ('projection_quarantine_v1', '1') ON CONFLICT(key) DO UPDATE SET value = excluded.value`); err != nil {
		return err
	}
	return tx.Commit()
}

func legacyGlobalInboundItemWins(existing, incoming globalInboundItem) bool {
	// A durable completion is terminal for dedupe purposes.  A newer legacy
	// snapshot can still be stale in status even when its timestamp is later;
	// importing it over SQLite "done" would reopen the message for claim and
	// repeat the handler side effect.
	if existing.Status == "done" && incoming.Status != "done" {
		return false
	}
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
	if err := quarantineInvalidGlobalInboundSQLiteRows(ctx, db, now); err != nil {
		return err
	}
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
		item.ChatID = strings.TrimSpace(item.ChatID)
		item.MessageID = strings.TrimSpace(item.MessageID)
		canonicalKey := globalInboundKey(item.ChatID, item.MessageID)
		if validationErr := validateLegacyGlobalInboundItem(item); validationErr != nil {
			// Do not write an untrusted legacy item into the strict active table:
			// subsequent claim/complete calls would fail forever on the poisoned
			// projection. Keep a deterministic quarantine record instead, so the
			// evidence remains available and a matching message cannot be claimed
			// as if the old dedupe state never existed.
			if strings.TrimSpace(item.ChatID) == "" || strings.TrimSpace(item.MessageID) == "" {
				canonicalKey = strings.TrimSpace(key)
			}
			if err := quarantineLegacyGlobalInboundItem(ctx, tx, canonicalKey, item, validationErr.Error(), now); err != nil {
				return err
			}
			continue
		}
		// Legacy JSON used the map key as an index but did not always validate
		// it against the item's identity. Normalize a recoverable wrong key
		// before checking collisions; the canonical SQLite key is the value used
		// by every later claim/complete CAS.
		key = canonicalKey
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
		if _, err := tx.ExecContext(ctx, `DELETE FROM inbound_ledger_quarantine WHERE source_key = ?`, canonicalKey); err != nil {
			return err
		}
	}
	if err := pruneGlobalInboundSQLiteTx(ctx, tx, now); err != nil {
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
	var row globalInboundSQLiteRow
	err := tx.QueryRowContext(ctx, `SELECT key, chat_id, message_id, owner, status, claimed_at, updated_at, json FROM inbound_ledger WHERE key = ?`, key).Scan(
		&row.key, &row.chatID, &row.messageID, &row.owner, &row.status, &row.claimedAt, &row.updatedAt, &row.raw,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return globalInboundItem{}, false, nil
	}
	if err != nil {
		return globalInboundItem{}, false, err
	}
	var item globalInboundItem
	if err := json.Unmarshal(row.raw, &item); err != nil {
		return globalInboundItem{}, false, err
	}
	if err := validateGlobalInboundSQLiteProjection(row, item); err != nil {
		return globalInboundItem{}, false, err
	}
	return item, true, nil
}

type globalInboundSQLiteRow struct {
	key       string
	chatID    string
	messageID string
	owner     string
	status    string
	claimedAt int64
	updatedAt int64
	raw       []byte
}

func validateGlobalInboundSQLiteProjection(row globalInboundSQLiteRow, item globalInboundItem) error {
	if strings.TrimSpace(row.key) == "" || strings.TrimSpace(item.ChatID) == "" || strings.TrimSpace(item.MessageID) == "" ||
		globalInboundKey(item.ChatID, item.MessageID) != row.key ||
		strings.TrimSpace(row.chatID) != strings.TrimSpace(item.ChatID) ||
		strings.TrimSpace(row.messageID) != strings.TrimSpace(item.MessageID) ||
		strings.TrimSpace(row.owner) != strings.TrimSpace(item.Owner) ||
		strings.TrimSpace(row.status) != strings.TrimSpace(item.Status) ||
		!inboundSQLiteTimeMatches(row.claimedAt, item.ClaimedAt) ||
		!inboundSQLiteTimeMatches(row.updatedAt, item.UpdatedAt) {
		return fmt.Errorf("%w: scalar/canonical inbound row %q disagrees", ErrInboundLedgerProjectionUntrusted, strings.TrimSpace(row.key))
	}
	if item.Status != "claimed" && item.Status != "done" {
		return fmt.Errorf("%w: inbound row %q has unknown status %q", ErrInboundLedgerProjectionUntrusted, strings.TrimSpace(row.key), item.Status)
	}
	if item.Status == "claimed" && (item.ClaimedAt.IsZero() || item.UpdatedAt.IsZero()) {
		return fmt.Errorf("%w: claimed inbound row %q has no complete timestamp", ErrInboundLedgerProjectionUntrusted, strings.TrimSpace(row.key))
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(row.raw, &fields); err != nil || fields == nil {
		return fmt.Errorf("%w: inbound row %q is not a JSON object", ErrInboundLedgerProjectionUntrusted, strings.TrimSpace(row.key))
	}
	statusRaw, ok := fields["status"]
	if !ok || len(statusRaw) == 0 || string(statusRaw) == "null" {
		return fmt.Errorf("%w: inbound row %q has no canonical status", ErrInboundLedgerProjectionUntrusted, strings.TrimSpace(row.key))
	}
	var canonicalStatus string
	if err := json.Unmarshal(statusRaw, &canonicalStatus); err != nil || strings.TrimSpace(canonicalStatus) != strings.TrimSpace(item.Status) {
		return fmt.Errorf("%w: inbound row %q has an invalid canonical status", ErrInboundLedgerProjectionUntrusted, strings.TrimSpace(row.key))
	}
	return nil
}

func upsertGlobalInboundSQLiteTx(ctx context.Context, tx *sql.Tx, key string, item globalInboundItem) error {
	item.ChatID = strings.TrimSpace(item.ChatID)
	item.MessageID = strings.TrimSpace(item.MessageID)
	if item.ChatID == "" || item.MessageID == "" {
		return nil
	}
	canonicalKey := globalInboundKey(item.ChatID, item.MessageID)
	if strings.TrimSpace(key) == "" {
		key = canonicalKey
	} else if strings.TrimSpace(key) != canonicalKey {
		return fmt.Errorf("%w: inbound key %q does not match %q", ErrInboundLedgerProjectionUntrusted, key, canonicalKey)
	}
	raw, err := json.Marshal(item)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO inbound_ledger(key, chat_id, message_id, owner, status, claimed_at, updated_at, json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(key) DO UPDATE SET chat_id = excluded.chat_id, message_id = excluded.message_id, owner = excluded.owner, status = excluded.status, claimed_at = excluded.claimed_at, updated_at = excluded.updated_at, json = excluded.json`,
		key, item.ChatID, item.MessageID, item.Owner, item.Status, globalInboundSQLiteTime(item.ClaimedAt), globalInboundSQLiteTime(item.UpdatedAt), raw)
	return err
}

func insertGlobalInboundSQLiteTxIfMissing(ctx context.Context, tx *sql.Tx, key string, item globalInboundItem) error {
	item.ChatID = strings.TrimSpace(item.ChatID)
	item.MessageID = strings.TrimSpace(item.MessageID)
	if item.ChatID == "" || item.MessageID == "" {
		return nil
	}
	canonicalKey := globalInboundKey(item.ChatID, item.MessageID)
	if strings.TrimSpace(key) == "" {
		key = canonicalKey
	} else if strings.TrimSpace(key) != canonicalKey {
		return fmt.Errorf("%w: inbound key %q does not match %q", ErrInboundLedgerProjectionUntrusted, key, canonicalKey)
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
		globalInboundSQLiteTime(item.ClaimedAt),
		globalInboundSQLiteTime(item.UpdatedAt),
		raw,
	)
	return err
}

func pruneGlobalInboundSQLiteTx(ctx context.Context, tx *sql.Tx, now time.Time) error {
	const maxPruneScanRows = 512
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger`).Scan(&count); err != nil {
		return err
	}
	over := count - maxGlobalInboundLedgerIDs
	if over <= 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	cutoff := now.Add(-globalInboundClaimTTL)
	// Do not decide claim liveness from only one projection.  The JSON column is
	// the CAS source used by claim/complete, while the scalar columns are an
	// indexed compatibility projection.  A torn write can leave either one
	// newer than the other.  In particular, a canonical JSON "claimed" row must
	// not be deleted merely because its scalar status says "done".  Read the
	// bounded over-cap set and delete only rows for which every known projection
	// is unambiguously terminal or every claimed projection has a valid, expired
	// updated timestamp.  Malformed JSON, unknown status, or a claimed row with
	// no usable timestamp is retained fail-closed.
	type candidate struct {
		key      string
		at       time.Time
		priority int
	}
	candidates := make([]candidate, 0, count)
	var lastUpdated, lastClaimed int64
	var lastKey string
	hasCursor := false
	for {
		query := `SELECT key, chat_id, message_id, owner, status, claimed_at, updated_at, json FROM inbound_ledger`
		args := []any{}
		if hasCursor {
			query += ` WHERE updated_at > ? OR (updated_at = ? AND claimed_at > ?) OR (updated_at = ? AND claimed_at = ? AND key > ?)`
			args = append(args, lastUpdated, lastUpdated, lastClaimed, lastUpdated, lastClaimed, lastKey)
		}
		query += ` ORDER BY updated_at, claimed_at, key LIMIT ?`
		args = append(args, maxPruneScanRows)
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		pageRows := 0
		for rows.Next() {
			var item globalInboundSQLitePruneRow
			if err := rows.Scan(&item.key, &item.scalarChatID, &item.scalarMessageID, &item.scalarOwner, &item.scalarStatus, &item.scalarClaim, &item.scalarUpdate, &item.raw); err != nil {
				_ = rows.Close()
				return err
			}
			pageRows++
			lastUpdated = item.scalarUpdate
			lastClaimed = item.scalarClaim
			lastKey = item.key
			if prune, at, priority := inboundSQLitePruneEligibility(item, cutoff); prune {
				candidates = append(candidates, candidate{key: item.key, at: at, priority: priority})
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if pageRows == 0 || pageRows < maxPruneScanRows {
			break
		}
		hasCursor = true
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].priority != candidates[j].priority {
			return candidates[i].priority < candidates[j].priority
		}
		if candidates[i].at.Equal(candidates[j].at) {
			return candidates[i].key < candidates[j].key
		}
		return candidates[i].at.Before(candidates[j].at)
	})
	for i := 0; i < over && i < len(candidates); i++ {
		if _, err := tx.ExecContext(ctx, `DELETE FROM inbound_ledger WHERE key = ?`, candidates[i].key); err != nil {
			return err
		}
	}
	return nil
}

func pruneGlobalInboundSQLiteDB(ctx context.Context, db *sql.DB, now time.Time) error {
	if db == nil {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := pruneGlobalInboundSQLiteTx(ctx, tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

// inboundSQLitePruneEligibility returns whether a row is safe to evict from
// the bounded replay ledger.  It deliberately accepts the scalar projection
// as absent when the canonical JSON is complete, preserving old rows written
// before the scalar columns were introduced; it never accepts an unknown or
// malformed representation as evidence that a claim is disposable.
type globalInboundSQLitePruneRow struct {
	key             string
	scalarChatID    string
	scalarMessageID string
	scalarOwner     string
	scalarStatus    string
	scalarClaim     int64
	scalarUpdate    int64
	raw             []byte
}

func inboundSQLitePruneEligibility(row globalInboundSQLitePruneRow, cutoff time.Time) (bool, time.Time, int) {
	var item globalInboundItem
	if json.Unmarshal(row.raw, &item) != nil {
		return false, time.Time{}, 0
	}
	// Pruning is a dedupe-safety operation, not merely a retention operation.
	// Require the same complete identity/projection proof used by claim and
	// completion. A row whose key/chat/message/owner projection is torn must be
	// retained for repair; deleting it could make a previously handled Teams
	// message claimable again.
	if err := validateGlobalInboundSQLiteProjection(globalInboundSQLiteRow{
		key: row.key, chatID: row.scalarChatID, messageID: row.scalarMessageID,
		owner: row.scalarOwner, status: row.scalarStatus,
		claimedAt: row.scalarClaim, updatedAt: row.scalarUpdate, raw: row.raw,
	}, item); err != nil {
		return false, time.Time{}, 0
	}
	canonical, canonicalOK := decodeInboundPruneProjection(row.raw)
	if !canonicalOK {
		return false, time.Time{}, 0
	}
	scalarStatus := strings.TrimSpace(row.scalarStatus)
	if scalarStatus != "" && scalarStatus != "claimed" && scalarStatus != "done" {
		return false, time.Time{}, 0
	}
	if scalarStatus == "" || scalarStatus != canonical.status {
		return false, time.Time{}, 0
	}
	scalarClaimedAt := inboundLedgerUnixTime(row.scalarClaim)
	scalarUpdatedAt := inboundLedgerUnixTime(row.scalarUpdate)
	scalar := inboundPruneProjection{
		status:    scalarStatus,
		present:   scalarStatus != "",
		claimedAt: scalarClaimedAt,
		updatedAt: scalarUpdatedAt,
	}
	if !scalarClaimedAt.Equal(canonical.claimedAt) || !scalarUpdatedAt.Equal(canonical.updatedAt) {
		return false, time.Time{}, 0
	}
	projections := []inboundPruneProjection{canonical, scalar}
	oldest := time.Time{}
	for _, projection := range projections {
		if !projection.present {
			continue
		}
		if projection.status == "claimed" {
			// Claim TTL is defined from updated_at.  Do not infer freshness from
			// a missing/zero timestamp or from claimed_at alone.
			if projection.updatedAt.IsZero() || projection.updatedAt.After(cutoff) {
				return false, time.Time{}, 0
			}
			if oldest.IsZero() || projection.updatedAt.Before(oldest) {
				oldest = projection.updatedAt
			}
			continue
		}
		if projection.status != "done" {
			return false, time.Time{}, 0
		}
		if at := inboundPruneProjectionTime(projection); !at.IsZero() && (oldest.IsZero() || at.Before(oldest)) {
			oldest = at
		}
	}
	if oldest.IsZero() {
		// A complete but timestamp-less terminal row is safe to evict, but keep
		// ordering deterministic and after timestamped terminal rows.
		oldest = time.Unix(0, 0).UTC()
	}
	priority := 1
	if canonical.status == "done" || scalar.status == "done" {
		priority = 0
	}
	return true, oldest, priority
}

type inboundPruneProjection struct {
	status    string
	present   bool
	claimedAt time.Time
	updatedAt time.Time
}

func decodeInboundPruneProjection(raw []byte) (inboundPruneProjection, bool) {
	var object map[string]json.RawMessage
	if len(strings.TrimSpace(string(raw))) == 0 || json.Unmarshal(raw, &object) != nil || object == nil {
		return inboundPruneProjection{}, false
	}
	projection := inboundPruneProjection{}
	value, ok := object["status"]
	if !ok || len(value) == 0 || string(value) == "null" {
		return inboundPruneProjection{}, false
	}
	var status string
	if json.Unmarshal(value, &status) != nil {
		return inboundPruneProjection{}, false
	}
	status = strings.TrimSpace(status)
	if status != "claimed" && status != "done" {
		return inboundPruneProjection{}, false
	}
	projection.status = status
	projection.present = true
	for field, target := range map[string]*time.Time{"claimed_at": &projection.claimedAt, "updated_at": &projection.updatedAt} {
		value, ok := object[field]
		if !ok {
			continue
		}
		if len(value) == 0 || string(value) == "null" || json.Unmarshal(value, target) != nil {
			return inboundPruneProjection{}, false
		}
	}
	return projection, true
}

func inboundLedgerUnixTime(value int64) time.Time {
	if value == 0 || value == (time.Time{}).UnixNano() {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}

// globalInboundSQLiteTime is the canonical scalar representation for an
// optional time.  Older writers called UnixNano directly on a zero time,
// producing a negative year-one value.  New rows use zero, while readers keep
// accepting that legacy representation so migration cannot make a completed
// legacy row opaque.
func globalInboundSQLiteTime(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}

func inboundSQLiteTimeMatches(value int64, expected time.Time) bool {
	if expected.IsZero() {
		return value == 0 || value == (time.Time{}).UnixNano()
	}
	return value == expected.UnixNano()
}

func inboundPruneProjectionTime(projection inboundPruneProjection) time.Time {
	if !projection.updatedAt.IsZero() {
		return projection.updatedAt
	}
	return projection.claimedAt
}

func pruneGlobalInboundLedger(ledger *globalInboundLedger, now time.Time) {
	if ledger == nil || len(ledger.Items) <= maxGlobalInboundLedgerIDs {
		return
	}
	type entry struct {
		key      string
		at       time.Time
		priority int
	}
	entries := make([]entry, 0, len(ledger.Items))
	for key, item := range ledger.Items {
		if strings.TrimSpace(key) != globalInboundKey(item.ChatID, item.MessageID) ||
			(item.Status != "claimed" && item.Status != "done") {
			// A malformed/unknown legacy row is evidence, not safe eviction
			// material. Keep it until an explicit repair can inspect the raw file.
			continue
		}
		at := item.UpdatedAt
		if at.IsZero() {
			at = item.ClaimedAt
		}
		if at.IsZero() {
			at = now
		}
		if item.Status == "claimed" && (item.UpdatedAt.IsZero() || now.Sub(item.UpdatedAt) < globalInboundClaimTTL) {
			continue
		}
		priority := 1
		if item.Status == "done" {
			priority = 0
		}
		entries = append(entries, entry{key: key, at: at, priority: priority})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].priority != entries[j].priority {
			return entries[i].priority < entries[j].priority
		}
		if entries[i].at.Equal(entries[j].at) {
			return entries[i].key < entries[j].key
		}
		return entries[i].at.Before(entries[j].at)
	})
	for len(ledger.Items) > maxGlobalInboundLedgerIDs && len(entries) > 0 {
		delete(ledger.Items, entries[0].key)
		entries = entries[1:]
	}
}

func globalInboundKey(chatID string, messageID string) string {
	return strings.TrimSpace(chatID) + "\x00" + strings.TrimSpace(messageID)
}
