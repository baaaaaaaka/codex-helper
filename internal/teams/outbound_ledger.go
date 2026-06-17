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

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	globalOutboundLockTimeout  = 500 * time.Millisecond
	maxGlobalOutboundLedgerIDs = 32768
)

type globalOutboundLedger struct {
	Version int                           `json:"version"`
	Items   map[string]globalOutboundItem `json:"items,omitempty"`
}

type globalOutboundItem struct {
	ChatID         string    `json:"chat_id"`
	MessageID      string    `json:"message_id"`
	ScopeID        string    `json:"scope_id,omitempty"`
	MachineID      string    `json:"machine_id,omitempty"`
	OutboxID       string    `json:"outbox_id,omitempty"`
	SessionID      string    `json:"session_id,omitempty"`
	TurnID         string    `json:"turn_id,omitempty"`
	Kind           string    `json:"kind,omitempty"`
	Origin         string    `json:"origin,omitempty"`
	RecordedAt     time.Time `json:"recorded_at,omitempty"`
	TeamsCreatedAt time.Time `json:"teams_created_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

func globalOutboundLedgerPathForRegistry(registryPath string) (string, bool) {
	return globalTeamsLedgerPathForScopedFile(registryPath, "registry.json", "global-outbound-ledger.json", "teams-global-outbound-ledger.json")
}

func globalOutboundLedgerPathForStore(storePath string) (string, bool) {
	return globalTeamsLedgerPathForScopedFile(storePath, "state.json", "global-outbound-ledger.json", "teams-global-outbound-ledger.json")
}

func globalTeamsLedgerPathForScopedFile(path string, fileName string, scopedLedgerName string, fallbackLedgerName string) (string, bool) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", false
	}
	clean := filepath.Clean(path)
	dir := filepath.Dir(clean)
	if filepath.Base(clean) == fileName && filepath.Base(filepath.Dir(dir)) == "scopes" {
		return filepath.Join(filepath.Dir(filepath.Dir(dir)), scopedLedgerName), true
	}
	return filepath.Join(dir, fallbackLedgerName), true
}

func (b *Bridge) globalOutboundLedgerPath() (string, bool) {
	if b == nil {
		return "", false
	}
	if path, ok := globalOutboundLedgerPathForRegistry(b.registryPath); ok {
		return path, true
	}
	if b.store != nil {
		return globalOutboundLedgerPathForStore(b.store.Path())
	}
	return "", false
}

func (b *Bridge) hasGlobalOutboundMessage(ctx context.Context, chatID string, messageID string) (bool, error) {
	chatID = strings.TrimSpace(chatID)
	messageID = strings.TrimSpace(messageID)
	if b == nil || chatID == "" || messageID == "" {
		return false, nil
	}
	path, ok := b.globalOutboundLedgerPath()
	if !ok {
		return false, nil
	}
	if err := b.ensureGlobalOutboundBackfilled(ctx, path); err != nil {
		return false, err
	}
	return hasGlobalOutboundLedgerItem(ctx, path, chatID, messageID)
}

func (b *Bridge) recordGlobalOutboundMessage(ctx context.Context, outbox teamstore.OutboxMessage, msg ChatMessage) error {
	if b == nil {
		return nil
	}
	messageID := strings.TrimSpace(firstNonEmptyString(msg.ID, outbox.TeamsMessageID))
	if strings.TrimSpace(outbox.TeamsChatID) == "" || messageID == "" {
		return nil
	}
	path, ok := b.globalOutboundLedgerPath()
	if !ok {
		return nil
	}
	item := globalOutboundItem{
		ChatID:         outbox.TeamsChatID,
		MessageID:      messageID,
		ScopeID:        firstNonEmptyString(outbox.ScopeID, b.scope.ID),
		MachineID:      firstNonEmptyString(outbox.MachineID, b.machine.ID),
		OutboxID:       outbox.ID,
		SessionID:      outbox.SessionID,
		TurnID:         outbox.TurnID,
		Kind:           outbox.Kind,
		Origin:         teamstore.MessageOriginHelperOutbox,
		RecordedAt:     firstNonZeroTime(outbox.SentAt, outbox.UpdatedAt, outbox.CreatedAt),
		TeamsCreatedAt: parseGraphTime(msg.CreatedDateTime),
	}
	return recordGlobalOutbound(ctx, path, item, time.Now())
}

func (b *Bridge) recordGlobalOutboundSuppressionProvenance(ctx context.Context, chatID string, messageID string) {
	if b == nil || b.store == nil {
		return
	}
	_, err := b.store.RecordMessageProvenance(ctx, teamstore.MessageProvenanceRecord{
		TeamsChatID:    chatID,
		TeamsMessageID: messageID,
		Origin:         teamstore.MessageOriginHelperOutbox,
		Diagnostic:     "global_outbound_ledger",
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	})
	if err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams global outbound provenance record error: %v\n", err)
	}
}

func (b *Bridge) ensureGlobalOutboundBackfilled(ctx context.Context, path string) error {
	if b == nil || strings.TrimSpace(path) == "" {
		return nil
	}
	b.globalOutboundMu.Lock()
	defer b.globalOutboundMu.Unlock()
	if b.globalOutboundBackfilled {
		return nil
	}
	records, err := b.globalOutboundBackfillItems(ctx)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		b.globalOutboundBackfilled = true
		return nil
	}
	if err := recordGlobalOutboundBatch(ctx, path, records, time.Now()); err != nil {
		return err
	}
	b.globalOutboundBackfilled = true
	return nil
}

func (b *Bridge) globalOutboundBackfillItems(ctx context.Context) ([]globalOutboundItem, error) {
	if b == nil || b.store == nil {
		return nil, nil
	}
	paths, err := siblingScopeStorePaths(b.store.Path())
	if err != nil {
		return nil, err
	}
	var out []globalOutboundItem
	for _, path := range paths {
		st, err := teamstore.Open(path)
		if err != nil {
			return nil, err
		}
		state, loadErr := st.Load(ctx)
		closeErr := st.Close()
		if loadErr != nil {
			return nil, loadErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if !b.globalOutboundBackfillStateMatches(state) {
			continue
		}
		for _, msg := range state.OutboxMessages {
			switch msg.Status {
			case teamstore.OutboxStatusAccepted, teamstore.OutboxStatusSent:
			default:
				continue
			}
			if strings.TrimSpace(msg.TeamsChatID) == "" || strings.TrimSpace(msg.TeamsMessageID) == "" {
				continue
			}
			out = append(out, globalOutboundItem{
				ChatID:     msg.TeamsChatID,
				MessageID:  msg.TeamsMessageID,
				ScopeID:    firstNonEmptyString(msg.ScopeID, state.Scope.ID),
				MachineID:  firstNonEmptyString(msg.MachineID, state.MachineIdentity.ID),
				OutboxID:   msg.ID,
				SessionID:  msg.SessionID,
				TurnID:     msg.TurnID,
				Kind:       msg.Kind,
				Origin:     teamstore.MessageOriginHelperOutbox,
				RecordedAt: firstNonZeroTime(msg.SentAt, msg.UpdatedAt, msg.CreatedAt),
			})
		}
		for _, record := range state.MessageProvenance {
			if strings.TrimSpace(record.Origin) != teamstore.MessageOriginHelperOutbox {
				continue
			}
			if strings.TrimSpace(record.TeamsChatID) == "" || strings.TrimSpace(record.TeamsMessageID) == "" {
				continue
			}
			out = append(out, globalOutboundItem{
				ChatID:     record.TeamsChatID,
				MessageID:  record.TeamsMessageID,
				ScopeID:    state.Scope.ID,
				MachineID:  state.MachineIdentity.ID,
				OutboxID:   record.OutboxID,
				SessionID:  record.SessionID,
				TurnID:     record.TurnID,
				Kind:       record.Kind,
				Origin:     teamstore.MessageOriginHelperOutbox,
				RecordedAt: firstNonZeroTime(record.UpdatedAt, record.CreatedAt),
			})
		}
	}
	return out, nil
}

func (b *Bridge) globalOutboundBackfillStateMatches(state teamstore.State) bool {
	if b == nil {
		return false
	}
	if scopeStateMatches(b.scope, state) {
		return true
	}
	controlChatID := strings.TrimSpace(firstNonEmptyString(b.reg.ControlChatID, state.ControlChat.TeamsChatID))
	if controlChatID == "" || strings.TrimSpace(state.ControlChat.TeamsChatID) != controlChatID {
		return false
	}
	if strings.TrimSpace(b.scope.Profile) != "" && strings.TrimSpace(state.ControlChat.Profile) != "" && strings.TrimSpace(b.scope.Profile) != strings.TrimSpace(state.ControlChat.Profile) {
		return false
	}
	if strings.TrimSpace(b.user.ID) != "" && strings.TrimSpace(state.ControlChat.AccountID) != "" && strings.TrimSpace(b.user.ID) != strings.TrimSpace(state.ControlChat.AccountID) {
		return false
	}
	return strings.TrimSpace(state.ControlChat.ScopeID) != "" || strings.TrimSpace(state.ControlChat.AccountID) != ""
}

func siblingScopeStorePaths(currentPath string) ([]string, error) {
	currentPath = strings.TrimSpace(currentPath)
	if currentPath == "" {
		return nil, nil
	}
	seen := map[string]bool{}
	var paths []string
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		clean := filepath.Clean(path)
		if seen[clean] {
			return
		}
		seen[clean] = true
		paths = append(paths, clean)
	}
	clean := filepath.Clean(currentPath)
	add(clean)
	dir := filepath.Dir(clean)
	if filepath.Base(clean) == "state.json" && filepath.Base(filepath.Dir(dir)) == "scopes" {
		matches, err := filepath.Glob(filepath.Join(filepath.Dir(dir), "*", "state.json"))
		if err != nil {
			return nil, err
		}
		sort.Strings(matches)
		for _, path := range matches {
			add(path)
		}
	}
	return paths, nil
}

func recordGlobalOutbound(ctx context.Context, path string, item globalOutboundItem, now time.Time) error {
	if strings.TrimSpace(path) == "" || strings.TrimSpace(item.ChatID) == "" || strings.TrimSpace(item.MessageID) == "" {
		return nil
	}
	return recordGlobalOutboundBatch(ctx, path, []globalOutboundItem{item}, now)
}

func recordGlobalOutboundBatch(ctx context.Context, path string, items []globalOutboundItem, now time.Time) error {
	if strings.TrimSpace(path) == "" || len(items) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	lock := flock.New(path + ".lock")
	ok, err := lock.TryLockContext(ctx, globalOutboundLockTimeout)
	if err != nil {
		return err
	}
	if !ok {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("global Teams outbound ledger is locked: %s", path)
	}
	defer func() { _ = lock.Unlock() }()
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if err := ensureGlobalOutboundSQLite(ctx, db); err != nil {
		return err
	}
	if err := importLegacyGlobalOutboundJSON(ctx, db, path, now); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, item := range items {
		if err := upsertGlobalOutboundSQLiteTx(ctx, tx, item, now); err != nil {
			return err
		}
	}
	if err := pruneGlobalOutboundSQLiteTx(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func readGlobalOutboundLedger(path string) (globalOutboundLedger, error) {
	if ledger, ok, err := readGlobalOutboundSQLite(path); ok || err != nil {
		return ledger, err
	}
	var ledger globalOutboundLedger
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		ledger.Version = 1
		ledger.Items = map[string]globalOutboundItem{}
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
		ledger.Items = map[string]globalOutboundItem{}
	}
	return ledger, nil
}

func hasGlobalOutboundLedgerItem(ctx context.Context, path string, chatID string, messageID string) (bool, error) {
	path = strings.TrimSpace(path)
	chatID = strings.TrimSpace(chatID)
	messageID = strings.TrimSpace(messageID)
	if path == "" || chatID == "" || messageID == "" {
		return false, nil
	}
	lock := flock.New(path + ".lock")
	ok, err := lock.TryLockContext(ctx, globalOutboundLockTimeout)
	if err != nil {
		return false, err
	}
	if !ok {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		return false, fmt.Errorf("global Teams outbound ledger is locked: %s", path)
	}
	defer func() { _ = lock.Unlock() }()
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		return false, err
	}
	defer func() { _ = db.Close() }()
	if err := ensureGlobalOutboundSQLite(ctx, db); err != nil {
		return false, err
	}
	if err := importLegacyGlobalOutboundJSON(ctx, db, path, time.Now()); err != nil {
		return false, err
	}
	var raw []byte
	err = db.QueryRowContext(ctx, `SELECT json FROM outbound_ledger WHERE key = ?`, globalOutboundKey(chatID, messageID)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func readGlobalOutboundSQLite(path string) (globalOutboundLedger, bool, error) {
	var ledger globalOutboundLedger
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
	if err := ensureGlobalOutboundSQLite(context.Background(), db); err != nil {
		return ledger, false, err
	}
	if err := importLegacyGlobalOutboundJSON(context.Background(), db, path, time.Now()); err != nil {
		return ledger, false, err
	}
	rows, err := db.Query(`SELECT json FROM outbound_ledger`)
	if err != nil {
		return ledger, false, err
	}
	defer rows.Close()
	ledger.Version = 1
	ledger.Items = map[string]globalOutboundItem{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return ledger, false, err
		}
		var item globalOutboundItem
		if err := json.Unmarshal(raw, &item); err != nil {
			return ledger, false, err
		}
		if strings.TrimSpace(item.ChatID) == "" || strings.TrimSpace(item.MessageID) == "" {
			continue
		}
		ledger.Items[globalOutboundKey(item.ChatID, item.MessageID)] = item
	}
	if err := rows.Err(); err != nil {
		return ledger, false, err
	}
	return ledger, true, nil
}

func ensureGlobalOutboundSQLite(ctx context.Context, db *sql.DB) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS outbound_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS outbound_ledger (key TEXT PRIMARY KEY, chat_id TEXT NOT NULL, message_id TEXT NOT NULL, updated_at INTEGER NOT NULL, recorded_at INTEGER NOT NULL, teams_created_at INTEGER NOT NULL, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS outbound_ledger_prune_idx ON outbound_ledger(updated_at, recorded_at, teams_created_at, key)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func importLegacyGlobalOutboundJSON(ctx context.Context, db *sql.DB, path string, now time.Time) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		_, err = db.ExecContext(ctx, `INSERT INTO outbound_meta(key, value) VALUES ('legacy_json_token', '') ON CONFLICT(key) DO UPDATE SET value = excluded.value`)
		return err
	}
	if err != nil {
		return err
	}
	token := fmt.Sprintf("%d:%d", info.Size(), info.ModTime().UnixNano())
	var existing string
	err = db.QueryRowContext(ctx, `SELECT value FROM outbound_meta WHERE key = 'legacy_json_token'`).Scan(&existing)
	if err == nil && existing == token {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	legacy, err := readGlobalOutboundJSON(path)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, item := range legacy.Items {
		if err := upsertGlobalOutboundSQLiteTx(ctx, tx, item, now); err != nil {
			return err
		}
	}
	if err := pruneGlobalOutboundSQLiteTx(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbound_meta(key, value) VALUES ('legacy_json_token', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, token); err != nil {
		return err
	}
	return tx.Commit()
}

func readGlobalOutboundJSON(path string) (globalOutboundLedger, error) {
	var ledger globalOutboundLedger
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		ledger.Version = 1
		ledger.Items = map[string]globalOutboundItem{}
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
		ledger.Items = map[string]globalOutboundItem{}
	}
	return ledger, nil
}

func upsertGlobalOutboundSQLiteTx(ctx context.Context, tx *sql.Tx, item globalOutboundItem, now time.Time) error {
	item.ChatID = strings.TrimSpace(item.ChatID)
	item.MessageID = strings.TrimSpace(item.MessageID)
	if item.ChatID == "" || item.MessageID == "" {
		return nil
	}
	key := globalOutboundKey(item.ChatID, item.MessageID)
	if item.RecordedAt.IsZero() {
		item.RecordedAt = now
	}
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = now
	}
	raw, err := json.Marshal(item)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO outbound_ledger(key, chat_id, message_id, updated_at, recorded_at, teams_created_at, json)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		key, item.ChatID, item.MessageID, item.UpdatedAt.UnixNano(), item.RecordedAt.UnixNano(), item.TeamsCreatedAt.UnixNano(), raw)
	if err != nil {
		return err
	}
	if rows, err := res.RowsAffected(); err == nil && rows == 1 {
		return nil
	}
	err = tx.QueryRowContext(ctx, `SELECT json FROM outbound_ledger WHERE key = ?`, key).Scan(&raw)
	if err == nil {
		var existing globalOutboundItem
		if err := json.Unmarshal(raw, &existing); err != nil {
			return err
		}
		item = mergeGlobalOutboundItem(existing, item)
		item.UpdatedAt = now
	} else if errors.Is(err, sql.ErrNoRows) {
		return nil
	} else {
		return err
	}
	raw, err = json.Marshal(item)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbound_ledger(key, chat_id, message_id, updated_at, recorded_at, teams_created_at, json)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(key) DO UPDATE SET chat_id = excluded.chat_id, message_id = excluded.message_id, updated_at = excluded.updated_at, recorded_at = excluded.recorded_at, teams_created_at = excluded.teams_created_at, json = excluded.json`,
		key, item.ChatID, item.MessageID, item.UpdatedAt.UnixNano(), item.RecordedAt.UnixNano(), item.TeamsCreatedAt.UnixNano(), raw)
	return err
}

func pruneGlobalOutboundSQLiteTx(ctx context.Context, tx *sql.Tx) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbound_ledger`).Scan(&count); err != nil {
		return err
	}
	over := count - maxGlobalOutboundLedgerIDs
	if over <= 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM outbound_ledger WHERE key IN (
SELECT key FROM outbound_ledger ORDER BY updated_at ASC, recorded_at ASC, teams_created_at ASC, key ASC LIMIT ?
)`, over)
	return err
}

func upsertGlobalOutboundItem(ledger *globalOutboundLedger, item globalOutboundItem, now time.Time) {
	if ledger == nil {
		return
	}
	if ledger.Items == nil {
		ledger.Items = map[string]globalOutboundItem{}
	}
	item.ChatID = strings.TrimSpace(item.ChatID)
	item.MessageID = strings.TrimSpace(item.MessageID)
	if item.ChatID == "" || item.MessageID == "" {
		return
	}
	if item.RecordedAt.IsZero() {
		item.RecordedAt = now
	}
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = now
	}
	key := globalOutboundKey(item.ChatID, item.MessageID)
	existing, ok := ledger.Items[key]
	if ok {
		item = mergeGlobalOutboundItem(existing, item)
		item.UpdatedAt = now
	}
	ledger.Items[key] = item
}

func mergeGlobalOutboundItem(existing globalOutboundItem, next globalOutboundItem) globalOutboundItem {
	if next.ChatID == "" {
		next.ChatID = existing.ChatID
	}
	if next.MessageID == "" {
		next.MessageID = existing.MessageID
	}
	if next.ScopeID == "" {
		next.ScopeID = existing.ScopeID
	}
	if next.MachineID == "" {
		next.MachineID = existing.MachineID
	}
	if next.OutboxID == "" {
		next.OutboxID = existing.OutboxID
	}
	if next.SessionID == "" {
		next.SessionID = existing.SessionID
	}
	if next.TurnID == "" {
		next.TurnID = existing.TurnID
	}
	if next.Kind == "" {
		next.Kind = existing.Kind
	}
	if next.Origin == "" {
		next.Origin = existing.Origin
	}
	if next.RecordedAt.IsZero() || (!existing.RecordedAt.IsZero() && existing.RecordedAt.Before(next.RecordedAt)) {
		next.RecordedAt = existing.RecordedAt
	}
	if next.TeamsCreatedAt.IsZero() {
		next.TeamsCreatedAt = existing.TeamsCreatedAt
	}
	return next
}

func pruneGlobalOutboundLedger(ledger *globalOutboundLedger, now time.Time) {
	if ledger == nil || len(ledger.Items) <= maxGlobalOutboundLedgerIDs {
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
			at = item.RecordedAt
		}
		if at.IsZero() {
			at = item.TeamsCreatedAt
		}
		if at.IsZero() {
			at = now
		}
		entries = append(entries, entry{key: key, at: at})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].at.Before(entries[j].at) })
	for len(entries) > maxGlobalOutboundLedgerIDs {
		delete(ledger.Items, entries[0].key)
		entries = entries[1:]
	}
}

func globalOutboundKey(chatID string, messageID string) string {
	return strings.TrimSpace(chatID) + "\x00" + strings.TrimSpace(messageID)
}
