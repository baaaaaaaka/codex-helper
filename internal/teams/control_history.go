package teams

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/flock"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	controlChatHistoryFileName       = "control-chat-history.jsonl"
	controlChatHistoryLockTimeout    = 500 * time.Millisecond
	maxControlChatHistoryEntries     = 500
	maxControlChatHistoryPromptItems = 20
	maxControlChatHistoryPromptChars = 8000
	maxControlChatHistoryEntryBytes  = 12000
)

type controlChatHistoryEntry struct {
	Version   int       `json:"version"`
	ChatID    string    `json:"chat_id,omitempty"`
	MessageID string    `json:"message_id,omitempty"`
	Direction string    `json:"direction,omitempty"`
	Kind      string    `json:"kind,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	Text      string    `json:"text,omitempty"`
}

func controlChatHistoryPathForStore(store *teamstore.Store) string {
	if store == nil || strings.TrimSpace(store.Path()) == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(store.Path()), controlChatHistoryFileName)
}

func (b *Bridge) controlChatHistoryPath() string {
	if b == nil {
		return ""
	}
	return controlChatHistoryPathForStore(b.store)
}

func (b *Bridge) recordControlChatUserMessage(ctx context.Context, msg ChatMessage, text string) {
	if b == nil || strings.TrimSpace(b.reg.ControlChatID) == "" {
		return
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	_ = appendControlChatHistoryEntry(ctx, b.controlChatHistoryPath(), controlChatHistoryEntry{
		ChatID:    b.reg.ControlChatID,
		MessageID: msg.ID,
		Direction: "user",
		Kind:      "control_inbound",
		CreatedAt: messageSortTime(msg),
		Text:      text,
	})
}

func (b *Bridge) recordControlChatHelperMessage(ctx context.Context, msg teamstore.OutboxMessage) {
	if b == nil || strings.TrimSpace(b.reg.ControlChatID) == "" {
		return
	}
	if strings.TrimSpace(msg.TeamsChatID) != strings.TrimSpace(b.reg.ControlChatID) {
		return
	}
	text := strings.TrimSpace(msg.Body)
	if text == "" {
		return
	}
	if isWorkflowFallbackOutboxKind(msg.Kind) && workflowFallbackBodyLooksHTML(text) {
		text = PlainTextFromTeamsHTML(text)
	}
	_ = appendControlChatHistoryEntry(ctx, b.controlChatHistoryPath(), controlChatHistoryEntry{
		ChatID:    msg.TeamsChatID,
		MessageID: firstNonEmptyString(msg.TeamsMessageID, msg.ID),
		Direction: "helper",
		Kind:      firstNonEmptyString(msg.Kind, "helper"),
		CreatedAt: msg.CreatedAt,
		Text:      text,
	})
}

func appendControlChatHistoryEntry(ctx context.Context, path string, entry controlChatHistoryEntry) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	entry = normalizeControlChatHistoryEntry(entry)
	if strings.TrimSpace(entry.Text) == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	lock := flock.New(path + ".lock")
	ok, err := lock.TryLockContext(ctx, controlChatHistoryLockTimeout)
	if err != nil {
		return err
	}
	if !ok {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("control chat history is locked: %s", path)
	}
	defer func() { _ = lock.Unlock() }()

	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if err := ensureControlChatHistorySQLite(ctx, db); err != nil {
		return err
	}
	if err := importLegacyControlChatHistoryJSONL(ctx, db, path); err != nil {
		return err
	}
	return appendControlChatHistorySQLite(ctx, db, entry)
}

func normalizeControlChatHistoryEntry(entry controlChatHistoryEntry) controlChatHistoryEntry {
	entry.Version = 1
	entry.ChatID = strings.TrimSpace(entry.ChatID)
	entry.MessageID = strings.TrimSpace(entry.MessageID)
	entry.Direction = strings.TrimSpace(entry.Direction)
	entry.Kind = strings.TrimSpace(entry.Kind)
	if entry.Direction == "" {
		entry.Direction = "unknown"
	}
	if entry.Kind == "" {
		entry.Kind = entry.Direction
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now().UTC()
	}
	entry.Text = truncateStringByBytes(redactControlFallbackContext(strings.TrimSpace(entry.Text)), maxControlChatHistoryEntryBytes)
	return entry
}

func controlChatHistorySameMessage(a controlChatHistoryEntry, b controlChatHistoryEntry) bool {
	if strings.TrimSpace(a.MessageID) == "" || strings.TrimSpace(b.MessageID) == "" {
		return false
	}
	return strings.TrimSpace(a.ChatID) == strings.TrimSpace(b.ChatID) &&
		strings.TrimSpace(a.MessageID) == strings.TrimSpace(b.MessageID) &&
		strings.TrimSpace(a.Direction) == strings.TrimSpace(b.Direction) &&
		strings.TrimSpace(a.Kind) == strings.TrimSpace(b.Kind)
}

func readControlChatHistoryEntries(path string) ([]controlChatHistoryEntry, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	if entries, ok, err := readControlChatHistorySQLite(path); ok || err != nil {
		return entries, err
	}
	return readControlChatHistoryJSONL(path)
}

func readControlChatHistoryJSONL(path string) ([]controlChatHistoryEntry, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	var entries []controlChatHistoryEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), maxControlChatHistoryEntryBytes*8)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry controlChatHistoryEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, err
		}
		entries = append(entries, normalizeControlChatHistoryEntry(entry))
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func readControlChatHistorySQLite(path string) ([]controlChatHistoryEntry, bool, error) {
	sqlitePath := teamsLedgerSQLitePath(path)
	if sqlitePath == "" {
		return nil, false, nil
	}
	if _, err := os.Stat(sqlitePath); os.IsNotExist(err) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	db, err := openTeamsLedgerSQLite(sqlitePath)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = db.Close() }()
	if err := ensureControlChatHistorySQLite(context.Background(), db); err != nil {
		return nil, false, err
	}
	if err := importLegacyControlChatHistoryJSONL(context.Background(), db, path); err != nil {
		return nil, false, err
	}
	rows, err := db.Query(`SELECT json FROM control_history ORDER BY created_at ASC, rowid ASC`)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var entries []controlChatHistoryEntry
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, false, err
		}
		var entry controlChatHistoryEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, false, err
		}
		entries = append(entries, normalizeControlChatHistoryEntry(entry))
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return entries, true, nil
}

func ensureControlChatHistorySQLite(ctx context.Context, db *sql.DB) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS control_history_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS control_history (key TEXT PRIMARY KEY, chat_id TEXT NOT NULL, message_id TEXT NOT NULL, direction TEXT NOT NULL, kind TEXT NOT NULL, created_at INTEGER NOT NULL, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS control_history_created_idx ON control_history(created_at, key)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func importLegacyControlChatHistoryJSONL(ctx context.Context, db *sql.DB, path string) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		_, err = db.ExecContext(ctx, `INSERT INTO control_history_meta(key, value) VALUES ('legacy_jsonl_token', '') ON CONFLICT(key) DO UPDATE SET value = excluded.value`)
		return err
	}
	if err != nil {
		return err
	}
	token := fmt.Sprintf("%d:%d", info.Size(), info.ModTime().UnixNano())
	var existing string
	err = db.QueryRowContext(ctx, `SELECT value FROM control_history_meta WHERE key = 'legacy_jsonl_token'`).Scan(&existing)
	if err == nil && existing == token {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	entries, err := readControlChatHistoryJSONL(path)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, entry := range entries {
		if err := upsertControlChatHistorySQLiteTx(ctx, tx, entry); err != nil {
			return err
		}
	}
	if err := pruneControlChatHistorySQLiteTx(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO control_history_meta(key, value) VALUES ('legacy_jsonl_token', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, token); err != nil {
		return err
	}
	return tx.Commit()
}

func appendControlChatHistorySQLite(ctx context.Context, db *sql.DB, entry controlChatHistoryEntry) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if strings.TrimSpace(entry.MessageID) != "" {
		var raw []byte
		err := tx.QueryRowContext(ctx, `SELECT json FROM control_history WHERE key = ?`, controlChatHistoryEntryKey(entry)).Scan(&raw)
		if err == nil {
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	if err := upsertControlChatHistorySQLiteTx(ctx, tx, entry); err != nil {
		return err
	}
	if err := pruneControlChatHistorySQLiteTx(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func upsertControlChatHistorySQLiteTx(ctx context.Context, tx *sql.Tx, entry controlChatHistoryEntry) error {
	entry = normalizeControlChatHistoryEntry(entry)
	key := controlChatHistoryEntryKey(entry)
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO control_history(key, chat_id, message_id, direction, kind, created_at, json)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(key) DO UPDATE SET chat_id = excluded.chat_id, message_id = excluded.message_id, direction = excluded.direction, kind = excluded.kind, created_at = excluded.created_at, json = excluded.json`,
		key, entry.ChatID, entry.MessageID, entry.Direction, entry.Kind, entry.CreatedAt.UnixNano(), raw)
	return err
}

func pruneControlChatHistorySQLiteTx(ctx context.Context, tx *sql.Tx) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM control_history`).Scan(&count); err != nil {
		return err
	}
	over := count - maxControlChatHistoryEntries
	if over <= 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM control_history WHERE key IN (
SELECT key FROM control_history ORDER BY created_at ASC, rowid ASC LIMIT ?
)`, over)
	return err
}

func controlChatHistoryEntryKey(entry controlChatHistoryEntry) string {
	entry = normalizeControlChatHistoryEntry(entry)
	if strings.TrimSpace(entry.MessageID) != "" {
		return strings.Join([]string{entry.ChatID, entry.MessageID, entry.Direction, entry.Kind}, "\x00")
	}
	return strings.Join([]string{"anonymous", entry.ChatID, entry.Direction, entry.Kind, strconv.FormatInt(entry.CreatedAt.UnixNano(), 10), normalizedTextHash(entry.Text)}, "\x00")
}

func (b *Bridge) controlChatHistoryPromptContext(excludeMessageID string) (string, string) {
	path := b.controlChatHistoryPath()
	if path == "" {
		return "", ""
	}
	entries, err := readControlChatHistoryEntries(path)
	if err != nil {
		return path, ""
	}
	chatID := ""
	if b != nil {
		chatID = strings.TrimSpace(b.reg.ControlChatID)
	}
	excludeMessageID = strings.TrimSpace(excludeMessageID)
	var filtered []controlChatHistoryEntry
	for _, entry := range entries {
		if chatID != "" && strings.TrimSpace(entry.ChatID) != chatID {
			continue
		}
		if excludeMessageID != "" && strings.TrimSpace(entry.MessageID) == excludeMessageID {
			continue
		}
		filtered = append(filtered, entry)
	}
	if len(filtered) > maxControlChatHistoryPromptItems {
		filtered = filtered[len(filtered)-maxControlChatHistoryPromptItems:]
	}
	return path, formatControlChatHistoryPromptTail(filtered)
}

func formatControlChatHistoryPromptTail(entries []controlChatHistoryEntry) string {
	var lines []string
	for _, entry := range entries {
		text := strings.TrimSpace(entry.Text)
		if text == "" {
			continue
		}
		at := entry.CreatedAt.UTC().Format(time.RFC3339)
		lines = append(lines, fmt.Sprintf("- %s %s/%s:", at, firstNonEmptyString(entry.Direction, "unknown"), firstNonEmptyString(entry.Kind, "message")))
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			lines = append(lines, "  "+line)
		}
	}
	return truncateControlFallbackContext(strings.Join(lines, "\n"), maxControlChatHistoryPromptChars)
}
