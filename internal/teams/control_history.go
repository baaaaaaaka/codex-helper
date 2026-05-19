package teams

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

	entries, err := readControlChatHistoryEntries(path)
	if err != nil {
		return err
	}
	for _, existing := range entries {
		if controlChatHistorySameMessage(existing, entry) {
			return nil
		}
	}
	entries = append(entries, entry)
	entries = pruneControlChatHistoryEntries(entries)
	return writeControlChatHistoryEntries(path, entries)
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

func writeControlChatHistoryEntries(path string, entries []controlChatHistoryEntry) error {
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	for _, entry := range entries {
		if err := enc.Encode(normalizeControlChatHistoryEntry(entry)); err != nil {
			return err
		}
	}
	return durableWriteFile(path, []byte(b.String()), 0o600)
}

func pruneControlChatHistoryEntries(entries []controlChatHistoryEntry) []controlChatHistoryEntry {
	if len(entries) <= maxControlChatHistoryEntries {
		return entries
	}
	return append([]controlChatHistoryEntry(nil), entries[len(entries)-maxControlChatHistoryEntries:]...)
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
