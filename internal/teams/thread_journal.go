package teams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

const (
	threadLinkJournalVersion       = 1
	threadLinkJournalLockTimeout   = 500 * time.Millisecond
	threadLinkJournalMaxReplayByte = 1024 * 1024
	threadLinkJournalMaxLineByte   = 64 * 1024
)

type threadLinkJournalRecord struct {
	Version       int       `json:"version"`
	EventType     string    `json:"event_type,omitempty"`
	Source        string    `json:"source,omitempty"`
	ScopeID       string    `json:"scope_id,omitempty"`
	MachineID     string    `json:"machine_id,omitempty"`
	SessionID     string    `json:"session_id,omitempty"`
	ChatID        string    `json:"chat_id,omitempty"`
	TeamsTurnID   string    `json:"teams_turn_id,omitempty"`
	CodexThreadID string    `json:"codex_thread_id,omitempty"`
	CodexTurnID   string    `json:"codex_turn_id,omitempty"`
	Cwd           string    `json:"cwd,omitempty"`
	Diagnostic    string    `json:"diagnostic,omitempty"`
	ObservedAt    time.Time `json:"observed_at,omitempty"`
}

func (b *Bridge) appendThreadLinkJournal(ctx context.Context, rec threadLinkJournalRecord) error {
	if b == nil || b.store == nil || strings.TrimSpace(rec.SessionID) == "" || strings.TrimSpace(rec.CodexThreadID) == "" {
		return nil
	}
	path := b.threadLinkJournalPath(rec.SessionID)
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if err := ensurePrivateDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	lock := flock.New(path + ".lock")
	ok, err := lock.TryLockContext(ctx, threadLinkJournalLockTimeout)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("thread-link journal lock timeout")
	}
	defer func() { _ = lock.Unlock() }()
	if rec.Version == 0 {
		rec.Version = threadLinkJournalVersion
	}
	if rec.EventType == "" {
		rec.EventType = "thread_link"
	}
	if rec.ObservedAt.IsZero() {
		rec.ObservedAt = time.Now()
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(line); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (b *Bridge) readThreadLinkJournal(ctx context.Context, sessionID string) ([]threadLinkJournalRecord, error) {
	if b == nil || b.store == nil || strings.TrimSpace(sessionID) == "" {
		return nil, nil
	}
	path := b.threadLinkJournalPath(sessionID)
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	limited := io.LimitReader(f, threadLinkJournalMaxReplayByte+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(data) > threadLinkJournalMaxReplayByte {
		return nil, errors.New("thread-link journal replay budget exceeded")
	}
	if len(data) == 0 {
		return nil, nil
	}
	if data[len(data)-1] != '\n' {
		if idx := bytes.LastIndexByte(data, '\n'); idx >= 0 {
			data = data[:idx+1]
		} else {
			return nil, nil
		}
	}
	var out []threadLinkJournalRecord
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || len(line) > threadLinkJournalMaxLineByte {
			continue
		}
		var rec threadLinkJournalRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			continue
		}
		if strings.TrimSpace(rec.SessionID) != strings.TrimSpace(sessionID) || strings.TrimSpace(rec.CodexThreadID) == "" {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

func (b *Bridge) threadLinkJournalPath(sessionID string) string {
	if b == nil || b.store == nil || strings.TrimSpace(b.store.Path()) == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(sessionID)))
	name := hex.EncodeToString(sum[:16]) + ".jsonl"
	return filepath.Join(filepath.Dir(b.store.Path()), "thread-links", name)
}
