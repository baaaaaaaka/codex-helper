package codexhistory

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type historyIndex struct {
	sessions map[string]*historySessionInfo
}

type historySessionInfo struct {
	FirstPrompt     string
	FirstPromptTime time.Time
}

// codexHistoryEntry maps to a line in ~/.codex/history.jsonl:
//
//	{"session_id":"uuid","ts":1770777540,"text":"user input"}
type codexHistoryEntry struct {
	SessionID string `json:"session_id"`
	Ts        int64  `json:"ts"`
	Text      string `json:"text"`
}

var openHistoryIndexFile = os.Open

func loadHistoryIndex(root string) historyIndex {
	idx, _ := loadHistoryIndexContext(context.Background(), root)
	return idx
}

func loadHistoryIndexContext(ctx context.Context, root string) (historyIndex, error) {
	idx := historyIndex{sessions: map[string]*historySessionInfo{}}
	if err := ctx.Err(); err != nil {
		return idx, err
	}
	path := filepath.Join(root, "history.jsonl")
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := deletePersistentHistoryIndexContext(ctx, path); err != nil {
				return idx, err
			}
			return idx, nil
		}
		return idx, nil
	}
	if cached, ok, err := readPersistentHistoryIndexContext(ctx, path, info); err != nil {
		return idx, err
	} else if ok {
		return cached, nil
	}

	f, err := openHistoryIndexFile(path)
	if err != nil {
		return idx, nil
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		if err := ctx.Err(); err != nil {
			return idx, err
		}
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return idx, nil
		}
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			var entry codexHistoryEntry
			if json.Unmarshal(line, &entry) == nil && entry.SessionID != "" {
				info := idx.sessions[entry.SessionID]
				if info == nil {
					info = &historySessionInfo{}
					idx.sessions[entry.SessionID] = info
				}
				text := strings.TrimSpace(entry.Text)
				if text != "" && !shouldSkipFirstPrompt(text) {
					ts := time.Unix(entry.Ts, 0)
					if info.FirstPrompt == "" || (!ts.IsZero() && (info.FirstPromptTime.IsZero() || ts.Before(info.FirstPromptTime))) {
						info.FirstPrompt = text
						info.FirstPromptTime = ts
					}
				}
			}
		}
		if err == io.EOF {
			break
		}
	}
	if err := writePersistentHistoryIndexContext(ctx, path, info, idx); err != nil {
		return idx, err
	}
	return idx, nil
}

func (idx historyIndex) lookup(sessionID string) (historySessionInfo, bool) {
	if sessionID == "" || idx.sessions == nil {
		return historySessionInfo{}, false
	}
	info, ok := idx.sessions[sessionID]
	if !ok || info == nil {
		return historySessionInfo{}, false
	}
	return *info, true
}
