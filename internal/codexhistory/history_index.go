package codexhistory

import (
	"bufio"
	"bytes"
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

func loadHistoryIndex(root string) historyIndex {
	idx := historyIndex{sessions: map[string]*historySessionInfo{}}
	path := filepath.Join(root, "history.jsonl")
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return idx
		}
		return idx
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return idx
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
	return idx
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
