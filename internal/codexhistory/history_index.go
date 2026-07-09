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
var historyIndexWindowReadHook func(offset int64, size int64)

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
			return idx, nil
		}
		return idx, nil
	}

	completeOffset, complete := sessionPreviewCompleteOffset(path, info)
	if !complete || completeOffset < info.Size() {
		parsed, _, readErr := readHistoryIndexWindowContext(ctx, path, 0, info.Size(), nil)
		if readErr != nil {
			return idx, nil
		}
		return historyIndex{sessions: parsed}, nil
	}

	store, storeErr := currentCatalogSQLiteStore(path)
	if isContextError(storeErr) {
		return idx, storeErr
	}
	if storeErr != nil {
		parsed, _, readErr := readHistoryIndexWindowContext(ctx, path, 0, completeOffset, nil)
		if readErr != nil {
			return idx, nil
		}
		return historyIndex{sessions: parsed}, nil
	}

	for attempt := 0; attempt < 3; attempt++ {
		cached, found, loadErr := store.loadHistory(ctx, path)
		if isContextError(loadErr) {
			return idx, loadErr
		}
		if loadErr != nil {
			found = false
		}
		if found && cached.parsedOffset == info.Size() && matchesFileInfo(path, info, cached.fileKey) {
			return historyIndex{sessions: cached.sessions}, nil
		}

		key := newFileCacheKey(path, info)
		if found && canAppendCacheFile(path, info, cached.fileKey, cached.parsedOffset, cached.prefixTailHash, cached.prefixTailSize) {
			sessions, changed, readErr := readHistoryIndexWindowContext(ctx, path, cached.parsedOffset, completeOffset-cached.parsedOffset, cached.sessions)
			if readErr != nil {
				return idx, nil
			}
			hash, hashSize, _ := sessionPreviewPrefixTailHash(path, completeOffset)
			writeErr := store.appendHistory(ctx, cached, key, completeOffset, hash, hashSize, changed)
			if errors.Is(writeErr, errCatalogSQLiteConflict) {
				continue
			}
			if isContextError(writeErr) {
				return idx, writeErr
			}
			return historyIndex{sessions: sessions}, nil
		}

		sessions, _, readErr := readHistoryIndexWindowContext(ctx, path, 0, completeOffset, nil)
		if readErr != nil {
			return idx, nil
		}
		cached.path = filepath.Clean(path)
		hash, hashSize, _ := sessionPreviewPrefixTailHash(path, completeOffset)
		writeErr := store.replaceHistory(ctx, cached, found, key, completeOffset, hash, hashSize, sessions)
		if errors.Is(writeErr, errCatalogSQLiteConflict) {
			continue
		}
		if isContextError(writeErr) {
			return idx, writeErr
		}
		return historyIndex{sessions: sessions}, nil
	}

	parsed, _, readErr := readHistoryIndexWindowContext(ctx, path, 0, completeOffset, nil)
	if readErr != nil {
		return idx, nil
	}
	return historyIndex{sessions: parsed}, nil
}

func readHistoryIndexWindowContext(ctx context.Context, path string, offset int64, size int64, base map[string]*historySessionInfo) (map[string]*historySessionInfo, map[string]*historySessionInfo, error) {
	sessions := cloneHistorySessions(base)
	changed := map[string]*historySessionInfo{}
	if offset < 0 || size < 0 {
		return sessions, changed, errors.New("invalid history index window")
	}
	if historyIndexWindowReadHook != nil {
		historyIndexWindowReadHook(offset, size)
	}
	f, err := openHistoryIndexFile(path)
	if err != nil {
		return sessions, changed, err
	}
	defer f.Close()

	reader := bufio.NewReader(io.NewSectionReader(f, offset, size))
	for {
		if err := ctx.Err(); err != nil {
			return sessions, changed, err
		}
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			return sessions, changed, readErr
		}
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			var entry codexHistoryEntry
			if json.Unmarshal(line, &entry) == nil && entry.SessionID != "" {
				current := sessions[entry.SessionID]
				if current == nil {
					current = &historySessionInfo{}
					sessions[entry.SessionID] = current
				}
				text := strings.TrimSpace(entry.Text)
				if text != "" && !shouldSkipFirstPrompt(text) {
					text = firstPromptTitleText(text)
					if text != "" {
						ts := time.Unix(entry.Ts, 0)
						if current.FirstPrompt == "" || (!ts.IsZero() && (current.FirstPromptTime.IsZero() || ts.Before(current.FirstPromptTime))) {
							current.FirstPrompt = text
							current.FirstPromptTime = ts
							copy := *current
							changed[entry.SessionID] = &copy
						}
					}
				}
			}
		}
		if readErr == io.EOF {
			break
		}
	}
	return sessions, changed, nil
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
