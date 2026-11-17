package codexhistory

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// The shared index is a synchronized view, not a snapshot copied on append.
// A provisional final record belongs only to this request's small tail map.
type historyIndex struct {
	data *historyIndexData
	tail map[string]*historySessionInfo
}
type historyIndexData struct {
	mu       sync.RWMutex
	sessions map[string]*historySessionInfo
}
type historySessionInfo struct {
	FirstPrompt     string
	FirstPromptTime time.Time
}

// codexHistoryEntry maps to a line in ~/.codex/history.jsonl.
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
func loadHistoryIndexContext(ctx context.Context, root string) (idx historyIndex, retErr error) {
	// History enrichment is optional. Preserve discovery on source I/O failure,
	// while propagating cancellation and leaving durable progress untouched.
	defer func() {
		if retErr != nil && !isContextError(retErr) {
			idx = historyIndex{}
			retErr = nil
		}
	}()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return idx, err
	}
	path := filepath.Join(root, "history.jsonl")
	info, err := os.Stat(path)
	if err != nil {
		return idx, nil
	}
	completeOffset, complete := sessionPreviewCompleteOffset(path, info)
	if !complete {
		return idx, nil
	}
	store, storeErr := currentCatalogSQLiteStore(path)
	if isContextError(storeErr) {
		return idx, storeErr
	}
	if storeErr != nil {
		return readHistoryIndexSource(ctx, path, info.Size(), completeOffset)
	}
	// Serialize this loader's updates. Readers use the data's RWMutex only for
	// lookup/merge; no global cache-map lock spans source or database I/O.
	store.historyUpdateMu.Lock()
	defer store.historyUpdateMu.Unlock()
	for attempt := 0; attempt < 3; attempt++ {
		cached, found, loadErr := store.loadHistory(ctx, path)
		if isContextError(loadErr) {
			return idx, loadErr
		}
		if loadErr != nil {
			found = false
		}
		if found && cached.parsedOffset == completeOffset && matchesFileInfo(path, info, cached.fileKey) {
			return historyIndexWithTail(ctx, path, info.Size(), completeOffset, historyIndex{data: cached.index})
		}
		key := newFileCacheKey(path, info)
		if found && canAppendCacheFile(path, info, cached.fileKey, cached.parsedOffset, cached.prefixTailHash, cached.prefixTailSize) {
			base := historyIndex{data: cached.index}
			changed, readErr := readHistoryIndexChangesContext(ctx, path, cached.parsedOffset, completeOffset-cached.parsedOffset, base.lookup)
			if readErr != nil {
				return idx, readErr
			}
			hash, hashSize, valid := sessionPreviewPrefixTailHash(path, completeOffset)
			if !valid || !unchangedJSONLPath(path, info) {
				return idx, nil
			}
			writeErr := store.appendHistory(ctx, cached, key, completeOffset, hash, hashSize, changed)
			if errors.Is(writeErr, errCatalogSQLiteConflict) {
				continue
			}
			if isContextError(writeErr) {
				return idx, writeErr
			}
			if writeErr == nil {
				cached.index.merge(changed)
				store.rememberHistory(cached, true, false, key, completeOffset, hash, hashSize, cached.index)
			} else {
				// Keep an uncommitted result request-local, without modifying the shared map.
				base.tail = changed
			}
			return historyIndexWithTail(ctx, path, info.Size(), completeOffset, base)
		}
		sessions, readErr := readHistoryIndexChangesContext(ctx, path, 0, completeOffset, nil)
		if readErr != nil {
			return idx, readErr
		}
		cached.path = filepath.Clean(path)
		hash, hashSize, valid := sessionPreviewPrefixTailHash(path, completeOffset)
		if !valid || !unchangedJSONLPath(path, info) {
			return idx, nil
		}
		data := &historyIndexData{sessions: sessions}
		writeErr := store.replaceHistory(ctx, cached, found, key, completeOffset, hash, hashSize, data)
		if errors.Is(writeErr, errCatalogSQLiteConflict) {
			continue
		}
		if isContextError(writeErr) {
			return idx, writeErr
		}
		return historyIndexWithTail(ctx, path, info.Size(), completeOffset, historyIndex{data: data})
	}
	return readHistoryIndexSource(ctx, path, info.Size(), completeOffset)
}
func readHistoryIndexSource(ctx context.Context, path string, size, offset int64) (historyIndex, error) {
	sessions, err := readHistoryIndexChangesContext(ctx, path, 0, offset, nil)
	if err != nil {
		return historyIndex{}, err
	}
	return historyIndexWithTail(ctx, path, size, offset, historyIndex{data: &historyIndexData{sessions: sessions}})
}
func historyIndexWithTail(ctx context.Context, path string, size, offset int64, idx historyIndex) (historyIndex, error) {
	if offset < size {
		changed, err := readHistoryIndexChangesContext(ctx, path, offset, size-offset, idx.lookup)
		if err != nil {
			return historyIndex{}, err
		}
		if idx.tail == nil {
			idx.tail = changed
		} else {
			for id, info := range changed {
				idx.tail[id] = info
			}
		}
	}
	return idx, nil
}
func readHistoryIndexChangesContext(ctx context.Context, path string, offset, size int64, base func(string) (historySessionInfo, bool)) (map[string]*historySessionInfo, error) {
	changed := map[string]*historySessionInfo{}
	if historyIndexWindowReadHook != nil {
		historyIndexWindowReadHook(offset, size)
	}
	f, err := openHistoryIndexFile(path)
	if err != nil {
		return changed, err
	}
	defer f.Close()
	err = readJSONLWindow(ctx, f, offset, size, func(line []byte) error {
		var entry codexHistoryEntry
		if json.Unmarshal(line, &entry) != nil || entry.SessionID == "" {
			return nil
		}
		current, found := changed[entry.SessionID]
		var value historySessionInfo
		known := found
		if found {
			value = *current
		} else if base != nil {
			value, known = base(entry.SessionID)
		}
		text := strings.TrimSpace(entry.Text)
		if text != "" && !shouldSkipFirstPrompt(text) {
			text = firstPromptTitleText(text)
			ts := time.Unix(entry.Ts, 0)
			if text != "" && (value.FirstPrompt == "" || (!ts.IsZero() && (value.FirstPromptTime.IsZero() || ts.Before(value.FirstPromptTime)))) {
				value.FirstPrompt, value.FirstPromptTime = text, ts
				changed[entry.SessionID] = &value
				return nil
			}
		}
		if !known {
			changed[entry.SessionID] = &value
		}
		return nil
	})
	return changed, err
}
func (idx historyIndex) lookup(id string) (historySessionInfo, bool) {
	if value, ok := idx.tail[id]; ok {
		return *value, true
	}
	if idx.data == nil || id == "" {
		return historySessionInfo{}, false
	}
	idx.data.mu.RLock()
	defer idx.data.mu.RUnlock()
	value, ok := idx.data.sessions[id]
	if !ok || value == nil {
		return historySessionInfo{}, false
	}
	return *value, true
}
func (data *historyIndexData) merge(changed map[string]*historySessionInfo) {
	data.mu.Lock()
	defer data.mu.Unlock()
	for id, value := range changed {
		data.sessions[id] = value
	}
}
func (idx historyIndex) len() int {
	if idx.data == nil {
		return len(idx.tail)
	}
	idx.data.mu.RLock()
	defer idx.data.mu.RUnlock()
	n := len(idx.data.sessions)
	for id := range idx.tail {
		if _, ok := idx.data.sessions[id]; !ok {
			n++
		}
	}
	return n
}
