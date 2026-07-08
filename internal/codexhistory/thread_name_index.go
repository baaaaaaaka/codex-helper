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
	"sync"
)

const threadNameIndexFile = "session_index.jsonl"

type threadNameIndexEntry struct {
	SessionID  string `json:"id"`
	ThreadName string `json:"thread_name"`
}

type cachedThreadNameIndex struct {
	fileKey fileCacheKey
	names   map[string]string
}

var threadNameIndexCache = struct {
	mu      sync.Mutex
	entries map[string]cachedThreadNameIndex
}{
	entries: map[string]cachedThreadNameIndex{},
}

var openThreadNameIndexFile = os.Open

func loadThreadNameIndexContext(ctx context.Context, root string) (map[string]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	path := filepath.Join(root, threadNameIndexFile)
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			deleteThreadNameIndexCacheEntry(path)
		}
		// Thread names are optional metadata. Preserve legacy discovery behavior
		// when old Codex versions do not have the index or it is unreadable.
		return map[string]string{}, nil
	}

	fileKey := newFileCacheKey(path, info)
	threadNameIndexCache.mu.Lock()
	cached, ok := threadNameIndexCache.entries[path]
	threadNameIndexCache.mu.Unlock()
	if ok && cached.fileKey == fileKey {
		return cached.names, nil
	}

	names, err := readThreadNameIndexContext(ctx, path)
	if err != nil {
		if isContextError(err) {
			return nil, err
		}
		// A concurrently replaced or temporarily locked compatibility index must
		// not make the underlying Codex sessions disappear from CXP.
		return map[string]string{}, nil
	}

	threadNameIndexCache.mu.Lock()
	threadNameIndexCache.entries[path] = cachedThreadNameIndex{
		fileKey: fileKey,
		names:   names,
	}
	threadNameIndexCache.mu.Unlock()
	return names, nil
}

func readThreadNameIndexContext(ctx context.Context, path string) (map[string]string, error) {
	names := map[string]string{}
	f, err := openThreadNameIndexFile(path)
	if err != nil {
		return names, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			return names, readErr
		}
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			var entry threadNameIndexEntry
			if json.Unmarshal(line, &entry) == nil {
				id := strings.TrimSpace(entry.SessionID)
				name := strings.TrimSpace(entry.ThreadName)
				// Match Codex's own compatibility-index lookup: malformed, unknown,
				// and empty-name entries do not replace the latest valid name.
				if id != "" && name != "" {
					names[id] = name
				}
			}
		}
		if readErr == io.EOF {
			break
		}
	}
	return names, nil
}

func deleteThreadNameIndexCacheEntry(path string) {
	threadNameIndexCache.mu.Lock()
	delete(threadNameIndexCache.entries, path)
	threadNameIndexCache.mu.Unlock()
}

func resetThreadNameIndexCache() {
	threadNameIndexCache.mu.Lock()
	threadNameIndexCache.entries = map[string]cachedThreadNameIndex{}
	threadNameIndexCache.mu.Unlock()
}
