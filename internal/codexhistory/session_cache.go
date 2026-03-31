package codexhistory

import (
	"context"
	"os"
	"sync"
	"time"
)

type sessionFileCacheEntry struct {
	mtime   time.Time
	meta    sessionFileMeta
	hasMeta bool
}

var sessionFileCache = struct {
	mu      sync.Mutex
	entries map[string]sessionFileCacheEntry
}{
	entries: map[string]sessionFileCacheEntry{},
}

func resetSessionFileCache() {
	sessionFileCache.mu.Lock()
	sessionFileCache.entries = map[string]sessionFileCacheEntry{}
	sessionFileCache.mu.Unlock()
}

func getSessionFileCacheEntry(filePath string) (sessionFileCacheEntry, os.FileInfo, bool, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		sessionFileCache.mu.Lock()
		delete(sessionFileCache.entries, filePath)
		sessionFileCache.mu.Unlock()
		return sessionFileCacheEntry{}, nil, false, err
	}
	mtime := info.ModTime()
	sessionFileCache.mu.Lock()
	entry, ok := sessionFileCache.entries[filePath]
	sessionFileCache.mu.Unlock()
	if ok && entry.mtime.Equal(mtime) {
		return entry, info, true, nil
	}
	return sessionFileCacheEntry{mtime: mtime}, info, false, nil
}

func setSessionFileCacheEntry(filePath string, entry sessionFileCacheEntry) {
	sessionFileCache.mu.Lock()
	sessionFileCache.entries[filePath] = entry
	sessionFileCache.mu.Unlock()
}

func readSessionFileMetaCached(filePath string) (sessionFileMeta, error) {
	return readSessionFileMetaCachedContext(context.Background(), filePath)
}

func readSessionFileMetaCachedContext(ctx context.Context, filePath string) (sessionFileMeta, error) {
	if err := ctx.Err(); err != nil {
		return sessionFileMeta{}, err
	}
	entry, info, ok, err := getSessionFileCacheEntry(filePath)
	if err != nil {
		if !stagePersistentSessionMetaDelete(ctx, filePath) {
			if delErr := deletePersistentSessionMetaContext(ctx, filePath); delErr != nil {
				return sessionFileMeta{}, delErr
			}
		}
		return sessionFileMeta{}, err
	}
	if ok && entry.hasMeta {
		return entry.meta, nil
	}
	if meta, ok, err := readPersistentSessionMetaContext(ctx, filePath, info); err != nil {
		return sessionFileMeta{}, err
	} else if ok {
		entry.meta = meta
		entry.hasMeta = true
		setSessionFileCacheEntry(filePath, entry)
		return meta, nil
	}
	meta, err := readSessionFileMetaContext(ctx, filePath)
	if err != nil {
		return meta, err
	}
	entry.meta = meta
	entry.hasMeta = true
	setSessionFileCacheEntry(filePath, entry)
	if !stagePersistentSessionMetaWrite(ctx, filePath, info, meta) {
		if err := writePersistentSessionMetaContext(ctx, filePath, info, meta); err != nil {
			return meta, err
		}
	}
	return meta, nil
}
