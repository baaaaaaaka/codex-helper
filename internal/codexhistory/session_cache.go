package codexhistory

import (
	"os"
	"sync"
	"time"
)

type sessionFileCacheEntry struct {
	mtime        time.Time
	sessionID    string
	hasSessionID bool
	meta         sessionFileMeta
	hasMeta      bool
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

func getSessionFileCacheEntry(filePath string) (sessionFileCacheEntry, bool, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		sessionFileCache.mu.Lock()
		delete(sessionFileCache.entries, filePath)
		sessionFileCache.mu.Unlock()
		return sessionFileCacheEntry{}, false, err
	}
	mtime := info.ModTime()
	sessionFileCache.mu.Lock()
	entry, ok := sessionFileCache.entries[filePath]
	sessionFileCache.mu.Unlock()
	if ok && entry.mtime.Equal(mtime) {
		return entry, true, nil
	}
	return sessionFileCacheEntry{mtime: mtime}, false, nil
}

func setSessionFileCacheEntry(filePath string, entry sessionFileCacheEntry) {
	sessionFileCache.mu.Lock()
	sessionFileCache.entries[filePath] = entry
	sessionFileCache.mu.Unlock()
}

func readSessionFileMetaCached(filePath string) (sessionFileMeta, error) {
	entry, ok, err := getSessionFileCacheEntry(filePath)
	if err != nil {
		return sessionFileMeta{}, err
	}
	if ok && entry.hasMeta {
		return entry.meta, nil
	}
	meta, err := readSessionFileMeta(filePath)
	if err != nil {
		return meta, err
	}
	entry.meta = meta
	entry.hasMeta = true
	setSessionFileCacheEntry(filePath, entry)
	return meta, nil
}
