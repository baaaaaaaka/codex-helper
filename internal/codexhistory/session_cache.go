package codexhistory

import (
	"context"
	"os"
	"sync"
)

type sessionFileCacheEntry struct {
	fileKey fileCacheKey
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
	fileKey := newFileCacheKey(filePath, info)
	sessionFileCache.mu.Lock()
	entry, ok := sessionFileCache.entries[filePath]
	sessionFileCache.mu.Unlock()
	if ok && entry.fileKey == fileKey {
		return entry, info, true, nil
	}
	return sessionFileCacheEntry{fileKey: fileKey}, info, false, nil
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
		if !stageCatalogSessionMetaDelete(ctx, filePath) {
			if delErr := deleteCatalogSessionMeta(ctx, filePath); isContextError(delErr) {
				return sessionFileMeta{}, delErr
			}
		}
		return sessionFileMeta{}, err
	}
	if ok && entry.hasMeta {
		return entry.meta, nil
	}
	store, storeErr := currentCatalogSQLiteStore(filePath)
	if isContextError(storeErr) {
		return sessionFileMeta{}, storeErr
	}
	var cached catalogSessionMetaEntry
	var found bool
	if storeErr == nil {
		cached, found, err = store.loadSessionMeta(ctx, filePath)
		if isContextError(err) {
			return sessionFileMeta{}, err
		}
		if err != nil {
			found = false
		}
	}
	if found && cached.parsedOffset == info.Size() && matchesFileInfo(filePath, info, cached.fileKey) {
		entry.meta = cached.meta
		entry.hasMeta = true
		setSessionFileCacheEntry(filePath, entry)
		return cached.meta, nil
	}

	completeOffset, complete := sessionPreviewCompleteOffset(filePath, info)
	if !complete || completeOffset < info.Size() {
		meta, readErr := readSessionFileMetaContext(ctx, filePath)
		if readErr != nil {
			return meta, readErr
		}
		entry.meta = meta
		entry.hasMeta = true
		setSessionFileCacheEntry(filePath, entry)
		return meta, nil
	}

	meta := sessionFileMeta{}
	startOffset := int64(0)
	if found && canAppendCacheFile(filePath, info, cached.fileKey, cached.parsedOffset, cached.prefixTailHash, cached.prefixTailSize) {
		meta = cached.meta
		startOffset = cached.parsedOffset
	}
	meta, err = readSessionFileMetaWindowContext(ctx, filePath, startOffset, completeOffset-startOffset, meta)
	if err != nil {
		return meta, err
	}
	entry.meta = meta
	entry.hasMeta = true
	setSessionFileCacheEntry(filePath, entry)
	hash, hashSize, _ := sessionPreviewPrefixTailHash(filePath, completeOffset)
	catalogEntry := catalogSessionMetaEntry{
		path:           filePath,
		fileKey:        newFileCacheKey(filePath, info),
		parsedOffset:   completeOffset,
		prefixTailHash: hash,
		prefixTailSize: hashSize,
		meta:           meta,
	}
	if !stageCatalogSessionMeta(ctx, catalogEntry) {
		if err := writeCatalogSessionMeta(ctx, catalogEntry); isContextError(err) {
			return meta, err
		}
	}
	return meta, nil
}

func SessionFileIsSubagentContext(ctx context.Context, filePath string) (bool, error) {
	meta, err := readSessionFileMetaCachedContext(ctx, filePath)
	if err != nil {
		return false, err
	}
	return meta.IsSubagent, nil
}
