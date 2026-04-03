package codexhistory

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
)

const (
	localPersistentCacheDirMode   = 0o755
	localPersistentCacheFileMode  = 0o600
	sharedPersistentCacheFileMode = 0o644
)

type persistentCacheWriteOptions struct {
	dirMode  os.FileMode
	fileMode os.FileMode
}

type sharedPersistentSessionMetaCandidate struct {
	entry      persistentSessionMetaEntry
	shardMtime int64
	shardOwner string
	hasOwner   bool
}

type sharedPersistentHistoryIndexCandidate struct {
	entry      persistentHistoryIndexEntry
	shardMtime int64
	shardOwner string
	hasOwner   bool
}

type sharedSessionMetaPersistentState struct {
	mu               sync.Mutex
	path             string
	ownerID          string
	cacheFilePresent bool
	cacheFileMtime   int64
	loaded           bool
	entries          map[string][]sharedPersistentSessionMetaCandidate
}

type sharedHistoryIndexPersistentState struct {
	mu               sync.Mutex
	path             string
	ownerID          string
	cacheFilePresent bool
	cacheFileMtime   int64
	loaded           bool
	entries          map[string][]sharedPersistentHistoryIndexCandidate
}

type persistentCacheWriterState struct {
	mu    sync.Mutex
	path  string
	value string
}

type sharedPersistentCacheShardFile struct {
	path     string
	mtime    int64
	ownerID  string
	hasOwner bool
}

var persistentSharedSessionMetaState sharedSessionMetaPersistentState
var persistentSharedHistoryIndexState sharedHistoryIndexPersistentState
var persistentCacheWriterID = defaultPersistentCacheWriterIDContext
var persistentCacheWriterScopeID = defaultPersistentCacheWriterScopeID
var persistentCacheLocalWriterState persistentCacheWriterState
var sharedPersistentCacheDirMode = os.ModeSticky | 0o777
var sharedPersistentCacheOwnerID = persistentCacheOwnerID
var errInvalidPersistentCacheWriterID = errors.New("invalid persistent cache writer id")
var readSharedSessionMetaCacheFile = os.ReadFile
var readSharedHistoryIndexCacheFile = os.ReadFile

func defaultPersistentCacheWriterID() string {
	id, _ := defaultPersistentCacheWriterIDContext(context.Background())
	return id
}

func defaultPersistentCacheWriterIDContext(ctx context.Context) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	path, err := persistentCacheWriterIDFile()
	if err != nil {
		return "", err
	}

	persistentCacheLocalWriterState.mu.Lock()
	if persistentCacheLocalWriterState.path == path && persistentCacheLocalWriterState.value != "" {
		id := persistentCacheLocalWriterState.value
		persistentCacheLocalWriterState.mu.Unlock()
		return id, nil
	}
	persistentCacheLocalWriterState.mu.Unlock()

	id, err := loadOrCreatePersistentCacheWriterIDContext(ctx, path)
	if err != nil || id == "" {
		return "", err
	}

	persistentCacheLocalWriterState.mu.Lock()
	persistentCacheLocalWriterState.path = path
	persistentCacheLocalWriterState.value = id
	persistentCacheLocalWriterState.mu.Unlock()
	return id, nil
}

func persistentCacheWriterIDFile() (string, error) {
	dir, err := persistentCacheDir()
	if err != nil {
		return "", err
	}
	scopeID, err := persistentCacheWriterScopeID()
	if err != nil {
		return "", err
	}
	scopeID = strings.TrimSpace(scopeID)
	if scopeID == "" {
		return "", fmt.Errorf("empty persistent cache writer scope id")
	}
	return filepath.Join(dir, "shared_writer_id."+scopeID+".json"), nil
}

func loadPersistentCacheWriterID(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	id := normalizePersistentCacheWriterID(data)
	if id == "" {
		return "", errInvalidPersistentCacheWriterID
	}
	return id, nil
}

func loadOrCreatePersistentCacheWriterIDContext(ctx context.Context, path string) (string, error) {
	var value string
	if id, err := loadPersistentCacheWriterID(path); err == nil {
		return id, nil
	} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errInvalidPersistentCacheWriterID) {
		return "", err
	}

	err := withLockedCacheContext(ctx, path, func() error {
		if id, err := loadPersistentCacheWriterID(path); err == nil {
			value = id
			return nil
		} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errInvalidPersistentCacheWriterID) {
			return err
		}

		id, err := newRandomPersistentCacheWriterID()
		if err != nil {
			return err
		}
		if err := writeJSONAtomicallyWithOptions(path, id, persistentCacheWriteOptions{
			dirMode:  localPersistentCacheDirMode,
			fileMode: localPersistentCacheFileMode,
		}); err != nil {
			return err
		}
		value = id
		return nil
	})
	if err != nil {
		return "", err
	}
	return value, nil
}

func normalizePersistentCacheWriterID(data []byte) string {
	var id string
	if err := json.Unmarshal(data, &id); err != nil {
		return ""
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'f':
		case r >= '0' && r <= '9':
		default:
			return ""
		}
	}
	return id
}

func newRandomPersistentCacheWriterID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b[:]), nil
}

func codexRootForSessionFile(filePath string) string {
	path := filepath.Clean(filePath)
	for dir := filepath.Dir(path); dir != "" && dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if filepath.Base(dir) == "sessions" {
			return filepath.Dir(dir)
		}
	}
	return ""
}

func sharedPersistentCacheBase(root string) string {
	root = strings.TrimSpace(root)
	if root == "" {
		return ""
	}
	return filepath.Join(root, ".codex-proxy", "codexhistory")
}

func sharedSessionMetaShardDir(filePath string) string {
	root := codexRootForSessionFile(filePath)
	if root == "" {
		return ""
	}
	return filepath.Join(sharedPersistentCacheBase(root), "session-meta")
}

func sharedSessionMetaShardFileContext(ctx context.Context, filePath string) (string, error) {
	dir := sharedSessionMetaShardDir(filePath)
	if dir == "" {
		return "", nil
	}
	writerID, err := persistentCacheWriterID(ctx)
	if err != nil || writerID == "" {
		return "", err
	}
	return filepath.Join(dir, writerID+".json"), nil
}

func sharedHistoryIndexShardDir(path string) string {
	root := strings.TrimSpace(filepath.Dir(filepath.Clean(path)))
	if root == "" {
		return ""
	}
	return filepath.Join(sharedPersistentCacheBase(root), "history-index")
}

func sharedHistoryIndexShardFileContext(ctx context.Context, path string) (string, error) {
	dir := sharedHistoryIndexShardDir(path)
	if dir == "" {
		return "", nil
	}
	writerID, err := persistentCacheWriterID(ctx)
	if err != nil || writerID == "" {
		return "", err
	}
	return filepath.Join(dir, writerID+".json"), nil
}

func ensureSharedPersistentCacheDir(dir string) error {
	dir = filepath.Clean(strings.TrimSpace(dir))
	if dir == "" {
		return fmt.Errorf("empty shared cache dir")
	}

	codexHistoryDir := filepath.Dir(dir)
	proxyDir := filepath.Dir(codexHistoryDir)
	for _, candidate := range []string{proxyDir, codexHistoryDir, dir} {
		if err := ensureSharedPersistentCacheDirStep(candidate, sharedPersistentCacheDirMode); err != nil {
			return err
		}
		// Best-effort: make shared cache directories multi-user writable when possible.
		if info, err := os.Lstat(candidate); err == nil && info.Mode()&os.ModeSymlink == 0 && info.IsDir() {
			_ = os.Chmod(candidate, sharedPersistentCacheDirMode)
		}
	}
	return nil
}

func ensureSharedPersistentCacheDirStep(path string, mode os.FileMode) error {
	for {
		info, err := os.Lstat(path)
		if err == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("shared cache path %q is a symlink", path)
			}
			if !info.IsDir() {
				return fmt.Errorf("shared cache path %q is not a directory", path)
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.Mkdir(path, mode); err != nil {
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return err
		}
	}
}

func readSharedPersistentSessionMetaContext(ctx context.Context, filePath string, info os.FileInfo) (sessionFileMeta, bool, error) {
	dir := sharedSessionMetaShardDir(filePath)
	if dir == "" {
		return sessionFileMeta{}, false, nil
	}

	sourceOwner, hasOwner := sharedPersistentCacheOwnerID(filePath, info)
	if !hasOwner {
		return sessionFileMeta{}, false, nil
	}

	persistentSharedSessionMetaState.mu.Lock()
	entries, err := loadSharedSessionMetaPersistentStateLockedContext(ctx, dir, sourceOwner)
	persistentSharedSessionMetaState.mu.Unlock()
	if err != nil {
		if isContextError(err) {
			return sessionFileMeta{}, false, err
		}
		return sessionFileMeta{}, false, nil
	}

	candidate, ok := selectSharedSessionMetaCandidate(filePath, info, sourceOwner, entries[filepath.Clean(filePath)])
	if !ok {
		return sessionFileMeta{}, false, nil
	}
	return candidate.entry.Meta, true, nil
}

func writeSharedPersistentSessionMetaContext(ctx context.Context, filePath string, info os.FileInfo, meta sessionFileMeta) error {
	shardPath, err := sharedSessionMetaShardFileContext(ctx, filePath)
	if err != nil {
		if isContextError(err) {
			return err
		}
		return nil
	}
	if shardPath == "" {
		return nil
	}

	cleanPath := filepath.Clean(filePath)
	entry := persistentSessionMetaEntry{
		FileCacheKey: newFileCacheKey(filePath, info),
		Meta:         meta,
	}
	if err := updateSharedSessionMetaShardContext(ctx, shardPath, func(cache *persistentSessionMetaCache) {
		cache.Entries[cleanPath] = entry
	}); err != nil {
		if isContextError(err) {
			return err
		}
		return nil
	}

	invalidateSharedSessionMetaState(filepath.Dir(shardPath))
	return nil
}

func deleteSharedPersistentSessionMetaContext(ctx context.Context, filePath string) error {
	shardPath, err := sharedSessionMetaShardFileContext(ctx, filePath)
	if err != nil {
		if isContextError(err) {
			return err
		}
		return nil
	}
	if shardPath == "" {
		return nil
	}

	cleanPath := filepath.Clean(filePath)
	if err := updateSharedSessionMetaShardContext(ctx, shardPath, func(cache *persistentSessionMetaCache) {
		delete(cache.Entries, cleanPath)
	}); err != nil {
		if isContextError(err) {
			return err
		}
		return nil
	}

	invalidateSharedSessionMetaState(filepath.Dir(shardPath))
	return nil
}

func writeSharedPersistentSessionMetaBatchContext(ctx context.Context, updates map[string]persistentSessionMetaEntry, deletes map[string]struct{}) error {
	if len(updates) == 0 && len(deletes) == 0 {
		return nil
	}

	type batchGroup struct {
		updates map[string]persistentSessionMetaEntry
		deletes map[string]struct{}
	}

	groups := map[string]*batchGroup{}
	for path, entry := range updates {
		shardPath, err := sharedSessionMetaShardFileContext(ctx, path)
		if err != nil {
			if isContextError(err) {
				return err
			}
			continue
		}
		if shardPath == "" {
			continue
		}
		group := groups[shardPath]
		if group == nil {
			group = &batchGroup{
				updates: map[string]persistentSessionMetaEntry{},
				deletes: map[string]struct{}{},
			}
			groups[shardPath] = group
		}
		delete(group.deletes, path)
		group.updates[path] = entry
	}
	for path := range deletes {
		shardPath, err := sharedSessionMetaShardFileContext(ctx, path)
		if err != nil {
			if isContextError(err) {
				return err
			}
			continue
		}
		if shardPath == "" {
			continue
		}
		group := groups[shardPath]
		if group == nil {
			group = &batchGroup{
				updates: map[string]persistentSessionMetaEntry{},
				deletes: map[string]struct{}{},
			}
			groups[shardPath] = group
		}
		delete(group.updates, path)
		group.deletes[path] = struct{}{}
	}

	for shardPath, group := range groups {
		if err := updateSharedSessionMetaShardContext(ctx, shardPath, func(cache *persistentSessionMetaCache) {
			for path := range group.deletes {
				delete(cache.Entries, path)
			}
			for path, entry := range group.updates {
				cache.Entries[path] = entry
			}
		}); err != nil {
			if isContextError(err) {
				return err
			}
		}
		invalidateSharedSessionMetaState(filepath.Dir(shardPath))
	}
	return nil
}

func readSharedPersistentHistoryIndexContext(ctx context.Context, path string, info os.FileInfo) (historyIndex, bool, error) {
	dir := sharedHistoryIndexShardDir(path)
	if dir == "" {
		return historyIndex{}, false, nil
	}

	sourceOwner, hasOwner := sharedPersistentCacheOwnerID(path, info)
	if !hasOwner {
		return historyIndex{}, false, nil
	}

	persistentSharedHistoryIndexState.mu.Lock()
	entries, err := loadSharedHistoryIndexPersistentStateLockedContext(ctx, dir, sourceOwner)
	persistentSharedHistoryIndexState.mu.Unlock()
	if err != nil {
		if isContextError(err) {
			return historyIndex{}, false, err
		}
		return historyIndex{}, false, nil
	}

	candidate, ok := selectSharedHistoryIndexCandidate(path, info, sourceOwner, entries[filepath.Clean(path)])
	if !ok {
		return historyIndex{}, false, nil
	}
	return historyIndex{sessions: cloneHistorySessions(candidate.entry.Sessions)}, true, nil
}

func writeSharedPersistentHistoryIndexContext(ctx context.Context, path string, info os.FileInfo, idx historyIndex) error {
	shardPath, err := sharedHistoryIndexShardFileContext(ctx, path)
	if err != nil {
		if isContextError(err) {
			return err
		}
		return nil
	}
	if shardPath == "" {
		return nil
	}

	cleanPath := filepath.Clean(path)
	entry := persistentHistoryIndexEntry{
		FileCacheKey: newFileCacheKey(path, info),
		Sessions:     cloneHistorySessions(idx.sessions),
	}
	if err := updateSharedHistoryIndexShardContext(ctx, shardPath, func(cache *persistentHistoryIndexCache) {
		cache.Entries[cleanPath] = entry
	}); err != nil {
		if isContextError(err) {
			return err
		}
		return nil
	}

	invalidateSharedHistoryIndexState(filepath.Dir(shardPath))
	return nil
}

func deleteSharedPersistentHistoryIndexContext(ctx context.Context, path string) error {
	shardPath, err := sharedHistoryIndexShardFileContext(ctx, path)
	if err != nil {
		if isContextError(err) {
			return err
		}
		return nil
	}
	if shardPath == "" {
		return nil
	}

	cleanPath := filepath.Clean(path)
	if err := updateSharedHistoryIndexShardContext(ctx, shardPath, func(cache *persistentHistoryIndexCache) {
		delete(cache.Entries, cleanPath)
	}); err != nil {
		if isContextError(err) {
			return err
		}
		return nil
	}

	invalidateSharedHistoryIndexState(filepath.Dir(shardPath))
	return nil
}

func loadSharedSessionMetaPersistentStateLockedContext(ctx context.Context, dir string, sourceOwner string) (map[string][]sharedPersistentSessionMetaCandidate, error) {
	present, mtime := cacheFileState(dir)
	if persistentSharedSessionMetaState.loaded &&
		persistentSharedSessionMetaState.path == dir &&
		persistentSharedSessionMetaState.ownerID == sourceOwner &&
		persistentSharedSessionMetaState.cacheFilePresent == present &&
		(!present || persistentSharedSessionMetaState.cacheFileMtime == mtime) {
		return persistentSharedSessionMetaState.entries, nil
	}

	entries, err := loadMergedSharedSessionMetaCacheContext(ctx, dir, sourceOwner)
	if err != nil {
		if isContextError(err) {
			return nil, err
		}
		entries = map[string][]sharedPersistentSessionMetaCandidate{}
	}

	persistentSharedSessionMetaState.path = dir
	persistentSharedSessionMetaState.ownerID = sourceOwner
	persistentSharedSessionMetaState.cacheFilePresent = present
	persistentSharedSessionMetaState.cacheFileMtime = mtime
	persistentSharedSessionMetaState.loaded = true
	persistentSharedSessionMetaState.entries = entries
	return entries, nil
}

func loadSharedHistoryIndexPersistentStateLockedContext(ctx context.Context, dir string, sourceOwner string) (map[string][]sharedPersistentHistoryIndexCandidate, error) {
	present, mtime := cacheFileState(dir)
	if persistentSharedHistoryIndexState.loaded &&
		persistentSharedHistoryIndexState.path == dir &&
		persistentSharedHistoryIndexState.ownerID == sourceOwner &&
		persistentSharedHistoryIndexState.cacheFilePresent == present &&
		(!present || persistentSharedHistoryIndexState.cacheFileMtime == mtime) {
		return persistentSharedHistoryIndexState.entries, nil
	}

	entries, err := loadMergedSharedHistoryIndexCacheContext(ctx, dir, sourceOwner)
	if err != nil {
		if isContextError(err) {
			return nil, err
		}
		entries = map[string][]sharedPersistentHistoryIndexCandidate{}
	}

	persistentSharedHistoryIndexState.path = dir
	persistentSharedHistoryIndexState.ownerID = sourceOwner
	persistentSharedHistoryIndexState.cacheFilePresent = present
	persistentSharedHistoryIndexState.cacheFileMtime = mtime
	persistentSharedHistoryIndexState.loaded = true
	persistentSharedHistoryIndexState.entries = entries
	return entries, nil
}

func invalidateSharedSessionMetaState(dir string) {
	persistentSharedSessionMetaState.mu.Lock()
	defer persistentSharedSessionMetaState.mu.Unlock()
	persistentSharedSessionMetaState.path = dir
	persistentSharedSessionMetaState.ownerID = ""
	persistentSharedSessionMetaState.loaded = false
	persistentSharedSessionMetaState.entries = nil
	persistentSharedSessionMetaState.cacheFilePresent, persistentSharedSessionMetaState.cacheFileMtime = cacheFileState(dir)
}

func invalidateSharedHistoryIndexState(dir string) {
	persistentSharedHistoryIndexState.mu.Lock()
	defer persistentSharedHistoryIndexState.mu.Unlock()
	persistentSharedHistoryIndexState.path = dir
	persistentSharedHistoryIndexState.ownerID = ""
	persistentSharedHistoryIndexState.loaded = false
	persistentSharedHistoryIndexState.entries = nil
	persistentSharedHistoryIndexState.cacheFilePresent, persistentSharedHistoryIndexState.cacheFileMtime = cacheFileState(dir)
}

func loadMergedSharedSessionMetaCacheContext(ctx context.Context, dir string, sourceOwner string) (map[string][]sharedPersistentSessionMetaCandidate, error) {
	entries := map[string][]sharedPersistentSessionMetaCandidate{}
	shardFiles, err := sharedPersistentCacheShardFiles(dir)
	if err != nil {
		return entries, err
	}

	for _, shard := range shardFiles {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !isTrustedSharedShardOwner(sourceOwner, shard.ownerID, shard.hasOwner) {
			continue
		}
		cache, err := loadPersistentSessionMetaCacheFileUnlocked(shard.path)
		if err != nil {
			continue
		}
		for path, entry := range cache.Entries {
			entries[path] = append(entries[path], sharedPersistentSessionMetaCandidate{
				entry:      entry,
				shardMtime: shard.mtime,
				shardOwner: shard.ownerID,
				hasOwner:   shard.hasOwner,
			})
		}
	}
	return entries, nil
}

func loadMergedSharedHistoryIndexCacheContext(ctx context.Context, dir string, sourceOwner string) (map[string][]sharedPersistentHistoryIndexCandidate, error) {
	entries := map[string][]sharedPersistentHistoryIndexCandidate{}
	shardFiles, err := sharedPersistentCacheShardFiles(dir)
	if err != nil {
		return entries, err
	}

	for _, shard := range shardFiles {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !isTrustedSharedShardOwner(sourceOwner, shard.ownerID, shard.hasOwner) {
			continue
		}
		cache, err := loadPersistentHistoryIndexCacheFileUnlocked(shard.path)
		if err != nil {
			continue
		}
		for path, entry := range cache.Entries {
			entries[path] = append(entries[path], sharedPersistentHistoryIndexCandidate{
				entry:      entry,
				shardMtime: shard.mtime,
				shardOwner: shard.ownerID,
				hasOwner:   shard.hasOwner,
			})
		}
	}
	return entries, nil
}

func sharedPersistentCacheShardFiles(dir string) ([]sharedPersistentCacheShardFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	files := make([]sharedPersistentCacheShardFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}
		ownerID, hasOwner := sharedPersistentCacheOwnerID(path, info)
		files = append(files, sharedPersistentCacheShardFile{
			path:     path,
			mtime:    info.ModTime().UnixNano(),
			ownerID:  ownerID,
			hasOwner: hasOwner,
		})
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].path < files[j].path
	})
	return files, nil
}

func loadPersistentSessionMetaCacheFileUnlocked(path string) (persistentSessionMetaCache, error) {
	cache := newPersistentSessionMetaCache()
	data, err := readSharedSessionMetaCacheFile(path)
	if err != nil {
		return cache, err
	}
	if err := json.Unmarshal(data, &cache); err != nil {
		return cache, err
	}
	if cache.Version != persistentCacheVersion || cache.Entries == nil {
		return newPersistentSessionMetaCache(), nil
	}
	return cache, nil
}

func loadPersistentHistoryIndexCacheFileUnlocked(path string) (persistentHistoryIndexCache, error) {
	cache := newPersistentHistoryIndexCache()
	data, err := readSharedHistoryIndexCacheFile(path)
	if err != nil {
		return cache, err
	}
	if err := json.Unmarshal(data, &cache); err != nil {
		return cache, err
	}
	if cache.Version != persistentCacheVersion || cache.Entries == nil {
		return newPersistentHistoryIndexCache(), nil
	}
	return cache, nil
}

func updateSharedSessionMetaShardContext(ctx context.Context, path string, fn func(*persistentSessionMetaCache)) error {
	if err := ensureSharedPersistentCacheDir(filepath.Dir(path)); err != nil {
		return err
	}
	return withLockedCacheContext(ctx, path, func() error {
		cache := newPersistentSessionMetaCache()
		if data, err := os.ReadFile(path); err == nil {
			if json.Unmarshal(data, &cache) != nil || cache.Version != persistentCacheVersion || cache.Entries == nil {
				cache = newPersistentSessionMetaCache()
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		fn(&cache)
		return writeJSONAtomicallyWithOptions(path, cache, persistentCacheWriteOptions{
			dirMode:  sharedPersistentCacheDirMode,
			fileMode: sharedPersistentCacheFileMode,
		})
	})
}

func updateSharedHistoryIndexShardContext(ctx context.Context, path string, fn func(*persistentHistoryIndexCache)) error {
	if err := ensureSharedPersistentCacheDir(filepath.Dir(path)); err != nil {
		return err
	}
	return withLockedCacheContext(ctx, path, func() error {
		cache := newPersistentHistoryIndexCache()
		if data, err := os.ReadFile(path); err == nil {
			if json.Unmarshal(data, &cache) != nil || cache.Version != persistentCacheVersion || cache.Entries == nil {
				cache = newPersistentHistoryIndexCache()
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		fn(&cache)
		return writeJSONAtomicallyWithOptions(path, cache, persistentCacheWriteOptions{
			dirMode:  sharedPersistentCacheDirMode,
			fileMode: sharedPersistentCacheFileMode,
		})
	})
}

func selectSharedSessionMetaCandidate(filePath string, info os.FileInfo, sourceOwner string, candidates []sharedPersistentSessionMetaCandidate) (sharedPersistentSessionMetaCandidate, bool) {
	var best sharedPersistentSessionMetaCandidate
	found := false
	for _, candidate := range candidates {
		if !isTrustedSharedShardOwner(sourceOwner, candidate.shardOwner, candidate.hasOwner) {
			continue
		}
		if !matchesFileInfo(filePath, info, candidate.entry.FileCacheKey) {
			continue
		}
		if !found || preferPersistentSessionMetaEntry(best.entry, best.shardMtime, candidate.entry, candidate.shardMtime) {
			best = candidate
			found = true
		}
	}
	return best, found
}

func selectSharedHistoryIndexCandidate(path string, info os.FileInfo, sourceOwner string, candidates []sharedPersistentHistoryIndexCandidate) (sharedPersistentHistoryIndexCandidate, bool) {
	var best sharedPersistentHistoryIndexCandidate
	found := false
	for _, candidate := range candidates {
		if !isTrustedSharedShardOwner(sourceOwner, candidate.shardOwner, candidate.hasOwner) {
			continue
		}
		if !matchesFileInfo(path, info, candidate.entry.FileCacheKey) {
			continue
		}
		if !found || preferPersistentHistoryIndexEntry(best.entry, best.shardMtime, candidate.entry, candidate.shardMtime) {
			best = candidate
			found = true
		}
	}
	return best, found
}

func isTrustedSharedShardOwner(sourceOwner string, shardOwner string, hasOwner bool) bool {
	if !hasOwner {
		return false
	}
	return shardOwner == sourceOwner || shardOwner == "uid:0"
}

func persistentCacheOwnerID(path string, info os.FileInfo) (string, bool) {
	if info == nil {
		return "", false
	}

	if ownerID, ok := populatePlatformPersistentCacheOwnerID(path, info); ok {
		return ownerID, true
	}

	sys := info.Sys()
	if sys == nil {
		return "", false
	}
	value := reflect.ValueOf(sys)
	if !value.IsValid() {
		return "", false
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return "", false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return "", false
	}

	for _, name := range []string{"Uid", "UID"} {
		if uid, ok := statUintField(value, name); ok {
			return fmt.Sprintf("uid:%d", uid), true
		}
	}
	return "", false
}

func preferPersistentSessionMetaEntry(current persistentSessionMetaEntry, currentShardMtime int64, candidate persistentSessionMetaEntry, candidateShardMtime int64) bool {
	return comparePersistentCacheKeys(current.FileCacheKey, currentShardMtime, candidate.FileCacheKey, candidateShardMtime) < 0
}

func preferPersistentHistoryIndexEntry(current persistentHistoryIndexEntry, currentShardMtime int64, candidate persistentHistoryIndexEntry, candidateShardMtime int64) bool {
	return comparePersistentCacheKeys(current.FileCacheKey, currentShardMtime, candidate.FileCacheKey, candidateShardMtime) < 0
}

func comparePersistentCacheKeys(current fileCacheKey, currentShardMtime int64, candidate fileCacheKey, candidateShardMtime int64) int {
	for _, pair := range [][2]int64{
		{current.MtimeUnixNano, candidate.MtimeUnixNano},
		{current.CtimeUnixNano, candidate.CtimeUnixNano},
		{current.Size, candidate.Size},
		{int64(current.Mode), int64(candidate.Mode)},
		{currentShardMtime, candidateShardMtime},
	} {
		switch {
		case pair[0] < pair[1]:
			return -1
		case pair[0] > pair[1]:
			return 1
		}
	}
	return 0
}
