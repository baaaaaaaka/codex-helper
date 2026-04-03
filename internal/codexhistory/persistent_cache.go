package codexhistory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

const persistentCacheVersion = 2

type fileCacheKey struct {
	Size          int64  `json:"size"`
	MtimeUnixNano int64  `json:"mtimeUnixNano"`
	Mode          uint32 `json:"mode"`
	HasFileID     bool   `json:"hasFileId,omitempty"`
	Dev           uint64 `json:"dev,omitempty"`
	Ino           uint64 `json:"ino,omitempty"`
	HasCtime      bool   `json:"hasCtime,omitempty"`
	CtimeUnixNano int64  `json:"ctimeUnixNano,omitempty"`
}

type persistentSessionMetaCache struct {
	Version int                                   `json:"version"`
	Entries map[string]persistentSessionMetaEntry `json:"entries"`
}

type persistentSessionMetaEntry struct {
	FileCacheKey fileCacheKey    `json:"fileCacheKey"`
	Meta         sessionFileMeta `json:"meta"`
}

type persistentHistoryIndexCache struct {
	Version int                                    `json:"version"`
	Entries map[string]persistentHistoryIndexEntry `json:"entries"`
}

type persistentHistoryIndexEntry struct {
	FileCacheKey fileCacheKey                   `json:"fileCacheKey"`
	Sessions     map[string]*historySessionInfo `json:"sessions"`
}

type sessionMetaPersistentState struct {
	mu               sync.Mutex
	path             string
	cacheFilePresent bool
	cacheFileMtime   int64
	loaded           bool
	cache            persistentSessionMetaCache
}

type historyIndexPersistentState struct {
	mu               sync.Mutex
	path             string
	cacheFilePresent bool
	cacheFileMtime   int64
	loaded           bool
	cache            persistentHistoryIndexCache
}

type sessionMetaPersistentBatch struct {
	mu      sync.Mutex
	updates map[string]persistentSessionMetaEntry
	deletes map[string]struct{}
}

type sessionMetaPersistentBatchKey struct{}

var persistentSessionMetaState sessionMetaPersistentState
var persistentHistoryIndexState historyIndexPersistentState
var persistentCacheWriteHook func(path string)
var platformFileCacheKey = populatePlatformFileCacheKey
var replacePersistentCacheFile = replacePersistentCacheFilePlatform

const persistentCacheLockRetryDelay = 10 * time.Millisecond

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func newPersistentSessionMetaCache() persistentSessionMetaCache {
	return persistentSessionMetaCache{
		Version: persistentCacheVersion,
		Entries: map[string]persistentSessionMetaEntry{},
	}
}

func newPersistentHistoryIndexCache() persistentHistoryIndexCache {
	return persistentHistoryIndexCache{
		Version: persistentCacheVersion,
		Entries: map[string]persistentHistoryIndexEntry{},
	}
}

func persistentCacheDir() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		home, homeErr := os.UserHomeDir()
		if homeErr != nil {
			return "", err
		}
		base = filepath.Join(home, ".cache")
	}
	dir := filepath.Join(base, "codex-proxy", "codexhistory")
	if err := os.MkdirAll(dir, localPersistentCacheDirMode); err != nil {
		return "", fmt.Errorf("create codexhistory cache dir: %w", err)
	}
	return dir, nil
}

func sessionMetaCacheFile() (string, error) {
	dir, err := persistentCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "session_meta_cache.json"), nil
}

func historyIndexCacheFile() (string, error) {
	dir, err := persistentCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "history_index_cache.json"), nil
}

func readPersistentSessionMeta(filePath string, info os.FileInfo) (sessionFileMeta, bool) {
	meta, ok, _ := readPersistentSessionMetaContext(context.Background(), filePath, info)
	return meta, ok
}

func readPersistentSessionMetaContext(ctx context.Context, filePath string, info os.FileInfo) (sessionFileMeta, bool, error) {
	cachePath, err := sessionMetaCacheFile()
	if err == nil {
		persistentSessionMetaState.mu.Lock()
		cache, loadErr := loadSessionMetaPersistentStateLockedContext(ctx, cachePath)
		persistentSessionMetaState.mu.Unlock()
		if loadErr != nil {
			if isContextError(loadErr) {
				return sessionFileMeta{}, false, loadErr
			}
		} else {
			entry, ok := cache.Entries[filepath.Clean(filePath)]
			if ok && matchesFileInfo(filePath, info, entry.FileCacheKey) {
				return entry.Meta, true, nil
			}
		}
	}
	if meta, ok, sharedErr := readSharedPersistentSessionMetaContext(ctx, filePath, info); sharedErr != nil {
		return sessionFileMeta{}, false, sharedErr
	} else if ok {
		return meta, true, nil
	}
	return sessionFileMeta{}, false, nil
}

func writePersistentSessionMeta(filePath string, info os.FileInfo, meta sessionFileMeta) {
	_ = writePersistentSessionMetaContext(context.Background(), filePath, info, meta)
}

func writePersistentSessionMetaContext(ctx context.Context, filePath string, info os.FileInfo, meta sessionFileMeta) error {
	cachePath, err := sessionMetaCacheFile()
	cleanPath := filepath.Clean(filePath)
	entry := persistentSessionMetaEntry{
		FileCacheKey: newFileCacheKey(filePath, info),
		Meta:         meta,
	}

	if err == nil {
		if updateErr := updatePersistentSessionMetaCacheContext(ctx, cachePath, func(cache *persistentSessionMetaCache) {
			cache.Entries[cleanPath] = entry
		}); updateErr != nil {
			if isContextError(updateErr) {
				return updateErr
			}
		} else {
			persistentSessionMetaState.mu.Lock()
			if persistentSessionMetaState.loaded && persistentSessionMetaState.path == cachePath {
				if persistentSessionMetaState.cache.Entries == nil {
					persistentSessionMetaState.cache = newPersistentSessionMetaCache()
				}
				persistentSessionMetaState.cache.Entries[cleanPath] = entry
			} else {
				persistentSessionMetaState.path = cachePath
				persistentSessionMetaState.loaded = false
			}
			persistentSessionMetaState.cacheFilePresent, persistentSessionMetaState.cacheFileMtime = cacheFileState(cachePath)
			persistentSessionMetaState.mu.Unlock()
		}
	}
	return writeSharedPersistentSessionMetaContext(ctx, filePath, info, meta)
}

func deletePersistentSessionMeta(filePath string) {
	_ = deletePersistentSessionMetaContext(context.Background(), filePath)
}

func deletePersistentSessionMetaContext(ctx context.Context, filePath string) error {
	cachePath, err := sessionMetaCacheFile()
	cleanPath := filepath.Clean(filePath)
	if err == nil {
		if updateErr := updatePersistentSessionMetaCacheContext(ctx, cachePath, func(cache *persistentSessionMetaCache) {
			delete(cache.Entries, cleanPath)
		}); updateErr != nil {
			if isContextError(updateErr) {
				return updateErr
			}
		} else {
			persistentSessionMetaState.mu.Lock()
			if persistentSessionMetaState.loaded && persistentSessionMetaState.path == cachePath && persistentSessionMetaState.cache.Entries != nil {
				delete(persistentSessionMetaState.cache.Entries, cleanPath)
			} else {
				persistentSessionMetaState.path = cachePath
				persistentSessionMetaState.loaded = false
			}
			persistentSessionMetaState.cacheFilePresent, persistentSessionMetaState.cacheFileMtime = cacheFileState(cachePath)
			persistentSessionMetaState.mu.Unlock()
		}
	}
	return deleteSharedPersistentSessionMetaContext(ctx, filePath)
}

func readPersistentHistoryIndex(path string, info os.FileInfo) (historyIndex, bool) {
	idx, ok, _ := readPersistentHistoryIndexContext(context.Background(), path, info)
	return idx, ok
}

func readPersistentHistoryIndexContext(ctx context.Context, path string, info os.FileInfo) (historyIndex, bool, error) {
	cachePath, err := historyIndexCacheFile()
	if err == nil {
		persistentHistoryIndexState.mu.Lock()
		cache, loadErr := loadHistoryIndexPersistentStateLockedContext(ctx, cachePath)
		persistentHistoryIndexState.mu.Unlock()
		if loadErr != nil {
			if isContextError(loadErr) {
				return historyIndex{}, false, loadErr
			}
		} else {
			entry, ok := cache.Entries[filepath.Clean(path)]
			if ok && matchesFileInfo(path, info, entry.FileCacheKey) {
				return historyIndex{sessions: entry.Sessions}, true, nil
			}
		}
	}
	if idx, ok, sharedErr := readSharedPersistentHistoryIndexContext(ctx, path, info); sharedErr != nil {
		return historyIndex{}, false, sharedErr
	} else if ok {
		return idx, true, nil
	}
	return historyIndex{}, false, nil
}

func writePersistentHistoryIndex(path string, info os.FileInfo, idx historyIndex) {
	_ = writePersistentHistoryIndexContext(context.Background(), path, info, idx)
}

func writePersistentHistoryIndexContext(ctx context.Context, path string, info os.FileInfo, idx historyIndex) error {
	cachePath, err := historyIndexCacheFile()
	cleanPath := filepath.Clean(path)
	entry := persistentHistoryIndexEntry{
		FileCacheKey: newFileCacheKey(path, info),
		Sessions:     cloneHistorySessions(idx.sessions),
	}

	if err == nil {
		if updateErr := updatePersistentHistoryIndexCacheContext(ctx, cachePath, func(cache *persistentHistoryIndexCache) {
			cache.Entries[cleanPath] = entry
		}); updateErr != nil {
			if isContextError(updateErr) {
				return updateErr
			}
		} else {
			persistentHistoryIndexState.mu.Lock()
			if persistentHistoryIndexState.loaded && persistentHistoryIndexState.path == cachePath {
				if persistentHistoryIndexState.cache.Entries == nil {
					persistentHistoryIndexState.cache = newPersistentHistoryIndexCache()
				}
				persistentHistoryIndexState.cache.Entries[cleanPath] = entry
			} else {
				persistentHistoryIndexState.path = cachePath
				persistentHistoryIndexState.loaded = false
			}
			persistentHistoryIndexState.cacheFilePresent, persistentHistoryIndexState.cacheFileMtime = cacheFileState(cachePath)
			persistentHistoryIndexState.mu.Unlock()
		}
	}
	return writeSharedPersistentHistoryIndexContext(ctx, path, info, idx)
}

func deletePersistentHistoryIndex(path string) {
	_ = deletePersistentHistoryIndexContext(context.Background(), path)
}

func deletePersistentHistoryIndexContext(ctx context.Context, path string) error {
	cachePath, err := historyIndexCacheFile()
	cleanPath := filepath.Clean(path)
	if err == nil {
		if updateErr := updatePersistentHistoryIndexCacheContext(ctx, cachePath, func(cache *persistentHistoryIndexCache) {
			delete(cache.Entries, cleanPath)
		}); updateErr != nil {
			if isContextError(updateErr) {
				return updateErr
			}
		} else {
			persistentHistoryIndexState.mu.Lock()
			if persistentHistoryIndexState.loaded && persistentHistoryIndexState.path == cachePath && persistentHistoryIndexState.cache.Entries != nil {
				delete(persistentHistoryIndexState.cache.Entries, cleanPath)
			} else {
				persistentHistoryIndexState.path = cachePath
				persistentHistoryIndexState.loaded = false
			}
			persistentHistoryIndexState.cacheFilePresent, persistentHistoryIndexState.cacheFileMtime = cacheFileState(cachePath)
			persistentHistoryIndexState.mu.Unlock()
		}
	}
	return deleteSharedPersistentHistoryIndexContext(ctx, path)
}

func withSessionMetaPersistentBatch(ctx context.Context) (context.Context, *sessionMetaPersistentBatch) {
	if ctx == nil {
		ctx = context.Background()
	}
	batch := &sessionMetaPersistentBatch{
		updates: map[string]persistentSessionMetaEntry{},
		deletes: map[string]struct{}{},
	}
	return context.WithValue(ctx, sessionMetaPersistentBatchKey{}, batch), batch
}

func sessionMetaPersistentBatchFromContext(ctx context.Context) *sessionMetaPersistentBatch {
	if ctx == nil {
		return nil
	}
	batch, _ := ctx.Value(sessionMetaPersistentBatchKey{}).(*sessionMetaPersistentBatch)
	return batch
}

func stagePersistentSessionMetaWrite(ctx context.Context, filePath string, info os.FileInfo, meta sessionFileMeta) bool {
	batch := sessionMetaPersistentBatchFromContext(ctx)
	if batch == nil {
		return false
	}
	cleanPath := filepath.Clean(filePath)
	batch.mu.Lock()
	defer batch.mu.Unlock()
	delete(batch.deletes, cleanPath)
	batch.updates[cleanPath] = persistentSessionMetaEntry{
		FileCacheKey: newFileCacheKey(filePath, info),
		Meta:         meta,
	}
	return true
}

func stagePersistentSessionMetaDelete(ctx context.Context, filePath string) bool {
	batch := sessionMetaPersistentBatchFromContext(ctx)
	if batch == nil {
		return false
	}
	cleanPath := filepath.Clean(filePath)
	batch.mu.Lock()
	defer batch.mu.Unlock()
	delete(batch.updates, cleanPath)
	batch.deletes[cleanPath] = struct{}{}
	return true
}

func flushPersistentSessionMetaBatch(batch *sessionMetaPersistentBatch) {
	_ = flushPersistentSessionMetaBatchContext(context.Background(), batch)
}

func flushPersistentSessionMetaBatchContext(ctx context.Context, batch *sessionMetaPersistentBatch) error {
	if batch == nil {
		return nil
	}

	batch.mu.Lock()
	if len(batch.updates) == 0 && len(batch.deletes) == 0 {
		batch.mu.Unlock()
		return nil
	}
	updates := make(map[string]persistentSessionMetaEntry, len(batch.updates))
	for path, entry := range batch.updates {
		updates[path] = entry
	}
	deletes := make(map[string]struct{}, len(batch.deletes))
	for path := range batch.deletes {
		deletes[path] = struct{}{}
	}
	batch.updates = map[string]persistentSessionMetaEntry{}
	batch.deletes = map[string]struct{}{}
	batch.mu.Unlock()

	cachePath, err := sessionMetaCacheFile()
	if err == nil {
		if updateErr := updatePersistentSessionMetaCacheContext(ctx, cachePath, func(cache *persistentSessionMetaCache) {
			for path := range deletes {
				delete(cache.Entries, path)
			}
			for path, entry := range updates {
				cache.Entries[path] = entry
			}
		}); updateErr != nil {
			if isContextError(updateErr) {
				return updateErr
			}
		} else {
			persistentSessionMetaState.mu.Lock()
			if persistentSessionMetaState.loaded && persistentSessionMetaState.path == cachePath {
				if persistentSessionMetaState.cache.Entries == nil {
					persistentSessionMetaState.cache = newPersistentSessionMetaCache()
				}
				for path := range deletes {
					delete(persistentSessionMetaState.cache.Entries, path)
				}
				for path, entry := range updates {
					persistentSessionMetaState.cache.Entries[path] = entry
				}
			} else {
				persistentSessionMetaState.path = cachePath
				persistentSessionMetaState.loaded = false
			}
			persistentSessionMetaState.cacheFilePresent, persistentSessionMetaState.cacheFileMtime = cacheFileState(cachePath)
			persistentSessionMetaState.mu.Unlock()
		}
	}
	return writeSharedPersistentSessionMetaBatchContext(ctx, updates, deletes)
}

func matchesFileInfo(path string, info os.FileInfo, key fileCacheKey) bool {
	if info == nil {
		return false
	}
	current := newFileCacheKey(path, info)
	if current.Size != key.Size ||
		current.MtimeUnixNano != key.MtimeUnixNano ||
		current.Mode != key.Mode {
		return false
	}
	if key.HasFileID {
		if !current.HasFileID || current.Dev != key.Dev || current.Ino != key.Ino {
			return false
		}
	}
	if key.HasCtime {
		if !current.HasCtime || current.CtimeUnixNano != key.CtimeUnixNano {
			return false
		}
	}
	return true
}

func cloneHistorySessions(src map[string]*historySessionInfo) map[string]*historySessionInfo {
	if len(src) == 0 {
		return map[string]*historySessionInfo{}
	}
	dst := make(map[string]*historySessionInfo, len(src))
	for key, value := range src {
		if value == nil {
			dst[key] = nil
			continue
		}
		entry := *value
		dst[key] = &entry
	}
	return dst
}

func newFileCacheKey(path string, info os.FileInfo) fileCacheKey {
	key := fileCacheKey{}
	if info == nil {
		return key
	}

	key.Size = info.Size()
	key.MtimeUnixNano = info.ModTime().UnixNano()
	key.Mode = uint32(info.Mode())

	sys := info.Sys()
	if sys == nil {
		return key
	}
	value := reflect.ValueOf(sys)
	if !value.IsValid() {
		return key
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return key
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return key
	}

	if dev, ok := statUintField(value, "Dev"); ok {
		if ino, ok := statUintField(value, "Ino"); ok {
			key.HasFileID = true
			key.Dev = dev
			key.Ino = ino
		}
	}
	if ctime, ok := statCtimeUnixNano(value); ok {
		key.HasCtime = true
		key.CtimeUnixNano = ctime
	}
	platformFileCacheKey(path, info, &key)
	return key
}

func cacheFileState(path string) (bool, int64) {
	info, err := os.Stat(path)
	if err != nil {
		return false, 0
	}
	return true, info.ModTime().UnixNano()
}

func statUintField(value reflect.Value, name string) (uint64, bool) {
	field := value.FieldByName(name)
	if !field.IsValid() {
		return 0, false
	}
	switch field.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return field.Uint(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if field.Int() < 0 {
			return 0, false
		}
		return uint64(field.Int()), true
	default:
		return 0, false
	}
}

func statIntField(value reflect.Value, name string) (int64, bool) {
	field := value.FieldByName(name)
	if !field.IsValid() {
		return 0, false
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return field.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return int64(field.Uint()), true
	default:
		return 0, false
	}
}

func statTimespecUnixNano(value reflect.Value) (int64, bool) {
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return 0, false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return 0, false
	}
	if sec, ok := statIntField(value, "Sec"); ok {
		if nsec, ok := statIntField(value, "Nsec"); ok {
			return sec*int64(time.Second) + nsec, true
		}
	}
	if sec, ok := statIntField(value, "Tv_sec"); ok {
		if nsec, ok := statIntField(value, "Tv_nsec"); ok {
			return sec*int64(time.Second) + nsec, true
		}
	}
	return 0, false
}

func statCtimeUnixNano(value reflect.Value) (int64, bool) {
	for _, name := range []string{"Ctim", "Ctimespec"} {
		field := value.FieldByName(name)
		if !field.IsValid() {
			continue
		}
		if ts, ok := statTimespecUnixNano(field); ok {
			return ts, true
		}
	}
	if sec, ok := statIntField(value, "Ctimesec"); ok {
		if nsec, ok := statIntField(value, "Ctimensec"); ok {
			return sec*int64(time.Second) + nsec, true
		}
	}
	if sec, ok := statIntField(value, "Ctime"); ok {
		return sec * int64(time.Second), true
	}
	return 0, false
}

func loadSessionMetaPersistentStateLocked(cachePath string) persistentSessionMetaCache {
	cache, err := loadSessionMetaPersistentStateLockedContext(context.Background(), cachePath)
	if err != nil {
		return newPersistentSessionMetaCache()
	}
	return cache
}

func loadSessionMetaPersistentStateLockedContext(ctx context.Context, cachePath string) (persistentSessionMetaCache, error) {
	present, mtime := cacheFileState(cachePath)
	if persistentSessionMetaState.loaded &&
		persistentSessionMetaState.path == cachePath &&
		persistentSessionMetaState.cacheFilePresent == present &&
		(!present || persistentSessionMetaState.cacheFileMtime == mtime) {
		return persistentSessionMetaState.cache, nil
	}

	cache, err := loadPersistentSessionMetaCacheContext(ctx, cachePath)
	if err != nil {
		if isContextError(err) {
			return persistentSessionMetaCache{}, err
		}
		cache = newPersistentSessionMetaCache()
	}
	persistentSessionMetaState.path = cachePath
	persistentSessionMetaState.cacheFilePresent = present
	persistentSessionMetaState.cacheFileMtime = mtime
	persistentSessionMetaState.loaded = true
	persistentSessionMetaState.cache = cache
	return cache, nil
}

func loadHistoryIndexPersistentStateLocked(cachePath string) persistentHistoryIndexCache {
	cache, err := loadHistoryIndexPersistentStateLockedContext(context.Background(), cachePath)
	if err != nil {
		return newPersistentHistoryIndexCache()
	}
	return cache
}

func loadHistoryIndexPersistentStateLockedContext(ctx context.Context, cachePath string) (persistentHistoryIndexCache, error) {
	present, mtime := cacheFileState(cachePath)
	if persistentHistoryIndexState.loaded &&
		persistentHistoryIndexState.path == cachePath &&
		persistentHistoryIndexState.cacheFilePresent == present &&
		(!present || persistentHistoryIndexState.cacheFileMtime == mtime) {
		return persistentHistoryIndexState.cache, nil
	}

	cache, err := loadPersistentHistoryIndexCacheContext(ctx, cachePath)
	if err != nil {
		if isContextError(err) {
			return persistentHistoryIndexCache{}, err
		}
		cache = newPersistentHistoryIndexCache()
	}
	persistentHistoryIndexState.path = cachePath
	persistentHistoryIndexState.cacheFilePresent = present
	persistentHistoryIndexState.cacheFileMtime = mtime
	persistentHistoryIndexState.loaded = true
	persistentHistoryIndexState.cache = cache
	return cache, nil
}

func loadPersistentSessionMetaCache(path string) (persistentSessionMetaCache, error) {
	return loadPersistentSessionMetaCacheContext(context.Background(), path)
}

func loadPersistentSessionMetaCacheContext(ctx context.Context, path string) (persistentSessionMetaCache, error) {
	cache := newPersistentSessionMetaCache()
	err := withLockedCacheContext(ctx, path, func() error {
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := json.Unmarshal(data, &cache); err != nil {
			return err
		}
		if cache.Version != persistentCacheVersion {
			cache = newPersistentSessionMetaCache()
		}
		if cache.Entries == nil {
			cache.Entries = map[string]persistentSessionMetaEntry{}
		}
		return nil
	})
	return cache, err
}

func updatePersistentSessionMetaCache(path string, fn func(*persistentSessionMetaCache)) error {
	return updatePersistentSessionMetaCacheContext(context.Background(), path, fn)
}

func updatePersistentSessionMetaCacheContext(ctx context.Context, path string, fn func(*persistentSessionMetaCache)) error {
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
		return writeJSONAtomically(path, cache)
	})
}

func loadPersistentHistoryIndexCache(path string) (persistentHistoryIndexCache, error) {
	return loadPersistentHistoryIndexCacheContext(context.Background(), path)
}

func loadPersistentHistoryIndexCacheContext(ctx context.Context, path string) (persistentHistoryIndexCache, error) {
	cache := newPersistentHistoryIndexCache()
	err := withLockedCacheContext(ctx, path, func() error {
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := json.Unmarshal(data, &cache); err != nil {
			return err
		}
		if cache.Version != persistentCacheVersion {
			cache = newPersistentHistoryIndexCache()
		}
		if cache.Entries == nil {
			cache.Entries = map[string]persistentHistoryIndexEntry{}
		}
		return nil
	})
	return cache, err
}

func updatePersistentHistoryIndexCache(path string, fn func(*persistentHistoryIndexCache)) error {
	return updatePersistentHistoryIndexCacheContext(context.Background(), path, fn)
}

func updatePersistentHistoryIndexCacheContext(ctx context.Context, path string, fn func(*persistentHistoryIndexCache)) error {
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
		return writeJSONAtomically(path, cache)
	})
}

func withLockedCache(path string, fn func() error) error {
	return withLockedCacheContext(context.Background(), path, fn)
}

func withLockedCacheContext(ctx context.Context, path string, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	lock := flock.New(path + ".lock")
	ok, err := lock.TryLockContext(ctx, persistentCacheLockRetryDelay)
	if err != nil {
		return fmt.Errorf("lock cache: %w", err)
	}
	if !ok {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("lock cache: %w", err)
		}
		return fmt.Errorf("lock cache: could not acquire lock %s", path)
	}
	defer func() { _ = lock.Unlock() }()
	return fn()
}

func writeJSONAtomically(path string, payload any) error {
	return writeJSONAtomicallyWithOptions(path, payload, persistentCacheWriteOptions{
		dirMode:  localPersistentCacheDirMode,
		fileMode: localPersistentCacheFileMode,
	})
}

func writeJSONAtomicallyWithOptions(path string, payload any, opts persistentCacheWriteOptions) error {
	dir := filepath.Dir(path)
	if opts.dirMode == 0 {
		opts.dirMode = localPersistentCacheDirMode
	}
	if opts.fileMode == 0 {
		opts.fileMode = localPersistentCacheFileMode
	}
	if err := os.MkdirAll(dir, opts.dirMode); err != nil {
		return err
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()

	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Chmod(opts.fileMode); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := replacePersistentCacheFile(tmpPath, path); err != nil {
		return err
	}
	if persistentCacheWriteHook != nil {
		persistentCacheWriteHook(path)
	}
	return nil
}
