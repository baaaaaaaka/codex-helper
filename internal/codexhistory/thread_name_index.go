package codexhistory

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

// Readers share only the committed names under a short lookup lock. A final
// newline-less record is private to the returned view.
type threadNameIndex struct {
	data *threadNameData
	tail map[string]string
}
type threadNameData struct {
	mu    sync.RWMutex
	names map[string]string
}

func (i threadNameIndex) lookup(id string) string {
	if name, ok := i.tail[id]; ok {
		return name
	}
	if i.data == nil {
		return ""
	}
	i.data.mu.RLock()
	defer i.data.mu.RUnlock()
	return i.data.names[id]
}
func (i threadNameIndex) size() int {
	if i.data == nil {
		return len(i.tail)
	}
	i.data.mu.RLock()
	defer i.data.mu.RUnlock()
	n := len(i.data.names)
	for id := range i.tail {
		if _, ok := i.data.names[id]; !ok {
			n++
		}
	}
	return n
}

type cachedThreadNameIndex struct {
	updateMu sync.Mutex
	fileKey  fileCacheKey
	loaded   bool
	header   catalogHistorySourceEntry
	result   threadNameIndex
}

var threadNameIndexCache = struct {
	mu      sync.Mutex
	entries map[string]*cachedThreadNameIndex
}{entries: map[string]*cachedThreadNameIndex{}}
var openThreadNameIndexFile = os.Open

func loadThreadNameIndexContext(ctx context.Context, root string) (threadNameIndex, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return threadNameIndex{}, err
	}
	path := filepath.Join(root, threadNameIndexFile)
	threadNameIndexCache.mu.Lock()
	cached := threadNameIndexCache.entries[path]
	if cached == nil {
		if len(threadNameIndexCache.entries) >= 16 {
			threadNameIndexCache.entries = map[string]*cachedThreadNameIndex{}
		}
		cached = &cachedThreadNameIndex{}
		threadNameIndexCache.entries[path] = cached
	}
	threadNameIndexCache.mu.Unlock()
	cached.updateMu.Lock()
	defer cached.updateMu.Unlock()
	info, err := os.Stat(path)
	if err != nil {
		cached.loaded = false
		cached.header = catalogHistorySourceEntry{}
		cached.result = threadNameIndex{}
		return threadNameIndex{}, nil
	}
	key := newFileCacheKey(path, info)
	if cached.loaded && cached.fileKey == key {
		return cached.result, nil
	}
	names, err := loadPersistentThreadNames(ctx, path, info, cached)
	if err != nil {
		cached.loaded = false
		if isContextError(err) {
			return threadNameIndex{}, err
		}
		return threadNameIndex{}, nil
	}
	cached.fileKey, cached.loaded, cached.result = key, true, names
	return names, nil
}

func readThreadNameIndexContext(ctx context.Context, path string) (map[string]string, error) {
	return readThreadNamesWindow(ctx, path, 0, -1, nil)
}
func readThreadNamesWindow(ctx context.Context, path string, offset, size int64, names map[string]string) (map[string]string, error) {
	if names == nil {
		names = map[string]string{}
	}
	f, err := openThreadNameIndexFile(path)
	if err != nil {
		return names, err
	}
	defer f.Close()

	if size < 0 {
		info, err := f.Stat()
		if err != nil {
			return names, err
		}
		size = info.Size() - offset
	}
	err = readJSONLWindow(ctx, f, offset, size, func(line []byte) error {
		var entry threadNameIndexEntry
		if json.Unmarshal(line, &entry) == nil {
			id, name := strings.TrimSpace(entry.SessionID), strings.TrimSpace(entry.ThreadName)
			if id != "" && name != "" {
				names[id] = name
			}
		}
		return nil
	})
	return names, err
}

func deleteThreadNameIndexCacheEntry(path string) {
	threadNameIndexCache.mu.Lock()
	delete(threadNameIndexCache.entries, path)
	threadNameIndexCache.mu.Unlock()
}

func resetThreadNameIndexCache() {
	threadNameIndexCache.mu.Lock()
	threadNameIndexCache.entries = map[string]*cachedThreadNameIndex{}
	threadNameIndexCache.mu.Unlock()
}

func loadPersistentThreadNames(ctx context.Context, path string, info os.FileInfo, cached *cachedThreadNameIndex) (threadNameIndex, error) {
	fallback := func() (threadNameIndex, error) {
		cached.header = catalogHistorySourceEntry{}
		names, err := readThreadNameIndexContext(ctx, path)
		return threadNameIndex{data: &threadNameData{names: names}}, err
	}
	withTail := func(data *threadNameData, complete int64) (threadNameIndex, error) {
		view := threadNameIndex{data: data}
		if complete < info.Size() {
			tail, err := readThreadNamesWindow(ctx, path, complete, info.Size()-complete, nil)
			view.tail = tail
			return view, err
		}
		return view, nil
	}
	store, err := currentCatalogSQLiteStore(path)
	if err != nil {
		return fallback()
	}
	complete, ok := sessionPreviewCompleteOffset(path, info)
	if !ok {
		return fallback()
	}
	for attempt := 0; attempt < 3; attempt++ {
		tx, err := store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return fallback()
		}
		header, found, err := loadCatalogIndexHeader(ctx, tx, path)
		data := cached.result.data
		shared := found && data != nil && cached.header.generation == header.generation && cached.header.parsedOffset == header.parsedOffset && cached.header.fileKey == header.fileKey
		names := map[string]string{}
		if err == nil && found && !shared {
			rows, e := tx.QueryContext(ctx, `SELECT session_id,name FROM thread_name WHERE source_path=?`, path)
			if e != nil {
				err = e
			} else {
				for rows.Next() {
					var id, name string
					if e = rows.Scan(&id, &name); e != nil {
						err = e
						break
					}
					names[id] = name
				}
				if e = rows.Err(); e != nil {
					err = e
				}
				rows.Close()
			}
		}
		tx.Rollback()
		if err != nil {
			return fallback()
		}
		if !shared {
			data = &threadNameData{names: names}
		}
		if found && header.parsedOffset == complete && matchesFileInfo(path, info, header.fileKey) {
			cached.header = header
			return withTail(data, complete)
		}
		start := int64(0)
		appendable := found && canAppendCacheFile(path, info, header.fileKey, header.parsedOffset, header.prefixTailHash, header.prefixTailSize)
		if appendable {
			start = header.parsedOffset
		} else {
			data = &threadNameData{names: map[string]string{}}
		}
		if appendable && start == complete {
			cached.header = header
			return withTail(data, complete)
		}
		changed, err := readThreadNamesWindow(ctx, path, start, complete-start, nil)
		if err != nil {
			return threadNameIndex{}, err
		}
		hash, size, valid := sessionPreviewPrefixTailHash(path, complete)
		if !valid || !unchangedJSONLPath(path, info) {
			return fallback()
		}
		err = store.writeIndex(ctx, header, found, !appendable, newFileCacheKey(path, info), complete, hash, size, func(tx *sql.Tx) error {
			if !appendable {
				if _, err := tx.ExecContext(ctx, `DELETE FROM thread_name WHERE source_path=?`, path); err != nil {
					return err
				}
			}
			for id, name := range changed {
				if _, err := tx.ExecContext(ctx, `INSERT INTO thread_name VALUES(?,?,?) ON CONFLICT(source_path,session_id) DO UPDATE SET name=excluded.name WHERE name<>excluded.name`, path, id, name); err != nil {
					return err
				}
			}
			return nil
		})
		if errors.Is(err, errCatalogSQLiteConflict) {
			continue
		}
		if err != nil {
			return fallback()
		}
		data.mu.Lock()
		for id, name := range changed {
			data.names[id] = name
		}
		data.mu.Unlock()
		if !found {
			header.generation = 1
		} else if !appendable {
			header.generation++
		}
		header.path, header.fileKey, header.parsedOffset = path, newFileCacheKey(path, info), complete
		header.prefixTailHash, header.prefixTailSize = hash, size
		cached.header = header
		return withTail(data, complete)
	}
	return fallback()
}
