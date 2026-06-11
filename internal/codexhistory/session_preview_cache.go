package codexhistory

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const sessionPreviewFilterVersion = "status-answer-v2"
const sessionPreviewPrefixHashBytes int64 = 64 * 1024

type persistentSessionPreviewCache struct {
	Version int                                      `json:"version"`
	Entries map[string]persistentSessionPreviewEntry `json:"entries"`
}

type persistentSessionPreviewEntry struct {
	FileCacheKey         fileCacheKey                              `json:"fileCacheKey"`
	FilterVersion        string                                    `json:"filterVersion"`
	Offset               int64                                     `json:"offset"`
	PrefixTailHash       string                                    `json:"prefixTailHash,omitempty"`
	PrefixTailSize       int64                                     `json:"prefixTailSize,omitempty"`
	Messages             []persistentSessionPreviewMessage         `json:"messages"`
	FormattedText        string                                    `json:"formattedText,omitempty"`
	SeenSourceIDs        []string                                  `json:"seenSourceIds,omitempty"`
	SeenFallbackMessages []persistentSessionPreviewFallbackMessage `json:"seenFallbackMessages,omitempty"`
}

type persistentSessionPreviewMessage struct {
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	SourceID  string    `json:"sourceId,omitempty"`
}

type persistentSessionPreviewFallbackMessage struct {
	Key       string    `json:"key"`
	Timestamp time.Time `json:"timestamp,omitempty"`
}

type sessionPreviewPersistentState struct {
	mu               sync.Mutex
	path             string
	cacheFilePresent bool
	cacheFileMtime   int64
	loaded           bool
	cache            persistentSessionPreviewCache
}

var persistentSessionPreviewState sessionPreviewPersistentState

func newPersistentSessionPreviewCache() persistentSessionPreviewCache {
	return persistentSessionPreviewCache{
		Version: persistentCacheVersion,
		Entries: map[string]persistentSessionPreviewEntry{},
	}
}

func sessionPreviewCacheFile() (string, error) {
	dir, err := persistentCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "session_preview_cache.json"), nil
}

func readSessionPreviewMessagesCached(filePath string) ([]Message, error) {
	messages, _, err := readSessionPreviewCacheValue(filePath, true)
	return messages, err
}

func readSessionPreviewTextCached(filePath string) (string, error) {
	_, text, err := readSessionPreviewCacheValue(filePath, false)
	return text, err
}

func readSessionPreviewCacheValue(filePath string, wantMessages bool) ([]Message, string, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		_ = deletePersistentSessionPreview(filePath)
		return nil, "", err
	}
	cachePath, err := sessionPreviewCacheFile()
	if err != nil {
		return readSessionPreviewUncached(filePath)
	}
	entry, ok := readPersistentSessionPreviewEntry(cachePath, filePath)
	if ok && entry.FilterVersion == sessionPreviewFilterVersion {
		if matchesFileInfo(filePath, info, entry.FileCacheKey) {
			if !wantMessages && entry.FormattedText != "" {
				return nil, entry.FormattedText, nil
			}
			messages := messagesFromPersistentSessionPreview(entry.Messages)
			return messages, sessionPreviewEntryText(entry, messages), nil
		}
		if canAppendPersistentSessionPreview(filePath, info, entry) {
			completeOffset, ok := sessionPreviewCompleteOffset(filePath, info)
			if !ok {
				return readSessionPreviewUncached(filePath)
			}
			if completeOffset < info.Size() {
				return readSessionPreviewUncached(filePath)
			}
			if completeOffset >= entry.Offset {
				seen := persistentSessionPreviewSeenState(entry)
				tail, err := readSessionMessagesWindow(filePath, entry.Offset, completeOffset-entry.Offset, 0, isPreviewMessage, seen)
				if err != nil {
					return nil, "", err
				}
				messages := messagesFromPersistentSessionPreview(entry.Messages)
				baseText := sessionPreviewEntryText(entry, messages)
				messages = append(messages, tail...)
				text := appendPreviewText(baseText, FormatPreviewMessages(tail, 0))
				_ = writePersistentSessionPreviewEntry(cachePath, filePath, info, completeOffset, messages, text, seen)
				return messages, text, nil
			}
		}
	}

	completeOffset, ok := sessionPreviewCompleteOffset(filePath, info)
	if !ok {
		return readSessionPreviewUncached(filePath)
	}
	if completeOffset < info.Size() {
		return readSessionPreviewUncached(filePath)
	}
	messages, err := readSessionMessagesWindow(filePath, 0, completeOffset, 0, isPreviewMessage)
	if err != nil {
		return nil, "", err
	}
	text := FormatPreviewMessages(messages, 0)
	seen := seenStateFromMessages(messages)
	_ = writePersistentSessionPreviewEntry(cachePath, filePath, info, completeOffset, messages, text, seen)
	return messages, text, nil
}

func readPersistentSessionPreviewEntry(cachePath string, filePath string) (persistentSessionPreviewEntry, bool) {
	cache, err := loadSessionPreviewPersistentState(cachePath)
	if err != nil {
		return persistentSessionPreviewEntry{}, false
	}
	entry, ok := cache.Entries[filepath.Clean(filePath)]
	return entry, ok
}

func loadSessionPreviewPersistentState(cachePath string) (persistentSessionPreviewCache, error) {
	present, mtime := cacheFileState(cachePath)
	persistentSessionPreviewState.mu.Lock()
	defer persistentSessionPreviewState.mu.Unlock()
	if persistentSessionPreviewState.loaded &&
		persistentSessionPreviewState.path == cachePath &&
		persistentSessionPreviewState.cacheFilePresent == present &&
		(!present || persistentSessionPreviewState.cacheFileMtime == mtime) {
		return persistentSessionPreviewState.cache, nil
	}
	cache := newPersistentSessionPreviewCache()
	err := withLockedCache(cachePath, func() error {
		data, err := os.ReadFile(cachePath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := json.Unmarshal(data, &cache); err != nil {
			return err
		}
		if cache.Version != persistentCacheVersion || cache.Entries == nil {
			cache = newPersistentSessionPreviewCache()
		}
		return nil
	})
	if err != nil {
		return persistentSessionPreviewCache{}, err
	}
	persistentSessionPreviewState.path = cachePath
	persistentSessionPreviewState.cacheFilePresent, persistentSessionPreviewState.cacheFileMtime = cacheFileState(cachePath)
	persistentSessionPreviewState.loaded = true
	persistentSessionPreviewState.cache = cache
	return cache, nil
}

func writePersistentSessionPreviewEntry(cachePath string, filePath string, info os.FileInfo, offset int64, messages []Message, text string, seen *messageSeenState) error {
	cleanPath := filepath.Clean(filePath)
	entry := persistentSessionPreviewEntry{
		FileCacheKey:         newFileCacheKey(filePath, info),
		FilterVersion:        sessionPreviewFilterVersion,
		Offset:               offset,
		Messages:             persistentMessagesFromSessionPreview(messages),
		FormattedText:        text,
		SeenSourceIDs:        seenSourceIDsFromMessages(messages),
		SeenFallbackMessages: seenFallbackMessagesFromState(seen),
	}
	entry.PrefixTailHash, entry.PrefixTailSize, _ = sessionPreviewPrefixTailHash(filePath, entry.Offset)
	return updatePersistentSessionPreviewCache(cachePath, func(cache *persistentSessionPreviewCache) {
		cache.Entries[cleanPath] = entry
	})
}

func deletePersistentSessionPreview(filePath string) error {
	cachePath, err := sessionPreviewCacheFile()
	if err != nil {
		return nil
	}
	cleanPath := filepath.Clean(filePath)
	return updatePersistentSessionPreviewCache(cachePath, func(cache *persistentSessionPreviewCache) {
		delete(cache.Entries, cleanPath)
	})
}

func updatePersistentSessionPreviewCache(cachePath string, fn func(*persistentSessionPreviewCache)) error {
	return withLockedCache(cachePath, func() error {
		cache := newPersistentSessionPreviewCache()
		if data, err := os.ReadFile(cachePath); err == nil {
			if json.Unmarshal(data, &cache) != nil || cache.Version != persistentCacheVersion || cache.Entries == nil {
				cache = newPersistentSessionPreviewCache()
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		fn(&cache)
		if err := writeJSONAtomically(cachePath, cache); err != nil {
			return err
		}
		persistentSessionPreviewState.mu.Lock()
		persistentSessionPreviewState.path = cachePath
		persistentSessionPreviewState.cacheFilePresent, persistentSessionPreviewState.cacheFileMtime = cacheFileState(cachePath)
		persistentSessionPreviewState.loaded = true
		persistentSessionPreviewState.cache = cache
		persistentSessionPreviewState.mu.Unlock()
		return nil
	})
}

func canAppendPersistentSessionPreview(path string, info os.FileInfo, entry persistentSessionPreviewEntry) bool {
	if info == nil || entry.Offset < 0 || entry.Offset > info.Size() {
		return false
	}
	current := newFileCacheKey(path, info)
	previous := entry.FileCacheKey
	if previous.Mode != current.Mode {
		return false
	}
	sameFile := false
	if previous.HasFileID && current.HasFileID {
		sameFile = previous.Dev == current.Dev && previous.Ino == current.Ino
	} else if previous.HasCtime && current.HasCtime {
		sameFile = previous.CtimeUnixNano == current.CtimeUnixNano
	}
	if !sameFile {
		return false
	}
	if entry.PrefixTailSize <= 0 || entry.PrefixTailHash == "" {
		return false
	}
	hash, size, ok := sessionPreviewPrefixTailHash(path, entry.Offset)
	return ok && size == entry.PrefixTailSize && hash == entry.PrefixTailHash
}

func sessionPreviewPrefixTailHash(path string, offset int64) (string, int64, bool) {
	if offset <= 0 {
		return "", 0, true
	}
	size := sessionPreviewPrefixHashBytes
	if size > offset {
		size = offset
	}
	if size <= 0 {
		return "", 0, true
	}
	f, err := os.Open(path)
	if err != nil {
		return "", 0, false
	}
	defer f.Close()
	buf := make([]byte, size)
	n, err := f.ReadAt(buf, offset-size)
	if err != nil && n <= 0 {
		return "", 0, false
	}
	buf = buf[:n]
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:]), int64(n), true
}

func sessionPreviewCompleteOffset(path string, info os.FileInfo) (int64, bool) {
	if info == nil {
		return 0, false
	}
	size := info.Size()
	if size <= 0 {
		return 0, true
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer f.Close()

	var last [1]byte
	n, err := f.ReadAt(last[:], size-1)
	if err != nil && n <= 0 {
		return 0, false
	}
	if n == 1 && last[0] == '\n' {
		return size, true
	}

	const chunkSize int64 = 64 * 1024
	end := size
	for end > 0 {
		start := end - chunkSize
		if start < 0 {
			start = 0
		}
		buf := make([]byte, end-start)
		n, err := f.ReadAt(buf, start)
		if err != nil && n <= 0 {
			return 0, false
		}
		if idx := bytes.LastIndexByte(buf[:n], '\n'); idx >= 0 {
			return start + int64(idx) + 1, true
		}
		end = start
	}
	return 0, true
}

func readSessionPreviewUncached(filePath string) ([]Message, string, error) {
	messages, err := readSessionMessages(filePath, 0, isPreviewMessage)
	if err != nil {
		return nil, "", err
	}
	return messages, FormatPreviewMessages(messages, 0), nil
}

func sessionPreviewEntryText(entry persistentSessionPreviewEntry, messages []Message) string {
	if entry.FormattedText != "" || len(messages) == 0 {
		return entry.FormattedText
	}
	return FormatPreviewMessages(messages, 0)
}

func appendPreviewText(base string, tail string) string {
	base = strings.TrimSpace(base)
	tail = strings.TrimSpace(tail)
	if base == "" {
		return tail
	}
	if tail == "" {
		return base
	}
	return base + "\n\n" + tail
}

func persistentMessagesFromSessionPreview(messages []Message) []persistentSessionPreviewMessage {
	out := make([]persistentSessionPreviewMessage, 0, len(messages))
	for _, msg := range messages {
		out = append(out, persistentSessionPreviewMessage{
			Role:      msg.Role,
			Content:   msg.Content,
			Timestamp: msg.Timestamp,
			SourceID:  msg.sourceID,
		})
	}
	return out
}

func messagesFromPersistentSessionPreview(messages []persistentSessionPreviewMessage) []Message {
	out := make([]Message, 0, len(messages))
	for _, msg := range messages {
		out = append(out, Message{
			Role:      msg.Role,
			Content:   msg.Content,
			Timestamp: msg.Timestamp,
			sourceID:  msg.SourceID,
		})
	}
	return out
}

func persistentSessionPreviewSeenState(entry persistentSessionPreviewEntry) *messageSeenState {
	seen := newMessageSeenState()
	for _, msg := range entry.Messages {
		rememberMessageSeen(Message{
			Role:      msg.Role,
			Content:   msg.Content,
			Timestamp: msg.Timestamp,
			sourceID:  msg.SourceID,
		}, seen)
	}
	for _, id := range entry.SeenSourceIDs {
		if key := sourceMessageDedupKey(id); key != "" {
			seen.sourceKeys[key] = true
		}
	}
	for _, msg := range entry.SeenFallbackMessages {
		if msg.Key != "" && !msg.Timestamp.IsZero() {
			if previous, ok := seen.fallbackTimes[msg.Key]; !ok || msg.Timestamp.After(previous) {
				seen.fallbackTimes[msg.Key] = msg.Timestamp
			}
		}
	}
	return seen
}

func seenSourceIDsFromMessages(messages []Message) []string {
	seen := map[string]bool{}
	for _, msg := range messages {
		if msg.sourceID != "" {
			seen[msg.sourceID] = true
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func seenStateFromMessages(messages []Message) *messageSeenState {
	seen := newMessageSeenState()
	for _, msg := range messages {
		rememberMessageSeen(msg, seen)
	}
	return seen
}

func seenFallbackMessagesFromState(seen *messageSeenState) []persistentSessionPreviewFallbackMessage {
	if seen == nil || len(seen.fallbackTimes) == 0 {
		return nil
	}
	out := make([]persistentSessionPreviewFallbackMessage, 0, len(seen.fallbackTimes))
	for key, ts := range seen.fallbackTimes {
		if key != "" && !ts.IsZero() {
			out = append(out, persistentSessionPreviewFallbackMessage{Key: key, Timestamp: ts})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key == out[j].Key {
			return out[i].Timestamp.Before(out[j].Timestamp)
		}
		return out[i].Key < out[j].Key
	})
	return out
}
