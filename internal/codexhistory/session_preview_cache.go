package codexhistory

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"time"
)

// SessionPreviewFilterVersion is shared with the TUI's in-memory cache metadata
// so a preview policy change invalidates both cache layers together.
const SessionPreviewFilterVersion = "user-status-answer-v5"
const sessionPreviewFilterVersion = SessionPreviewFilterVersion
const sessionPreviewPrefixHashBytes int64 = 64 * 1024

// "off" is retained for diagnostics. Every other value, including the former
// "json" rollback value, selects SQLite so JSON cache writes cannot reappear.
const envSessionPreviewCacheBackend = "CODEX_HELPER_PREVIEW_CACHE_BACKEND"

const (
	sessionPreviewBackendSQLite = "sqlite"
	sessionPreviewBackendOff    = "off"
)

type persistentSessionPreviewFallbackMessage struct {
	Key        string
	Timestamp  time.Time
	SourceKind string
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
	if sessionPreviewCacheBackend() == sessionPreviewBackendOff {
		return readSessionPreviewUncached(filePath)
	}
	messages, text, err := readSessionPreviewCacheValueSQLite(filePath, wantMessages)
	if err == nil {
		return messages, text, nil
	}
	// Cache state is disposable acceleration. Open, lock, permission, and
	// corruption failures must never hide the source session.
	return readSessionPreviewUncached(filePath)
}

func sessionPreviewCacheBackend() string {
	if strings.EqualFold(strings.TrimSpace(os.Getenv(envSessionPreviewCacheBackend)), sessionPreviewBackendOff) {
		return sessionPreviewBackendOff
	}
	return sessionPreviewBackendSQLite
}

func canAppendCacheFile(path string, info os.FileInfo, previous fileCacheKey, offset int64, prefixTailHash string, prefixTailSize int64) bool {
	if info == nil || offset < 0 || offset > info.Size() {
		return false
	}
	current := newFileCacheKey(path, info)
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
	if offset == 0 && prefixTailSize == 0 && prefixTailHash == "" {
		return true
	}
	if prefixTailSize <= 0 || prefixTailHash == "" {
		return false
	}
	hash, size, ok := sessionPreviewPrefixTailHash(path, offset)
	return ok && size == prefixTailSize && hash == prefixTailHash
}

func sessionPreviewPrefixTailHash(path string, offset int64) (string, int64, bool) {
	if offset <= 0 {
		return "", 0, true
	}
	f, err := os.Open(path)
	if err != nil {
		return "", 0, false
	}
	defer f.Close()
	headSize := sessionPreviewPrefixHashBytes / 2
	if headSize > offset {
		headSize = offset
	}
	tailStart := offset - sessionPreviewPrefixHashBytes/2
	if tailStart < headSize {
		tailStart = headSize
	}
	tailSize := offset - tailStart
	buf := make([]byte, headSize+tailSize)
	headRead, err := f.ReadAt(buf[:headSize], 0)
	if err != nil && int64(headRead) < headSize {
		return "", 0, false
	}
	tailRead := 0
	if tailSize > 0 {
		tailRead, err = f.ReadAt(buf[headSize:], tailStart)
		if err != nil && int64(tailRead) < tailSize {
			return "", 0, false
		}
	}
	buf = buf[:headRead+tailRead]
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:]), int64(len(buf)), true
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
	messages, err := readSessionMessages(filePath, 0, projectPreviewMessage)
	if err != nil {
		return nil, "", err
	}
	return messages, FormatPreviewMessages(messages, 0), nil
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
