package codexhistory

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/flock"
)

func setTestUserCacheDir(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", dir)
	t.Setenv("HOME", dir)
	t.Setenv("LOCALAPPDATA", dir)
	resetPersistentCacheStatesForTest()
	return dir
}

func resetPersistentCacheStatesForTest() {
	persistentSessionMetaState.mu.Lock()
	persistentSessionMetaState.path = ""
	persistentSessionMetaState.cacheFilePresent = false
	persistentSessionMetaState.cacheFileMtime = 0
	persistentSessionMetaState.loaded = false
	persistentSessionMetaState.cache = persistentSessionMetaCache{}
	persistentSessionMetaState.mu.Unlock()

	persistentHistoryIndexState.mu.Lock()
	persistentHistoryIndexState.path = ""
	persistentHistoryIndexState.cacheFilePresent = false
	persistentHistoryIndexState.cacheFileMtime = 0
	persistentHistoryIndexState.loaded = false
	persistentHistoryIndexState.cache = persistentHistoryIndexCache{}
	persistentHistoryIndexState.mu.Unlock()
}

func writeSessionMetaFile(t *testing.T, path, sessionID, cwd, prompt string) {
	t.Helper()

	content := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(cwd) + `","source":"cli"}}` + "\n"
	if prompt != "" {
		content += `{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"` + prompt + `"}]}}` + "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write session file: %v", err)
	}
}

func replaceFilePreservingMetadata(t *testing.T, path string, content string) {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("metadata-preserving rename over existing file is not reliable on Windows")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat original file: %v", err)
	}

	tmpPath := filepath.Join(filepath.Dir(path), ".replacement-"+filepath.Base(path))
	if err := os.WriteFile(tmpPath, []byte(content), info.Mode()); err != nil {
		t.Fatalf("write replacement file: %v", err)
	}
	if err := os.Chtimes(tmpPath, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("chtimes replacement file: %v", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		t.Fatalf("rename replacement file: %v", err)
	}
}

func TestSessionFilePersistentCache_PersistsAcrossReset(t *testing.T) {
	setTestUserCacheDir(t)
	resetSessionFileCache()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "persist.jsonl")
	writeSessionMetaFile(t, filePath, "persist-1", dir, "persisted prompt")

	meta1, err := readSessionFileMetaCached(filePath)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}

	cachePath, err := sessionMetaCacheFile()
	if err != nil {
		t.Fatalf("sessionMetaCacheFile: %v", err)
	}
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("expected persistent cache file, stat error: %v", err)
	}

	resetSessionFileCache()

	meta2, err := readSessionFileMetaCached(filePath)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}

	if meta1 != meta2 {
		t.Fatalf("expected persistent cache round-trip to preserve metadata, got %#v vs %#v", meta1, meta2)
	}
}

func TestReadSessionFileMetaCachedContext_CanceledWhileWaitingOnPersistentCacheLock(t *testing.T) {
	setTestUserCacheDir(t)
	resetSessionFileCache()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "locked.jsonl")
	writeSessionMetaFile(t, filePath, "locked-1", dir, "prompt")

	cachePath, err := sessionMetaCacheFile()
	if err != nil {
		t.Fatalf("sessionMetaCacheFile: %v", err)
	}
	lock := flock.New(cachePath + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock cache file: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := readSessionFileMetaCachedContext(ctx, filePath)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("readSessionFileMetaCachedContext error = %v, want context cancellation", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for cache read to honor context cancellation")
	}
}

func TestFlushPersistentSessionMetaBatchContext_CanceledWhileWaitingOnPersistentCacheLock(t *testing.T) {
	setTestUserCacheDir(t)
	resetSessionFileCache()

	cachePath, err := sessionMetaCacheFile()
	if err != nil {
		t.Fatalf("sessionMetaCacheFile: %v", err)
	}
	lock := flock.New(cachePath + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock cache file: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	batch := &sessionMetaPersistentBatch{
		updates: map[string]persistentSessionMetaEntry{
			filepath.Clean("/tmp/session.jsonl"): {
				FileCacheKey: fileCacheKey{Size: 1, MtimeUnixNano: 2, Mode: 0o644},
				Meta:         sessionFileMeta{FirstPrompt: "prompt"},
			},
		},
		deletes: map[string]struct{}{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = flushPersistentSessionMetaBatchContext(ctx, batch)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("flushPersistentSessionMetaBatchContext error = %v, want context cancellation", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("flushPersistentSessionMetaBatchContext took too long: %v", time.Since(start))
	}
}

func TestSessionFilePersistentCache_InvalidatesOnFileChange(t *testing.T) {
	setTestUserCacheDir(t)
	resetSessionFileCache()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "invalidate.jsonl")
	writeSessionMetaFile(t, filePath, "invalidate-1", dir, "old prompt")

	meta1, err := readSessionFileMetaCached(filePath)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if meta1.FirstPrompt != "old prompt" {
		t.Fatalf("FirstPrompt = %q, want old prompt", meta1.FirstPrompt)
	}

	writeSessionMetaFile(t, filePath, "invalidate-1", dir, "new prompt")
	future := fixedTime().Add(2 * time.Hour)
	if err := os.Chtimes(filePath, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	resetSessionFileCache()

	meta2, err := readSessionFileMetaCached(filePath)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if meta2.FirstPrompt != "new prompt" {
		t.Fatalf("FirstPrompt = %q, want new prompt", meta2.FirstPrompt)
	}
}

func TestSessionFilePersistentCache_InvalidatesOnPreservedMetadataRestore(t *testing.T) {
	setTestUserCacheDir(t)
	resetSessionFileCache()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "restore.jsonl")
	writeSessionMetaFile(t, filePath, "restore-1", dir, "alpha")

	meta1, err := readSessionFileMetaCached(filePath)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if meta1.FirstPrompt != "alpha" {
		t.Fatalf("FirstPrompt = %q, want alpha", meta1.FirstPrompt)
	}

	replacement := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"restore-1","cwd":"` + jsonEscapePath(dir) + `","source":"cli"}}` + "\n" +
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"bravo"}]}}` + "\n"
	replaceFilePreservingMetadata(t, filePath, replacement)

	resetSessionFileCache()
	resetPersistentCacheStatesForTest()

	meta2, err := readSessionFileMetaCached(filePath)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if meta2.FirstPrompt != "bravo" {
		t.Fatalf("FirstPrompt = %q, want bravo", meta2.FirstPrompt)
	}
}

func TestDiscoverProjects_BatchesSessionMetaPersistentCacheWrites(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	resetSessionFileCache()

	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionIDs := []string{
		"11111111-1111-1111-1111-111111111111",
		"22222222-2222-2222-2222-222222222222",
		"33333333-3333-3333-3333-333333333333",
	}
	for i, sessionID := range sessionIDs {
		fileName := "rollout-2026-01-01T00-00-0" + string(rune('1'+i)) + "-" + sessionID + ".jsonl"
		writeSessionMetaFile(t, filepath.Join(sessionsDir, fileName), sessionID, projDir, "prompt "+string(rune('A'+i)))
	}

	cachePath, err := sessionMetaCacheFile()
	if err != nil {
		t.Fatalf("sessionMetaCacheFile: %v", err)
	}

	var sessionCacheWrites int
	prevHook := persistentCacheWriteHook
	persistentCacheWriteHook = func(path string) {
		if filepath.Clean(path) == filepath.Clean(cachePath) {
			sessionCacheWrites++
		}
	}
	t.Cleanup(func() { persistentCacheWriteHook = prevHook })

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	if got := len(collectAllSessions(projects)); got != len(sessionIDs) {
		t.Fatalf("session count = %d, want %d", got, len(sessionIDs))
	}
	if sessionCacheWrites != 1 {
		t.Fatalf("session cache writes = %d, want 1", sessionCacheWrites)
	}

	data, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	var cache persistentSessionMetaCache
	if err := json.Unmarshal(data, &cache); err != nil {
		t.Fatalf("unmarshal cache: %v", err)
	}
	if got := len(cache.Entries); got != len(sessionIDs) {
		t.Fatalf("cache entry count = %d, want %d", got, len(sessionIDs))
	}
	for _, sessionID := range sessionIDs {
		found := false
		for path := range cache.Entries {
			if strings.Contains(path, sessionID) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing cache entry for session %s", sessionID)
		}
	}
}

func TestNewFileCacheKey_UsesPlatformIdentity(t *testing.T) {
	lockCodexHistoryTestHooks(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "identity.jsonl")
	writeSessionMetaFile(t, filePath, "identity-1", dir, "prompt")

	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}

	prevPlatformFileCacheKey := platformFileCacheKey
	platformFileCacheKey = func(path string, info os.FileInfo, key *fileCacheKey) {
		if filepath.Clean(path) != filepath.Clean(filePath) {
			return
		}
		key.HasFileID = true
		key.Dev = 17
		key.Ino = 29
		key.HasCtime = true
		key.CtimeUnixNano = 41
	}
	t.Cleanup(func() { platformFileCacheKey = prevPlatformFileCacheKey })

	key := newFileCacheKey(filePath, info)
	if !key.HasFileID || key.Dev != 17 || key.Ino != 29 {
		t.Fatalf("unexpected file identity in key: %+v", key)
	}
	if !key.HasCtime || key.CtimeUnixNano != 41 {
		t.Fatalf("unexpected ctime in key: %+v", key)
	}
	if !matchesFileInfo(filePath, info, key) {
		t.Fatalf("expected key to match original file info")
	}

	platformFileCacheKey = func(path string, info os.FileInfo, key *fileCacheKey) {
		if filepath.Clean(path) != filepath.Clean(filePath) {
			return
		}
		key.HasFileID = true
		key.Dev = 17
		key.Ino = 99
		key.HasCtime = true
		key.CtimeUnixNano = 41
	}
	if matchesFileInfo(filePath, info, key) {
		t.Fatalf("expected mismatched platform identity to invalidate cache key")
	}
}

func TestWriteJSONAtomically_UsesReplaceHelperForExistingFile(t *testing.T) {
	lockCodexHistoryTestHooks(t)

	path := filepath.Join(t.TempDir(), "cache.json")

	prevReplacePersistentCacheFile := replacePersistentCacheFile
	replaceCalls := 0
	replacePersistentCacheFile = func(src, dst string) error {
		replaceCalls++
		if replaceCalls == 2 {
			if _, err := os.Stat(dst); err != nil {
				t.Fatalf("expected destination to exist on second replace, stat error: %v", err)
			}
		}
		data, err := os.ReadFile(src)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, data, 0o600)
	}
	t.Cleanup(func() { replacePersistentCacheFile = prevReplacePersistentCacheFile })

	if err := writeJSONAtomically(path, map[string]string{"prompt": "alpha"}); err != nil {
		t.Fatalf("first writeJSONAtomically: %v", err)
	}
	if err := writeJSONAtomically(path, map[string]string{"prompt": "bravo"}); err != nil {
		t.Fatalf("second writeJSONAtomically: %v", err)
	}
	if replaceCalls != 2 {
		t.Fatalf("replace calls = %d, want 2", replaceCalls)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	if !strings.Contains(string(data), `"prompt":"bravo"`) {
		t.Fatalf("unexpected cache content: %s", string(data))
	}
}

func TestWritePersistentHistoryIndexContext_CanceledWhileWaitingOnPersistentCacheLock(t *testing.T) {
	setTestUserCacheDir(t)

	tmpDir := t.TempDir()
	historyPath := filepath.Join(tmpDir, "history.jsonl")
	if err := os.WriteFile(historyPath, []byte(`{"session_id":"s1","ts":1770777540,"text":"prompt"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write history file: %v", err)
	}
	info, err := os.Stat(historyPath)
	if err != nil {
		t.Fatalf("stat history file: %v", err)
	}

	cachePath, err := historyIndexCacheFile()
	if err != nil {
		t.Fatalf("historyIndexCacheFile: %v", err)
	}
	lock := flock.New(cachePath + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock history cache file: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = writePersistentHistoryIndexContext(ctx, historyPath, info, historyIndex{
		sessions: map[string]*historySessionInfo{
			"s1": {FirstPrompt: "prompt"},
		},
	})
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("writePersistentHistoryIndexContext error = %v, want context cancellation", err)
	}
}

func TestLoadHistoryIndexContext_CanceledWhileWaitingOnPersistentCacheLock(t *testing.T) {
	setTestUserCacheDir(t)
	resetSessionFileCache()

	tmpDir := t.TempDir()
	historyPath := filepath.Join(tmpDir, "history.jsonl")
	if err := os.WriteFile(historyPath, []byte(`{"session_id":"s1","ts":1770777540,"text":"prompt"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write history file: %v", err)
	}

	cachePath, err := historyIndexCacheFile()
	if err != nil {
		t.Fatalf("historyIndexCacheFile: %v", err)
	}
	lock := flock.New(cachePath + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock history cache file: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := loadHistoryIndexContext(ctx, tmpDir)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("loadHistoryIndexContext error = %v, want context cancellation", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for history index load to honor context cancellation")
	}
}

func TestDeletePersistentHistoryIndexContext_CanceledWhileWaitingOnPersistentCacheLock(t *testing.T) {
	setTestUserCacheDir(t)

	cachePath, err := historyIndexCacheFile()
	if err != nil {
		t.Fatalf("historyIndexCacheFile: %v", err)
	}
	lock := flock.New(cachePath + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock history cache file: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = deletePersistentHistoryIndexContext(ctx, filepath.Join(t.TempDir(), "history.jsonl"))
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("deletePersistentHistoryIndexContext error = %v, want context cancellation", err)
	}
}

func TestSessionFilePersistentCache_IgnoresCorruptCache(t *testing.T) {
	setTestUserCacheDir(t)
	resetSessionFileCache()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "corrupt.jsonl")
	writeSessionMetaFile(t, filePath, "corrupt-1", dir, "recovered prompt")

	if _, err := readSessionFileMetaCached(filePath); err != nil {
		t.Fatalf("warm cache: %v", err)
	}

	cachePath, err := sessionMetaCacheFile()
	if err != nil {
		t.Fatalf("sessionMetaCacheFile: %v", err)
	}
	if err := os.WriteFile(cachePath, []byte("{broken json"), 0o600); err != nil {
		t.Fatalf("corrupt cache file: %v", err)
	}

	resetSessionFileCache()

	meta, err := readSessionFileMetaCached(filePath)
	if err != nil {
		t.Fatalf("read with corrupt cache: %v", err)
	}
	if meta.FirstPrompt != "recovered prompt" {
		t.Fatalf("FirstPrompt = %q, want recovered prompt", meta.FirstPrompt)
	}
}

func TestSessionFilePersistentCache_DoesNotMaskUnreadableFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod 000 does not restrict reads on Windows")
	}

	setTestUserCacheDir(t)
	resetSessionFileCache()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "unreadable.jsonl")
	writeSessionMetaFile(t, filePath, "unreadable-1", dir, "secret prompt")

	if _, err := readSessionFileMetaCached(filePath); err != nil {
		t.Fatalf("warm cache: %v", err)
	}

	if err := os.Chmod(filePath, 0o000); err != nil {
		t.Skip("cannot chmod on this platform")
	}
	t.Cleanup(func() { _ = os.Chmod(filePath, 0o644) })

	resetSessionFileCache()

	if _, err := readSessionFileMetaCached(filePath); err == nil {
		t.Fatal("expected unreadable file to return error even if persistent cache was warmed")
	}
}

func TestSessionFilePersistentCache_ReloadsChangedCacheFileInProcess(t *testing.T) {
	setTestUserCacheDir(t)

	cachePath, err := sessionMetaCacheFile()
	if err != nil {
		t.Fatalf("sessionMetaCacheFile: %v", err)
	}

	writeCache := func(prompt string) {
		cache := persistentSessionMetaCache{
			Version: persistentCacheVersion,
			Entries: map[string]persistentSessionMetaEntry{
				"/tmp/session.jsonl": {
					FileCacheKey: fileCacheKey{
						Size:          1,
						MtimeUnixNano: 2,
						Mode:          0o644,
					},
					Meta: sessionFileMeta{FirstPrompt: prompt},
				},
			},
		}
		data, err := json.Marshal(cache)
		if err != nil {
			t.Fatalf("marshal cache: %v", err)
		}
		if err := os.WriteFile(cachePath, append(data, '\n'), 0o600); err != nil {
			t.Fatalf("write cache: %v", err)
		}
	}

	writeCache("first prompt")

	persistentSessionMetaState.mu.Lock()
	cache := loadSessionMetaPersistentStateLocked(cachePath)
	persistentSessionMetaState.mu.Unlock()
	if got := cache.Entries["/tmp/session.jsonl"].Meta.FirstPrompt; got != "first prompt" {
		t.Fatalf("FirstPrompt = %q, want first prompt", got)
	}

	writeCache("second prompt")
	future := fixedTime().Add(4 * time.Hour)
	if err := os.Chtimes(cachePath, future, future); err != nil {
		t.Fatalf("chtimes cache: %v", err)
	}

	persistentSessionMetaState.mu.Lock()
	cache = loadSessionMetaPersistentStateLocked(cachePath)
	persistentSessionMetaState.mu.Unlock()
	if got := cache.Entries["/tmp/session.jsonl"].Meta.FirstPrompt; got != "second prompt" {
		t.Fatalf("FirstPrompt = %q, want second prompt", got)
	}
}

func TestHistoryIndexPersistentCache_InvalidatesOnHistoryChange(t *testing.T) {
	setTestUserCacheDir(t)
	resetSessionFileCache()

	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	fileName := "rollout-2026-01-01T00-00-00-" + sessionID + ".jsonl"
	content := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}` + "\n"
	if err := os.WriteFile(filepath.Join(sessionsDir, fileName), []byte(content), 0o644); err != nil {
		t.Fatalf("write session file: %v", err)
	}

	historyPath := filepath.Join(tmpDir, "history.jsonl")
	if err := os.WriteFile(historyPath, []byte(`{"session_id":"`+sessionID+`","ts":1770777540,"text":"old history prompt"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write history file: %v", err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects first read: %v", err)
	}
	sess := findSession(collectAllSessions(projects), sessionID)
	if sess == nil || sess.FirstPrompt != "old history prompt" {
		t.Fatalf("FirstPrompt = %q, want old history prompt", sess.FirstPrompt)
	}

	if err := os.WriteFile(historyPath, []byte(`{"session_id":"`+sessionID+`","ts":1770777540,"text":"new history prompt"}`+"\n"), 0o644); err != nil {
		t.Fatalf("rewrite history file: %v", err)
	}
	future := fixedTime().Add(3 * time.Hour)
	if err := os.Chtimes(historyPath, future, future); err != nil {
		t.Fatalf("chtimes history: %v", err)
	}

	resetSessionFileCache()

	projects, err = DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects second read: %v", err)
	}
	sess = findSession(collectAllSessions(projects), sessionID)
	if sess == nil || sess.FirstPrompt != "new history prompt" {
		t.Fatalf("FirstPrompt = %q, want new history prompt", sess.FirstPrompt)
	}
}

func TestHistoryIndexPersistentCache_InvalidatesOnPreservedMetadataRestore(t *testing.T) {
	setTestUserCacheDir(t)
	resetSessionFileCache()

	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb"

	fileName := "rollout-2026-01-01T00-00-00-" + sessionID + ".jsonl"
	content := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}` + "\n"
	if err := os.WriteFile(filepath.Join(sessionsDir, fileName), []byte(content), 0o644); err != nil {
		t.Fatalf("write session file: %v", err)
	}

	historyPath := filepath.Join(tmpDir, "history.jsonl")
	if err := os.WriteFile(historyPath, []byte(`{"session_id":"`+sessionID+`","ts":1770777540,"text":"alpha"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write history file: %v", err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects first read: %v", err)
	}
	sess := findSession(collectAllSessions(projects), sessionID)
	if sess == nil || sess.FirstPrompt != "alpha" {
		t.Fatalf("FirstPrompt = %q, want alpha", sess.FirstPrompt)
	}

	replacement := `{"session_id":"` + sessionID + `","ts":1770777540,"text":"bravo"}` + "\n"
	replaceFilePreservingMetadata(t, historyPath, replacement)

	resetSessionFileCache()
	resetPersistentCacheStatesForTest()

	projects, err = DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects second read: %v", err)
	}
	sess = findSession(collectAllSessions(projects), sessionID)
	if sess == nil || sess.FirstPrompt != "bravo" {
		t.Fatalf("FirstPrompt = %q, want bravo", sess.FirstPrompt)
	}
}

func TestHistoryIndexPersistentCache_IgnoresCorruptCache(t *testing.T) {
	setTestUserCacheDir(t)

	dir := t.TempDir()
	historyPath := filepath.Join(dir, "history.jsonl")
	if err := os.WriteFile(historyPath, []byte(`{"session_id":"s1","ts":1770777540,"text":"valid prompt"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write history file: %v", err)
	}

	idx := loadHistoryIndex(dir)
	info, ok := idx.lookup("s1")
	if !ok || info.FirstPrompt != "valid prompt" {
		t.Fatalf("unexpected initial index: ok=%v info=%+v", ok, info)
	}

	cachePath, err := historyIndexCacheFile()
	if err != nil {
		t.Fatalf("historyIndexCacheFile: %v", err)
	}
	if err := os.WriteFile(cachePath, []byte("{broken json"), 0o600); err != nil {
		t.Fatalf("corrupt history cache: %v", err)
	}

	idx = loadHistoryIndex(dir)
	info, ok = idx.lookup("s1")
	if !ok || info.FirstPrompt != "valid prompt" {
		t.Fatalf("unexpected recovered index: ok=%v info=%+v", ok, info)
	}
}

func TestHistoryIndexPersistentCache_ReloadsChangedCacheFileInProcess(t *testing.T) {
	setTestUserCacheDir(t)

	cachePath, err := historyIndexCacheFile()
	if err != nil {
		t.Fatalf("historyIndexCacheFile: %v", err)
	}

	writeCache := func(prompt string) {
		cache := persistentHistoryIndexCache{
			Version: persistentCacheVersion,
			Entries: map[string]persistentHistoryIndexEntry{
				"/tmp/history.jsonl": {
					FileCacheKey: fileCacheKey{
						Size:          1,
						MtimeUnixNano: 2,
						Mode:          0o644,
					},
					Sessions: map[string]*historySessionInfo{
						"s1": {FirstPrompt: prompt},
					},
				},
			},
		}
		data, err := json.Marshal(cache)
		if err != nil {
			t.Fatalf("marshal cache: %v", err)
		}
		if err := os.WriteFile(cachePath, append(data, '\n'), 0o600); err != nil {
			t.Fatalf("write cache: %v", err)
		}
	}

	writeCache("first prompt")

	persistentHistoryIndexState.mu.Lock()
	cache := loadHistoryIndexPersistentStateLocked(cachePath)
	persistentHistoryIndexState.mu.Unlock()
	if got := cache.Entries["/tmp/history.jsonl"].Sessions["s1"].FirstPrompt; got != "first prompt" {
		t.Fatalf("FirstPrompt = %q, want first prompt", got)
	}

	writeCache("second prompt")
	future := fixedTime().Add(5 * time.Hour)
	if err := os.Chtimes(cachePath, future, future); err != nil {
		t.Fatalf("chtimes cache: %v", err)
	}

	persistentHistoryIndexState.mu.Lock()
	cache = loadHistoryIndexPersistentStateLocked(cachePath)
	persistentHistoryIndexState.mu.Unlock()
	if got := cache.Entries["/tmp/history.jsonl"].Sessions["s1"].FirstPrompt; got != "second prompt" {
		t.Fatalf("FirstPrompt = %q, want second prompt", got)
	}
}
