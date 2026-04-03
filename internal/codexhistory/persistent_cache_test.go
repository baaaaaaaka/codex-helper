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

	persistentSharedSessionMetaState.mu.Lock()
	persistentSharedSessionMetaState.path = ""
	persistentSharedSessionMetaState.ownerID = ""
	persistentSharedSessionMetaState.cacheFilePresent = false
	persistentSharedSessionMetaState.cacheFileMtime = 0
	persistentSharedSessionMetaState.loaded = false
	persistentSharedSessionMetaState.entries = nil
	persistentSharedSessionMetaState.mu.Unlock()

	persistentSharedHistoryIndexState.mu.Lock()
	persistentSharedHistoryIndexState.path = ""
	persistentSharedHistoryIndexState.ownerID = ""
	persistentSharedHistoryIndexState.cacheFilePresent = false
	persistentSharedHistoryIndexState.cacheFileMtime = 0
	persistentSharedHistoryIndexState.loaded = false
	persistentSharedHistoryIndexState.entries = nil
	persistentSharedHistoryIndexState.mu.Unlock()

	persistentCacheLocalWriterState.mu.Lock()
	persistentCacheLocalWriterState.path = ""
	persistentCacheLocalWriterState.value = ""
	persistentCacheLocalWriterState.mu.Unlock()
}

func setPersistentCacheWriterIDForTest(t *testing.T, id string) {
	t.Helper()

	prevWriterID := persistentCacheWriterID
	persistentCacheWriterID = func(context.Context) (string, error) { return id, nil }
	t.Cleanup(func() { persistentCacheWriterID = prevWriterID })
}

func setPersistentCacheWriterScopeIDForTest(t *testing.T, id string) {
	t.Helper()

	prevScopeID := persistentCacheWriterScopeID
	persistentCacheWriterScopeID = func() (string, error) { return id, nil }
	t.Cleanup(func() { persistentCacheWriterScopeID = prevScopeID })
}

func setSharedPersistentCacheOwnerIDForTest(t *testing.T, fn func(path string, info os.FileInfo) (string, bool)) {
	t.Helper()

	prev := sharedPersistentCacheOwnerID
	sharedPersistentCacheOwnerID = fn
	t.Cleanup(func() { sharedPersistentCacheOwnerID = prev })
}

func setReadSharedSessionMetaCacheFileForTest(t *testing.T, fn func(path string) ([]byte, error)) {
	t.Helper()

	prev := readSharedSessionMetaCacheFile
	readSharedSessionMetaCacheFile = fn
	t.Cleanup(func() { readSharedSessionMetaCacheFile = prev })
}

func setReadSharedHistoryIndexCacheFileForTest(t *testing.T, fn func(path string) ([]byte, error)) {
	t.Helper()

	prev := readSharedHistoryIndexCacheFile
	readSharedHistoryIndexCacheFile = fn
	t.Cleanup(func() { readSharedHistoryIndexCacheFile = prev })
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

func TestDiscoverProjects_UsesSharedSessionMetaCacheAcrossWriters(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "root-writer")

	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "44444444-4444-4444-4444-444444444444"
	fileName := "rollout-2026-01-01T00-00-04-" + sessionID + ".jsonl"
	filePath := filepath.Join(sessionsDir, fileName)
	writeSessionMetaFile(t, filePath, sessionID, projDir, "shared prompt")

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects warm cache: %v", err)
	}
	if sess := findSession(collectAllSessions(projects), sessionID); sess == nil || sess.FirstPrompt != "shared prompt" {
		t.Fatalf("warm cache session = %#v, want shared prompt", sess)
	}

	prevOpenSessionMetaFile := openSessionMetaFile
	openSessionMetaFile = func(string) (*os.File, error) {
		return nil, errors.New("session parse should not be needed when shared cache is warm")
	}
	t.Cleanup(func() { openSessionMetaFile = prevOpenSessionMetaFile })

	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "user-writer")

	projects, err = DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects shared cache reuse: %v", err)
	}
	sess := findSession(collectAllSessions(projects), sessionID)
	if sess == nil {
		t.Fatal("expected session from shared session-meta cache")
	}
	if sess.FirstPrompt != "shared prompt" {
		t.Fatalf("FirstPrompt = %q, want %q", sess.FirstPrompt, "shared prompt")
	}

	sharedShard := filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "session-meta", "root-writer.json")
	if _, err := os.Stat(sharedShard); err != nil {
		t.Fatalf("expected shared shard %q: %v", sharedShard, err)
	}
}

func TestDiscoverProjects_UsesSharedHistoryIndexCacheAcrossWriters(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "root-writer")

	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "55555555-5555-5555-5555-555555555555"
	fileName := "rollout-2026-01-01T00-00-05-" + sessionID + ".jsonl"
	filePath := filepath.Join(sessionsDir, fileName)
	content := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}` + "\n"
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("write session file: %v", err)
	}

	historyPath := filepath.Join(tmpDir, "history.jsonl")
	if err := os.WriteFile(historyPath, []byte(`{"session_id":"`+sessionID+`","ts":1770777540,"text":"history prompt"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write history file: %v", err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects warm history cache: %v", err)
	}
	if sess := findSession(collectAllSessions(projects), sessionID); sess == nil || sess.FirstPrompt != "history prompt" {
		t.Fatalf("warm history session = %#v, want history prompt", sess)
	}

	prevOpenHistoryIndexFile := openHistoryIndexFile
	openHistoryIndexFile = func(string) (*os.File, error) {
		return nil, errors.New("history index parse should not be needed when shared cache is warm")
	}
	t.Cleanup(func() { openHistoryIndexFile = prevOpenHistoryIndexFile })

	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "user-writer")

	projects, err = DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects shared history reuse: %v", err)
	}
	sess := findSession(collectAllSessions(projects), sessionID)
	if sess == nil {
		t.Fatal("expected session from shared history-index cache")
	}
	if sess.FirstPrompt != "history prompt" {
		t.Fatalf("FirstPrompt = %q, want %q", sess.FirstPrompt, "history prompt")
	}

	sharedShard := filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "history-index", "root-writer.json")
	if _, err := os.Stat(sharedShard); err != nil {
		t.Fatalf("expected shared shard %q: %v", sharedShard, err)
	}
}

func TestDiscoverProjects_IgnoresForgedSharedSessionMetaShardFromDifferentOwner(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "root-writer")

	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "77777777-7777-7777-7777-777777777777"
	fileName := "rollout-2026-01-01T00-00-07-" + sessionID + ".jsonl"
	filePath := filepath.Join(sessionsDir, fileName)
	writeSessionMetaFile(t, filePath, sessionID, projDir, "shared prompt")

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects warm shared cache: %v", err)
	}
	if sess := findSession(collectAllSessions(projects), sessionID); sess == nil || sess.FirstPrompt != "shared prompt" {
		t.Fatalf("warm session = %#v, want shared prompt", sess)
	}

	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat session file: %v", err)
	}
	forgedShard := filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "session-meta", "attacker-writer.json")
	forgedCache := persistentSessionMetaCache{
		Version: persistentCacheVersion,
		Entries: map[string]persistentSessionMetaEntry{
			filepath.Clean(filePath): {
				FileCacheKey: newFileCacheKey(filePath, info),
				Meta: sessionFileMeta{
					SessionID:   sessionID,
					ProjectPath: "/tmp/forged-project",
					FirstPrompt: "forged prompt",
				},
			},
		},
	}
	if err := writeJSONAtomicallyWithOptions(forgedShard, forgedCache, persistentCacheWriteOptions{
		dirMode:  sharedPersistentCacheDirMode,
		fileMode: sharedPersistentCacheFileMode,
	}); err != nil {
		t.Fatalf("write forged shard: %v", err)
	}

	setSharedPersistentCacheOwnerIDForTest(t, func(path string, info os.FileInfo) (string, bool) {
		switch filepath.Clean(path) {
		case filepath.Clean(filePath):
			return "uid:1000", true
		case filepath.Clean(filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "session-meta", "root-writer.json")):
			return "uid:1000", true
		case filepath.Clean(forgedShard):
			return "uid:2000", true
		default:
			return persistentCacheOwnerID(path, info)
		}
	})

	prevOpenSessionMetaFile := openSessionMetaFile
	openSessionMetaFile = func(string) (*os.File, error) {
		return nil, errors.New("session parse should not be needed when trusted shared cache is warm")
	}
	t.Cleanup(func() { openSessionMetaFile = prevOpenSessionMetaFile })

	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "user-writer")

	projects, err = DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects with forged shard: %v", err)
	}
	sess := findSession(collectAllSessions(projects), sessionID)
	if sess == nil {
		t.Fatal("expected session from trusted shared session-meta cache")
	}
	if sess.FirstPrompt != "shared prompt" {
		t.Fatalf("FirstPrompt = %q, want %q", sess.FirstPrompt, "shared prompt")
	}
	if sess.ProjectPath != projDir {
		t.Fatalf("ProjectPath = %q, want %q", sess.ProjectPath, projDir)
	}
}

func TestDiscoverProjects_IgnoresForgedSharedHistoryIndexShardFromDifferentOwner(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "root-writer")

	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "88888888-8888-8888-8888-888888888888"
	fileName := "rollout-2026-01-01T00-00-08-" + sessionID + ".jsonl"
	filePath := filepath.Join(sessionsDir, fileName)
	content := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}` + "\n"
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("write session file: %v", err)
	}

	historyPath := filepath.Join(tmpDir, "history.jsonl")
	if err := os.WriteFile(historyPath, []byte(`{"session_id":"`+sessionID+`","ts":1770777540,"text":"history prompt"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write history file: %v", err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects warm history cache: %v", err)
	}
	if sess := findSession(collectAllSessions(projects), sessionID); sess == nil || sess.FirstPrompt != "history prompt" {
		t.Fatalf("warm history session = %#v, want history prompt", sess)
	}

	info, err := os.Stat(historyPath)
	if err != nil {
		t.Fatalf("stat history file: %v", err)
	}
	forgedShard := filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "history-index", "attacker-writer.json")
	forgedCache := persistentHistoryIndexCache{
		Version: persistentCacheVersion,
		Entries: map[string]persistentHistoryIndexEntry{
			filepath.Clean(historyPath): {
				FileCacheKey: newFileCacheKey(historyPath, info),
				Sessions: map[string]*historySessionInfo{
					sessionID: {FirstPrompt: "forged history prompt"},
				},
			},
		},
	}
	if err := writeJSONAtomicallyWithOptions(forgedShard, forgedCache, persistentCacheWriteOptions{
		dirMode:  sharedPersistentCacheDirMode,
		fileMode: sharedPersistentCacheFileMode,
	}); err != nil {
		t.Fatalf("write forged history shard: %v", err)
	}

	setSharedPersistentCacheOwnerIDForTest(t, func(path string, info os.FileInfo) (string, bool) {
		switch filepath.Clean(path) {
		case filepath.Clean(historyPath):
			return "uid:1000", true
		case filepath.Clean(filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "history-index", "root-writer.json")):
			return "uid:1000", true
		case filepath.Clean(forgedShard):
			return "uid:2000", true
		default:
			return persistentCacheOwnerID(path, info)
		}
	})

	prevOpenHistoryIndexFile := openHistoryIndexFile
	openHistoryIndexFile = func(string) (*os.File, error) {
		return nil, errors.New("history index parse should not be needed when trusted shared cache is warm")
	}
	t.Cleanup(func() { openHistoryIndexFile = prevOpenHistoryIndexFile })

	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "user-writer")

	projects, err = DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects with forged history shard: %v", err)
	}
	sess := findSession(collectAllSessions(projects), sessionID)
	if sess == nil {
		t.Fatal("expected session from trusted shared history-index cache")
	}
	if sess.FirstPrompt != "history prompt" {
		t.Fatalf("FirstPrompt = %q, want %q", sess.FirstPrompt, "history prompt")
	}
}

func TestSharedPersistentCache_UsesMultiUserFriendlyPermissions(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	if runtime.GOOS == "windows" {
		t.Skip("sticky-bit permissions are Unix-specific")
	}
	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "root-writer")

	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "66666666-6666-6666-6666-666666666666"
	fileName := "rollout-2026-01-01T00-00-06-" + sessionID + ".jsonl"
	writeSessionMetaFile(t, filepath.Join(sessionsDir, fileName), sessionID, projDir, "permission prompt")
	if err := os.WriteFile(filepath.Join(tmpDir, "history.jsonl"), []byte(`{"session_id":"`+sessionID+`","ts":1770777540,"text":"permission prompt"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write history file: %v", err)
	}

	if _, err := DiscoverProjects(tmpDir); err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}

	for _, dir := range []string{
		filepath.Join(tmpDir, ".codex-proxy"),
		filepath.Join(tmpDir, ".codex-proxy", "codexhistory"),
		filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "session-meta"),
		filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "history-index"),
	} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat shared dir %q: %v", dir, err)
		}
		if info.Mode()&os.ModeSticky == 0 {
			t.Fatalf("expected sticky bit on %q, mode=%#o", dir, info.Mode())
		}
		if info.Mode().Perm() != 0o777 {
			t.Fatalf("expected %#o perms on %q, got %#o", 0o777, dir, info.Mode().Perm())
		}
	}

	for _, file := range []string{
		filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "session-meta", "root-writer.json"),
		filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "history-index", "root-writer.json"),
	} {
		info, err := os.Stat(file)
		if err != nil {
			t.Fatalf("stat shared file %q: %v", file, err)
		}
		if info.Mode().Perm() != sharedPersistentCacheFileMode {
			t.Fatalf("expected %#o perms on %q, got %#o", sharedPersistentCacheFileMode, file, info.Mode().Perm())
		}
	}
}

func TestEnsureSharedPersistentCacheDir_RejectsSymlinkedPath(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation for this test is Unix-specific")
	}

	tmpDir := t.TempDir()
	outsideDir := t.TempDir()
	outsideInfoBefore, err := os.Stat(outsideDir)
	if err != nil {
		t.Fatalf("stat outside dir before: %v", err)
	}

	linkPath := filepath.Join(tmpDir, ".codex-proxy")
	if err := os.Symlink(outsideDir, linkPath); err != nil {
		t.Fatalf("symlink shared cache dir: %v", err)
	}

	err = ensureSharedPersistentCacheDir(filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "session-meta"))
	if err == nil {
		t.Fatal("expected symlinked shared cache path to be rejected")
	}

	outsideInfoAfter, err := os.Stat(outsideDir)
	if err != nil {
		t.Fatalf("stat outside dir after: %v", err)
	}
	if outsideInfoAfter.Mode() != outsideInfoBefore.Mode() {
		t.Fatalf("outside dir mode changed via symlink: before=%#o after=%#o", outsideInfoBefore.Mode(), outsideInfoAfter.Mode())
	}
}

func TestEnsureSharedPersistentCacheDir_AllowsSystemStyleSymlinkedAncestor(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation for this test is Unix-specific")
	}

	realParent := t.TempDir()
	linkRoot := filepath.Join(t.TempDir(), "linked-root")
	if err := os.Symlink(realParent, linkRoot); err != nil {
		t.Fatalf("symlink ancestor path: %v", err)
	}

	dir := filepath.Join(linkRoot, ".codex-proxy", "codexhistory", "session-meta")
	if err := ensureSharedPersistentCacheDir(dir); err != nil {
		t.Fatalf("ensureSharedPersistentCacheDir under symlinked ancestor: %v", err)
	}

	for _, path := range []string{
		filepath.Join(realParent, ".codex-proxy"),
		filepath.Join(realParent, ".codex-proxy", "codexhistory"),
		filepath.Join(realParent, ".codex-proxy", "codexhistory", "session-meta"),
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat shared cache dir %q: %v", path, err)
		}
		if !info.IsDir() {
			t.Fatalf("shared cache path %q is not a directory", path)
		}
	}
}

func TestDefaultPersistentCacheWriterID_PersistsRandomLocalToken(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)

	id1 := defaultPersistentCacheWriterID()
	if len(id1) != 32 {
		t.Fatalf("writer id length = %d, want 32", len(id1))
	}
	for _, r := range id1 {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			t.Fatalf("writer id %q contains non-hex rune %q", id1, string(r))
		}
	}

	resetPersistentCacheStatesForTest()

	id2 := defaultPersistentCacheWriterID()
	if id1 != id2 {
		t.Fatalf("writer id = %q after reset, want %q", id2, id1)
	}

	path, err := persistentCacheWriterIDFile()
	if err != nil {
		t.Fatalf("persistentCacheWriterIDFile: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read writer id file: %v", err)
	}
	if got := normalizePersistentCacheWriterID(data); got != id1 {
		t.Fatalf("persisted writer id = %q, want %q", got, id1)
	}
}

func TestDefaultPersistentCacheWriterID_UsesPerScopeLocalFiles(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)

	setPersistentCacheWriterScopeIDForTest(t, "uid-0")
	rootID := defaultPersistentCacheWriterID()
	rootPath, err := persistentCacheWriterIDFile()
	if err != nil {
		t.Fatalf("persistentCacheWriterIDFile root: %v", err)
	}
	if !strings.Contains(filepath.Base(rootPath), "uid-0") {
		t.Fatalf("root writer id path = %q, want scope suffix", rootPath)
	}

	resetPersistentCacheStatesForTest()
	setPersistentCacheWriterScopeIDForTest(t, "uid-1000")
	userID := defaultPersistentCacheWriterID()
	userPath, err := persistentCacheWriterIDFile()
	if err != nil {
		t.Fatalf("persistentCacheWriterIDFile user: %v", err)
	}
	if !strings.Contains(filepath.Base(userPath), "uid-1000") {
		t.Fatalf("user writer id path = %q, want scope suffix", userPath)
	}
	if rootPath == userPath {
		t.Fatalf("writer id paths should differ across scopes: %q", rootPath)
	}
	if rootID == "" || userID == "" {
		t.Fatalf("writer ids should be non-empty, got root=%q user=%q", rootID, userID)
	}

	rootData, err := os.ReadFile(rootPath)
	if err != nil {
		t.Fatalf("read root writer id file: %v", err)
	}
	userData, err := os.ReadFile(userPath)
	if err != nil {
		t.Fatalf("read user writer id file: %v", err)
	}
	if got := normalizePersistentCacheWriterID(rootData); got != rootID {
		t.Fatalf("persisted root writer id = %q, want %q", got, rootID)
	}
	if got := normalizePersistentCacheWriterID(userData); got != userID {
		t.Fatalf("persisted user writer id = %q, want %q", got, userID)
	}
}

func TestDefaultPersistentCacheWriterIDContext_CanceledWhileWaitingOnLock(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)

	path, err := persistentCacheWriterIDFile()
	if err != nil {
		t.Fatalf("persistentCacheWriterIDFile: %v", err)
	}
	lock := flock.New(path + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock writer id file: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = defaultPersistentCacheWriterIDContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("defaultPersistentCacheWriterIDContext error = %v, want context cancellation", err)
	}
}

func TestDiscoverProjects_SkipsUntrustedSharedSessionMetaShardBeforeRead(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "root-writer")

	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	filePath := filepath.Join(sessionsDir, "rollout-2026-01-01T00-00-10-"+sessionID+".jsonl")
	writeSessionMetaFile(t, filePath, sessionID, projDir, "trusted prompt")

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects warm cache: %v", err)
	}
	if sess := findSession(collectAllSessions(projects), sessionID); sess == nil || sess.FirstPrompt != "trusted prompt" {
		t.Fatalf("warm cache session = %#v, want trusted prompt", sess)
	}

	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat session file: %v", err)
	}
	forgedShard := filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "session-meta", "attacker-writer.json")
	forgedCache := persistentSessionMetaCache{
		Version: persistentCacheVersion,
		Entries: map[string]persistentSessionMetaEntry{
			filepath.Clean(filePath): {
				FileCacheKey: newFileCacheKey(filePath, info),
				Meta: sessionFileMeta{
					SessionID:   sessionID,
					ProjectPath: "/tmp/forged-project",
					FirstPrompt: "forged prompt",
				},
			},
		},
	}
	if err := writeJSONAtomicallyWithOptions(forgedShard, forgedCache, persistentCacheWriteOptions{
		dirMode:  sharedPersistentCacheDirMode,
		fileMode: sharedPersistentCacheFileMode,
	}); err != nil {
		t.Fatalf("write forged shard: %v", err)
	}

	trustedShard := filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "session-meta", "root-writer.json")
	setSharedPersistentCacheOwnerIDForTest(t, func(path string, info os.FileInfo) (string, bool) {
		switch filepath.Clean(path) {
		case filepath.Clean(filePath):
			return "uid:1000", true
		case filepath.Clean(trustedShard):
			return "uid:1000", true
		case filepath.Clean(forgedShard):
			return "uid:2000", true
		default:
			return persistentCacheOwnerID(path, info)
		}
	})

	attackerReads := 0
	setReadSharedSessionMetaCacheFileForTest(t, func(path string) ([]byte, error) {
		if filepath.Clean(path) == filepath.Clean(forgedShard) {
			attackerReads++
		}
		return os.ReadFile(path)
	})

	prevOpenSessionMetaFile := openSessionMetaFile
	openSessionMetaFile = func(string) (*os.File, error) {
		return nil, errors.New("session parse should not be needed when trusted shared cache is warm")
	}
	t.Cleanup(func() { openSessionMetaFile = prevOpenSessionMetaFile })

	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "user-writer")

	projects, err = DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects with forged shard: %v", err)
	}
	if attackerReads != 0 {
		t.Fatalf("untrusted shared session-meta shard was read %d times", attackerReads)
	}
	sess := findSession(collectAllSessions(projects), sessionID)
	if sess == nil || sess.FirstPrompt != "trusted prompt" {
		t.Fatalf("session = %#v, want trusted prompt", sess)
	}
}

func TestDiscoverProjects_SkipsUntrustedSharedHistoryIndexShardBeforeRead(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "root-writer")

	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	filePath := filepath.Join(sessionsDir, "rollout-2026-01-01T00-00-11-"+sessionID+".jsonl")
	content := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}` + "\n"
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("write session file: %v", err)
	}

	historyPath := filepath.Join(tmpDir, "history.jsonl")
	if err := os.WriteFile(historyPath, []byte(`{"session_id":"`+sessionID+`","ts":1770777540,"text":"trusted history prompt"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write history file: %v", err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects warm history cache: %v", err)
	}
	if sess := findSession(collectAllSessions(projects), sessionID); sess == nil || sess.FirstPrompt != "trusted history prompt" {
		t.Fatalf("warm history session = %#v, want trusted history prompt", sess)
	}

	info, err := os.Stat(historyPath)
	if err != nil {
		t.Fatalf("stat history file: %v", err)
	}
	forgedShard := filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "history-index", "attacker-writer.json")
	forgedCache := persistentHistoryIndexCache{
		Version: persistentCacheVersion,
		Entries: map[string]persistentHistoryIndexEntry{
			filepath.Clean(historyPath): {
				FileCacheKey: newFileCacheKey(historyPath, info),
				Sessions: map[string]*historySessionInfo{
					sessionID: {FirstPrompt: "forged history prompt"},
				},
			},
		},
	}
	if err := writeJSONAtomicallyWithOptions(forgedShard, forgedCache, persistentCacheWriteOptions{
		dirMode:  sharedPersistentCacheDirMode,
		fileMode: sharedPersistentCacheFileMode,
	}); err != nil {
		t.Fatalf("write forged history shard: %v", err)
	}

	trustedShard := filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "history-index", "root-writer.json")
	setSharedPersistentCacheOwnerIDForTest(t, func(path string, info os.FileInfo) (string, bool) {
		switch filepath.Clean(path) {
		case filepath.Clean(historyPath):
			return "uid:1000", true
		case filepath.Clean(trustedShard):
			return "uid:1000", true
		case filepath.Clean(forgedShard):
			return "uid:2000", true
		default:
			return persistentCacheOwnerID(path, info)
		}
	})

	attackerReads := 0
	setReadSharedHistoryIndexCacheFileForTest(t, func(path string) ([]byte, error) {
		if filepath.Clean(path) == filepath.Clean(forgedShard) {
			attackerReads++
		}
		return os.ReadFile(path)
	})

	prevOpenHistoryIndexFile := openHistoryIndexFile
	openHistoryIndexFile = func(string) (*os.File, error) {
		return nil, errors.New("history parse should not be needed when trusted shared cache is warm")
	}
	t.Cleanup(func() { openHistoryIndexFile = prevOpenHistoryIndexFile })

	setTestUserCacheDir(t)
	resetSessionFileCache()
	setPersistentCacheWriterIDForTest(t, "user-writer")

	projects, err = DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects with forged history shard: %v", err)
	}
	if attackerReads != 0 {
		t.Fatalf("untrusted shared history-index shard was read %d times", attackerReads)
	}
	sess := findSession(collectAllSessions(projects), sessionID)
	if sess == nil || sess.FirstPrompt != "trusted history prompt" {
		t.Fatalf("session = %#v, want trusted history prompt", sess)
	}
}

func TestWriteSharedPersistentSessionMetaContext_SkipsShardWhenWriterIDUnavailable(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	resetSessionFileCache()

	prevWriterID := persistentCacheWriterID
	persistentCacheWriterID = func(context.Context) (string, error) {
		return "", errors.New("writer id unavailable")
	}
	t.Cleanup(func() { persistentCacheWriterID = prevWriterID })

	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "99999999-9999-9999-9999-999999999999"
	filePath := filepath.Join(sessionsDir, "rollout-2026-01-01T00-00-09-"+sessionID+".jsonl")
	writeSessionMetaFile(t, filePath, sessionID, projDir, "skip shared write")

	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat session file: %v", err)
	}
	if err := writeSharedPersistentSessionMetaContext(context.Background(), filePath, info, sessionFileMeta{
		SessionID:   sessionID,
		ProjectPath: projDir,
		FirstPrompt: "skip shared write",
	}); err != nil {
		t.Fatalf("writeSharedPersistentSessionMetaContext: %v", err)
	}

	shardDir := filepath.Join(tmpDir, ".codex-proxy", "codexhistory", "session-meta")
	entries, err := os.ReadDir(shardDir)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatalf("ReadDir(%q): %v", shardDir, err)
	}
	if len(entries) != 0 {
		t.Fatalf("unexpected shared shards when writer id is unavailable: %d", len(entries))
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
