package codexhistory

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSessionPreviewSQLiteRuntimeVersion(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	var version string
	if err := db.QueryRow(`SELECT sqlite_version()`).Scan(&version); err != nil {
		t.Fatalf("read sqlite version: %v", err)
	}
	if version != "3.53.2" {
		t.Fatalf("sqlite version = %q, want 3.53.2", version)
	}
}

func TestSessionPreviewSQLiteExactHitDoesNotWriteAndAppendIsBatched(t *testing.T) {
	t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
	setTestUserCacheDir(t)

	now := time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC)
	previousNow := sessionPreviewNow
	sessionPreviewNow = func() time.Time { return now }
	t.Cleanup(func() { sessionPreviewNow = previousNow })

	var commits int
	previousHook := sessionPreviewSQLiteCommitHook
	sessionPreviewSQLiteCommitHook = func(string) { commits++ }
	t.Cleanup(func() { sessionPreviewSQLiteCommitHook = previousHook })

	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"first answer"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatal(err)
	}

	first, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil {
		t.Fatalf("initial read: %v", err)
	}
	if !strings.Contains(first, "first answer") {
		t.Fatalf("initial preview = %q", first)
	}
	if commits != 1 {
		t.Fatalf("initial commits = %d, want 1", commits)
	}

	second, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil {
		t.Fatalf("exact hit: %v", err)
	}
	if second != first {
		t.Fatalf("exact-hit preview changed: got %q want %q", second, first)
	}
	if commits != 1 {
		t.Fatalf("exact hit added a commit: got %d want 1", commits)
	}

	appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:01Z","type":"event_msg","payload":{"id":"status-1","type":"agent_message","phase":"commentary","message":"new status"}}`)
	third, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil {
		t.Fatalf("batched append read: %v", err)
	}
	if !strings.Contains(third, "first answer") || !strings.Contains(third, "new status") {
		t.Fatalf("batched preview = %q", third)
	}
	if commits != 1 {
		t.Fatalf("small append committed immediately: got %d want 1", commits)
	}

	now = now.Add(sessionPreviewAppendFlushDelay + time.Second)
	fourth, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil {
		t.Fatalf("delayed flush read: %v", err)
	}
	if fourth != third {
		t.Fatalf("flushed preview changed: got %q want %q", fourth, third)
	}
	if commits != 2 {
		t.Fatalf("delayed append commits = %d, want 2", commits)
	}

	resetSessionPreviewSQLiteForTest()
	fifth, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil {
		t.Fatalf("cold sqlite reload: %v", err)
	}
	if fifth != fourth {
		t.Fatalf("cold sqlite preview = %q, want %q", fifth, fourth)
	}
	if commits != 2 {
		t.Fatalf("cold exact hit added a commit: got %d want 2", commits)
	}

	sqlitePath, err := sessionPreviewSQLiteFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sqlitePath); err != nil {
		t.Fatalf("sqlite cache missing: %v", err)
	}
	jsonPath, err := sessionPreviewCacheFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(jsonPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy JSON cache unexpectedly written: %v", err)
	}
}

func TestSessionPreviewSQLitePersistsHiddenTailOffset(t *testing.T) {
	t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
	setTestUserCacheDir(t)

	previousDelay := sessionPreviewAppendFlushDelay
	sessionPreviewAppendFlushDelay = 0
	t.Cleanup(func() { sessionPreviewAppendFlushDelay = previousDelay })

	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}

	appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"function_call_output","output":"hidden"}}`)
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}

	store, err := currentSessionPreviewSQLiteStore()
	if err != nil {
		t.Fatal(err)
	}
	entry, ok, err := store.load(path, true, true)
	if err != nil || !ok {
		t.Fatalf("load cache entry: ok=%v err=%v", ok, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if entry.offset != info.Size() {
		t.Fatalf("parsed offset = %d, want source size %d", entry.offset, info.Size())
	}
	if len(entry.messages) != 1 || entry.messages[0].Content != "answer" {
		t.Fatalf("messages = %#v, want only visible answer", entry.messages)
	}
}

func TestSessionPreviewSQLiteCompareAndAppendRejectsStaleWriter(t *testing.T) {
	t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
	setTestUserCacheDir(t)

	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"first"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}

	storeOne, err := currentSessionPreviewSQLiteStore()
	if err != nil {
		t.Fatal(err)
	}
	dbTwo, err := openSessionPreviewSQLite(storeOne.path)
	if err != nil {
		t.Fatal(err)
	}
	defer dbTwo.Close()
	storeTwo := &sessionPreviewSQLiteStore{path: storeOne.path, db: dbTwo}

	entryOne, ok, err := storeOne.load(path, true, true)
	if err != nil || !ok {
		t.Fatalf("load writer one: ok=%v err=%v", ok, err)
	}
	entryTwo, ok, err := storeTwo.load(path, true, true)
	if err != nil || !ok {
		t.Fatalf("load writer two: ok=%v err=%v", ok, err)
	}

	appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"answer-2","type":"message","role":"assistant","content":[{"type":"output_text","text":"second"}]}}`)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	completeOffset, ok := sessionPreviewCompleteOffset(path, info)
	if !ok {
		t.Fatal("complete offset unavailable")
	}
	seenBefore := cloneMessageSeenState(entryOne.seen)
	tail, err := readSessionMessagesWindow(path, entryOne.offset, completeOffset-entryOne.offset, 0, isPreviewMessage, entryOne.seen)
	if err != nil {
		t.Fatal(err)
	}
	delta := diffMessageSeenState(seenBefore, entryOne.seen)
	hash, hashSize, _ := sessionPreviewPrefixTailHash(path, completeOffset)
	key := newFileCacheKey(path, info)
	if err := storeOne.append(entryOne, key, completeOffset, hash, hashSize, tail, delta); err != nil {
		t.Fatalf("writer one append: %v", err)
	}
	if err := storeTwo.append(entryTwo, key, completeOffset, hash, hashSize, tail, delta); !errors.Is(err, errSessionPreviewSQLiteConflict) {
		t.Fatalf("writer two error = %v, want conflict", err)
	}

	stored, ok, err := storeOne.load(path, true, true)
	if err != nil || !ok {
		t.Fatalf("load final entry: ok=%v err=%v", ok, err)
	}
	if len(stored.messages) != 2 || stored.messages[1].Content != "second" {
		t.Fatalf("stored messages = %#v", stored.messages)
	}
}

func TestSessionPreviewSQLiteBusyWriterFallsBackWithoutLosingPreview(t *testing.T) {
	t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
	setTestUserCacheDir(t)
	previousDelay := sessionPreviewAppendFlushDelay
	sessionPreviewAppendFlushDelay = 0
	t.Cleanup(func() { sessionPreviewAppendFlushDelay = previousDelay })

	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"first"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	store, err := currentSessionPreviewSQLiteStore()
	if err != nil {
		t.Fatal(err)
	}
	lockDB, err := openSessionPreviewSQLite(store.path)
	if err != nil {
		t.Fatal(err)
	}
	defer lockDB.Close()
	lockTx, err := lockDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.Exec(`UPDATE preview_session SET last_write_ns = last_write_ns WHERE path = ?`, filepath.Clean(path)); err != nil {
		_ = lockTx.Rollback()
		t.Fatal(err)
	}

	appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"answer-2","type":"message","role":"assistant","content":[{"type":"output_text","text":"second"}]}}`)
	started := time.Now()
	text, err := ReadSessionPreviewText(path, 0, 0)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("preview under write lock: %v", err)
	}
	if !strings.Contains(text, "first") || !strings.Contains(text, "second") {
		t.Fatalf("preview under write lock = %q", text)
	}
	if elapsed > time.Second {
		t.Fatalf("preview waited %s for disposable cache writer", elapsed)
	}
	if err := lockTx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatalf("flush after releasing writer: %v", err)
	}
	resetSessionPreviewSQLiteForTest()
	text, err = ReadSessionPreviewText(path, 0, 0)
	if err != nil || !strings.Contains(text, "second") {
		t.Fatalf("cold reload after flush: text=%q err=%v", text, err)
	}
}

func TestSessionPreviewSQLiteStoresOnlySkippedSourceIDs(t *testing.T) {
	t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
	setTestUserCacheDir(t)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	body := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"working"}}`,
		`{"timestamp":"2026-01-01T00:00:00.003Z","type":"response_item","payload":{"id":"status-cold-1","type":"message","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"working"}]}}`,
		`{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	store, err := currentSessionPreviewSQLiteStore()
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.db.QueryRow(`SELECT count(*) FROM preview_seen_source`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("persisted source IDs = %d, want only skipped mirror", count)
	}

	resetSessionPreviewSQLiteForTest()
	appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"id":"status-cold-1","type":"message","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"working"}]}}`)
	text, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(text, "working") != 1 {
		t.Fatalf("replayed skipped source was not deduplicated: %q", text)
	}
}

func TestSessionPreviewSQLiteCorruptionFallsBackToFreshCache(t *testing.T) {
	t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
	setTestUserCacheDir(t)

	sqlitePath, err := sessionPreviewSQLiteFile()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sqlitePath, []byte("not a sqlite database"), 0o600); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "session.jsonl")
	line := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"survives corruption"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	text, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil {
		t.Fatalf("preview after corruption: %v", err)
	}
	if !strings.Contains(text, "survives corruption") {
		t.Fatalf("preview = %q", text)
	}

	invalid, err := filepath.Glob(sqlitePath + ".invalid-*")
	if err != nil {
		t.Fatal(err)
	}
	if len(invalid) != 1 {
		t.Fatalf("quarantined databases = %#v, want one", invalid)
	}
	store, err := currentSessionPreviewSQLiteStore()
	if err != nil {
		t.Fatalf("open rebuilt store: %v", err)
	}
	var integrity string
	if err := store.db.QueryRow(`PRAGMA integrity_check`).Scan(&integrity); err != nil {
		t.Fatal(err)
	}
	if integrity != "ok" {
		t.Fatalf("integrity_check = %q", integrity)
	}
}

func TestSessionPreviewSQLitePragmas(t *testing.T) {
	t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
	setTestUserCacheDir(t)
	store, err := currentSessionPreviewSQLiteStore()
	if err != nil {
		t.Fatal(err)
	}

	var journal string
	if err := store.db.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(journal, "wal") {
		t.Fatalf("journal_mode = %q, want wal", journal)
	}
	var synchronous, autocheckpoint, busyTimeout int
	if err := store.db.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if synchronous != 1 {
		t.Fatalf("synchronous = %d, want NORMAL/1", synchronous)
	}
	if err := store.db.QueryRow(`PRAGMA wal_autocheckpoint`).Scan(&autocheckpoint); err != nil {
		t.Fatal(err)
	}
	if autocheckpoint != 1000 {
		t.Fatalf("wal_autocheckpoint = %d, want 1000", autocheckpoint)
	}
	if err := store.db.QueryRow(`PRAGMA busy_timeout`).Scan(&busyTimeout); err != nil {
		t.Fatal(err)
	}
	if busyTimeout != sessionPreviewSQLiteBusyMillis {
		t.Fatalf("busy_timeout = %d, want %d", busyTimeout, sessionPreviewSQLiteBusyMillis)
	}
}

func TestSessionPreviewSQLiteRepeatedExactHitsDoNotGrowCacheFiles(t *testing.T) {
	t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
	setTestUserCacheDir(t)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	line := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"stable"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}

	store, err := currentSessionPreviewSQLiteStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`PRAGMA wal_checkpoint(PASSIVE)`); err != nil {
		t.Fatal(err)
	}
	before := sessionPreviewCacheFileStates(t, store.path)
	for index := 0; index < 100; index++ {
		text, err := ReadSessionPreviewText(path, 0, 0)
		if err != nil || !strings.Contains(text, "stable") {
			t.Fatalf("exact hit %d: text=%q err=%v", index, text, err)
		}
	}
	after := sessionPreviewCacheFileStates(t, store.path)
	for name, initial := range before {
		current, ok := after[name]
		if !ok {
			t.Fatalf("cache sidecar %q disappeared", name)
		}
		if initial.size != current.size || initial.modTime != current.modTime {
			t.Fatalf("cache file %q changed across exact hits: before=%+v after=%+v", name, initial, current)
		}
	}
}

func TestSessionPreviewSQLiteAppendWritesOnlyBoundedWALDelta(t *testing.T) {
	t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
	setTestUserCacheDir(t)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	line := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"stable"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	store, err := currentSessionPreviewSQLiteStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}

	payload := strings.Repeat("x", 64*1024)
	appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"answer-2","type":"message","role":"assistant","content":[{"type":"output_text","text":"`+payload+`"}]}}`)
	text, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil || !strings.Contains(text, payload[:128]) {
		t.Fatalf("append preview: len=%d err=%v", len(text), err)
	}
	walInfo, err := os.Stat(store.path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if walInfo.Size() <= 0 || walInfo.Size() > 1024*1024 {
		t.Fatalf("64 KiB append WAL size = %d, want a bounded delta under 1 MiB", walInfo.Size())
	}
}

func TestSessionPreviewCacheOffDoesNotCreatePersistentCache(t *testing.T) {
	t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendOff)
	setTestUserCacheDir(t)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	line := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"uncached"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	text, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil || !strings.Contains(text, "uncached") {
		t.Fatalf("text=%q err=%v", text, err)
	}
	sqlitePath, err := sessionPreviewSQLiteFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sqlitePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disabled backend created sqlite cache: %v", err)
	}
}

func BenchmarkSessionPreviewSQLiteWarmLoadLargeVisible(b *testing.B) {
	b.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
	cacheDir := b.TempDir()
	b.Setenv("XDG_CACHE_HOME", cacheDir)
	b.Setenv("HOME", cacheDir)
	b.Setenv("LOCALAPPDATA", cacheDir)
	resetPersistentCacheStatesForTest()
	b.Cleanup(resetPersistentCacheStatesForTest)

	path := filepath.Join(b.TempDir(), "large-visible-session.jsonl")
	var body strings.Builder
	content := strings.Repeat("preview payload ", 16)
	for index := 0; index < 8800; index++ {
		fmt.Fprintf(&body, `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-%d","type":"message","role":"assistant","content":[{"type":"output_text","text":"%s %d"}]}}`+"\n", index, content, index)
	}
	if err := os.WriteFile(path, []byte(body.String()), 0o600); err != nil {
		b.Fatal(err)
	}
	if text, err := ReadSessionPreviewText(path, 0, 0); err != nil || text == "" {
		b.Fatalf("warm cache: text=%d err=%v", len(text), err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		text, err := ReadSessionPreviewText(path, 0, 0)
		if err != nil || text == "" {
			b.Fatalf("read cache: text=%d err=%v", len(text), err)
		}
	}
}

func BenchmarkSessionPreviewSQLiteColdOpenLargeVisible(b *testing.B) {
	b.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
	cacheDir := b.TempDir()
	b.Setenv("XDG_CACHE_HOME", cacheDir)
	b.Setenv("HOME", cacheDir)
	b.Setenv("LOCALAPPDATA", cacheDir)
	resetPersistentCacheStatesForTest()
	b.Cleanup(resetPersistentCacheStatesForTest)

	path := filepath.Join(b.TempDir(), "large-visible-session.jsonl")
	var body strings.Builder
	content := strings.Repeat("preview payload ", 16)
	for index := 0; index < 8800; index++ {
		fmt.Fprintf(&body, `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-%d","type":"message","role":"assistant","content":[{"type":"output_text","text":"%s %d"}]}}`+"\n", index, content, index)
	}
	if err := os.WriteFile(path, []byte(body.String()), 0o600); err != nil {
		b.Fatal(err)
	}
	if text, err := ReadSessionPreviewText(path, 0, 0); err != nil || text == "" {
		b.Fatalf("warm cache: text=%d err=%v", len(text), err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		resetSessionPreviewSQLiteForTest()
		text, err := ReadSessionPreviewText(path, 0, 0)
		if err != nil || text == "" {
			b.Fatalf("cold-open cache: text=%d err=%v", len(text), err)
		}
	}
}

func appendPreviewLine(t *testing.T, path string, line string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(line + "\n"); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

type sessionPreviewCacheFileState struct {
	size    int64
	modTime int64
}

func sessionPreviewCacheFileStates(t *testing.T, path string) map[string]sessionPreviewCacheFileState {
	t.Helper()
	states := make(map[string]sessionPreviewCacheFileState)
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		info, err := os.Stat(candidate)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		states[filepath.Base(candidate)] = sessionPreviewCacheFileState{size: info.Size(), modTime: info.ModTime().UnixNano()}
	}
	return states
}
