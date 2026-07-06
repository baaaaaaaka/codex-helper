package codexhistory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlite "modernc.org/sqlite"
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
	store, err := currentSessionPreviewSQLiteStore()
	if err != nil {
		t.Fatal(err)
	}
	resetSessionPreviewSQLiteWriteCounters(t, store)

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
	assertSessionPreviewSQLiteWritePages(t, store, 0)

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
	assertSessionPreviewSQLiteWritePages(t, store, 0)

	now = now.Add(sessionPreviewAppendFlushDelay + time.Second)
	resetSessionPreviewSQLiteWriteCounters(t, store)
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
	if writes := sessionPreviewSQLiteWritePages(t, store, false); writes <= 0 || writes > 5 {
		t.Fatalf("small visible append cache writes = %d pages, want 1..5", writes)
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
	resetSessionPreviewSQLiteWriteCounters(t, storeTwo)
	if err := storeTwo.append(entryTwo, key, completeOffset, hash, hashSize, tail, delta); !errors.Is(err, errSessionPreviewSQLiteConflict) {
		t.Fatalf("writer two error = %v, want conflict", err)
	}
	assertSessionPreviewSQLiteWritePages(t, storeTwo, 0)
	if frames := sessionPreviewSQLiteWALFrames(t, storeTwo); frames != 0 {
		t.Fatalf("stale writer produced %d WAL frames", frames)
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
	resetSessionPreviewSQLiteWriteCounters(t, store)
	lockConn, err := lockDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lockConn.Close()
	if _, err := lockConn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
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
	assertSessionPreviewSQLiteWritePages(t, store, 0)
	if frames := sessionPreviewSQLiteWALFrames(t, store); frames != 0 {
		t.Fatalf("busy fallback produced %d WAL frames", frames)
	}
	if _, err := lockConn.ExecContext(context.Background(), `ROLLBACK`); err != nil {
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
	if autocheckpoint != sessionPreviewSQLiteAutoCheckpointPages {
		t.Fatalf("wal_autocheckpoint = %d, want %d", autocheckpoint, sessionPreviewSQLiteAutoCheckpointPages)
	}
	if err := store.db.QueryRow(`PRAGMA busy_timeout`).Scan(&busyTimeout); err != nil {
		t.Fatal(err)
	}
	if busyTimeout != sessionPreviewSQLiteBusyMillis {
		t.Fatalf("busy_timeout = %d, want %d", busyTimeout, sessionPreviewSQLiteBusyMillis)
	}
	conn, err := store.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Raw(func(driverConn any) error {
		fileControl, ok := driverConn.(sqlite.FileControl)
		if !ok {
			return errors.New("sqlite driver does not support persistent WAL")
		}
		mode, err := fileControl.FileControlPersistWAL("main", -1)
		if err != nil {
			return err
		}
		if mode != 1 {
			return fmt.Errorf("persistent WAL mode = %d, want 1", mode)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSessionPreviewSQLiteMissingAndIncompleteSourcesDoNotCreateDatabase(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
		setTestUserCacheDir(t)
		missing := filepath.Join(t.TempDir(), "missing.jsonl")
		if _, err := ReadSessionPreviewText(missing, 0, 0); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("missing source error = %v", err)
		}
		assertSessionPreviewSQLiteDatabaseAbsent(t)
	})

	t.Run("incomplete tail", func(t *testing.T) {
		t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
		setTestUserCacheDir(t)
		path := filepath.Join(t.TempDir(), "partial.jsonl")
		partial := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message"`
		if err := os.WriteFile(path, []byte(partial), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
			t.Fatalf("partial source read: %v", err)
		}
		assertSessionPreviewSQLiteDatabaseAbsent(t)
	})
}

func TestSessionPreviewSQLitePersistentSidecarsAvoidCloseReopenChurn(t *testing.T) {
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
	resetSessionPreviewSQLiteWriteCounters(t, store)
	beforeClose := sessionPreviewCacheFileStates(t, store.path)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if _, ok := beforeClose[filepath.Base(store.path+suffix)]; !ok {
			t.Fatalf("persistent cache file %q is missing before close", store.path+suffix)
		}
	}

	resetSessionPreviewSQLiteForTest()
	afterClose := sessionPreviewCacheFileStates(t, store.path)
	assertNoSessionPreviewSidecarChurn(t, beforeClose, afterClose)

	reopened, err := currentSessionPreviewSQLiteStore()
	if err != nil {
		t.Fatal(err)
	}
	resetSessionPreviewSQLiteDBStatus(t, reopened)
	text, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil || !strings.Contains(text, "stable") {
		t.Fatalf("reopened exact hit: text=%q err=%v", text, err)
	}
	assertSessionPreviewSQLiteWritePages(t, reopened, 0)
	afterRead := sessionPreviewCacheFileStates(t, reopened.path)
	assertNoSessionPreviewSidecarChurn(t, afterClose, afterRead)
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

func TestSessionPreviewSQLiteLargeAppendDefersCheckpointAndStaysNearPayloadPages(t *testing.T) {
	forceImmediateSessionPreviewFlush(t)
	path, store := newSessionPreviewCachedFile(t, `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"stable"}]}}`+"\n")
	resetSessionPreviewSQLiteWriteCounters(t, store)
	mainBefore, err := os.Stat(store.path)
	if err != nil {
		t.Fatal(err)
	}

	payload := strings.Repeat("x", 5*1024*1024)
	appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"answer-2","type":"message","role":"assistant","content":[{"type":"output_text","text":"`+payload+`"}]}}`)
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	frames := sessionPreviewSQLiteWALFrames(t, store)
	writes := sessionPreviewSQLiteWritePages(t, store, false)
	if frames != int64(writes) {
		t.Fatalf("WAL frames = %d, cache writes = %d", frames, writes)
	}
	var pageSize int64
	if err := store.db.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		t.Fatal(err)
	}
	minimumPayloadPages := (int64(len(payload)) + pageSize - 1) / pageSize
	if frames < minimumPayloadPages || frames > minimumPayloadPages+16 {
		t.Fatalf("5 MiB append wrote %d frames, payload needs %d pages", frames, minimumPayloadPages)
	}
	if frames >= sessionPreviewSQLiteAutoCheckpointPages {
		t.Fatalf("test append unexpectedly reached auto-checkpoint threshold: frames=%d threshold=%d", frames, sessionPreviewSQLiteAutoCheckpointPages)
	}
	mainAfter, err := os.Stat(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if mainAfter.Size() != mainBefore.Size() || mainAfter.ModTime() != mainBefore.ModTime() {
		t.Fatalf("main database changed before checkpoint: before=%+v after=%+v", mainBefore, mainAfter)
	}

	var busy, logged, checkpointed int64
	if err := store.db.QueryRow(`PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &logged, &checkpointed); err != nil {
		t.Fatal(err)
	}
	if busy != 0 || logged != frames || checkpointed != frames {
		t.Fatalf("checkpoint result busy=%d logged=%d checkpointed=%d frames=%d", busy, logged, checkpointed, frames)
	}
}

func TestSessionPreviewSQLiteWriteBudgetByMutation(t *testing.T) {
	t.Run("mtime only", func(t *testing.T) {
		path, store := newSessionPreviewCachedFile(t, `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"stable"}]}}`+"\n")
		metrics := measureSessionPreviewSQLiteMutation(t, store, func() {
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			changed := info.ModTime().Add(2 * time.Second)
			if err := os.Chtimes(path, changed, changed); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
				t.Fatal(err)
			}
		})
		assertSessionPreviewSQLiteMutationBudget(t, metrics, 0, 0)
	})

	t.Run("partial tail", func(t *testing.T) {
		path, store := newSessionPreviewCachedFile(t, `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"stable"}]}}`+"\n")
		metrics := measureSessionPreviewSQLiteMutation(t, store, func() {
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteString(`{"timestamp":"2026-01-01T00:00:01Z"`); err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
				t.Fatal(err)
			}
		})
		assertSessionPreviewSQLiteMutationBudget(t, metrics, 0, 0)
	})

	t.Run("hidden append", func(t *testing.T) {
		forceImmediateSessionPreviewFlush(t)
		path, store := newSessionPreviewCachedFile(t, `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"stable"}]}}`+"\n")
		metrics := measureSessionPreviewSQLiteMutation(t, store, func() {
			appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"function_call_output","output":"hidden"}}`)
			if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
				t.Fatal(err)
			}
		})
		assertSessionPreviewSQLiteMutationBudget(t, metrics, 1, 2)
	})

	t.Run("small visible append", func(t *testing.T) {
		forceImmediateSessionPreviewFlush(t)
		path, store := newSessionPreviewCachedFile(t, `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"stable"}]}}`+"\n")
		metrics := measureSessionPreviewSQLiteMutation(t, store, func() {
			appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"answer-2","type":"message","role":"assistant","content":[{"type":"output_text","text":"new answer"}]}}`)
			if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
				t.Fatal(err)
			}
		})
		assertSessionPreviewSQLiteMutationBudget(t, metrics, 3, 5)
	})

	t.Run("skipped mirror identity", func(t *testing.T) {
		forceImmediateSessionPreviewFlush(t)
		path, store := newSessionPreviewCachedFile(t, `{"timestamp":"2026-01-01T00:00:00Z","type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"working"}}`+"\n")
		metrics := measureSessionPreviewSQLiteMutation(t, store, func() {
			appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:00.003Z","type":"response_item","payload":{"id":"status-1","type":"message","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"working"}]}}`)
			text, err := ReadSessionPreviewText(path, 0, 0)
			if err != nil || strings.Count(text, "working") != 1 {
				t.Fatalf("mirror preview=%q err=%v", text, err)
			}
		})
		assertSessionPreviewSQLiteMutationBudget(t, metrics, 2, 3)
	})

	t.Run("source rewrite", func(t *testing.T) {
		path, store := newSessionPreviewCachedFile(t, `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"old answer"}]}}`+"\n")
		metrics := measureSessionPreviewSQLiteMutation(t, store, func() {
			replacement := `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"answer-2","type":"message","role":"assistant","content":[{"type":"output_text","text":"replacement answer"}]}}` + "\n"
			if err := os.WriteFile(path, []byte(replacement), 0o600); err != nil {
				t.Fatal(err)
			}
			text, err := ReadSessionPreviewText(path, 0, 0)
			if err != nil || !strings.Contains(text, "replacement answer") || strings.Contains(text, "old answer") {
				t.Fatalf("rewrite preview=%q err=%v", text, err)
			}
		})
		assertSessionPreviewSQLiteMutationBudget(t, metrics, 3, 6)
	})

	t.Run("missing after warm cache", func(t *testing.T) {
		path, store := newSessionPreviewCachedFile(t, `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"stable"}]}}`+"\n")
		metrics := measureSessionPreviewSQLiteMutation(t, store, func() {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadSessionPreviewText(path, 0, 0); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("missing source error=%v", err)
			}
		})
		assertSessionPreviewSQLiteMutationBudget(t, metrics, 0, 0)
	})
}

func TestSessionPreviewSQLiteFiveSecondRefreshesCoalesceWrites(t *testing.T) {
	for _, test := range []struct {
		name      string
		line      func(int) string
		wantPages int
	}{
		{
			name: "visible messages",
			line: func(index int) string {
				return fmt.Sprintf(`{"timestamp":"2026-01-01T00:00:%02dZ","type":"response_item","payload":{"id":"answer-%d","type":"message","role":"assistant","content":[{"type":"output_text","text":"answer %d"}]}}`, index+1, index+2, index+2)
			},
			wantPages: 3,
		},
		{
			name: "hidden messages",
			line: func(index int) string {
				return fmt.Sprintf(`{"timestamp":"2026-01-01T00:00:%02dZ","type":"response_item","payload":{"type":"function_call_output","output":"hidden %d"}}`, index+1, index+1)
			},
			wantPages: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC)
			previousNow := sessionPreviewNow
			sessionPreviewNow = func() time.Time { return now }
			t.Cleanup(func() { sessionPreviewNow = previousNow })
			path, store := newSessionPreviewCachedFile(t, `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"stable"}]}}`+"\n")
			resetSessionPreviewSQLiteWriteCounters(t, store)
			commits := 0
			previousHook := sessionPreviewSQLiteCommitHook
			sessionPreviewSQLiteCommitHook = func(string) { commits++ }
			t.Cleanup(func() { sessionPreviewSQLiteCommitHook = previousHook })

			for index := 0; index < 7; index++ {
				appendPreviewLine(t, path, test.line(index))
				if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
					t.Fatal(err)
				}
				if index < 6 && commits != 0 {
					t.Fatalf("refresh %d committed early", index)
				}
				now = now.Add(5 * time.Second)
			}
			if commits != 1 {
				t.Fatalf("commits after seven refreshes = %d, want 1", commits)
			}
			if pages := sessionPreviewSQLiteWritePages(t, store, false); pages != test.wantPages {
				t.Fatalf("coalesced page writes = %d, want %d", pages, test.wantPages)
			}
			if frames := sessionPreviewSQLiteWALFrames(t, store); frames != int64(test.wantPages) {
				t.Fatalf("coalesced WAL frames = %d, want %d", frames, test.wantPages)
			}
		})
	}
}

func TestSessionPreviewSQLiteAppendWriteBudgetDoesNotScaleWithOtherSessions(t *testing.T) {
	forceImmediateSessionPreviewFlush(t)
	var baseline, populated sessionPreviewSQLiteMutationMetrics
	measure := func(t *testing.T, unrelated int) sessionPreviewSQLiteMutationMetrics {
		path, store := newSessionPreviewCachedFile(t, `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"stable"}]}}`+"\n")
		insertUnrelatedSessionPreviewRows(t, store, unrelated)
		return measureSessionPreviewSQLiteMutation(t, store, func() {
			appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"answer-2","type":"message","role":"assistant","content":[{"type":"output_text","text":"new answer"}]}}`)
			if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("baseline", func(t *testing.T) { baseline = measure(t, 0) })
	t.Run("500 unrelated sessions", func(t *testing.T) { populated = measure(t, 500) })
	if baseline.writePages != populated.writePages || baseline.walFrames != populated.walFrames {
		t.Fatalf("append write budget scaled with unrelated cache: baseline=%+v populated=%+v", baseline, populated)
	}
	if baseline.writePages != 3 || baseline.commits != 1 {
		t.Fatalf("unexpected baseline append budget: %+v", baseline)
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

func TestSessionPreviewSQLiteSyscallProbe(t *testing.T) {
	if os.Getenv("CXP_SESSION_PREVIEW_IO_PROBE") != "1" {
		t.Skip("set CXP_SESSION_PREVIEW_IO_PROBE=1 and run under strace")
	}
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
	resetSessionPreviewSQLiteWriteCounters(t, store)

	probeMarker := func(name string) { _, _ = fmt.Fprintln(os.Stderr, "CXP_PREVIEW_IO_PHASE", name) }
	probeMarker("exact-hit-begin")
	for index := 0; index < 100; index++ {
		if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	probeMarker("exact-hit-end")

	probeMarker("close-begin")
	resetSessionPreviewSQLiteForTest()
	probeMarker("close-end")

	probeMarker("reopen-begin")
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	store, err = currentSessionPreviewSQLiteStore()
	if err != nil {
		t.Fatal(err)
	}
	probeMarker("reopen-end")

	probeMarker("batched-small-begin")
	appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"answer-2","type":"message","role":"assistant","content":[{"type":"output_text","text":"small"}]}}`)
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	probeMarker("batched-small-end")

	probeMarker("threshold-append-begin")
	payload := strings.Repeat("x", 64*1024)
	appendPreviewLine(t, path, `{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"id":"answer-3","type":"message","role":"assistant","content":[{"type":"output_text","text":"`+payload+`"}]}}`)
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	probeMarker("threshold-append-end")
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

type sessionPreviewSQLiteMutationMetrics struct {
	commits    int
	writePages int
	walFrames  int64
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

func assertSessionPreviewSQLiteDatabaseAbsent(t *testing.T) {
	t.Helper()
	path, err := sessionPreviewSQLiteFile()
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if _, err := os.Stat(candidate); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unexpected SQLite cache file %q: %v", candidate, err)
		}
	}
}

func assertNoSessionPreviewSidecarChurn(t *testing.T, before map[string]sessionPreviewCacheFileState, after map[string]sessionPreviewCacheFileState) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("cache file count changed: before=%v after=%v", before, after)
	}
	for name, initial := range before {
		current, ok := after[name]
		if !ok {
			t.Fatalf("cache file %q disappeared: before=%v after=%v", name, before, after)
		}
		if strings.HasSuffix(name, "-shm") {
			if initial.size != current.size {
				t.Fatalf("shared-memory sidecar %q changed size: before=%+v after=%+v", name, initial, current)
			}
			continue
		}
		if initial != current {
			t.Fatalf("cache file %q changed: before=%+v after=%+v", name, initial, current)
		}
	}
}

func resetSessionPreviewSQLiteWriteCounters(t *testing.T, store *sessionPreviewSQLiteStore) {
	t.Helper()
	if _, err := store.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	resetSessionPreviewSQLiteDBStatus(t, store)
}

func resetSessionPreviewSQLiteDBStatus(t *testing.T, store *sessionPreviewSQLiteStore) {
	t.Helper()
	_ = sessionPreviewSQLiteDBStatus(t, store, sqlite.DBStatusCacheWrite, true)
	_ = sessionPreviewSQLiteDBStatus(t, store, sqlite.DBStatusCacheSpill, true)
}

func sessionPreviewSQLiteWritePages(t *testing.T, store *sessionPreviewSQLiteStore, reset bool) int {
	t.Helper()
	return sessionPreviewSQLiteDBStatus(t, store, sqlite.DBStatusCacheWrite, reset)
}

func assertSessionPreviewSQLiteWritePages(t *testing.T, store *sessionPreviewSQLiteStore, want int) {
	t.Helper()
	if got := sessionPreviewSQLiteWritePages(t, store, false); got != want {
		t.Fatalf("SQLite cache writes = %d pages, want %d", got, want)
	}
	if spills := sessionPreviewSQLiteDBStatus(t, store, sqlite.DBStatusCacheSpill, false); spills != 0 {
		t.Fatalf("SQLite cache spills = %d, want 0", spills)
	}
}

func sessionPreviewSQLiteDBStatus(t *testing.T, store *sessionPreviewSQLiteStore, op sqlite.DBStatusOp, reset bool) int {
	t.Helper()
	conn, err := store.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var current int
	if err := conn.Raw(func(driverConn any) error {
		status, ok := driverConn.(sqlite.DBStatus)
		if !ok {
			return errors.New("sqlite driver does not support DBStatus")
		}
		value, _, err := status.Status(op, reset)
		current = value
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return current
}

func sessionPreviewSQLiteWALFrames(t *testing.T, store *sessionPreviewSQLiteStore) int64 {
	t.Helper()
	info, err := os.Stat(store.path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		return 0
	}
	var pageSize int64
	if err := store.db.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		t.Fatal(err)
	}
	const walHeaderBytes int64 = 32
	const walFrameHeaderBytes int64 = 24
	if info.Size() < walHeaderBytes || (info.Size()-walHeaderBytes)%(pageSize+walFrameHeaderBytes) != 0 {
		t.Fatalf("unexpected WAL size %d for page size %d", info.Size(), pageSize)
	}
	return (info.Size() - walHeaderBytes) / (pageSize + walFrameHeaderBytes)
}

func newSessionPreviewCachedFile(t *testing.T, body string) (string, *sessionPreviewSQLiteStore) {
	t.Helper()
	t.Setenv(envSessionPreviewCacheBackend, sessionPreviewBackendSQLite)
	setTestUserCacheDir(t)
	path := filepath.Join(t.TempDir(), "session.jsonl")
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
	return path, store
}

func insertUnrelatedSessionPreviewRows(t *testing.T, store *sessionPreviewSQLiteStore, count int) {
	t.Helper()
	if count <= 0 {
		return
	}
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	statement, err := tx.Prepare(`INSERT INTO preview_session (
		path, filter_version, generation, parsed_offset, prefix_tail_hash, prefix_tail_size,
		source_size, source_mtime_ns, source_mode, has_file_id, source_dev, source_ino,
		has_ctime, source_ctime_ns, next_ordinal, last_write_ns
	) VALUES (?, ?, 1, 0, '', 0, 0, 0, 0, 0, '0', '0', 0, 0, 0, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	for index := 0; index < count; index++ {
		if _, err := statement.Exec(fmt.Sprintf("/unrelated/session-%04d.jsonl", index), sessionPreviewFilterVersion, index+1); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func forceImmediateSessionPreviewFlush(t *testing.T) {
	t.Helper()
	previousDelay := sessionPreviewAppendFlushDelay
	sessionPreviewAppendFlushDelay = 0
	t.Cleanup(func() { sessionPreviewAppendFlushDelay = previousDelay })
}

func measureSessionPreviewSQLiteMutation(t *testing.T, store *sessionPreviewSQLiteStore, mutate func()) sessionPreviewSQLiteMutationMetrics {
	t.Helper()
	resetSessionPreviewSQLiteWriteCounters(t, store)
	commits := 0
	previousHook := sessionPreviewSQLiteCommitHook
	sessionPreviewSQLiteCommitHook = func(string) { commits++ }
	t.Cleanup(func() { sessionPreviewSQLiteCommitHook = previousHook })
	mutate()
	return sessionPreviewSQLiteMutationMetrics{
		commits:    commits,
		writePages: sessionPreviewSQLiteWritePages(t, store, false),
		walFrames:  sessionPreviewSQLiteWALFrames(t, store),
	}
}

func assertSessionPreviewSQLiteMutationBudget(t *testing.T, metrics sessionPreviewSQLiteMutationMetrics, minPages int, maxPages int) {
	t.Helper()
	t.Logf("SQLite preview mutation metrics: commits=%d page-writes=%d WAL-frames=%d", metrics.commits, metrics.writePages, metrics.walFrames)
	wantCommits := 0
	if maxPages > 0 {
		wantCommits = 1
	}
	if metrics.commits != wantCommits {
		t.Fatalf("commits = %d, want %d; metrics=%+v", metrics.commits, wantCommits, metrics)
	}
	if metrics.writePages < minPages || metrics.writePages > maxPages {
		t.Fatalf("page writes = %d, want %d..%d; metrics=%+v", metrics.writePages, minPages, maxPages, metrics)
	}
	if metrics.walFrames != int64(metrics.writePages) {
		t.Fatalf("WAL frames = %d, page writes = %d; metrics=%+v", metrics.walFrames, metrics.writePages, metrics)
	}
}
