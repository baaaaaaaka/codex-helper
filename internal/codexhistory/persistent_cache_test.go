package codexhistory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	sqlite "modernc.org/sqlite"
)

func setTestUserCacheDir(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	codexDir := filepath.Join(base, ".codex")
	if err := os.MkdirAll(codexDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CACHE_HOME", base)
	t.Setenv("HOME", base)
	t.Setenv("LOCALAPPDATA", base)
	t.Setenv(EnvCodexDir, codexDir)
	resetPersistentCacheStatesForTest()
	// Registered last so open SQLite handles close before TempDir cleanup.
	t.Cleanup(resetPersistentCacheStatesForTest)
	return base
}

func setBenchmarkUserCacheDir(b *testing.B) string {
	b.Helper()
	base := b.TempDir()
	codexDir := filepath.Join(base, ".codex")
	if err := os.MkdirAll(codexDir, 0o700); err != nil {
		b.Fatal(err)
	}
	b.Setenv("XDG_CACHE_HOME", base)
	b.Setenv("HOME", base)
	b.Setenv("LOCALAPPDATA", base)
	b.Setenv(EnvCodexDir, codexDir)
	resetPersistentCacheStatesForTest()
	b.Cleanup(resetPersistentCacheStatesForTest)
	return base
}

func resetPersistentCacheStatesForTest() {
	resetSessionPreviewSQLiteForTest()
	resetCatalogSQLiteForTest()
	resetCacheV2ForTest()
	resetSessionFileCache()
}

func TestCacheV2PathFollowsCodexDirAndScopesIdentity(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	source := filepath.Join(root, "sessions", "2026", "session.jsonl")
	if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
		t.Fatal(err)
	}

	scope, err := persistentCacheWriterScopeID()
	if err != nil {
		t.Fatal(err)
	}
	wantDir := filepath.Join(root, ".codex-proxy", "codexhistory", cacheVersionDirName(), scope)
	for name, wantBase := range map[string]string{
		cacheV2CatalogFile: cacheV2CatalogFile,
		cacheV2PreviewFile: cacheV2PreviewFile,
	} {
		path, err := cacheV2DatabasePath(source, name)
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Dir(path) != wantDir || filepath.Base(path) != wantBase {
			t.Fatalf("cache path = %q, want %q/%q", path, wantDir, wantBase)
		}
	}
	if strings.Contains(wantDir, filepath.Join(os.Getenv("XDG_CACHE_HOME"), "codex-proxy")) {
		t.Fatalf("v2 cache unexpectedly used platform cache directory: %q", wantDir)
	}
}

func TestCacheV2IdentityScopeSeparatesUsersWithoutConfiguration(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	source := filepath.Join(root, "sessions", "2026", "session.jsonl")
	originalScope := persistentCacheWriterScopeID
	t.Cleanup(func() { persistentCacheWriterScopeID = originalScope })

	persistentCacheWriterScopeID = func() (string, error) { return "uid-1000", nil }
	userPath, err := cacheV2DatabasePath(source, cacheV2CatalogFile)
	if err != nil {
		t.Fatal(err)
	}
	persistentCacheWriterScopeID = func() (string, error) { return "uid-0", nil }
	rootPath, err := cacheV2DatabasePath(source, cacheV2CatalogFile)
	if err != nil {
		t.Fatal(err)
	}

	if userPath == rootPath {
		t.Fatalf("different effective users shared cache path %q", userPath)
	}
	if !strings.Contains(userPath, filepath.Join(cacheVersionDirName(), "uid-1000")) ||
		!strings.Contains(rootPath, filepath.Join(cacheVersionDirName(), "uid-0")) {
		t.Fatalf("unexpected scoped paths: user=%q root=%q", userPath, rootPath)
	}
}

func TestCacheV2DatabasesUseUnifiedVersionAndRollbackJournal(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	source := testSessionPath(t, root, "versioned")
	writeSessionMetaFile(t, source, "versioned", root, "prompt")
	if _, err := readSessionFileMetaCached(source); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSessionPreviewText(source, 0, 0); err != nil {
		t.Fatal(err)
	}

	paths := []string{}
	catalog, err := currentCatalogSQLiteStore(source)
	if err != nil {
		t.Fatal(err)
	}
	preview, err := currentSessionPreviewSQLiteStoreForSource(source)
	if err != nil {
		t.Fatal(err)
	}
	for label, db := range map[string]*sql.DB{"catalog": catalog.db, "preview": preview.db} {
		var version int
		var journal string
		var synchronous int
		if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil {
			t.Fatal(err)
		}
		if version != cacheVersion || !strings.EqualFold(journal, "delete") || synchronous != 1 {
			t.Fatalf("%s pragmas: version=%d journal=%q synchronous=%d", label, version, journal, synchronous)
		}
	}
	paths = append(paths, catalog.path, preview.path)
	for _, path := range paths {
		assertNoSQLiteSidecars(t, path)
	}
}

func TestSessionMetaCatalogPersistsAcrossProcessStateResetWithoutWriting(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	source := testSessionPath(t, root, "persist")
	writeSessionMetaFile(t, source, "persist", root, "persistent prompt")

	commits := 0
	previousHook := catalogSQLiteCommitHook
	catalogSQLiteCommitHook = func(string) { commits++ }
	t.Cleanup(func() { catalogSQLiteCommitHook = previousHook })
	first, err := readSessionFileMetaCached(source)
	if err != nil {
		t.Fatal(err)
	}
	if commits != 1 {
		t.Fatalf("initial catalog commits = %d, want 1", commits)
	}
	path, err := catalogSQLiteFileForSource(source)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	resetSessionFileCache()
	resetCatalogSQLiteForTest()
	previousOpen := openSessionMetaFile
	openSessionMetaFile = func(string) (*os.File, error) {
		return nil, errors.New("exact catalog hit reparsed source")
	}
	t.Cleanup(func() { openSessionMetaFile = previousOpen })
	second, err := readSessionFileMetaCached(source)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || commits != 1 {
		t.Fatalf("persistent metadata mismatch or write: first=%#v second=%#v commits=%d", first, second, commits)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("exact catalog hit changed database: before=%+v after=%+v", before, after)
	}
}

func TestSessionMetaCatalogAppendReadsOnlyNewBytes(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	source := testSessionPath(t, root, "long-session")
	writeLargeSessionMetaFile(t, source, "long-session", root, 4*1024*1024)
	if _, err := readSessionFileMetaCached(source); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	appendLine := `{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"id":"tail-answer","type":"message","role":"assistant","content":[{"type":"output_text","text":"small tail"}]}}` + "\n"
	appendFile(t, source, appendLine)
	after, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}

	var offsets, sizes []int64
	previousHook := sessionMetaWindowReadHook
	sessionMetaWindowReadHook = func(offset, size int64) {
		offsets = append(offsets, offset)
		sizes = append(sizes, size)
	}
	t.Cleanup(func() { sessionMetaWindowReadHook = previousHook })
	resetSessionFileCache()
	meta, err := readSessionFileMetaCached(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 1 || offsets[0] != before.Size() || sizes[0] != after.Size()-before.Size() {
		t.Fatalf("metadata append windows offsets=%v sizes=%v old=%d new=%d", offsets, sizes, before.Size(), after.Size())
	}
	if meta.MessageCount == 0 || meta.FirstPrompt != "first prompt" {
		t.Fatalf("incremental metadata = %#v", meta)
	}
}

func TestDiscoverProjectsBatchesCatalogRowsInOneTransaction(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	for index := 0; index < 3; index++ {
		id := fmt.Sprintf("00000000-0000-0000-0000-%012d", index+1)
		writeSessionMetaFile(t, testSessionPath(t, root, id), id, root, fmt.Sprintf("prompt %d", index))
	}
	commits := 0
	previousHook := catalogSQLiteCommitHook
	catalogSQLiteCommitHook = func(string) { commits++ }
	t.Cleanup(func() { catalogSQLiteCommitHook = previousHook })
	projects, err := DiscoverProjects(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectAllSessions(projects)) != 3 || commits != 1 {
		t.Fatalf("sessions=%d commits=%d, want 3 sessions in one commit", len(collectAllSessions(projects)), commits)
	}
	store, err := currentCatalogSQLiteStore(filepath.Join(root, "history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.db.QueryRow(`SELECT count(*) FROM session_meta`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("session_meta rows = %d, want 3", count)
	}
}

func TestHistoryCatalogAppendReadsOnlyTailAndExactHitDoesNotWrite(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	historyPath := filepath.Join(root, "history.jsonl")
	firstLine := `{"session_id":"s1","ts":1770777540,"text":"first prompt"}` + "\n"
	if err := os.WriteFile(historyPath, []byte(firstLine), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := loadHistoryIndexContext(context.Background(), root); err != nil || got.sessions["s1"].FirstPrompt != "first prompt" {
		t.Fatalf("initial history: %#v err=%v", got.sessions, err)
	}
	oldInfo, err := os.Stat(historyPath)
	if err != nil {
		t.Fatal(err)
	}
	appendFile(t, historyPath, `{"session_id":"s2","ts":1770777550,"text":"second prompt"}`+"\n")
	newInfo, err := os.Stat(historyPath)
	if err != nil {
		t.Fatal(err)
	}
	var offset, size int64
	previousReadHook := historyIndexWindowReadHook
	historyIndexWindowReadHook = func(gotOffset, gotSize int64) { offset, size = gotOffset, gotSize }
	t.Cleanup(func() { historyIndexWindowReadHook = previousReadHook })
	commits := 0
	previousCommitHook := catalogSQLiteCommitHook
	catalogSQLiteCommitHook = func(string) { commits++ }
	t.Cleanup(func() { catalogSQLiteCommitHook = previousCommitHook })
	idx, err := loadHistoryIndexContext(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if offset != oldInfo.Size() || size != newInfo.Size()-oldInfo.Size() || idx.sessions["s2"].FirstPrompt != "second prompt" || commits != 1 {
		t.Fatalf("history append offset=%d size=%d session=%#v commits=%d", offset, size, idx.sessions["s2"], commits)
	}
	store, err := currentCatalogSQLiteStore(historyPath)
	if err != nil {
		t.Fatal(err)
	}
	resetCatalogWritePages(t, store)
	historyIndexWindowReadHook = func(int64, int64) { t.Fatal("exact history hit read source") }
	if _, err := loadHistoryIndexContext(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if commits != 1 || catalogWritePages(t, store, false) != 0 {
		t.Fatalf("exact history hit commits=%d page writes=%d", commits, catalogWritePages(t, store, false))
	}
}

func TestCatalogIdenticalConcurrentWritersCommitOnlyOnce(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	source := testSessionPath(t, root, "idempotent")
	writeSessionMetaFile(t, source, "idempotent", root, "prompt")
	info, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := readSessionFileMetaContext(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	hash, hashSize, _ := sessionPreviewPrefixTailHash(source, info.Size())
	entry := catalogSessionMetaEntry{path: source, fileKey: newFileCacheKey(source, info), parsedOffset: info.Size(), prefixTailHash: hash, prefixTailSize: hashSize, meta: meta}
	storeOne, err := currentCatalogSQLiteStore(source)
	if err != nil {
		t.Fatal(err)
	}
	dbTwo, err := openCatalogSQLite(storeOne.path)
	if err != nil {
		t.Fatal(err)
	}
	defer dbTwo.Close()
	storeTwo := &catalogSQLiteStore{path: storeOne.path, db: dbTwo}
	commits := 0
	previousHook := catalogSQLiteCommitHook
	catalogSQLiteCommitHook = func(string) { commits++ }
	t.Cleanup(func() { catalogSQLiteCommitHook = previousHook })
	if err := storeOne.writeSessionMetaBatch(context.Background(), map[string]catalogSessionMetaEntry{source: entry}, nil); err != nil {
		t.Fatal(err)
	}
	resetCatalogWritePages(t, storeTwo)
	if err := storeTwo.writeSessionMetaBatch(context.Background(), map[string]catalogSessionMetaEntry{source: entry}, nil); err != nil {
		t.Fatal(err)
	}
	if commits != 1 || catalogWritePages(t, storeTwo, false) != 0 {
		t.Fatalf("identical writers commits=%d second page writes=%d", commits, catalogWritePages(t, storeTwo, false))
	}
}

func TestCacheV2DoesNotCreateJSONWALOrSHM(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	source := testSessionPath(t, root, "no-sidecars")
	writeSessionMetaFile(t, source, "no-sidecars", root, "prompt")
	if _, err := readSessionFileMetaCached(source); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSessionPreviewText(source, 0, 0); err != nil {
		t.Fatal(err)
	}
	legacy, err := legacyLocalPersistentCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	for _, tree := range []string{sharedPersistentCacheBase(root), legacy} {
		_ = filepath.WalkDir(tree, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return nil
			}
			name := strings.ToLower(entry.Name())
			if strings.HasSuffix(name, ".json") || strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-shm") || strings.HasSuffix(name, "-journal") {
				t.Fatalf("unexpected cache artifact %q", path)
			}
			return nil
		})
	}
}

func TestMissingSessionDoesNotCreateCatalog(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	source := filepath.Join(root, "sessions", "missing.jsonl")
	if _, err := readSessionFileMetaCached(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing source error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".codex-proxy")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing source created cache directories: %v", err)
	}
}

func TestCacheV2ReadOnlyCodexDirFallsBackToSource(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("read-only directory semantics require an unprivileged Unix user")
	}
	setTestUserCacheDir(t)
	root := filepath.Join(t.TempDir(), "readonly-codex")
	source := testSessionPath(t, root, "readonly")
	writeSessionMetaFile(t, source, "readonly", root, "source remains authoritative")
	if err := os.Chmod(filepath.Dir(source), 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(root, 0o700)
		_ = os.Chmod(filepath.Dir(source), 0o700)
	})
	resetPersistentCacheStatesForTest()
	meta, err := readSessionFileMetaCached(source)
	if err != nil || meta.FirstPrompt != "source remains authoritative" {
		t.Fatalf("read-only metadata=%#v err=%v", meta, err)
	}
	text, err := ReadSessionPreviewText(source, 0, 0)
	if err != nil || !strings.Contains(text, "source remains authoritative") {
		t.Fatalf("read-only preview=%q err=%v", text, err)
	}
	if _, err := os.Stat(filepath.Join(root, ".codex-proxy")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only fallback created cache: %v", err)
	}
}

func TestCatalogBusyWriterFallsBackToSourcePromptly(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	source := testSessionPath(t, root, "busy-catalog")
	writeSessionMetaFile(t, source, "busy-catalog", root, "first prompt")
	if _, err := readSessionFileMetaCached(source); err != nil {
		t.Fatal(err)
	}
	store, err := currentCatalogSQLiteStore(source)
	if err != nil {
		t.Fatal(err)
	}
	lockDB, err := openCatalogSQLite(store.path)
	if err != nil {
		t.Fatal(err)
	}
	defer lockDB.Close()
	lockConn, err := lockDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lockConn.Close()
	if _, err := lockConn.ExecContext(context.Background(), `BEGIN EXCLUSIVE`); err != nil {
		t.Fatal(err)
	}
	defer lockConn.ExecContext(context.Background(), `ROLLBACK`)

	appendFile(t, source, `{"timestamp":"2026-01-01T00:02:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"second prompt"}]}}`+"\n")
	resetSessionFileCache()
	started := time.Now()
	meta, err := readSessionFileMetaCached(source)
	elapsed := time.Since(started)
	if err != nil || meta.MessageCount < 2 {
		t.Fatalf("metadata under catalog lock=%#v err=%v", meta, err)
	}
	if elapsed > time.Second {
		t.Fatalf("catalog lock fallback took %s", elapsed)
	}
}

func TestCacheV2CleansKnownLegacyArtifactsButKeepsForeignShard(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	legacy, err := legacyLocalPersistentCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(legacy, 0o700); err != nil {
		t.Fatal(err)
	}
	writerID := strings.Repeat("a", 32)
	if err := os.WriteFile(filepath.Join(legacy, "shared_writer_id.json"), []byte(strconv.Quote(writerID)), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"session_meta_cache.json", "history_index_cache.json", "session_preview_cache.json", "session_preview_cache.sqlite3", "session_preview_cache.sqlite3-wal", "session_preview_cache.sqlite3-shm"} {
		if err := os.WriteFile(filepath.Join(legacy, name), []byte("legacy"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	sharedDir := filepath.Join(sharedPersistentCacheBase(root), "session-meta")
	if err := os.MkdirAll(sharedDir, 0o700); err != nil {
		t.Fatal(err)
	}
	ownShard := filepath.Join(sharedDir, writerID+".json")
	foreignShard := filepath.Join(sharedDir, "foreign.json")
	if err := os.WriteFile(ownShard, []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(foreignShard, []byte("foreign"), 0o600); err != nil {
		t.Fatal(err)
	}
	scope, err := persistentCacheWriterScopeID()
	if err != nil {
		t.Fatal(err)
	}
	oldScope := filepath.Join(sharedPersistentCacheBase(root), "v1", scope)
	foreignScope := filepath.Join(sharedPersistentCacheBase(root), "v1", "foreign-scope")
	if err := os.MkdirAll(oldScope, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(foreignScope, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldScope, cacheV2CatalogFile), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreignScope, cacheV2CatalogFile), []byte("foreign"), 0o600); err != nil {
		t.Fatal(err)
	}
	source := testSessionPath(t, root, "cleanup")
	writeSessionMetaFile(t, source, "cleanup", root, "prompt")
	if _, err := readSessionFileMetaCached(source); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"session_meta_cache.json", "history_index_cache.json", "session_preview_cache.json", "session_preview_cache.sqlite3", "session_preview_cache.sqlite3-wal", "session_preview_cache.sqlite3-shm", "shared_writer_id.json"} {
		if _, err := os.Stat(filepath.Join(legacy, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("legacy artifact %q survived: %v", name, err)
		}
	}
	if _, err := os.Stat(ownShard); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("own legacy shard survived: %v", err)
	}
	if got, err := os.ReadFile(foreignShard); err != nil || string(got) != "foreign" {
		t.Fatalf("foreign shard changed: %q err=%v", got, err)
	}
	if _, err := os.Stat(oldScope); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old cache version scope survived: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(foreignScope, cacheV2CatalogFile)); err != nil || string(got) != "foreign" {
		t.Fatalf("foreign old-version scope changed: %q err=%v", got, err)
	}
}

func TestCatalogCorruptionIsQuarantinedAndSourceStillLoads(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	source := testSessionPath(t, root, "corrupt")
	writeSessionMetaFile(t, source, "corrupt", root, "survives corruption")
	path, err := catalogSQLiteFileForSource(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	meta, err := readSessionFileMetaCached(source)
	if err != nil || meta.FirstPrompt != "survives corruption" {
		t.Fatalf("metadata=%#v err=%v", meta, err)
	}
	invalid, err := filepath.Glob(path + ".invalid-*")
	if err != nil || len(invalid) != 1 {
		t.Fatalf("quarantine=%v err=%v", invalid, err)
	}
}

func TestCatalogSQLiteSyscallProbe(t *testing.T) {
	if os.Getenv("CXP_CATALOG_IO_PROBE") != "1" {
		t.Skip("set CXP_CATALOG_IO_PROBE=1 and run under strace")
	}
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	source := testSessionPath(t, root, "catalog-io")
	writeLargeSessionMetaFile(t, source, "catalog-io", root, 8*1024*1024)
	if _, err := readSessionFileMetaCached(source); err != nil {
		t.Fatal(err)
	}
	historyPath := filepath.Join(root, "history.jsonl")
	if err := os.WriteFile(historyPath, []byte(`{"session_id":"s1","ts":1770777540,"text":"first"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHistoryIndexContext(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	marker := func(name string) { _, _ = fmt.Fprintln(os.Stderr, "CXP_CATALOG_IO_PHASE", name) }

	resetSessionFileCache()
	marker("exact-hit-begin")
	if _, err := readSessionFileMetaCached(source); err != nil {
		t.Fatal(err)
	}
	marker("exact-hit-end")

	appendFile(t, source, `{"timestamp":"2026-01-01T00:02:00Z","type":"response_item","payload":{"id":"small-tail","type":"message","role":"assistant","content":[{"type":"output_text","text":"`+strings.Repeat("x", 4*1024)+`"}]}}`+"\n")
	resetSessionFileCache()
	marker("session-append-begin")
	if _, err := readSessionFileMetaCached(source); err != nil {
		t.Fatal(err)
	}
	marker("session-append-end")

	appendFile(t, historyPath, `{"session_id":"s2","ts":1770777550,"text":"second"}`+"\n")
	marker("history-append-begin")
	if _, err := loadHistoryIndexContext(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	marker("history-append-end")

	resetSessionFileCache()
	marker("steady-state-begin")
	if _, err := readSessionFileMetaCached(source); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHistoryIndexContext(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	marker("steady-state-end")
}

func TestCacheV2LiveNFS(t *testing.T) {
	const (
		rootEnv   = "CXP_CACHE_NFS_ROOT"
		childEnv  = "CXP_CACHE_NFS_CHILD"
		sourceEnv = "CXP_CACHE_NFS_SOURCE"
		readyEnv  = "CXP_CACHE_NFS_READY"
		goEnv     = "CXP_CACHE_NFS_GO"
	)
	root := strings.TrimSpace(os.Getenv(rootEnv))
	if root == "" {
		t.Skip("set CXP_CACHE_NFS_ROOT to a real NFS mount")
	}
	if os.Getenv(childEnv) == "1" {
		t.Setenv(EnvCodexDir, root)
		resetPersistentCacheStatesForTest()
		if err := os.WriteFile(os.Getenv(readyEnv), []byte("ready"), 0o600); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			if _, err := os.Stat(os.Getenv(goEnv)); err == nil {
				break
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for NFS process barrier")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if _, err := DiscoverProjects(root); err != nil {
			t.Fatal(err)
		}
		text, err := ReadSessionPreviewText(os.Getenv(sourceEnv), 0, 0)
		if err != nil || !strings.Contains(text, "nfs append") {
			t.Fatalf("NFS child preview=%d err=%v", len(text), err)
		}
		return
	}

	root = filepath.Join(root, fmt.Sprintf("cxp-cache-v2-%d", os.Getpid()))
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	t.Setenv(EnvCodexDir, root)
	localCache := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", localCache)
	t.Setenv("HOME", localCache)
	t.Setenv("LOCALAPPDATA", localCache)
	resetPersistentCacheStatesForTest()
	source := testSessionPath(t, root, "11111111-1111-1111-1111-111111111111")
	writeSessionMetaFile(t, source, "11111111-1111-1111-1111-111111111111", root, "nfs prompt")
	historyPath := filepath.Join(root, "history.jsonl")
	if err := os.WriteFile(historyPath, []byte(`{"session_id":"11111111-1111-1111-1111-111111111111","ts":1770777540,"text":"nfs prompt"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := DiscoverProjects(root); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSessionPreviewText(source, 0, 0); err != nil {
		t.Fatal(err)
	}
	appendFile(t, source, `{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"id":"nfs-tail","type":"message","role":"assistant","content":[{"type":"output_text","text":"nfs append `+strings.Repeat("x", 70*1024)+`"}]}}`+"\n")
	appendFile(t, historyPath, `{"session_id":"22222222-2222-2222-2222-222222222222","ts":1770777550,"text":"nfs second"}`+"\n")
	resetPersistentCacheStatesForTest()

	executable, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	barrier := t.TempDir()
	goPath := filepath.Join(barrier, "go")
	type child struct {
		cmd    *exec.Cmd
		ready  string
		output strings.Builder
	}
	children := make([]*child, 2)
	for index := range children {
		item := &child{ready: filepath.Join(barrier, fmt.Sprintf("ready-%d", index))}
		item.cmd = exec.Command(executable, "-test.run=^TestCacheV2LiveNFS$", "-test.count=1")
		item.cmd.Env = append(os.Environ(),
			childEnv+"=1",
			rootEnv+"="+root,
			sourceEnv+"="+source,
			readyEnv+"="+item.ready,
			goEnv+"="+goPath,
			EnvCodexDir+"="+root,
		)
		item.cmd.Stdout = &item.output
		item.cmd.Stderr = &item.output
		if err := item.cmd.Start(); err != nil {
			t.Fatal(err)
		}
		children[index] = item
	}
	deadline := time.Now().Add(30 * time.Second)
	for _, item := range children {
		for {
			if _, err := os.Stat(item.ready); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for NFS children")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := os.WriteFile(goPath, []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}
	for index, item := range children {
		if err := item.cmd.Wait(); err != nil {
			t.Fatalf("NFS child %d: %v\n%s", index, err, item.output.String())
		}
	}

	preview, err := currentSessionPreviewSQLiteStoreForSource(source)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := currentCatalogSQLiteStore(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, store := range []struct {
		path string
		db   *sql.DB
	}{{preview.path, preview.db}, {catalog.path, catalog.db}} {
		var integrity string
		if err := store.db.QueryRow(`PRAGMA integrity_check`).Scan(&integrity); err != nil || integrity != "ok" {
			t.Fatalf("NFS integrity %q: %q err=%v", store.path, integrity, err)
		}
		assertNoSQLiteSidecars(t, store.path)
	}
}

func testSessionPath(t *testing.T, root string, id string) string {
	t.Helper()
	dir := filepath.Join(root, "sessions", "2026", "07", "09")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "rollout-2026-07-09T00-00-00-"+id+".jsonl")
}

func writeSessionMetaFile(t *testing.T, path, sessionID, cwd, prompt string) {
	t.Helper()
	body := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":` + strconv.Quote(sessionID) + `,"cwd":` + strconv.Quote(cwd) + `,"source":"cli"}}` + "\n" +
		`{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"prompt-1","type":"message","role":"user","content":[{"type":"input_text","text":` + strconv.Quote(prompt) + `}]}}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeLargeSessionMetaFile(t *testing.T, path, sessionID, cwd string, minimumBytes int) {
	t.Helper()
	var body strings.Builder
	body.WriteString(`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":` + strconv.Quote(sessionID) + `,"cwd":` + strconv.Quote(cwd) + `,"source":"cli"}}` + "\n")
	body.WriteString(`{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"id":"prompt-1","type":"message","role":"user","content":[{"type":"input_text","text":"first prompt"}]}}` + "\n")
	hidden := `{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"type":"function_call_output","output":"` + strings.Repeat("x", 1024) + `"}}` + "\n"
	for body.Len() < minimumBytes {
		body.WriteString(hidden)
	}
	if err := os.WriteFile(path, []byte(body.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

func appendFile(t *testing.T, path string, value string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(value); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertNoSQLiteSidecars(t *testing.T, path string) {
	t.Helper()
	for _, suffix := range []string{"-journal", "-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unexpected SQLite sidecar %q: %v", path+suffix, err)
		}
	}
}

func resetCatalogWritePages(t *testing.T, store *catalogSQLiteStore) {
	t.Helper()
	_ = catalogDBStatus(t, store, sqlite.DBStatusCacheWrite, true)
	_ = catalogDBStatus(t, store, sqlite.DBStatusCacheSpill, true)
}

func catalogWritePages(t *testing.T, store *catalogSQLiteStore, reset bool) int {
	t.Helper()
	return catalogDBStatus(t, store, sqlite.DBStatusCacheWrite, reset)
}

func catalogDBStatus(t *testing.T, store *catalogSQLiteStore, op sqlite.DBStatusOp, reset bool) int {
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
