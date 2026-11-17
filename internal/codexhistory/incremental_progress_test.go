package codexhistory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func previewRecord(id int) string {
	return fmt.Sprintf(`{"type":"response_item","payload":{"id":"answer-%d","type":"message","role":"assistant","content":[{"type":"output_text","text":"answer %d"}]}}`, id, id)
}

func TestPreviewPendingProgressReadsEachCompleteRecordOnce(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	path, store := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	oldDelay, oldBytes := sessionPreviewAppendFlushDelay, sessionPreviewAppendFlushBytes
	sessionPreviewAppendFlushDelay, sessionPreviewAppendFlushBytes = time.Hour, 1<<30
	t.Cleanup(func() { sessionPreviewAppendFlushDelay, sessionPreviewAppendFlushBytes = oldDelay, oldBytes })
	info, _ := os.Stat(path)
	wantOffset := info.Size()
	oldHook := sessionMessagesWindowReadHook
	t.Cleanup(func() { sessionMessagesWindowReadHook = oldHook })
	var windows int
	sessionMessagesWindowReadHook = func(offset, size int64) {
		if offset != wantOffset {
			t.Errorf("window offset=%d, want %d", offset, wantOffset)
		}
		wantOffset += size
		windows++
	}
	for i := 1; i <= 20; i++ {
		appendPreviewLine(t, path, previewRecord(i))
		text, err := ReadSessionPreviewText(path, 0, 0)
		if err != nil || strings.Count(text, "Codex answer:") != i+1 {
			t.Fatalf("append %d: %q %v", i, text, err)
		}
	}
	if windows != 20 {
		t.Fatalf("parsed %d windows", windows)
	}
	var offset int64
	if err := store.db.QueryRow(`SELECT parsed_offset FROM preview_session`).Scan(&offset); err != nil || offset != info.Size() {
		t.Fatalf("small appends wrote: %d %v", offset, err)
	}
	// Force the existing pending batch to commit on the next source growth.
	sessionPreviewAppendFlushDelay = 0
	appendPreviewLine(t, path, previewRecord(21))
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	sessionMessagesWindowReadHook = nil
	resetSessionPreviewSQLiteForTest()
	text, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil || strings.Count(text, "Codex answer:") != 22 {
		t.Fatalf("reopen: %q %v", text, err)
	}
}

func TestPreviewPartialTailKeepsPrefixAndProvisionalRecord(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	path, _ := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	info, _ := os.Stat(path)
	prefix := info.Size()
	oldHook := sessionMessagesWindowReadHook
	t.Cleanup(func() { sessionMessagesWindowReadHook = oldHook })
	sessionMessagesWindowReadHook = func(offset, size int64) {
		if offset < prefix {
			t.Errorf("reparsed prefix at %d", offset)
		}
	}
	appendFile(t, path, `{"type":"response_item","payload":`)
	text, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil || strings.Count(text, "Codex answer:") != 1 {
		t.Fatalf("partial: %q %v", text, err)
	}
	// Finish a valid record without adding a newline, then finalize it later.
	appendFile(t, path, `{"id":"answer-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"answer 1"}]}}`)
	for range 2 {
		text, err = ReadSessionPreviewText(path, 0, 0)
		if err != nil || strings.Count(text, "Codex answer:") != 2 {
			t.Fatalf("provisional: %q %v", text, err)
		}
	}
	appendFile(t, path, "\n"+previewRecord(2)+"\n")
	text, err = ReadSessionPreviewText(path, 0, 0)
	if err != nil || strings.Count(text, "Codex answer:") != 3 {
		t.Fatalf("completed: %q %v", text, err)
	}
}

func TestPreviewConcurrentReadersOwnPendingState(t *testing.T) {
	setTestUserCacheDir(t)
	path, _ := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	appendPreviewLine(t, path, previewRecord(1))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			text, err := ReadSessionPreviewText(path, 0, 0)
			if err != nil || strings.Count(text, "Codex answer:") != 2 {
				t.Errorf("concurrent: %q %v", text, err)
			}
		}()
	}
	close(start)
	wg.Wait()
}

func TestPreviewOversizeAdmissionDiscardsMutatedState(t *testing.T) {
	setTestUserCacheDir(t)
	path, store := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	base, ok := loadSessionPreviewSQLiteAppendBase(store.path, path)
	if !ok {
		t.Fatal("missing append base")
	}
	base.text = strings.Repeat("x", int(sessionPreviewSQLiteMemoryMaxBytes)+1)
	storeSessionPreviewSQLiteMemory(store.path, path, base.fileKey, nil, base.text, false, &base)
	appendPreviewLine(t, path, previewRecord(1))
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	appendPreviewLine(t, path, previewRecord(2))
	text, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil || strings.Count(text, "Codex answer:") != 3 {
		t.Fatalf("admission lost messages: %q %v", text, err)
	}
}

func TestPreviewFailedCommitDiscardsPendingState(t *testing.T) {
	forceImmediateSessionPreviewFlush(t)
	path, store := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	if _, err := store.db.Exec(`CREATE TRIGGER fail_append BEFORE INSERT ON preview_message BEGIN SELECT RAISE(FAIL,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	appendPreviewLine(t, path, previewRecord(1))
	if text, err := ReadSessionPreviewText(path, 0, 0); err != nil || strings.Count(text, "Codex answer:") != 2 {
		t.Fatalf("source fallback: %q %v", text, err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER fail_append`); err != nil {
		t.Fatal(err)
	}
	appendPreviewLine(t, path, previewRecord(2))
	text, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil || strings.Count(text, "Codex answer:") != 3 {
		t.Fatalf("recovery: %q %v", text, err)
	}
	resetSessionPreviewSQLiteForTest()
	text, err = ReadSessionPreviewText(path, 0, 0)
	if err != nil || strings.Count(text, "Codex answer:") != 3 {
		t.Fatalf("durable recovery: %q %v", text, err)
	}
}

func TestSummaryPersistsAcrossCloseWithoutOpeningTranscript(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	root, dir, project := setupCodexDir(t)
	writeSessionFile(t, dir, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "2026-01-01T00:00:00Z", project, `"cli"`, "task")
	if _, err := DiscoverProjectSummariesContext(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if err := CloseCaches(); err != nil {
		t.Fatal(err)
	}
	old := openSessionMetaFile
	t.Cleanup(func() { openSessionMetaFile = old })
	openSessionMetaFile = func(path string) (*os.File, error) {
		t.Errorf("reopened unchanged transcript %s", path)
		return os.Open(path)
	}
	projects, err := DiscoverProjectSummariesContext(context.Background(), root)
	if err != nil || len(projects) != 1 {
		t.Fatalf("reopen summary: %+v %v", projects, err)
	}
}

func TestNamesPersistAcrossCloseAndAppendOnlyTail(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	root := t.TempDir()
	path := filepath.Join(root, threadNameIndexFile)
	if err := os.WriteFile(path, []byte(`{"id":"one","thread_name":"old"}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadThreadNameIndexContext(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	resetThreadNameIndexCache()
	if err := CloseCaches(); err != nil {
		t.Fatal(err)
	}
	old := openThreadNameIndexFile
	t.Cleanup(func() { openThreadNameIndexFile = old })
	openThreadNameIndexFile = func(path string) (*os.File, error) { t.Error("unchanged name source reopened"); return os.Open(path) }
	names, err := loadThreadNameIndexContext(context.Background(), root)
	if err != nil || names.lookup("one") != "old" {
		t.Fatal(names, err)
	}
	openThreadNameIndexFile = old
	appendFile(t, path, `{"id":"one","thread_name":"new"}`+"\n"+`{"id":"one","thread_name":""}`+"\n")
	names, err = loadThreadNameIndexContext(context.Background(), root)
	if err != nil || names.lookup("one") != "new" {
		t.Fatal(names, err)
	}
}

func TestPreviewLogicalQuotaEvictsWithoutChangingSources(t *testing.T) {
	setTestUserCacheDir(t)
	path, store := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	// Use metadata charges to exercise the quota without allocating 128 MiB.
	if _, err := store.db.Exec(`UPDATE preview_session SET cached_bytes=?`, sessionPreviewSQLiteMaxBytes+1); err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := pruneSessionPreviewBytes(tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM preview_message`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("cascade: %d %v", count, err)
	}
	resetSessionPreviewSQLiteForTest()
	text, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil || !strings.Contains(text, "answer 0") {
		t.Fatalf("source lost: %q %v", text, err)
	}
}

func TestSummaryCannotDowngradeConcurrentCompleteMetadata(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	path := testSessionPath(t, root, "summary-concurrent")
	writeLargeSessionMetaFile(t, path, "summary-concurrent", root, 1024*1024)
	full, err := readSessionFileMetaCached(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := currentCatalogSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := store.loadSessionMeta(context.Background(), path)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	entry.isSummary = true
	entry.meta.MessageCount = -1
	if err = store.writeSessionMetaBatch(context.Background(), map[string]catalogSessionMetaEntry{path: entry}, nil); err != nil {
		t.Fatal(err)
	}
	after, _, err := store.loadSessionMeta(context.Background(), path)
	if err != nil || after.parsedOffset < 0 || after.meta.MessageCount != full.MessageCount {
		t.Fatalf("summary overwrote complete parse: %+v %v", after, err)
	}
}

func TestHistoryHeaderInvalidatesAfterPeerConversion(t *testing.T) {
	setTestUserCacheDir(t)
	root := t.TempDir()
	path := filepath.Join(root, "history.jsonl")
	if err := os.WriteFile(path, []byte(`{"session_id":"one","ts":1,"text":"original"}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHistoryIndexContext(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	store, err := currentCatalogSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.loadHistory(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	db, err := openCatalogSQLite(store.path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	peer := &catalogSQLiteStore{path: store.path, db: db}
	entry, found, err := peer.loadHistory(context.Background(), path)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	changed := map[string]*historySessionInfo{"one": {FirstPrompt: "peer", FirstPromptTime: time.Unix(1, 0)}}
	if err = peer.replaceHistory(context.Background(), entry, true, entry.fileKey, entry.parsedOffset, entry.prefixTailHash, entry.prefixTailSize, &historyIndexData{sessions: changed}); err != nil {
		t.Fatal(err)
	}
	idx, err := loadHistoryIndexContext(context.Background(), root)
	if err != nil || historyPromptForTest(idx, "one") != "peer" {
		t.Fatalf("stale header reuse: %+v %v", idx, err)
	}
}

func TestPreviewMessagesThenTextAfterColdAppend(t *testing.T) {
	setTestUserCacheDir(t)
	path, _ := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	resetSessionPreviewSQLiteForTest()
	appendPreviewLine(t, path, previewRecord(1))
	messages, _, err := readSessionPreviewCacheValueSQLite(path, true)
	if err != nil || len(messages) != 2 {
		t.Fatalf("messages: %+v %v", messages, err)
	}
	messages[0].Content = "caller mutation"
	text, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil || !strings.Contains(text, "answer 0") || !strings.Contains(text, "answer 1") || strings.Contains(text, "caller mutation") {
		t.Fatalf("text after message append: %q %v", text, err)
	}
}

func TestNamesUnchangedPartialTailDoesNotWriteOnReopen(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	root := t.TempDir()
	path := filepath.Join(root, threadNameIndexFile)
	if err := os.WriteFile(path, []byte(`{"id":"one","thread_name":"old"}`+"\n"+`{"id":"one","thread_name":"new"}`), 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		names, err := loadThreadNameIndexContext(context.Background(), root)
		if err != nil || names.lookup("one") != "new" {
			t.Fatal(names, err)
		}
		if err := CloseCaches(); err != nil {
			t.Fatal(err)
		}
		resetThreadNameIndexCache()
		if i == 0 {
			old := catalogSQLiteCommitHook
			t.Cleanup(func() { catalogSQLiteCommitHook = old })
			catalogSQLiteCommitHook = func(string) { t.Error("unchanged partial name file caused a commit") }
		}
	}
}

func TestPreviewSourceChangedDuringParseDoesNotPublishCheckpoint(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	path, store := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	initial, _, err := store.loadHeader(path)
	if err != nil {
		t.Fatal(err)
	}
	appendPreviewLine(t, path, previewRecord(1))
	old := sessionMessagesWindowReadHook
	t.Cleanup(func() { sessionMessagesWindowReadHook = old })
	sessionMessagesWindowReadHook = func(int64, int64) {
		sessionMessagesWindowReadHook = nil
		if runtime.GOOS == "windows" {
			// An open source file cannot be replaced on Windows. Truncating it
			// still exercises a changed snapshot during the active read.
			if err := os.WriteFile(path, []byte(previewRecord(9)+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			return
		}
		replacement := path + ".replacement"
		if err := os.WriteFile(replacement, []byte(previewRecord(9)+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(replacement, path); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := readSessionPreviewCacheValueSQLite(path, false); err == nil {
		t.Fatal("changed snapshot was accepted")
	}
	after, _, err := store.loadHeader(path)
	if err != nil || after.offset != initial.offset || after.generation != initial.generation {
		t.Fatalf("checkpoint changed: %+v %v", after, err)
	}
	text, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil || !strings.Contains(text, "answer 9") || strings.Contains(text, "answer 0") {
		t.Fatalf("replacement: %q %v", text, err)
	}
}

func TestHistoryPartialTailOnlyReadsTailWithoutWriting(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	root := t.TempDir()
	path := filepath.Join(root, "history.jsonl")
	prefix := `{"session_id":"one","ts":1,"text":"first"}` + "\n"
	if err := os.WriteFile(path, []byte(prefix), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHistoryIndexContext(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	appendFile(t, path, `{"session_id":"two","ts":2,"text":`)
	if _, err := loadHistoryIndexContext(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	oldRead, oldCommit := historyIndexWindowReadHook, catalogSQLiteCommitHook
	t.Cleanup(func() { historyIndexWindowReadHook = oldRead; catalogSQLiteCommitHook = oldCommit })
	historyIndexWindowReadHook = func(offset, size int64) {
		if offset != int64(len(prefix)) {
			t.Errorf("read old history at %d", offset)
		}
	}
	catalogSQLiteCommitHook = func(string) { t.Error("unchanged partial history wrote to catalog") }
	idx, err := loadHistoryIndexContext(context.Background(), root)
	if err != nil || idx.len() != 1 {
		t.Fatal(idx, err)
	}
	catalogSQLiteCommitHook = nil
	appendFile(t, path, `"second"}`)
	idx, err = loadHistoryIndexContext(context.Background(), root)
	if err != nil || idx.len() != 2 {
		t.Fatal(idx, err)
	}
	// The temporary tail must never mutate the committed map.
	appendFile(t, path, "\n")
	idx, err = loadHistoryIndexContext(context.Background(), root)
	if err != nil || idx.len() != 2 {
		t.Fatal(idx, err)
	}
}

func TestPreviewHeaderReadDetectsPeerEviction(t *testing.T) {
	setTestUserCacheDir(t)
	path, store := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	entry, found, err := store.loadHeader(path)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	peer, err := openSessionPreviewSQLite(store.path)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if _, err := peer.Exec(`DELETE FROM preview_session WHERE id=?`, entry.sessionID); err != nil {
		t.Fatal(err)
	}
	if err := store.loadMessages(&entry, false); err != errSessionPreviewSQLiteConflict {
		t.Fatalf("stale payload accepted: %v", err)
	}
	if err := store.loadSeenState(&previewLoadedEntry{sessionPreviewSQLiteEntry: entry}); err != errSessionPreviewSQLiteConflict {
		t.Fatalf("stale seen accepted: %v", err)
	}
}

func TestPreviewQuotaEvictionRetainsProvisionalTail(t *testing.T) {
	setTestUserCacheDir(t)
	path, store := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	resetPath := path + "-large.jsonl"
	if err := os.WriteFile(resetPath, []byte(previewRecord(1)+"\n"+previewRecord(2)), 0600); err != nil {
		t.Fatal(err)
	}
	// Charge a synthetic oversized session without allocating a 128 MiB fixture.
	if _, err := store.db.Exec(fmt.Sprintf(`CREATE TRIGGER oversized AFTER INSERT ON preview_session BEGIN UPDATE preview_session SET cached_bytes=%d WHERE id=NEW.id; END`, sessionPreviewSQLiteMaxBytes+1)); err != nil {
		t.Fatal(err)
	}
	_, text, err := readSessionPreviewCacheValueSQLite(resetPath, false)
	if err != nil || strings.Count(text, "Codex answer:") != 2 || !strings.Contains(text, "answer 2") {
		t.Fatalf("eviction lost provisional tail: %q %v", text, err)
	}
	var count int
	if err := store.db.QueryRow(`SELECT count(*) FROM preview_session WHERE path=?`, resetPath).Scan(&count); err != nil || count != 0 {
		t.Fatalf("oversized entry retained: %d %v", count, err)
	}
}

func TestCatalogPrefetchBatchesOnlyDiscoveredTranscripts(t *testing.T) {
	setTestUserCacheDir(t)
	root := t.TempDir()
	store, err := currentCatalogSQLiteStore(filepath.Join(root, "history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	paths := make([]string, 600)
	updates := map[string]catalogSessionMetaEntry{}
	for i := range paths {
		paths[i] = filepath.Join(root, fmt.Sprintf("session-%d.jsonl", i))
		updates[paths[i]] = catalogSessionMetaEntry{path: paths[i], isSummary: true, meta: sessionFileMeta{FirstPrompt: fmt.Sprint(i), MessageCount: -1}}
	}
	stale := filepath.Join(root, "deleted.jsonl")
	updates[stale] = catalogSessionMetaEntry{path: stale, isSummary: true}
	if err := store.writeSessionMetaBatch(context.Background(), updates, nil); err != nil {
		t.Fatal(err)
	}
	ctx := prefetchCatalogMetadata(context.Background(), root, paths)
	entries, ok := ctx.Value(catalogMetadataKey{}).(map[string]*catalogSessionMetaEntry)
	if !ok || len(entries) != len(paths) {
		t.Fatalf("prefetch size=%d, want %d", len(entries), len(paths))
	}
	if _, ok := entries[stale]; ok {
		t.Fatal("loaded metadata for a removed transcript")
	}
	for i, path := range paths {
		entry, found, err := loadCatalogMetadata(ctx, store, path)
		if err != nil || !found || entry.meta.FirstPrompt != fmt.Sprint(i) {
			t.Fatalf("batch record %d: %+v %v", i, entry, err)
		}
	}
}
