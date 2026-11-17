package codexhistory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func historyRecord(id int) string {
	return fmt.Sprintf("{\"session_id\":\"session-%d\",\"ts\":10,\"text\":\"task %d\"}\n", id, id)
}
func TestHistoryAppendMergesOnlyAfterCommitWithConcurrentReader(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	root := t.TempDir()
	path := filepath.Join(root, "history.jsonl")
	if err := os.WriteFile(path, []byte(historyRecord(0)), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := loadHistoryIndexContext(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	appendFile(t, path, historyRecord(1))
	committed := make(chan struct{})
	old := catalogSQLiteCommitHook
	t.Cleanup(func() { catalogSQLiteCommitHook = old })
	catalogSQLiteCommitHook = func(string) { close(committed) }
	first.data.mu.RLock()
	original := first.data.sessions["session-0"]
	type result struct {
		idx historyIndex
		err error
	}
	done := make(chan result, 1)
	go func() { idx, err := loadHistoryIndexContext(context.Background(), root); done <- result{idx, err} }()
	// The reader keeps the map stable while the writer commits SQLite, then
	// the short merge waits for that reader. No full map copy is needed.
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		first.data.mu.RUnlock()
		t.Fatal("writer did not commit")
	}
	if len(first.data.sessions) != 1 {
		t.Error("shared map changed while read lock held")
	}
	first.data.mu.RUnlock()
	got := <-done
	if got.err != nil || got.idx.data != first.data || got.idx.len() != 2 {
		t.Fatalf("incremental merge: %+v %v", got.idx, got.err)
	}
	first.data.mu.RLock()
	same := first.data.sessions["session-0"] == original
	first.data.mu.RUnlock()
	if !same {
		t.Error("unchanged history record was copied")
	}
	if _, ok := first.lookup("session-1"); !ok {
		t.Error("existing synchronized view missed committed append")
	}
}
func TestHistoryFailedAppendLeavesSharedMapUnchanged(t *testing.T) {
	setTestUserCacheDir(t)
	root := t.TempDir()
	path := filepath.Join(root, "history.jsonl")
	if err := os.WriteFile(path, []byte(historyRecord(0)), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := loadHistoryIndexContext(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	store, err := currentCatalogSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER fail_history BEFORE INSERT ON history_session BEGIN SELECT RAISE(FAIL,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	appendFile(t, path, historyRecord(1))
	provisional, err := loadHistoryIndexContext(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := provisional.lookup("session-1"); !ok {
		t.Error("failed cache write hid source result")
	}
	if _, ok := first.lookup("session-1"); ok {
		t.Error("failed cache write mutated shared index")
	}
	if _, err := store.db.Exec(`DROP TRIGGER fail_history`); err != nil {
		t.Fatal(err)
	}
	next, err := loadHistoryIndexContext(context.Background(), root)
	if err != nil || next.data != first.data || next.len() != 2 {
		t.Fatalf("retry: %+v %v", next, err)
	}
}
func TestPreviewModeSwitchReusesUncommittedTail(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	path, store := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	oldBytes, oldDelay, oldHook := sessionPreviewAppendFlushBytes, sessionPreviewAppendFlushDelay, sessionMessagesWindowReadHook
	t.Cleanup(func() {
		sessionPreviewAppendFlushBytes = oldBytes
		sessionPreviewAppendFlushDelay = oldDelay
		sessionMessagesWindowReadHook = oldHook
	})
	sessionPreviewAppendFlushBytes, sessionPreviewAppendFlushDelay = 1<<30, time.Hour
	info, _ := os.Stat(path)
	expectedOffset := info.Size()
	windows := 0
	sessionMessagesWindowReadHook = func(offset, size int64) {
		if offset != expectedOffset {
			t.Errorf("mode switch reparsed %d, want %d", offset, expectedOffset)
		}
		expectedOffset += size
		windows++
	}
	for i := 1; i <= 12; i++ {
		appendPreviewLine(t, path, previewRecord(i))
		text, err := ReadSessionPreviewText(path, 0, 0)
		if err != nil || strings.Count(text, "Codex answer:") != i+1 {
			t.Fatalf("text %d: %v", i, err)
		}
		messages, _, err := readSessionPreviewCacheValueSQLite(path, true)
		if err != nil || len(messages) != i+1 {
			t.Fatalf("messages %d: got %d %v", i, len(messages), err)
		}
		messages[0].Content = "caller mutation"
		messages[len(messages)-1].Content = "caller mutation"
	}
	if windows != 12 {
		t.Fatalf("parsed %d windows for 12 appends", windows)
	}
	sessionPreviewSQLiteState.mu.Lock()
	cached := sessionPreviewSQLiteState.memory[sessionPreviewSQLiteMemoryKey(store.path, path)]
	sessionPreviewSQLiteState.mu.Unlock()
	if !cached.hasMessages || !cached.parsed.messagesLoaded || len(cached.messages) != 13 || cached.messages[0].Content == "caller mutation" || cached.messages[12].Content == "caller mutation" {
		t.Fatal("text read lost or aliased canonical messages")
	}
	var checkpoint int64
	if err := store.db.QueryRow(`SELECT parsed_offset FROM preview_session WHERE path=?`, path).Scan(&checkpoint); err != nil || checkpoint != info.Size() {
		t.Fatalf("view switch wrote pending batch: %d %v", checkpoint, err)
	}
	sessionPreviewAppendFlushDelay = 0
	appendPreviewLine(t, path, previewRecord(13))
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	sessionMessagesWindowReadHook = nil
	resetSessionPreviewSQLiteForTest()
	messages, _, err := readSessionPreviewCacheValueSQLite(path, true)
	if err != nil || len(messages) != 14 {
		t.Fatalf("flushed reopen: %d %v", len(messages), err)
	}
}

func TestCatalogRebuildsEarlierDerivedSchemaWithoutTouchingSource(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	source := testSessionPath(t, root, "earlier-cache")
	writeSessionMetaFile(t, source, "earlier-cache", root, "source survives rebuild")
	before, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readSessionFileSummaryCachedContext(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	path, err := catalogSQLiteFileForSource(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := CloseCaches(); err != nil {
		t.Fatal(err)
	}
	// Simulate the earlier, unpublished derived-cache layout. The business
	// database is unrelated and must never be passed through this rebuild path.
	db, err := sql.Open("sqlite", sessionPreviewSQLiteFileURI(path))
	if err != nil {
		t.Fatal(err)
	}
	_, alterErr := db.Exec(`ALTER TABLE session_meta DROP COLUMN is_summary`)
	closeErr := db.Close()
	if alterErr != nil || closeErr != nil {
		t.Fatal(alterErr, closeErr)
	}
	meta, err := readSessionFileSummaryCachedContext(context.Background(), source)
	if err != nil || meta.FirstPrompt != "source survives rebuild" {
		t.Fatal(meta, err)
	}
	store, err := currentCatalogSQLiteStore(source)
	if err != nil {
		t.Fatal(err)
	}
	row, found, err := store.loadSessionMeta(context.Background(), source)
	if err != nil || !found || !row.isSummary || row.parsedOffset <= 0 {
		t.Fatalf("rebuilt summary: %+v %v", row, err)
	}
	quarantined, err := filepath.Glob(path + ".invalid-*")
	if err != nil || len(quarantined) != 1 {
		t.Fatalf("quarantine: %v %v", quarantined, err)
	}
	after, err := os.ReadFile(source)
	if err != nil || string(after) != string(before) {
		t.Fatal("cache rebuild changed source", err)
	}
}

func TestJSONLWindowBoundariesAndReadFailure(t *testing.T) {
	for _, tt := range []struct {
		name, body   string
		offset, size int64
		want         string
		wantErr      error
	}{
		{"empty", "", 0, 0, "", nil},
		{"final record", "one\ntwo", 0, 7, "one,two", nil},
		{"unaligned start", "one\ntwo\nthree\n", 2, 12, "two,three", nil},
		{"bounded prefix", "one\ntwo\n", 0, 4, "one", nil},
		{"truncated window", "one\n", 0, 8, "one", io.ErrUnexpectedEOF},
		{"invalid window", "", -1, 0, "", errors.New("invalid")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var lines []string
			err := readJSONLWindow(context.Background(), strings.NewReader(tt.body), tt.offset, tt.size, func(line []byte) error { lines = append(lines, string(line)); return nil })
			if strings.Join(lines, ",") != tt.want {
				t.Fatalf("lines=%v", lines)
			}
			if tt.wantErr == nil && err != nil || tt.wantErr != nil && err == nil {
				t.Fatal(err)
			}
			if tt.wantErr == io.ErrUnexpectedEOF && !errors.Is(err, tt.wantErr) {
				t.Fatal(err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := readJSONLWindow(ctx, strings.NewReader("one\n"), 0, 4, func([]byte) error { t.Fatal("read after cancellation"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	injected := errors.New("visitor failed")
	if err := readJSONLWindow(context.Background(), strings.NewReader("one\n"), 0, 4, func([]byte) error { return injected }); !errors.Is(err, injected) {
		t.Fatal(err)
	}
}
func TestSummaryPersistsActualCoverageAndCannotResumeFullParse(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	path := testSessionPath(t, root, "coverage")
	header := `{"type":"session_meta","payload":{"id":"coverage","cwd":"/project","source":"cli"}}` + "\n"
	prompt := `{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"task"}]}}` + "\n"
	rest := strings.Repeat(`{"type":"response_item","payload":{"type":"message","role":"assistant","content":[]}}`+"\n", 2000)
	if err := os.WriteFile(path, []byte(header+prompt+rest), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSessionFileSummaryCachedContext(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	if err := CloseCaches(); err != nil {
		t.Fatal(err)
	}
	store, err := currentCatalogSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	cached, found, err := store.loadSessionMeta(context.Background(), path)
	if err != nil || !found || !cached.isSummary || cached.parsedOffset != int64(len(header+prompt)) || cached.meta.MessageCount != -1 {
		t.Fatalf("coverage: %+v %v", cached, err)
	}
	if _, err := readSessionFileMetaCached(path); err != nil {
		t.Fatal(err)
	}
	complete, found, err := store.loadSessionMeta(context.Background(), path)
	if err != nil || !found || complete.isSummary || complete.parsedOffset != int64(len(header+prompt+rest)) || complete.meta.MessageCount != 2001 {
		t.Fatalf("full parse: %+v %v", complete, err)
	}
}
func TestMetadataLoaderSnapshotDiesWithContext(t *testing.T) {
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	path := testSessionPath(t, root, "loader")
	writeSessionMetaFile(t, path, "loader", root, "original")
	if _, err := readSessionFileMetaCached(path); err != nil {
		t.Fatal(err)
	}
	ctx := prefetchCatalogMetadata(context.Background(), root, []string{path})
	store, err := currentCatalogSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	row, _, err := store.loadSessionMeta(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	row.meta.FirstPrompt = "peer update"
	if err := store.writeSessionMetaBatch(context.Background(), map[string]catalogSessionMetaEntry{path: row}, nil); err != nil {
		t.Fatal(err)
	}
	old, err := readSessionFileMetaCachedContext(ctx, path)
	if err != nil || old.FirstPrompt != "original" {
		t.Fatal(old, err)
	}
	// No process-global metadata entry is allowed to override a fresh loader.
	next, err := readSessionFileMetaCachedContext(context.Background(), path)
	if err != nil || next.FirstPrompt != "peer update" {
		t.Fatal(next, err)
	}
}

func TestHistoryReadFailureDoesNotAdvanceOrHideDiscovery(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	root := t.TempDir()
	path := filepath.Join(root, "history.jsonl")
	if err := os.WriteFile(path, []byte(historyRecord(0)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHistoryIndexContext(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	store, err := currentCatalogSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	before, _, err := store.loadHistory(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	appendFile(t, path, historyRecord(1))
	old := historyIndexWindowReadHook
	t.Cleanup(func() { historyIndexWindowReadHook = old })
	historyIndexWindowReadHook = func(int64, int64) {
		historyIndexWindowReadHook = nil
		if err := os.Truncate(path, 0); err != nil {
			t.Fatal(err)
		}
	}
	idx, err := loadHistoryIndexContext(context.Background(), root)
	if err != nil || idx.len() != 0 {
		t.Fatalf("optional unreadable history: %+v %v", idx, err)
	}
	after, _, err := store.loadHistory(context.Background(), path)
	if err != nil || after.parsedOffset != before.parsedOffset {
		t.Fatalf("read failure checkpoint: %+v %v", after, err)
	}
}
func TestSessionMetadataTruncationDoesNotPublishProgress(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	path := testSessionPath(t, root, "truncated")
	writeSessionMetaFile(t, path, "truncated", root, "task")
	store, err := currentCatalogSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	old := sessionMetaWindowReadHook
	t.Cleanup(func() { sessionMetaWindowReadHook = old })
	sessionMetaWindowReadHook = func(int64, int64) {
		sessionMetaWindowReadHook = nil
		if err := os.Truncate(path, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := readSessionFileMetaCached(path); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated read: %v", err)
	}
	if _, found, err := store.loadSessionMeta(context.Background(), path); err != nil || found {
		t.Fatalf("failed read published metadata: %v %v", found, err)
	}
}
