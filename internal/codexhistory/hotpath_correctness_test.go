package codexhistory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSessionPreviewCandidateKeysMatchFullParseAcrossBatches(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	oldBytes, oldDelay := sessionPreviewAppendFlushBytes, sessionPreviewAppendFlushDelay
	t.Cleanup(func() { sessionPreviewAppendFlushBytes, sessionPreviewAppendFlushDelay = oldBytes, oldDelay })
	sessionPreviewAppendFlushBytes, sessionPreviewAppendFlushDelay = 500, time.Hour
	path := filepath.Join(os.Getenv(EnvCodexDir), "candidates.jsonl")
	response := func(id, text string, second int) string {
		return fmt.Sprintf(`{"timestamp":"2026-11-17T00:00:%02dZ","type":"response_item","payload":{"id":%q,"type":"message","role":"assistant","content":[{"type":"output_text","text":%q}]}}`, second, id, text)
	}
	event := func(text string, second int) string {
		return fmt.Sprintf(`{"timestamp":"2026-11-17T00:00:%02dZ","type":"event_msg","payload":{"type":"agent_message","message":%q}}`, second, text)
	}
	lines := []string{response("a", "same", 1), event("same", 1), response("b", "same", 1), response("b", "ignored duplicate id", 2), event("same", 30), response("c", "same", 31), response("d", "unique", 20), event("unique", 20), response("e", "same", 0), response("a", "old source id", 50)}
	wantCounts := []int{1, 1, 2, 2, 3, 4, 5, 5, 6, 6}
	var body strings.Builder
	for i, line := range lines {
		body.WriteString(line + "\n")
		if err := os.WriteFile(path, []byte(body.String()), 0600); err != nil {
			t.Fatal(err)
		}
		want, err := readSessionMessagesWindow(path, 0, int64(body.Len()), 0, projectPreviewMessage)
		if err != nil {
			t.Fatal(err)
		}
		if len(want) != wantCounts[i] {
			t.Fatalf("golden prefix %d: got %d messages, want %d", i, len(want), wantCounts[i])
		}
		got, err := ReadSessionPreviewText(path, 0, 0)
		if err != nil || got != FormatPreviewMessages(want, 0) {
			t.Fatalf("prefix %d: %v\ngot %s\nwant %s", i, err, got, FormatPreviewMessages(want, 0))
		}
		if i%3 == 2 {
			if err := CloseCaches(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Force a peer commit after taking a local header. Candidate lookups must
	// reject the obsolete snapshot rather than combine generations.
	store, err := currentSessionPreviewSQLiteStoreForSource(path)
	if err != nil {
		t.Fatal(err)
	}
	entry, _, err := store.loadHeader(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE preview_session SET generation=generation+1 WHERE id=?`, entry.sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.loadCandidateSeen(entry, []Message{{sourceID: "response_item:a"}}); err != errSessionPreviewSQLiteConflict {
		t.Fatalf("stale candidate snapshot: %v", err)
	}
}

func TestSessionMetaPartialTailMatchesFullParse(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	path := filepath.Join(os.Getenv(EnvCodexDir), "partial-meta.jsonl")
	header := `{"type":"session_meta","payload":{"id":"partial","cwd":"/project"}}` + "\r\n"
	line := `{"timestamp":"2026-11-17T00:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"你好"}]}}`
	for _, body := range []string{"", "{", header, header + line[:len(line)-4], header + line, header + line + "\n", header + line + "\n" + line} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		got, err := readSessionFileMetaCached(path)
		if err != nil {
			t.Fatal(err)
		}
		want, err := readSessionFileMetaWindowContext(context.Background(), path, 0, int64(len(body)), sessionFileMeta{})
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("body %q\ngot %+v\nwant %+v err=%v", body, got, want, err)
		}
	}
}

func TestThreadNameSharedViewPublishesOnlyCommittedAppend(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	path := filepath.Join(root, threadNameIndexFile)
	if err := os.WriteFile(path, []byte("{\"id\":\"a\",\"thread_name\":\"old\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := loadThreadNameIndexContext(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	store, err := currentCatalogSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER fail_name BEFORE INSERT ON thread_name BEGIN SELECT RAISE(FAIL,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	appendFile(t, path, "{\"id\":\"a\",\"thread_name\":\"new\"}\n")
	fallback, err := loadThreadNameIndexContext(context.Background(), root)
	if err != nil || fallback.lookup("a") != "new" || first.lookup("a") != "old" {
		t.Fatal("failed write changed shared names", err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER fail_name`); err != nil {
		t.Fatal(err)
	}
	// Keep a reader locked while the writer commits, then release its merge.
	resetThreadNameIndexCache()
	first, err = loadThreadNameIndexContext(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	appendFile(t, path, "{\"id\":\"a\",\"thread_name\":\"latest\"}\n")
	old := catalogSQLiteCommitHook
	t.Cleanup(func() { catalogSQLiteCommitHook = old })
	committed := make(chan struct{})
	catalogSQLiteCommitHook = func(string) { close(committed) }
	first.data.mu.RLock()
	done := make(chan error, 1)
	go func() { _, err := loadThreadNameIndexContext(context.Background(), root); done <- err }()
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		first.data.mu.RUnlock()
		t.Fatal("no commit")
	}
	if first.data.names["a"] != "new" {
		t.Error("published before reader released")
	}
	first.data.mu.RUnlock()
	if err := <-done; err != nil || first.lookup("a") != "latest" {
		t.Fatal("committed merge missing", err)
	}
}

func TestSessionPreviewRecoversWhenPeerReplacesDatabase(t *testing.T) {
	for _, switchRoot := range []bool{false, true} {
		t.Run(fmt.Sprint(switchRoot), func(t *testing.T) {
			setTestUserCacheDir(t)
			forceImmediateSessionPreviewFlush(t)
			path, store := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
			if switchRoot {
				if _, err := currentSessionPreviewSQLiteStoreAtPath(store.path + ".other-root"); err != nil {
					t.Fatal(err)
				}
			}
			// A separate SQLite handle represents a new cache created by another
			// process after it quarantined the old database. No source file changes.
			replacement := store.path + ".replacement"
			peer, err := openSessionPreviewSQLite(replacement)
			if err != nil {
				t.Fatal(err)
			}
			if err := peer.Close(); err != nil {
				t.Fatal(err)
			}
			// Windows cannot rename an open SQLite file; the native migration/close
			// tests cover that platform's supported replacement boundary.
			oldPath := store.path + ".previous"
			if err := os.Rename(store.path, oldPath); err != nil {
				if runtime.GOOS == "windows" {
					t.Skipf("Windows refuses open database replacement: %v", err)
				}
				t.Fatal(err)
			}
			if err := os.Rename(replacement, store.path); err != nil {
				t.Fatal(err)
			}
			appendPreviewLine(t, path, previewRecord(1))
			got, err := ReadSessionPreviewText(path, 0, 0)
			if err != nil || strings.Count(got, "Codex answer:") != 2 {
				t.Fatalf("replacement recovery: %v %s", err, got)
			}
			verify, err := openSessionPreviewSQLite(store.path)
			if err != nil {
				t.Fatal(err)
			}
			var count int
			err = verify.QueryRow(`SELECT COUNT(*) FROM preview_message`).Scan(&count)
			verify.Close()
			if err != nil || count != 2 {
				t.Fatalf("replacement did not receive rebuilt cache: count=%d err=%v", count, err)
			}
			if err := CloseCaches(); err != nil {
				t.Fatal(err)
			}
			got, err = ReadSessionPreviewText(path, 0, 0)
			if err != nil || strings.Count(got, "Codex answer:") != 2 {
				t.Fatalf("replacement reopen: %v %s", err, got)
			}
		})
	}

}

func TestSessionPreviewSQLiteEvictionCannotReuseRevision(t *testing.T) {
	setTestUserCacheDir(t)
	path, store := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	stale, _, err := store.loadHeader(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DELETE FROM preview_session WHERE id=?`, stale.sessionID); err != nil {
		t.Fatal(err)
	}
	invalidateSessionPreviewSQLiteMemory(store.path, path)
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	current, _, err := store.loadHeader(path)
	if err != nil {
		t.Fatal(err)
	}
	if current.sessionID == stale.sessionID {
		t.Fatal("an evicted cache ID was reused; generation/offset/ordinal no longer identify a revision")
	}
	if _, err := store.loadCandidateSeen(stale, []Message{{sourceID: "message:0"}}); err != errSessionPreviewSQLiteConflict {
		t.Fatalf("candidate lookup accepted evicted revision: %v", err)
	}
	if err := store.append(stale, current.fileKey, current.offset, current.prefixTailHash, current.prefixTailSize, nil, sessionPreviewSeenDelta{}); err != errSessionPreviewSQLiteConflict {
		t.Fatalf("append accepted evicted revision: %v", err)
	}
}
