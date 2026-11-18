package codexhistory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type summaryCountingReader struct {
	io.Reader
	bytes int64
}

func (r *summaryCountingReader) Read(b []byte) (int, error) {
	n, e := r.Reader.Read(b)
	r.bytes += int64(n)
	return n, e
}

func TestSessionSummaryReadBudget(t *testing.T) {
	header := `{"type":"session_meta","payload":{"id":"test","cwd":"/project","source":"cli"}}` + "\n"
	prompt := `{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"real task"}]}}` + "\n"
	for _, tt := range []struct {
		name, body, title string
		count             int
	}{
		{"normal", header + prompt + strings.Repeat("x", 1024*1024), "real task", -1},
		{"huge record", header + `{"type":"turn_context","payload":"` + strings.Repeat("x", 1024*1024) + `"}` + "\n" + prompt, "", -1},
		{"huge metadata", `{"type":"session_meta","payload":{"data":"` + strings.Repeat("x", 1024*1024) + `"}}` + "\n", "", -1},
		{"final record without newline", header + strings.TrimSuffix(prompt, "\n"), "real task", 1},
		{"empty", header, "", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := &summaryCountingReader{Reader: strings.NewReader(tt.body)}
			meta, _, err := readSessionSummary(context.Background(), source, int64(len(tt.body)))
			if err != nil || meta.FirstPrompt != tt.title || meta.MessageCount != tt.count {
				t.Fatalf("meta=%+v err=%v", meta, err)
			}
			if source.bytes > sessionSummaryMaxBytes {
				t.Fatalf("read %d bytes, budget %d", source.bytes, sessionSummaryMaxBytes)
			}
			if tt.count == -1 && isEmptySession(Session{MessageCount: meta.MessageCount}) {
				t.Fatal("unknown count treated as empty")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := &summaryCountingReader{Reader: strings.NewReader(header)}
	if _, _, err := readSessionSummary(ctx, reader, int64(len(header))); !errors.Is(err, context.Canceled) || reader.bytes != 0 {
		t.Fatalf("cancellation: %v bytes=%d", err, reader.bytes)
	}
}

func TestSessionSummaryCacheDoesNotPoisonFullMetadata(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	root := os.Getenv(EnvCodexDir)
	p := testSessionPath(t, root, "summary")
	writeLargeSessionMetaFile(t, p, "summary", root, 1024*1024)
	var opens int
	old := openSessionMetaFile
	t.Cleanup(func() { openSessionMetaFile = old })
	openSessionMetaFile = func(path string) (*os.File, error) { opens++; return os.Open(path) }
	summary, err := readSessionFileSummaryCachedContext(context.Background(), p)
	if err != nil || summary.MessageCount != -1 {
		t.Fatalf("summary=%+v err=%v", summary, err)
	}
	if _, err = readSessionFileSummaryCachedContext(context.Background(), p); err != nil || opens != 1 {
		t.Fatalf("warm summary reread: opens=%d err=%v", opens, err)
	}
	full, err := readSessionFileMetaCached(p)
	if err != nil || full.MessageCount < 1 || opens != 2 {
		t.Fatalf("full read reused partial metadata: %+v opens=%d err=%v", full, opens, err)
	}
	enriched, err := readSessionFileSummaryCachedContext(context.Background(), p)
	if err != nil || enriched.MessageCount != full.MessageCount || opens != 2 {
		t.Fatalf("complete metadata did not supersede summary: %+v opens=%d err=%v", enriched, opens, err)
	}
	store, err := currentCatalogSQLiteStore(p)
	if err != nil {
		t.Fatal(err)
	}
	row, found, err := store.loadSessionMeta(context.Background(), p)
	if err != nil || !found || row.meta.MessageCount != full.MessageCount {
		t.Fatalf("bad persistent metadata: %+v %v", row, err)
	}
	appendFile(t, p, `{"type":"response_item","payload":{"type":"message","role":"assistant","content":[]}}`+"\n")
	if _, err = readSessionFileSummaryCachedContext(context.Background(), p); err != nil || opens != 3 {
		t.Fatalf("append did not invalidate memory: opens=%d err=%v", opens, err)
	}
}

func TestSessionSummaryDiscoveryPreservesRelationshipsAndUnknownSessions(t *testing.T) {
	root, dir, project := setupCodexDir(t)
	parent := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	child := "11111111-2222-3333-4444-555555555555"
	fork := "22222222-3333-4444-5555-666666666666"
	unknown := "33333333-3333-4444-5555-666666666666"
	writeSessionFile(t, dir, parent, "2026-01-01T00:00:00Z", project, `"cli"`, "normal task")
	writeSessionFile(t, dir, child, "2026-01-01T00:00:01Z", project, `{"subagent":{"thread_spawn":{"parent_thread_id":"`+parent+`"}}}`, "child task")
	writeForkSessionFile(t, dir, fork, "2026-01-01T00:00:02Z", project, parent, "fork task")
	writeSessionFile(t, dir, "44444444-3333-4444-5555-666666666666", "2026-01-01T00:00:00Z", project, `"cli"`, "")
	p := writeSessionFile(t, dir, unknown, "2026-01-01T00:00:00Z", project, `"cli"`, "")
	appendFile(t, p, strings.Repeat("x", int(sessionSummaryMaxBytes)*2))
	// A broken persistent cache directory must not prevent bounded source discovery.
	if err := os.WriteFile(filepath.Join(root, ".codex-proxy"), []byte("unavailable"), 0600); err != nil {
		t.Fatal(err)
	}
	projects, err := DiscoverProjectSummariesContext(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	sessions := collectAllSessions(projects)
	if len(sessions) != 3 {
		t.Fatalf("unexpected visible sessions: %+v", sessions)
	}
	main := findSession(sessions, parent)
	if main == nil || len(main.Subagents) != 1 || main.Subagents[0].SessionID != child {
		t.Fatalf("lost subagent relationship: %+v", main)
	}
	forked := findSession(sessions, fork)
	if forked == nil || forked.ParentSessionID != parent {
		t.Fatalf("lost fork: %+v", forked)
	}
	pending := findSession(sessions, unknown)
	if pending == nil || pending.MessageCount != -1 {
		t.Fatalf("unknown session hidden: %+v", pending)
	}
}

func BenchmarkSessionSummaryLargeTranscript(b *testing.B) {
	header := `{"type":"session_meta","payload":{"id":"test","cwd":"/project","source":"cli"}}` + "\n"
	prompt := `{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"real task"}]}}` + "\n"
	body := header + prompt + strings.Repeat("x", 64*1024*1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(header + prompt)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := readSessionSummary(context.Background(), strings.NewReader(body), int64(len(body))); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSessionSummaryRecencyAndCancellation(t *testing.T) {
	root, dir, project := setupCodexDir(t)
	p := writeSessionFile(t, dir, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "2026-01-01T00:00:00Z", project, `"cli"`, "task")
	want := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(p, want, want); err != nil {
		t.Fatal(err)
	}
	projects, err := DiscoverProjectSummariesContext(context.Background(), root)
	if err != nil || len(projects) != 1 || !projects[0].Sessions[0].ModifiedAt.Equal(want) {
		t.Fatalf("bad recency: %+v %v", projects, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = DiscoverProjectSummariesContext(ctx, root); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestSessionSummaryRecentKeepsCompleteDiscoveryOrder(t *testing.T) {
	root, dir, project := setupCodexDir(t)
	parent := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	child := "11111111-2222-3333-4444-555555555555"
	duplicate := "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
	older := "2026-01-01T00:00:00Z"
	newer := "2026-01-02T00:00:00Z"
	oldTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	write := func(id, ts, source, title string, mod time.Time) {
		t.Helper()
		path := writeSessionFile(t, dir, id, ts, project, source, title)
		if err := os.Chtimes(path, mod, mod); err != nil {
			t.Fatal(err)
		}
	}
	for i := range recentSessionFileLimit {
		write(fmt.Sprintf("aaaaaaaa-0000-0000-0000-%012x", i), older, `"cli"`, "older task", oldTime)
	}
	write(parent, older, `"cli"`, "parent", oldTime.Add(2*time.Hour))
	write(child, older, `{"subagent":{"thread_spawn":{"parent_thread_id":"`+parent+`"}}}`, "child", oldTime.Add(4*time.Hour))
	write(duplicate, newer, `"cli"`, "newer copy", oldTime.Add(3*time.Hour))
	oldPath := filepath.Join(dir, "rollout-2026-01-01T00-00-00-"+duplicate+".jsonl")
	newPath := filepath.Join(dir, "rollout-2026-01-02T00-00-00-"+duplicate+".jsonl")
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	write(duplicate, older, `"cli"`, "older copy", oldTime.Add(time.Hour))

	ctx := context.Background()
	var recent []Project
	publishes := 0
	full, err := DiscoverProjectSummariesWithRecent(ctx, root, func(p []Project) {
		publishes++
		recent = p
	})
	if err != nil {
		t.Fatal(err)
	}
	sessions := collectAllSessions(recent)
	main := findSession(sessions, parent)
	if publishes != 1 || len(sessions) > recentSessionFileLimit || main == nil || len(main.Subagents) != 0 ||
		findSession(sessions, duplicate) != nil || findSession(sessions, child) != nil {
		t.Fatalf("invalid recent publish: count=%d, sessions=%d, parent=%+v", publishes, len(sessions), main)
	}
	fullSessions := collectAllSessions(full)
	main = findSession(fullSessions, parent)
	copy := findSession(fullSessions, duplicate)
	if main == nil || len(main.Subagents) != 1 || main.Subagents[0].SessionID != child || copy == nil || copy.FirstPrompt != "older copy" {
		t.Fatal("full discovery changed parent association or the duplicate's canonical title")
	}
	ordinary, err := DiscoverProjectSummariesWithRecent(ctx, root, func([]Project) {
		t.Error("warm catalog published a recent snapshot")
	})
	if err != nil || !reflect.DeepEqual(full, ordinary) {
		t.Fatalf("full cold and ordinary warm discovery differ, error: %v", err)
	}
}

func TestSessionSummaryKeepsPromptAndVisibilityRules(t *testing.T) {
	root, dir, project := setupCodexDir(t)
	visible := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	hidden := "11111111-2222-3333-4444-555555555555"
	p := writeSessionFile(t, dir, visible, "2026-01-01T00:00:00Z", project, `"cli"`, "# AGENTS.md injected instructions")
	appendFile(t, p, `{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"User message:\nreal task\n\nTeams helper safety:\ninjected suffix"}]}}`+"\n")
	writeSessionFile(t, dir, hidden, "2026-01-01T00:00:00Z", project, `"cli"`, "[codex-helper-debug] hidden debug")
	// Older control transcripts can have >64 KiB of injected context before
	// their first user prompt. Keep that marker visible to the normal filter.
	control := writeSessionFile(t, dir, "22222222-2222-3333-4444-555555555555", "2026-01-01T00:00:00Z", project, `"cli"`, "")
	appendFile(t, control, `{"type":"response_item","payload":{"type":"message","role":"developer","content":[{"type":"input_text","text":"`+strings.Repeat("p", 70*1024)+`"}]}}`+"\n")
	appendFile(t, control, `{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"You are handling an unrecognized message from the user's Microsoft Teams control chat for codex-helper.\nUser message:\ncontrol task"}]}}`+"\n")
	projects, err := DiscoverProjectSummariesContext(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	sessions := collectAllSessions(FilterUserVisibleProjects(projects))
	if len(sessions) != 1 || sessions[0].SessionID != visible || sessions[0].FirstPrompt != "real task" {
		t.Fatalf("prompt/visibility changed: %+v", sessions)
	}
}

func TestSessionSummaryPersistsOnceAndReusesCatalog(t *testing.T) {
	root, dir, project := setupCodexDir(t)
	id := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	writeSessionFile(t, dir, id, "2026-01-01T00:00:00Z", project, `"cli"`, "task")
	var commits int
	old := catalogSQLiteCommitHook
	t.Cleanup(func() { catalogSQLiteCommitHook = old })
	catalogSQLiteCommitHook = func(string) { commits++ }
	for i := 0; i < 2; i++ {
		if _, err := DiscoverProjectSummariesContext(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		// Each pass reloads metadata from the catalog.
	}
	if commits != 1 {
		t.Fatalf("summary commits=%d, want one batched publication", commits)
	}
	if _, err := os.Stat(filepath.Join(root, ".codex-proxy")); err != nil {
		t.Fatalf("summary cache missing: %v", err)
	}
}

func TestSessionSummaryBusyCatalogDoesNotCausePerFileFallback(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	root, dir, project := setupCodexDir(t)
	for i := 0; i < 32; i++ {
		id := fmt.Sprintf("%08d-1111-2222-3333-444444444444", i)
		writeSessionFile(t, dir, id, "2026-01-01T00:00:00Z", project, `"cli"`, "task")
	}
	history := filepath.Join(root, "history.jsonl")
	if err := os.WriteFile(history, []byte(`{"session_id":"unused","ts":1,"text":"task"}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHistoryIndexContext(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	store, err := currentCatalogSQLiteStore(history)
	if err != nil {
		t.Fatal(err)
	}
	lockDB, err := openCatalogSQLite(store.path)
	if err != nil {
		t.Fatal(err)
	}
	defer lockDB.Close()
	conn, err := lockDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(context.Background(), "BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "ROLLBACK")
	old := sessionMetaWindowReadHook
	t.Cleanup(func() { sessionMetaWindowReadHook = old })
	sessionMetaWindowReadHook = func(int64, int64) { t.Fatal("busy catalog triggered a full transcript fallback") }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	projects, err := DiscoverProjectSummariesContext(ctx, root)
	if err != nil || len(collectAllSessions(projects)) != 32 {
		t.Fatalf("busy catalog prevented summaries: projects=%d err=%v", len(projects), err)
	}
}

func TestSessionSummaryReusesPersistentCatalogWithoutSourceReadsOrWrites(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	root, dir, project := setupCodexDir(t)
	writeSessionFile(t, dir, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "2026-01-01T00:00:00Z", project, `"cli"`, "complete title")
	if _, err := DiscoverProjects(root); err != nil {
		t.Fatal(err)
	}
	if err := CloseCaches(); err != nil {
		t.Fatal(err)
	}
	oldOpen, oldCommit := openSessionMetaFile, catalogSQLiteCommitHook
	t.Cleanup(func() { openSessionMetaFile = oldOpen; catalogSQLiteCommitHook = oldCommit })
	openSessionMetaFile = func(string) (*os.File, error) {
		t.Fatal("valid persistent metadata was not reused")
		return nil, errors.New("unexpected source read")
	}
	catalogSQLiteCommitHook = func(string) { t.Fatal("read-only summary discovery committed metadata") }
	projects, err := DiscoverProjectSummariesContext(context.Background(), root)
	if err != nil || len(projects) != 1 || len(projects[0].Sessions) != 1 || projects[0].Sessions[0].FirstPrompt != "complete title" || projects[0].Sessions[0].MessageCount != 1 {
		t.Fatalf("persistent reuse: %+v err=%v", projects, err)
	}
}
