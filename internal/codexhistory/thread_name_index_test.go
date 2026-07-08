package codexhistory

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadThreadNameIndexUsesLatestValidName(t *testing.T) {
	path := filepath.Join(t.TempDir(), threadNameIndexFile)
	content := strings.Join([]string{
		`{"id":"session-1","thread_name":"first","updated_at":"2026-01-01T00:00:00Z"}`,
		`not json`,
		`{"id":"session-2","thread_name":"second"}`,
		`{"id":"session-1","thread_name":" latest "}`,
		`{"id":"session-1","thread_name":""}`,
		`{"id":"","thread_name":"ignored"}`,
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write index: %v", err)
	}

	names, err := readThreadNameIndexContext(context.Background(), path)
	if err != nil {
		t.Fatalf("readThreadNameIndexContext: %v", err)
	}
	if got := names["session-1"]; got != "latest" {
		t.Fatalf("session-1 name = %q, want latest", got)
	}
	if got := names["session-2"]; got != "second" {
		t.Fatalf("session-2 name = %q, want second", got)
	}
	if len(names) != 2 {
		t.Fatalf("names = %#v, want exactly two valid sessions", names)
	}
}

func TestLoadThreadNameIndexCachesUntilFileChanges(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	resetThreadNameIndexCache()
	t.Cleanup(resetThreadNameIndexCache)

	root := t.TempDir()
	path := filepath.Join(root, threadNameIndexFile)
	if err := os.WriteFile(path, []byte(`{"id":"session-1","thread_name":"first"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write index: %v", err)
	}

	previousOpen := openThreadNameIndexFile
	opens := 0
	openThreadNameIndexFile = func(path string) (*os.File, error) {
		opens++
		return os.Open(path)
	}
	t.Cleanup(func() { openThreadNameIndexFile = previousOpen })

	for range 2 {
		names, err := loadThreadNameIndexContext(context.Background(), root)
		if err != nil {
			t.Fatalf("loadThreadNameIndexContext: %v", err)
		}
		if got := names["session-1"]; got != "first" {
			t.Fatalf("cached name = %q, want first", got)
		}
	}
	if opens != 1 {
		t.Fatalf("unchanged index opened %d times, want 1", opens)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open index for append: %v", err)
	}
	if _, err := f.WriteString(`{"id":"session-1","thread_name":"renamed"}` + "\n"); err != nil {
		_ = f.Close()
		t.Fatalf("append index: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close index: %v", err)
	}

	names, err := loadThreadNameIndexContext(context.Background(), root)
	if err != nil {
		t.Fatalf("reload changed index: %v", err)
	}
	if got := names["session-1"]; got != "renamed" {
		t.Fatalf("changed name = %q, want renamed", got)
	}
	if opens != 2 {
		t.Fatalf("changed index opened %d times, want 2", opens)
	}
}

func TestLoadThreadNameIndexIsOptionalMetadata(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	resetThreadNameIndexCache()
	t.Cleanup(resetThreadNameIndexCache)

	root := t.TempDir()
	names, err := loadThreadNameIndexContext(context.Background(), root)
	if err != nil {
		t.Fatalf("missing index error: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("missing index names = %#v, want empty", names)
	}

	path := filepath.Join(root, threadNameIndexFile)
	if err := os.WriteFile(path, []byte("ignored"), 0o600); err != nil {
		t.Fatalf("write index: %v", err)
	}
	previousOpen := openThreadNameIndexFile
	openThreadNameIndexFile = func(string) (*os.File, error) {
		return nil, os.ErrPermission
	}
	t.Cleanup(func() { openThreadNameIndexFile = previousOpen })
	names, err = loadThreadNameIndexContext(context.Background(), root)
	if err != nil {
		t.Fatalf("unreadable index error: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("unreadable index names = %#v, want empty", names)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := loadThreadNameIndexContext(ctx, root); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v, want context.Canceled", err)
	}
}

func TestDiscoverProjectsAndFindSessionUseThreadNameIndex(t *testing.T) {
	root, sessionsDir, projectDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	writeSessionFile(t, sessionsDir, sessionID, "2026-01-01T00:00:00Z", projectDir, `"cli"`, "original prompt")
	if err := os.WriteFile(
		filepath.Join(root, threadNameIndexFile),
		[]byte(`{"id":"`+sessionID+`","thread_name":"renamed thread"}`+"\n"),
		0o600,
	); err != nil {
		t.Fatalf("write index: %v", err)
	}

	projects, err := DiscoverProjects(root)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	sessions := collectAllSessions(projects)
	if len(sessions) != 1 {
		t.Fatalf("sessions = %d, want 1", len(sessions))
	}
	if sessions[0].ThreadName != "renamed thread" {
		t.Fatalf("ThreadName = %q, want renamed thread", sessions[0].ThreadName)
	}
	if sessions[0].DisplayTitle() != "renamed thread" {
		t.Fatalf("DisplayTitle = %q, want renamed thread", sessions[0].DisplayTitle())
	}
	if sessions[0].FirstPrompt != "original prompt" {
		t.Fatalf("FirstPrompt = %q, want original prompt", sessions[0].FirstPrompt)
	}

	found, err := FindSessionByID(root, sessionID)
	if err != nil {
		t.Fatalf("FindSessionByID: %v", err)
	}
	if found.ThreadName != "renamed thread" || found.DisplayTitle() != "renamed thread" {
		t.Fatalf("found session = %#v, want renamed thread title", found)
	}
}

func TestDiscoverProjectsPreservesPromptWithoutValidThreadName(t *testing.T) {
	root, sessionsDir, projectDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	writeSessionFile(t, sessionsDir, sessionID, "2026-01-01T00:00:00Z", projectDir, `"cli"`, "original prompt")
	if err := os.WriteFile(filepath.Join(root, threadNameIndexFile), []byte("{broken json\n"), 0o600); err != nil {
		t.Fatalf("write malformed index: %v", err)
	}

	projects, err := DiscoverProjects(root)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	sessions := collectAllSessions(projects)
	if len(sessions) != 1 || sessions[0].DisplayTitle() != "original prompt" {
		t.Fatalf("sessions = %#v, want original prompt fallback", sessions)
	}
}

func TestDiscoverProjectsRefreshesThreadNameWhenIndexChanges(t *testing.T) {
	root, sessionsDir, projectDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	writeSessionFile(t, sessionsDir, sessionID, "2026-01-01T00:00:00Z", projectDir, `"cli"`, "original prompt")

	displayTitle := func() string {
		t.Helper()
		projects, err := DiscoverProjects(root)
		if err != nil {
			t.Fatalf("DiscoverProjects: %v", err)
		}
		sessions := collectAllSessions(projects)
		if len(sessions) != 1 {
			t.Fatalf("sessions = %d, want 1", len(sessions))
		}
		return sessions[0].DisplayTitle()
	}

	if got := displayTitle(); got != "original prompt" {
		t.Fatalf("initial title = %q, want original prompt", got)
	}
	path := filepath.Join(root, threadNameIndexFile)
	if err := os.WriteFile(path, []byte(`{"id":"`+sessionID+`","thread_name":"first rename"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if got := displayTitle(); got != "first rename" {
		t.Fatalf("created-index title = %q, want first rename", got)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open index for append: %v", err)
	}
	if _, err := f.WriteString(`{"id":"` + sessionID + `","thread_name":"second rename"}` + "\n"); err != nil {
		_ = f.Close()
		t.Fatalf("append index: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close index: %v", err)
	}
	if got := displayTitle(); got != "second rename" {
		t.Fatalf("appended-index title = %q, want second rename", got)
	}
}

func BenchmarkLoadThreadNameIndexCached(b *testing.B) {
	resetThreadNameIndexCache()
	b.Cleanup(resetThreadNameIndexCache)

	root := b.TempDir()
	var content strings.Builder
	for i := range 1_000 {
		_, _ = fmt.Fprintf(&content, `{"id":"session-%d","thread_name":"name-%d"}`+"\n", i, i)
	}
	if err := os.WriteFile(filepath.Join(root, threadNameIndexFile), []byte(content.String()), 0o600); err != nil {
		b.Fatalf("write index: %v", err)
	}
	if _, err := loadThreadNameIndexContext(context.Background(), root); err != nil {
		b.Fatalf("warm index: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := loadThreadNameIndexContext(context.Background(), root); err != nil {
			b.Fatalf("load cached index: %v", err)
		}
	}
}

func BenchmarkReadThreadNameIndex1000(b *testing.B) {
	root := b.TempDir()
	path := filepath.Join(root, threadNameIndexFile)
	var content strings.Builder
	for i := range 1_000 {
		_, _ = fmt.Fprintf(&content, `{"id":"session-%d","thread_name":"name-%d"}`+"\n", i, i)
	}
	if err := os.WriteFile(path, []byte(content.String()), 0o600); err != nil {
		b.Fatalf("write index: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := readThreadNameIndexContext(context.Background(), path); err != nil {
			b.Fatalf("read index: %v", err)
		}
	}
}
