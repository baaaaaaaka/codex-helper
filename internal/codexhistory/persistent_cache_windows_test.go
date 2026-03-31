//go:build windows

package codexhistory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func replaceFilePreservingModTimeWindows(t *testing.T, path string, content string) {
	t.Helper()

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
	if err := replacePersistentCacheFilePlatform(tmpPath, path); err != nil {
		t.Fatalf("replace replacement file: %v", err)
	}
}

func TestWriteJSONAtomically_RewritesExistingFileWindows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")

	if err := writeJSONAtomically(path, map[string]string{"prompt": "alpha"}); err != nil {
		t.Fatalf("first writeJSONAtomically: %v", err)
	}
	if err := writeJSONAtomically(path, map[string]string{"prompt": "bravo"}); err != nil {
		t.Fatalf("second writeJSONAtomically: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	if string(data) == "" {
		t.Fatal("expected cache file contents")
	}
	var payload map[string]string
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal cache: %v", err)
	}
	if payload["prompt"] != "bravo" {
		t.Fatalf("prompt = %q, want bravo", payload["prompt"])
	}
}

func TestSessionFilePersistentCache_InvalidatesOnPreservedMetadataReplaceWindows(t *testing.T) {
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
	replaceFilePreservingModTimeWindows(t, filePath, replacement)

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

func TestHistoryIndexPersistentCache_InvalidatesOnPreservedMetadataReplaceWindows(t *testing.T) {
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
	replaceFilePreservingModTimeWindows(t, historyPath, replacement)

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
