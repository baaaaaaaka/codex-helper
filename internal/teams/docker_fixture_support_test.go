package teams

import (
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	dockerFixtureDirEnv   = "CXP_TEAMS_DOCKER_FIXTURE_DIR"
	dockerRuntimeDirEnv   = "CXP_TEAMS_DOCKER_RUNTIME_DIR"
	dockerCodexMountEnv   = "CXP_TEAMS_DOCKER_CODEX_MOUNTED"
	dockerFixtureCodexDir = "/home/baka/.codex/"
)

// dockerTeamsFixtureRoot is intentionally opt-in. The fixture contains a
// point-in-time copy of user state and must never become normal package or CI
// test data.
func dockerTeamsFixtureRoot(t *testing.T) string {
	t.Helper()
	root := strings.TrimSpace(os.Getenv(dockerFixtureDirEnv))
	if root == "" {
		t.Skipf("set %s to run the copied SQLite/Codex Docker fixture", dockerFixtureDirEnv)
	}
	for _, name := range []string{
		"teams/state.json",
		"teams/store.sqlite",
		"teams/registry.json",
		"codex/history.jsonl",
		"codex/session_index.jsonl",
	} {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(name))); err != nil {
			t.Fatalf("Docker fixture file %q: %v", name, err)
		}
	}
	return root
}

func dockerTeamsFixtureRuntimeRoot(t *testing.T) string {
	t.Helper()
	root := strings.TrimSpace(os.Getenv(dockerRuntimeDirEnv))
	if root == "" {
		return t.TempDir()
	}
	testName := strings.NewReplacer("/", "-", "\\", "-", " ", "-").Replace(t.Name())
	root = filepath.Join(root, testName)
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("create Docker fixture runtime root: %v", err)
	}
	return root
}

func copyDockerFixtureFile(t *testing.T, sourcePath string, destinationPath string) {
	t.Helper()
	source, err := os.Open(sourcePath)
	if err != nil {
		t.Fatalf("open Docker fixture source %q: %v", sourcePath, err)
	}
	defer source.Close()
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o700); err != nil {
		t.Fatalf("create Docker fixture destination directory: %v", err)
	}
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("create Docker fixture destination %q: %v", destinationPath, err)
	}
	_, copyErr := io.Copy(destination, source)
	syncErr := destination.Sync()
	closeErr := destination.Close()
	if copyErr != nil {
		t.Fatalf("copy Docker fixture %q to %q: %v", sourcePath, destinationPath, copyErr)
	}
	if syncErr != nil {
		t.Fatalf("sync Docker fixture copy %q: %v", destinationPath, syncErr)
	}
	if closeErr != nil {
		t.Fatalf("close Docker fixture copy %q: %v", destinationPath, closeErr)
	}
}

func prepareDockerFixtureStore(t *testing.T, fixtureRoot string) (*teamstore.Store, string) {
	t.Helper()
	runtimeRoot := dockerTeamsFixtureRuntimeRoot(t)
	stateRoot := filepath.Join(runtimeRoot, "state")
	statePath := filepath.Join(stateRoot, "state.json")
	copyDockerFixtureFile(t, filepath.Join(fixtureRoot, "teams", "state.json"), statePath)
	copyDockerFixtureFile(t, filepath.Join(fixtureRoot, "teams", "store.sqlite"), filepath.Join(stateRoot, "store.sqlite"))
	for _, name := range []string{"global-inbound-ledger", "global-outbound-ledger"} {
		for _, suffix := range []string{".json", ".sqlite"} {
			sourcePath := filepath.Join(fixtureRoot, "teams", name+suffix)
			if _, err := os.Stat(sourcePath); err == nil {
				copyDockerFixtureFile(t, sourcePath, filepath.Join(stateRoot, "teams-"+name+suffix))
			} else if !os.IsNotExist(err) {
				t.Fatalf("stat optional Docker fixture shared ledger %q: %v", sourcePath, err)
			}
		}
	}
	store, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatalf("open copied Docker fixture store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close copied Docker fixture store: %v", err)
		}
	})
	return store, statePath
}

func dockerFixtureSourcePath(fixtureRoot string, persistedPath string) string {
	persistedPath = strings.TrimSpace(persistedPath)
	if strings.HasPrefix(persistedPath, dockerFixtureCodexDir) {
		if os.Getenv(dockerCodexMountEnv) == "1" {
			return persistedPath
		}
		return filepath.Join(fixtureRoot, "codex", strings.TrimPrefix(persistedPath, dockerFixtureCodexDir))
	}
	return persistedPath
}

func dockerFixtureHistoryLag(t *testing.T, fixtureRoot string, state teamstore.State) (int, int64, int64) {
	t.Helper()
	threadIDs := make(map[string]struct{}, len(state.Sessions))
	for _, session := range state.Sessions {
		if threadID := strings.TrimSpace(session.CodexThreadID); threadID != "" {
			threadIDs[threadID] = struct{}{}
		}
	}
	paths := make(map[string]struct{})
	missing := 0
	ahead := 0
	var extraBytes int64
	var maxExtraBytes int64
	for _, checkpoint := range state.HistoryWatch {
		if _, ok := threadIDs[strings.TrimSpace(checkpoint.ThreadID)]; !ok {
			continue
		}
		filePath := dockerFixtureSourcePath(fixtureRoot, checkpoint.Path)
		if strings.TrimSpace(filePath) == "" {
			continue
		}
		if _, ok := paths[filePath]; ok {
			continue
		}
		paths[filePath] = struct{}{}
		info, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				missing++
				continue
			}
			t.Fatalf("stat copied Codex history %q: %v", filePath, err)
		}
		if info.IsDir() {
			t.Fatalf("copied Codex history path is a directory: %q", filePath)
		}
		if checkpoint.Offset < 0 {
			t.Fatalf("negative durable Codex history offset for %q: %d", filePath, checkpoint.Offset)
		}
		if extra := info.Size() - checkpoint.Offset; extra > 0 {
			ahead++
			extraBytes += extra
			if extra > maxExtraBytes {
				maxExtraBytes = extra
			}
		}
	}
	t.Logf("copied Teams-linked Codex history: total_history_watch_rows=%d unique_files=%d missing=%d ahead=%d extra_bytes=%d max_extra_bytes=%d tail_budget=%d", len(state.HistoryWatch), len(paths), missing, ahead, extraBytes, maxExtraBytes, historyTieredMaxTailBytes)
	if len(paths) == 0 {
		t.Fatal("copied fixture contains no Codex history files linked to Teams sessions")
	}
	if missing != 0 {
		t.Fatalf("copied fixture is incomplete: %d Teams-linked Codex history files are missing", missing)
	}
	if ahead == 0 || extraBytes == 0 {
		t.Fatal("copied fixture no longer demonstrates the observed SQLite cursor lag")
	}
	if maxExtraBytes <= historyTieredMaxTailBytes {
		t.Fatalf("maximum Codex cursor lag = %d bytes, want more than one bounded tail pass (%d bytes)", maxExtraBytes, historyTieredMaxTailBytes)
	}
	return ahead, extraBytes, maxExtraBytes
}

func dockerFixtureChatIDFromMessagesPath(path string) (string, bool) {
	const prefix = "/chats/"
	const suffix = "/messages"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return "", false
	}
	encoded := strings.TrimSuffix(strings.TrimPrefix(path, prefix), suffix)
	if encoded == "" {
		return "", false
	}
	chatID, err := url.PathUnescape(encoded)
	if err != nil || strings.TrimSpace(chatID) == "" {
		return "", false
	}
	return chatID, true
}
