package teams

import (
	"context"
	"fmt"
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
	dockerRuntimeReuseEnv = "CXP_TEAMS_DOCKER_RUNTIME_REUSE"
	dockerCodexMountEnv   = "CXP_TEAMS_DOCKER_CODEX_MOUNTED"
	dockerCodexSourceEnv  = "CXP_TEAMS_DOCKER_CODEX_SOURCE_PREFIX"
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
		path := filepath.Join(root, filepath.FromSlash(name))
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			t.Fatalf("Docker fixture file %q: %v", name, err)
		}
		if err != nil {
			t.Fatalf("Docker fixture file %q: %v", name, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			t.Fatalf("Docker fixture file %q is not a regular file", name)
		}
	}
	sessionsPath := filepath.Join(root, "codex", "sessions")
	sessionsInfo, err := os.Lstat(sessionsPath)
	if err != nil {
		t.Fatalf("Docker fixture sessions directory: %v", err)
	}
	if sessionsInfo.Mode()&os.ModeSymlink != 0 || !sessionsInfo.IsDir() {
		t.Fatalf("Docker fixture sessions path is not a real directory: %q", sessionsPath)
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
	info, err := os.Lstat(sourcePath)
	if err != nil {
		t.Fatalf("stat Docker fixture source %q: %v", sourcePath, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		t.Fatalf("Docker fixture source is not a regular file: %q", sourcePath)
	}
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

func validateDockerFixtureRuntime(t *testing.T, runtimeRoot string) {
	t.Helper()
	stateRoot := filepath.Join(runtimeRoot, "state")
	for _, name := range []string{"state.json", "store.sqlite", "registry.json"} {
		path := filepath.Join(stateRoot, name)
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatalf("reused Docker fixture runtime file %q: %v", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			t.Fatalf("reused Docker fixture runtime file is not a regular file: %q", path)
		}
	}
	err := filepath.Walk(runtimeRoot, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("reused Docker fixture runtime contains a symlink: %q", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate reused Docker fixture runtime: %v", err)
	}
}

func prepareDockerFixtureStore(t *testing.T, fixtureRoot string) (*teamstore.Store, string) {
	t.Helper()
	runtimeRoot := dockerTeamsFixtureRuntimeRoot(t)
	stateRoot := filepath.Join(runtimeRoot, "state")
	statePath := filepath.Join(stateRoot, "state.json")
	if strings.TrimSpace(os.Getenv(dockerRuntimeReuseEnv)) == "1" {
		validateDockerFixtureRuntime(t, runtimeRoot)
	} else {
		copyDockerFixtureFile(t, filepath.Join(fixtureRoot, "teams", "state.json"), statePath)
		copyDockerFixtureFile(t, filepath.Join(fixtureRoot, "teams", "store.sqlite"), filepath.Join(stateRoot, "store.sqlite"))
		// The listener reloads the registry projection at every disposable restart.
		// Keep it beside the copied state rather than relying on the source fixture
		// path, which is read-only and is not mounted at the runtime location.
		copyDockerFixtureFile(t, filepath.Join(fixtureRoot, "teams", "registry.json"), filepath.Join(stateRoot, "registry.json"))
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
		for _, name := range []string{
			"control-chat-history.jsonl",
			"control-chat-history.sqlite",
			"control-chat-history.sqlite-wal",
			"control-chat-history.sqlite-shm",
			"helper-restart-pending.json",
			"workflow-notifications.json",
		} {
			sourcePath := filepath.Join(fixtureRoot, "teams", name)
			info, err := os.Lstat(sourcePath)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				t.Fatalf("stat optional Docker fixture sidecar %q: %v", sourcePath, err)
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				t.Fatalf("optional Docker fixture sidecar is not a regular file: %q", sourcePath)
			}
			copyDockerFixtureFile(t, sourcePath, filepath.Join(stateRoot, name))
		}
		threadLinksRoot := filepath.Join(fixtureRoot, "teams", "thread-links")
		if _, err := os.Lstat(threadLinksRoot); err == nil {
			err = filepath.Walk(threadLinksRoot, func(sourcePath string, info os.FileInfo, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if info.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("Docker fixture thread-link tree contains a symlink: %q", sourcePath)
				}
				if info.IsDir() || filepath.Ext(info.Name()) != ".jsonl" {
					return nil
				}
				relative, relativeErr := filepath.Rel(threadLinksRoot, sourcePath)
				if relativeErr != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
					return fmt.Errorf("Docker fixture thread-link path escapes its root: %q", sourcePath)
				}
				copyDockerFixtureFile(t, sourcePath, filepath.Join(stateRoot, "thread-links", relative))
				return nil
			})
			if err != nil {
				t.Fatalf("copy optional Docker fixture thread-links: %v", err)
			}
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat optional Docker fixture thread-links: %v", err)
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

// dockerFixtureRemapCodexPaths rewrites only the copied fixture's persisted
// Codex-home/source paths.  The live state deliberately keeps the host path;
// the container has no access to that path and must never be given the whole
// host Codex home merely to make a replay pass.  Source paths that claim to be
// under the configured Codex home but escape the copied sessions tree fail
// closed instead of being silently dropped or redirected to a sibling.
func dockerFixtureRemapCodexPaths(t *testing.T, store *teamstore.Store) {
	t.Helper()
	if store == nil {
		t.Fatal("cannot remap Codex paths on a nil Docker fixture store")
	}
	sourcePrefix := strings.TrimSpace(os.Getenv(dockerCodexSourceEnv))
	if sourcePrefix == "" {
		sourcePrefix = dockerFixtureCodexDir
	}
	canonicalHome := strings.TrimSuffix(filepath.Clean(filepath.FromSlash(dockerFixtureCodexDir)), string(filepath.Separator))
	mapPath := func(path string, allowHome bool) (string, error) {
		path = strings.TrimSpace(path)
		if path == "" {
			return "", nil
		}
		sourceRoot := filepath.Clean(filepath.FromSlash(sourcePrefix))
		candidate := filepath.Clean(filepath.FromSlash(path))
		if candidate == sourceRoot {
			if allowHome {
				return canonicalHome, nil
			}
			return "", fmt.Errorf("Codex source path is the home directory, not a copied session: %q", path)
		}
		relative, err := filepath.Rel(sourceRoot, candidate)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || !filepath.IsAbs(candidate) {
			return "", fmt.Errorf("Codex source path is outside the configured Codex home: %q", path)
		}
		sessionsRoot := filepath.FromSlash("sessions")
		if relative != sessionsRoot && !strings.HasPrefix(relative, sessionsRoot+string(filepath.Separator)) {
			return "", fmt.Errorf("Codex source path is outside the copied sessions tree: %q", path)
		}
		return filepath.Join(canonicalHome, relative), nil
	}
	mapRequired := func(label string, value *string, allowHome bool) error {
		mapped, err := mapPath(*value, allowHome)
		if err != nil {
			return fmt.Errorf("remap %s: %w", label, err)
		}
		*value = mapped
		return nil
	}
	mapOptionalCodexPath := func(label string, value *string) error {
		if strings.TrimSpace(*value) == "" {
			return nil
		}
		candidate := filepath.Clean(filepath.FromSlash(strings.TrimSpace(*value)))
		sourceRoot := filepath.Clean(filepath.FromSlash(sourcePrefix))
		relative, err := filepath.Rel(sourceRoot, candidate)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			// Attachment/artifact paths normally belong to the Teams outbox tree,
			// not Codex sessions. Leave those paths alone; only a path that is
			// actually under Codex is subject to the strict copied-tree check.
			return nil
		}
		mapped, err := mapPath(*value, false)
		if err != nil {
			return fmt.Errorf("remap %s: %w", label, err)
		}
		*value = mapped
		return nil
	}

	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.Scope.CodexHome = canonicalHome
		// No copied fixture may read a real config or webhook secret. Graph and
		// execution are replaced by the test boundary; workflow notifications are
		// therefore disabled in the disposable state as an explicit safety fence.
		state.Scope.ConfigPath = ""
		state.Workflow.Enabled = false
		state.Workflow.ControlWebhookURLFile = ""
		for id, session := range state.Sessions {
			if err := mapRequired("session "+id+" CodexHome", &session.CodexHome, true); err != nil {
				return err
			}
			// A worktree is not part of the fixture mount. Clear the source
			// workspace instead of leaving an absolute host path that a future
			// executor or attachment helper could accidentally open.
			session.Cwd = ""
			state.Sessions[id] = session
		}
		for id, workspace := range state.Workspaces {
			workspace.Path = ""
			state.Workspaces[id] = workspace
		}
		for id, checkpoint := range state.ImportCheckpoints {
			if err := mapRequired("import checkpoint "+id+" source", &checkpoint.SourcePath, false); err != nil {
				return err
			}
			if checkpoint.PendingHistoryRange != nil {
				if err := mapRequired("import checkpoint "+id+" pending range", &checkpoint.PendingHistoryRange.SourcePath, false); err != nil {
					return err
				}
			}
			if checkpoint.TranscriptQuarantine != nil {
				if err := mapRequired("import checkpoint "+id+" quarantine", &checkpoint.TranscriptQuarantine.SourcePath, false); err != nil {
					return err
				}
			}
			if checkpoint.UnresolvedExecution != nil {
				if err := mapRequired("import checkpoint "+id+" execution anchor", &checkpoint.UnresolvedExecution.SourcePath, false); err != nil {
					return err
				}
			}
			state.ImportCheckpoints[id] = checkpoint
		}
		for id, checkpoint := range state.HistoryWatch {
			if err := mapRequired("history checkpoint "+id, &checkpoint.Path, false); err != nil {
				return err
			}
			if checkpoint.PendingHistoryRange != nil {
				if err := mapRequired("history checkpoint "+id+" pending range", &checkpoint.PendingHistoryRange.SourcePath, false); err != nil {
					return err
				}
			}
			if checkpoint.TranscriptQuarantine != nil {
				if err := mapRequired("history checkpoint "+id+" quarantine", &checkpoint.TranscriptQuarantine.SourcePath, false); err != nil {
					return err
				}
			}
			state.HistoryWatch[id] = checkpoint
		}
		for id, outbox := range state.OutboxMessages {
			if err := mapOptionalCodexPath("outbox "+id+" transcript source", &outbox.TranscriptSourcePath); err != nil {
				return err
			}
			if err := mapOptionalCodexPath("outbox "+id+" attachment", &outbox.AttachmentPath); err != nil {
				return err
			}
			state.OutboxMessages[id] = outbox
		}
		for id, record := range state.TranscriptLedger {
			if err := mapRequired("transcript ledger "+id, &record.SourcePath, false); err != nil {
				return err
			}
			state.TranscriptLedger[id] = record
		}
		for id, record := range state.TranscriptDeliveries {
			if err := mapRequired("transcript delivery "+id, &record.SourcePath, false); err != nil {
				return err
			}
			state.TranscriptDeliveries[id] = record
		}
		for id, record := range state.ArtifactRecords {
			if err := mapOptionalCodexPath("artifact "+id, &record.Path); err != nil {
				return err
			}
			state.ArtifactRecords[id] = record
		}
		return nil
	}); err != nil {
		t.Fatalf("remap copied Docker fixture Codex paths: %v", err)
	}
}

func dockerFixtureSanitizeRegistryWorkspacePaths(registry *Registry) {
	if registry == nil {
		return
	}
	for i := range registry.Sessions {
		registry.Sessions[i].Cwd = ""
	}
}

func dockerFixtureSourcePath(fixtureRoot string, persistedPath string) string {
	persistedPath = strings.TrimSpace(persistedPath)
	if persistedPath == "" {
		return ""
	}
	sourcePrefix := strings.TrimSpace(os.Getenv(dockerCodexSourceEnv))
	if sourcePrefix == "" {
		sourcePrefix = dockerFixtureCodexDir
	}
	if !strings.HasSuffix(sourcePrefix, string(filepath.Separator)) {
		sourcePrefix += string(filepath.Separator)
	}
	prefixes := []string{sourcePrefix}
	canonicalPrefix := filepath.FromSlash(dockerFixtureCodexDir)
	if canonicalPrefix != sourcePrefix {
		prefixes = append(prefixes, canonicalPrefix)
	}
	for _, prefix := range prefixes {
		if !strings.HasPrefix(persistedPath, prefix) {
			continue
		}
		relative := filepath.Clean(filepath.FromSlash(strings.TrimPrefix(persistedPath, prefix)))
		sessionsRoot := filepath.FromSlash("sessions")
		if relative != sessionsRoot && !strings.HasPrefix(relative, sessionsRoot+string(filepath.Separator)) {
			// The persisted path is inside the Codex home prefix but not inside
			// the copied sessions tree. Never let a fixture test fall back to
			// auth/config data or a sibling directory.
			return ""
		}
		if os.Getenv(dockerCodexMountEnv) == "1" {
			return dockerFixtureContainedRegularPath(filepath.Dir(sourcePrefix), persistedPath)
		}
		return dockerFixtureContainedRegularPath(filepath.Join(fixtureRoot, "codex"), filepath.Join(fixtureRoot, "codex", relative))
	}
	if strings.TrimSpace(os.Getenv(dockerFixtureDirEnv)) != "" {
		// Docker fixture mode must fail closed for relative, missing-prefix, or
		// traversal paths. The shell runner validates the source projection too;
		// this second boundary prevents a future test helper from accidentally
		// bypassing that check.
		return ""
	}
	return persistedPath
}

func dockerFixtureContainedRegularPath(root string, candidate string) string {
	rootAbs, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return ""
	}
	candidateAbs, err := filepath.Abs(filepath.Clean(candidate))
	if err != nil {
		return ""
	}
	relative, err := filepath.Rel(rootAbs, candidateAbs)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return ""
	}
	info, err := os.Lstat(candidateAbs)
	if os.IsNotExist(err) {
		// Preserve a missing path so dockerFixtureHistoryLag can report an
		// incomplete fixture instead of silently ignoring the checkpoint.
		return candidateAbs
	}
	if err != nil || !info.Mode().IsRegular() {
		return ""
	}
	resolvedRoot, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return ""
	}
	resolvedCandidate, err := filepath.EvalSymlinks(candidateAbs)
	if err != nil {
		return ""
	}
	resolvedRelative, err := filepath.Rel(resolvedRoot, resolvedCandidate)
	if err != nil || resolvedRelative == ".." || strings.HasPrefix(resolvedRelative, ".."+string(filepath.Separator)) {
		return ""
	}
	return candidateAbs
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
			t.Fatalf("copied Codex history path cannot be mapped into the read-only sessions fixture: %q", checkpoint.Path)
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

func TestDockerFixtureSourcePathFailsClosedOutsideCopiedSessions(t *testing.T) {
	fixtureRoot := t.TempDir()
	sessionsRoot := filepath.Join(fixtureRoot, "codex", "sessions", "2026", "09", "07")
	if err := os.MkdirAll(sessionsRoot, 0o700); err != nil {
		t.Fatalf("create copied sessions root: %v", err)
	}
	valid := filepath.Join(sessionsRoot, "thread.jsonl")
	if err := os.WriteFile(valid, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write copied session: %v", err)
	}
	previousFixture, fixtureWasSet := os.LookupEnv(dockerFixtureDirEnv)
	previousPrefix, prefixWasSet := os.LookupEnv(dockerCodexSourceEnv)
	t.Cleanup(func() {
		if fixtureWasSet {
			_ = os.Setenv(dockerFixtureDirEnv, previousFixture)
		} else {
			_ = os.Unsetenv(dockerFixtureDirEnv)
		}
		if prefixWasSet {
			_ = os.Setenv(dockerCodexSourceEnv, previousPrefix)
		} else {
			_ = os.Unsetenv(dockerCodexSourceEnv)
		}
	})
	if err := os.Setenv(dockerFixtureDirEnv, fixtureRoot); err != nil {
		t.Fatalf("set fixture root: %v", err)
	}
	if err := os.Setenv(dockerCodexSourceEnv, "/source/.codex/"); err != nil {
		t.Fatalf("set source prefix: %v", err)
	}
	if got := dockerFixtureSourcePath(fixtureRoot, "/source/.codex/sessions/2026/09/07/thread.jsonl"); got != valid {
		t.Fatalf("valid copied session path = %q, want %q", got, valid)
	}
	for _, path := range []string{
		"/source/.codex/sessions-evil/thread.jsonl",
		"/source/.codex/sessions/../secrets.jsonl",
		"/other/.codex/sessions/thread.jsonl",
		"relative/sessions/thread.jsonl",
	} {
		if got := dockerFixtureSourcePath(fixtureRoot, path); got != "" {
			t.Fatalf("unsafe copied session path %q mapped to %q", path, got)
		}
	}
}
