package skills

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestManagerAddAuthFailureRecordsAuthRequiredAndScrubsSecrets(t *testing.T) {
	mgr := newTestManager(t)
	mgr.Git = authFailGitRunner{}
	ctx := context.Background()

	source, _, err := mgr.Add(ctx, "https://token:secret@github.com/acme/private/tree/main/skills", AddOptions{Name: "private"})
	if err == nil {
		t.Fatal("add auth failure succeeded, want error")
	}
	text := err.Error()
	if !strings.Contains(text, "Authentication hint: run `gh auth login`") {
		t.Fatalf("auth hint missing:\n%s", text)
	}
	if strings.Contains(text, "token:secret") {
		t.Fatalf("auth error leaked token:\n%s", text)
	}
	st, err := mgr.Store.LoadState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	state, ok := sourceStateByID(st, source.ID)
	if !ok {
		t.Fatalf("missing state for %s", source.ID)
	}
	if state.Status != StatusAuthRequired {
		t.Fatalf("status = %q, want %q", state.Status, StatusAuthRequired)
	}
	if strings.Contains(state.LastError, "token:secret") {
		t.Fatalf("state leaked token:\n%s", state.LastError)
	}
}

func TestManagerSyncRefusesToOverwriteLocalChanges(t *testing.T) {
	repo := initSkillRepo(t)
	mgr := newTestManager(t)
	ctx := context.Background()

	_, result, err := mgr.Add(ctx, repo, AddOptions{Name: "acme", Ref: "HEAD"})
	if err != nil {
		t.Fatalf("add source: %v", err)
	}
	if len(result.Installed) != 1 {
		t.Fatalf("installed len = %d, want 1", len(result.Installed))
	}
	targetSkill := result.Installed[0].TargetPath
	localBody := "---\nname: review\ndescription: Local edit\n---\nlocal body\n"
	writeFile(t, filepath.Join(targetSkill, "SKILL.md"), localBody, 0o644)

	writeFile(t, filepath.Join(repo, "SKILL.md"), "---\nname: review\ndescription: Remote edit\n---\nremote body\n", 0o644)
	gitCommitAll(t, repo, "remote edit")

	results, err := mgr.Sync(ctx, SyncOptions{All: true})
	if err == nil {
		t.Fatal("sync with local changes succeeded, want error")
	}
	if len(results) != 1 || results[0].State.Status != StatusModified {
		t.Fatalf("sync status = %#v, want local_modified", results)
	}
	got := readFile(t, filepath.Join(targetSkill, "SKILL.md"))
	if got != localBody {
		t.Fatalf("local edit was overwritten:\n%s", got)
	}
	entries, err := mgr.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 || entries[0].State.Status != StatusModified {
		t.Fatalf("list status = %#v, want local_modified", entries)
	}
}

func TestNewManagerDoesNotResolveMissingHomesToDot(t *testing.T) {
	t.Setenv("HOME", "")
	t.Setenv("USERPROFILE", "")
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	if runtime.GOOS == "plan9" {
		t.Setenv("home", "")
	}
	mgr, err := NewManager(ManagerOptions{
		ConfigDir: filepath.Join(t.TempDir(), "config"),
		CacheDir:  filepath.Join(t.TempDir(), "cache"),
		HomeDir:   " ",
		CodexDir:  " ",
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if mgr.HomeDir == "." || mgr.CodexDir == "." {
		t.Fatalf("manager resolved empty paths to dot: home=%q codex=%q", mgr.HomeDir, mgr.CodexDir)
	}
	if _, err := mgr.TargetRoot(TargetCodexHome); err == nil {
		t.Fatal("TargetRoot(codex-home) with unresolved codex dir succeeded, want error")
	}
	if _, err := mgr.TargetRoot(TargetAgents); err == nil {
		t.Fatal("TargetRoot(agents) with unresolved home dir succeeded, want error")
	}
}

func TestManagerSyncAllKeepsSuccessfulSourceWhenAnotherSourceFails(t *testing.T) {
	goodRepo := initSkillRepo(t)
	mgr := newTestManager(t)
	ctx := context.Background()

	good, result, err := mgr.Add(ctx, goodRepo, AddOptions{Name: "good", Ref: "HEAD"})
	if err != nil {
		t.Fatalf("add good source: %v", err)
	}
	if len(result.Installed) != 1 {
		t.Fatalf("good installed len = %d, want 1", len(result.Installed))
	}
	bad := Source{
		ID:         stableID("bad", "not-a-repo"),
		Name:       "bad",
		URL:        filepath.Join(t.TempDir(), "missing"),
		RemoteURL:  filepath.Join(t.TempDir(), "missing"),
		Ref:        "HEAD",
		TargetKind: TargetCodexHome,
		TargetRoot: filepath.Join(mgr.CodexDir, "skills"),
		AutoSync:   true,
		AddedAt:    nowUTC(),
		UpdatedAt:  nowUTC(),
	}
	if err := mgr.Store.Update(func(cfg *Config, st *State) error {
		cfg.Sources = append(cfg.Sources, bad)
		return nil
	}); err != nil {
		t.Fatalf("seed bad source: %v", err)
	}

	results, err := mgr.Sync(ctx, SyncOptions{All: true})
	if err == nil {
		t.Fatal("sync all succeeded, want aggregate failure")
	}
	if len(results) != 2 {
		t.Fatalf("results len = %d, want 2", len(results))
	}
	seenGood := false
	seenBad := false
	for _, res := range results {
		switch res.Source.ID {
		case good.ID:
			seenGood = true
			if res.Error != nil || len(res.Installed) != 1 {
				t.Fatalf("good result = %#v", res)
			}
		case bad.ID:
			seenBad = true
			if res.Error == nil || res.State.Status == StatusReady {
				t.Fatalf("bad result = %#v", res)
			}
		}
	}
	if !seenGood || !seenBad {
		t.Fatalf("results missing good/bad: %#v", results)
	}
	if _, err := os.Stat(result.Installed[0].TargetPath); err != nil {
		t.Fatalf("good installed target missing after failed sync all: %v", err)
	}
}

func TestManagerSyncAllRunsSourcesInParallel(t *testing.T) {
	mgr := newTestManager(t)
	targetRoot := filepath.Join(mgr.CodexDir, "skills")
	runner := &blockingFetchGitRunner{
		fetchStarted: make(chan string, 2),
		releaseFetch: make(chan struct{}),
	}
	mgr.Git = runner
	now := nowUTC()
	sources := []Source{
		{
			ID:         stableID("one"),
			Name:       "one",
			URL:        "one",
			RemoteURL:  "one",
			Ref:        "main",
			TargetKind: TargetCodexHome,
			TargetRoot: targetRoot,
			AutoSync:   true,
			AddedAt:    now,
			UpdatedAt:  now,
		},
		{
			ID:         stableID("two"),
			Name:       "two",
			URL:        "two",
			RemoteURL:  "two",
			Ref:        "main",
			TargetKind: TargetCodexHome,
			TargetRoot: targetRoot,
			AutoSync:   true,
			AddedAt:    now,
			UpdatedAt:  now,
		},
	}
	if err := mgr.Store.Update(func(cfg *Config, st *State) error {
		cfg.Sources = sources
		st.Sources = nil
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), skillsTestTimeout(5*time.Second))
	defer cancel()
	done := make(chan error, 1)
	go func() {
		results, err := mgr.Sync(ctx, SyncOptions{All: true})
		if len(results) != 2 {
			done <- fmt.Errorf("results len = %d, want 2", len(results))
			return
		}
		done <- err
	}()

	waitFetchesStarted(t, runner.fetchStarted, "one", "two")
	close(runner.releaseFetch)
	if err := <-done; err != nil {
		t.Fatalf("sync all: %v", err)
	}
}

func TestManagerNormalizesLegacyGitLabSSHRemoteForGitOperations(t *testing.T) {
	mgr := newTestManager(t)
	runner := &recordingFetchGitRunner{}
	mgr.Git = runner
	source := Source{
		ID:         "legacy-gitlab",
		Name:       "legacy",
		RemoteURL:  "ssh://gitlab-master.nvidia.com:12051/jawei/fgx_tin_skill.git",
		Ref:        "main",
		TargetKind: TargetCodexHome,
		TargetRoot: filepath.Join(mgr.CodexDir, "skills"),
		AutoSync:   true,
		AddedAt:    nowUTC(),
		UpdatedAt:  nowUTC(),
	}
	if err := mgr.Store.Update(func(cfg *Config, st *State) error {
		cfg.Sources = []Source{source}
		st.Sources = nil
		return nil
	}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	results, err := mgr.Sync(context.Background(), SyncOptions{Name: "legacy"})
	if err != nil {
		t.Fatalf("sync legacy source: %v", err)
	}
	wantRemote := "ssh://git@gitlab-master.nvidia.com:12051/jawei/fgx_tin_skill.git"
	if runner.fetchRemote != wantRemote {
		t.Fatalf("fetch remote = %q, want %q", runner.fetchRemote, wantRemote)
	}
	if len(results) != 1 || results[0].Source.RemoteURL != wantRemote {
		t.Fatalf("result source = %#v, want normalized remote", results)
	}
	if results[0].Source.Provider != "gitlab" {
		t.Fatalf("result provider = %q, want gitlab", results[0].Source.Provider)
	}
	entries, err := mgr.List(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 || entries[0].Source.RemoteURL != wantRemote {
		t.Fatalf("list source = %#v, want normalized remote", entries)
	}
}

func TestStartDailyAutoSyncSkipsDisabledAndAlreadySyncedSources(t *testing.T) {
	mgr := newTestManager(t)
	mgr.Git = panicGitRunner{}
	targetRoot := filepath.Join(mgr.CodexDir, "skills")
	disabled := Source{
		ID:         stableID("disabled"),
		Name:       "disabled",
		RemoteURL:  "disabled",
		Ref:        "main",
		TargetKind: TargetCodexHome,
		TargetRoot: targetRoot,
		AutoSync:   false,
		AddedAt:    nowUTC(),
		UpdatedAt:  nowUTC(),
	}
	doneToday := Source{
		ID:         stableID("done"),
		Name:       "done",
		RemoteURL:  "done",
		Ref:        "main",
		TargetKind: TargetCodexHome,
		TargetRoot: targetRoot,
		AutoSync:   true,
		AddedAt:    nowUTC(),
		UpdatedAt:  nowUTC(),
	}
	if err := mgr.Store.Update(func(cfg *Config, st *State) error {
		cfg.Sources = []Source{disabled, doneToday}
		st.Sources = []SourceState{{ID: doneToday.ID, Status: StatusReady, LastAutoSyncDay: todayString()}}
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	mgr.StartDailyAutoSync(context.Background())
}

func TestStartDailyAutoSyncReturnsBeforeGitCompletesAndMarksToday(t *testing.T) {
	mgr := newTestManager(t)
	targetRoot := filepath.Join(mgr.CodexDir, "skills")
	runner := &blockingFetchGitRunner{
		fetchStarted: make(chan string, 1),
		releaseFetch: make(chan struct{}),
	}
	mgr.Git = runner
	source := Source{
		ID:         stableID("auto"),
		Name:       "auto",
		URL:        "auto",
		RemoteURL:  "auto",
		Ref:        "main",
		TargetKind: TargetCodexHome,
		TargetRoot: targetRoot,
		AutoSync:   true,
		AddedAt:    nowUTC(),
		UpdatedAt:  nowUTC(),
	}
	if err := mgr.Store.Update(func(cfg *Config, st *State) error {
		cfg.Sources = []Source{source}
		st.Sources = nil
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), skillsTestTimeout(5*time.Second))
	defer cancel()
	start := time.Now()
	mgr.StartDailyAutoSync(ctx)
	if time.Since(start) > 250*time.Millisecond {
		t.Fatalf("StartDailyAutoSync blocked for %s", time.Since(start))
	}
	waitFetchStarted(t, runner.fetchStarted, "auto")
	close(runner.releaseFetch)

	deadline := time.Now().Add(skillsTestTimeout(2 * time.Second))
	for {
		st, err := mgr.Store.LoadState()
		if err != nil {
			t.Fatalf("load state: %v", err)
		}
		state, _ := sourceStateByID(st, source.ID)
		if state.LastAutoSyncDay == todayString() && state.Status == StatusReady {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("auto-sync state = %#v, want ready with today's auto-sync day", state)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPublishSkillsRefusesLocalChangesBeforeReplacing(t *testing.T) {
	targetRoot := t.TempDir()
	source := Source{ID: "source-1", Name: "source", RemoteURL: "repo"}
	initial := []skillTree{{
		Name:       "review",
		SourceDir:  "skills/review",
		ExportName: "source__review",
		Files: []treeFile{{
			RepoPath: "skills/review/SKILL.md",
			RelPath:  "SKILL.md",
			Mode:     "100644",
			Data:     []byte("---\nname: review\ndescription: Review code\n---\nbody\n"),
		}},
	}}
	installed, err := publishSkills(targetRoot, source, "commit-one", initial)
	if err != nil {
		t.Fatalf("publish initial: %v", err)
	}
	localBody := "---\nname: review\ndescription: Local edit\n---\nlocal body\n"
	writeFile(t, filepath.Join(installed[0].TargetPath, "SKILL.md"), localBody, 0o644)

	replacement := []skillTree{{
		Name:       "review",
		SourceDir:  "skills/review",
		ExportName: "source__review",
		Files: []treeFile{{
			RepoPath: "skills/review/SKILL.md",
			RelPath:  "SKILL.md",
			Mode:     "100644",
			Data:     []byte("---\nname: review\ndescription: Remote edit\n---\nremote body\n"),
		}},
	}}
	if _, err := publishSkills(targetRoot, source, "commit-two", replacement); err == nil {
		t.Fatal("publish with local changes succeeded, want error")
	}
	got := readFile(t, filepath.Join(installed[0].TargetPath, "SKILL.md"))
	if got != localBody {
		t.Fatalf("local edit was overwritten:\n%s", got)
	}
}

type authFailGitRunner struct{}

func (authFailGitRunner) Run(ctx context.Context, dir string, _ []string, args ...string) ([]byte, error) {
	switch {
	case len(args) >= 1 && args[0] == "ls-remote":
		return nil, &GitError{Args: args, Output: "fatal: Authentication failed for 'https://token:secret@github.com/acme/private.git/'", Err: fmt.Errorf("exit status 128")}
	case len(args) >= 3 && args[0] == "init" && args[1] == "--bare":
		if err := os.MkdirAll(args[2], 0o700); err != nil {
			return nil, err
		}
		return nil, os.WriteFile(filepath.Join(args[2], "HEAD"), []byte("ref: refs/heads/main\n"), 0o600)
	case len(args) >= 1 && args[0] == "config":
		return nil, nil
	case len(args) >= 1 && args[0] == "fetch":
		return nil, &GitError{Args: args, Output: "fatal: Authentication failed for 'https://token:secret@github.com/acme/private.git/'", Err: fmt.Errorf("exit status 128")}
	default:
		return nil, fmt.Errorf("unexpected git args in %s: %v", dir, args)
	}
}

type panicGitRunner struct{}

func (panicGitRunner) Run(_ context.Context, dir string, _ []string, args ...string) ([]byte, error) {
	panic(fmt.Sprintf("unexpected git call in %s: %v", dir, args))
}

type blockingFetchGitRunner struct {
	fetchStarted chan string
	releaseFetch chan struct{}
}

type recordingFetchGitRunner struct {
	fetchRemote string
}

func (r *recordingFetchGitRunner) Run(_ context.Context, dir string, _ []string, args ...string) ([]byte, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("empty git args")
	}
	switch args[0] {
	case "init":
		if len(args) < 3 || args[1] != "--bare" {
			return nil, fmt.Errorf("unexpected init args: %v", args)
		}
		if err := os.MkdirAll(args[2], 0o700); err != nil {
			return nil, err
		}
		return nil, os.WriteFile(filepath.Join(args[2], "HEAD"), []byte("ref: refs/heads/main\n"), 0o600)
	case "config":
		return nil, nil
	case "fetch":
		if len(args) < 6 {
			return nil, fmt.Errorf("unexpected fetch args: %v", args)
		}
		r.fetchRemote = args[4]
		return nil, nil
	case "rev-parse":
		return []byte("0123456789abcdef0123456789abcdef01234567\n"), nil
	case "ls-tree":
		return []byte("100644 blob skilloid\tSKILL.md\x00"), nil
	case "cat-file":
		return []byte("---\nname: review\ndescription: Review code\n---\nbody\n"), nil
	default:
		return nil, fmt.Errorf("unexpected git args in %s: %v", dir, args)
	}
}

func (r *blockingFetchGitRunner) Run(ctx context.Context, dir string, _ []string, args ...string) ([]byte, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("empty git args")
	}
	switch args[0] {
	case "init":
		if len(args) < 3 || args[1] != "--bare" {
			return nil, fmt.Errorf("unexpected init args: %v", args)
		}
		if err := os.MkdirAll(args[2], 0o700); err != nil {
			return nil, err
		}
		return nil, os.WriteFile(filepath.Join(args[2], "HEAD"), []byte("ref: refs/heads/main\n"), 0o600)
	case "config":
		return nil, nil
	case "fetch":
		if len(args) < 6 {
			return nil, fmt.Errorf("unexpected fetch args: %v", args)
		}
		select {
		case r.fetchStarted <- args[4]:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-r.releaseFetch:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case "rev-parse":
		return []byte("0123456789abcdef0123456789abcdef01234567\n"), nil
	case "ls-tree":
		return []byte("100644 blob skilloid\tSKILL.md\x00"), nil
	case "cat-file":
		return []byte("---\nname: review\ndescription: Review code\n---\nbody\n"), nil
	default:
		return nil, fmt.Errorf("unexpected git args in %s: %v", dir, args)
	}
}

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	root := t.TempDir()
	mgr, err := NewManager(ManagerOptions{
		ConfigDir: filepath.Join(root, "config"),
		CacheDir:  filepath.Join(root, "cache"),
		CodexDir:  filepath.Join(root, "codex"),
		HomeDir:   filepath.Join(root, "home"),
		Out:       io.Discard,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mgr
}

func initSkillRepo(t *testing.T) string {
	t.Helper()
	requireGit(t)
	repo := t.TempDir()
	gitRun(t, repo, "init")
	gitRun(t, repo, "config", "user.name", "Skill Test")
	gitRun(t, repo, "config", "user.email", "skill-test@example.invalid")
	writeFile(t, filepath.Join(repo, "SKILL.md"), "---\nname: review\ndescription: Review code\n---\nbody\n", 0o644)
	writeFile(t, filepath.Join(repo, "scripts", "check.sh"), "#!/bin/sh\necho ok\n", 0o755)
	gitCommitAll(t, repo, "initial skill")
	return repo
}

func gitCommitAll(t *testing.T, repo string, message string) {
	t.Helper()
	gitRun(t, repo, "add", "-A")
	gitRun(t, repo, "commit", "-m", message)
}

func gitRun(t *testing.T, repo string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = repo
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, string(out))
	}
	return string(out)
}

func requireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
}

func writeFile(t *testing.T, path string, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func waitFetchStarted(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("fetch source = %q, want %q", got, want)
		}
	case <-time.After(skillsTestTimeout(2 * time.Second)):
		t.Fatalf("timed out waiting for fetch %q", want)
	}
}

func waitFetchesStarted(t *testing.T, ch <-chan string, want ...string) {
	t.Helper()
	remaining := map[string]bool{}
	for _, name := range want {
		remaining[name] = true
	}
	deadline := time.After(skillsTestTimeout(2 * time.Second))
	for len(remaining) > 0 {
		select {
		case got := <-ch:
			if !remaining[got] {
				t.Fatalf("unexpected fetch source %q, remaining %v", got, remaining)
			}
			delete(remaining, got)
		case <-deadline:
			t.Fatalf("timed out waiting for fetches %v", remaining)
		}
	}
}

func skillsTestTimeout(base time.Duration) time.Duration {
	if runtime.GOOS == "windows" {
		return 30 * time.Second
	}
	return base
}
