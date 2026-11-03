package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

func TestTeamsHelperReloadSuccessBuildsReplacesAndRestarts(t *testing.T) {
	lockCLITestHooks(t)
	prevRun := teamsReloadRunCommand
	prevRestart := teamsReloadRestart
	prevNow := teamsReloadNow
	t.Cleanup(func() {
		teamsReloadRunCommand = prevRun
		teamsReloadRestart = prevRestart
		teamsReloadNow = prevNow
	})

	sourceDir := writeTeamsReloadSource(t)
	installPath := filepath.Join(t.TempDir(), "codex-proxy")
	if err := os.WriteFile(installPath, []byte("old helper"), 0o755); err != nil {
		t.Fatalf("write install binary: %v", err)
	}
	teamsReloadNow = func() time.Time { return time.Unix(100, 0) }
	var steps []string
	teamsReloadRunCommand = func(_ context.Context, _ string, _ []string, name string, args ...string) (teamsReloadCommandResult, error) {
		steps = append(steps, strings.TrimSpace(name+" "+strings.Join(args, " ")))
		if name == "go" && len(args) >= 4 && args[0] == "build" {
			outPath := args[3]
			if err := os.WriteFile(outPath, []byte("new helper"), 0o755); err != nil {
				t.Fatalf("write rebuilt helper: %v", err)
			}
		}
		return teamsReloadCommandResult{Output: "ok"}, nil
	}
	var restarted bool
	teamsReloadRestart = func(context.Context) error {
		restarted = true
		return nil
	}
	var beforeRestart bool
	err := runTeamsHelperReload(context.Background(), teamsHelperReloadOptions{
		SourceDir:   sourceDir,
		InstallPath: installPath,
		BeforeRestart: func(context.Context) error {
			beforeRestart = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("runTeamsHelperReload error: %v", err)
	}
	if !beforeRestart || !restarted {
		t.Fatalf("beforeRestart/restarted = %v/%v, want true/true", beforeRestart, restarted)
	}
	if got := readFileString(t, installPath); got != "new helper" {
		t.Fatalf("installed helper = %q, want rebuilt helper", got)
	}
	if len(steps) != 3 || !strings.HasPrefix(steps[0], "go test ") || !strings.HasPrefix(steps[1], "go build ") || !strings.Contains(steps[2], "--version") {
		t.Fatalf("reload command steps = %#v", steps)
	}
}

func TestReloadTeamsHelperFromTeamsRefusesTeamsCodexChild(t *testing.T) {
	t.Setenv(envTeamsCodexChild, "1")
	err := reloadTeamsHelperFromTeams(context.Background(), teams.HelperReloadOptions{})
	if err == nil {
		t.Fatal("reloadTeamsHelperFromTeams error = nil, want refusal")
	}
	if !strings.Contains(err.Error(), "helper reload now") || !strings.Contains(err.Error(), "launched by Teams helper") {
		t.Fatalf("reloadTeamsHelperFromTeams error = %v, want Teams child refusal", err)
	}
}

func TestTeamsHelperReloadUsesStablePathWhenRunningFromReloadBackup(t *testing.T) {
	lockCLITestHooks(t)
	prevRun := teamsReloadRunCommand
	prevRestart := teamsReloadRestart
	prevNow := teamsReloadNow
	t.Cleanup(func() {
		teamsReloadRunCommand = prevRun
		teamsReloadRestart = prevRestart
		teamsReloadNow = prevNow
	})

	sourceDir := writeTeamsReloadSource(t)
	dir := t.TempDir()
	stablePath := filepath.Join(dir, "codex-proxy-teams-dev")
	if err := os.WriteFile(stablePath, []byte("old stable helper"), 0o755); err != nil {
		t.Fatalf("write stable helper: %v", err)
	}
	runningPath := stablePath + ".reload-backup-111-222.reload-backup-111-333"
	if err := os.WriteFile(runningPath, []byte("running backup helper"), 0o755); err != nil {
		t.Fatalf("write running backup helper: %v", err)
	}
	teamsReloadNow = func() time.Time { return time.Unix(300, 0) }
	teamsReloadRunCommand = func(_ context.Context, _ string, _ []string, name string, args ...string) (teamsReloadCommandResult, error) {
		if name == "go" && len(args) >= 4 && args[0] == "build" {
			if err := os.WriteFile(args[3], []byte("new stable helper"), 0o755); err != nil {
				t.Fatalf("write rebuilt helper: %v", err)
			}
		}
		return teamsReloadCommandResult{}, nil
	}
	teamsReloadRestart = func(context.Context) error { return nil }

	if err := runTeamsHelperReload(context.Background(), teamsHelperReloadOptions{
		SourceDir:   sourceDir,
		InstallPath: runningPath + " (deleted)",
	}); err != nil {
		t.Fatalf("runTeamsHelperReload error: %v", err)
	}
	if got := readFileString(t, stablePath); got != "new stable helper" {
		t.Fatalf("stable helper after reload = %q, want new helper", got)
	}
	if got := readFileString(t, runningPath); got != "running backup helper" {
		t.Fatalf("running backup helper changed = %q", got)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".reload-backup-111-222.reload-backup-111-333.reload-backup-") {
			t.Fatalf("reload created nested backup path: %s", entry.Name())
		}
	}
}

func TestTeamsHelperReloadTestFailureDoesNotReplaceOrRestart(t *testing.T) {
	lockCLITestHooks(t)
	prevRun := teamsReloadRunCommand
	prevRestart := teamsReloadRestart
	t.Cleanup(func() {
		teamsReloadRunCommand = prevRun
		teamsReloadRestart = prevRestart
	})

	sourceDir := writeTeamsReloadSource(t)
	installPath := filepath.Join(t.TempDir(), "codex-proxy")
	if err := os.WriteFile(installPath, []byte("old helper"), 0o755); err != nil {
		t.Fatalf("write install binary: %v", err)
	}
	var steps []string
	teamsReloadRunCommand = func(_ context.Context, _ string, _ []string, name string, args ...string) (teamsReloadCommandResult, error) {
		steps = append(steps, name+" "+strings.Join(args, " "))
		return teamsReloadCommandResult{Output: "tests failed"}, errors.New("synthetic test failure")
	}
	teamsReloadRestart = func(context.Context) error {
		t.Fatal("restart should not run after test failure")
		return nil
	}

	err := runTeamsHelperReload(context.Background(), teamsHelperReloadOptions{SourceDir: sourceDir, InstallPath: installPath})
	if err == nil || !strings.Contains(err.Error(), "safety tests") {
		t.Fatalf("runTeamsHelperReload error = %v, want safety test failure", err)
	}
	if got := readFileString(t, installPath); got != "old helper" {
		t.Fatalf("install binary changed after test failure: %q", got)
	}
	if len(steps) != 1 || !strings.HasPrefix(steps[0], "go test ") {
		t.Fatalf("reload command steps = %#v, want only go test", steps)
	}
}

func TestTeamsHelperReloadRestartFailureRestoresBackup(t *testing.T) {
	lockCLITestHooks(t)
	prevRun := teamsReloadRunCommand
	prevRestart := teamsReloadRestart
	prevNow := teamsReloadNow
	t.Cleanup(func() {
		teamsReloadRunCommand = prevRun
		teamsReloadRestart = prevRestart
		teamsReloadNow = prevNow
	})

	sourceDir := writeTeamsReloadSource(t)
	installPath := filepath.Join(t.TempDir(), "codex-proxy")
	if err := os.WriteFile(installPath, []byte("old helper"), 0o755); err != nil {
		t.Fatalf("write install binary: %v", err)
	}
	teamsReloadNow = func() time.Time { return time.Unix(200, 0) }
	teamsReloadRunCommand = func(_ context.Context, _ string, _ []string, name string, args ...string) (teamsReloadCommandResult, error) {
		if name == "go" && len(args) >= 4 && args[0] == "build" {
			if err := os.WriteFile(args[3], []byte("new helper"), 0o755); err != nil {
				t.Fatalf("write rebuilt helper: %v", err)
			}
		}
		return teamsReloadCommandResult{}, nil
	}
	teamsReloadRestart = func(context.Context) error {
		return errors.New("synthetic restart failure")
	}

	err := runTeamsHelperReload(context.Background(), teamsHelperReloadOptions{SourceDir: sourceDir, InstallPath: installPath})
	if err == nil || !strings.Contains(err.Error(), "restart helper after reload") {
		t.Fatalf("runTeamsHelperReload error = %v, want restart failure", err)
	}
	if got := readFileString(t, installPath); got != "old helper" {
		t.Fatalf("install binary after rollback = %q, want old helper", got)
	}
}

func TestTeamsHelperReloadBuildEnvDropsTeamsSecretsAndProxies(t *testing.T) {
	t.Setenv("HOME", "/home/alice")
	t.Setenv("PATH", "/usr/bin")
	t.Setenv("CODEX_HELPER_TEAMS_TOKEN_CACHE", "/secret/token-cache")
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "client-id")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1234")
	t.Setenv("https_proxy", "http://proxy.example")

	env := teamsReloadBuildEnv()
	joined := "\n" + strings.Join(env, "\n") + "\n"
	for _, forbidden := range []string{"CODEX_HELPER_TEAMS_TOKEN_CACHE=", "CODEX_HELPER_TEAMS_CLIENT_ID=", "HTTP_PROXY=", "https_proxy="} {
		if strings.Contains(joined, "\n"+forbidden) {
			t.Fatalf("reload build env leaked %s in %#v", forbidden, env)
		}
	}
	if !strings.Contains(joined, "\nHOME=/home/alice\n") || !strings.Contains(joined, "\nPATH=/usr/bin") {
		t.Fatalf("reload build env did not preserve required HOME/PATH: %#v", env)
	}
	path := teamsReloadEnvValue(env, "PATH")
	for _, want := range []string{filepath.Join("/home/alice", ".local", "go", "bin"), "/usr/local/go/bin", "/usr/bin"} {
		if !pathContains(path, want) {
			t.Fatalf("reload build PATH = %q, want %q", path, want)
		}
	}
}

func TestTeamsHelperReloadLookPathUsesReloadEnvPath(t *testing.T) {
	dir := t.TempDir()
	name := "go"
	if runtime.GOOS == "windows" {
		name = "go.exe"
	}
	goPath := filepath.Join(dir, name)
	if err := os.WriteFile(goPath, []byte("fake go"), 0o755); err != nil {
		t.Fatalf("write fake go: %v", err)
	}
	resolved, ok := teamsReloadLookPath(name, []string{"PATH=" + dir})
	if !ok || resolved != goPath {
		t.Fatalf("teamsReloadLookPath = %q,%v want %q,true", resolved, ok, goPath)
	}
}

func teamsReloadEnvValue(env []string, key string) string {
	for _, entry := range env {
		gotKey, value, ok := strings.Cut(entry, "=")
		if ok && gotKey == key {
			return value
		}
	}
	return ""
}

func pathContains(pathValue string, want string) bool {
	for _, part := range filepath.SplitList(pathValue) {
		if part == want {
			return true
		}
	}
	return false
}

func writeTeamsReloadSource(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module github.com/baaaaaaaka/codex-helper\n"), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "cmd", "codex-proxy"), 0o755); err != nil {
		t.Fatalf("mkdir cmd/codex-proxy: %v", err)
	}
	return dir
}

func readFileString(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
