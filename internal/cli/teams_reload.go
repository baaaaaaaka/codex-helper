package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

type teamsReloadCommandResult struct {
	Output string
}

type teamsReloadCommandRunner func(context.Context, string, []string, string, ...string) (teamsReloadCommandResult, error)

var (
	teamsReloadExecutable = executablePath
	teamsReloadGetwd      = os.Getwd
	teamsReloadRestart    = restartTeamsHelperFromTeams
	teamsReloadRunCommand = runTeamsReloadCommand
	teamsReloadNow        = time.Now
)

func reloadTeamsHelperFromTeams(ctx context.Context, opts teams.HelperReloadOptions) error {
	sourceDir, err := teamsReloadSourceDir()
	if err != nil {
		return err
	}
	installPath, err := teamsReloadExecutable()
	if err != nil {
		return err
	}
	return runTeamsHelperReload(ctx, teamsHelperReloadOptions{
		SourceDir:     sourceDir,
		InstallPath:   installPath,
		BeforeRestart: opts.BeforeRestart,
	})
}

type teamsHelperReloadOptions struct {
	SourceDir     string
	InstallPath   string
	BeforeRestart func(context.Context) error
}

func runTeamsHelperReload(ctx context.Context, opts teamsHelperReloadOptions) error {
	sourceDir, err := validateTeamsReloadSourceDir(opts.SourceDir)
	if err != nil {
		return err
	}
	installPath, err := validateTeamsReloadInstallPath(opts.InstallPath)
	if err != nil {
		return err
	}
	env := teamsReloadBuildEnv()
	if err := runTeamsReloadTests(ctx, sourceDir, env); err != nil {
		return err
	}
	tmpPath, err := teamsReloadTempBinaryPath(installPath)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmpPath) }()
	if _, err := teamsReloadRunCommand(ctx, sourceDir, env, "go", "build", "-trimpath", "-o", tmpPath, "./cmd/codex-proxy"); err != nil {
		return fmt.Errorf("build helper reload binary: %w", err)
	}
	if err := validateTeamsReloadBuiltBinary(ctx, tmpPath, env); err != nil {
		return err
	}
	backupPath, err := replaceTeamsReloadBinary(installPath, tmpPath)
	if err != nil {
		return err
	}
	installed := true
	restore := func(reason string, cause error) error {
		if !installed {
			return cause
		}
		if err := restoreTeamsReloadBackup(installPath, backupPath); err != nil {
			return fmt.Errorf("%s: %w; rollback failed: %v", reason, cause, err)
		}
		installed = false
		return fmt.Errorf("%s: %w", reason, cause)
	}
	if opts.BeforeRestart != nil {
		if err := opts.BeforeRestart(ctx); err != nil {
			return restore("prepare helper restart", err)
		}
	}
	if err := teamsReloadRestart(ctx); err != nil {
		return restore("restart helper after reload", err)
	}
	return nil
}

func teamsReloadSourceDir() (string, error) {
	if override := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_RELOAD_SOURCE_DIR")); override != "" {
		return override, nil
	}
	return teamsReloadGetwd()
}

func validateTeamsReloadSourceDir(dir string) (string, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return "", fmt.Errorf("helper reload source directory is empty")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return "", fmt.Errorf("helper reload source directory is not accessible: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("helper reload source path is not a directory: %s", abs)
	}
	goMod := filepath.Join(abs, "go.mod")
	data, err := os.ReadFile(goMod)
	if err != nil {
		return "", fmt.Errorf("helper reload requires a codex-helper source checkout with go.mod at %s: %w", goMod, err)
	}
	if !strings.Contains(string(data), "module github.com/baaaaaaaka/codex-helper") {
		return "", fmt.Errorf("helper reload refused source directory %s: go.mod is not github.com/baaaaaaaka/codex-helper", abs)
	}
	return abs, nil
}

func validateTeamsReloadInstallPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("helper reload install path is empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	for _, part := range strings.FieldsFunc(filepath.Clean(abs), func(r rune) bool {
		return r == filepath.Separator || r == '/' || r == '\\'
	}) {
		if strings.HasPrefix(part, "go-build") {
			return "", fmt.Errorf("helper reload refused temporary go run binary: %s", path)
		}
	}
	info, err := os.Stat(abs)
	if err != nil {
		return "", fmt.Errorf("helper reload install path is not accessible: %w", err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("helper reload install path is a directory: %s", abs)
	}
	return abs, nil
}

func runTeamsReloadTests(ctx context.Context, sourceDir string, env []string) error {
	if skip := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_RELOAD_SKIP_TESTS")); skip == "1" || strings.EqualFold(skip, "true") {
		return nil
	}
	if custom := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_RELOAD_TEST_CMD")); custom != "" {
		name, args := teamsReloadShellCommand(custom)
		if _, err := teamsReloadRunCommand(ctx, sourceDir, env, name, args...); err != nil {
			return fmt.Errorf("run helper reload test command: %w", err)
		}
		return nil
	}
	if _, err := teamsReloadRunCommand(ctx, sourceDir, env, "go", "test", "./internal/teams", "./internal/cli", "-run", "Test(ParseControlReload|BridgeControlReload|TeamsHelperReload)", "-count=1"); err != nil {
		return fmt.Errorf("run helper reload safety tests: %w", err)
	}
	return nil
}

func teamsReloadShellCommand(script string) (string, []string) {
	if runtime.GOOS == "windows" {
		return "cmd", []string{"/C", script}
	}
	return "sh", []string{"-c", script}
}

func validateTeamsReloadBuiltBinary(ctx context.Context, tmpPath string, env []string) error {
	info, err := os.Stat(tmpPath)
	if err != nil {
		return fmt.Errorf("helper reload build did not create binary: %w", err)
	}
	if info.IsDir() || info.Size() == 0 {
		return fmt.Errorf("helper reload build created invalid binary: %s", tmpPath)
	}
	if runtime.GOOS != "windows" && info.Mode()&0o111 == 0 {
		return fmt.Errorf("helper reload build created non-executable binary: %s", tmpPath)
	}
	if _, err := teamsReloadRunCommand(ctx, filepath.Dir(tmpPath), env, tmpPath, "--version"); err != nil {
		return fmt.Errorf("verify helper reload binary: %w", err)
	}
	return nil
}

func teamsReloadTempBinaryPath(installPath string) (string, error) {
	dir := filepath.Dir(installPath)
	base := filepath.Base(installPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), teamsReloadNow().UnixNano())
	return filepath.Join(dir, "."+base+".reload-"+suffix), nil
}

func replaceTeamsReloadBinary(installPath, tmpPath string) (string, error) {
	backupPath := installPath + ".reload-backup-" + fmt.Sprintf("%d-%d", os.Getpid(), teamsReloadNow().UnixNano())
	if err := os.Rename(installPath, backupPath); err != nil {
		return "", fmt.Errorf("backup current helper binary: %w", err)
	}
	if err := os.Rename(tmpPath, installPath); err != nil {
		_ = os.Rename(backupPath, installPath)
		return "", fmt.Errorf("install rebuilt helper binary: %w", err)
	}
	return backupPath, nil
}

func restoreTeamsReloadBackup(installPath, backupPath string) error {
	if strings.TrimSpace(backupPath) == "" {
		return nil
	}
	_ = os.Remove(installPath)
	return os.Rename(backupPath, installPath)
}

func teamsReloadBuildEnv() []string {
	allow := map[string]bool{
		"HOME":         true,
		"PATH":         true,
		"GOCACHE":      true,
		"GOMODCACHE":   true,
		"GOPATH":       true,
		"GOTOOLCHAIN":  true,
		"GOFLAGS":      true,
		"TMPDIR":       true,
		"TEMP":         true,
		"TMP":          true,
		"USERPROFILE":  true,
		"LOCALAPPDATA": true,
		"APPDATA":      true,
		"SystemRoot":   true,
		"WINDIR":       true,
	}
	var out []string
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || !allow[key] {
			continue
		}
		if strings.TrimSpace(value) == "" {
			continue
		}
		out = append(out, key+"="+value)
	}
	return out
}

func runTeamsReloadCommand(ctx context.Context, dir string, env []string, name string, args ...string) (teamsReloadCommandResult, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
	output, err := cmd.CombinedOutput()
	result := teamsReloadCommandResult{Output: string(output)}
	if err != nil {
		return result, fmt.Errorf("%s %s failed: %w\n%s", name, strings.Join(args, " "), err, teamsReloadSafeOutput(result.Output))
	}
	return result, nil
}

func teamsReloadSafeOutput(output string) string {
	output = strings.TrimSpace(output)
	if len(output) > 4000 {
		output = output[:4000] + "\n... output truncated ..."
	}
	return output
}
