package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

var (
	performUpdate            = performUpdateWithRuntime
	checkForUpdate           = update.CheckForUpdate
	resolveInstallPathForCLI = resolveManagedInstallPathForCLI
	executablePath           = helperpath.RawExecutable
	restartArgv0             = func() string {
		if len(os.Args) == 0 {
			return ""
		}
		return os.Args[0]
	}
	execSelf  = syscall.Exec
	exitFunc  = os.Exit
	startSelf = startRestartProcess
)

func performUpdateWithRuntime(ctx context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
	if current, ok := helperruntime.Current(); ok && currentRuntimeOwnsUpdateTarget(current, opts.InstallPath) {
		opts.RuntimeRoot = current.Root
		opts.RuntimeEntryPath = current.EntryPath
	}
	return update.PerformUpdate(ctx, opts)
}

func currentRuntimeOwnsUpdateTarget(current helperruntime.Context, installPath string) bool {
	installPath = strings.TrimSpace(installPath)
	if installPath == "" {
		return true
	}
	candidates := []string{
		current.EntryPath,
		current.RuntimePath,
		filepath.Join(filepath.Dir(current.EntryPath), helperpath.BinaryName(runtime.GOOS)),
	}
	for _, candidate := range candidates {
		if sameHelperInstallLocation(installPath, candidate, runtime.GOOS) {
			return true
		}
	}
	return false
}

func handleUpdateAndRestart(ctx context.Context, cmd *cobra.Command) error {
	installPath, err := resolveInstallPathForCLI("")
	if err != nil {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Upgrade failed: %v\n", err)
		return err
	}
	var res update.ApplyResult
	err = withTeamsHelperUpgradeInstallLock(ctx, installPath, func() error {
		var updateErr error
		res, updateErr = performUpdate(ctx, update.UpdateOptions{
			Repo:           "",
			Version:        "latest",
			InstallPath:    installPath,
			Timeout:        120 * time.Second,
			ValidateBinary: true,
		})
		return updateErr
	})
	if err != nil {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Upgrade failed: %v\n", err)
		return err
	}

	if res.RestartRequired {
		if err := ensureCXPShimForInstallPath(res.InstallPath); err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Warning: failed to install cxp shim after update: %v\n", err)
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Update replacement for v%s is pending. Restart `codex-proxy`, then verify `codex-proxy --version` before treating the update as installed.\n", res.Version)
		return nil
	}

	if err := finalizeHelperUpdateResult(res, cmd.ErrOrStderr()); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Updated to v%s. Restarting...\n", res.Version)
	return restartSelf()
}

func restartSelf() error {
	if current, ok := helperruntime.Current(); ok {
		return restartSelfWithResolvedExecutableAndEnv(current.EntryPath, helperruntime.LauncherEnvironment(os.Environ()))
	}
	exe, err := executablePath()
	if err != nil {
		return err
	}
	exe, err = resolveRestartExecutablePathFromSources(exe, restartArgv0())
	if err != nil {
		return err
	}
	return restartSelfWithResolvedExecutable(exe)
}

func restartSelfWithExecutable(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return restartSelf()
	}
	exe, err := resolveRestartExecutablePathFromSources(path, "")
	if err != nil {
		return err
	}
	return restartSelfWithResolvedExecutable(exe)
}

func restartSelfWithResolvedExecutable(exe string) error {
	return restartSelfWithResolvedExecutableAndEnv(exe, os.Environ())
}

func restartSelfWithResolvedExecutableAndEnv(exe string, env []string) error {
	args := append([]string{exe}, os.Args[1:]...)
	if runtime.GOOS == "windows" {
		if err := startSelf(exe, args[1:]); err != nil {
			return err
		}
		exitFunc(0)
		return nil
	}
	return execSelf(exe, args, env)
}

func stableRestartExecutablePath(path string) string {
	exe, err := resolveRestartExecutablePath(path)
	if err != nil {
		return path
	}
	return exe
}

func resolveRestartExecutablePath(path string) (string, error) {
	return resolveRestartExecutablePathFromSources(path, "")
}

func resolveRestartExecutablePathFromSources(path string, argv0 string) (string, error) {
	resolved, err := helperpath.StableRunnablePathFromSources(path, argv0, helperpath.Options{})
	if err != nil {
		return "", err
	}
	return resolved.Path, nil
}

func stripReloadBackupSuffix(path string) string {
	path = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(path), " (deleted)"))
	if path == "" {
		return path
	}
	dir, base := filepath.Split(path)
	if idx := strings.Index(base, ".reload-backup-"); idx >= 0 {
		return filepath.Join(dir, base[:idx])
	}
	return path
}

func startRestartProcess(exe string, args []string) error {
	c := exec.Command(exe, args...)
	c.Stdin = os.Stdin
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Start()
}
