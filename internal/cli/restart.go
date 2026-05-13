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

	"github.com/baaaaaaaka/codex-helper/internal/update"
)

var (
	performUpdate  = update.PerformUpdate
	checkForUpdate = update.CheckForUpdate
	executablePath = os.Executable
	execSelf       = syscall.Exec
	exitFunc       = os.Exit
	startSelf      = startRestartProcess
)

func handleUpdateAndRestart(ctx context.Context, cmd *cobra.Command) error {
	res, err := performUpdate(ctx, update.UpdateOptions{
		Repo:           "",
		Version:        "latest",
		InstallPath:    "",
		Timeout:        120 * time.Second,
		ValidateBinary: true,
	})
	if err != nil {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Upgrade failed: %v\n", err)
		return err
	}

	if res.RestartRequired {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Update replacement for v%s is pending. Restart `codex-proxy`, then verify `codex-proxy --version` before treating the update as installed.\n", res.Version)
		return nil
	}

	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Updated to v%s. Restarting...\n", res.Version)
	return restartSelf()
}

func restartSelf() error {
	exe, err := executablePath()
	if err != nil {
		return err
	}
	exe = stableRestartExecutablePath(exe)
	args := append([]string{exe}, os.Args[1:]...)
	if runtime.GOOS == "windows" {
		if err := startSelf(exe, args[1:]); err != nil {
			return err
		}
		exitFunc(0)
		return nil
	}
	return execSelf(exe, args, os.Environ())
}

func stableRestartExecutablePath(path string) string {
	stable := stripReloadBackupSuffix(path)
	if stable == path {
		return path
	}
	if info, err := os.Stat(stable); err == nil && !info.IsDir() {
		return stable
	}
	return path
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
