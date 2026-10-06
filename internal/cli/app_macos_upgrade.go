package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gofrs/flock"
)

type codexDesktopMacUpgradeState struct {
	AppPath string
	Version string
}

var codexAppMacProcessRunningFn = codexDesktopMacAppProcessRunning

func upgradeCodexDesktopAppMac(ctx context.Context, opts codexDesktopAppOptions) (codexDesktopMacUpgradeState, bool, error) {
	home, err := codexDesktopMacInstallHome(opts)
	if err != nil {
		return codexDesktopMacUpgradeState{}, false, err
	}
	var state codexDesktopMacUpgradeState
	var changed bool
	err = withCodexDesktopMacLock(ctx, home, opts.ExecIdentity, func() error {
		installDir, err := codexDesktopMacApplicationsDir(home)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(installDir, 0o755); err != nil {
			return err
		}
		if err := ensurePathOwnedByIdentity(installDir, opts.ExecIdentity); err != nil {
			return fmt.Errorf("set ChatGPT install directory ownership: %w", err)
		}
		if err := rejectRunningCodexDesktopMacApp(ctx); err != nil {
			return err
		}
		if err := cleanCodexDesktopMacStagingDirs(installDir); err != nil {
			return err
		}
		for _, candidate := range codexDesktopMacCandidatePaths(home) {
			if err := recoverCodexDesktopMacAppBackup(ctx, installDir, candidate, opts.Log); err != nil {
				return err
			}
		}
		destination, err := findExistingCodexDesktopAppMac(ctx, opts, home)
		if err != nil {
			return err
		}
		state.AppPath, state.Version, changed, err = installCodexDesktopAppMacVersion(
			ctx,
			codexDesktopAppOptions{
				InstallHome:         home,
				ProxyURL:            opts.ProxyURL,
				RejectRunningMacApp: true,
				ExecIdentity:        opts.ExecIdentity,
				Log:                 opts.Log,
			},
			home,
			codexAppMacInstallURL(),
			destination,
		)
		return err
	})
	return state, changed, err
}

func cleanCodexDesktopMacStagingDirs(installDir string) error {
	entries, err := os.ReadDir(installDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read ChatGPT Applications directory: %w", err)
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".codex-desktop-install-") {
			continue
		}
		path := filepath.Join(installDir, entry.Name())
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect interrupted ChatGPT staging path %s: %w", path, err)
		}
		if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("remove interrupted ChatGPT staging directory %s: %w", path, err)
			}
		}
	}
	return nil
}

func withCodexDesktopMacLock(ctx context.Context, home string, identity *execIdentity, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	lockDir := filepath.Join(home, "Library", "Caches", "cxp")
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		return fmt.Errorf("create ChatGPT app lock directory: %w", err)
	}
	if err := ensurePathOwnedByIdentity(lockDir, identity); err != nil {
		return fmt.Errorf("set ChatGPT app lock directory ownership: %w", err)
	}
	lock := flock.New(filepath.Join(lockDir, "chatgpt-app.lock"))
	locked, err := lock.TryLock()
	if err != nil {
		return fmt.Errorf("lock ChatGPT app install: %w", err)
	}
	if !locked {
		return errors.New("another CXP ChatGPT app launch or upgrade is in progress; wait for it to finish and retry")
	}
	defer func() { _ = lock.Unlock() }()
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn()
}

func rejectRunningCodexDesktopMacApp(ctx context.Context) error {
	running, err := codexAppMacProcessRunningFn(ctx)
	if err != nil {
		return fmt.Errorf("check whether ChatGPT is running: %w", err)
	}
	if running {
		return errors.New("ChatGPT or Codex is running; quit every desktop app instance, including copies in /Applications, and do not reopen one until the upgrade finishes")
	}
	return nil
}

func rejectRunningCodexDesktopMacLaunch(ctx context.Context) error {
	running, err := codexAppMacProcessRunningFn(ctx)
	if err != nil {
		return fmt.Errorf("check whether ChatGPT is running: %w", err)
	}
	if running {
		return errors.New("ChatGPT or Codex is already running; quit all instances before launching through CXP to ensure it uses the current-user app")
	}
	return nil
}

func codexDesktopMacAppProcessRunning(ctx context.Context) (bool, error) {
	for _, processName := range []string{codexDesktopMacCurrentExecutableName, codexDesktopMacLegacyExecutableName} {
		output, err := codexAppCommandOutput(ctx, "/usr/bin/pgrep", "-x", processName)
		if err == nil {
			if strings.TrimSpace(string(output)) != "" {
				return true, nil
			}
			continue
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			continue
		}
		return false, fmt.Errorf("pgrep %s failed: %w", processName, err)
	}
	return false, nil
}

func recoverCodexDesktopMacAppBackup(ctx context.Context, installDir string, appPath string, log io.Writer) error {
	if _, err := os.Lstat(appPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect ChatGPT app path %s: %w", appPath, err)
	}
	pattern := filepath.Join(installDir, "."+filepath.Base(appPath)+".backup-*.app")
	backups, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("find interrupted ChatGPT app replacements: %w", err)
	}
	for index := len(backups) - 1; index >= 0; index-- {
		backup := backups[index]
		if _, err := codexDesktopMacExecutablePath(backup); err != nil {
			continue
		}
		if err := verifyCodexDesktopAppMac(ctx, backup, log); err != nil {
			continue
		}
		if err := os.Rename(backup, appPath); err != nil {
			return fmt.Errorf("restore interrupted ChatGPT app replacement from %s: %w", backup, err)
		}
		codexAppWarn(log, "restored the previous ChatGPT app after an interrupted update: %s", appPath)
		return nil
	}
	if len(backups) > 0 {
		return fmt.Errorf("ChatGPT app %s is missing and no verifiable backup could be restored", appPath)
	}
	return nil
}

func codexDesktopMacBundleVersion(ctx context.Context, appPath string) (string, error) {
	plistPath := filepath.Join(appPath, "Contents", "Info.plist")
	output, err := codexAppCommandOutput(ctx, "/usr/libexec/PlistBuddy", "-c", "Print :CFBundleVersion", plistPath)
	if err != nil {
		return "", err
	}
	version := strings.TrimSpace(string(output))
	if _, err := parseCodexDesktopMacVersion(version); err != nil {
		return "", err
	}
	return version, nil
}

func compareCodexDesktopMacVersions(left, right string) (int, error) {
	leftParts, err := parseCodexDesktopMacVersion(left)
	if err != nil {
		return 0, err
	}
	rightParts, err := parseCodexDesktopMacVersion(right)
	if err != nil {
		return 0, err
	}
	length := len(leftParts)
	if len(rightParts) > length {
		length = len(rightParts)
	}
	for index := 0; index < length; index++ {
		var leftPart, rightPart uint64
		if index < len(leftParts) {
			leftPart = leftParts[index]
		}
		if index < len(rightParts) {
			rightPart = rightParts[index]
		}
		if leftPart < rightPart {
			return -1, nil
		}
		if leftPart > rightPart {
			return 1, nil
		}
	}
	return 0, nil
}

func parseCodexDesktopMacVersion(version string) ([]uint64, error) {
	version = strings.TrimSpace(version)
	parts := strings.Split(version, ".")
	if version == "" || len(parts) > 8 {
		return nil, fmt.Errorf("invalid numeric ChatGPT app version %q", version)
	}
	values := make([]uint64, len(parts))
	for index, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("invalid numeric ChatGPT app version %q", version)
		}
		value, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid numeric ChatGPT app version %q: %w", version, err)
		}
		values[index] = value
	}
	return values, nil
}
