package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

var materializeManagedTeamsInstallTarget = defaultMaterializeManagedTeamsInstallTarget

func resolveManagedInstallPathForCLI(explicit string) (string, error) {
	raw, _ := executablePath()
	target, err := managedinstall.Resolve(managedinstall.Options{
		ExplicitPath:  explicit,
		EnvPath:       os.Getenv(update.EnvInstallPath),
		EnvDir:        os.Getenv(update.EnvInstallDir),
		RawExecutable: raw,
		Argv0:         restartArgv0(),
		GOOS:          runtime.GOOS,
	})
	if err != nil {
		return "", err
	}
	return target.Path, nil
}

func resolveManagedInstallPathForTeams(explicit string) (string, error) {
	target, err := resolveManagedTeamsInstallTarget(explicit, false, true)
	if err != nil {
		return "", err
	}
	return target.Path, nil
}

func resolveManagedTeamsServiceExecutable(requireExisting bool) (managedinstall.Target, error) {
	allowMissingDefault := !requireExisting && !strings.EqualFold(teamsServiceGOOS(), "windows") && managedTeamsMaterializationSourceAvailable()
	target, err := resolveManagedTeamsInstallTarget("", requireExisting, allowMissingDefault)
	if err != nil {
		if raw, rawErr := teamsServiceExecutable(); rawErr == nil {
			if class := helperpath.Classify(raw); class.Kind == helperpath.KindGoBuildTemp {
				return managedinstall.Target{}, fmt.Errorf("cannot install Teams service from temporary helper executable path %q: %s", class.Clean, class.Reason)
			}
		}
	}
	return target, err
}

func managedTeamsMaterializationSourceAvailable() bool {
	raw, err := teamsServiceExecutable()
	if err != nil || strings.TrimSpace(raw) == "" {
		return false
	}
	running, err := helperpath.StableRunnablePathFromSources(raw, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat})
	if err != nil {
		return false
	}
	probe := helperpath.ProbePath(running.Path, helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat})
	if !probe.Exists || probe.IsDir || !probe.Executable || !probe.PlausibleHelperEntry {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = update.ProbeBinaryVersion(ctx, running.Path, 5*time.Second)
	return err == nil
}

func resolveManagedTeamsInstallTarget(explicit string, requireExisting bool, allowMissingDefault bool) (managedinstall.Target, error) {
	raw, _ := teamsServiceExecutable()
	return managedinstall.Resolve(managedinstall.Options{
		ExplicitPath:                explicit,
		EnvPath:                     os.Getenv(update.EnvInstallPath),
		EnvDir:                      os.Getenv(update.EnvInstallDir),
		RawExecutable:               raw,
		Argv0:                       teamsServiceArgv0(),
		GOOS:                        teamsServiceGOOS(),
		RequireExisting:             requireExisting,
		AllowMissingDefault:         allowMissingDefault,
		FallbackOnInvalidEnvPath:    true,
		PreferRecordBeforeLegacyEnv: true,
		Stat:                        teamsServiceStat,
	})
}

func defaultMaterializeManagedTeamsInstallTarget(ctx context.Context, target managedinstall.Target) error {
	if target.State != managedinstall.StateManaged {
		return nil
	}
	if target.Source != managedinstall.SourceDefault && target.Source != managedinstall.SourceRecord {
		return nil
	}
	if strings.EqualFold(teamsServiceGOOS(), "windows") {
		return nil
	}
	raw, err := teamsServiceExecutable()
	if err != nil || strings.TrimSpace(raw) == "" {
		return nil
	}
	running, err := helperpath.StableRunnablePathFromSources(raw, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat})
	if err != nil {
		return nil
	}
	if sameHelperExecutablePath(running.Path, target.Path, teamsServiceGOOS()) {
		return nil
	}
	runningProbe := helperpath.ProbePath(running.Path, helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat})
	if !runningProbe.Exists || runningProbe.IsDir || !runningProbe.Executable || !runningProbe.PlausibleHelperEntry {
		return nil
	}
	targetProbe := helperpath.ProbePath(target.Path, helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat})
	if targetProbe.IsDir || !targetProbe.PlausibleHelperEntry {
		return nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	runningVersion, runningErr := update.ProbeBinaryVersion(probeCtx, running.Path, 5*time.Second)
	if runningErr != nil {
		return nil
	}
	shouldCopy := !targetProbe.Exists || !targetProbe.Executable
	targetVersion := update.BinaryVersion{}
	if !shouldCopy {
		var targetErr error
		targetVersion, targetErr = update.ProbeBinaryVersion(probeCtx, target.Path, 5*time.Second)
		shouldCopy = targetErr != nil
		if targetErr == nil {
			cmp, ok := update.CompareVersions(runningVersion.Version, targetVersion.Version)
			shouldCopy = ok && cmp > 0
		}
	}
	shims := materializedManagedInstallShims(target.Path)
	if !shouldCopy {
		if err := ensureCXPShimForInstallPath(target.Path); err != nil {
			return err
		}
		if strings.TrimSpace(targetVersion.Version) != "" {
			saveManagedInstallRecordBestEffort(target, targetVersion.Version, shims)
		}
		return nil
	}
	if err := copyExecutableAtomically(running.Path, target.Path); err != nil {
		return fmt.Errorf("materialize managed Teams install target %s from running helper %s: %w", target.Path, running.Path, err)
	}
	for _, shim := range shims {
		if err := materializeManagedInstallShim(ctx, running.Path, target.Path, shim, runningVersion.Version); err != nil {
			return err
		}
	}
	saveManagedInstallRecordBestEffort(target, runningVersion.Version, shims)
	return nil
}

func materializedManagedInstallShims(targetPath string) []string {
	if strings.EqualFold(teamsServiceGOOS(), "windows") {
		return nil
	}
	return []string{filepath.Join(filepath.Dir(targetPath), "cxp")}
}

func ensureCXPShimForInstallPath(installPath string) error {
	return ensureCXPShimForInstallPathForGOOS(installPath, runtime.GOOS)
}

func ensureCXPShimForInstallPathForGOOS(installPath string, goos string) error {
	if strings.EqualFold(goos, "windows") {
		return ensureWindowsCXPShimForInstallPath(installPath)
	}
	installPath = strings.TrimSpace(installPath)
	if installPath == "" || !strings.EqualFold(filepath.Base(installPath), helperpath.BinaryName(goos)) {
		return nil
	}
	shimPath := filepath.Join(filepath.Dir(installPath), "cxp")
	if info, err := os.Lstat(shimPath); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			target, readErr := os.Readlink(shimPath)
			if readErr != nil {
				return readErr
			}
			resolvedTarget := target
			if !filepath.IsAbs(resolvedTarget) {
				resolvedTarget = filepath.Join(filepath.Dir(shimPath), resolvedTarget)
			}
			if sameHelperExecutablePath(resolvedTarget, installPath, goos) {
				return nil
			}
			if err := replaceSymlinkAtomically(shimPath, installPath); err != nil {
				if copyErr := copyExecutableAtomically(installPath, shimPath); copyErr != nil {
					return fmt.Errorf("repair cxp shim %s -> %s: symlink failed: %v; copy failed: %w", shimPath, installPath, err, copyErr)
				}
			}
			return nil
		}
		probe := helperpath.ProbePath(shimPath, helperpath.Options{GOOS: goos})
		if probe.Exists && !probe.IsDir && probe.Executable && probe.PlausibleHelperEntry {
			repair, err := cxpShimNeedsRepair(installPath, shimPath)
			if err != nil {
				return err
			}
			if repair {
				return copyExecutableAtomically(installPath, shimPath)
			}
			return nil
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(shimPath), 0o755); err != nil {
		return err
	}
	if err := os.Symlink(installPath, shimPath); err != nil {
		if copyErr := copyExecutableAtomically(installPath, shimPath); copyErr != nil {
			return fmt.Errorf("create cxp shim %s -> %s: symlink failed: %v; copy failed: %w", shimPath, installPath, err, copyErr)
		}
	}
	return nil
}

func replaceSymlinkAtomically(linkPath string, targetPath string) error {
	dir := filepath.Dir(linkPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(linkPath)+".symlink-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Remove(tmpPath); err != nil {
		return err
	}
	if err := os.Symlink(targetPath, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, linkPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func ensureWindowsCXPShimForInstallPath(installPath string) error {
	installPath = strings.TrimSpace(installPath)
	if installPath == "" || !strings.EqualFold(filepath.Base(installPath), helperpath.BinaryName("windows")) {
		return nil
	}
	shimPath := filepath.Join(filepath.Dir(installPath), "cxp.cmd")
	expected := windowsCXPShimContent()
	if info, err := os.Stat(shimPath); err == nil {
		if info.IsDir() {
			return nil
		}
		data, readErr := os.ReadFile(shimPath)
		if readErr != nil {
			return readErr
		}
		if string(data) == expected {
			return nil
		}
		if !strings.Contains(strings.ToLower(string(data)), "codex-proxy.exe") {
			return nil
		}
		return os.WriteFile(shimPath, []byte(expected), 0o755)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(shimPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(shimPath, []byte(expected), 0o755)
}

func windowsCXPShimContent() string {
	return "@echo off\r\n\"%~dp0codex-proxy.exe\" %*\r\n"
}

func cxpShimNeedsRepair(installPath string, shimPath string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	installVersion, installErr := update.ProbeBinaryVersion(ctx, installPath, 5*time.Second)
	shimVersion, shimErr := update.ProbeBinaryVersion(ctx, shimPath, 5*time.Second)
	if shimErr != nil {
		return installErr == nil, nil
	}
	if installErr != nil {
		return false, nil
	}
	cmp, ok := update.CompareVersions(installVersion.Version, shimVersion.Version)
	return ok && cmp > 0, nil
}

func materializeManagedInstallShim(ctx context.Context, runningPath string, targetPath string, shimPath string, runningVersion string) error {
	if info, err := os.Lstat(shimPath); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil
	}
	probe := helperpath.ProbePath(shimPath, helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat})
	if !probe.Exists {
		if err := os.MkdirAll(filepath.Dir(shimPath), 0o755); err != nil {
			return err
		}
		if err := os.Symlink(targetPath, shimPath); err != nil {
			return fmt.Errorf("materialize managed Teams install shim %s -> %s: %w", shimPath, targetPath, err)
		}
		return nil
	}
	if probe.IsDir || !probe.Executable || !probe.PlausibleHelperEntry {
		return nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	shimVersion, err := update.ProbeBinaryVersion(probeCtx, shimPath, 5*time.Second)
	shouldCopy := err != nil
	if err == nil {
		cmp, ok := update.CompareVersions(runningVersion, shimVersion.Version)
		shouldCopy = ok && cmp > 0
	}
	if !shouldCopy {
		return nil
	}
	if err := copyExecutableAtomically(runningPath, shimPath); err != nil {
		return fmt.Errorf("materialize managed Teams install shim %s from running helper %s: %w", shimPath, runningPath, err)
	}
	return nil
}

func saveManagedInstallRecordBestEffort(target managedinstall.Target, version string, shims []string) {
	if strings.TrimSpace(target.RecordPath) == "" {
		return
	}
	record := managedinstall.Record{
		TargetPath:   target.Path,
		TargetSource: string(target.Source),
		TargetState:  string(target.State),
		Version:      version,
		GOOS:         teamsServiceGOOS(),
		GOARCH:       runtime.GOARCH,
		Shims:        existingManagedInstallShims(shims),
	}
	_ = managedinstall.SaveRecord(target.RecordPath, record)
}

func existingManagedInstallShims(shims []string) []string {
	var out []string
	for _, shim := range shims {
		if strings.TrimSpace(shim) == "" {
			continue
		}
		if probe := helperpath.ProbePath(shim, helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat}); probe.Exists && !probe.IsDir && probe.PlausibleHelperEntry {
			out = append(out, shim)
		}
	}
	return out
}

func copyExecutableAtomically(src string, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	if srcInfo.IsDir() {
		return fmt.Errorf("source is a directory: %s", src)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp, err := os.CreateTemp(filepath.Dir(dst), "."+filepath.Base(dst)+".materialize-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := io.Copy(tmp, in); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(srcInfo.Mode().Perm() | 0o700); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return err
	}
	cleanup = false
	return nil
}
