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
	running, err := helperpath.StableRunnablePathFromSources(raw, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS()})
	if err != nil {
		return false
	}
	probe := helperpath.ProbePath(running.Path, helperpath.Options{GOOS: teamsServiceGOOS()})
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
	running, err := helperpath.StableRunnablePathFromSources(raw, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS()})
	if err != nil {
		return nil
	}
	if sameHelperExecutablePath(running.Path, target.Path, teamsServiceGOOS()) {
		return nil
	}
	runningProbe := helperpath.ProbePath(running.Path, helperpath.Options{GOOS: teamsServiceGOOS()})
	if !runningProbe.Exists || runningProbe.IsDir || !runningProbe.Executable || !runningProbe.PlausibleHelperEntry {
		return nil
	}
	targetProbe := helperpath.ProbePath(target.Path, helperpath.Options{GOOS: teamsServiceGOOS()})
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
	if !shouldCopy {
		targetVersion, targetErr := update.ProbeBinaryVersion(probeCtx, target.Path, 5*time.Second)
		shouldCopy = targetErr != nil
		if targetErr == nil {
			cmp, ok := update.CompareVersions(runningVersion.Version, targetVersion.Version)
			shouldCopy = ok && cmp > 0
		}
	}
	if !shouldCopy {
		return nil
	}
	if err := copyExecutableAtomically(running.Path, target.Path); err != nil {
		return fmt.Errorf("materialize managed Teams install target %s from running helper %s: %w", target.Path, running.Path, err)
	}
	shims := materializedManagedInstallShims(target.Path)
	for _, shim := range shims {
		if err := materializeManagedInstallShim(ctx, running.Path, shim, runningVersion.Version); err != nil {
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

func materializeManagedInstallShim(ctx context.Context, runningPath string, shimPath string, runningVersion string) error {
	if info, err := os.Lstat(shimPath); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil
	}
	probe := helperpath.ProbePath(shimPath, helperpath.Options{GOOS: teamsServiceGOOS()})
	if !probe.Exists {
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
		if probe := helperpath.ProbePath(shim, helperpath.Options{GOOS: teamsServiceGOOS()}); probe.Exists && !probe.IsDir && probe.PlausibleHelperEntry {
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
