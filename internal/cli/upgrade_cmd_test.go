package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/flock"

	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

func upgradeArgsWithTeamsRestart(args ...string) []string {
	return append([]string{"--restart-teams-service"}, args...)
}

func TestUpgradeCmdAlreadyUpToDateSkipsDownload(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(_ context.Context, opts update.CheckOptions) update.Status {
		if opts.Repo != "owner/name" {
			t.Fatalf("expected repo owner/name, got %q", opts.Repo)
		}
		if opts.InstalledVersion == "" {
			t.Fatal("expected installed version")
		}
		if opts.Timeout != 8*time.Second {
			t.Fatalf("expected 8s check timeout, got %s", opts.Timeout)
		}
		return update.Status{Supported: true, UpdateAvailable: false}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		t.Fatal("PerformUpdate should not run when latest is already installed")
		return update.ApplyResult{}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--repo", "owner/name"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if !strings.Contains(out.String(), "Already up to date.") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestUpgradeCmdExplicitVersionCallsPerformUpdate(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	explicitInstallPath := "/tmp/codex-proxy"
	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--repo", "owner/name", "--version", "v1.2.3", "--install-path", explicitInstallPath})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}

	if got.Repo != "owner/name" || got.Version != "v1.2.3" || got.InstallPath != filepath.Clean(explicitInstallPath) {
		t.Fatalf("unexpected update options: %+v", got)
	}
	if got.Timeout != 120*time.Second {
		t.Fatalf("expected 120s update timeout, got %s", got.Timeout)
	}
	if !got.ValidateBinary {
		t.Fatal("upgrade command must validate the downloaded binary before replacing itself")
	}
	if !strings.Contains(out.String(), "Updated to v1.2.3.") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestUpgradeCmdUsesManagedDefaultWhenLaunchedAsCXP(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX cxp entry simulation")
	}
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevResolve := resolveInstallPathForCLI
	prevExecutable := executablePath
	prevArgv0 := restartArgv0
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		resolveInstallPathForCLI = prevResolve
		executablePath = prevExecutable
		restartArgv0 = prevArgv0
	})

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	managed := filepath.Join(tmp, ".local", "bin", "codex-proxy")
	cxp := filepath.Join(tmp, ".local", "bin", "cxp")
	helperScript := "#!/bin/sh\n" +
		"if [ \"$1\" = \"skills\" ] && [ \"$2\" = \"install-builtin\" ]; then exit 0; fi\n" +
		"if [ \"$1\" = \"--version\" ]; then echo 'codex-proxy version 1.2.2 (test)'; exit 0; fi\n" +
		"exit 0\n"
	writeCLIFile(t, managed, helperScript, 0o755)
	writeCLIFile(t, cxp, helperScript, 0o755)
	resolveInstallPathForCLI = resolveManagedInstallPathForCLI
	executablePath = func() (string, error) { return cxp, nil }
	restartArgv0 = func() string { return cxp }
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		writeCLIFile(t, opts.InstallPath, upgradeCXPShimTestScript("1.2.3"), 0o755)
		return update.ApplyResult{Version: "1.2.3", InstallPath: opts.InstallPath}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if got.InstallPath != managed {
		t.Fatalf("upgrade install path = %q, want managed codex-proxy %q", got.InstallPath, managed)
	}
}

func TestUpgradeCmdCreatesMissingCXPShimForManagedDefault(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX cxp shim creation")
	}
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevResolve := resolveInstallPathForCLI
	prevExecutable := executablePath
	prevArgv0 := restartArgv0
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		resolveInstallPathForCLI = prevResolve
		executablePath = prevExecutable
		restartArgv0 = prevArgv0
	})

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	managed := filepath.Join(tmp, ".local", "bin", "codex-proxy")
	cxp := filepath.Join(tmp, ".local", "bin", "cxp")
	writeCLIFile(t, managed, upgradeCXPShimTestScript("1.2.3"), 0o755)
	resolveInstallPathForCLI = resolveManagedInstallPathForCLI
	executablePath = func() (string, error) { return managed, nil }
	restartArgv0 = func() string { return managed }
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		writeCLIFile(t, opts.InstallPath, upgradeCXPShimTestScript("1.2.4"), 0o755)
		return update.ApplyResult{Version: "1.2.4", InstallPath: opts.InstallPath}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.4"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if target, err := os.Readlink(cxp); err != nil || target != managed {
		t.Fatalf("cxp shim = %q err=%v, want symlink to %s", target, err, managed)
	}
}

func TestUpgradeCmdRepairsStaleCXPRegularFileForManagedDefault(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX cxp shim repair")
	}
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevResolve := resolveInstallPathForCLI
	prevExecutable := executablePath
	prevArgv0 := restartArgv0
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		resolveInstallPathForCLI = prevResolve
		executablePath = prevExecutable
		restartArgv0 = prevArgv0
	})

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	managed := filepath.Join(tmp, ".local", "bin", "codex-proxy")
	cxp := filepath.Join(tmp, ".local", "bin", "cxp")
	writeCLIFile(t, managed, upgradeCXPShimTestScript("1.2.3"), 0o755)
	writeCLIFile(t, cxp, upgradeCXPShimTestScript("1.2.3"), 0o755)
	resolveInstallPathForCLI = resolveManagedInstallPathForCLI
	executablePath = func() (string, error) { return cxp, nil }
	restartArgv0 = func() string { return cxp }
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		writeCLIFile(t, opts.InstallPath, upgradeCXPShimTestScript("1.2.4"), 0o755)
		return update.ApplyResult{Version: "1.2.4", InstallPath: opts.InstallPath}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.4"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if out, err := exec.Command(cxp, "--version").CombinedOutput(); err != nil || !strings.Contains(string(out), "1.2.4") {
		t.Fatalf("cxp version after repair = %q err=%v, want 1.2.4", out, err)
	}
	if target, err := os.Readlink(cxp); err != nil || target != managed {
		t.Fatalf("cxp repair should unify regular executable copy as symlink to %s, target=%q err=%v", managed, target, err)
	}
}

func TestUpgradeCmdIgnoresBrokenEnvInstallPathAndUnifiesAliases(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX alias unification")
	}
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevResolve := resolveInstallPathForCLI
	prevExecutable := executablePath
	prevArgv0 := restartArgv0
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		resolveInstallPathForCLI = prevResolve
		executablePath = prevExecutable
		restartArgv0 = prevArgv0
	})

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	current := filepath.Join(tmp, "go", "bin", "codex-proxy")
	currentCXP := filepath.Join(tmp, "go", "bin", "cxp")
	envTarget := filepath.Join(tmp, ".local", "bin", "codex-proxy")
	envCXP := filepath.Join(tmp, ".local", "bin", "cxp")
	writeCLIFile(t, current, upgradeCXPShimTestScript("1.2.3"), 0o755)
	if err := os.Symlink(current, currentCXP); err != nil {
		t.Fatalf("create current cxp symlink: %v", err)
	}
	writeCLIFile(t, envTarget, "#!/bin/sh\necho binary-payload >&2\nexit 127\n", 0o755)
	writeCLIFile(t, envCXP, upgradeCXPShimTestScript("1.2.2"), 0o755)
	t.Setenv(update.EnvInstallPath, envTarget)
	resolveInstallPathForCLI = resolveManagedInstallPathForCLI
	executablePath = func() (string, error) { return current, nil }
	restartArgv0 = func() string { return currentCXP }
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		writeCLIFile(t, opts.InstallPath, upgradeCXPShimTestScript("1.2.4"), 0o755)
		return update.ApplyResult{Version: "1.2.4", InstallPath: opts.InstallPath}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.4"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if got.InstallPath != current {
		t.Fatalf("upgrade install path = %q, want current helper %q", got.InstallPath, current)
	}
	if out, err := exec.Command(envTarget, "--version").CombinedOutput(); err != nil || !strings.Contains(string(out), "1.2.4") {
		t.Fatalf("env install path after unification = %q err=%v, want 1.2.4", out, err)
	}
	if target, err := os.Readlink(envTarget); err != nil || target != current {
		t.Fatalf("env install path should be symlink to current helper, target=%q err=%v", target, err)
	}
	if out, err := exec.Command(envCXP, "--version").CombinedOutput(); err != nil || !strings.Contains(string(out), "1.2.4") {
		t.Fatalf("env cxp after unification = %q err=%v, want 1.2.4", out, err)
	}
	if target, err := os.Readlink(envCXP); err != nil || target != current {
		t.Fatalf("env cxp should be symlink to current helper, target=%q err=%v", target, err)
	}
}

func TestUpgradeCmdUnifiesDefaultInstallAliasWhenCurrentGoBinWins(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX alias unification")
	}
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevResolve := resolveInstallPathForCLI
	prevExecutable := executablePath
	prevArgv0 := restartArgv0
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		resolveInstallPathForCLI = prevResolve
		executablePath = prevExecutable
		restartArgv0 = prevArgv0
	})

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	current := filepath.Join(tmp, "go", "bin", "codex-proxy")
	currentCXP := filepath.Join(tmp, "go", "bin", "cxp")
	managed := filepath.Join(tmp, ".local", "bin", "codex-proxy")
	managedCXP := filepath.Join(tmp, ".local", "bin", "cxp")
	writeCLIFile(t, current, upgradeCXPShimTestScript("1.2.3"), 0o755)
	if err := os.Symlink(current, currentCXP); err != nil {
		t.Fatalf("create current cxp symlink: %v", err)
	}
	writeCLIFile(t, managed, upgradeCXPShimTestScript("1.2.2"), 0o755)
	writeCLIFile(t, managedCXP, upgradeCXPShimTestScript("1.2.2"), 0o755)
	resolveInstallPathForCLI = resolveManagedInstallPathForCLI
	executablePath = func() (string, error) { return current, nil }
	restartArgv0 = func() string { return currentCXP }
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		writeCLIFile(t, opts.InstallPath, upgradeCXPShimTestScript("1.2.4"), 0o755)
		return update.ApplyResult{Version: "1.2.4", InstallPath: opts.InstallPath}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.4"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if got.InstallPath != current {
		t.Fatalf("upgrade install path = %q, want current helper %q", got.InstallPath, current)
	}
	if out, err := exec.Command(managed, "--version").CombinedOutput(); err != nil || !strings.Contains(string(out), "1.2.4") {
		t.Fatalf("managed default after unification = %q err=%v, want 1.2.4", out, err)
	}
	if target, err := os.Readlink(managed); err != nil || target != current {
		t.Fatalf("managed default should be symlink to current helper, target=%q err=%v", target, err)
	}
	if out, err := exec.Command(managedCXP, "--version").CombinedOutput(); err != nil || !strings.Contains(string(out), "1.2.4") {
		t.Fatalf("managed cxp after unification = %q err=%v, want 1.2.4", out, err)
	}
	if target, err := os.Readlink(managedCXP); err != nil || target != current {
		t.Fatalf("managed cxp should be symlink to current helper, target=%q err=%v", target, err)
	}
}

func TestEnsureCXPShimForInstallPathRepairsStalePOSIXSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX symlink repair")
	}
	tmp := t.TempDir()
	installPath := filepath.Join(tmp, "codex-proxy")
	stalePath := filepath.Join(tmp, "go", "bin", "codex-proxy")
	shimPath := filepath.Join(tmp, "cxp")
	writeCLIFile(t, installPath, upgradeCXPShimTestScript("1.2.4"), 0o755)
	writeCLIFile(t, stalePath, upgradeCXPShimTestScript("1.2.3"), 0o755)
	if err := os.Symlink(stalePath, shimPath); err != nil {
		t.Fatalf("create stale cxp symlink: %v", err)
	}

	if err := ensureCXPShimForInstallPathForGOOS(installPath, "linux"); err != nil {
		t.Fatalf("ensure cxp shim: %v", err)
	}
	target, err := os.Readlink(shimPath)
	if err != nil {
		t.Fatalf("read repaired cxp symlink: %v", err)
	}
	if target != installPath {
		t.Fatalf("cxp symlink = %q, want %q", target, installPath)
	}
}

func TestRepairKnownHelperEntrypointsSkipsInstallPathThroughSymlinkedParent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX symlinked parent alias repair")
	}

	for _, tc := range []struct {
		name      string
		linkLocal bool
	}{
		{name: "local directory", linkLocal: true},
		{name: "local bin directory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			isolateTeamsUserDirsForTest(t, tmp)
			home := filepath.Join(tmp, "home")
			physicalLocal := filepath.Join(tmp, "local-overflow")
			physicalBin := filepath.Join(physicalLocal, "bin")
			if err := os.MkdirAll(filepath.Join(home, ".local"), 0o755); err != nil {
				t.Fatalf("mkdir home local: %v", err)
			}
			if err := os.MkdirAll(physicalBin, 0o755); err != nil {
				t.Fatalf("mkdir physical bin: %v", err)
			}
			if tc.linkLocal {
				if err := os.Remove(filepath.Join(home, ".local")); err != nil {
					t.Fatalf("remove home local: %v", err)
				}
				if err := os.Symlink(physicalLocal, filepath.Join(home, ".local")); err != nil {
					t.Fatalf("symlink .local: %v", err)
				}
			} else if err := os.Symlink(physicalBin, filepath.Join(home, ".local", "bin")); err != nil {
				t.Fatalf("symlink .local/bin: %v", err)
			}
			t.Setenv("HOME", home)

			installPath := filepath.Join(physicalBin, "codex-proxy")
			shimPath := filepath.Join(physicalBin, "cxp")
			writeCLIFile(t, installPath, upgradeCXPShimTestScript("1.2.4"), 0o755)
			if err := os.Symlink(installPath, shimPath); err != nil {
				t.Fatalf("create cxp shim: %v", err)
			}

			for _, err := range repairKnownHelperEntrypointsForInstallPath(installPath) {
				t.Fatalf("repair known helper entrypoints: %v", err)
			}
			if info, err := os.Lstat(installPath); err != nil {
				t.Fatalf("lstat install path: %v", err)
			} else if info.Mode()&os.ModeSymlink != 0 {
				target, _ := os.Readlink(installPath)
				t.Fatalf("install path was replaced by symlink to %q", target)
			}
			if out, err := exec.Command(installPath, "--version").CombinedOutput(); err != nil || !strings.Contains(string(out), "1.2.4") {
				t.Fatalf("install path after repair = %q err=%v, want 1.2.4", out, err)
			}
			if target, err := os.Readlink(shimPath); err != nil || target != installPath {
				t.Fatalf("cxp shim = %q err=%v, want %s", target, err, installPath)
			}
		})
	}
}

func TestSameHelperInstallLocationUsesResolvedParentForSelfSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX self-symlink")
	}
	tmp := t.TempDir()
	home := filepath.Join(tmp, "home")
	physicalLocal := filepath.Join(tmp, "local-overflow")
	physicalBin := filepath.Join(physicalLocal, "bin")
	if err := os.MkdirAll(filepath.Dir(filepath.Join(home, ".local")), 0o755); err != nil {
		t.Fatalf("mkdir home: %v", err)
	}
	if err := os.MkdirAll(physicalBin, 0o755); err != nil {
		t.Fatalf("mkdir physical bin: %v", err)
	}
	if err := os.Symlink(physicalLocal, filepath.Join(home, ".local")); err != nil {
		t.Fatalf("symlink .local: %v", err)
	}
	installPath := filepath.Join(physicalBin, "codex-proxy")
	logicalPath := filepath.Join(home, ".local", "bin", "codex-proxy")
	if err := os.Symlink(installPath, installPath); err != nil {
		t.Fatalf("create self symlink: %v", err)
	}

	if !sameHelperInstallLocation(logicalPath, installPath, "linux") {
		t.Fatalf("sameHelperInstallLocation(%q, %q) = false, want true through resolved parent fallback", logicalPath, installPath)
	}
}

func TestReplaceSymlinkAtomicallySkipsSameInstallLocationThroughSymlinkedParent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX symlink replacement")
	}
	tmp := t.TempDir()
	home := filepath.Join(tmp, "home")
	physicalLocal := filepath.Join(tmp, "local-overflow")
	physicalBin := filepath.Join(physicalLocal, "bin")
	if err := os.MkdirAll(filepath.Dir(filepath.Join(home, ".local")), 0o755); err != nil {
		t.Fatalf("mkdir home: %v", err)
	}
	if err := os.MkdirAll(physicalBin, 0o755); err != nil {
		t.Fatalf("mkdir physical bin: %v", err)
	}
	if err := os.Symlink(physicalLocal, filepath.Join(home, ".local")); err != nil {
		t.Fatalf("symlink .local: %v", err)
	}
	installPath := filepath.Join(physicalBin, "codex-proxy")
	logicalPath := filepath.Join(home, ".local", "bin", "codex-proxy")
	writeCLIFile(t, installPath, upgradeCXPShimTestScript("1.2.4"), 0o755)

	if err := replaceSymlinkAtomically(logicalPath, installPath); err != nil {
		t.Fatalf("replaceSymlinkAtomically same install location: %v", err)
	}
	if info, err := os.Lstat(installPath); err != nil {
		t.Fatalf("lstat install path: %v", err)
	} else if info.Mode()&os.ModeSymlink != 0 {
		target, _ := os.Readlink(installPath)
		t.Fatalf("install path was replaced by symlink to %q", target)
	}
	if out, err := exec.Command(installPath, "--version").CombinedOutput(); err != nil || !strings.Contains(string(out), "1.2.4") {
		t.Fatalf("install path after skipped replace = %q err=%v, want 1.2.4", out, err)
	}
}

func TestResolveManagedInstallPathPrefersLogicalDefaultThroughSymlinkedLocal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX symlinked managed install path")
	}
	lockCLITestHooks(t)

	prevExecutable := executablePath
	prevArgv0 := restartArgv0
	t.Cleanup(func() {
		executablePath = prevExecutable
		restartArgv0 = prevArgv0
	})

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	home := filepath.Join(tmp, "home")
	physicalLocal := filepath.Join(tmp, "local-overflow")
	physicalBin := filepath.Join(physicalLocal, "bin")
	if err := os.MkdirAll(filepath.Dir(filepath.Join(home, ".local")), 0o755); err != nil {
		t.Fatalf("mkdir home: %v", err)
	}
	if err := os.MkdirAll(physicalBin, 0o755); err != nil {
		t.Fatalf("mkdir physical bin: %v", err)
	}
	if err := os.Symlink(physicalLocal, filepath.Join(home, ".local")); err != nil {
		t.Fatalf("symlink .local: %v", err)
	}
	t.Setenv("HOME", home)

	physicalInstall := filepath.Join(physicalBin, "codex-proxy")
	logicalInstall := filepath.Join(home, ".local", "bin", "codex-proxy")
	writeCLIFile(t, physicalInstall, upgradeCXPShimTestScript("1.2.4"), 0o755)
	recordPath, err := managedinstall.DefaultRecordPath()
	if err != nil {
		t.Fatalf("default record path: %v", err)
	}
	if err := managedinstall.SaveRecord(recordPath, managedinstall.Record{
		TargetPath:   logicalInstall,
		TargetSource: string(managedinstall.SourceDefault),
		TargetState:  string(managedinstall.StateManaged),
		Version:      "1.2.4",
		GOOS:         "linux",
		GOARCH:       runtime.GOARCH,
		Shims:        []string{filepath.Join(home, ".local", "bin", "cxp")},
	}); err != nil {
		t.Fatalf("save install record: %v", err)
	}
	executablePath = func() (string, error) { return physicalInstall, nil }
	restartArgv0 = func() string { return physicalInstall }

	got, err := resolveManagedInstallPathForCLI("")
	if err != nil {
		t.Fatalf("resolve managed install path: %v", err)
	}
	if got != logicalInstall {
		t.Fatalf("resolve managed install path = %q, want logical default %q", got, logicalInstall)
	}
}

func TestFinalizeHelperEntrypointsPreservesLogicalRecordThroughSymlinkedLocal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX symlinked managed install record")
	}
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	home := filepath.Join(tmp, "home")
	physicalLocal := filepath.Join(tmp, "local-overflow")
	physicalBin := filepath.Join(physicalLocal, "bin")
	if err := os.MkdirAll(filepath.Dir(filepath.Join(home, ".local")), 0o755); err != nil {
		t.Fatalf("mkdir home: %v", err)
	}
	if err := os.MkdirAll(physicalBin, 0o755); err != nil {
		t.Fatalf("mkdir physical bin: %v", err)
	}
	if err := os.Symlink(physicalLocal, filepath.Join(home, ".local")); err != nil {
		t.Fatalf("symlink .local: %v", err)
	}
	t.Setenv("HOME", home)

	physicalInstall := filepath.Join(physicalBin, "codex-proxy")
	logicalInstall := filepath.Join(home, ".local", "bin", "codex-proxy")
	logicalCXP := filepath.Join(home, ".local", "bin", "cxp")
	writeCLIFile(t, physicalInstall, upgradeCXPShimTestScript("1.2.4"), 0o755)
	if err := os.Symlink(logicalInstall, logicalCXP); err != nil {
		t.Fatalf("symlink logical cxp: %v", err)
	}
	recordPath, err := managedinstall.DefaultRecordPath()
	if err != nil {
		t.Fatalf("default record path: %v", err)
	}
	if err := managedinstall.SaveRecord(recordPath, managedinstall.Record{
		TargetPath:   logicalInstall,
		TargetSource: string(managedinstall.SourceDefault),
		TargetState:  string(managedinstall.StateManaged),
		Version:      "1.2.3",
		GOOS:         "linux",
		GOARCH:       runtime.GOARCH,
		Shims:        []string{logicalCXP},
	}); err != nil {
		t.Fatalf("save install record: %v", err)
	}

	if err := finalizeHelperEntrypointsAfterUpgrade(physicalInstall, "1.2.4", io.Discard); err != nil {
		t.Fatalf("finalize helper entrypoints: %v", err)
	}

	record, err := managedinstall.LoadRecord(recordPath)
	if err != nil {
		t.Fatalf("load install record: %v", err)
	}
	if record.TargetPath != logicalInstall {
		t.Fatalf("record target path = %q, want logical path %q", record.TargetPath, logicalInstall)
	}
	if !slices.Contains(record.Shims, logicalCXP) {
		t.Fatalf("record shims = %#v, want logical cxp %s", record.Shims, logicalCXP)
	}
	if slices.Contains(record.Shims, filepath.Join(physicalBin, "cxp")) {
		t.Fatalf("record shims should not drift to physical cxp path: %#v", record.Shims)
	}
}

func TestRepairRecordedHelperEntrypointsConvergesLegacyGoBinOnly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX managed alias convergence")
	}
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	home := filepath.Join(tmp, "home")
	t.Setenv("HOME", home)
	canonical := filepath.Join(home, ".local", "bin", "codex-proxy")
	legacyTarget := filepath.Join(home, "go", "bin", "codex-proxy")
	legacyCXP := filepath.Join(home, "go", "bin", "cxp")
	unknown := filepath.Join(home, "bin", "cxp")
	writeCLIFile(t, canonical, upgradeCXPShimTestScript("1.2.4"), 0o755)
	testExecutable, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	if err := copyExecutableAtomically(testExecutable, legacyTarget); err != nil {
		t.Fatalf("copy legacy target fixture: %v", err)
	}
	if err := copyExecutableAtomically(testExecutable, legacyCXP); err != nil {
		t.Fatalf("copy legacy cxp fixture: %v", err)
	}
	writeCLIFile(t, unknown, "#!/bin/sh\necho user-owned\n", 0o755)
	recordPath, err := managedinstall.DefaultRecordPath()
	if err != nil {
		t.Fatalf("DefaultRecordPath: %v", err)
	}
	if err := managedinstall.SaveRecord(recordPath, managedinstall.Record{
		TargetPath:  legacyTarget,
		TargetState: string(managedinstall.StateManaged),
		Version:     "1.2.3",
		GOOS:        runtime.GOOS,
		GOARCH:      runtime.GOARCH,
		Shims:       []string{legacyCXP, unknown},
	}); err != nil {
		t.Fatalf("SaveRecord: %v", err)
	}
	if errs := repairRecordedHelperEntrypointsForInstallPath(canonical); len(errs) != 0 {
		t.Fatalf("repair errors: %v", errs)
	}
	for _, path := range []string{legacyTarget, legacyCXP} {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			t.Fatalf("EvalSymlinks(%s): %v", path, err)
		}
		if resolvedCanonical, _ := filepath.EvalSymlinks(canonical); resolved != resolvedCanonical {
			t.Fatalf("%s resolved to %s, want %s", path, resolved, resolvedCanonical)
		}
	}
	data, err := os.ReadFile(unknown)
	if err != nil {
		t.Fatalf("read unknown binary: %v", err)
	}
	if !strings.Contains(string(data), "user-owned") {
		t.Fatalf("recorded path repurposed by the user was modified: %q", data)
	}
}

func TestRepairRecordedHelperEntrypointsSkipsNonManagedRecord(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX managed alias convergence")
	}
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	home := filepath.Join(tmp, "home")
	t.Setenv("HOME", home)
	canonical := filepath.Join(home, ".local", "bin", "codex-proxy")
	explicit := filepath.Join(home, "custom", "codex-proxy")
	writeCLIFile(t, canonical, upgradeCXPShimTestScript("1.2.4"), 0o755)
	writeCLIFile(t, explicit, upgradeCXPShimTestScript("1.2.3"), 0o755)
	recordPath, err := managedinstall.DefaultRecordPath()
	if err != nil {
		t.Fatalf("DefaultRecordPath: %v", err)
	}
	if err := managedinstall.SaveRecord(recordPath, managedinstall.Record{
		TargetPath:  explicit,
		TargetState: string(managedinstall.StateExplicit),
		Version:     "1.2.3",
		GOOS:        runtime.GOOS,
	}); err != nil {
		t.Fatalf("SaveRecord: %v", err)
	}
	if errs := repairRecordedHelperEntrypointsForInstallPath(canonical); len(errs) != 0 {
		t.Fatalf("repair errors: %v", errs)
	}
	if info, err := os.Lstat(explicit); err != nil {
		t.Fatalf("Lstat explicit target: %v", err)
	} else if info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("explicit non-managed target was rewritten as a symlink: %s", explicit)
	}
	if out, err := exec.Command(explicit, "--version").CombinedOutput(); err != nil || !strings.Contains(string(out), "1.2.3") {
		t.Fatalf("explicit target changed: out=%q err=%v", out, err)
	}
}

func TestFinalizeHelperUpdateResultAcceptsOlderStableDispatcher(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX executable fixture")
	}
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	installPath := filepath.Join(tmp, "bin", "codex-proxy")
	runtimePath := filepath.Join(tmp, ".cxp-runtime", "versions", "v1.2.4", helperruntime.BinaryName(runtime.GOOS))
	if err := helperruntime.Activate(filepath.Join(tmp, ".cxp-runtime"), "v1.2.4"); err != nil {
		t.Fatal(err)
	}
	writeCLIFile(t, installPath, upgradeCXPShimTestScript("1.2.3"), 0o755)
	writeCLIFile(t, runtimePath, upgradeCXPShimTestScript("1.2.4"), 0o755)

	var out bytes.Buffer
	err := finalizeHelperUpdateResult(update.ApplyResult{
		Version:          "1.2.4",
		InstallPath:      installPath,
		RuntimePath:      runtimePath,
		RuntimeActivated: true,
	}, &out)
	if err != nil {
		t.Fatalf("finalize managed runtime update: %v", err)
	}
	if !strings.Contains(out.String(), "reports v1.2.3") || !strings.Contains(out.String(), "immutable runtime v1.2.4 is authoritative") {
		t.Fatalf("expected stale stable dispatcher warning, got %q", out.String())
	}
}

func TestFinalizeHelperUpdateResultRejectsUnrunnableStableDispatcher(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX executable fixture")
	}
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	root := filepath.Join(tmp, ".cxp-runtime")
	installPath := filepath.Join(tmp, "bin", "codex-proxy")
	runtimePath := filepath.Join(root, "versions", "v1.2.4", helperruntime.BinaryName(runtime.GOOS))
	if err := helperruntime.Activate(root, "v1.2.4"); err != nil {
		t.Fatal(err)
	}
	quotedRoot := strings.ReplaceAll(root, "'", "'\\''")
	writeCLIFile(t, installPath, "#!/bin/sh\nprintf 'v1.2.3\\n' > '"+quotedRoot+"/active'\nexit 23\n", 0o755)
	writeCLIFile(t, runtimePath, upgradeCXPShimTestScript("1.2.4"), 0o755)

	err := finalizeHelperUpdateResult(update.ApplyResult{
		Version:          "1.2.4",
		InstallPath:      installPath,
		RuntimePath:      runtimePath,
		RuntimeActivated: true,
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "stable cxp entrypoint") {
		t.Fatalf("finalize error = %v, want unrunnable stable entry failure", err)
	}
	if active, readErr := helperruntime.ReadActive(root); readErr != nil || active != "v1.2.4" {
		t.Fatalf("active after unrunnable stable probe = %q, %v; want target restored", active, readErr)
	}
}

func TestFinalizeHelperUpdateResultRejectsStableDispatcherThatReactivatesOlderRuntime(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX executable fixture")
	}
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	root := filepath.Join(tmp, ".cxp-runtime")
	installPath := filepath.Join(tmp, "bin", "codex-proxy")
	runtimePath := filepath.Join(root, "versions", "v1.2.4", helperruntime.BinaryName(runtime.GOOS))
	if err := helperruntime.Activate(root, "v1.2.4"); err != nil {
		t.Fatal(err)
	}
	quotedRoot := strings.ReplaceAll(root, "'", "'\\''")
	stableScript := "#!/bin/sh\n" +
		"printf 'v1.2.3\\n' > '" + quotedRoot + "/active'\n" +
		"echo 'codex-proxy version 1.2.3 (old dispatcher)'\n"
	writeCLIFile(t, installPath, stableScript, 0o755)
	writeCLIFile(t, runtimePath, upgradeCXPShimTestScript("1.2.4"), 0o755)

	err := finalizeHelperUpdateResult(update.ApplyResult{
		Version:          "1.2.4",
		InstallPath:      installPath,
		RuntimePath:      runtimePath,
		RuntimeActivated: true,
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "changed active runtime") {
		t.Fatalf("finalize error = %v, want active-pointer regression", err)
	}
	if active, readErr := helperruntime.ReadActive(root); readErr != nil || active != "v1.2.4" {
		t.Fatalf("active after rejected stable probe = %q, %v; want target restored", active, readErr)
	}
}

func TestFinalizeHelperEntrypointsRepairsAndVerifiesCXPEntrypointVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX cxp entrypoint verification")
	}
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	installPath := filepath.Join(tmp, "bin", "codex-proxy")
	shimPath := filepath.Join(tmp, "bin", "cxp")
	stalePath := filepath.Join(tmp, "old", "codex-proxy")
	writeCLIFile(t, installPath, upgradeCXPShimTestScript("1.2.4"), 0o755)
	writeCLIFile(t, stalePath, upgradeCXPShimTestScript("1.2.3"), 0o755)
	if err := os.Symlink(stalePath, shimPath); err != nil {
		t.Fatalf("create stale cxp shim: %v", err)
	}

	if err := finalizeHelperEntrypointsAfterUpgrade(installPath, "1.2.4", io.Discard); err != nil {
		t.Fatalf("finalize helper entrypoints: %v", err)
	}
	if out, err := exec.Command(shimPath, "--version").CombinedOutput(); err != nil || !strings.Contains(string(out), "1.2.4") {
		t.Fatalf("cxp after finalize = %q err=%v, want 1.2.4", out, err)
	}
	if target, err := os.Readlink(shimPath); err != nil || target != installPath {
		t.Fatalf("cxp shim = %q err=%v, want %s", target, err, installPath)
	}
}

func TestFinalizeHelperEntrypointsFailsWhenCXPEntrypointCannotBeVerified(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX cxp entrypoint verification")
	}
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	installPath := filepath.Join(tmp, "bin", "codex-proxy")
	shimPath := filepath.Join(tmp, "bin", "cxp")
	writeCLIFile(t, installPath, upgradeCXPShimTestScript("1.2.4"), 0o755)
	if err := os.MkdirAll(shimPath, 0o755); err != nil {
		t.Fatalf("mkdir blocked cxp path: %v", err)
	}

	err := finalizeHelperEntrypointsAfterUpgrade(installPath, "1.2.4", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "cxp entrypoint") {
		t.Fatalf("finalize error = %v, want cxp entrypoint verification failure", err)
	}
}

func TestFinalizeHelperEntrypointsMigratesCXPRecordTargetToManagedBinary(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX managed record migration")
	}
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	installPath := filepath.Join(tmp, "bin", "codex-proxy")
	shimPath := filepath.Join(tmp, "bin", "cxp")
	writeCLIFile(t, installPath, upgradeCXPShimTestScript("1.2.4"), 0o755)
	if err := os.Symlink(installPath, shimPath); err != nil {
		t.Fatalf("create cxp shim: %v", err)
	}
	recordPath, err := managedinstall.DefaultRecordPath()
	if err != nil {
		t.Fatalf("default record path: %v", err)
	}
	if err := managedinstall.SaveRecord(recordPath, managedinstall.Record{
		TargetPath:   shimPath,
		TargetSource: string(managedinstall.SourceRecord),
		TargetState:  string(managedinstall.StateManaged),
		Version:      "1.2.3",
		GOOS:         "linux",
		GOARCH:       runtime.GOARCH,
		Shims:        []string{shimPath},
	}); err != nil {
		t.Fatalf("save install record: %v", err)
	}

	if err := finalizeHelperEntrypointsAfterUpgrade(installPath, "1.2.4", io.Discard); err != nil {
		t.Fatalf("finalize helper entrypoints: %v", err)
	}
	record, err := managedinstall.LoadRecord(recordPath)
	if err != nil {
		t.Fatalf("load install record: %v", err)
	}
	if managedinstall.IsShimEntryPath(record.TargetPath, runtime.GOOS) || !managedinstall.IsCanonicalTargetPath(record.TargetPath, runtime.GOOS) {
		t.Fatalf("record target path = %q, want canonical managed binary target", record.TargetPath)
	}
	hasCXPShim := false
	for _, shim := range record.Shims {
		if filepath.Base(shim) == "cxp" {
			hasCXPShim = true
		}
	}
	if !hasCXPShim {
		t.Fatalf("record shims = %#v, want a cxp shim", record.Shims)
	}
}

func TestReplaceSymlinkAtomicallyRejectsTargetChainThroughLinkPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX symlink loop prevention")
	}
	tmp := t.TempDir()
	linkPath := filepath.Join(tmp, "cxp")
	targetPath := filepath.Join(tmp, "codex-proxy")
	if err := os.Symlink(linkPath, targetPath); err != nil {
		t.Fatalf("create target chain through link: %v", err)
	}

	err := replaceSymlinkAtomically(linkPath, targetPath)
	if err == nil || !strings.Contains(err.Error(), "target symlink chain references the link path") {
		t.Fatalf("replaceSymlinkAtomically error = %v, want target-chain rejection", err)
	}
}

func TestCurrentHelperInstallCandidatesTreatCXPAsShimOnly(t *testing.T) {
	tmp := t.TempDir()
	cxp := filepath.Join(tmp, "cxp")
	managed := filepath.Join(tmp, helperpath.BinaryName("linux"))
	writeCLIFile(t, cxp, upgradeCXPShimTestScript("1.2.4"), 0o755)
	got := currentHelperInstallCandidatesForGOOS(cxp, "linux", os.Stat)
	if !slices.Equal(got, []string{managed}) {
		t.Fatalf("current helper candidates = %#v, want only managed sibling %#v", got, []string{managed})
	}
}

func upgradeCXPShimTestScript(version string) string {
	return "#!/bin/sh\n" +
		"if [ \"$1\" = \"skills\" ] && [ \"$2\" = \"install-builtin\" ]; then exit 0; fi\n" +
		"if [ \"$1\" = \"--version\" ]; then echo 'codex-proxy version " + version + " (test)'; exit 0; fi\n" +
		"exit 0\n"
}

func TestEnsureCXPShimForInstallPathCreatesWindowsCmdShim(t *testing.T) {
	tmp := t.TempDir()
	installPath := filepath.Join(tmp, "codex-proxy.exe")
	writeCLIFile(t, installPath, "binary", 0o755)

	if err := ensureCXPShimForInstallPathForGOOS(installPath, "windows"); err != nil {
		t.Fatalf("ensure windows cxp shim: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(tmp, "cxp.cmd"))
	if err != nil {
		t.Fatalf("read cxp.cmd: %v", err)
	}
	if string(data) != windowsCXPShimContent() {
		t.Fatalf("cxp.cmd = %q, want %q", data, windowsCXPShimContent())
	}
	stable, err := os.ReadFile(filepath.Join(tmp, "cxp.exe"))
	if err != nil {
		t.Fatalf("read stable cxp.exe: %v", err)
	}
	if string(stable) != "binary" {
		t.Fatalf("stable cxp.exe = %q, want install binary", stable)
	}
}

func TestRefreshWindowsStableCXPExecutableReplacesOwnedStaleBinary(t *testing.T) {
	tmp := t.TempDir()
	installPath := filepath.Join(tmp, "codex-proxy.exe")
	stablePath := filepath.Join(tmp, "cxp.exe")
	shimPath := filepath.Join(tmp, "cxp.cmd")
	writeCLIFile(t, installPath, "new-binary", 0o755)
	writeCLIFile(t, stablePath, "old-binary", 0o755)
	writeCLIFile(t, shimPath, windowsCXPShimContent(), 0o755)

	if err := refreshWindowsStableCXPExecutable(installPath); err != nil {
		t.Fatalf("refresh stable cxp.exe: %v", err)
	}
	data, err := os.ReadFile(stablePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "new-binary" {
		t.Fatalf("stable cxp.exe = %q, want refreshed install binary", data)
	}
}

func TestRefreshWindowsStableCXPExecutableFromPendingSource(t *testing.T) {
	tmp := t.TempDir()
	installPath := filepath.Join(tmp, "codex-proxy.exe")
	stablePath := filepath.Join(tmp, "cxp.exe")
	shimPath := filepath.Join(tmp, "cxp.cmd")
	pendingPath := filepath.Join(tmp, ".codex-proxy_1.2.4_windows_amd64.exe.pending")
	writeCLIFile(t, installPath, "old-binary", 0o755)
	writeCLIFile(t, stablePath, "old-binary", 0o755)
	writeCLIFile(t, shimPath, windowsCXPShimContent(), 0o755)
	writeCLIFile(t, pendingPath, "new-binary", 0o755)

	if err := refreshWindowsStableCXPExecutableFromSource(installPath, pendingPath); err != nil {
		t.Fatalf("refresh stable cxp.exe from pending source: %v", err)
	}
	stable, err := os.ReadFile(stablePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(stable) != "new-binary" {
		t.Fatalf("stable cxp.exe = %q, want pending binary", stable)
	}
	installed, err := os.ReadFile(installPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(installed) != "old-binary" {
		t.Fatalf("pending cxp convergence changed primary install to %q", installed)
	}
}

func TestRefreshWindowsStableCXPExecutableFromPendingPreservesCustomEntrypoint(t *testing.T) {
	tmp := t.TempDir()
	installPath := filepath.Join(tmp, "codex-proxy.exe")
	stablePath := filepath.Join(tmp, "cxp.exe")
	shimPath := filepath.Join(tmp, "cxp.cmd")
	pendingPath := filepath.Join(tmp, ".codex-proxy_1.2.4_windows_amd64.exe.pending")
	writeCLIFile(t, installPath, "old-binary", 0o755)
	writeCLIFile(t, stablePath, "custom-binary", 0o755)
	writeCLIFile(t, shimPath, "@echo off\r\necho custom\r\n", 0o755)
	writeCLIFile(t, pendingPath, "new-binary", 0o755)

	if err := refreshWindowsStableCXPExecutableFromSource(installPath, pendingPath); err != nil {
		t.Fatalf("preserve custom cxp.exe: %v", err)
	}
	stable, err := os.ReadFile(stablePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(stable) != "custom-binary" {
		t.Fatalf("custom cxp.exe changed to %q", stable)
	}
}

func TestRefreshWindowsStableCXPExecutableDoesNotReplaceMatchingBinary(t *testing.T) {
	tmp := t.TempDir()
	installPath := filepath.Join(tmp, "codex-proxy.exe")
	stablePath := filepath.Join(tmp, "cxp.exe")
	shimPath := filepath.Join(tmp, "cxp.cmd")
	writeCLIFile(t, installPath, "same-binary", 0o755)
	writeCLIFile(t, stablePath, "same-binary", 0o755)
	writeCLIFile(t, shimPath, windowsCXPShimContent(), 0o755)
	before, err := os.Stat(stablePath)
	if err != nil {
		t.Fatal(err)
	}

	if err := refreshWindowsStableCXPExecutable(installPath); err != nil {
		t.Fatalf("refresh matching stable cxp.exe: %v", err)
	}
	after, err := os.Stat(stablePath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("matching stable cxp.exe was unnecessarily replaced")
	}
}

func TestRefreshWindowsStableCXPExecutablePreservesCustomEntrypoint(t *testing.T) {
	tmp := t.TempDir()
	installPath := filepath.Join(tmp, "codex-proxy.exe")
	stablePath := filepath.Join(tmp, "cxp.exe")
	shimPath := filepath.Join(tmp, "cxp.cmd")
	writeCLIFile(t, installPath, "new-binary", 0o755)
	writeCLIFile(t, stablePath, "user-binary", 0o755)
	writeCLIFile(t, shimPath, "@echo off\r\necho custom\r\n", 0o755)

	if err := refreshWindowsStableCXPExecutable(installPath); err != nil {
		t.Fatalf("refresh custom cxp entrypoint: %v", err)
	}
	data, err := os.ReadFile(stablePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "user-binary" {
		t.Fatalf("custom cxp.exe changed to %q", data)
	}
}

func TestCopyExecutableAtomicallyReplacesExistingFile(t *testing.T) {
	tmp := t.TempDir()
	source := filepath.Join(tmp, "source")
	target := filepath.Join(tmp, "target")
	writeCLIFile(t, source, "replacement", 0o755)
	writeCLIFile(t, target, "old", 0o755)

	if err := copyExecutableAtomically(source, target); err != nil {
		t.Fatalf("copy executable atomically: %v", err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "replacement" {
		t.Fatalf("target = %q, want replacement", data)
	}
}

func TestEnsureCXPShimForInstallPathRepairsWindowsCmdShim(t *testing.T) {
	tmp := t.TempDir()
	installPath := filepath.Join(tmp, "codex-proxy.exe")
	shimPath := filepath.Join(tmp, "cxp.cmd")
	writeCLIFile(t, installPath, "binary", 0o755)
	writeCLIFile(t, shimPath, "@echo off\r\n\"C:\\old\\codex-proxy.exe\" %*\r\n", 0o755)

	if err := ensureCXPShimForInstallPathForGOOS(installPath, "windows"); err != nil {
		t.Fatalf("ensure windows cxp shim: %v", err)
	}
	data, err := os.ReadFile(shimPath)
	if err != nil {
		t.Fatalf("read cxp.cmd: %v", err)
	}
	if string(data) != windowsCXPShimContent() {
		t.Fatalf("cxp.cmd = %q, want %q", data, windowsCXPShimContent())
	}
}

func TestEnsureCXPShimForInstallPathNormalizesManagedWindowsCmdShim(t *testing.T) {
	tmp := t.TempDir()
	installPath := filepath.Join(tmp, "codex-proxy.exe")
	shimPath := filepath.Join(tmp, "cxp.cmd")
	writeCLIFile(t, installPath, "binary", 0o755)
	writeCLIFile(t, shimPath, "\xef\xbb\xbf@ECHO OFF\n\"%~dp0CXP.EXE\" %*\n", 0o755)

	if err := ensureCXPShimForInstallPathForGOOS(installPath, "windows"); err != nil {
		t.Fatalf("ensure windows cxp shim: %v", err)
	}
	data, err := os.ReadFile(shimPath)
	if err != nil {
		t.Fatalf("read cxp.cmd: %v", err)
	}
	if string(data) != windowsCXPShimContent() {
		t.Fatalf("cxp.cmd = %q, want normalized %q", data, windowsCXPShimContent())
	}
}

func TestEnsureCXPShimForInstallPathKeepsNonHelperWindowsCmd(t *testing.T) {
	tmp := t.TempDir()
	installPath := filepath.Join(tmp, "codex-proxy.exe")
	shimPath := filepath.Join(tmp, "cxp.cmd")
	custom := "@echo off\r\necho custom\r\n"
	writeCLIFile(t, installPath, "binary", 0o755)
	writeCLIFile(t, shimPath, custom, 0o755)

	if err := ensureCXPShimForInstallPathForGOOS(installPath, "windows"); err != nil {
		t.Fatalf("ensure windows cxp shim: %v", err)
	}
	data, err := os.ReadFile(shimPath)
	if err != nil {
		t.Fatalf("read cxp.cmd: %v", err)
	}
	if string(data) != custom {
		t.Fatalf("cxp.cmd = %q, want custom content unchanged", data)
	}
}

func TestRewriteLegacyCXPProfilePreservesFormattingAndUnrelatedContent(t *testing.T) {
	profile := filepath.Join(t.TempDir(), "Microsoft.PowerShell_profile.ps1")
	before := []byte("# user content\r\nSet-Alias -Name cxp -Value codex-proxy\r\n$env:KEEP = 'yes'\r\n")
	if err := os.WriteFile(profile, before, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := rewriteLegacyCXPProfile(profile); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(profile)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("# user content\r\nSet-Alias -Name cxp -Value cxp.exe\r\n$env:KEEP = 'yes'\r\n")
	if !bytes.Equal(after, want) {
		t.Fatalf("profile rewrite changed unexpected bytes:\n got %q\nwant %q", after, want)
	}
	info, err := os.Stat(profile)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o640 {
		t.Fatalf("profile permissions = %o, want 640", info.Mode().Perm())
	}
}

func TestRewriteLegacyCXPProfileOnlyChangesExactAliasLines(t *testing.T) {
	profile := filepath.Join(t.TempDir(), "Microsoft.PowerShell_profile.ps1")
	before := []byte("\xef\xbb\xbfSet-Alias -Name cxp -Value codex-proxy\r\n# Set-Alias -Name cxp -Value codex-proxy\r\nSet-Alias -Name cxp -Value codex-proxy -Scope Global\r\n$note = 'Set-Alias -Name cxp -Value codex-proxy'\r\n")
	if err := os.WriteFile(profile, before, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := rewriteLegacyCXPProfile(profile); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(profile)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("\xef\xbb\xbfSet-Alias -Name cxp -Value cxp.exe\r\n# Set-Alias -Name cxp -Value codex-proxy\r\nSet-Alias -Name cxp -Value codex-proxy -Scope Global\r\n$note = 'Set-Alias -Name cxp -Value codex-proxy'\r\n")
	if !bytes.Equal(after, want) {
		t.Fatalf("profile rewrite changed non-exact content\ngot:  %q\nwant: %q", after, want)
	}
}

func TestUpgradeCmdInstallsBundledSkillsWithUpdatedBinary(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script helper stub is POSIX-only")
	}
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	root := t.TempDir()
	marker := filepath.Join(root, "builtin-install-called")
	helperPath := filepath.Join(root, "codex-proxy")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then echo 'codex-proxy version 1.2.3 (test)'; exit 0; fi\n" +
		"if [ \"$1\" = \"skills\" ] && [ \"$2\" = \"install-builtin\" ] && [ \"$3\" = \"--yes\" ]; then\n" +
		"  printf called > \"" + marker + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 64\n"
	if err := os.WriteFile(helperPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write helper stub: %v", err)
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{Version: "1.2.3", InstallPath: helperPath}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3", "--install-path", helperPath})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("updated binary was not asked to install bundled skills: %v", err)
	}
}

func TestUpgradeCmdBuiltinSkillInstallFailureWarnsButUpgradeSucceeds(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script helper stub is POSIX-only")
	}
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	root := t.TempDir()
	helperPath := filepath.Join(root, "codex-proxy")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then echo 'codex-proxy version 1.2.3 (test)'; exit 0; fi\n" +
		"if [ \"$1\" = \"skills\" ] && [ \"$2\" = \"install-builtin\" ] && [ \"$3\" = \"--yes\" ]; then\n" +
		"  echo builtin install failed >&2\n" +
		"  exit 42\n" +
		"fi\n" +
		"exit 64\n"
	if err := os.WriteFile(helperPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write helper stub: %v", err)
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{Version: "1.2.3", InstallPath: helperPath}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3", "--install-path", helperPath})
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade should succeed despite builtin skill warning: %v\nstdout=%s\nstderr=%s", err, out.String(), stderr.String())
	}
	if !strings.Contains(out.String(), "Updated to v1.2.3.") {
		t.Fatalf("upgrade success output missing: %q", out.String())
	}
	if !strings.Contains(stderr.String(), "Warning: failed to install built-in cxp skill after upgrade") || !strings.Contains(stderr.String(), "builtin install failed") {
		t.Fatalf("expected builtin install warning with detail, got %q", stderr.String())
	}
}

func TestUpgradeCmdUsesAutoUpdateInstallLock(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		t.Fatal("PerformUpdate should not run while the shared auto-update lock is held")
		return update.ApplyResult{}, nil
	}

	installPath := filepath.Join(t.TempDir(), "codex-proxy")
	lock := flock.New(installPath + ".auto-update.lock")
	locked, err := lock.TryLock()
	if err != nil {
		t.Fatalf("TryLock error: %v", err)
	}
	if !locked {
		t.Fatal("failed to acquire test auto-update lock")
	}
	t.Cleanup(func() { _ = lock.Unlock() })

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3", "--install-path", installPath})
	err = cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "another codex-helper upgrade is already using install path") {
		t.Fatalf("expected shared install lock error, got %v", err)
	}
}

func TestUpgradeCmdFallsBackToCacheLockWhenInstallPathLockUnavailable(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("uses Linux procfs as an unwritable install-lock directory")
	}
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performCalls := 0
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		performCalls++
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3", "--install-path", "/proc/codex-proxy"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("upgrade should use cache lock when install-path lock is unavailable: %v", err)
	}
	if performCalls != 1 {
		t.Fatalf("performUpdate calls = %d, want 1", performCalls)
	}
}

func TestHelperInstallLockPathsUseSymlinkAwareCacheKey(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX symlinked install location")
	}
	tmp := t.TempDir()
	t.Setenv("HOME", filepath.Join(tmp, "home"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "cache"))
	home := filepath.Join(tmp, "user-home")
	physicalLocal := filepath.Join(tmp, "local-overflow")
	physicalBin := filepath.Join(physicalLocal, "bin")
	if err := os.MkdirAll(filepath.Dir(filepath.Join(home, ".local")), 0o755); err != nil {
		t.Fatalf("mkdir home: %v", err)
	}
	if err := os.MkdirAll(physicalBin, 0o755); err != nil {
		t.Fatalf("mkdir physical bin: %v", err)
	}
	if err := os.Symlink(physicalLocal, filepath.Join(home, ".local")); err != nil {
		t.Fatalf("symlink .local: %v", err)
	}
	logicalPath := filepath.Join(home, ".local", "bin", "codex-proxy")
	physicalPath := filepath.Join(physicalBin, "codex-proxy")

	logicalLocks := helperInstallLockPaths(logicalPath)
	physicalLocks := helperInstallLockPaths(physicalPath)
	if len(logicalLocks) < 2 || len(physicalLocks) < 2 {
		t.Fatalf("helperInstallLockPaths returned too few paths: logical=%v physical=%v", logicalLocks, physicalLocks)
	}
	if logicalLocks[1] != physicalLocks[1] {
		t.Fatalf("cache lock differs for same symlinked install location:\nlogical:  %s\nphysical: %s", logicalLocks[1], physicalLocks[1])
	}
}

func TestUpgradeCmdLatestCanIncludePrerelease(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(_ context.Context, opts update.CheckOptions) update.Status {
		if !opts.IncludePrerelease {
			t.Fatalf("CheckForUpdate IncludePrerelease = false, want true")
		}
		return update.Status{Supported: true, UpdateAvailable: true}
	}
	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		return update.ApplyResult{Version: "1.2.4-rc.1"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--repo", "owner/name", "--include-prerelease"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if got.Repo != "owner/name" || got.Version != "" || !got.IncludePrerelease || !got.ValidateBinary {
		t.Fatalf("PerformUpdate options = %#v, want latest prerelease lookup", got)
	}
	if !strings.Contains(out.String(), "Updated to v1.2.4-rc.1.") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestUpgradeCmdRestartRequiredMessage(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)
	tmp := t.TempDir()
	pendingPath := filepath.Join(tmp, ".codex-proxy_1.2.3.tmp")
	marker := filepath.Join(tmp, "pending-builtin-install-called")
	if runtime.GOOS != "windows" {
		script := "#!/bin/sh\n" +
			"if [ \"$1\" = \"skills\" ] && [ \"$2\" = \"install-builtin\" ] && [ \"$3\" = \"--yes\" ]; then\n" +
			"  printf called > \"" + marker + "\"\n" +
			"  exit 0\n" +
			"fi\n" +
			"exit 64\n"
		if err := os.WriteFile(pendingPath, []byte(script), 0o755); err != nil {
			t.Fatalf("write pending helper stub: %v", err)
		}
	}

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		return update.Status{Supported: true, UpdateAvailable: true}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{Version: "1.2.3", RestartRequired: true, PendingReplacePath: pendingPath}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if !strings.Contains(out.String(), "Update replacement for v1.2.3 is pending.") ||
		!strings.Contains(out.String(), "verify `codex-proxy --version`") {
		t.Fatalf("unexpected output: %q", out.String())
	}
	if runtime.GOOS != "windows" {
		if _, err := os.Stat(marker); err != nil {
			t.Fatalf("pending replacement binary was not asked to install bundled skills: %v", err)
		}
	}
}

func TestUpgradeCmdPropagatesUpdateError(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		return update.Status{Supported: false, Error: "network unavailable"}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{}, errors.New("download failed")
	}

	cmd := newUpgradeCmd(&rootOptions{})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "download failed") {
		t.Fatalf("expected update error, got %v", err)
	}
}

func TestUpgradeCmdDefaultDoesNotTouchTeamsServiceOrRequestUAC(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	helperPath := filepath.Join(tmp, "codex-proxy")
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			errTeamsKeepaliveAccessDeniedForTest{},
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            helperPath,
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		writeCLIFile(t, helperPath, upgradeCXPShimTestScript("1.2.3"), 0o755)
		return update.ApplyResult{Version: "1.2.3", InstallPath: helperPath}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3", "--install-path", helperPath})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute default upgrade: %v\n%s", err, out.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("default upgrade must not query, stop, refresh, or restart Teams service, calls=%#v", runner.calls)
	}
	for _, unexpected := range []string{
		"Stopping Teams service before upgrade",
		"Refreshing Teams service config before restart",
		"Restarting Teams service after upgrade",
		"NEXT STEP: TYPE yes TO CONTINUE",
	} {
		if strings.Contains(out.String(), unexpected) {
			t.Fatalf("default upgrade output should not contain %q:\n%s", unexpected, out.String())
		}
	}
	if !strings.Contains(out.String(), "Updated to v1.2.3.") ||
		!strings.Contains(out.String(), "helper restart now") {
		t.Fatalf("default upgrade output missing update result and manual activation hint:\n%s", out.String())
	}
}

func TestUpgradeCmdDefaultDoesNotRestartTeamsServiceAfterUpdateError(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{}, errors.New("context deadline exceeded (Client.Timeout or context cancellation while reading body)")
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("execute default upgrade error = %v, want context deadline exceeded\n%s", err, out.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("failed default upgrade must not stop or restart Teams service, calls=%#v", runner.calls)
	}
	if strings.Contains(out.String(), "Stopping Teams service") || strings.Contains(out.String(), "Restarting Teams service") {
		t.Fatalf("failed default upgrade output should not include service lifecycle messages:\n%s", out.String())
	}
}

func isolateUpgradeTeamsServiceForTest(t *testing.T) *recordingTeamsServiceRunner {
	t.Helper()
	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})
	return runner
}

func isolateUpgradeTeamsStateForTest(t *testing.T, tmp string) {
	t.Helper()
	t.Setenv(envTeamsCodexChild, "")
	t.Setenv(envTeamsCodexParentPID, "")
	t.Setenv(update.EnvInstallPath, "")
	t.Setenv(update.EnvInstallDir, "")
	t.Setenv(update.EnvRepo, "")
	t.Setenv(update.EnvVersion, "")
	isolateTeamsUserDirsForTest(t, tmp)
}

func teamsServiceJoinedCalls(calls []teamsServiceCommandCall) string {
	var parts []string
	for _, call := range calls {
		parts = append(parts, call.name+" "+strings.Join(call.args, " "))
	}
	return strings.Join(parts, "\n")
}

func TestUpgradeCmdStopsAndRestartsActiveTeamsServiceAroundUpdate(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:38471")

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevPoll := teamsUpgradePollInterval
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		teamsUpgradePollInterval = prevPoll
	})
	teamsUpgradePollInterval = time.Millisecond
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !teamsServiceCallSeen(runner.calls, "stop") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs(upgradeArgsWithTeamsRestart("--version", "v1.2.3", "--teams-drain-timeout", "1s"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if !teamsServiceCallSeen(runner.calls, "is-active") || !teamsServiceCallSeen(runner.calls, "stop") || !teamsServiceCallSeen(runner.calls, "start") {
		t.Fatalf("expected is-active, stop, and start calls, got %#v", runner.calls)
	}
	if !strings.Contains(out.String(), "Stopping Teams service before upgrade") ||
		!strings.Contains(out.String(), "Refreshing Teams service config before restart") ||
		!strings.Contains(out.String(), "Restarting Teams service after upgrade") {
		t.Fatalf("upgrade output missing service restart messages:\n%s", out.String())
	}
	unit, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read refreshed unit: %v", err)
	}
	if !strings.Contains(string(unit), "Environment=HTTP_PROXY=http://127.0.0.1:38471") {
		t.Fatalf("upgrade should refresh Teams service env with current proxy, unit:\n%s", string(unit))
	}
}

func TestUpgradeCmdWSLAccessDeniedRefreshUsesUACRepair(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			nil,
			nil,
			nil,
			nil,
			nil,
			[]byte("DESKTOP\\alice\n"),
			nil,
			nil,
		},
		errs: []error{
			nil,
			nil,
			errors.New("task config mismatch"),
			errTeamsKeepaliveAccessDeniedForTest{},
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
			nil,
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !strings.Contains(teamsServiceJoinedCalls(runner.calls), "Stop-ScheduledTask -TaskName $taskName") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs(upgradeArgsWithTeamsRestart("--version", "v1.2.3"))
	cmd.SetIn(strings.NewReader("yes\n"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "NEXT STEP: TYPE yes TO CONTINUE") ||
		!strings.Contains(out.String(), "Restarting Teams service after upgrade") {
		t.Fatalf("upgrade output missing UAC and restart messages:\n%s", out.String())
	}
	if strings.Contains(out.String(), "Register-ScheduledTask") {
		t.Fatalf("upgrade output should not include noisy PowerShell registration details:\n%s", out.String())
	}
	joined := teamsServiceJoinedCalls(runner.calls)
	if !strings.Contains(joined, "Start-Process -FilePath 'powershell.exe'") {
		t.Fatalf("expected elevated repair command, calls=%#v", runner.calls)
	}
	lastCall := runner.calls[len(runner.calls)-1]
	lastJoined := lastCall.name + " " + strings.Join(lastCall.args, " ")
	if !strings.Contains(lastJoined, "Start-CodexHelperScheduledTaskIfStopped $taskName") || strings.Contains(lastJoined, "Register-ScheduledTask") {
		t.Fatalf("expected normal Scheduled Task start after elevated repair, last call=%#v all calls=%#v", lastCall, runner.calls)
	}
}

func TestUpgradeCmdWSLMatchingTaskRefreshSkipsRepairAndUAC(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &scriptedTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !strings.Contains(teamsServiceJoinedCalls(runner.calls), "Stop-ScheduledTask -TaskName $taskName") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs(upgradeArgsWithTeamsRestart("--version", "v1.2.3"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if strings.Contains(out.String(), "NEXT STEP: TYPE yes TO CONTINUE") ||
		strings.Contains(out.String(), "NOTICE: USING STARTUP WATCHDOG FALLBACK") {
		t.Fatalf("matching task refresh should be quiet, got output:\n%s", out.String())
	}
	joined := teamsServiceJoinedCalls(runner.calls)
	if strings.Contains(joined, "Register-ScheduledTask") {
		t.Fatalf("matching task refresh should not re-register Scheduled Tasks, calls=%#v", runner.calls)
	}
	if !strings.Contains(joined, "$allMatched = $true") {
		t.Fatalf("expected read-only task match probe before repair, calls=%#v", runner.calls)
	}
	for _, want := range []string{
		"Test-CodexHelperTaskDurationMinutes",
		"$task.Principal.UserId -ne $expectedPrincipalUser",
		"$task.Principal.LogonType -ne 'Interactive'",
		"$task.Principal.RunLevel -ne 'Limited'",
		"$settings.MultipleInstances -ne 'IgnoreNew'",
		"$settings.RestartCount -ne 999",
		"$settings.RestartInterval 1",
		"$settings.ExecutionTimeLimit 0",
		"$expectedActionExecute = 'wscript.exe'",
		"Test-Path -LiteralPath $launcherPowerShellPath",
		"Test-Path -LiteralPath $launcherVbsPath",
		"Get-Content -LiteralPath $launcherPowerShellPath -Raw",
		"Get-Content -LiteralPath $launcherVbsPath -Raw",
		"$expectedLauncherPowerShell",
		"$expectedLauncherVbs",
		"$hasLogonTrigger",
		"$hasRepeatingTrigger",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("matching probe should verify %q before skipping repair, calls=%#v", want, runner.calls)
		}
	}
	lastCall := runner.calls[len(runner.calls)-1]
	lastJoined := lastCall.name + " " + strings.Join(lastCall.args, " ")
	if !strings.Contains(lastJoined, "Enable-ScheduledTask -TaskName $taskName") ||
		!strings.Contains(lastJoined, "Start-CodexHelperScheduledTaskIfStopped $taskName") {
		t.Fatalf("expected normal Scheduled Task start, last call=%#v all calls=%#v", lastCall, runner.calls)
	}
}

func TestUpgradeCmdWSLMismatchRefreshRepairsWithoutUACWhenAllowed(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			nil,
			nil,
			errors.New("task config mismatch"),
			nil,
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !strings.Contains(teamsServiceJoinedCalls(runner.calls), "Stop-ScheduledTask -TaskName $taskName") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs(upgradeArgsWithTeamsRestart("--version", "v1.2.3"))
	cmd.SetIn(strings.NewReader("yes\n"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if strings.Contains(out.String(), "NEXT STEP: TYPE yes TO CONTINUE") ||
		strings.Contains(out.String(), "NOTICE: USING STARTUP WATCHDOG FALLBACK") {
		t.Fatalf("repairable mismatch should not ask for UAC or fallback:\n%s", out.String())
	}
	joined := teamsServiceJoinedCalls(runner.calls)
	if !strings.Contains(joined, "Register-ScheduledTask -TaskName $taskName") {
		t.Fatalf("mismatched task should be repaired with normal registration, calls=%#v", runner.calls)
	}
	if strings.Contains(joined, "Start-Process -FilePath 'powershell.exe'") {
		t.Fatalf("normal registration success should not use elevated repair, calls=%#v", runner.calls)
	}
}

func TestUpgradeCmdWSLAccessDeniedRefreshFallsBackWithoutUACWhenCleanupIsAllowed(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			nil,
			nil,
			errors.New("task config mismatch"),
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !strings.Contains(teamsServiceJoinedCalls(runner.calls), "Stop-ScheduledTask -TaskName $taskName") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs(upgradeArgsWithTeamsRestart("--version", "v1.2.3"))
	cmd.SetIn(strings.NewReader(""))
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "NOTICE: USING STARTUP WATCHDOG FALLBACK") ||
		!strings.Contains(out.String(), "Restarting Teams service after upgrade") {
		t.Fatalf("upgrade output missing fallback and restart messages:\n%s", out.String())
	}
	if strings.Contains(out.String(), "NEXT STEP: TYPE yes TO CONTINUE") ||
		strings.Contains(out.String(), "UAC prompt") {
		t.Fatalf("upgrade should use quiet fallback without asking for UAC when old tasks can be retired normally:\n%s", out.String())
	}
	lastCall := runner.calls[len(runner.calls)-1]
	lastJoined := lastCall.name + " " + strings.Join(lastCall.args, " ")
	if !strings.Contains(teamsServiceJoinedCalls(runner.calls), "Disable-ScheduledTask") {
		t.Fatalf("fallback should retire old WSL Scheduled Tasks before installing Startup fallback, calls=%#v", runner.calls)
	}
	if strings.Contains(lastJoined, "Start-ScheduledTask -TaskName $taskName") {
		t.Fatalf("fallback start should not call Scheduled Task start after access denied, last call=%#v all calls=%#v", lastCall, runner.calls)
	}
	if strings.Contains(teamsServiceJoinedCalls(runner.calls), "-Verb RunAs") {
		t.Fatalf("quiet fallback must not use elevated PowerShell, calls=%#v", runner.calls)
	}
	if !strings.Contains(lastJoined, "Start-Process -FilePath 'wscript.exe'") || !strings.Contains(lastJoined, "WScript.Shell") ||
		strings.Contains(lastJoined, "Set-Content -LiteralPath $legacyCmdLauncherPath") || strings.Contains(lastJoined, "Start-Process -FilePath 'cmd.exe'") {
		t.Fatalf("expected Startup fallback start command, last call=%#v all calls=%#v", lastCall, runner.calls)
	}
	backend := teamsServiceWSLWindowsTaskBackend{}
	if installed, err := backend.StartupFallbackMarkerExists(); err != nil || !installed {
		t.Fatalf("Startup fallback marker installed=%v err=%v", installed, err)
	}
	if _, err := os.Stat(filepath.Join(tmp, "wsl-task", "codex-helper-teams-wsl-task-"+teamsServiceWSLTaskIdentity().Suffix+".txt")); !os.IsNotExist(err) {
		t.Fatalf("stale Scheduled Task config should be removed after fallback, err=%v", err)
	}
}

func TestUpgradeCmdWSLAccessDeniedRefreshUsesElevatedRetireBeforeFallback(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			nil,
			[]byte("NVIDIA\\jason\n"),
			nil,
			nil,
			nil,
		},
		errs: []error{
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	var out bytes.Buffer
	refresh, err := recoverWSLTeamsServiceRefreshAccessDenied(context.Background(), strings.NewReader("yes\n"), &out, errTeamsKeepaliveAccessDeniedForTest{})
	if err != nil {
		t.Fatalf("recover refresh access denied: %v\n%s", err, out.String())
	}
	if !refresh.StartupFallback {
		t.Fatalf("refresh should install Startup fallback when elevated repair is denied")
	}
	if len(runner.calls) != 5 {
		t.Fatalf("refresh calls=%#v, want normal retire, user query, elevated repair, elevated retire, fallback", runner.calls)
	}
	elevatedRetire := strings.Join(runner.calls[3].args, " ")
	for _, want := range []string{
		"Start-Process",
		"-Verb RunAs",
		"Disable-ScheduledTask",
		"Codex Helper Teams Bridge",
		"Codex Helper Teams Watchdog",
	} {
		if !strings.Contains(elevatedRetire, want) {
			t.Fatalf("elevated retire command missing %q:\n%s", want, elevatedRetire)
		}
	}
	if strings.Contains(elevatedRetire, "Register-ScheduledTask") {
		t.Fatalf("elevated retire must not try to create or repair tasks:\n%s", elevatedRetire)
	}
	if !strings.Contains(out.String(), "Old WSL Scheduled Tasks were disabled using Windows permission") {
		t.Fatalf("upgrade fallback output missing elevated cleanup explanation:\n%s", out.String())
	}
}

func TestUpgradeCmdWSLAccessDeniedRefreshFailsWhenCleanupNeedsUACAndUACDeclined(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			errTeamsKeepaliveAccessDeniedForTest{},
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	var out bytes.Buffer
	refresh, err := recoverWSLTeamsServiceRefreshAccessDenied(context.Background(), strings.NewReader("no\n"), &out, errTeamsKeepaliveAccessDeniedForTest{})
	if err == nil {
		t.Fatalf("recover refresh should fail when normal cleanup needs UAC and UAC is declined")
	}
	if refresh.StartupFallback {
		t.Fatalf("refresh must not install Startup fallback when old Scheduled Tasks remain active")
	}
	if !strings.Contains(err.Error(), "UAC was not confirmed") {
		t.Fatalf("error should explain that UAC was needed and declined, got %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("refresh calls=%#v, want only normal retire before declined UAC", runner.calls)
	}
	if !strings.Contains(out.String(), "NEXT STEP: TYPE yes TO CONTINUE") {
		t.Fatalf("cleanup failure should ask for UAC before failing:\n%s", out.String())
	}
}

func TestUpgradeCmdDoesNotUpdateWhenTeamsServiceStopFails(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &scriptedTeamsServiceRunner{errs: []error{nil, errors.New("stop failed")}}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		t.Fatal("performUpdate should not run if Teams service stop fails")
		return update.ApplyResult{}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs(upgradeArgsWithTeamsRestart("--version", "v1.2.3"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "stop failed") {
		t.Fatalf("execute upgrade error = %v, want stop failed\n%s", err, out.String())
	}
	if !teamsServiceCallSeen(runner.calls, "is-active") || !teamsServiceCallSeen(runner.calls, "stop") {
		t.Fatalf("expected is-active and stop calls, got %#v", runner.calls)
	}
	if teamsServiceCallSeen(runner.calls, "start") {
		t.Fatalf("service should not be restarted after stop failure, calls=%#v", runner.calls)
	}
}

func TestUpgradeCmdRestartsTeamsServiceAfterUpdateFailure(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !teamsServiceCallSeen(runner.calls, "stop") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{}, errors.New("download failed")
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs(upgradeArgsWithTeamsRestart("--version", "v1.2.3"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "download failed") {
		t.Fatalf("execute upgrade error = %v, want download failed\n%s", err, out.String())
	}
	if !teamsServiceCallSeen(runner.calls, "is-active") || !teamsServiceCallSeen(runner.calls, "stop") || !teamsServiceCallSeen(runner.calls, "start") {
		t.Fatalf("expected is-active, stop, and restart after failed update, got %#v", runner.calls)
	}
}

func TestUpgradeCmdReturnsDelayedTeamsRestartFailure(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		teamsServiceStartDetached = prevDetached
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !teamsServiceCallSeen(runner.calls, "stop") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3", RestartRequired: true}, nil
	}
	var detachedCalled bool
	teamsServiceStartDetached = func(context.Context, string, ...string) error {
		detachedCalled = true
		return errors.New("detached restart failed")
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs(upgradeArgsWithTeamsRestart("--version", "v1.2.3"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "detached restart failed") {
		t.Fatalf("execute upgrade error = %v, want detached restart failed\n%s", err, out.String())
	}
	if !detachedCalled {
		t.Fatal("delayed restart launcher was not called")
	}
	if teamsServiceCallSeen(runner.calls, "start") {
		t.Fatalf("restart-required upgrade must not immediately start service, calls=%#v", runner.calls)
	}
}

func TestScheduleDelayedTeamsServiceStartUsesWSLWindowsTask(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "work")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu-22.04",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	prevDetached := teamsServiceStartDetached
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string{}, args...)
		return nil
	}
	t.Cleanup(func() { teamsServiceStartDetached = prevDetached })

	if err := scheduleDelayedTeamsServiceStart(context.Background(), ""); err != nil {
		t.Fatalf("scheduleDelayedTeamsServiceStart error: %v", err)
	}
	joined := strings.Join(detachedArgs, " ")
	if detachedName != "powershell.exe" ||
		!strings.Contains(joined, "Enable-ScheduledTask") ||
		!strings.Contains(joined, "Start-CodexHelperScheduledTaskIfStopped $taskName") ||
		!strings.Contains(joined, "Test-CodexHelperScheduledTaskRunning") ||
		!strings.Contains(joined, "Codex Helper Teams Bridge (WSL Ubuntu-22.04 alice work ") ||
		!strings.Contains(joined, "Codex Helper Teams Watchdog (WSL Ubuntu-22.04 alice work ") ||
		!strings.Contains(joined, "$task.State -eq 'Disabled'") {
		t.Fatalf("unexpected WSL delayed restart: name=%q args=%#v", detachedName, detachedArgs)
	}
}

func TestDelayedTeamsServiceStartCommandUsesLocalSupervisor(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos: "linux",
		exe:  exe,
		cwd:  tmp,
	})
	t.Setenv(envTeamsLinuxServiceBackend, "local-supervisor")
	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("local supervisor config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec: teamsServiceSpec{
			Executable: exe,
			WorkingDir: tmp,
		},
	}); err != nil {
		t.Fatalf("write local supervisor config: %v", err)
	}

	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("backend error: %v", err)
	}
	name, args, err := delayedTeamsServiceStartCommand(backend, "")
	if err != nil {
		t.Fatalf("delayedTeamsServiceStartCommand error: %v", err)
	}
	joined := strings.Join(args, " ")
	if name != "sh" ||
		!strings.Contains(joined, envTeamsLinuxServiceBackend+"=local-supervisor") ||
		!strings.Contains(joined, envTeamsWSLServiceBackend+"=local-supervisor") ||
		!strings.Contains(joined, shellQuote(exe)+" teams service start") ||
		strings.Contains(joined, "systemctl") {
		t.Fatalf("delayed start command = %q %#v, want local-supervisor start", name, args)
	}

	name, args, err = delayedLocalSupervisorServiceCommand(backend, "restart")
	if err != nil {
		t.Fatalf("delayedLocalSupervisorServiceCommand restart error: %v", err)
	}
	joined = strings.Join(args, " ")
	if name != "sh" || !strings.Contains(joined, shellQuote(exe)+" teams service restart") || strings.Contains(joined, "systemctl") {
		t.Fatalf("delayed restart command = %q %#v, want local-supervisor restart", name, args)
	}
}

func TestScheduleDelayedTeamsServiceStartUsesConfiguredPowerShell(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	configuredPowerShell := "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "codex-proxy"),
		cwd:                  tmp,
		windowsTaskDir:       filepath.Join(tmp, "wsl-task"),
		isWSL:                true,
		wslDistro:            "Ubuntu-22.04",
		wslLinuxUser:         "alice",
		powerShellExecutable: configuredPowerShell,
		runner:               runner,
	})
	prevDetached := teamsServiceStartDetached
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string{}, args...)
		return nil
	}
	t.Cleanup(func() { teamsServiceStartDetached = prevDetached })

	if err := scheduleDelayedTeamsServiceStart(context.Background(), ""); err != nil {
		t.Fatalf("scheduleDelayedTeamsServiceStart error: %v", err)
	}
	if detachedName != configuredPowerShell || !strings.Contains(strings.Join(detachedArgs, " "), "Start-CodexHelperScheduledTaskIfStopped $taskName") {
		t.Fatalf("unexpected delayed restart command: name=%q args=%#v", detachedName, detachedArgs)
	}
}

func TestScheduleDelayedTeamsServiceStartUsesWindowsPendingActivation(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	pending := filepath.Join(tmp, ".codex-proxy_1.2.3_windows_amd64.exe.123")
	if err := os.WriteFile(pending, []byte("new"), 0o600); err != nil {
		t.Fatalf("write pending helper: %v", err)
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		windowsTaskDir: filepath.Join(tmp, "tasks"),
	})
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() { teamsServiceStartDetached = prevDetached })
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string(nil), args...)
		return nil
	}

	if err := scheduleDelayedTeamsServiceStartAfterUpgrade(context.Background(), teamsUpgradeServiceRefreshResult{}, pending, filepath.Join(tmp, "codex-proxy.exe")); err != nil {
		t.Fatalf("scheduleDelayedTeamsServiceStartAfterUpgrade error: %v", err)
	}
	if detachedName != "powershell.exe" {
		t.Fatalf("detached name = %q, want powershell.exe", detachedName)
	}
	joined := strings.Join(detachedArgs, " ")
	for _, want := range []string{
		pending,
		filepath.Join(tmp, "codex-proxy.exe"),
		teamsServiceWindowsWatchdogTaskName,
		teamsServiceWindowsTaskName,
		"$want='1.2.3'",
		".activation.json",
		"Move-Item -Force",
		"if (Test-DestVersion) { $ready=$true }",
		"Write-Status 'failed'",
		"Start-CodexHelperScheduledTaskIfStopped",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("windows activation command missing %q:\n%s", want, joined)
		}
	}
}

func TestUpgradeFinalizerRefreshesWindowsServiceBeforePendingActivation(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy.exe")
	pending := filepath.Join(tmp, ".codex-proxy_1.2.3_windows_amd64.exe.123")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            exe,
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "tasks"),
		runner:         runner,
	})
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() { teamsServiceStartDetached = prevDetached })
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, _ string, args ...string) error {
		detachedArgs = append([]string(nil), args...)
		return nil
	}

	finalize, err := stopTeamsServiceForHelperUpgrade(context.Background(), nil, io.Discard, nil)
	if err != nil {
		t.Fatalf("stopTeamsServiceForHelperUpgrade error: %v", err)
	}
	if err := finalize(context.Background(), teamsUpgradeFinishOptions{
		Success:            false,
		ServiceRestart:     teamsUpgradeRestartDelayed,
		InstallPath:        exe,
		PendingReplacePath: pending,
	}); err != nil {
		t.Fatalf("finalize pending Windows helper activation error: %v", err)
	}
	joinedCalls := make([]string, 0, len(runner.calls))
	for _, call := range runner.calls {
		joinedCalls = append(joinedCalls, strings.Join(call.args, " "))
	}
	joined := strings.Join(joinedCalls, "\n")
	for _, want := range []string{"Stop-ScheduledTask", "Register-ScheduledTask", "Enable-ScheduledTask"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("finalizer should refresh Windows service before activation, missing %q in calls=%#v", want, runner.calls)
		}
	}
	activation := strings.Join(detachedArgs, " ")
	for _, want := range []string{pending, exe, ".activation.json", "Move-Item -Force", "Write-Status 'failed'", "Enable-ScheduledTask", "Start-CodexHelperScheduledTaskIfStopped"} {
		if !strings.Contains(activation, want) {
			t.Fatalf("activation command missing %q:\n%s", want, activation)
		}
	}
}

func TestScheduleDelayedTeamsStartupFallbackStartUsesWSLStartupCommand(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
	})
	prevDetached := teamsServiceStartDetached
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string{}, args...)
		return nil
	}
	t.Cleanup(func() { teamsServiceStartDetached = prevDetached })

	if err := scheduleDelayedTeamsStartupFallbackStart(context.Background()); err != nil {
		t.Fatalf("scheduleDelayedTeamsStartupFallbackStart error: %v", err)
	}
	joined := strings.Join(detachedArgs, " ")
	if detachedName != "powershell.exe" ||
		!strings.Contains(joined, "Start-Sleep -Seconds 3") ||
		!strings.Contains(joined, "Start-Process -FilePath 'wscript.exe'") ||
		strings.Contains(joined, "Start-ScheduledTask") {
		t.Fatalf("unexpected delayed Startup fallback command: name=%q args=%#v", detachedName, detachedArgs)
	}
}

func TestUpgradeCmdStopsActiveTeamsServiceWithoutStateFile(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !teamsServiceCallSeen(runner.calls, "stop") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs(upgradeArgsWithTeamsRestart("--version", "v1.2.3"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if !teamsServiceCallSeen(runner.calls, "is-active") || !teamsServiceCallSeen(runner.calls, "stop") || !teamsServiceCallSeen(runner.calls, "start") {
		t.Fatalf("expected is-active, stop, and start calls, got %#v", runner.calls)
	}
}

// A running Codex request is never interrupted by a helper upgrade: the
// upgrade waits for it (up to the timeout) and does not update.
func TestUpgradeCmdTimesOutBeforeUpdatingWhenTeamsRequestStaysRunning(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})
	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevPoll := teamsUpgradePollInterval
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		teamsUpgradePollInterval = prevPoll
	})
	teamsUpgradePollInterval = time.Millisecond
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status { return update.Status{} }
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		t.Fatal("performUpdate should not run while a Teams request runs")
		return update.ApplyResult{}, nil
	}
	finish := seedRunningTeamsTurnForUpgradeTest(t)
	defer finish()

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs(upgradeArgsWithTeamsRestart("--version", "v1.2.3", "--teams-drain-timeout", "1ms"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "still running 1 request") {
		t.Fatalf("expected a running-request timeout, got %v\n%s", err, out.String())
	}
	if teamsServiceCallSeen(runner.calls, "stop") {
		t.Fatalf("service stopped although a request runs: %#v", runner.calls)
	}
}

// seedRunningTeamsTurnForUpgradeTest writes a store (CODEX_HELPER_TEAMS_CORE_STORE)
// with one running Codex request and releases it; finish completes it.
func seedRunningTeamsTurnForUpgradeTest(t *testing.T) (finish func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "core.sqlite")
	t.Setenv(envTeamsCoreStore, path)
	st, err := corestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Acquire(corestore.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	var turn corestore.Turn
	err = st.Update(func(tx *corestore.Tx) error {
		conv, err := tx.CreateConversation(corestore.NewConversation{Label: "s1", Kind: corestore.KindWork, Channel: "teams", ExtChatID: "chat-s1"})
		if err != nil {
			return err
		}
		if err := tx.Bind(corestore.Thread{ConvID: conv, ThreadID: "th-1", Cwd: "/w"}); err != nil {
			return err
		}
		if _, err := tx.AdmitWindow("k", conv, []corestore.InboundMsg{{ExtMsgID: "m1", ExtTime: 1, Sender: corestore.SenderOwner, Kind: corestore.InPrompt, Body: "hi"}}, "c1"); err != nil {
			return err
		}
		var ok bool
		if turn, ok, err = tx.NextRunnable(conv); err != nil || !ok {
			return fmt.Errorf("no runnable turn: %v", err)
		}
		if err := tx.MarkStarting(turn.ID, "th-1"); err != nil {
			return err
		}
		return tx.MarkRunning(turn.ID, "codex-1")
	})
	if err != nil {
		t.Fatal(err)
	}
	st.Release()
	st.Close()
	return func() {
		st, err := corestore.Open(path)
		if err != nil {
			t.Error(err)
			return
		}
		defer st.Close()
		if _, err := st.Acquire(corestore.Ident{Host: "h", PID: 2, BootID: "b"}); err != nil {
			t.Error(err)
			return
		}
		defer st.Release()
		if err := st.Update(func(tx *corestore.Tx) error { return tx.FinishTurn(turn.ID, corestore.TurnCompleted, "", nil) }); err != nil {
			t.Error(err)
		}
	}
}

func TestUpgradeCmdDelaysTeamsServiceRestartWhenUpdateNeedsProcessExit(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevPoll := teamsUpgradePollInterval
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		teamsUpgradePollInterval = prevPoll
		teamsServiceStartDetached = prevDetached
	})
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string{}, args...)
		return nil
	}
	teamsUpgradePollInterval = time.Millisecond
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !teamsServiceCallSeen(runner.calls, "stop") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3", RestartRequired: true}, nil
	}

	// A request still running when the upgrade starts: the service is
	// stopped only after it finished.
	finish := seedRunningTeamsTurnForUpgradeTest(t)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		time.Sleep(50 * time.Millisecond)
		finish()
	}()
	t.Cleanup(func() { <-finished })

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs(upgradeArgsWithTeamsRestart("--version", "v1.2.3", "--teams-drain-timeout", "1s"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if !teamsServiceCallSeen(runner.calls, "is-active") || !teamsServiceCallSeen(runner.calls, "stop") {
		t.Fatalf("expected is-active and stop calls, got %#v", runner.calls)
	}
	if teamsServiceCallSeen(runner.calls, "start") {
		t.Fatalf("restart-required upgrade must not immediately start service, calls=%#v", runner.calls)
	}
	if detachedName != "sh" || len(detachedArgs) != 2 || !strings.Contains(detachedArgs[1], "systemctl --user start '"+teamsServiceUnitName+"'") {
		t.Fatalf("unexpected detached restart: name=%q args=%#v", detachedName, detachedArgs)
	}
	if !strings.Contains(out.String(), "Scheduling Teams service restart after the pending helper replacement") ||
		!strings.Contains(out.String(), "Update replacement for v1.2.3 is pending.") ||
		!strings.Contains(out.String(), "verify `codex-proxy --version`") {
		t.Fatalf("upgrade output missing delayed restart/restart-required messages:\n%s", out.String())
	}
}
