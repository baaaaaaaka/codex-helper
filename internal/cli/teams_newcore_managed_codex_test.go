package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/userpath"
)

// managedCodexLaunchFixture is a HOME with a CXP-managed Codex and a
// different `codex` first on the login-shell PATH (SPEC R-24, SC-12a).
type managedCodexLaunchFixture struct {
	store   *config.Store
	managed string
	global  string
}

func newManagedCodexLaunchFixture(t *testing.T, withManaged bool) managedCodexLaunchFixture {
	t.Helper()
	if os.PathSeparator != '/' {
		t.Skip("POSIX fixture")
	}
	lockCLITestHooks(t)
	rootDir := t.TempDir()
	home := filepath.Join(rootDir, "home")
	t.Setenv("HOME", home)
	t.Setenv("CODEX_NPM_PREFIX", "")
	setTestCodexHomeEnv(t, filepath.Join(rootDir, "codex-home"))
	script := "#!/bin/sh\necho 'codex-cli 0.58.0'\n"
	f := managedCodexLaunchFixture{
		managed: filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex"),
		global:  filepath.Join(rootDir, "globalbin", "codex"),
	}
	for _, p := range []string{f.managed, f.global} {
		if p == f.managed && !withManaged {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	previousResolver, previousNative := teamsUserPathResolver, teamsFindNativeCodex
	previousEnsure, previousUpgrade := ensureCodexInstalledForTargetRun, upgradeCodexInstalledForTargetRun
	t.Cleanup(func() {
		teamsUserPathResolver, teamsFindNativeCodex = previousResolver, previousNative
		ensureCodexInstalledForTargetRun, upgradeCodexInstalledForTargetRun = previousEnsure, previousUpgrade
	})
	teamsUserPathResolver = cliTestUserPathResolverFunc(func(context.Context, userpath.Request) (userpath.Result, error) {
		return userpath.Result{Path: filepath.Dir(f.global) + ":/usr/bin:/bin", Mode: userpath.ModeAccountDefault, Source: "test-account"}, nil
	})
	teamsFindNativeCodex = func(string) (string, string, error) { return "", "", errors.New("no native binary") }
	ensureCodexInstalledForTargetRun = func(context.Context, string, io.Writer, codexInstallOptions) (string, error) {
		t.Fatal("unexpected Codex install")
		return "", nil
	}
	upgradeCodexInstalledForTargetRun = func(context.Context, io.Writer, codexInstallOptions) (string, error) {
		t.Fatal("unexpected Codex upgrade")
		return "", nil
	}
	t.Setenv("PATH", filepath.Dir(f.global)+":/usr/bin:/bin")
	store, err := config.NewStore(filepath.Join(rootDir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, RuntimeGeneration: currentRuntimeGeneration}); err != nil {
		t.Fatal(err)
	}
	f.store = store
	return f
}

// Without --codex-path the app-servers run the CXP-managed Codex, not the
// `codex` first on PATH, and it is the binary the Teams upgrade targets.
func TestTeamsNewCoreLaunchPrefersManagedCodexOverPATH(t *testing.T) {
	f := newManagedCodexLaunchFixture(t, true)
	root := &rootOptions{configPath: f.store.Path()}
	launch, err := newTeamsNewCoreCodexLaunch(context.Background(), root, "", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer launch.Close()
	if launch.Bin != f.managed {
		t.Fatalf("app-server binary = %q, want the managed %q (not %q on PATH)", launch.Bin, f.managed, f.global)
	}
	store, paths, err := newRootStore(root, "")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	target, err := resolveTeamsCodexUpgradeTarget(context.Background(), cfg, paths)
	if err != nil {
		t.Fatal(err)
	}
	if target.path != launch.Bin {
		t.Fatalf("upgrade target = %q, app-servers run %q", target.path, launch.Bin)
	}
}

// An explicit --codex-path still wins over the managed install.
func TestTeamsNewCoreLaunchExplicitCodexPathWins(t *testing.T) {
	f := newManagedCodexLaunchFixture(t, true)
	launch, err := newTeamsNewCoreCodexLaunch(context.Background(), &rootOptions{configPath: f.store.Path()}, f.global, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer launch.Close()
	if launch.Bin != f.global {
		t.Fatalf("app-server binary = %q, want the explicit %q", launch.Bin, f.global)
	}
}

// Without a managed install the launch installs one (the old core's
// behaviour) instead of falling back to the `codex` on PATH.
func TestTeamsNewCoreLaunchInstallsManagedCodexWhenMissing(t *testing.T) {
	f := newManagedCodexLaunchFixture(t, false)
	var got codexInstallOptions
	ensureCodexInstalledForTargetRun = func(_ context.Context, codexPath string, _ io.Writer, opts codexInstallOptions) (string, error) {
		if codexPath != "" {
			t.Fatalf("install codexPath = %q", codexPath)
		}
		got = opts
		return f.managed, nil
	}
	launch, err := newTeamsNewCoreCodexLaunch(context.Background(), &rootOptions{configPath: f.store.Path()}, "", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer launch.Close()
	if !got.requireManaged || got.upgradeCodex || got.upgradeCodexPath != "" {
		t.Fatalf("install options = %+v, want a managed install", got)
	}
	if launch.Bin != f.managed {
		t.Fatalf("app-server binary = %q, want the installed %q", launch.Bin, f.managed)
	}
}

// An upgrade that leaves the Codex version unchanged fails, so the engine
// reports it and does not restart into the same CLI.
func TestTeamsNewCoreCodexUpgraderUnchangedVersionFails(t *testing.T) {
	lockCLITestHooks(t)
	previousUpgrade, previousVersion := upgradeTeamsCodexForNewCore, teamsCodexVersionForUpgrade
	t.Cleanup(func() { upgradeTeamsCodexForNewCore, teamsCodexVersionForUpgrade = previousUpgrade, previousVersion })
	versions := map[string]string{"/managed/codex": "codex-cli 0.58.0"}
	teamsCodexVersionForUpgrade = func(_ context.Context, path string, _ *execIdentity) string { return versions[path] }
	upgradeTeamsCodexForNewCore = func(context.Context, *rootOptions, io.Writer) (string, error) {
		return "/managed/codex", nil
	}
	up := teamsNewCoreCodexUpgrader{bin: "/managed/codex", log: io.Discard}
	err := up.Upgrade(context.Background())
	if err == nil || !strings.Contains(err.Error(), "already codex-cli 0.58.0") {
		t.Fatalf("unchanged upgrade error = %v", err)
	}
	upgradeTeamsCodexForNewCore = func(context.Context, *rootOptions, io.Writer) (string, error) {
		versions["/managed/codex"] = "codex-cli 0.59.0"
		return "/managed/codex", nil
	}
	if err := up.Upgrade(context.Background()); err != nil {
		t.Fatalf("upgrade to a new version: %v", err)
	}
	upgradeTeamsCodexForNewCore = func(context.Context, *rootOptions, io.Writer) (string, error) {
		return "", errors.New("npm failed")
	}
	if err := up.Upgrade(context.Background()); err == nil || err.Error() != "npm failed" {
		t.Fatalf("failed upgrade error = %v", err)
	}
}
