package cli

import (
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func setEffectivePathsHooksForTest(t *testing.T) {
	t.Helper()

	prevGetenv := effectivePathsGetenv
	prevUserHome := effectivePathsUserHomeDir
	prevLookupByID := effectivePathsLookupUserByID
	prevLookupByName := effectivePathsLookupUserByName
	prevRunningAsRoot := effectivePathsRunningAsRoot
	prevStat := effectivePathsStat
	prevSameFile := effectivePathsSameFile
	prevEvalSymlinks := effectivePathsEvalSymlinks
	prevExecIdentityHome := effectivePathsExecIdentityHome
	prevExecIdentityUser := effectivePathsExecIdentityUser
	t.Cleanup(func() {
		effectivePathsGetenv = prevGetenv
		effectivePathsUserHomeDir = prevUserHome
		effectivePathsLookupUserByID = prevLookupByID
		effectivePathsLookupUserByName = prevLookupByName
		effectivePathsRunningAsRoot = prevRunningAsRoot
		effectivePathsStat = prevStat
		effectivePathsSameFile = prevSameFile
		effectivePathsEvalSymlinks = prevEvalSymlinks
		effectivePathsExecIdentityHome = prevExecIdentityHome
		effectivePathsExecIdentityUser = prevExecIdentityUser
	})
}

func TestResolveEffectivePaths_KeepsAliasRootHome(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)
	if runtime.GOOS == "windows" {
		t.Skip("symlink alias test is unix-focused")
	}

	realHome := t.TempDir()
	currentHome := filepath.Join(t.TempDir(), "root-home")
	candidateHome := filepath.Join(t.TempDir(), "user-home")
	if err := os.Symlink(realHome, currentHome); err != nil {
		t.Fatalf("symlink current home: %v", err)
	}
	if err := os.Symlink(realHome, candidateHome); err != nil {
		t.Fatalf("symlink candidate home: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(realHome, ".codex"), 0o755); err != nil {
		t.Fatalf("mkdir .codex: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(realHome, ".config", "codex-proxy"), 0o755); err != nil {
		t.Fatalf("mkdir .config: %v", err)
	}

	t.Setenv("HOME", currentHome)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv(envUserHomeHint, candidateHome)
	t.Setenv(codexhistory.EnvCodexDir, "")
	t.Setenv(envCodexHome, "")

	effectivePathsRunningAsRoot = func() bool { return true }
	effectivePathsUserHomeDir = func() (string, error) { return currentHome, nil }
	effectivePathsExecIdentityHome = func(string) (*execIdentity, error) { return nil, nil }

	got, err := resolveEffectivePaths("", "", "")
	if err != nil {
		t.Fatalf("resolveEffectivePaths: %v", err)
	}

	if got.Home != currentHome {
		t.Fatalf("Home = %q, want %q", got.Home, currentHome)
	}
	if got.CodexDir != filepath.Join(currentHome, ".codex") {
		t.Fatalf("CodexDir = %q", got.CodexDir)
	}
	wantConfig, err := config.DefaultPathForHome(currentHome)
	if err != nil {
		t.Fatalf("DefaultPathForHome: %v", err)
	}
	if got.ConfigPath != wantConfig {
		t.Fatalf("ConfigPath = %q, want %q", got.ConfigPath, wantConfig)
	}
	if !got.AliasRootHome {
		t.Fatal("expected AliasRootHome=true")
	}
	if got.AliasProof == "" {
		t.Fatal("expected AliasProof to be set")
	}
	if got.ExecIdentity != nil {
		t.Fatalf("ExecIdentity = %+v, want nil", got.ExecIdentity)
	}
}

func TestResolveEffectivePaths_SwitchesToTrustedUserHomeHint(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)

	currentHome := t.TempDir()
	candidateHome := t.TempDir()
	t.Setenv("HOME", currentHome)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv(envUserHomeHint, candidateHome)
	t.Setenv(codexhistory.EnvCodexDir, "")
	t.Setenv(envCodexHome, "")

	effectivePathsRunningAsRoot = func() bool { return true }
	effectivePathsUserHomeDir = func() (string, error) { return currentHome, nil }
	effectivePathsExecIdentityHome = func(home string) (*execIdentity, error) {
		if home != candidateHome {
			t.Fatalf("unexpected identity lookup home: %q", home)
		}
		return &execIdentity{
			UID:         1000,
			GID:         1000,
			GroupsKnown: true,
			Username:    "alice",
			Home:        candidateHome,
		}, nil
	}

	got, err := resolveEffectivePaths("", "", "")
	if err != nil {
		t.Fatalf("resolveEffectivePaths: %v", err)
	}

	if got.Home != candidateHome {
		t.Fatalf("Home = %q, want %q", got.Home, candidateHome)
	}
	if got.CodexDir != filepath.Join(candidateHome, ".codex") {
		t.Fatalf("CodexDir = %q", got.CodexDir)
	}
	wantConfig, err := config.DefaultPath()
	if err != nil {
		t.Fatalf("DefaultPath: %v", err)
	}
	if got.ConfigPath != wantConfig {
		t.Fatalf("ConfigPath = %q, want %q", got.ConfigPath, wantConfig)
	}
	if got.AliasRootHome {
		t.Fatal("expected AliasRootHome=false")
	}
	if got.ExecIdentity == nil {
		t.Fatal("expected ExecIdentity to be set")
	}
	if got.ExecIdentity.UID != 1000 || got.ExecIdentity.GID != 1000 {
		t.Fatalf("ExecIdentity = %+v", got.ExecIdentity)
	}
}

func TestResolveEffectivePaths_KeepsExecIdentityForAliasedTrustedHome(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)
	if runtime.GOOS == "windows" {
		t.Skip("symlink alias test is unix-focused")
	}

	realHome := t.TempDir()
	currentHome := filepath.Join(t.TempDir(), "root-home")
	candidateHome := filepath.Join(t.TempDir(), "user-home")
	if err := os.Symlink(realHome, currentHome); err != nil {
		t.Fatalf("symlink current home: %v", err)
	}
	if err := os.Symlink(realHome, candidateHome); err != nil {
		t.Fatalf("symlink candidate home: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(realHome, ".codex"), 0o755); err != nil {
		t.Fatalf("mkdir .codex: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(realHome, ".config", "codex-proxy"), 0o755); err != nil {
		t.Fatalf("mkdir .config: %v", err)
	}

	t.Setenv("HOME", currentHome)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv(envUserHomeHint, candidateHome)
	t.Setenv(codexhistory.EnvCodexDir, "")
	t.Setenv(envCodexHome, "")

	effectivePathsRunningAsRoot = func() bool { return true }
	effectivePathsUserHomeDir = func() (string, error) { return currentHome, nil }
	effectivePathsExecIdentityHome = func(home string) (*execIdentity, error) {
		if home != candidateHome {
			t.Fatalf("unexpected identity lookup home: %q", home)
		}
		return &execIdentity{
			UID:         1000,
			GID:         1000,
			GroupsKnown: true,
			Username:    "alice",
			Home:        candidateHome,
		}, nil
	}

	got, err := resolveEffectivePaths("", "", "")
	if err != nil {
		t.Fatalf("resolveEffectivePaths: %v", err)
	}

	if !got.AliasRootHome {
		t.Fatal("expected AliasRootHome=true")
	}
	if got.ExecIdentity == nil {
		t.Fatal("expected ExecIdentity to be preserved")
	}
	if got.ExecIdentity.UID != 1000 || !got.ExecIdentity.GroupsKnown {
		t.Fatalf("ExecIdentity = %+v", got.ExecIdentity)
	}
}

func TestResolveEffectivePaths_DoesNotGuessWithoutTrustedHint(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)

	currentHome := t.TempDir()
	t.Setenv("HOME", currentHome)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv(envUserHomeHint, "")
	t.Setenv(codexhistory.EnvCodexDir, "")
	t.Setenv(envCodexHome, "")

	effectivePathsRunningAsRoot = func() bool { return true }
	effectivePathsUserHomeDir = func() (string, error) { return currentHome, nil }
	effectivePathsLookupUserByID = func(string) (*user.User, error) { return nil, os.ErrNotExist }
	effectivePathsLookupUserByName = func(string) (*user.User, error) { return nil, os.ErrNotExist }

	got, err := resolveEffectivePaths("", "", "")
	if err != nil {
		t.Fatalf("resolveEffectivePaths: %v", err)
	}

	if got.Home != currentHome {
		t.Fatalf("Home = %q, want %q", got.Home, currentHome)
	}
	if got.CodexDir != filepath.Join(currentHome, ".codex") {
		t.Fatalf("CodexDir = %q", got.CodexDir)
	}
	wantConfig, err := config.DefaultPath()
	if err != nil {
		t.Fatalf("DefaultPath: %v", err)
	}
	if got.ConfigPath != wantConfig {
		t.Fatalf("ConfigPath = %q, want %q", got.ConfigPath, wantConfig)
	}
	if got.AliasRootHome {
		t.Fatal("expected AliasRootHome=false")
	}
	if got.ExecIdentity != nil {
		t.Fatalf("ExecIdentity = %+v, want nil", got.ExecIdentity)
	}
}

func TestResolveEffectivePaths_PathOnlyAllowsForeignHomeWithoutRunnableIdentity(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)

	currentHome := t.TempDir()
	candidateHome := t.TempDir()
	t.Setenv("HOME", currentHome)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv(envUserHomeHint, candidateHome)
	t.Setenv(codexhistory.EnvCodexDir, "")
	t.Setenv(envCodexHome, "")

	effectivePathsRunningAsRoot = func() bool { return true }
	effectivePathsUserHomeDir = func() (string, error) { return currentHome, nil }
	effectivePathsExecIdentityHome = func(string) (*execIdentity, error) { return nil, nil }

	got, err := resolveEffectivePaths("", "", "")
	if err != nil {
		t.Fatalf("resolveEffectivePaths: %v", err)
	}
	if got.Home != candidateHome {
		t.Fatalf("Home = %q, want %q", got.Home, candidateHome)
	}
	if got.CodexDir != filepath.Join(candidateHome, ".codex") {
		t.Fatalf("CodexDir = %q", got.CodexDir)
	}
	wantConfig, err := config.DefaultPathForHome(candidateHome)
	if err != nil {
		t.Fatalf("DefaultPathForHome: %v", err)
	}
	if got.ConfigPath != wantConfig {
		t.Fatalf("ConfigPath = %q, want %q", got.ConfigPath, wantConfig)
	}
	if got.ExecIdentity != nil {
		t.Fatalf("ExecIdentity = %+v, want nil", got.ExecIdentity)
	}
}

func TestNewRootStore_PathOnlyAllowsForeignHomeWithoutRunnableIdentity(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)

	currentHome := t.TempDir()
	candidateHome := t.TempDir()
	t.Setenv("HOME", currentHome)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv(envUserHomeHint, candidateHome)
	t.Setenv(codexhistory.EnvCodexDir, "")
	t.Setenv(envCodexHome, "")

	effectivePathsRunningAsRoot = func() bool { return true }
	effectivePathsUserHomeDir = func() (string, error) { return currentHome, nil }
	effectivePathsExecIdentityHome = func(string) (*execIdentity, error) { return nil, nil }

	store, paths, err := newRootStore(&rootOptions{}, "")
	if err != nil {
		t.Fatalf("newRootStore: %v", err)
	}

	wantConfig, err := config.DefaultPathForHome(candidateHome)
	if err != nil {
		t.Fatalf("DefaultPathForHome: %v", err)
	}
	if store.Path() != wantConfig {
		t.Fatalf("store.Path = %q, want %q", store.Path(), wantConfig)
	}
	if paths.Home != candidateHome {
		t.Fatalf("Home = %q, want %q", paths.Home, candidateHome)
	}
	if paths.CodexDir != filepath.Join(candidateHome, ".codex") {
		t.Fatalf("CodexDir = %q", paths.CodexDir)
	}
	if paths.ConfigPath != wantConfig {
		t.Fatalf("ConfigPath = %q, want %q", paths.ConfigPath, wantConfig)
	}
	if paths.ExecIdentity != nil {
		t.Fatalf("ExecIdentity = %+v, want nil", paths.ExecIdentity)
	}
}

func TestResolveEffectiveLaunchPaths_ForeignHomeRequiresResolvableIdentity(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)
	if runtime.GOOS == "windows" {
		t.Skip("windows does not enforce exec identity requirements")
	}

	currentHome := t.TempDir()
	candidateHome := t.TempDir()
	t.Setenv("HOME", currentHome)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv(envUserHomeHint, candidateHome)
	t.Setenv(codexhistory.EnvCodexDir, "")
	t.Setenv(envCodexHome, "")

	effectivePathsRunningAsRoot = func() bool { return true }
	effectivePathsUserHomeDir = func() (string, error) { return currentHome, nil }
	effectivePathsExecIdentityHome = func(string) (*execIdentity, error) { return nil, nil }

	_, err := resolveEffectiveLaunchPaths("", "", "")
	if err == nil {
		t.Fatal("expected error when foreign home has no resolvable identity")
	}
	var targetErr *execIdentityRequired
	if !errors.As(err, &targetErr) {
		t.Fatalf("expected execIdentityRequired, got %T %v", err, err)
	}
}
