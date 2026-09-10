package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestRootProfileArg(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		want    string
		wantErr string
	}{
		{
			name: "no args",
			args: nil,
			want: "",
		},
		{
			name: "one profile arg",
			args: []string{"profile-a"},
			want: "profile-a",
		},
		{
			name:    "too many args before dash",
			args:    []string{"profile-a", "profile-b"},
			wantErr: "unexpected args before --",
		},
		{
			name:    "args after dash",
			args:    []string{"profile-a", "--", "echo"},
			wantErr: "unexpected args after --",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			if err := cmd.Flags().Parse(tc.args); err != nil {
				t.Fatalf("parse args: %v", err)
			}
			got, err := rootProfileArg(cmd)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %q", tc.wantErr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected profile %q, got %q", tc.want, got)
			}
		})
	}
}

func TestUpgradeUsesProxy(t *testing.T) {
	enabled := true
	disabled := false

	cases := []struct {
		name string
		cfg  config.Config
		want bool
	}{
		{
			name: "unset with no profiles",
			cfg:  config.Config{},
			want: false,
		},
		{
			name: "unset with profiles",
			cfg: config.Config{
				Profiles: []config.Profile{{ID: "p1", Name: "p1"}},
			},
			want: true,
		},
		{
			name: "explicit false with profiles",
			cfg: config.Config{
				ProxyEnabled: &disabled,
				Profiles:     []config.Profile{{ID: "p1", Name: "p1"}},
			},
			want: false,
		},
		{
			name: "explicit true with no profiles is incomplete",
			cfg: config.Config{
				ProxyEnabled: &enabled,
			},
			want: false,
		},
		{
			name: "explicit true with profiles",
			cfg: config.Config{
				ProxyEnabled: &enabled,
				Profiles:     []config.Profile{{ID: "p1", Name: "p1"}},
			},
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := upgradeUsesProxy(tc.cfg)
			if got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestRootUpgradeCodexPrefersManagedInstallOverPATHCodex(t *testing.T) {
	lockCLITestHooks(t)
	home := t.TempDir()
	managedPrefix := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global")
	managedBin := filepath.Join(managedPrefix, "bin")
	if err := os.MkdirAll(managedBin, 0o755); err != nil {
		t.Fatal(err)
	}
	managedCodex := writeProbeableCodex(t, managedBin, true)
	pathBin := t.TempDir()
	pathCodex := writeProbeableCodex(t, pathBin, true)

	t.Setenv("HOME", home)
	t.Setenv("CODEX_NPM_PREFIX", managedPrefix)
	t.Setenv("PATH", pathBin)
	t.Setenv(envUserHomeHint, "")
	t.Setenv("SUDO_UID", "")
	t.Setenv("SUDO_USER", "")

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false)}); err != nil {
		t.Fatal(err)
	}

	previousUpgrade := upgradeCodexInstalledForTargetRun
	previousEnsure := ensureCodexInstalledForTargetRun
	t.Cleanup(func() {
		upgradeCodexInstalledForTargetRun = previousUpgrade
		ensureCodexInstalledForTargetRun = previousEnsure
	})
	ensureCodexInstalledForTargetRun = func(context.Context, string, io.Writer, codexInstallOptions) (string, error) {
		t.Fatal("existing managed Codex should be upgraded, not installed")
		return "", nil
	}
	var got codexInstallOptions
	upgradeCodexInstalledForTargetRun = func(_ context.Context, _ io.Writer, opts codexInstallOptions) (string, error) {
		got = opts
		return opts.upgradeCodexPath, nil
	}

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", cfgPath, "--upgrade-codex"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if got.upgradeCodexPath != managedCodex {
		t.Fatalf("upgrade target = %q, want managed %q instead of PATH Codex %q", got.upgradeCodexPath, managedCodex, pathCodex)
	}
	if !got.requireManaged {
		t.Fatal("root managed upgrade must require the managed installation")
	}
}

func TestRootUpgradeCodexInstallsManagedWhenOnlyPATHCodexExists(t *testing.T) {
	lockCLITestHooks(t)
	home := t.TempDir()
	managedPrefix := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global")
	pathBin := t.TempDir()
	writeProbeableCodex(t, pathBin, true)

	t.Setenv("HOME", home)
	t.Setenv("CODEX_NPM_PREFIX", managedPrefix)
	t.Setenv("PATH", pathBin)
	t.Setenv(envUserHomeHint, "")
	t.Setenv("SUDO_UID", "")
	t.Setenv("SUDO_USER", "")

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false)}); err != nil {
		t.Fatal(err)
	}

	previousUpgrade := upgradeCodexInstalledForTargetRun
	previousEnsure := ensureCodexInstalledForTargetRun
	t.Cleanup(func() {
		upgradeCodexInstalledForTargetRun = previousUpgrade
		ensureCodexInstalledForTargetRun = previousEnsure
	})
	upgradeCodexInstalledForTargetRun = func(context.Context, io.Writer, codexInstallOptions) (string, error) {
		t.Fatal("PATH Codex must not be upgraded when managed Codex is missing")
		return "", nil
	}
	var got codexInstallOptions
	ensureCodexInstalledForTargetRun = func(_ context.Context, path string, _ io.Writer, opts codexInstallOptions) (string, error) {
		if path != "" {
			t.Fatalf("explicit install path = %q", path)
		}
		got = opts
		return filepath.Join(managedPrefix, "bin", "codex"), nil
	}

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", cfgPath, "--upgrade-codex"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if got.upgradeCodex || got.upgradeCodexPath != "" || !got.requireManaged {
		t.Fatalf("managed install options = %#v", got)
	}
	if prefix := envValue(got.installerEnv, "CODEX_NPM_PREFIX"); prefix != managedPrefix {
		t.Fatalf("managed prefix = %q, want %q", prefix, managedPrefix)
	}
}

func TestRootUpgradeCodexExplicitPathPreservesExternalUpgrade(t *testing.T) {
	lockCLITestHooks(t)
	home := t.TempDir()
	external := writeProbeableCodex(t, t.TempDir(), true)
	t.Setenv("HOME", home)
	t.Setenv("PATH", t.TempDir())
	t.Setenv(envUserHomeHint, "")
	t.Setenv("SUDO_UID", "")
	t.Setenv("SUDO_USER", "")

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false)}); err != nil {
		t.Fatal(err)
	}

	previousUpgrade := upgradeCodexInstalledForTargetRun
	t.Cleanup(func() { upgradeCodexInstalledForTargetRun = previousUpgrade })
	var got codexInstallOptions
	upgradeCodexInstalledForTargetRun = func(_ context.Context, _ io.Writer, opts codexInstallOptions) (string, error) {
		got = opts
		return opts.upgradeCodexPath, nil
	}

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", cfgPath, "--upgrade-codex", "--upgrade-codex-path", external})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if got.upgradeCodexPath != external || got.requireManaged {
		t.Fatalf("external upgrade options = %#v", got)
	}
}

func TestRootUpgradeCodexExplicitPathCanRepairNonfunctionalExecutable(t *testing.T) {
	lockCLITestHooks(t)
	home := t.TempDir()
	external := writeProbeableCodex(t, t.TempDir(), false)
	t.Setenv("HOME", home)
	t.Setenv("PATH", t.TempDir())
	t.Setenv(envUserHomeHint, "")
	t.Setenv("SUDO_UID", "")
	t.Setenv("SUDO_USER", "")

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false)}); err != nil {
		t.Fatal(err)
	}

	previousUpgrade := upgradeCodexInstalledForTargetRun
	t.Cleanup(func() { upgradeCodexInstalledForTargetRun = previousUpgrade })
	var got codexInstallOptions
	upgradeCodexInstalledForTargetRun = func(_ context.Context, _ io.Writer, opts codexInstallOptions) (string, error) {
		got = opts
		return opts.upgradeCodexPath, nil
	}

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", cfgPath, "--upgrade-codex", "--upgrade-codex-path", external})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if got.upgradeCodexPath != external || got.requireManaged {
		t.Fatalf("repair upgrade options = %#v", got)
	}
}

func TestRootUpgradeCodexPathValidation(t *testing.T) {
	t.Run("requires upgrade flag", func(t *testing.T) {
		cmd := newRootCmd()
		cmd.SetArgs([]string{"--upgrade-codex-path", filepath.Join(t.TempDir(), "codex")})
		err := cmd.Execute()
		if err == nil || !strings.Contains(err.Error(), "requires --upgrade-codex") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("requires absolute path", func(t *testing.T) {
		cmd := newRootCmd()
		cmd.SetArgs([]string{"--upgrade-codex", "--upgrade-codex-path", "codex"})
		err := cmd.Execute()
		if err == nil || !strings.Contains(err.Error(), "absolute executable path") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestProbeManagedCodexUpgradeCandidateRetriesTransientFailure(t *testing.T) {
	previousDelay := managedCodexProbeRetryDelay
	managedCodexProbeRetryDelay = time.Millisecond
	t.Cleanup(func() { managedCodexProbeRetryDelay = previousDelay })

	path, counter := writeTransientProbeCodex(t)
	if err := probeManagedCodexUpgradeCandidate(context.Background(), path, nil, nil); err != nil {
		t.Fatalf("transient probe should recover: %v", err)
	}
	if _, err := os.Stat(counter); err != nil {
		t.Fatal(err)
	}
}

func TestProbeManagedCodexUpgradeCandidatePreservesPermanentFailure(t *testing.T) {
	previousDelay := managedCodexProbeRetryDelay
	managedCodexProbeRetryDelay = time.Millisecond
	t.Cleanup(func() { managedCodexProbeRetryDelay = previousDelay })

	path, counter := writePermanentProbeCodex(t)
	err := probeManagedCodexUpgradeCandidate(context.Background(), path, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "not functional") {
		t.Fatalf("permanent probe error = %v", err)
	}
	attempts, readErr := os.ReadFile(counter)
	if readErr != nil {
		t.Fatal(readErr)
	}
	// The Windows .cmd fixture uses `echo`, which writes CRLF. Count the
	// marker rather than bytes so the assertion describes invocations on both
	// platforms instead of treating each Windows line ending as two attempts.
	if got := strings.Count(string(attempts), "x"); got != managedCodexProbeAttempts {
		t.Fatalf("probe attempts = %d, want %d", got, managedCodexProbeAttempts)
	}
}

func writeTransientProbeCodex(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	marker := filepath.Join(dir, "probe-attempt")
	if runtime.GOOS == "windows" {
		path := writeProbeScript(t, dir, "codex.cmd", "@echo off\r\nset \"marker=%~dp0probe-attempt\"\r\nif exist \"%marker%\" goto success\r\ntype nul > \"%marker%\"\r\necho codex-cli transient failure\r\nexit /b 1\r\n:success\r\necho codex-cli 0.153.4\r\nexit /b 0\r\n")
		return path, marker
	}
	path := writeProbeScript(t, dir, "codex", fmt.Sprintf("#!/bin/sh\nif [ ! -f %q ]; then\n  printf x > %q\n  echo 'codex-cli transient failure'\n  exit 1\nfi\necho 'codex-cli 0.153.4'\nexit 0\n", marker, marker))
	return path, marker
}

func writePermanentProbeCodex(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	counter := filepath.Join(dir, "probe-attempts")
	if runtime.GOOS == "windows" {
		path := writeProbeScript(t, dir, "codex.cmd", "@echo off\r\necho x>>\"%~dp0probe-attempts\"\r\necho codex-cli permanently broken\r\nexit /b 1\r\n")
		return path, counter
	}
	path := writeProbeScript(t, dir, "codex", fmt.Sprintf("#!/bin/sh\nprintf x >> %q\necho 'codex-cli permanently broken'\nexit 1\n", counter))
	return path, counter
}

func TestCodexUpgradeTargetInstallOptionsUsesTargetIdentity(t *testing.T) {
	targetHome := t.TempDir()
	identity := &execIdentity{
		UID:         1000,
		GID:         100,
		Groups:      []uint32{20, 30},
		GroupsKnown: true,
		Username:    "alice",
		Home:        targetHome,
	}
	target := managedCodexUpgradeTarget{
		path:        filepath.Join(targetHome, "codex"),
		environment: []string{"HOME=/root", "PATH=/usr/bin", "CODEX_NPM_PREFIX=/root/managed"},
		identity:    identity,
	}
	opts := managedCodexUpgradeInstallOptions(target)
	if opts.configureInstallerCommand == nil {
		t.Fatal("target identity must configure installer commands")
	}
	cmd := exec.Command("ignored")
	cmd.Env = append([]string(nil), opts.installerEnv...)
	if err := opts.configureInstallerCommand(cmd); err != nil {
		t.Fatal(err)
	}
	if home := envValue(cmd.Env, "HOME"); home != targetHome {
		t.Fatalf("HOME = %q, want %q", home, targetHome)
	}
	if user := envValue(cmd.Env, "USER"); user != "alice" {
		t.Fatalf("USER = %q, want alice", user)
	}
	wantPrefix := filepath.Join(targetHome, ".local", "share", "codex-proxy", "npm-global")
	if runtime.GOOS == "windows" {
		wantPrefix = filepath.Join(targetHome, "AppData", "Local", "codex-proxy", "npm-global")
	}
	if prefix := envValue(cmd.Env, "CODEX_NPM_PREFIX"); prefix != wantPrefix {
		t.Fatalf("CODEX_NPM_PREFIX = %q, want %q", prefix, wantPrefix)
	}
}

func TestEnsureManagedCodexHonorsTargetIdentityProbeDuringInstallRecheck(t *testing.T) {
	home := t.TempDir()
	prefix := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global")
	binDir := filepath.Join(prefix, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeProbeableCodex(t, binDir, true)
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("LOCALAPPDATA", t.TempDir())

	wantErr := errors.New("target user cannot execute managed Codex")
	probeCalls := 0
	installerCalls := 0
	opts := codexInstallOptions{
		installerEnv:   []string{"HOME=" + home, "CODEX_NPM_PREFIX=" + prefix, "PATH=/usr/bin"},
		requireManaged: true,
		probeManagedCodex: func(context.Context, string, []string) error {
			probeCalls++
			return wantErr
		},
		withInstallerEnv: func(context.Context, func([]string) error) error {
			installerCalls++
			return wantErr
		},
	}
	_, err := ensureCodexInstalledWithOptions(context.Background(), "", io.Discard, opts)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want target identity probe failure", err)
	}
	if probeCalls < 2 {
		t.Fatalf("target identity probe calls = %d, want initial and locked recheck", probeCalls)
	}
	if installerCalls != 1 {
		t.Fatalf("installer calls = %d, want 1", installerCalls)
	}
}
