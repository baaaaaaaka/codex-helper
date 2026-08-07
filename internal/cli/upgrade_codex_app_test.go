package cli

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestRootUpgradeCodexAppDispatchesManagedUpdate(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevGOARCH := codexAppGOARCH
	prevWSL := codexAppIsWSL
	prevRoot := codexAppWindowsManagedRootFn
	prevUpgrade := codexAppUpgradeManagedInstallFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppGOARCH = prevGOARCH
		codexAppIsWSL = prevWSL
		codexAppWindowsManagedRootFn = prevRoot
		codexAppUpgradeManagedInstallFn = prevUpgrade
	})

	codexAppGOOS = func() string { return "windows" }
	codexAppGOARCH = func() string { return "amd64" }
	codexAppIsWSL = func() bool { return false }
	managedRoot := filepath.Join(t.TempDir(), "chatgpt")
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return managedRoot, nil }
	var gotRoot string
	codexAppUpgradeManagedInstallFn = func(_ context.Context, root string, opts codexDesktopAppOptions) (codexWindowsManagedInstallState, bool, error) {
		gotRoot = root
		if opts.ProxyURL != "" {
			t.Fatalf("unexpected proxy URL for empty config: %q", opts.ProxyURL)
		}
		return codexWindowsManagedInstallState{PackageVersion: "26.803.5235.0"}, true, nil
	}

	var out bytes.Buffer
	cmd := newRootCmd()
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", filepath.Join(t.TempDir(), "config.json"), "--upgrade-codex-app"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("upgrade app command: %v", err)
	}
	if gotRoot != managedRoot {
		t.Fatalf("managed update root = %q, want %q", gotRoot, managedRoot)
	}
	text := out.String()
	if !strings.Contains(text, "Codex desktop app update target") || !strings.Contains(text, "Codex desktop app upgraded: 26.803.5235.0") {
		t.Fatalf("unexpected upgrade output: %q", text)
	}
}

func TestRootUpgradeCodexAppAcceptsProfileArgument(t *testing.T) {
	lockCLITestHooks(t)
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("new config store: %v", err)
	}
	profile := config.Profile{ID: "profile-a", Name: "CI recording proxy", Host: "127.0.0.1", Port: 1}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Profiles: []config.Profile{profile}}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	prevGOOS := codexAppGOOS
	prevGOARCH := codexAppGOARCH
	prevWSL := codexAppIsWSL
	prevRoot := codexAppWindowsManagedRootFn
	prevEnsureProxyURL := codexAppEnsureProxyURLFn
	prevUpgrade := codexAppUpgradeManagedInstallFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppGOARCH = prevGOARCH
		codexAppIsWSL = prevWSL
		codexAppWindowsManagedRootFn = prevRoot
		codexAppEnsureProxyURLFn = prevEnsureProxyURL
		codexAppUpgradeManagedInstallFn = prevUpgrade
	})

	codexAppGOOS = func() string { return "windows" }
	codexAppGOARCH = func() string { return "amd64" }
	codexAppIsWSL = func() bool { return false }
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) {
		return filepath.Join(t.TempDir(), "chatgpt"), nil
	}
	const wantProxyURL = "http://127.0.0.1:1234"
	codexAppEnsureProxyURLFn = func(_ context.Context, gotStore *config.Store, gotProfile config.Profile, _ []config.Instance, _ io.Writer) (string, error) {
		if gotStore.Path() != configPath {
			t.Fatalf("config store path = %q, want %q", gotStore.Path(), configPath)
		}
		if gotProfile.ID != profile.ID {
			t.Fatalf("profile = %#v, want %#v", gotProfile, profile)
		}
		return wantProxyURL, nil
	}
	var gotProxyURL string
	codexAppUpgradeManagedInstallFn = func(_ context.Context, _ string, opts codexDesktopAppOptions) (codexWindowsManagedInstallState, bool, error) {
		gotProxyURL = opts.ProxyURL
		return codexWindowsManagedInstallState{PackageVersion: "26.803.5235.0"}, true, nil
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"--config", configPath, "--upgrade-codex-app", "CI recording proxy"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("upgrade app command with profile: %v", err)
	}
	if gotProxyURL != wantProxyURL {
		t.Fatalf("managed update proxy URL = %q, want %q", gotProxyURL, wantProxyURL)
	}
}

func TestRootUpgradeCodexAppRejectsUnsupportedPlatform(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	prevUpgrade := codexAppUpgradeManagedInstallFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
		codexAppUpgradeManagedInstallFn = prevUpgrade
	})
	codexAppGOOS = func() string { return "darwin" }
	codexAppIsWSL = func() bool { return false }
	codexAppUpgradeManagedInstallFn = func(context.Context, string, codexDesktopAppOptions) (codexWindowsManagedInstallState, bool, error) {
		t.Fatal("unsupported platform must not start managed app update")
		return codexWindowsManagedInstallState{}, false, nil
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"--upgrade-codex-app"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "only supported on native Windows or WSL") {
		t.Fatalf("unsupported platform error = %v", err)
	}
}

func TestRootUpgradeCodexAppRejectsUnsupportedArchitecture(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevGOARCH := codexAppGOARCH
	prevWSL := codexAppIsWSL
	prevUpgrade := codexAppUpgradeManagedInstallFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppGOARCH = prevGOARCH
		codexAppIsWSL = prevWSL
		codexAppUpgradeManagedInstallFn = prevUpgrade
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppGOARCH = func() string { return "arm64" }
	codexAppIsWSL = func() bool { return false }
	codexAppUpgradeManagedInstallFn = func(context.Context, string, codexDesktopAppOptions) (codexWindowsManagedInstallState, bool, error) {
		t.Fatal("unsupported architecture must not start managed app update")
		return codexWindowsManagedInstallState{}, false, nil
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"--upgrade-codex-app"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "supports x64 Windows only") {
		t.Fatalf("unsupported architecture error = %v", err)
	}
}

func TestRootUpgradeCodexAppRejectsCLIUpgradeCombination(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{"--upgrade-codex", "--upgrade-codex-app"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "cannot be used together") {
		t.Fatalf("combined upgrade flags error = %v", err)
	}
}
