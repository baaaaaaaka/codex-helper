package cli

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/manager"
)

func TestWindowsManagedAppReadManifestAndRejectsUnsafeZipPath(t *testing.T) {
	packagePath := filepath.Join(t.TempDir(), "ChatGPT.msix")
	writeTestCodexWindowsManagedMSIX(t, packagePath, "CN=TestPublisher", "app/ChatGPT.exe", []byte("chatgpt"))

	manifest, err := readCodexWindowsManagedManifest(packagePath)
	if err != nil {
		t.Fatalf("read managed manifest: %v", err)
	}
	if manifest.PackageName != codexDesktopWindowsPackageName || manifest.Architecture != codexWindowsManagedArch || manifest.Executable != "app/ChatGPT.exe" {
		t.Fatalf("managed manifest = %#v", manifest)
	}
	for _, name := range []string{"../escape", "/absolute", `C:\absolute`, `foo/../../escape`, `foo:bar`} {
		if _, err := safeCodexWindowsManagedZipPath(t.TempDir(), name); err == nil {
			t.Fatalf("unsafe managed package path %q was accepted", name)
		}
	}
}

func TestWindowsManagedAppInstallPublishesAndReusesCache(t *testing.T) {
	lockCLITestHooks(t)
	root := t.TempDir()
	packagePath := filepath.Join(t.TempDir(), "ChatGPT.msix")
	writeTestCodexWindowsManagedMSIX(t, packagePath, "CN=TestPublisher", "app/ChatGPT.exe", []byte("chatgpt-v1"))

	prevRoot := codexAppWindowsManagedRootFn
	prevDownload := codexAppDownloadPackageFn
	prevOutput := codexAppCommandOutput
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppWindowsManagedRootFn = prevRoot
		codexAppDownloadPackageFn = prevDownload
		codexAppCommandOutput = prevOutput
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return root, nil }
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	codexAppDownloadPackageFn = func(_ context.Context, opts codexAppDownloadOptions) error {
		data, err := os.ReadFile(packagePath)
		if err != nil {
			return err
		}
		return os.WriteFile(opts.Path, data, 0o600)
	}
	codexAppCommandOutput = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "Get-AuthenticodeSignature") {
			return []byte("CN=TestPublisher\n"), nil
		}
		return nil, errors.New("unexpected PowerShell command")
	}

	first, err := ensureCodexWindowsManagedInstall(context.Background(), root, codexDesktopAppOptions{Log: io.Discard})
	if err != nil {
		t.Fatalf("first managed install: %v", err)
	}
	if first.RuntimeRelative == "" || first.ExecutableSHA256 == "" {
		t.Fatalf("managed install state missing runtime/hash: %#v", first)
	}
	exePath := filepath.Join(root, filepath.FromSlash(first.RuntimeRelative), "app", codexDesktopWindowsCurrentExecutable)
	if data, err := os.ReadFile(exePath); err != nil || string(data) != "chatgpt-v1" {
		t.Fatalf("extracted ChatGPT executable = %q, err=%v", string(data), err)
	}
	statePath := filepath.Join(root, codexWindowsManagedCurrentState)
	stateInfo, err := os.Stat(statePath)
	if err != nil {
		t.Fatalf("stat published managed state: %v", err)
	}

	codexAppDownloadPackageFn = func(context.Context, codexAppDownloadOptions) error {
		t.Fatal("valid managed cache should not download the package again")
		return nil
	}
	second, err := ensureCodexWindowsManagedInstall(context.Background(), root, codexDesktopAppOptions{})
	if err != nil {
		t.Fatalf("cached managed install: %v", err)
	}
	if second.RuntimeRelative != first.RuntimeRelative || second.PackageSHA256 != first.PackageSHA256 {
		t.Fatalf("cached state changed: first=%#v second=%#v", first, second)
	}
	afterInfo, err := os.Stat(statePath)
	if err != nil {
		t.Fatalf("stat cached managed state: %v", err)
	}
	if !afterInfo.ModTime().Equal(stateInfo.ModTime()) {
		t.Fatalf("warm cache rewrote current.json: before=%s after=%s", stateInfo.ModTime(), afterInfo.ModTime())
	}
}

func TestWindowsManagedAppVerifyPackageRejectsSignerPublisherMismatch(t *testing.T) {
	lockCLITestHooks(t)
	packagePath := filepath.Join(t.TempDir(), "ChatGPT.msix")
	writeTestCodexWindowsManagedMSIX(t, packagePath, "CN=ManifestPublisher", "app/ChatGPT.exe", []byte("chatgpt"))
	prevOutput := codexAppCommandOutput
	prevLookPath := codexAppLookPath
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppCommandOutput = prevOutput
		codexAppLookPath = prevLookPath
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	codexAppLookPath = func(name string) (string, error) { return name, nil }
	codexAppCommandOutput = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("CN=DifferentSigner"), nil
	}
	if _, _, err := verifyCodexWindowsManagedPackage(context.Background(), packagePath); err == nil || !strings.Contains(err.Error(), "does not match manifest publisher") {
		t.Fatalf("signer mismatch error = %v", err)
	}
}

func TestWindowsManagedAppAuthenticodeFallsBackToPowerShellSeven(t *testing.T) {
	lockCLITestHooks(t)
	packagePath := filepath.Join(t.TempDir(), "ChatGPT.msix")
	writeTestCodexWindowsManagedMSIX(t, packagePath, "CN=TestPublisher", "app/ChatGPT.exe", []byte("chatgpt"))

	prevOutput := codexAppCommandOutput
	prevLookPath := codexAppLookPath
	prevPowerShell := teamsServicePowerShellExecutable
	t.Cleanup(func() {
		codexAppCommandOutput = prevOutput
		codexAppLookPath = prevLookPath
		teamsServicePowerShellExecutable = prevPowerShell
	})
	teamsServicePowerShellExecutable = func() string { return "powershell.exe" }
	codexAppLookPath = func(name string) (string, error) {
		if strings.EqualFold(name, "pwsh.exe") {
			return name, nil
		}
		return "", os.ErrNotExist
	}
	var shells []string
	codexAppCommandOutput = func(_ context.Context, name string, args ...string) ([]byte, error) {
		shells = append(shells, name)
		if !strings.Contains(strings.Join(args, " "), "Get-AuthenticodeSignature") {
			return nil, errors.New("missing Authenticode verification command")
		}
		if strings.EqualFold(name, "powershell.exe") {
			return nil, errors.New("Get-AuthenticodeSignature: Microsoft.PowerShell.Security could not be loaded")
		}
		return []byte("CN=TestPublisher\n"), nil
	}

	if _, _, err := verifyCodexWindowsManagedPackage(context.Background(), packagePath); err != nil {
		t.Fatalf("PowerShell 7 Authenticode fallback: %v", err)
	}
	if len(shells) != 2 || !strings.EqualFold(shells[0], "powershell.exe") || !strings.EqualFold(shells[1], "pwsh.exe") {
		t.Fatalf("Authenticode shells = %v, want powershell.exe then pwsh.exe", shells)
	}
}

func TestWindowsManagedAppAuthenticodeNeverAcceptsUnavailableVerifiers(t *testing.T) {
	lockCLITestHooks(t)
	packagePath := filepath.Join(t.TempDir(), "ChatGPT.msix")
	writeTestCodexWindowsManagedMSIX(t, packagePath, "CN=TestPublisher", "app/ChatGPT.exe", []byte("chatgpt"))

	prevOutput := codexAppCommandOutput
	prevLookPath := codexAppLookPath
	prevPowerShell := teamsServicePowerShellExecutable
	t.Cleanup(func() {
		codexAppCommandOutput = prevOutput
		codexAppLookPath = prevLookPath
		teamsServicePowerShellExecutable = prevPowerShell
	})
	teamsServicePowerShellExecutable = func() string { return "powershell.exe" }
	codexAppLookPath = func(string) (string, error) { return "", os.ErrNotExist }
	codexAppCommandOutput = func(context.Context, string, ...string) ([]byte, error) {
		return nil, errors.New("Authenticode verifier unavailable")
	}

	if _, _, err := verifyCodexWindowsManagedPackage(context.Background(), packagePath); err == nil || !strings.Contains(err.Error(), "Authenticode verifier unavailable") {
		t.Fatalf("unavailable verifier error = %v", err)
	}
}

func TestWindowsManagedAppAuthenticodeRejectionNeverFallsBack(t *testing.T) {
	lockCLITestHooks(t)
	packagePath := filepath.Join(t.TempDir(), "ChatGPT.msix")
	writeTestCodexWindowsManagedMSIX(t, packagePath, "CN=TestPublisher", "app/ChatGPT.exe", []byte("chatgpt"))

	prevOutput := codexAppCommandOutput
	prevLookPath := codexAppLookPath
	prevPowerShell := teamsServicePowerShellExecutable
	t.Cleanup(func() {
		codexAppCommandOutput = prevOutput
		codexAppLookPath = prevLookPath
		teamsServicePowerShellExecutable = prevPowerShell
	})
	teamsServicePowerShellExecutable = func() string { return "powershell.exe" }
	codexAppLookPath = func(name string) (string, error) { return name, nil }
	var shells []string
	codexAppCommandOutput = func(_ context.Context, name string, _ ...string) ([]byte, error) {
		shells = append(shells, name)
		if strings.EqualFold(name, "pwsh.exe") {
			return []byte("CN=TestPublisher\n"), nil
		}
		return nil, errors.New("Authenticode status is NotSigned")
	}

	if _, _, err := verifyCodexWindowsManagedPackage(context.Background(), packagePath); err == nil || !strings.Contains(err.Error(), "Authenticode status is NotSigned") {
		t.Fatalf("signature rejection error = %v", err)
	}
	if len(shells) != 1 || !strings.EqualFold(shells[0], "powershell.exe") {
		t.Fatalf("signature rejection shells = %v, want no fallback", shells)
	}
}

func TestWindowsManagedAppFallbackRunsOnceWithoutStoreOrAppX(t *testing.T) {
	lockCLITestHooks(t)
	prevRoot := codexAppWindowsManagedRootFn
	prevLookPath := codexAppLookPath
	prevRun := codexAppRunCommand
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppWindowsManagedRootFn = prevRoot
		codexAppLookPath = prevLookPath
		codexAppRunCommand = prevRun
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return "", errors.New("managed cache unavailable") }
	codexAppLookPath = func(name string) (string, error) { return name, nil }
	var script string
	legacyCalls := 0
	codexAppRunCommand = func(_ context.Context, _ io.Writer, _ string, args ...string) error {
		legacyCalls++
		script = args[len(args)-1]
		return nil
	}
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "auto")

	err := launchCodexDesktopAppWindows(context.Background(), codexDesktopAppOptions{
		Cwd:                  `C:\Users\Alice`,
		ProxyURL:             "http://127.0.0.1:23123",
		RequiresDirectLaunch: true,
	})
	if err != nil {
		t.Fatalf("managed launch with legacy fallback: %v", err)
	}
	if !strings.Contains(script, "$allowStoreInstall = $false") || !strings.Contains(script, "$allowAppXFallback = $false") {
		t.Fatalf("legacy fallback script allowed Store/AppX activation:\n%s", script)
	}
	if legacyCalls != 1 {
		t.Fatalf("legacy fallback calls = %d, want exactly one", legacyCalls)
	}
}

func TestWindowsManagedAppManagedOnlyDoesNotFallback(t *testing.T) {
	lockCLITestHooks(t)
	prevRoot := codexAppWindowsManagedRootFn
	prevLookPath := codexAppLookPath
	prevRun := codexAppRunCommand
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppWindowsManagedRootFn = prevRoot
		codexAppLookPath = prevLookPath
		codexAppRunCommand = prevRun
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return "", errors.New("managed cache unavailable") }
	codexAppLookPath = func(name string) (string, error) { return name, nil }
	codexAppRunCommand = func(context.Context, io.Writer, string, ...string) error {
		t.Fatal("managed-only backend must not invoke legacy fallback")
		return nil
	}
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "managed-only")

	err := launchCodexDesktopAppWindows(context.Background(), codexDesktopAppOptions{
		Cwd:                  `C:\Users\Alice`,
		ProxyURL:             "http://127.0.0.1:23123",
		RequiresDirectLaunch: true,
	})
	if err == nil || !strings.Contains(err.Error(), "managed cache unavailable") {
		t.Fatalf("managed-only error = %v", err)
	}
}

func TestWindowsManagedAppConflictDoesNotFallback(t *testing.T) {
	lockCLITestHooks(t)
	root := t.TempDir()
	exePath := filepath.Join(root, "versions", "v", "app", codexDesktopWindowsCurrentExecutable)
	if err := os.MkdirAll(filepath.Dir(exePath), 0o700); err != nil {
		t.Fatalf("create managed conflict runtime: %v", err)
	}
	if err := os.WriteFile(exePath, []byte("fake-exe"), 0o700); err != nil {
		t.Fatalf("write managed conflict executable: %v", err)
	}
	exeHash, err := sha256File(exePath)
	if err != nil {
		t.Fatalf("hash managed conflict executable: %v", err)
	}
	if err := writeCodexWindowsManagedState(root, codexWindowsManagedInstallState{
		PackageName:      codexDesktopWindowsPackageName,
		Architecture:     codexWindowsManagedArch,
		RuntimeRelative:  "versions/v",
		ExecutableSHA256: exeHash,
	}); err != nil {
		t.Fatalf("write managed conflict state: %v", err)
	}

	prevRoot := codexAppWindowsManagedRootFn
	prevLookPath := codexAppLookPath
	prevOutput := codexAppCommandOutput
	prevRun := codexAppRunCommand
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppWindowsManagedRootFn = prevRoot
		codexAppLookPath = prevLookPath
		codexAppCommandOutput = prevOutput
		codexAppRunCommand = prevRun
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return root, nil }
	codexAppLookPath = func(name string) (string, error) { return name, nil }
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	codexAppCommandOutput = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("CXP_MANAGED_CONFLICT"), errors.New("already running")
	}
	codexAppRunCommand = func(context.Context, io.Writer, string, ...string) error {
		t.Fatal("managed conflict must not invoke legacy fallback")
		return nil
	}
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "auto")

	err = launchCodexDesktopAppWindows(context.Background(), codexDesktopAppOptions{
		Cwd:                  `C:\Users\Alice`,
		ProxyURL:             "http://127.0.0.1:23123",
		RequiresDirectLaunch: true,
	})
	if err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("managed conflict error = %v", err)
	}
}

func TestWindowsManagedAppStartedUncertainDoesNotFallback(t *testing.T) {
	lockCLITestHooks(t)
	root := t.TempDir()
	exePath := filepath.Join(root, "versions", "cached", "app", codexDesktopWindowsCurrentExecutable)
	if err := os.MkdirAll(filepath.Dir(exePath), 0o700); err != nil {
		t.Fatalf("create managed uncertain runtime: %v", err)
	}
	if err := os.WriteFile(exePath, []byte("fake-exe"), 0o700); err != nil {
		t.Fatalf("write managed uncertain executable: %v", err)
	}
	exeHash, err := sha256File(exePath)
	if err != nil {
		t.Fatalf("hash managed uncertain executable: %v", err)
	}
	if err := writeCodexWindowsManagedState(root, codexWindowsManagedInstallState{
		PackageName:      codexDesktopWindowsPackageName,
		PackageVersion:   "26.721.4979.0",
		Architecture:     codexWindowsManagedArch,
		Publisher:        "CN=TestPublisher",
		PackageSHA256:    strings.Repeat("a", sha256.Size*2),
		ExecutableSHA256: exeHash,
		RuntimeRelative:  "versions/cached",
	}); err != nil {
		t.Fatalf("write managed uncertain state: %v", err)
	}

	prevRoot := codexAppWindowsManagedRootFn
	prevLookPath := codexAppLookPath
	prevOutput := codexAppCommandOutput
	prevRun := codexAppRunCommand
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppWindowsManagedRootFn = prevRoot
		codexAppLookPath = prevLookPath
		codexAppCommandOutput = prevOutput
		codexAppRunCommand = prevRun
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return root, nil }
	codexAppLookPath = func(name string) (string, error) { return name, nil }
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	codexAppCommandOutput = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		script := strings.Join(args, " ")
		if strings.Contains(script, "CXP_MANAGED_STARTED_UNCERTAIN") {
			return []byte("CXP_MANAGED_PID=4242\nCXP_MANAGED_STARTED_UNCERTAIN\n"), errors.New("managed process path mismatch")
		}
		return nil, nil
	}
	codexAppRunCommand = func(context.Context, io.Writer, string, ...string) error {
		t.Fatal("uncertain managed launch must not invoke legacy fallback")
		return nil
	}
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "auto")

	err = launchCodexDesktopAppWindows(context.Background(), codexDesktopAppOptions{
		Cwd:                  `C:\Users\Alice`,
		ProxyURL:             "http://127.0.0.1:23123",
		RequiresDirectLaunch: true,
	})
	if err == nil || !strings.Contains(err.Error(), "may have started") {
		t.Fatalf("uncertain managed launch error = %v", err)
	}
}

func TestWindowsManagedAppLaunchScriptPassesProxyAndIsolatedHome(t *testing.T) {
	script := codexDesktopWindowsManagedLaunchScript(codexDesktopAppOptions{
		Cwd:              `C:\Users\Alice\work`,
		ExtraEnv:         []string{`CODEX_DIR=C:\Users\Alice\.codex-profile`, `CODEX_HOME=C:\Users\Alice\.codex-profile`},
		ProxyURL:         "http://127.0.0.1:23123",
		ModelProfileName: "profile",
	}, `C:\Users\Alice\AppData\Local\cxp\apps\chatgpt\versions\v\app\ChatGPT.exe`)
	for _, want := range []string{
		"--proxy-server=http://127.0.0.1:23123",
		"$env:CODEX_DIR = 'C:\\Users\\Alice\\.codex-profile'",
		"$env:CODEX_HOME = 'C:\\Users\\Alice\\.codex-profile'",
		"$env:NO_PROXY = ''",
		"$env:no_proxy = ''",
		"Get-Process -Name 'ChatGPT','Codex'",
		"Start-Process @start",
		"$process.MainModule.FileName",
		"CXP_MANAGED_STARTED_UNCERTAIN",
		"CXP_MANAGED_PID=",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("managed launch script missing %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, "Set-ItemProperty") || strings.Contains(script, "netsh winhttp set proxy") {
		t.Fatalf("managed launch script must not change system proxy:\n%s", script)
	}
}

func TestWindowsManagedAppLaunchScriptHonorsWaitForExit(t *testing.T) {
	for _, tc := range []struct {
		name string
		wait bool
		want string
	}{
		{name: "detached", want: "$codexWaitForExit = $false"},
		{name: "wait", wait: true, want: "$codexWaitForExit = $true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			script := codexDesktopWindowsManagedLaunchScript(codexDesktopAppOptions{WaitForExit: tc.wait}, `C:\managed\ChatGPT.exe`)
			if !strings.Contains(script, tc.want) {
				t.Fatalf("managed launch script missing %q:\n%s", tc.want, script)
			}
		})
	}
}

func TestWindowsManagedAppPathForLaunchConvertsWSLPath(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	prevPath := codexAppWSLPathFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
		codexAppWSLPathFn = prevPath
	})
	codexAppGOOS = func() string { return "linux" }
	codexAppIsWSL = func() bool { return true }
	codexAppWSLPathFn = func(path string) (string, error) { return `C:\Users\Alice\` + filepath.Base(path), nil }

	got, err := codexWindowsManagedPathForLaunch(`/mnt/c/Users/Alice/AppData/Local/cxp/apps/chatgpt/current.msix`)
	if err != nil {
		t.Fatalf("convert managed WSL path: %v", err)
	}
	if got != `C:\Users\Alice\current.msix` {
		t.Fatalf("converted managed path = %q", got)
	}
}

func TestWindowsManagedAppLaunchScriptClearsRuntimeMarkersBeforeInheritance(t *testing.T) {
	script := codexDesktopWindowsManagedLaunchScript(codexDesktopAppOptions{
		Cwd:      `C:\Users\Alice\work`,
		ProxyURL: "http://127.0.0.1:23123",
		ExtraEnv: []string{
			"CXP_RUNTIME=private",
			"CXP_RUNTIME_ROOT=C:\\private",
			"CXP_RUNTIME_DISABLE=1",
			"CODEX_HOME=C:\\Users\\Alice\\.codex",
		},
	}, `C:\Users\Alice\AppData\Local\cxp\apps\chatgpt\versions\v\app\ChatGPT.exe`)
	cleanup := codexDesktopWindowsRuntimeMarkerCleanupPowerShell()
	cleanupAt := strings.Index(script, cleanup)
	envAt := strings.Index(script, "$env:CODEX_HOME")
	if cleanupAt < 0 {
		t.Fatalf("managed launch script does not clear CXP runtime markers:\n%s", script)
	}
	if envAt >= 0 && cleanupAt > envAt {
		t.Fatalf("runtime marker cleanup must happen before explicit environment assignments:\n%s", script)
	}
	for _, marker := range []string{"CXP_RUNTIME", "CXP_RUNTIME_ROOT", "CXP_RUNTIME_DISABLE", "CXP_RUNTIME_FORCE"} {
		if !strings.Contains(cleanup, "'"+marker+"'") {
			t.Fatalf("runtime marker %q is not cleared by %q", marker, cleanup)
		}
	}
}

func TestWindowsManagedAppCorruptStateTriggersVerifiedReinstall(t *testing.T) {
	lockCLITestHooks(t)
	root := t.TempDir()
	packagePath := filepath.Join(t.TempDir(), "ChatGPT.msix")
	writeTestCodexWindowsManagedMSIX(t, packagePath, "CN=TestPublisher", "app/ChatGPT.exe", []byte("verified-child"))
	if err := os.WriteFile(filepath.Join(root, codexWindowsManagedCurrentState), []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("write corrupt managed state: %v", err)
	}
	prevDownload := codexAppDownloadPackageFn
	prevOutput := codexAppCommandOutput
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppDownloadPackageFn = prevDownload
		codexAppCommandOutput = prevOutput
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	downloads := 0
	codexAppDownloadPackageFn = func(_ context.Context, opts codexAppDownloadOptions) error {
		downloads++
		data, err := os.ReadFile(packagePath)
		if err != nil {
			return err
		}
		return os.WriteFile(opts.Path, data, 0o600)
	}
	codexAppCommandOutput = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "Get-AuthenticodeSignature") {
			return []byte("CN=TestPublisher\n"), nil
		}
		return nil, errors.New("unexpected PowerShell command")
	}
	state, err := ensureCodexWindowsManagedInstall(context.Background(), root, codexDesktopAppOptions{Log: io.Discard})
	if err != nil {
		t.Fatalf("reinstall after corrupt state: %v", err)
	}
	if downloads != 1 || !isCodexWindowsManagedSHA256(state.ExecutableSHA256) {
		t.Fatalf("reinstall downloads=%d state=%#v", downloads, state)
	}
	if _, ok, err := readValidCodexWindowsManagedState(root); err != nil || !ok {
		t.Fatalf("published state valid=%v err=%v", ok, err)
	}
}

func TestWindowsManagedAppEmptyExecutableHashCannotWarmStart(t *testing.T) {
	lockCLITestHooks(t)
	root := t.TempDir()
	exePath := filepath.Join(root, "versions", "cached", "app", codexDesktopWindowsCurrentExecutable)
	if err := os.MkdirAll(filepath.Dir(exePath), 0o700); err != nil {
		t.Fatalf("create cached runtime: %v", err)
	}
	if err := os.WriteFile(exePath, []byte("cached"), 0o700); err != nil {
		t.Fatalf("write cached executable: %v", err)
	}
	packageHash := strings.Repeat("a", sha256.Size*2)
	if err := writeCodexWindowsManagedState(root, codexWindowsManagedInstallState{
		PackageName:      codexDesktopWindowsPackageName,
		PackageVersion:   "26.721.4979.0",
		Architecture:     codexWindowsManagedArch,
		Publisher:        "CN=TestPublisher",
		PackageSHA256:    packageHash,
		ExecutableSHA256: "",
		RuntimeRelative:  "versions/cached",
	}); err != nil {
		t.Fatalf("write empty-hash state: %v", err)
	}
	if _, ok, err := readValidCodexWindowsManagedState(root); err != nil || ok {
		t.Fatalf("empty executable hash valid=%v err=%v; cache must not warm-start", ok, err)
	}
}

func TestWindowsManagedAppCancelledStartupNeverFallsBack(t *testing.T) {
	lockCLITestHooks(t)
	prevRoot := codexAppWindowsManagedRootFn
	prevLookPath := codexAppLookPath
	prevRun := codexAppRunCommand
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppWindowsManagedRootFn = prevRoot
		codexAppLookPath = prevLookPath
		codexAppRunCommand = prevRun
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	codexAppLookPath = func(name string) (string, error) { return name, nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return "", ctx.Err() }
	codexAppRunCommand = func(context.Context, io.Writer, string, ...string) error {
		t.Fatal("cancelled managed startup must not invoke legacy fallback")
		return nil
	}
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "auto")
	err := launchCodexDesktopAppWindows(ctx, codexDesktopAppOptions{ProxyURL: "http://127.0.0.1:23123", RequiresDirectLaunch: true})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled managed startup error = %v, want context.Canceled", err)
	}
}

func TestWindowsManagedAppHealthCancellationNeverFallsBack(t *testing.T) {
	lockCLITestHooks(t)
	prevOutput := codexAppCommandOutput
	prevRun := codexAppRunCommand
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppCommandOutput = prevOutput
		codexAppRunCommand = prevRun
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "linux" }
	codexAppIsWSL = func() bool { return true }
	ctx, cancel := context.WithCancel(context.Background())
	codexAppCommandOutput = func(context.Context, string, ...string) ([]byte, error) {
		cancel()
		return nil, context.Canceled
	}
	codexAppRunCommand = func(context.Context, io.Writer, string, ...string) error {
		t.Fatal("health cancellation must not invoke legacy fallback")
		return nil
	}
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "auto")
	err := launchCodexDesktopAppWindows(ctx, codexDesktopAppOptions{ProxyURL: "http://127.0.0.1:23123", RequiresDirectLaunch: true})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("health cancellation error = %v, want context.Canceled", err)
	}
}

func TestWindowsManagedAppDirectModeUsesLegacyBackend(t *testing.T) {
	lockCLITestHooks(t)
	prevLookPath := codexAppLookPath
	prevRun := codexAppRunCommand
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppLookPath = prevLookPath
		codexAppRunCommand = prevRun
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	codexAppLookPath = func(name string) (string, error) { return name, nil }
	var script string
	codexAppRunCommand = func(_ context.Context, _ io.Writer, _ string, args ...string) error {
		script = args[len(args)-1]
		return nil
	}
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "auto")
	if err := launchCodexDesktopAppWindows(context.Background(), codexDesktopAppOptions{Cwd: `C:\Users\Alice`, Log: io.Discard}); err != nil {
		t.Fatalf("direct legacy launch: %v", err)
	}
	if !strings.Contains(script, "$allowStoreInstall = $true") || !strings.Contains(script, "$allowAppXFallback = $true") {
		t.Fatalf("ordinary direct launch did not retain legacy Store/AppX policy:\n%s", script)
	}
}

func TestWindowsManagedAppModelProfileUsesDirectBackendAndSafeFallback(t *testing.T) {
	lockCLITestHooks(t)
	prevRoot := codexAppWindowsManagedRootFn
	prevLookPath := codexAppLookPath
	prevRun := codexAppRunCommand
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppWindowsManagedRootFn = prevRoot
		codexAppLookPath = prevLookPath
		codexAppRunCommand = prevRun
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return "", errors.New("managed cache unavailable") }
	codexAppLookPath = func(name string) (string, error) { return name, nil }
	var script string
	codexAppRunCommand = func(_ context.Context, _ io.Writer, _ string, args ...string) error {
		script = args[len(args)-1]
		return nil
	}
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "auto")
	if err := launchCodexDesktopAppWindows(context.Background(), codexDesktopAppOptions{
		Cwd:                  `C:\Users\Alice`,
		ModelProfileName:     "mimo25",
		RequiresDirectLaunch: true,
	}); err != nil {
		t.Fatalf("model-profile managed launch with fallback: %v", err)
	}
	if !strings.Contains(script, "$allowStoreInstall = $false") || !strings.Contains(script, "$allowAppXFallback = $false") {
		t.Fatalf("model-profile fallback allowed Store/AppX activation:\n%s", script)
	}
}

func TestWindowsManagedAppRejectsUnsupportedWindowsArchitecture(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevGOARCH := codexAppGOARCH
	prevWSL := codexAppIsWSL
	prevRun := codexAppRunCommand
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppGOARCH = prevGOARCH
		codexAppIsWSL = prevWSL
		codexAppRunCommand = prevRun
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppGOARCH = func() string { return "arm64" }
	codexAppIsWSL = func() bool { return false }
	codexAppRunCommand = func(context.Context, io.Writer, string, ...string) error {
		t.Fatal("managed-only unsupported architecture must not invoke legacy fallback")
		return nil
	}
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "managed-only")
	err := launchCodexDesktopAppWindows(context.Background(), codexDesktopAppOptions{ProxyURL: "http://127.0.0.1:23123", RequiresDirectLaunch: true})
	if err == nil || !strings.Contains(err.Error(), "supports x64 only") {
		t.Fatalf("unsupported architecture error = %v", err)
	}
}

func TestWindowsManagedAppProxyHealthRejectsWrongIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_codex_proxy/health" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true,"instanceId":"different"}`)
	}))
	t.Cleanup(server.Close)
	port := server.Listener.Addr().(*net.TCPAddr).Port
	err := (manager.HealthClient{Timeout: time.Second}).CheckHTTPProxy(port, "expected")
	if err == nil || !strings.Contains(err.Error(), "unexpected instance id") {
		t.Fatalf("wrong proxy health identity error = %v", err)
	}
}

func TestWindowsManagedAppProxyHealthRejectsFalseHealth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":false,"instanceId":"expected"}`)
	}))
	t.Cleanup(server.Close)
	port := server.Listener.Addr().(*net.TCPAddr).Port
	err := (manager.HealthClient{Timeout: time.Second}).CheckHTTPProxy(port, "expected")
	if err == nil || !strings.Contains(err.Error(), "health check not ok") {
		t.Fatalf("false proxy health error = %v", err)
	}
}

func TestWindowsManagedAppWSLProxyPreflightRequiresHealthAndConnect(t *testing.T) {
	lockCLITestHooks(t)
	prevOutput := codexAppCommandOutput
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppCommandOutput = prevOutput
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "linux" }
	codexAppIsWSL = func() bool { return true }
	var script string
	codexAppCommandOutput = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		script = args[len(args)-1]
		return nil, nil
	}
	if err := ensureWindowsManagedProxyReachable(context.Background(), codexDesktopAppOptions{ProxyURL: "http://127.0.0.1:23123"}); err != nil {
		t.Fatalf("WSL proxy preflight: %v", err)
	}
	for _, want := range []string{"Invoke-RestMethod", "/_codex_proxy/health", "$connectTarget = '127.0.0.1:1'", "'CONNECT ' + $connectTarget", "ReadTimeout = 2000"} {
		if !strings.Contains(script, want) {
			t.Fatalf("WSL proxy preflight script missing %q:\n%s", want, script)
		}
	}
}

func TestWindowsManagedAppWSLProxyPreflightRejectsMalformedURL(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "linux" }
	codexAppIsWSL = func() bool { return true }
	if err := ensureWindowsManagedProxyReachable(context.Background(), codexDesktopAppOptions{ProxyURL: "not-a-proxy"}); err == nil {
		t.Fatal("malformed WSL proxy URL was accepted")
	}
}

func TestWindowsManagedAppPackageHashFormatRejectsFakePass(t *testing.T) {
	for _, value := range []string{"", "deadbeef", strings.Repeat("z", sha256.Size*2), strings.Repeat("a", sha256.Size*2-1)} {
		if isCodexWindowsManagedSHA256(value) {
			t.Fatalf("invalid SHA-256 digest %q was accepted", value)
		}
	}
	if !isCodexWindowsManagedSHA256(hex.EncodeToString(make([]byte, sha256.Size))) {
		t.Fatal("valid all-zero SHA-256 digest was rejected")
	}
}

func TestWindowsManagedAppWrongArchitectureIsRejectedBeforePublish(t *testing.T) {
	lockCLITestHooks(t)
	root := t.TempDir()
	packagePath := filepath.Join(t.TempDir(), "ChatGPT.msix")
	writeTestCodexWindowsManagedMSIXWithArchitecture(t, packagePath, "CN=TestPublisher", "arm64", "app/ChatGPT.exe", []byte("wrong-arch"))
	prevDownload := codexAppDownloadPackageFn
	prevOutput := codexAppCommandOutput
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppDownloadPackageFn = prevDownload
		codexAppCommandOutput = prevOutput
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	codexAppDownloadPackageFn = func(_ context.Context, opts codexAppDownloadOptions) error {
		data, err := os.ReadFile(packagePath)
		if err != nil {
			return err
		}
		return os.WriteFile(opts.Path, data, 0o600)
	}
	codexAppCommandOutput = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "Get-AuthenticodeSignature") {
			return []byte("CN=TestPublisher\n"), nil
		}
		return nil, errors.New("unexpected PowerShell command")
	}
	_, err := ensureCodexWindowsManagedInstall(context.Background(), root, codexDesktopAppOptions{})
	if err == nil || !strings.Contains(err.Error(), "architecture") {
		t.Fatalf("wrong architecture install error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, codexWindowsManagedCurrentState)); !os.IsNotExist(err) {
		t.Fatalf("wrong architecture unexpectedly published state, stat err=%v", err)
	}
}

func TestWindowsManagedAppWrongPackageIdentityIsRejectedBeforePublish(t *testing.T) {
	lockCLITestHooks(t)
	root := t.TempDir()
	packagePath := filepath.Join(t.TempDir(), "ChatGPT.msix")
	writeTestCodexWindowsManagedMSIXWithIdentity(t, packagePath, "Contoso.ChatGPT", "CN=TestPublisher", codexWindowsManagedArch, "app/ChatGPT.exe", []byte("wrong-identity"))
	prevDownload := codexAppDownloadPackageFn
	prevOutput := codexAppCommandOutput
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppDownloadPackageFn = prevDownload
		codexAppCommandOutput = prevOutput
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	codexAppDownloadPackageFn = func(_ context.Context, opts codexAppDownloadOptions) error {
		data, err := os.ReadFile(packagePath)
		if err != nil {
			return err
		}
		return os.WriteFile(opts.Path, data, 0o600)
	}
	codexAppCommandOutput = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "Get-AuthenticodeSignature") {
			return []byte("CN=TestPublisher\n"), nil
		}
		return nil, errors.New("unexpected PowerShell command")
	}
	_, err := ensureCodexWindowsManagedInstall(context.Background(), root, codexDesktopAppOptions{})
	if err == nil || !strings.Contains(err.Error(), "package name") {
		t.Fatalf("wrong package identity install error = %v", err)
	}
}

func TestWindowsManagedAppMissingChatGPTExecutableIsRejected(t *testing.T) {
	lockCLITestHooks(t)
	root := t.TempDir()
	packagePath := filepath.Join(t.TempDir(), "ChatGPT.msix")
	writeTestCodexWindowsManagedMSIX(t, packagePath, "CN=TestPublisher", "app/Other.exe", []byte("not-chatgpt"))
	prevDownload := codexAppDownloadPackageFn
	prevOutput := codexAppCommandOutput
	prevGOOS := codexAppGOOS
	prevWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppDownloadPackageFn = prevDownload
		codexAppCommandOutput = prevOutput
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevWSL
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	codexAppDownloadPackageFn = func(_ context.Context, opts codexAppDownloadOptions) error {
		data, err := os.ReadFile(packagePath)
		if err != nil {
			return err
		}
		return os.WriteFile(opts.Path, data, 0o600)
	}
	codexAppCommandOutput = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "Get-AuthenticodeSignature") {
			return []byte("CN=TestPublisher\n"), nil
		}
		return nil, errors.New("unexpected PowerShell command")
	}
	_, err := ensureCodexWindowsManagedInstall(context.Background(), root, codexDesktopAppOptions{})
	if err == nil || !strings.Contains(err.Error(), "hash extracted ChatGPT executable") {
		t.Fatalf("missing executable install error = %v", err)
	}
}

func TestWindowsManagedAppZipSymlinkIsRejected(t *testing.T) {
	packagePath := filepath.Join(t.TempDir(), "ChatGPT.msix")
	file, err := os.Create(packagePath)
	if err != nil {
		t.Fatalf("create symlink package: %v", err)
	}
	archive := zip.NewWriter(file)
	header := &zip.FileHeader{Name: "app/link", Method: zip.Deflate}
	header.SetMode(os.ModeSymlink | 0o777)
	entry, err := archive.CreateHeader(header)
	if err != nil {
		t.Fatalf("create symlink entry: %v", err)
	}
	if _, err := entry.Write([]byte("target")); err != nil {
		t.Fatalf("write symlink entry: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("close symlink package: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close symlink package file: %v", err)
	}
	if err := extractCodexWindowsManagedPackage(packagePath, t.TempDir()); err == nil || !strings.Contains(err.Error(), "unsupported symlink") {
		t.Fatalf("symlink package error = %v", err)
	}
}

func TestWindowsManagedAppPreexistingSymlinkParentIsRejected(t *testing.T) {
	if os.Getenv("CI") != "" && os.Getenv("RUNNER_OS") == "Windows" {
		t.Skip("creating a symlink requires developer-mode or SeCreateSymbolicLinkPrivilege on Windows")
	}
	destination := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(destination, "app")); err != nil {
		t.Skipf("symlink is unavailable: %v", err)
	}
	packagePath := filepath.Join(t.TempDir(), "ChatGPT.msix")
	writeTestCodexWindowsManagedMSIX(t, packagePath, "CN=TestPublisher", "app/ChatGPT.exe", []byte("chatgpt"))
	if err := extractCodexWindowsManagedPackage(packagePath, destination); err == nil || !strings.Contains(err.Error(), "symlink or reparse point") {
		t.Fatalf("preexisting symlink parent error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, codexDesktopWindowsCurrentExecutable)); !os.IsNotExist(err) {
		t.Fatalf("managed extraction wrote through symlink, stat err=%v", err)
	}
}

func TestWindowsManagedAppUnsafeCurrentStateIsCacheMiss(t *testing.T) {
	root := t.TempDir()
	if err := writeCodexWindowsManagedState(root, codexWindowsManagedInstallState{
		PackageName:      codexDesktopWindowsPackageName,
		PackageVersion:   "26.721.4979.0",
		Architecture:     codexWindowsManagedArch,
		Publisher:        "CN=TestPublisher",
		PackageSHA256:    strings.Repeat("a", sha256.Size*2),
		ExecutableSHA256: strings.Repeat("b", sha256.Size*2),
		RuntimeRelative:  "versions/../escape",
	}); err != nil {
		t.Fatalf("write unsafe current state: %v", err)
	}
	if _, ok, err := readValidCodexWindowsManagedState(root); err != nil || ok {
		t.Fatalf("unsafe current state valid=%v err=%v; expected a recoverable cache miss", ok, err)
	}
}

func TestWindowsManagedAppStaleDeadLockIsReclaimed(t *testing.T) {
	lockCLITestHooks(t)
	lockPath := filepath.Join(t.TempDir(), codexWindowsManagedLockDir)
	if err := os.MkdirAll(lockPath, 0o700); err != nil {
		t.Fatalf("create stale lock: %v", err)
	}
	oldNow := codexWindowsManagedNow
	oldAlive := codexWindowsManagedProcessAlive
	t.Cleanup(func() {
		codexWindowsManagedNow = oldNow
		codexWindowsManagedProcessAlive = oldAlive
	})
	now := time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC)
	codexWindowsManagedNow = func() time.Time { return now }
	codexWindowsManagedProcessAlive = func(pid int) bool { return pid == 4242 }
	owner, err := json.Marshal(codexWindowsManagedLockMetadata{PID: 999999, CreatedAt: now.Add(-codexWindowsManagedLockStaleAfter - time.Second)})
	if err != nil {
		t.Fatalf("marshal stale owner: %v", err)
	}
	if err := os.WriteFile(filepath.Join(lockPath, codexWindowsManagedLockOwner), append(owner, '\n'), 0o600); err != nil {
		t.Fatalf("write stale owner: %v", err)
	}
	reclaimed, err := reclaimStaleCodexWindowsManagedLock(lockPath)
	if err != nil || !reclaimed {
		t.Fatalf("stale lock reclaimed=%v err=%v", reclaimed, err)
	}
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("stale lock still exists, stat err=%v", err)
	}
}

func TestWindowsManagedAppLiveOrUnknownLockIsNeverDeletedAndWaitIsBounded(t *testing.T) {
	for _, tc := range []struct {
		name  string
		owner *codexWindowsManagedLockMetadata
		alive bool
	}{
		{name: "live", owner: &codexWindowsManagedLockMetadata{PID: 4242, CreatedAt: time.Unix(0, 0).UTC()}, alive: true},
		{name: "unknown", owner: nil, alive: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lockCLITestHooks(t)
			root := t.TempDir()
			lockPath := filepath.Join(root, codexWindowsManagedLockDir)
			if err := os.MkdirAll(lockPath, 0o700); err != nil {
				t.Fatalf("create lock: %v", err)
			}
			if tc.owner != nil {
				data, err := json.Marshal(tc.owner)
				if err != nil {
					t.Fatalf("marshal owner: %v", err)
				}
				if err := os.WriteFile(filepath.Join(lockPath, codexWindowsManagedLockOwner), append(data, '\n'), 0o600); err != nil {
					t.Fatalf("write owner: %v", err)
				}
			}
			oldNow := codexWindowsManagedNow
			oldAlive := codexWindowsManagedProcessAlive
			codexWindowsManagedNow = func() time.Time { return time.Unix(0, 0).Add(codexWindowsManagedLockStaleAfter + time.Hour) }
			codexWindowsManagedProcessAlive = func(int) bool { return tc.alive }
			t.Cleanup(func() {
				codexWindowsManagedNow = oldNow
				codexWindowsManagedProcessAlive = oldAlive
			})
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if _, err := ensureCodexWindowsManagedInstall(ctx, root, codexDesktopAppOptions{}); err == nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lock wait error = %v, want bounded context deadline", err)
			}
			if _, err := os.Stat(lockPath); err != nil {
				t.Fatalf("protected lock disappeared: %v", err)
			}
		})
	}
}

func writeTestCodexWindowsManagedMSIX(t *testing.T, packagePath, publisher, executable string, executableData []byte) {
	writeTestCodexWindowsManagedMSIXWithArchitecture(t, packagePath, publisher, codexWindowsManagedArch, executable, executableData)
}

func writeTestCodexWindowsManagedMSIXWithArchitecture(t *testing.T, packagePath, publisher, architecture, executable string, executableData []byte) {
	writeTestCodexWindowsManagedMSIXWithIdentity(t, packagePath, codexDesktopWindowsPackageName, publisher, architecture, executable, executableData)
}

func writeTestCodexWindowsManagedMSIXWithIdentity(t *testing.T, packagePath, packageName, publisher, architecture, executable string, executableData []byte) {
	t.Helper()
	file, err := os.Create(packagePath)
	if err != nil {
		t.Fatalf("create test MSIX: %v", err)
	}
	archive := zip.NewWriter(file)
	manifest := `<Package><Identity Name="` + packageName + `" ProcessorArchitecture="` + architecture + `" Version="26.721.4979.0" Publisher="` + publisher + `"/><Applications><Application Executable="` + executable + `"/></Applications></Package>`
	for name, data := range map[string][]byte{
		"AppxManifest.xml": []byte(manifest),
		executable:         executableData,
	} {
		entry, err := archive.Create(name)
		if err != nil {
			t.Fatalf("create test MSIX entry %s: %v", name, err)
		}
		if _, err := entry.Write(data); err != nil {
			t.Fatalf("write test MSIX entry %s: %v", name, err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("close test MSIX: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close test MSIX file: %v", err)
	}
}
