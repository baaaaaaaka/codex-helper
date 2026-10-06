package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCurrentWindowsTokenElevationQuery(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("native Windows token API")
	}
	if _, err := currentWindowsTokenElevated(); err != nil {
		t.Fatalf("query current process elevation token: %v", err)
	}
}

func TestWarnIfWindowsAppElevatedIsNativeWindowsOnly(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevElevation := codexAppTokenElevationFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppTokenElevationFn = prevElevation
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppTokenElevationFn = func() (bool, error) { return true, nil }
	var warning strings.Builder
	warnIfWindowsAppElevated(&warning)
	if !strings.Contains(warning.String(), "elevated Windows token") {
		t.Fatalf("missing elevated-token warning: %q", warning.String())
	}

	codexAppGOOS = func() string { return "linux" }
	warning.Reset()
	warnIfWindowsAppElevated(&warning)
	if warning.Len() != 0 {
		t.Fatalf("non-Windows host emitted elevation warning: %q", warning.String())
	}
}

func TestPreflightCodexWindowsAppElevationAllowsExistingManagedRuntime(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevElevation := codexAppTokenElevationFn
	prevRoot := codexAppWindowsManagedRootFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppTokenElevationFn = prevElevation
		codexAppWindowsManagedRootFn = prevRoot
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppTokenElevationFn = func() (bool, error) { return true, nil }
	root := t.TempDir()
	exePath := filepath.Join(root, "versions", "v1", "app", codexDesktopWindowsCurrentExecutable)
	if err := os.MkdirAll(filepath.Dir(exePath), 0o700); err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	if err := os.WriteFile(exePath, []byte("managed-app"), 0o700); err != nil {
		t.Fatalf("write executable: %v", err)
	}
	exeHash, err := sha256File(exePath)
	if err != nil {
		t.Fatalf("hash executable: %v", err)
	}
	if err := writeCodexWindowsManagedState(root, codexWindowsManagedInstallState{
		PackageName:      codexDesktopWindowsPackageName,
		PackageVersion:   "26.721.4979.0",
		Architecture:     codexWindowsManagedArch,
		Publisher:        "CN=TestPublisher",
		PackageSHA256:    strings.Repeat("a", 64),
		ExecutableSHA256: exeHash,
		RuntimeRelative:  "versions/v1",
	}); err != nil {
		t.Fatalf("write current state: %v", err)
	}
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return root, nil }
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "managed-only")
	if err := preflightCodexWindowsAppElevation(context.Background(), codexDesktopAppOptions{}); err != nil {
		t.Fatalf("existing managed runtime should remain launchable: %v", err)
	}
}

func TestPreflightCodexWindowsAppElevationRefusesMissingManagedRuntimeBeforeWrite(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevElevation := codexAppTokenElevationFn
	prevRoot := codexAppWindowsManagedRootFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppTokenElevationFn = prevElevation
		codexAppWindowsManagedRootFn = prevRoot
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppTokenElevationFn = func() (bool, error) { return true, nil }
	root := filepath.Join(t.TempDir(), "missing-managed-root")
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return root, nil }
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "managed-only")
	err := preflightCodexWindowsAppElevation(context.Background(), codexDesktopAppOptions{})
	if err == nil || !strings.Contains(err.Error(), "without Administrator privileges") {
		t.Fatalf("elevated managed install error = %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("preflight created managed root: stat err=%v", err)
	}
}

func TestPreflightCodexWindowsAppElevationFailsClosedWhenTokenUnknown(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevElevation := codexAppTokenElevationFn
	prevRoot := codexAppWindowsManagedRootFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppTokenElevationFn = prevElevation
		codexAppWindowsManagedRootFn = prevRoot
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppTokenElevationFn = func() (bool, error) { return false, errors.New("token query failed") }
	root := filepath.Join(t.TempDir(), "missing-managed-root")
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return root, nil }
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "managed-only")
	err := preflightCodexWindowsAppElevation(context.Background(), codexDesktopAppOptions{})
	if err == nil || !strings.Contains(err.Error(), "cannot verify the current Windows token") {
		t.Fatalf("unknown-token install error = %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("preflight created managed root: stat err=%v", err)
	}
}

func TestPreflightCodexWindowsAppElevationAllowsInstalledStoreApp(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevElevation := codexAppTokenElevationFn
	prevOutput := codexAppCommandOutput
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppTokenElevationFn = prevElevation
		codexAppCommandOutput = prevOutput
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppTokenElevationFn = func() (bool, error) { return true, nil }
	codexAppCommandOutput = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("CXP_APPX_PRESENT"), nil
	}
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "legacy")
	if err := preflightCodexWindowsAppElevation(context.Background(), codexDesktopAppOptions{}); err != nil {
		t.Fatalf("installed Store app should remain launchable: %v", err)
	}
}

func TestPreflightCodexWindowsAppElevationRefusesMissingStoreApp(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevElevation := codexAppTokenElevationFn
	prevOutput := codexAppCommandOutput
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppTokenElevationFn = prevElevation
		codexAppCommandOutput = prevOutput
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppTokenElevationFn = func() (bool, error) { return true, nil }
	codexAppCommandOutput = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("CXP_APPX_MISSING"), nil
	}
	t.Setenv("CXP_WINDOWS_APP_BACKEND", "legacy")
	err := preflightCodexWindowsAppElevation(context.Background(), codexDesktopAppOptions{})
	if err == nil || !strings.Contains(err.Error(), "Microsoft Store app") {
		t.Fatalf("elevated Store install error = %v", err)
	}
}

func TestPreflightCodexWindowsAppElevationSkipsWSL(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevElevation := codexAppTokenElevationFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppTokenElevationFn = prevElevation
	})
	codexAppGOOS = func() string { return "linux" }
	codexAppTokenElevationFn = func() (bool, error) { return false, errors.New("must not be queried") }
	if err := preflightCodexWindowsAppElevation(context.Background(), codexDesktopAppOptions{}); err != nil {
		t.Fatalf("WSL must skip native Windows elevation policy: %v", err)
	}
}

func TestManagedWindowsWriterRejectsElevationBeforeCreatingRoot(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevElevation := codexAppTokenElevationFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppTokenElevationFn = prevElevation
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppTokenElevationFn = func() (bool, error) { return true, nil }
	root := filepath.Join(t.TempDir(), "managed-root")
	_, err := ensureCodexWindowsManagedInstall(context.Background(), root, codexDesktopAppOptions{})
	if err == nil || !strings.Contains(err.Error(), "without Administrator privileges") {
		t.Fatalf("elevated managed writer error = %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("managed writer created root before refusing elevation: %v", err)
	}
}

func TestLegacyWindowsLauncherSuppressesWingetWhenElevated(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := codexAppGOOS
	prevElevation := codexAppTokenElevationFn
	prevLookPath := codexAppLookPath
	prevRun := codexAppRunCommand
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppTokenElevationFn = prevElevation
		codexAppLookPath = prevLookPath
		codexAppRunCommand = prevRun
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppTokenElevationFn = func() (bool, error) { return true, nil }
	codexAppLookPath = func(string) (string, error) { return "powershell.exe", nil }
	var script string
	codexAppRunCommand = func(_ context.Context, _ io.Writer, _ string, args ...string) error {
		script = args[len(args)-1]
		return nil
	}
	if err := launchCodexDesktopAppWindowsLegacy(context.Background(), codexDesktopAppOptions{Log: io.Discard}, true, true); err != nil {
		t.Fatalf("legacy launch with elevated token: %v", err)
	}
	if !strings.Contains(script, "$allowStoreInstall = $false") || strings.Contains(script, "install --id $storeId") && strings.Contains(script, "$allowStoreInstall = $true") {
		t.Fatalf("elevated legacy launch must not permit winget installation:\n%s", script)
	}
}
