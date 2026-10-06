package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompareCodexDesktopMacVersions(t *testing.T) {
	for _, tc := range []struct {
		left    string
		right   string
		want    int
		wantErr bool
	}{
		{left: "26.930.7945.1", right: "26.930.7945.0", want: 1},
		{left: "26.930.7945", right: "26.930.7945.0", want: 0},
		{left: "26.930.7944.9", right: "26.930.7945", want: -1},
		{left: "1.beta", right: "1.0", wantErr: true},
	} {
		got, err := compareCodexDesktopMacVersions(tc.left, tc.right)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("compare %q and %q succeeded; want error", tc.left, tc.right)
			}
			continue
		}
		if err != nil {
			t.Fatalf("compare %q and %q: %v", tc.left, tc.right, err)
		}
		if got != tc.want {
			t.Fatalf("compare %q and %q = %d, want %d", tc.left, tc.right, got, tc.want)
		}
	}
}

func TestCodexDesktopMacAppLockRejectsConcurrentOperation(t *testing.T) {
	home := t.TempDir()
	err := withCodexDesktopMacLock(context.Background(), home, nil, func() error {
		return withCodexDesktopMacLock(context.Background(), home, nil, func() error {
			t.Fatal("second operation unexpectedly acquired the ChatGPT app lock")
			return nil
		})
	})
	if err == nil || !strings.Contains(err.Error(), "another CXP ChatGPT app launch or upgrade is in progress") {
		t.Fatalf("nested lock error = %v", err)
	}
}

func TestRecoverCodexDesktopMacAppBackup(t *testing.T) {
	lockCLITestHooks(t)
	stubCodexAppMacOpenAIIdentity(t)

	prevRunCommand := codexAppRunCommand
	t.Cleanup(func() { codexAppRunCommand = prevRunCommand })
	codexAppRunCommand = func(context.Context, io.Writer, string, ...string) error { return nil }

	installDir := t.TempDir()
	appPath := filepath.Join(installDir, codexDesktopMacCurrentAppName)
	backup := filepath.Join(installDir, ".ChatGPT.app.backup-123.app")
	writeFakeChatGPTMacApp(t, backup, "previous")
	if err := recoverCodexDesktopMacAppBackup(context.Background(), installDir, appPath, io.Discard); err != nil {
		t.Fatalf("recover interrupted app update: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(appPath, "Contents", "MacOS", codexDesktopMacCurrentExecutableName))
	if err != nil {
		t.Fatalf("read restored app: %v", err)
	}
	if string(data) != "previous" {
		t.Fatalf("restored app = %q, want previous", data)
	}
	if _, err := os.Lstat(backup); !os.IsNotExist(err) {
		t.Fatalf("backup remains after recovery: %v", err)
	}
}

func TestCodexDesktopMacApplicationsDirRejectsSymbolicLink(t *testing.T) {
	home := t.TempDir()
	target := t.TempDir()
	applicationsDir := filepath.Join(home, "Applications")
	if err := os.Symlink(target, applicationsDir); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}
	if _, err := codexDesktopMacApplicationsDir(home); err == nil || !strings.Contains(err.Error(), "must be a real directory") {
		t.Fatalf("Applications symlink error = %v", err)
	}
}

func TestCleanCodexDesktopMacStagingDirsRemovesOnlyInterruptedStages(t *testing.T) {
	installDir := t.TempDir()
	staleStage := filepath.Join(installDir, ".codex-desktop-install-crashed")
	if err := os.MkdirAll(staleStage, 0o700); err != nil {
		t.Fatalf("create stale stage: %v", err)
	}
	keep := filepath.Join(installDir, "user-data")
	if err := os.Mkdir(keep, 0o700); err != nil {
		t.Fatalf("create unrelated directory: %v", err)
	}
	if err := cleanCodexDesktopMacStagingDirs(installDir); err != nil {
		t.Fatalf("clean interrupted stages: %v", err)
	}
	if _, err := os.Lstat(staleStage); !os.IsNotExist(err) {
		t.Fatalf("stale stage remains: %v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("unrelated directory was removed: %v", err)
	}
}

func TestUpgradeCodexDesktopAppMacRefusesRunningAppBeforeDownload(t *testing.T) {
	lockCLITestHooks(t)

	prevProcessCheck := codexAppMacProcessRunningFn
	prevDownload := codexAppDownloadPackageFn
	t.Cleanup(func() {
		codexAppMacProcessRunningFn = prevProcessCheck
		codexAppDownloadPackageFn = prevDownload
	})
	codexAppMacProcessRunningFn = func(context.Context) (bool, error) { return true, nil }
	codexAppDownloadPackageFn = func(context.Context, codexAppDownloadOptions) error {
		t.Fatal("upgrade must reject running app before downloading")
		return nil
	}

	_, _, err := upgradeCodexDesktopAppMac(context.Background(), codexDesktopAppOptions{InstallHome: t.TempDir(), Log: io.Discard})
	if err == nil || !strings.Contains(err.Error(), "quit every desktop app instance") {
		t.Fatalf("running app error = %v", err)
	}
}

func TestLaunchCodexDesktopAppMacRejectsRunningDuplicateBeforeEnsure(t *testing.T) {
	lockCLITestHooks(t)

	prevProcessCheck := codexAppMacProcessRunningFn
	prevDownload := codexAppDownloadPackageFn
	t.Cleanup(func() {
		codexAppMacProcessRunningFn = prevProcessCheck
		codexAppDownloadPackageFn = prevDownload
	})
	codexAppMacProcessRunningFn = func(context.Context) (bool, error) { return true, nil }
	codexAppDownloadPackageFn = func(context.Context, codexAppDownloadOptions) error {
		t.Fatal("launch must refuse an existing instance before installing an app")
		return nil
	}

	err := launchCodexDesktopAppMac(context.Background(), codexDesktopAppOptions{InstallHome: t.TempDir(), Log: io.Discard})
	if err == nil || !strings.Contains(err.Error(), "ensure it uses the current-user app") {
		t.Fatalf("duplicate app launch error = %v", err)
	}
}

func TestUpgradeCodexDesktopAppMacUpdatesCurrentUserBundleInPlace(t *testing.T) {
	lockCLITestHooks(t)

	prevProcessCheck := codexAppMacProcessRunningFn
	prevRunCommand := codexAppRunCommand
	prevCommandOutput := codexAppCommandOutput
	prevDownload := codexAppDownloadPackageFn
	prevInstallURL := codexAppMacInstallURL
	prevSystemApps := codexAppMacSystemAppsDir
	t.Cleanup(func() {
		codexAppMacProcessRunningFn = prevProcessCheck
		codexAppRunCommand = prevRunCommand
		codexAppCommandOutput = prevCommandOutput
		codexAppDownloadPackageFn = prevDownload
		codexAppMacInstallURL = prevInstallURL
		codexAppMacSystemAppsDir = prevSystemApps
	})

	root := t.TempDir()
	home := filepath.Join(root, "home")
	applicationsDir := filepath.Join(home, "Applications")
	currentApp := filepath.Join(applicationsDir, codexDesktopMacCurrentAppName)
	writeFakeChatGPTMacApp(t, currentApp, "old")
	codexAppMacSystemAppsDir = filepath.Join(root, "Applications")
	systemApp := filepath.Join(codexAppMacSystemAppsDir, codexDesktopMacCurrentAppName)
	writeFakeChatGPTMacApp(t, systemApp, "system")

	var processChecks int
	codexAppMacProcessRunningFn = func(context.Context) (bool, error) {
		processChecks++
		return false, nil
	}
	codexAppMacInstallURL = func() string { return codexDesktopMacAppleSiliconDownloadURL }
	codexAppDownloadPackageFn = func(_ context.Context, opts codexAppDownloadOptions) error {
		return os.WriteFile(opts.Path, []byte("fake dmg"), 0o600)
	}
	codexAppRunCommand = func(_ context.Context, _ io.Writer, name string, args ...string) error {
		switch name {
		case "hdiutil":
			if len(args) > 0 && args[0] == "attach" {
				writeFakeChatGPTMacApp(t, filepath.Join(commandArgAfter(args, "-mountpoint"), codexDesktopMacCurrentAppName), "mounted")
			}
			return nil
		case "ditto":
			writeFakeChatGPTMacApp(t, args[1], "new")
			return nil
		case "codesign", "spctl", "xattr":
			return nil
		default:
			t.Fatalf("unexpected command %s %v", name, args)
			return nil
		}
	}
	codexAppCommandOutput = func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch name {
		case "codesign":
			return []byte("Identifier=" + codexDesktopMacBundleIdentifier + "\nTeamIdentifier=" + codexDesktopMacOpenAITeamID + "\n"), nil
		case "/usr/libexec/PlistBuddy":
			if len(args) == 0 {
				return nil, errors.New("missing plist path")
			}
			if strings.Contains(args[len(args)-1], ".codex-desktop-install-") {
				return []byte("1.2.0"), nil
			}
			return []byte("1.1.9"), nil
		default:
			return nil, errors.New("unexpected command output request: " + name)
		}
	}

	state, changed, err := upgradeCodexDesktopAppMac(context.Background(), codexDesktopAppOptions{InstallHome: home, Log: io.Discard})
	if err != nil {
		t.Fatalf("upgrade macOS app: %v", err)
	}
	if !changed {
		t.Fatal("upgrade reported no change")
	}
	if state.AppPath != currentApp || state.Version != "1.2.0" {
		t.Fatalf("upgrade state = %#v, want path %q and version 1.2.0", state, currentApp)
	}
	if processChecks < 2 {
		t.Fatalf("process checks = %d, want pre-download and pre-replacement checks", processChecks)
	}
	for _, tc := range []struct {
		path string
		want string
	}{
		{path: filepath.Join(currentApp, "Contents", "MacOS", codexDesktopMacCurrentExecutableName), want: "new"},
		{path: filepath.Join(systemApp, "Contents", "MacOS", codexDesktopMacCurrentExecutableName), want: "system"},
	} {
		data, err := os.ReadFile(tc.path)
		if err != nil {
			t.Fatalf("read %s: %v", tc.path, err)
		}
		if string(data) != tc.want {
			t.Fatalf("%s = %q, want %q", tc.path, data, tc.want)
		}
	}
}

func TestInstallCodexDesktopAppMacDoesNotDowngradeOrReplaceEqualVersion(t *testing.T) {
	for _, tc := range []struct {
		name        string
		current     string
		downloaded  string
		wantErr     string
		wantChanged bool
		wantContent string
	}{
		{name: "downgrade refused", current: "2.0.0", downloaded: "1.9.9", wantErr: "refusing to downgrade", wantContent: "old"},
		{name: "same version left untouched", current: "2.0.0", downloaded: "2.0.0", wantContent: "old"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lockCLITestHooks(t)
			home := t.TempDir()
			currentApp := filepath.Join(home, "Applications", codexDesktopMacCurrentAppName)
			writeFakeChatGPTMacApp(t, currentApp, "old")
			stubCodexDesktopMacReplacement(t, home, tc.current, tc.downloaded)

			got, err := installCodexDesktopAppMac(context.Background(), codexDesktopAppOptions{InstallHome: home, Log: io.Discard}, home, codexDesktopMacAppleSiliconDownloadURL)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("install error = %v, want %q", err, tc.wantErr)
				}
			} else {
				if err != nil {
					t.Fatalf("install app: %v", err)
				}
				if got != currentApp {
					t.Fatalf("app path = %q, want %q", got, currentApp)
				}
			}
			data, err := os.ReadFile(filepath.Join(currentApp, "Contents", "MacOS", codexDesktopMacCurrentExecutableName))
			if err != nil {
				t.Fatalf("read current app: %v", err)
			}
			if string(data) != tc.wantContent {
				t.Fatalf("current app content = %q, want %q", data, tc.wantContent)
			}
			if matches, err := filepath.Glob(filepath.Join(home, "Applications", ".ChatGPT.app.backup-*")); err != nil || len(matches) != 0 {
				t.Fatalf("backup leftovers = %v, err = %v", matches, err)
			}
		})
	}
}

func TestInstallCodexDesktopAppMacRestoresPreviousBundleAfterFinalVerificationFailure(t *testing.T) {
	lockCLITestHooks(t)
	home := t.TempDir()
	currentApp := filepath.Join(home, "Applications", codexDesktopMacCurrentAppName)
	writeFakeChatGPTMacApp(t, currentApp, "old")
	stubCodexDesktopMacReplacement(t, home, "1.0.0", "2.0.0")

	originalRunCommand := codexAppRunCommand
	verificationCount := 0
	codexAppRunCommand = func(ctx context.Context, log io.Writer, name string, args ...string) error {
		if name == "codesign" && len(args) > 0 && args[0] == "--verify" {
			verificationCount++
			if verificationCount == 3 {
				return errors.New("post-replacement signature failure")
			}
		}
		return originalRunCommand(ctx, log, name, args...)
	}

	_, err := installCodexDesktopAppMac(context.Background(), codexDesktopAppOptions{InstallHome: home, Log: io.Discard}, home, codexDesktopMacAppleSiliconDownloadURL)
	if err == nil || !strings.Contains(err.Error(), "verify installed ChatGPT app after replacement") {
		t.Fatalf("install error = %v, want post-replacement verification failure", err)
	}
	data, err := os.ReadFile(filepath.Join(currentApp, "Contents", "MacOS", codexDesktopMacCurrentExecutableName))
	if err != nil {
		t.Fatalf("read restored app: %v", err)
	}
	if string(data) != "old" {
		t.Fatalf("app after rollback = %q, want old", data)
	}
}

func stubCodexDesktopMacReplacement(t *testing.T, home string, currentVersion, downloadedVersion string) {
	t.Helper()
	prevRunCommand := codexAppRunCommand
	prevCommandOutput := codexAppCommandOutput
	prevDownload := codexAppDownloadPackageFn
	t.Cleanup(func() {
		codexAppRunCommand = prevRunCommand
		codexAppCommandOutput = prevCommandOutput
		codexAppDownloadPackageFn = prevDownload
	})
	codexAppDownloadPackageFn = func(_ context.Context, opts codexAppDownloadOptions) error {
		return os.WriteFile(opts.Path, []byte("fake dmg"), 0o600)
	}
	codexAppRunCommand = func(_ context.Context, _ io.Writer, name string, args ...string) error {
		switch name {
		case "hdiutil":
			if len(args) > 0 && args[0] == "attach" {
				writeFakeChatGPTMacApp(t, filepath.Join(commandArgAfter(args, "-mountpoint"), codexDesktopMacCurrentAppName), "mounted")
			}
			return nil
		case "ditto":
			writeFakeChatGPTMacApp(t, args[1], "new")
			return nil
		case "codesign", "spctl", "xattr":
			return nil
		default:
			t.Fatalf("unexpected command %s %v", name, args)
			return nil
		}
	}
	codexAppCommandOutput = func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch name {
		case "codesign":
			return []byte("Identifier=" + codexDesktopMacBundleIdentifier + "\nTeamIdentifier=" + codexDesktopMacOpenAITeamID + "\n"), nil
		case "/usr/libexec/PlistBuddy":
			if strings.Contains(args[len(args)-1], ".codex-desktop-install-") {
				return []byte(downloadedVersion), nil
			}
			return []byte(currentVersion), nil
		default:
			return nil, errors.New("unexpected command output request: " + name)
		}
	}
}
