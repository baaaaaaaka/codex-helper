package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestParseCodexAppArgs(t *testing.T) {
	cases := []struct {
		name        string
		args        []string
		profileFlag string
		wantProfile string
		wantErr     string
	}{
		{name: "no args", wantProfile: ""},
		{name: "positional profile", args: []string{"dev"}, wantProfile: "dev"},
		{name: "profile flag", profileFlag: "dev", wantProfile: "dev"},
		{name: "multiple positional profiles", args: []string{"dev", "extra"}, wantErr: "unexpected args"},
		{name: "profile twice", args: []string{"dev"}, profileFlag: "other", wantErr: "profile specified twice"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			if err := cmd.Flags().Parse(tc.args); err != nil {
				t.Fatalf("parse flags: %v", err)
			}
			gotProfile, err := parseCodexAppArgs(cmd.Flags().Args(), tc.profileFlag)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseCodexAppArgs error: %v", err)
			}
			if gotProfile != tc.wantProfile {
				t.Fatalf("profile = %q, want %q", gotProfile, tc.wantProfile)
			}
		})
	}
}

func TestCodexDesktopPlatformForCurrentHost(t *testing.T) {
	lockCLITestHooks(t)

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
	})

	cases := []struct {
		name    string
		goos    string
		wsl     bool
		want    codexDesktopPlatform
		wantErr bool
	}{
		{name: "macos", goos: "darwin", want: codexDesktopPlatformMac},
		{name: "windows", goos: "windows", want: codexDesktopPlatformWindows},
		{name: "wsl", goos: "linux", wsl: true, want: codexDesktopPlatformWindows},
		{name: "linux", goos: "linux", wantErr: true},
		{name: "freebsd", goos: "freebsd", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			codexAppGOOS = func() string { return tc.goos }
			codexAppIsWSL = func() bool { return tc.wsl }
			got, err := codexDesktopPlatformForCurrentHost()
			if tc.wantErr {
				if err == nil || !errors.Is(err, errCodexDesktopAppUnsupported) {
					t.Fatalf("error = %v, want unsupported desktop app error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("codexDesktopPlatformForCurrentHost error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("platform = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCodexDesktopMacDownloadURLForArch(t *testing.T) {
	cases := []struct {
		arch string
		want string
	}{
		{arch: "arm64", want: codexDesktopMacAppleSiliconDownloadURL},
		{arch: "amd64", want: codexDesktopMacIntelDownloadURL},
		{arch: "x86_64", want: codexDesktopMacIntelDownloadURL},
		{arch: "unknown", want: codexDesktopMacAppleSiliconDownloadURL},
	}
	for _, tc := range cases {
		t.Run(tc.arch, func(t *testing.T) {
			if got := codexDesktopMacDownloadURLForArch(tc.arch); got != tc.want {
				t.Fatalf("download URL = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRunCodexAppLinuxFailsBeforeProxyPrompt(t *testing.T) {
	lockCLITestHooks(t)

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevLaunch := codexAppLaunchDesktopFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
		ensureProxyPreferenceFunc = prevEnsureProxy
		codexAppLaunchDesktopFn = prevLaunch
	})

	codexAppGOOS = func() string { return "linux" }
	codexAppIsWSL = func() bool { return false }
	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		t.Fatal("unsupported Linux desktop app must fail before proxy prompting")
		return false, config.Config{}, nil
	}
	codexAppLaunchDesktopFn = func(context.Context, codexDesktopAppOptions) error {
		t.Fatal("unsupported Linux desktop app must not launch")
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	err := runCodexApp(cmd, &rootOptions{configPath: filepath.Join(t.TempDir(), "config.json")}, codexAppOptions{cwd: t.TempDir()})
	if err == nil || !errors.Is(err, errCodexDesktopAppUnsupported) {
		t.Fatalf("runCodexApp error = %v, want unsupported desktop app error", err)
	}
}

func TestRunCodexAppDirectLaunchesDesktopApp(t *testing.T) {
	lockCLITestHooks(t)

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false)}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevEnsureProfile := ensureProfileFunc
	prevEnsureProxyURL := codexAppEnsureProxyURLFn
	prevLaunch := codexAppLaunchDesktopFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
		ensureProxyPreferenceFunc = prevEnsureProxy
		ensureProfileFunc = prevEnsureProfile
		codexAppEnsureProxyURLFn = prevEnsureProxyURL
		codexAppLaunchDesktopFn = prevLaunch
	})

	codexAppGOOS = func() string { return "darwin" }
	codexAppIsWSL = func() bool { return false }
	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false)}, nil
	}
	ensureProfileFunc = func(context.Context, *config.Store, string, bool, io.Writer) (config.Profile, config.Config, error) {
		t.Fatal("direct desktop launch should not configure a proxy profile")
		return config.Profile{}, config.Config{}, nil
	}
	codexAppEnsureProxyURLFn = func(context.Context, *config.Store, config.Profile, []config.Instance, io.Writer) (string, error) {
		t.Fatal("direct desktop launch should not start a proxy daemon")
		return "", nil
	}

	var got codexDesktopAppOptions
	codexAppLaunchDesktopFn = func(_ context.Context, opts codexDesktopAppOptions) error {
		got = opts
		return nil
	}

	cwd := t.TempDir()
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	err = runCodexApp(cmd, &rootOptions{configPath: cfgPath}, codexAppOptions{
		cwd:      cwd,
		codexDir: "codex-home",
		appPath:  "/Applications/Codex.app",
	})
	if err != nil {
		t.Fatalf("runCodexApp error: %v", err)
	}
	if got.Platform != codexDesktopPlatformMac {
		t.Fatalf("platform = %q, want macos", got.Platform)
	}
	if got.ProxyURL != "" {
		t.Fatalf("proxy URL = %q, want empty", got.ProxyURL)
	}
	if got.Cwd != cwd {
		t.Fatalf("cwd = %q, want %q", got.Cwd, cwd)
	}
	if got.AppPath != "/Applications/Codex.app" {
		t.Fatalf("app path = %q", got.AppPath)
	}
	if got := envValue(got.ExtraEnv, envCodexHome); got != filepath.Join(cwd, "codex-home") {
		t.Fatalf("CODEX_HOME = %q", got)
	}
}

func TestRunCodexAppProxyLaunchUsesLongLivedProxy(t *testing.T) {
	lockCLITestHooks(t)

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevEnsureProfile := ensureProfileFunc
	prevPersist := persistProxyPreferenceFunc
	prevEnsureProxyURL := codexAppEnsureProxyURLFn
	prevLaunch := codexAppLaunchDesktopFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
		ensureProxyPreferenceFunc = prevEnsureProxy
		ensureProfileFunc = prevEnsureProfile
		persistProxyPreferenceFunc = prevPersist
		codexAppEnsureProxyURLFn = prevEnsureProxyURL
		codexAppLaunchDesktopFn = prevLaunch
	})

	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	profile := config.Profile{ID: "p1", Name: "dev"}
	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return true, config.Config{Version: config.CurrentVersion}, nil
	}
	ensureProfileFunc = func(_ context.Context, _ *config.Store, profileRef string, autoInit bool, _ io.Writer) (config.Profile, config.Config, error) {
		if profileRef != "dev" {
			t.Fatalf("profileRef = %q, want dev", profileRef)
		}
		if !autoInit {
			t.Fatal("desktop app launch should auto-init missing proxy profiles")
		}
		return profile, config.Config{
			Version:   config.CurrentVersion,
			Profiles:  []config.Profile{profile},
			Instances: []config.Instance{{ID: "inst-1", ProfileID: profile.ID}},
		}, nil
	}
	persistCalls := 0
	persistProxyPreferenceFunc = func(s *config.Store, enabled bool) error {
		persistCalls++
		if !enabled {
			t.Fatal("expected proxy preference to persist true after profile setup")
		}
		return persistProxyPreference(s, enabled)
	}
	var gotInstances []config.Instance
	codexAppEnsureProxyURLFn = func(_ context.Context, _ *config.Store, gotProfile config.Profile, instances []config.Instance, _ io.Writer) (string, error) {
		if gotProfile.ID != profile.ID {
			t.Fatalf("profile = %#v, want %#v", gotProfile, profile)
		}
		gotInstances = append([]config.Instance(nil), instances...)
		return "http://127.0.0.1:23123", nil
	}
	var got codexDesktopAppOptions
	codexAppLaunchDesktopFn = func(_ context.Context, opts codexDesktopAppOptions) error {
		got = opts
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cwd := t.TempDir()
	err = runCodexApp(cmd, &rootOptions{configPath: cfgPath}, codexAppOptions{
		profileRef: "dev",
		cwd:        cwd,
	})
	if err != nil {
		t.Fatalf("runCodexApp error: %v", err)
	}
	if persistCalls != 1 {
		t.Fatalf("persist calls = %d, want 1", persistCalls)
	}
	if len(gotInstances) != 1 || gotInstances[0].ID != "inst-1" {
		t.Fatalf("instances = %#v", gotInstances)
	}
	if got.Platform != codexDesktopPlatformWindows {
		t.Fatalf("platform = %q, want windows", got.Platform)
	}
	if got.ProxyURL != "http://127.0.0.1:23123" {
		t.Fatalf("proxy URL = %q", got.ProxyURL)
	}
}

func TestRunCodexAppExplicitProfilePreservesDisabledPreference(t *testing.T) {
	lockCLITestHooks(t)

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false)}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevEnsureProfile := ensureProfileFunc
	prevPersist := persistProxyPreferenceFunc
	prevEnsureProxyURL := codexAppEnsureProxyURLFn
	prevLaunch := codexAppLaunchDesktopFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
		ensureProxyPreferenceFunc = prevEnsureProxy
		ensureProfileFunc = prevEnsureProfile
		persistProxyPreferenceFunc = prevPersist
		codexAppEnsureProxyURLFn = prevEnsureProxyURL
		codexAppLaunchDesktopFn = prevLaunch
	})

	codexAppGOOS = func() string { return "darwin" }
	codexAppIsWSL = func() bool { return false }
	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		t.Fatal("explicit profile should bypass saved direct preference")
		return false, config.Config{}, nil
	}
	profile := config.Profile{ID: "p1", Name: "dev"}
	ensureProfileFunc = func(context.Context, *config.Store, string, bool, io.Writer) (config.Profile, config.Config, error) {
		return profile, config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false), Profiles: []config.Profile{profile}}, nil
	}
	persistProxyPreferenceFunc = func(*config.Store, bool) error {
		t.Fatal("explicit profile should not change an existing saved direct preference")
		return nil
	}
	codexAppEnsureProxyURLFn = func(context.Context, *config.Store, config.Profile, []config.Instance, io.Writer) (string, error) {
		return "http://127.0.0.1:25000", nil
	}
	codexAppLaunchDesktopFn = func(_ context.Context, opts codexDesktopAppOptions) error {
		if opts.ProxyURL == "" {
			t.Fatal("explicit profile should force proxy launch")
		}
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	if err := runCodexApp(cmd, &rootOptions{configPath: cfgPath}, codexAppOptions{profileRef: "dev", cwd: t.TempDir()}); err != nil {
		t.Fatalf("runCodexApp error: %v", err)
	}
}

func TestRunCodexAppDoesNotPersistProxyPreferenceWhenProfileSetupFails(t *testing.T) {
	lockCLITestHooks(t)

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevEnsureProfile := ensureProfileFunc
	prevPersist := persistProxyPreferenceFunc
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
		ensureProxyPreferenceFunc = prevEnsureProxy
		ensureProfileFunc = prevEnsureProfile
		persistProxyPreferenceFunc = prevPersist
	})

	codexAppGOOS = func() string { return "windows" }
	codexAppIsWSL = func() bool { return false }
	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return true, config.Config{Version: config.CurrentVersion}, nil
	}
	profileErr := errors.New("profile setup failed")
	ensureProfileFunc = func(context.Context, *config.Store, string, bool, io.Writer) (config.Profile, config.Config, error) {
		return config.Profile{}, config.Config{}, profileErr
	}
	persistProxyPreferenceFunc = func(*config.Store, bool) error {
		t.Fatal("proxy preference should not be persisted when profile setup fails")
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	err = runCodexApp(cmd, &rootOptions{configPath: cfgPath}, codexAppOptions{cwd: t.TempDir()})
	if !errors.Is(err, profileErr) {
		t.Fatalf("error = %v, want %v", err, profileErr)
	}
}

func TestCodexDesktopAppLaunchOptionsConvertsWSLPaths(t *testing.T) {
	lockCLITestHooks(t)

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	prevWSLPath := codexAppWSLPathFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
		codexAppWSLPathFn = prevWSLPath
	})

	codexAppGOOS = func() string { return "linux" }
	codexAppIsWSL = func() bool { return true }
	codexAppWSLPathFn = func(path string) (string, error) {
		return `\\wsl.localhost\Ubuntu` + strings.ReplaceAll(path, "/", `\`), nil
	}

	cwd := t.TempDir()
	got, err := codexDesktopAppLaunchOptions(&rootOptions{configPath: filepath.Join(t.TempDir(), "config.json")}, "codex-home", cwd, codexDesktopPlatformWindows, "/mnt/c/Users/Alice/Codex.exe", io.Discard)
	if err != nil {
		t.Fatalf("codexDesktopAppLaunchOptions error: %v", err)
	}
	if !strings.HasPrefix(got.Cwd, `\\wsl.localhost\Ubuntu`) {
		t.Fatalf("cwd was not converted for Windows launch: %q", got.Cwd)
	}
	if home := envValue(got.ExtraEnv, envCodexHome); !strings.HasPrefix(home, `\\wsl.localhost\Ubuntu`) {
		t.Fatalf("CODEX_HOME was not converted for Windows launch: %q", home)
	}
	if !strings.HasPrefix(got.AppPath, `\\wsl.localhost\Ubuntu`) {
		t.Fatalf("app path was not converted for Windows launch: %q", got.AppPath)
	}
}

func TestPrintCodexDesktopAppLaunchAdvisoriesWarnsForWSLProxy(t *testing.T) {
	lockCLITestHooks(t)

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
	})

	codexAppGOOS = func() string { return "linux" }
	codexAppIsWSL = func() bool { return true }

	var log bytes.Buffer
	printCodexDesktopAppLaunchAdvisories(codexDesktopAppOptions{
		Platform: codexDesktopPlatformWindows,
		ProxyURL: "http://127.0.0.1:23123",
		Log:      &log,
	})

	for _, want := range []string{
		"proxy environment variables",
		"Windows Codex desktop app from WSL",
		"Windows must be able to access the converted working directory and CODEX_HOME paths",
		"Windows-reachable address",
	} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("desktop app advisory missing %q:\n%s", want, log.String())
		}
	}
}

func TestCodexDesktopWindowsScriptInstallsStorePackageAndLaunchesExe(t *testing.T) {
	script := codexDesktopWindowsInstallAndLaunchScript(codexDesktopAppOptions{
		Cwd:      `C:\work`,
		ExtraEnv: []string{envCodexHome + `=C:\Users\Alice\.codex`},
		ProxyURL: "http://127.0.0.1:23123",
	})

	for _, want := range []string{
		"Get-AppxPackage -Name $packageName",
		"Get-CodexWinget",
		"winget was not found",
		codexDesktopWindowsStoreID,
		"--source msstore",
		"Microsoft Store/winget may be blocked by enterprise policy",
		"OpenAI.Codex",
		"app\\Codex.exe",
		"Start-CodexDesktopProcess $exe",
		"falling back to AppX activation",
		"CODEX_HOME/proxy environment may not be inherited",
		"pass --app-path to the installed Codex.exe",
		"Current Windows session is non-interactive",
		"Start-Process -FilePath ('shell:AppsFolder\\' + $aumid) -WorkingDirectory $cwd",
		"$env:CODEX_HOME = 'C:\\Users\\Alice\\.codex'",
		"$env:HTTP_PROXY = 'http://127.0.0.1:23123'",
		"$env:ALL_PROXY = 'http://127.0.0.1:23123'",
		"$env:WSS_PROXY = 'http://127.0.0.1:23123'",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("Windows desktop app script missing %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, "codex app") {
		t.Fatalf("Windows desktop app script must not launch Codex CLI app subcommand:\n%s", script)
	}
	if strings.Contains(script, "ArgumentList") {
		t.Fatalf("Windows desktop app script should not pass unsupported desktop app args:\n%s", script)
	}
}

func TestLaunchCodexDesktopAppWindowsRequiresPowerShell(t *testing.T) {
	lockCLITestHooks(t)

	prevPowerShell := teamsServicePowerShellExecutable
	prevLookPath := codexAppLookPath
	prevRunCommand := codexAppRunCommand
	t.Cleanup(func() {
		teamsServicePowerShellExecutable = prevPowerShell
		codexAppLookPath = prevLookPath
		codexAppRunCommand = prevRunCommand
	})

	teamsServicePowerShellExecutable = func() string { return "missing-powershell.exe" }
	codexAppLookPath = func(string) (string, error) { return "", exec.ErrNotFound }
	codexAppRunCommand = func(context.Context, io.Writer, string, ...string) error {
		t.Fatal("Windows launch should fail before invoking PowerShell when PowerShell is missing")
		return nil
	}

	err := launchCodexDesktopAppWindows(context.Background(), codexDesktopAppOptions{})
	if err == nil || !strings.Contains(err.Error(), "PowerShell is required") || !strings.Contains(err.Error(), "missing-powershell.exe") {
		t.Fatalf("launch error = %v, want PowerShell requirement", err)
	}
}

func TestFindCodexAppBundle(t *testing.T) {
	root := t.TempDir()
	app := filepath.Join(root, "nested", codexDesktopMacAppName)
	if err := os.MkdirAll(app, 0o755); err != nil {
		t.Fatalf("mkdir app bundle: %v", err)
	}
	got, err := findCodexAppBundle(root)
	if err != nil {
		t.Fatalf("findCodexAppBundle error: %v", err)
	}
	if got != app {
		t.Fatalf("bundle = %q, want %q", got, app)
	}
}

func TestEnsureCodexDesktopAppMacSkipsBrokenCandidate(t *testing.T) {
	lockCLITestHooks(t)
	stubCodexAppMacOpenAIIdentity(t)

	prevSystemApps := codexAppMacSystemAppsDir
	prevRunCommand := codexAppRunCommand
	t.Cleanup(func() {
		codexAppMacSystemAppsDir = prevSystemApps
		codexAppRunCommand = prevRunCommand
	})

	root := t.TempDir()
	codexAppMacSystemAppsDir = filepath.Join(root, "Applications")
	brokenSystemApp := filepath.Join(codexAppMacSystemAppsDir, codexDesktopMacAppName)
	if err := os.MkdirAll(brokenSystemApp, 0o755); err != nil {
		t.Fatalf("mkdir broken app: %v", err)
	}
	home := filepath.Join(root, "home")
	userApp := filepath.Join(home, "Applications", codexDesktopMacAppName)
	writeFakeCodexMacApp(t, userApp, "user")

	var verified []string
	codexAppRunCommand = func(_ context.Context, _ io.Writer, name string, _ ...string) error {
		verified = append(verified, name)
		return nil
	}

	got, err := ensureCodexDesktopAppMac(context.Background(), codexDesktopAppOptions{InstallHome: home, Log: io.Discard})
	if err != nil {
		t.Fatalf("ensureCodexDesktopAppMac error: %v", err)
	}
	if got != userApp {
		t.Fatalf("app = %q, want %q", got, userApp)
	}
	if strings.Join(verified, ",") != "codesign,spctl" {
		t.Fatalf("verification calls = %v, want codesign and spctl", verified)
	}
}

func TestEnsureCodexDesktopAppMacSkipsUntrustedCandidate(t *testing.T) {
	lockCLITestHooks(t)
	stubCodexAppMacOpenAIIdentity(t)

	prevSystemApps := codexAppMacSystemAppsDir
	prevRunCommand := codexAppRunCommand
	t.Cleanup(func() {
		codexAppMacSystemAppsDir = prevSystemApps
		codexAppRunCommand = prevRunCommand
	})

	root := t.TempDir()
	codexAppMacSystemAppsDir = filepath.Join(root, "Applications")
	systemApp := filepath.Join(codexAppMacSystemAppsDir, codexDesktopMacAppName)
	writeFakeCodexMacApp(t, systemApp, "system")
	home := filepath.Join(root, "home")
	userApp := filepath.Join(home, "Applications", codexDesktopMacAppName)
	writeFakeCodexMacApp(t, userApp, "user")

	codexAppRunCommand = func(_ context.Context, _ io.Writer, _ string, args ...string) error {
		if len(args) > 0 && args[len(args)-1] == systemApp {
			return errors.New("blocked by Gatekeeper")
		}
		return nil
	}

	var log bytes.Buffer
	got, err := ensureCodexDesktopAppMac(context.Background(), codexDesktopAppOptions{InstallHome: home, Log: &log})
	if err != nil {
		t.Fatalf("ensureCodexDesktopAppMac error: %v", err)
	}
	if got != userApp {
		t.Fatalf("app = %q, want %q", got, userApp)
	}
	for _, want := range []string{"ignoring existing Codex desktop app", "macOS security verification failed", "blocked by Gatekeeper"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log missing %q:\n%s", want, log.String())
		}
	}
}

func TestEnsureCodexDesktopAppMacExplicitUntrustedAppFails(t *testing.T) {
	lockCLITestHooks(t)

	prevRunCommand := codexAppRunCommand
	t.Cleanup(func() { codexAppRunCommand = prevRunCommand })

	app := filepath.Join(t.TempDir(), codexDesktopMacAppName)
	writeFakeCodexMacApp(t, app, "explicit")
	codexAppRunCommand = func(context.Context, io.Writer, string, ...string) error {
		return errors.New("signature rejected")
	}

	_, err := ensureCodexDesktopAppMac(context.Background(), codexDesktopAppOptions{AppPath: app, Log: io.Discard})
	if err == nil || !strings.Contains(err.Error(), "did not pass macOS security assessment") || !strings.Contains(err.Error(), "refusing to launch") {
		t.Fatalf("ensure error = %v, want explicit security failure", err)
	}
}

func TestVerifyCodexDesktopAppMacRejectsWrongTeamID(t *testing.T) {
	lockCLITestHooks(t)

	prevRunCommand := codexAppRunCommand
	prevCommandOutput := codexAppCommandOutput
	t.Cleanup(func() {
		codexAppRunCommand = prevRunCommand
		codexAppCommandOutput = prevCommandOutput
	})

	app := filepath.Join(t.TempDir(), codexDesktopMacAppName)
	writeFakeCodexMacApp(t, app, "wrong-team")
	codexAppRunCommand = func(context.Context, io.Writer, string, ...string) error {
		return nil
	}
	codexAppCommandOutput = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("TeamIdentifier=NOTOPENAI\n"), nil
	}

	err := verifyCodexDesktopAppMac(context.Background(), app, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "expected OpenAI Team ID") || !strings.Contains(err.Error(), codexDesktopMacOpenAITeamID) {
		t.Fatalf("verify error = %v, want OpenAI Team ID failure", err)
	}
}

func TestCodexDesktopMacInstallHomePrefersEffectiveUser(t *testing.T) {
	aliceHome := filepath.Clean("/Users/alice")
	bobHome := filepath.Clean("/Users/bob")
	got, err := codexDesktopMacInstallHome(codexDesktopAppOptions{
		ExecIdentity: &execIdentity{
			Home: aliceHome,
		},
	})
	if err != nil {
		t.Fatalf("codexDesktopMacInstallHome error: %v", err)
	}
	if got != aliceHome {
		t.Fatalf("install home = %q, want %q", got, aliceHome)
	}
	got, err = codexDesktopMacInstallHome(codexDesktopAppOptions{
		InstallHome: bobHome,
		ExecIdentity: &execIdentity{
			Home: aliceHome,
		},
	})
	if err != nil {
		t.Fatalf("codexDesktopMacInstallHome with explicit home error: %v", err)
	}
	if got != bobHome {
		t.Fatalf("install home = %q, want explicit %q", got, bobHome)
	}
}

func TestInstallCodexDesktopAppMacVerifiesBeforeReplacing(t *testing.T) {
	lockCLITestHooks(t)
	stubCodexAppMacOpenAIIdentity(t)

	prevRunCommand := codexAppRunCommand
	t.Cleanup(func() { codexAppRunCommand = prevRunCommand })

	home := t.TempDir()
	oldApp := filepath.Join(home, "Applications", codexDesktopMacAppName)
	writeFakeCodexMacApp(t, oldApp, "old")

	var calls []string
	codexAppRunCommand = func(_ context.Context, _ io.Writer, name string, args ...string) error {
		calls = append(calls, name+" "+strings.Join(args, " "))
		switch name {
		case "curl":
			out := commandArgAfter(args, "-o")
			if out == "" {
				t.Fatalf("curl missing -o: %v", args)
			}
			return os.WriteFile(out, []byte("dmg"), 0o600)
		case "hdiutil":
			if len(args) > 0 && args[0] == "attach" {
				mount := commandArgAfter(args, "-mountpoint")
				if mount == "" {
					t.Fatalf("hdiutil attach missing mountpoint: %v", args)
				}
				writeFakeCodexMacApp(t, filepath.Join(mount, codexDesktopMacAppName), "mounted")
			}
			return nil
		case "ditto":
			if len(args) != 2 {
				t.Fatalf("ditto args = %v", args)
			}
			writeFakeCodexMacApp(t, args[1], "new")
			return nil
		case "codesign", "spctl", "xattr":
			return nil
		default:
			t.Fatalf("unexpected command %s %v", name, args)
			return nil
		}
	}

	got, err := installCodexDesktopAppMac(context.Background(), codexDesktopAppOptions{InstallHome: home, ProxyURL: "http://127.0.0.1:23123", Log: io.Discard}, home, codexDesktopMacAppleSiliconDownloadURL)
	if err != nil {
		t.Fatalf("installCodexDesktopAppMac error: %v", err)
	}
	if got != oldApp {
		t.Fatalf("installed app = %q, want %q", got, oldApp)
	}
	data, err := os.ReadFile(filepath.Join(oldApp, "Contents", "MacOS", "Codex"))
	if err != nil {
		t.Fatalf("read installed executable: %v", err)
	}
	if string(data) != "new" {
		t.Fatalf("installed executable = %q, want new", data)
	}
	if !commandContains(calls, "curl --proxy http://127.0.0.1:23123") {
		t.Fatalf("curl did not use selected proxy:\n%s", strings.Join(calls, "\n"))
	}
	assertCommandBefore(t, calls, "codesign ", "spctl ")
	assertCommandBefore(t, calls, "spctl ", "xattr ")
	if matches, err := filepath.Glob(filepath.Join(home, "Applications", ".Codex.app.backup-*")); err != nil || len(matches) != 0 {
		t.Fatalf("backup leftovers = %v, err = %v", matches, err)
	}
}

func TestInstallCodexDesktopAppMacKeepsExistingBundleWhenCopyFails(t *testing.T) {
	lockCLITestHooks(t)

	prevRunCommand := codexAppRunCommand
	t.Cleanup(func() { codexAppRunCommand = prevRunCommand })

	home := t.TempDir()
	oldApp := filepath.Join(home, "Applications", codexDesktopMacAppName)
	writeFakeCodexMacApp(t, oldApp, "old")
	copyErr := errors.New("copy failed")

	codexAppRunCommand = func(_ context.Context, _ io.Writer, name string, args ...string) error {
		switch name {
		case "curl":
			return os.WriteFile(commandArgAfter(args, "-o"), []byte("dmg"), 0o600)
		case "hdiutil":
			if len(args) > 0 && args[0] == "attach" {
				writeFakeCodexMacApp(t, filepath.Join(commandArgAfter(args, "-mountpoint"), codexDesktopMacAppName), "mounted")
			}
			return nil
		case "ditto":
			return copyErr
		default:
			return nil
		}
	}

	_, err := installCodexDesktopAppMac(context.Background(), codexDesktopAppOptions{InstallHome: home, Log: io.Discard}, home, codexDesktopMacAppleSiliconDownloadURL)
	if !errors.Is(err, copyErr) {
		t.Fatalf("install error = %v, want %v", err, copyErr)
	}
	data, err := os.ReadFile(filepath.Join(oldApp, "Contents", "MacOS", "Codex"))
	if err != nil {
		t.Fatalf("read existing executable: %v", err)
	}
	if string(data) != "old" {
		t.Fatalf("existing app was replaced on copy failure: %q", data)
	}
}

func TestStartCodexAppProxyDaemonCleansPlaceholderOnStartFailure(t *testing.T) {
	lockCLITestHooks(t)

	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatalf("seed config: %v", err)
	}
	helper := filepath.Join(filepath.Dir(store.Path()), "codex-proxy")
	if err := os.WriteFile(helper, []byte("helper"), 0o755); err != nil {
		t.Fatalf("write helper: %v", err)
	}

	prevExecutable := proxyExecutable
	prevCommand := proxyCommand
	t.Cleanup(func() {
		proxyExecutable = prevExecutable
		proxyCommand = prevCommand
	})
	proxyExecutable = func() (string, error) { return helper, nil }
	proxyCommand = func(string, ...string) *exec.Cmd {
		return exec.Command(filepath.Join(filepath.Dir(store.Path()), "missing-helper"))
	}

	_, err := startCodexAppProxyDaemon(context.Background(), store, config.Profile{ID: "p1", Name: "dev"})
	if err == nil {
		t.Fatal("expected proxy daemon start failure")
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Instances) != 0 {
		t.Fatalf("placeholder instance was not cleaned up: %#v", cfg.Instances)
	}
}

func TestEnsureCodexAppProxyURLCleansInstanceWhenReadinessTimesOut(t *testing.T) {
	lockCLITestHooks(t)

	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatalf("seed config: %v", err)
	}
	helper := filepath.Join(filepath.Dir(store.Path()), "codex-proxy")
	if err := os.WriteFile(helper, []byte("helper"), 0o755); err != nil {
		t.Fatalf("write helper: %v", err)
	}

	prevExecutable := proxyExecutable
	prevCommand := proxyCommand
	prevProcessAlive := proxyProcessAlive
	prevFindProcess := proxyFindProcess
	prevTerminate := proxyTerminate
	prevReadyTimeout := codexAppProxyReadyTimeout
	prevPollInterval := codexAppProxyPollInterval
	t.Cleanup(func() {
		proxyExecutable = prevExecutable
		proxyCommand = prevCommand
		proxyProcessAlive = prevProcessAlive
		proxyFindProcess = prevFindProcess
		proxyTerminate = prevTerminate
		codexAppProxyReadyTimeout = prevReadyTimeout
		codexAppProxyPollInterval = prevPollInterval
	})

	proxyExecutable = func() (string, error) { return helper, nil }
	proxyCommand = func(string, ...string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=TestCodexAppProxyDaemonHelperProcess")
		cmd.Env = append(os.Environ(), "CODEX_APP_PROXY_DAEMON_HELPER=1")
		return cmd
	}
	codexAppProxyReadyTimeout = 20 * time.Millisecond
	codexAppProxyPollInterval = time.Millisecond
	var foundPID int
	var terminatedPID int
	proxyProcessAlive = func(pid int) bool { return pid > 0 }
	proxyFindProcess = func(pid int) (*os.Process, error) {
		foundPID = pid
		return &os.Process{Pid: pid}, nil
	}
	proxyTerminate = func(p *os.Process, _ time.Duration) error {
		terminatedPID = p.Pid
		return nil
	}

	_, err := ensureCodexAppProxyURL(context.Background(), store, config.Profile{ID: "p1", Name: "dev"}, nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "did not become ready") {
		t.Fatalf("ensure proxy URL error = %v, want readiness timeout", err)
	}
	if foundPID == 0 || terminatedPID != foundPID {
		t.Fatalf("timed-out proxy daemon was not stopped, foundPID=%d terminatedPID=%d", foundPID, terminatedPID)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Instances) != 0 {
		t.Fatalf("timed-out proxy instance was not cleaned up: %#v", cfg.Instances)
	}
}

func TestCodexAppProxyDaemonHelperProcess(t *testing.T) {
	if os.Getenv("CODEX_APP_PROXY_DAEMON_HELPER") != "1" {
		return
	}
	os.Exit(0)
}

func writeFakeCodexMacApp(t *testing.T, appPath string, contents string) {
	t.Helper()
	exe := filepath.Join(appPath, "Contents", "MacOS", "Codex")
	if err := os.MkdirAll(filepath.Dir(exe), 0o755); err != nil {
		t.Fatalf("mkdir fake app: %v", err)
	}
	if err := os.WriteFile(exe, []byte(contents), 0o755); err != nil {
		t.Fatalf("write fake app executable: %v", err)
	}
}

func stubCodexAppMacOpenAIIdentity(t *testing.T) {
	t.Helper()
	prevCommandOutput := codexAppCommandOutput
	codexAppCommandOutput = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("TeamIdentifier=" + codexDesktopMacOpenAITeamID + "\n"), nil
	}
	t.Cleanup(func() { codexAppCommandOutput = prevCommandOutput })
}

func commandArgAfter(args []string, name string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == name {
			return args[i+1]
		}
	}
	return ""
}

func assertCommandBefore(t *testing.T, calls []string, first string, second string) {
	t.Helper()
	firstIndex := -1
	secondIndex := -1
	for i, call := range calls {
		if firstIndex < 0 && strings.HasPrefix(call, first) {
			firstIndex = i
		}
		if secondIndex < 0 && strings.HasPrefix(call, second) {
			secondIndex = i
		}
	}
	if firstIndex < 0 || secondIndex < 0 || firstIndex >= secondIndex {
		t.Fatalf("command order %q before %q not observed in calls:\n%s", first, second, strings.Join(calls, "\n"))
	}
}

func commandContains(calls []string, want string) bool {
	for _, call := range calls {
		if strings.Contains(call, want) {
			return true
		}
	}
	return false
}
