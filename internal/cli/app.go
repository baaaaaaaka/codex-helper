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
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/env"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/ids"
	"github.com/baaaaaaaka/codex-helper/internal/manager"
)

const (
	codexDesktopMacAppleSiliconDownloadURL = "https://persistent.oaistatic.com/codex-app-prod/Codex.dmg"
	codexDesktopMacIntelDownloadURL        = "https://persistent.oaistatic.com/codex-app-prod/Codex-latest-x64.dmg"
	codexDesktopMacAppName                 = "Codex.app"
	codexDesktopMacOpenAITeamID            = "2DC432GLL2"
	codexDesktopWindowsStoreID             = "9PLM9XGG6VKS"
	codexDesktopWindowsPackageName         = "OpenAI.Codex"
)

var (
	codexAppGOOS                  = func() string { return runtime.GOOS }
	codexAppGOARCH                = func() string { return runtime.GOARCH }
	codexAppIsWSL                 = func() bool { return teamsServiceIsWSL() }
	codexAppLaunchDesktopFn       = launchCodexDesktopApp
	codexAppEnsureProxyURLFn      = ensureCodexAppProxyURL
	codexAppCommandContext        = exec.CommandContext
	codexAppRunCommand            = runCodexAppLoggedCommand
	codexAppCommandOutput         = runCodexAppCommandOutput
	codexAppLookPath              = exec.LookPath
	codexAppUserHomeDir           = os.UserHomeDir
	codexAppMacSystemAppsDir      = "/Applications"
	codexAppWSLPathFn             = defaultCodexAppWSLPath
	codexAppProxyPollInterval     = 200 * time.Millisecond
	codexAppProxyReadyTimeout     = 15 * time.Second
	codexAppMacInstallURL         = func() string { return codexDesktopMacDownloadURLForArch(codexAppGOARCH()) }
	errCodexDesktopAppUnsupported = errors.New("codex desktop app is only available for macOS and Windows")
)

type codexAppOptions struct {
	profileRef string
	codexDir   string
	appPath    string
	cwd        string
}

type codexDesktopAppOptions struct {
	Platform     codexDesktopPlatform
	Cwd          string
	AppPath      string
	InstallHome  string
	ExtraEnv     []string
	ProxyURL     string
	ExecIdentity *execIdentity
	Log          io.Writer
}

type codexDesktopPlatform string

const (
	codexDesktopPlatformMac     codexDesktopPlatform = "macos"
	codexDesktopPlatformWindows codexDesktopPlatform = "windows"
)

func newAppCmd(root *rootOptions) *cobra.Command {
	var codexDir string
	var appPath string
	var cwd string
	var profileFlag string

	cmd := &cobra.Command{
		Use:   "app [profile]",
		Short: "Install and launch the Codex desktop app",
		Long: "Install and launch the Codex desktop app. macOS uses the official OpenAI DMG; Windows uses the Microsoft Store package. " +
			"Linux outside WSL is not supported by the official Codex desktop app.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			profileRef, err := parseCodexAppArgs(args, profileFlag)
			if err != nil {
				return err
			}
			return runCodexApp(cmd, root, codexAppOptions{
				profileRef: profileRef,
				codexDir:   codexDir,
				appPath:    appPath,
				cwd:        cwd,
			})
		},
	}

	cmd.Flags().StringVar(&codexDir, "codex-dir", "", "Override Codex data dir (default: ~/.codex)")
	cmd.Flags().StringVar(&appPath, "app-path", "", "Override Codex desktop app path")
	cmd.Flags().StringVar(&cwd, "cwd", "", "Working directory for Codex app (default: current directory)")
	cmd.Flags().StringVar(&profileFlag, "profile", "", "Proxy profile id or name")
	return cmd
}

func parseCodexAppArgs(args []string, profileFlag string) (string, error) {
	if len(args) > 1 {
		return "", fmt.Errorf("unexpected args (only profile is allowed)")
	}

	profileRef := strings.TrimSpace(profileFlag)
	if len(args) == 1 {
		if profileRef != "" {
			return "", fmt.Errorf("profile specified twice")
		}
		profileRef = args[0]
	}

	return profileRef, nil
}

func runCodexApp(cmd *cobra.Command, root *rootOptions, opts codexAppOptions) error {
	ctx, stop := withSignalContext(cmd.Context())
	defer stop()

	cwd := strings.TrimSpace(opts.cwd)
	if cwd == "" {
		var err error
		cwd, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	cwd, err := normalizeWorkingDir(cwd)
	if err != nil {
		return err
	}

	platform, err := codexDesktopPlatformForCurrentHost()
	if err != nil {
		return err
	}

	store, _, err := newRootStore(root, opts.codexDir)
	if err != nil {
		return err
	}

	useProxy := false
	var cfg config.Config
	if strings.TrimSpace(opts.profileRef) != "" {
		cfg, err = store.Load()
		if err != nil {
			return err
		}
		useProxy = true
	} else {
		useProxy, cfg, err = ensureProxyPreferenceFunc(ctx, store, opts.profileRef, cmd.ErrOrStderr())
		if err != nil {
			return err
		}
	}

	var profile *config.Profile
	if useProxy {
		p, cfgWithProfile, err := ensureProfileFunc(ctx, store, opts.profileRef, true, cmd.OutOrStdout())
		if err != nil {
			return err
		}
		cfg = cfgWithProfile
		if cfg.ProxyEnabled == nil {
			enabled := true
			if err := persistProxyPreferenceFunc(store, enabled); err != nil {
				return err
			}
			cfg.ProxyEnabled = &enabled
		}
		profile = &p
	}

	launchOpts, err := codexDesktopAppLaunchOptions(root, opts.codexDir, cwd, platform, opts.appPath, cmd.ErrOrStderr())
	if err != nil {
		return err
	}

	if useProxy {
		proxyURL, err := codexAppEnsureProxyURLFn(ctx, store, *profile, cfg.Instances, cmd.ErrOrStderr())
		if err != nil {
			return err
		}
		launchOpts.ProxyURL = proxyURL
	}

	return codexAppLaunchDesktopFn(ctx, launchOpts)
}

func codexDesktopPlatformForCurrentHost() (codexDesktopPlatform, error) {
	switch codexAppGOOS() {
	case "darwin":
		return codexDesktopPlatformMac, nil
	case "windows":
		return codexDesktopPlatformWindows, nil
	case "linux":
		if codexAppIsWSL() {
			return codexDesktopPlatformWindows, nil
		}
	}
	return "", fmt.Errorf("%w; current platform: %s", errCodexDesktopAppUnsupported, codexAppGOOS())
}

func codexDesktopMacDownloadURLForArch(arch string) string {
	switch strings.TrimSpace(arch) {
	case "amd64", "x86_64":
		return codexDesktopMacIntelDownloadURL
	default:
		return codexDesktopMacAppleSiliconDownloadURL
	}
}

func codexDesktopAppLaunchOptions(
	root *rootOptions,
	codexDir string,
	cwd string,
	platform codexDesktopPlatform,
	appPath string,
	log io.Writer,
) (codexDesktopAppOptions, error) {
	configPathOverride := ""
	if root != nil {
		configPathOverride = root.configPath
	}
	paths, err := resolveEffectiveLaunchPaths(configPathOverride, codexDir, cwd)
	if err != nil {
		return codexDesktopAppOptions{}, err
	}
	codexHome, err := resolveCodexHomePath(paths.CodexDir, cwd)
	if err != nil {
		return codexDesktopAppOptions{}, err
	}
	launchCwd := cwd
	launchAppPath := strings.TrimSpace(appPath)
	if platform == codexDesktopPlatformWindows && codexAppGOOS() == "linux" && codexAppIsWSL() {
		if converted, err := codexAppWSLPathFn(cwd); err == nil && strings.TrimSpace(converted) != "" {
			launchCwd = converted
		} else if err != nil {
			return codexDesktopAppOptions{}, fmt.Errorf("convert working directory for Windows Codex desktop app: %w", err)
		}
		if converted, err := codexAppWSLPathFn(codexHome); err == nil && strings.TrimSpace(converted) != "" {
			codexHome = converted
		} else if err != nil {
			return codexDesktopAppOptions{}, fmt.Errorf("convert Codex home for Windows Codex desktop app: %w", err)
		}
		if launchAppPath != "" {
			if converted, err := codexAppWSLPathFn(launchAppPath); err == nil && strings.TrimSpace(converted) != "" {
				launchAppPath = converted
			} else if err != nil {
				return codexDesktopAppOptions{}, fmt.Errorf("convert Codex desktop app path for Windows launch: %w", err)
			}
		}
	}

	return codexDesktopAppOptions{
		Platform:     platform,
		Cwd:          launchCwd,
		AppPath:      launchAppPath,
		InstallHome:  paths.Home,
		ExtraEnv:     codexHomeEnv(codexHome),
		ExecIdentity: paths.ExecIdentity,
		Log:          log,
	}, nil
}

func defaultCodexAppWSLPath(path string) (string, error) {
	out, err := exec.Command("wslpath", "-w", path).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func ensureCodexAppProxyURL(ctx context.Context, store *config.Store, profile config.Profile, instances []config.Instance, log io.Writer) (string, error) {
	hc := manager.HealthClient{Timeout: 1 * time.Second}
	if inst := manager.FindReusableInstance(instances, profile.ID, hc); inst != nil {
		return fmt.Sprintf("http://127.0.0.1:%d", inst.HTTPPort), nil
	}
	if freshCfg, err := store.Load(); err == nil {
		if inst := manager.FindReusableInstance(freshCfg.Instances, profile.ID, hc); inst != nil {
			return fmt.Sprintf("http://127.0.0.1:%d", inst.HTTPPort), nil
		}
	}
	if log != nil {
		_, _ = fmt.Fprintln(log, "starting a long-lived proxy instance for the Codex desktop app...")
	}
	instanceID, err := startCodexAppProxyDaemon(ctx, store, profile)
	if err != nil {
		return "", err
	}
	proxyURL, err := waitForCodexAppProxyURL(ctx, store, profile.ID, instanceID, hc)
	if err != nil {
		cleanupCodexAppProxyStartup(store, instanceID)
		return "", err
	}
	return proxyURL, nil
}

func cleanupCodexAppProxyStartup(store *config.Store, instanceID string) {
	if cfg, err := store.Load(); err == nil {
		for _, inst := range cfg.Instances {
			if inst.ID == instanceID {
				_ = stopProxyInstances([]config.Instance{inst})
				break
			}
		}
	}
	_ = proxyRemoveInstance(store, instanceID)
}

func startCodexAppProxyDaemon(_ context.Context, store *config.Store, profile config.Profile) (string, error) {
	instanceID, err := ids.New()
	if err != nil {
		return "", err
	}

	now := proxyNow()
	inst := config.Instance{
		ID:         instanceID,
		ProfileID:  profile.ID,
		Kind:       config.InstanceKindDaemon,
		HTTPPort:   0,
		SocksPort:  0,
		DaemonPID:  0,
		StartedAt:  now,
		LastSeenAt: now,
	}
	if err := proxyRecordInstance(store, inst); err != nil {
		return "", err
	}
	started := false
	defer func() {
		if !started {
			cleanupCodexAppProxyStartup(store, instanceID)
		}
	}()

	exe, err := proxyExecutable()
	if err != nil {
		return "", err
	}
	resolvedExe, err := helperpath.StableRunnablePathFromSources(exe, restartArgv0(), helperpath.Options{})
	if err != nil {
		return "", err
	}
	exe = resolvedExe.Path

	args := []string{"--config", store.Path(), "proxy", "daemon", "--instance-id", instanceID}
	c := proxyCommand(exe, args...)
	c.Stdin = nil

	logPath := filepath.Join(filepath.Dir(store.Path()), "instances", instanceID+".log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return "", err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	defer logFile.Close()
	c.Stdout = logFile
	c.Stderr = logFile

	if err := c.Start(); err != nil {
		return "", err
	}
	started = true
	pid := c.Process.Pid
	_ = c.Process.Release()
	_ = store.Update(func(cfg *config.Config) error {
		for i := range cfg.Instances {
			if cfg.Instances[i].ID == instanceID {
				cfg.Instances[i].DaemonPID = pid
				cfg.Instances[i].LastSeenAt = proxyNow()
				return nil
			}
		}
		return nil
	})
	return instanceID, nil
}

func waitForCodexAppProxyURL(ctx context.Context, store *config.Store, profileID string, instanceID string, hc manager.HealthClient) (string, error) {
	deadline := time.NewTimer(codexAppProxyReadyTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(codexAppProxyPollInterval)
	defer ticker.Stop()

	for {
		cfg, err := store.Load()
		if err == nil {
			if inst := manager.FindReusableInstance(cfg.Instances, profileID, hc); inst != nil {
				return fmt.Sprintf("http://127.0.0.1:%d", inst.HTTPPort), nil
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline.C:
			return "", fmt.Errorf("proxy instance %s did not become ready within %s", instanceID, codexAppProxyReadyTimeout)
		case <-ticker.C:
		}
	}
}

func launchCodexDesktopApp(ctx context.Context, opts codexDesktopAppOptions) error {
	printCodexDesktopAppLaunchAdvisories(opts)
	switch opts.Platform {
	case codexDesktopPlatformMac:
		return launchCodexDesktopAppMac(ctx, opts)
	case codexDesktopPlatformWindows:
		return launchCodexDesktopAppWindows(ctx, opts)
	default:
		return fmt.Errorf("%w; current platform: %s", errCodexDesktopAppUnsupported, opts.Platform)
	}
}

func printCodexDesktopAppLaunchAdvisories(opts codexDesktopAppOptions) {
	if strings.TrimSpace(opts.ProxyURL) != "" {
		codexAppWarn(opts.Log, "Codex desktop app will receive proxy environment variables. If sign-in or network requests still bypass the proxy, configure the desktop app or system proxy directly.")
	}
	if opts.Platform == codexDesktopPlatformWindows && codexAppGOOS() == "linux" && codexAppIsWSL() {
		codexAppWarn(opts.Log, "launching the Windows Codex desktop app from WSL. Windows must be able to access the converted working directory and CODEX_HOME paths.")
		if strings.TrimSpace(opts.ProxyURL) != "" {
			codexAppWarn(opts.Log, "the selected proxy is running at %s from this WSL environment. If the desktop app cannot connect, expose the proxy on a Windows-reachable address or use a Windows-side proxy.", opts.ProxyURL)
		}
	}
	if opts.Platform == codexDesktopPlatformMac && (strings.TrimSpace(os.Getenv("SSH_CONNECTION")) != "" || strings.TrimSpace(os.Getenv("SSH_TTY")) != "") {
		codexAppWarn(opts.Log, "launching a macOS desktop app from SSH may install successfully but not show a visible window. Run from an interactive macOS desktop session if the app is not visible.")
	}
}

func codexAppWarn(log io.Writer, format string, args ...any) {
	if log == nil {
		return
	}
	_, _ = fmt.Fprintf(log, "warning: "+format+"\n", args...)
}

func launchCodexDesktopAppMac(ctx context.Context, opts codexDesktopAppOptions) error {
	appPath, err := ensureCodexDesktopAppMac(ctx, opts)
	if err != nil {
		return err
	}
	executable, err := codexDesktopMacExecutablePath(appPath)
	if err != nil {
		return err
	}
	return startCodexDesktopProcess(ctx, executable, opts)
}

func ensureCodexDesktopAppMac(ctx context.Context, opts codexDesktopAppOptions) (string, error) {
	if appPath := strings.TrimSpace(opts.AppPath); appPath != "" {
		appPath = filepath.Clean(appPath)
		if err := verifyCodexDesktopAppMac(ctx, appPath, opts.Log); err != nil {
			return "", fmt.Errorf("Codex desktop app at %s did not pass macOS security assessment; refusing to launch an unsigned, unnotarized, or blocked app: %w", appPath, err)
		}
		return appPath, nil
	}

	home, err := codexDesktopMacInstallHome(opts)
	if err != nil {
		return "", err
	}
	for _, candidate := range codexDesktopMacCandidatePaths(home) {
		if _, err := codexDesktopMacExecutablePath(candidate); err != nil {
			continue
		}
		if err := verifyCodexDesktopAppMac(ctx, candidate, opts.Log); err != nil {
			codexAppWarn(opts.Log, "ignoring existing Codex desktop app at %s because macOS security verification failed: %v", candidate, err)
			continue
		}
		return candidate, nil
	}

	installURL := codexAppMacInstallURL()
	if opts.Log != nil {
		_, _ = fmt.Fprintf(opts.Log, "installing Codex desktop app from %s...\n", installURL)
	}
	return installCodexDesktopAppMac(ctx, opts, home, installURL)
}

func codexDesktopMacInstallHome(opts codexDesktopAppOptions) (string, error) {
	if home := strings.TrimSpace(opts.InstallHome); home != "" {
		return filepath.Clean(home), nil
	}
	if opts.ExecIdentity != nil && strings.TrimSpace(opts.ExecIdentity.Home) != "" {
		return filepath.Clean(opts.ExecIdentity.Home), nil
	}
	home, err := codexAppUserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Clean(home), nil
}

func codexDesktopMacCandidatePaths(home string) []string {
	candidates := []string{filepath.Join(codexAppMacSystemAppsDir, codexDesktopMacAppName)}
	if strings.TrimSpace(home) != "" {
		candidates = append(candidates, filepath.Join(home, "Applications", codexDesktopMacAppName))
	}
	return candidates
}

func codexDesktopMacExecutablePath(appPath string) (string, error) {
	appPath = filepath.Clean(strings.TrimSpace(appPath))
	executable := appPath
	if strings.HasSuffix(appPath, ".app") {
		executable = filepath.Join(appPath, "Contents", "MacOS", "Codex")
	}
	if info, err := os.Stat(executable); err != nil || info.IsDir() {
		if err == nil {
			err = fmt.Errorf("%s is a directory", executable)
		}
		return "", fmt.Errorf("Codex desktop app executable not found: %w", err)
	}
	return executable, nil
}

func installCodexDesktopAppMac(ctx context.Context, opts codexDesktopAppOptions, home string, installURL string) (string, error) {
	installDir := filepath.Join(home, "Applications")
	if err := os.MkdirAll(installDir, 0o755); err != nil {
		return "", err
	}
	if err := ensurePathOwnedByIdentity(installDir, opts.ExecIdentity); err != nil {
		return "", fmt.Errorf("set Codex desktop install dir ownership: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "codex-desktop-app-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmpDir)

	dmgPath := filepath.Join(tmpDir, "Codex.dmg")
	curlArgs := []string{"--retry", "5", "--retry-delay", "5", "--connect-timeout", "30", "-fsSL", "-o", dmgPath, installURL}
	if proxyURL := strings.TrimSpace(opts.ProxyURL); proxyURL != "" {
		curlArgs = append([]string{"--proxy", proxyURL}, curlArgs...)
	}
	if err := codexAppRunCommand(ctx, opts.Log, "curl", curlArgs...); err != nil {
		return "", fmt.Errorf("download Codex desktop app DMG; check network, proxy, and TLS inspection settings: %w", err)
	}

	mountPath := filepath.Join(tmpDir, "mount")
	if err := os.MkdirAll(mountPath, 0o755); err != nil {
		return "", err
	}
	if err := codexAppRunCommand(ctx, opts.Log, "hdiutil", "attach", "-nobrowse", "-readonly", "-mountpoint", mountPath, dmgPath); err != nil {
		return "", fmt.Errorf("mount Codex desktop app DMG: %w", err)
	}
	defer func() {
		if err := codexAppRunCommand(context.Background(), io.Discard, "hdiutil", "detach", mountPath); err != nil {
			codexAppWarn(opts.Log, "could not detach Codex desktop app DMG mount at %s: %v", mountPath, err)
		}
	}()

	sourceApp, err := findCodexAppBundle(mountPath)
	if err != nil {
		return "", err
	}

	stagingRoot, err := os.MkdirTemp(installDir, ".codex-desktop-install-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(stagingRoot)
	stagedApp := filepath.Join(stagingRoot, codexDesktopMacAppName)
	if err := codexAppRunCommand(ctx, opts.Log, "ditto", sourceApp, stagedApp); err != nil {
		return "", fmt.Errorf("copy Codex desktop app bundle: %w", err)
	}
	if err := verifyCodexDesktopAppMac(ctx, stagedApp, opts.Log); err != nil {
		return "", err
	}
	_ = codexAppRunCommand(ctx, io.Discard, "xattr", "-dr", "com.apple.quarantine", stagedApp)
	if err := ensureTreeOwnedByIdentity(stagedApp, opts.ExecIdentity); err != nil {
		return "", fmt.Errorf("set Codex desktop app ownership: %w", err)
	}

	destApp := filepath.Join(installDir, codexDesktopMacAppName)
	backupApp := filepath.Join(installDir, fmt.Sprintf(".%s.backup-%d", codexDesktopMacAppName, time.Now().UnixNano()))
	hadExisting := false
	if _, err := os.Stat(destApp); err == nil {
		hadExisting = true
		if err := os.Rename(destApp, backupApp); err != nil {
			return "", fmt.Errorf("stage existing Codex desktop app for replacement: %w", err)
		}
	} else if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("inspect existing Codex desktop app: %w", err)
	}
	if err := os.Rename(stagedApp, destApp); err != nil {
		if hadExisting {
			_ = os.Rename(backupApp, destApp)
		}
		return "", fmt.Errorf("replace Codex desktop app bundle: %w", err)
	}
	if hadExisting {
		_ = os.RemoveAll(backupApp)
	}
	return destApp, nil
}

func verifyCodexDesktopAppMac(ctx context.Context, appPath string, log io.Writer) error {
	if _, err := codexDesktopMacExecutablePath(appPath); err != nil {
		return err
	}
	if err := codexAppRunCommand(ctx, log, "codesign", "--verify", "--strict", "--verbose=2", appPath); err != nil {
		return fmt.Errorf("macOS signature verification failed for Codex desktop app; refusing to install or launch an untrusted app: %w", err)
	}
	if err := codexAppRunCommand(ctx, log, "spctl", "--assess", "--type", "execute", "--verbose=2", appPath); err != nil {
		return fmt.Errorf("macOS Gatekeeper assessment failed for Codex desktop app; it may be unsigned, unnotarized, or blocked by local security policy: %w", err)
	}
	if err := verifyCodexDesktopAppMacOpenAIIdentity(ctx, appPath); err != nil {
		return err
	}
	return nil
}

func verifyCodexDesktopAppMacOpenAIIdentity(ctx context.Context, appPath string) error {
	data, err := codexAppCommandOutput(ctx, "codesign", "--display", "--verbose=4", appPath)
	if err != nil {
		return fmt.Errorf("inspect Codex desktop app signing identity: %w", err)
	}
	if !strings.Contains(string(data), "TeamIdentifier="+codexDesktopMacOpenAITeamID) {
		return fmt.Errorf("Codex desktop app is not signed by the expected OpenAI Team ID %s", codexDesktopMacOpenAITeamID)
	}
	return nil
}

func ensureTreeOwnedByIdentity(root string, identity *execIdentity) error {
	if identity == nil || identity.UID == 0 || strings.TrimSpace(root) == "" {
		return nil
	}
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		return ensurePathOwnedByIdentity(path, identity)
	})
}

func findCodexAppBundle(root string) (string, error) {
	var found string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if found != "" {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() && d.Name() == codexDesktopMacAppName {
			found = path
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if found == "" {
		return "", fmt.Errorf("Codex.app not found in mounted DMG")
	}
	return found, nil
}

func launchCodexDesktopAppWindows(ctx context.Context, opts codexDesktopAppOptions) error {
	script := codexDesktopWindowsInstallAndLaunchScript(opts)
	name := teamsServicePowerShellExecutable()
	if _, err := codexAppLookPath(name); err != nil {
		return fmt.Errorf("PowerShell is required to install and launch the Windows Codex desktop app: %s not found. Run from a Windows session with powershell.exe available, or install PowerShell and retry: %w", name, err)
	}
	args := []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script}
	return codexAppRunCommand(ctx, opts.Log, name, args...)
}

func codexDesktopWindowsInstallAndLaunchScript(opts codexDesktopAppOptions) string {
	envAssignments := codexDesktopWindowsEnvPowerShell(opts)
	cwd := powershellSingleQuote(opts.Cwd)
	appPath := powershellSingleQuote(opts.AppPath)
	packageName := powershellSingleQuote(codexDesktopWindowsPackageName)
	storeID := powershellSingleQuote(codexDesktopWindowsStoreID)
	return strings.Join([]string{
		"$ErrorActionPreference = 'Stop'",
		envAssignments,
		"$appPath = " + appPath,
		"$cwd = " + cwd,
		"$packageName = " + packageName,
		"$storeId = " + storeID,
		"function Get-CodexPackage { Get-AppxPackage -Name $packageName -ErrorAction SilentlyContinue | Sort-Object Version -Descending | Select-Object -First 1 }",
		"function Get-CodexWinget { $cmd = Get-Command winget -ErrorAction SilentlyContinue; if ($null -eq $cmd) { throw 'winget was not found. Install or update App Installer, enable Microsoft Store/winget, or pass --app-path to an existing Codex.exe.' }; return $cmd }",
		"function Warn-NonInteractiveDesktop { if (-not ([Environment]::UserInteractive)) { Write-Warning 'Current Windows session is non-interactive. The Codex desktop app may install successfully but no visible window may appear; run from an interactive Windows desktop session if launch is not visible.' } }",
		"function Start-CodexDesktopProcess([string]$FilePath) { Start-Process -FilePath $FilePath -WorkingDirectory $cwd | Out-Null }",
		"Warn-NonInteractiveDesktop",
		"$pkg = Get-CodexPackage",
		"if ($null -eq $pkg -and [string]::IsNullOrWhiteSpace($appPath)) { $winget = Get-CodexWinget; & $winget.Source install --id $storeId --source msstore --exact --accept-source-agreements --accept-package-agreements --disable-interactivity; if ($LASTEXITCODE -ne 0) { throw ('winget Microsoft Store install failed with exit code ' + $LASTEXITCODE + '. Microsoft Store/winget may be blocked by enterprise policy, unavailable on this Windows edition, or unable to reach the network/proxy.') }; $pkg = Get-CodexPackage }",
		"if (-not [string]::IsNullOrWhiteSpace($appPath)) { if (-not (Test-Path -LiteralPath $appPath)) { throw ('Codex desktop app path not found: ' + $appPath) }; Start-CodexDesktopProcess $appPath; return }",
		"if ($null -eq $pkg) { throw 'OpenAI.Codex package was not found after installation. Microsoft Store/winget may be blocked by policy or source availability.' }",
		"$exe = Join-Path $pkg.InstallLocation 'app\\Codex.exe'",
		"if (Test-Path -LiteralPath $exe) { try { Start-CodexDesktopProcess $exe; return } catch { Write-Warning ('direct Codex.exe launch failed: ' + $_.Exception.Message + '; falling back to AppX activation') } }",
		"$manifest = Get-AppxPackageManifest -Package $pkg.PackageFullName",
		"$app = @($manifest.Package.Applications.Application | Select-Object -First 1)",
		"if ($app.Count -eq 0) { throw 'Codex desktop app manifest does not define an application entry' }",
		"$aumid = $pkg.PackageFamilyName + '!' + $app[0].Id",
		"Write-Warning 'Falling back to AppX activation; CODEX_HOME/proxy environment may not be inherited by the desktop app.'",
		"Start-Process -FilePath ('shell:AppsFolder\\' + $aumid) -WorkingDirectory $cwd | Out-Null",
	}, "; ")
}

func codexDesktopWindowsEnvPowerShell(opts codexDesktopAppOptions) string {
	var parts []string
	for _, item := range opts.ExtraEnv {
		key, value, ok := strings.Cut(item, "=")
		if !ok || strings.TrimSpace(key) == "" {
			continue
		}
		parts = append(parts, "$env:"+key+" = "+powershellSingleQuote(value))
	}
	if proxyURL := strings.TrimSpace(opts.ProxyURL); proxyURL != "" {
		for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy", "WS_PROXY", "WSS_PROXY"} {
			parts = append(parts, "$env:"+key+" = "+powershellSingleQuote(proxyURL))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "; ")
}

func startCodexDesktopProcess(ctx context.Context, executable string, opts codexDesktopAppOptions) error {
	cmd := codexAppCommandContext(ctx, executable)
	if strings.TrimSpace(opts.Cwd) != "" {
		cmd.Dir = opts.Cwd
	}
	envVars := os.Environ()
	if strings.TrimSpace(opts.ProxyURL) != "" {
		envVars = env.WithProxy(envVars, opts.ProxyURL)
		for _, key := range []string{"WS_PROXY", "WSS_PROXY"} {
			envVars = append(envVars, key+"="+opts.ProxyURL)
		}
	}
	envVars = append(envVars, opts.ExtraEnv...)
	updatedEnv, err := applyExecIdentity(cmd, envVars, opts.ExecIdentity)
	if err != nil {
		return err
	}
	cmd.Env = updatedEnv
	configureTeamsServiceDetachedCommand(cmd)
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}

func runCodexAppLoggedCommand(ctx context.Context, log io.Writer, name string, args ...string) error {
	cmd := codexAppCommandContext(ctx, name, args...)
	if log != nil {
		cmd.Stdout = log
		cmd.Stderr = log
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s failed: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

func runCodexAppCommandOutput(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := codexAppCommandContext(ctx, name, args...)
	data, err := cmd.CombinedOutput()
	if err != nil {
		detail := strings.TrimSpace(string(data))
		if detail != "" {
			return data, fmt.Errorf("%s %s failed: %w\n%s", name, strings.Join(args, " "), err, detail)
		}
		return data, fmt.Errorf("%s %s failed: %w", name, strings.Join(args, " "), err)
	}
	return data, nil
}
