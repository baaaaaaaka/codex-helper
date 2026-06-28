package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

const (
	codexAppAuthAppServerStartTimeout = 30 * time.Second
	codexAppAuthDefaultTimeout        = 15 * time.Minute
	codexAppAuthAccountReadyTimeout   = 5 * time.Second
)

var (
	codexAppAuthStarter                 codexrunner.AppServerTransportStarter = codexrunner.AppServerProcessStarter{}
	codexAppOpenAuthURLFn                                                     = openCodexAppAuthURL
	codexAppAuthBrowserRunIDFn                                                = defaultCodexAppAuthBrowserRunID
	codexAppAuthWindowsProxyReachableFn                                       = codexAppAuthWindowsProxyReachable
	codexAppAuthAccountPollInterval                                           = 200 * time.Millisecond
	codexAppAuthProbeCodexForIdentityFn                                       = probeCodexForAppAuthIdentity
)

type codexAppAuthOptions struct {
	profileRef    string
	codexDir      string
	cwd           string
	codexPath     string
	noOpenBrowser bool
	timeout       time.Duration
}

type codexAppAuthStartResult struct {
	LoginID         string
	VerificationURL string
	UserCode        string
}

type codexAppAuthAccount struct {
	Type     string `json:"type"`
	Email    string `json:"email"`
	PlanType string `json:"planType"`
}

func newAppAuthCmd(root *rootOptions) *cobra.Command {
	var codexDir string
	var cwd string
	var profileFlag string
	var codexPath string
	var noOpenBrowser bool
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "auth [profile]",
		Short: "Authenticate Codex for the desktop app",
		Long: "Authenticate Codex for the desktop app using Codex app-server device-code login. " +
			"The command writes auth through Codex's own login flow into the same CODEX_HOME that `cxp app` uses.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			profileRef, err := parseCodexAppArgs(args, profileFlag)
			if err != nil {
				return err
			}
			return runCodexAppAuth(cmd, root, codexAppAuthOptions{
				profileRef:    profileRef,
				codexDir:      codexDir,
				cwd:           cwd,
				codexPath:     codexPath,
				noOpenBrowser: noOpenBrowser,
				timeout:       timeout,
			})
		},
	}

	cmd.Flags().StringVar(&codexDir, "codex-dir", "", "Override Codex data dir (default: ~/.codex)")
	cmd.Flags().StringVar(&cwd, "cwd", "", "Working directory for Codex auth (default: current directory)")
	cmd.Flags().StringVar(&profileFlag, "profile", "", "Proxy profile id or name")
	cmd.Flags().StringVar(&codexPath, "codex-path", "", "Override Codex CLI path (default: install or search PATH)")
	cmd.Flags().BoolVar(&noOpenBrowser, "no-open-browser", false, "Print the device-code URL without opening a browser")
	cmd.Flags().DurationVar(&timeout, "timeout", codexAppAuthDefaultTimeout, "How long to wait for browser sign-in to complete")
	return cmd
}

func runCodexAppAuth(cmd *cobra.Command, root *rootOptions, opts codexAppAuthOptions) error {
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

	codexHome, execIdentity, err := codexAppAuthCodexHome(root, opts.codexDir, cwd)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		return fmt.Errorf("create Codex auth home: %w", err)
	}
	if err := ensurePathOwnedByIdentity(codexHome, execIdentity); err != nil {
		return fmt.Errorf("set Codex auth home ownership: %w", err)
	}

	installOpts := codexInstallOptions{}
	if useProxy {
		installOpts.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, *profile, cfg.Instances, runInstall)
		}
	}
	codexPath, err := ensureCodexAppAuthCodexInstalled(ctx, opts.codexPath, cmd.ErrOrStderr(), installOpts, execIdentity)
	if err != nil {
		return err
	}

	proxyURL := ""
	if useProxy {
		proxyURL, err = codexAppEnsureProxyURLFn(ctx, store, *profile, cfg.Instances, cmd.ErrOrStderr())
		if err != nil {
			return err
		}
	}

	if platform == codexDesktopPlatformWindows && codexAppGOOS() == "linux" && codexAppIsWSL() && strings.TrimSpace(proxyURL) != "" {
		codexAppWarn(cmd.ErrOrStderr(), "using device-code auth from WSL. Codex token polling will use the selected WSL proxy, but a Windows browser can only use that proxy if the WSL loopback port is reachable from Windows.")
	}

	extraEnv := codexAppAuthEnv(codexHome, proxyURL)
	return runCodexAppDeviceCodeAuth(ctx, codexAppDeviceCodeAuthOptions{
		CodexPath:     codexPath,
		Cwd:           cwd,
		CodexHome:     codexHome,
		ExtraEnv:      extraEnv,
		ExecIdentity:  execIdentity,
		ProxyURL:      proxyURL,
		NoOpenBrowser: opts.noOpenBrowser,
		Timeout:       opts.timeout,
		Out:           cmd.OutOrStdout(),
		Err:           cmd.ErrOrStderr(),
	})
}

func codexAppAuthCodexHome(root *rootOptions, codexDir string, cwd string) (string, *execIdentity, error) {
	configPathOverride := ""
	if root != nil {
		configPathOverride = root.configPath
	}
	paths, err := resolveEffectiveLaunchPaths(configPathOverride, codexDir, cwd)
	if err != nil {
		return "", nil, err
	}
	codexHome, err := resolveCodexHomePath(paths.CodexDir, cwd)
	if err != nil {
		return "", nil, err
	}
	if effectivePathsRunningAsRoot() && paths.ExecIdentity == nil && !pathWithinDir(codexHome, paths.Home) {
		return "", nil, execIdentityRequiredError(codexHome)
	}
	return codexHome, paths.ExecIdentity, nil
}

func codexAppAuthEnv(codexHome string, proxyURL string) []string {
	extraEnv := codexHomeEnv(codexHome)
	if noProxy := strings.TrimSpace(os.Getenv("NO_PROXY")); noProxy != "" {
		extraEnv = append(extraEnv, "NO_PROXY="+noProxy)
	}
	if noProxy := strings.TrimSpace(os.Getenv("no_proxy")); noProxy != "" {
		extraEnv = append(extraEnv, "no_proxy="+noProxy)
	}
	if strings.TrimSpace(proxyURL) == "" {
		return extraEnv
	}
	return codexAppProxyEnv(extraEnv, proxyURL)
}

func ensureCodexAppAuthCodexInstalled(ctx context.Context, codexPath string, out io.Writer, opts codexInstallOptions, identity *execIdentity) (string, error) {
	if identity != nil && identity.UID != 0 {
		if strings.TrimSpace(codexPath) != "" {
			resolved := normalizeExecutablePath(codexPath)
			if !executableExists(resolved) {
				return "", fmt.Errorf("codex not found at %s", codexPath)
			}
			if err := codexAppAuthProbeCodexForIdentityFn(ctx, resolved, identity); err != nil {
				return "", err
			}
			return resolved, nil
		}
		return ensureCodexAppAuthCodexInstalledForIdentity(ctx, out, opts, identity)
	}
	resolved, err := ensureCodexInstalledWithOptions(ctx, codexPath, out, opts)
	if err != nil {
		return "", err
	}
	if err := codexAppAuthProbeCodexForIdentityFn(ctx, resolved, identity); err != nil {
		return "", err
	}
	return resolved, nil
}

func ensureCodexAppAuthCodexInstalledForIdentity(ctx context.Context, out io.Writer, opts codexInstallOptions, identity *execIdentity) (string, error) {
	if !opts.upgradeCodex {
		if path, err := findCodexAppAuthCodexForIdentity(ctx, identity); err == nil {
			return path, nil
		}
	}
	if out != nil {
		if opts.upgradeCodex {
			_, _ = fmt.Fprintf(out, "upgrading codex for target user %s...\n", codexAppAuthIdentityLabel(identity))
		} else {
			_, _ = fmt.Fprintf(out, "codex not found for target user %s; installing for that user...\n", codexAppAuthIdentityLabel(identity))
		}
	}
	if err := withCodexInstallLock(ctx, out, func() error {
		if !opts.upgradeCodex {
			if _, err := findCodexAppAuthCodexForIdentity(ctx, identity); err == nil {
				return nil
			}
		}
		runInstall := func(installerEnv []string) error {
			installerEnv = codexAppAuthInstallerEnvForIdentity(installerEnv, identity)
			return runCodexInstallerWithOptions(ctx, out, installerEnv, func(cmd *exec.Cmd) error {
				if opts.configureInstallerCommand != nil {
					if err := opts.configureInstallerCommand(cmd); err != nil {
						return err
					}
				}
				updatedEnv, err := applyExecIdentity(cmd, cmd.Env, identity)
				if err != nil {
					return err
				}
				cmd.Env = codexAppAuthInstallerEnvForIdentity(updatedEnv, identity)
				return nil
			})
		}
		if opts.withInstallerEnv != nil {
			return opts.withInstallerEnv(ctx, runInstall)
		}
		return runInstall(opts.installerEnv)
	}); err != nil {
		return "", err
	}
	path, err := findCodexAppAuthCodexForIdentity(ctx, identity)
	if err != nil {
		return "", codexPostInstallError("installation for target user "+codexAppAuthIdentityLabel(identity), err)
	}
	return path, nil
}

func codexAppAuthInstallerEnvForIdentity(base []string, identity *execIdentity) []string {
	envVars := applyExecIdentityEnv(base, identity)
	if identity == nil || strings.TrimSpace(identity.Home) == "" {
		return envVars
	}
	home := filepath.Clean(identity.Home)
	if runtime.GOOS == "windows" {
		localAppData := filepath.Join(home, "AppData", "Local")
		envVars = setEnvValue(envVars, "LOCALAPPDATA", localAppData)
		envVars = setEnvValue(envVars, "CODEX_NPM_PREFIX", filepath.Join(localAppData, "codex-proxy", "npm-global"))
		envVars = setEnvValue(envVars, "CODEX_NODE_INSTALL_ROOT", filepath.Join(localAppData, "codex-proxy", "node"))
		envVars = setEnvValue(envVars, "NPM_CONFIG_CACHE", filepath.Join(home, "AppData", "Roaming", "npm-cache"))
		return envVars
	}
	envVars = setEnvValue(envVars, "CODEX_NPM_PREFIX", filepath.Join(home, ".local", "share", "codex-proxy", "npm-global"))
	envVars = setEnvValue(envVars, "CODEX_NODE_INSTALL_ROOT", filepath.Join(home, ".cache", "codex-proxy", "node"))
	envVars = setEnvValue(envVars, "NPM_CONFIG_CACHE", filepath.Join(home, ".npm"))
	envVars = setEnvValue(envVars, "npm_config_cache", filepath.Join(home, ".npm"))
	return envVars
}

func codexAppAuthRuntimeEnvForIdentity(base []string, identity *execIdentity) []string {
	envVars := codexAppAuthInstallerEnvForIdentity(base, identity)
	nodeDirs := managedNodeBinCandidatesForEnv(
		runtime.GOOS,
		runtime.GOARCH,
		envValue(envVars, "HOME"),
		envValue(envVars, "CODEX_NODE_INSTALL_ROOT"),
		envValue(envVars, "CODEX_NODE_MAJOR"),
		envValue(envVars, "LOCALAPPDATA"),
		envTempDir(envVars),
	)
	if len(nodeDirs) == 0 {
		return envVars
	}
	pathParts := filepath.SplitList(envValue(envVars, "PATH"))
	seen := map[string]struct{}{}
	normalize := func(path string) string {
		path = filepath.Clean(strings.TrimSpace(path))
		if runtime.GOOS == "windows" {
			path = strings.ToLower(path)
		}
		return path
	}
	for _, dir := range pathParts {
		key := normalize(dir)
		if key != "" {
			seen[key] = struct{}{}
		}
	}
	prepend := make([]string, 0, len(nodeDirs))
	for _, dir := range nodeDirs {
		key := normalize(dir)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		prepend = append(prepend, filepath.Clean(dir))
	}
	updated := append(prepend, pathParts...)
	return setEnvValue(envVars, "PATH", strings.Join(updated, string(os.PathListSeparator)))
}

func findCodexAppAuthCodexForIdentity(ctx context.Context, identity *execIdentity) (string, error) {
	candidates := make([]string, 0, 8)
	seen := map[string]struct{}{}
	add := func(path string) {
		path = normalizeExecutablePath(path)
		if path == "" {
			return
		}
		key := path
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		candidates = append(candidates, path)
	}

	if path, err := exec.LookPath("codex"); err == nil {
		add(path)
	}
	if cached := strings.TrimSpace(readCachedCodexPath()); cached != "" {
		add(cached)
	}
	identityEnv := codexAppAuthInstallerEnvForIdentity(os.Environ(), identity)
	for _, candidate := range codexBinaryCandidatesForEnv(
		runtime.GOOS,
		envValue(identityEnv, "HOME"),
		envValue(identityEnv, "CODEX_NPM_PREFIX"),
		envValue(identityEnv, "LOCALAPPDATA"),
		envValue(identityEnv, "APPDATA"),
		envTempDir(identityEnv),
	) {
		add(candidate)
	}

	var failures []string
	for _, candidate := range candidates {
		if !executableExists(candidate) {
			continue
		}
		if err := codexAppAuthProbeCodexForIdentityFn(ctx, candidate, identity); err == nil {
			return candidate, nil
		} else if len(failures) < 3 {
			failures = append(failures, fmt.Sprintf("%s: %v", candidate, err))
		}
	}
	if len(failures) > 0 {
		return "", fmt.Errorf("codex found but not executable by target user %s (%s)", codexAppAuthIdentityLabel(identity), strings.Join(failures, "; "))
	}
	return "", errCodexBinaryNotFound
}

func probeCodexForAppAuthIdentity(ctx context.Context, codexPath string, identity *execIdentity) error {
	if identity == nil || identity.UID == 0 {
		return nil
	}
	cmd := exec.CommandContext(ctx, codexPath, "--version")
	envVars, err := applyExecIdentity(cmd, codexAppAuthRuntimeEnvForIdentity(os.Environ(), identity), identity)
	if err != nil {
		return err
	}
	envVars = codexAppAuthRuntimeEnvForIdentity(envVars, identity)
	cmd.Env = envVars
	out, err := cmd.CombinedOutput()
	if err != nil {
		detail := strings.TrimSpace(string(out))
		if detail != "" {
			return fmt.Errorf("codex at %s is not executable by target user %s; pass --codex-path to a Codex binary that the target user can run: %w\n%s", codexPath, codexAppAuthIdentityLabel(identity), err, detail)
		}
		return fmt.Errorf("codex at %s is not executable by target user %s; pass --codex-path to a Codex binary that the target user can run: %w", codexPath, codexAppAuthIdentityLabel(identity), err)
	}
	return nil
}

func codexAppAuthIdentityLabel(identity *execIdentity) string {
	if identity == nil {
		return "current user"
	}
	if username := strings.TrimSpace(identity.Username); username != "" {
		return username
	}
	return fmt.Sprintf("uid %d", identity.UID)
}

type codexAppDeviceCodeAuthOptions struct {
	CodexPath     string
	Cwd           string
	CodexHome     string
	ExtraEnv      []string
	ExecIdentity  *execIdentity
	ProxyURL      string
	NoOpenBrowser bool
	Timeout       time.Duration
	Out           io.Writer
	Err           io.Writer
}

func runCodexAppDeviceCodeAuth(ctx context.Context, opts codexAppDeviceCodeAuthOptions) error {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = codexAppAuthDefaultTimeout
	}
	authCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	startupCtx, cancelStartup := context.WithTimeout(authCtx, minPositiveDuration(codexAppAuthAppServerStartTimeout, timeout))
	defer cancelStartup()
	if opts.Out != nil {
		_, _ = fmt.Fprintf(opts.Out, "Starting Codex ChatGPT auth for the desktop app...\nCODEX_HOME: %s\n", opts.CodexHome)
	}

	transport, err := codexAppAuthStarter.StartAppServer(startupCtx, codexrunner.AppServerStartRequest{
		Command:    opts.CodexPath,
		Args:       []string{"app-server"},
		WorkingDir: opts.Cwd,
		ExtraEnv:   append([]string{}, opts.ExtraEnv...),
		Timeout:    codexAppAuthAppServerStartTimeout,
		ConfigureCommand: func(c *exec.Cmd) error {
			if opts.ExecIdentity == nil {
				return nil
			}
			updatedEnv, err := applyExecIdentity(c, c.Env, opts.ExecIdentity)
			if err != nil {
				return err
			}
			updatedEnv = codexAppAuthRuntimeEnvForIdentity(updatedEnv, opts.ExecIdentity)
			c.Env = updatedEnv
			return nil
		},
	})
	if err != nil {
		return err
	}
	defer transport.Close()

	client := &codexAppAuthClient{transport: transport}
	if err := client.initialize(startupCtx); err != nil {
		return codexAppAuthMaybeTimeoutError(startupCtx, err, "timed out waiting for Codex app-server to become ready")
	}

	account, err := client.readAccount(startupCtx, false)
	startupErr := startupCtx.Err()
	cancelStartup()
	if err != nil {
		if errors.Is(startupErr, context.DeadlineExceeded) {
			return codexAppAuthMaybeTimeoutError(startupCtx, err, "timed out waiting for Codex app-server to read account state")
		}
		if errors.Is(startupErr, context.Canceled) {
			return startupErr
		}
		codexAppWarn(opts.Err, "could not read existing Codex account state; starting a fresh ChatGPT login: %v", err)
	}

	if account != nil && account.Type == "chatgpt" {
		if opts.Out != nil {
			_, _ = fmt.Fprintf(opts.Out, "Codex is already authenticated with ChatGPT")
			if strings.TrimSpace(account.Email) != "" {
				_, _ = fmt.Fprintf(opts.Out, " as %s", account.Email)
			}
			_, _ = fmt.Fprintln(opts.Out, ".")
			printCodexAppAuthNextStep(opts.Out)
		}
		return nil
	}

	login, err := client.startDeviceCodeLogin(authCtx)
	if err != nil {
		return codexAppAuthMaybeTimeoutError(authCtx, err, "timed out waiting for Codex desktop app auth to complete")
	}
	printCodexAppAuthPrompt(opts.Out, login, opts.ProxyURL, opts.NoOpenBrowser)

	if !opts.NoOpenBrowser {
		if err := codexAppOpenAuthURLFn(authCtx, login.VerificationURL, opts.ProxyURL, opts.ExecIdentity, opts.Err); err != nil {
			codexAppWarn(opts.Err, "could not open the auth URL automatically: %v", err)
			if strings.TrimSpace(opts.ProxyURL) != "" {
				codexAppWarn(opts.Err, "open %s manually in a browser configured to use proxy %s and enter code %s", login.VerificationURL, opts.ProxyURL, login.UserCode)
			} else {
				codexAppWarn(opts.Err, "open %s manually and enter code %s", login.VerificationURL, login.UserCode)
			}
		}
	}

	if err := client.waitForLoginCompleted(authCtx, login.LoginID); err != nil {
		return codexAppAuthMaybeTimeoutError(authCtx, err, "timed out waiting for Codex desktop app auth to complete")
	}
	if err := ensureTreeOwnedByIdentity(opts.CodexHome, opts.ExecIdentity); err != nil {
		return fmt.Errorf("set Codex auth file ownership: %w", err)
	}
	account, err = client.waitForChatGPTAccount(authCtx, codexAppAuthAccountReadyTimeout, codexAppAuthAccountPollInterval)
	if err != nil {
		return codexAppAuthMaybeTimeoutError(authCtx, err, "timed out waiting for Codex desktop app auth to become readable")
	}
	if opts.Out != nil {
		_, _ = fmt.Fprintf(opts.Out, "Codex desktop app auth completed")
		if strings.TrimSpace(account.Email) != "" {
			_, _ = fmt.Fprintf(opts.Out, " for %s", account.Email)
		}
		_, _ = fmt.Fprintln(opts.Out, ".")
		printCodexAppAuthNextStep(opts.Out)
	}
	return nil
}

func minPositiveDuration(a time.Duration, b time.Duration) time.Duration {
	if a <= 0 {
		return b
	}
	if b <= 0 || a < b {
		return a
	}
	return b
}

func codexAppAuthMaybeTimeoutError(ctx context.Context, err error, message string) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%s", message)
	}
	return err
}

func printCodexAppAuthPrompt(out io.Writer, login codexAppAuthStartResult, proxyURL string, noOpenBrowser bool) {
	if out == nil {
		return
	}
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintln(out, "Open this URL in the browser and sign in:")
	_, _ = fmt.Fprintf(out, "  %s\n", login.VerificationURL)
	_, _ = fmt.Fprintln(out, "Enter this one-time code:")
	_, _ = fmt.Fprintf(out, "  %s\n", login.UserCode)
	if strings.TrimSpace(proxyURL) != "" {
		if noOpenBrowser {
			_, _ = fmt.Fprintf(out, "The Codex auth process is using proxy %s. Open the URL manually in a browser configured to use that proxy.\n", proxyURL)
		} else {
			_, _ = fmt.Fprintf(out, "The Codex auth process is using proxy %s. A browser window will be opened with the same proxy when a supported browser is available.\n", proxyURL)
		}
	}
	_, _ = fmt.Fprintln(out, "Waiting for sign-in to complete...")
	_, _ = fmt.Fprintln(out)
}

func printCodexAppAuthNextStep(out io.Writer) {
	if out == nil {
		return
	}
	_, _ = fmt.Fprintln(out, "Quit any already-running Codex desktop app, then launch it with `cxp app` so it inherits this CODEX_HOME and proxy environment.")
}

type codexAppAuthClient struct {
	transport     codexrunner.AppServerLineTransport
	nextID        int64
	notifications []codexAppAuthMessage
}

type codexAppAuthRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type codexAppAuthNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type codexAppAuthErrorResponse struct {
	JSONRPC string                 `json:"jsonrpc"`
	ID      json.RawMessage        `json:"id"`
	Error   codexAppAuthErrorField `json:"error"`
}

type codexAppAuthMessage struct {
	ID     json.RawMessage         `json:"id"`
	Method string                  `json:"method"`
	Params json.RawMessage         `json:"params"`
	Result json.RawMessage         `json:"result"`
	Error  *codexAppAuthErrorField `json:"error"`
}

type codexAppAuthErrorField struct {
	Code    json.RawMessage `json:"code"`
	Message string          `json:"message"`
}

func (c *codexAppAuthClient) initialize(ctx context.Context) error {
	if _, err := c.request(ctx, "initialize", map[string]any{
		"clientInfo": map[string]string{
			"name":    "codex-helper-app-auth",
			"version": buildVersion(),
		},
		"capabilities": nil,
	}, nil); err != nil {
		return err
	}
	return c.writeNotification(ctx, "initialized", map[string]any{})
}

func (c *codexAppAuthClient) readAccount(ctx context.Context, refresh bool) (*codexAppAuthAccount, error) {
	raw, err := c.request(ctx, "account/read", map[string]any{"refreshToken": refresh}, nil)
	if err != nil {
		return nil, err
	}
	var response struct {
		Account *codexAppAuthAccount `json:"account"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("parse account/read response: %w", err)
	}
	return response.Account, nil
}

func (c *codexAppAuthClient) startDeviceCodeLogin(ctx context.Context) (codexAppAuthStartResult, error) {
	raw, err := c.request(ctx, "account/login/start", map[string]any{"type": "chatgptDeviceCode"}, nil)
	if err != nil {
		return codexAppAuthStartResult{}, err
	}
	var response struct {
		Type            string `json:"type"`
		LoginID         string `json:"loginId"`
		LoginIDSnake    string `json:"login_id"`
		VerificationURL string `json:"verificationUrl"`
		UserCode        string `json:"userCode"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return codexAppAuthStartResult{}, fmt.Errorf("parse account/login/start response: %w", err)
	}
	loginID := firstNonEmptyString(response.LoginID, response.LoginIDSnake)
	if response.Type != "chatgptDeviceCode" || strings.TrimSpace(loginID) == "" || strings.TrimSpace(response.VerificationURL) == "" || strings.TrimSpace(response.UserCode) == "" {
		return codexAppAuthStartResult{}, fmt.Errorf("unexpected account/login/start response for device-code auth")
	}
	return codexAppAuthStartResult{
		LoginID:         loginID,
		VerificationURL: response.VerificationURL,
		UserCode:        response.UserCode,
	}, nil
}

func (c *codexAppAuthClient) waitForLoginCompleted(ctx context.Context, loginID string) error {
	for {
		if msg, ok := c.takeBufferedNotification("account/login/completed"); ok {
			done, err := c.handleLoginCompletedMessage(msg, loginID)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
			continue
		}
		msg, err := c.readMessage(ctx)
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("timed out waiting for Codex desktop app auth to complete")
			}
			return err
		}
		if c.isServerRequest(msg) {
			if err := c.writeUnsupportedServerRequest(ctx, msg); err != nil {
				return err
			}
			continue
		}
		if strings.TrimSpace(msg.Method) != "account/login/completed" {
			continue
		}
		done, err := c.handleLoginCompletedMessage(msg, loginID)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func (c *codexAppAuthClient) handleLoginCompletedMessage(msg codexAppAuthMessage, loginID string) (bool, error) {
	var params struct {
		LoginID      string `json:"loginId"`
		LoginIDSnake string `json:"login_id"`
		Success      bool   `json:"success"`
		Error        string `json:"error"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return false, fmt.Errorf("parse account/login/completed notification: %w", err)
	}
	gotLoginID := firstNonEmptyString(params.LoginID, params.LoginIDSnake)
	if strings.TrimSpace(gotLoginID) == "" || gotLoginID != loginID {
		return false, nil
	}
	if params.Success {
		return true, nil
	}
	if strings.TrimSpace(params.Error) != "" {
		return false, fmt.Errorf("Codex desktop app auth failed: %s", params.Error)
	}
	return false, fmt.Errorf("Codex desktop app auth was not completed")
}

func (c *codexAppAuthClient) takeBufferedNotification(method string) (codexAppAuthMessage, bool) {
	for i, msg := range c.notifications {
		if strings.TrimSpace(msg.Method) != method {
			continue
		}
		c.notifications = append(c.notifications[:i], c.notifications[i+1:]...)
		return msg, true
	}
	return codexAppAuthMessage{}, false
}

func (c *codexAppAuthClient) bufferNotification(msg codexAppAuthMessage) {
	c.notifications = append(c.notifications, msg)
}

func (c *codexAppAuthClient) waitForChatGPTAccount(ctx context.Context, readyTimeout time.Duration, interval time.Duration) (*codexAppAuthAccount, error) {
	if readyTimeout <= 0 {
		readyTimeout = codexAppAuthAccountReadyTimeout
	}
	if interval <= 0 {
		interval = codexAppAuthAccountPollInterval
	}
	readyCtx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()

	for {
		account, err := c.readAccount(readyCtx, false)
		if err != nil {
			if errors.Is(readyCtx.Err(), context.DeadlineExceeded) {
				return nil, fmt.Errorf("timed out waiting for Codex desktop app auth to become readable")
			}
			return nil, err
		}
		if account != nil && account.Type == "chatgpt" {
			return account, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-readyCtx.Done():
			timer.Stop()
			if !errors.Is(readyCtx.Err(), context.DeadlineExceeded) {
				return nil, readyCtx.Err()
			}
			return nil, fmt.Errorf("timed out waiting for Codex desktop app auth to become readable")
		case <-timer.C:
		}
	}
}

func (c *codexAppAuthClient) request(ctx context.Context, method string, params any, onNotify func(codexAppAuthMessage) error) (json.RawMessage, error) {
	c.nextID++
	id := c.nextID
	line, err := json.Marshal(codexAppAuthRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return nil, err
	}
	if err := c.transport.WriteLine(ctx, line); err != nil {
		return nil, err
	}
	for {
		msg, err := c.readMessage(ctx)
		if err != nil {
			return nil, err
		}
		if c.isServerRequest(msg) {
			if err := c.writeUnsupportedServerRequest(ctx, msg); err != nil {
				return nil, err
			}
			continue
		}
		if len(bytes.TrimSpace(msg.ID)) == 0 {
			if onNotify != nil {
				if err := onNotify(msg); err != nil {
					return nil, err
				}
			} else {
				c.bufferNotification(msg)
			}
			continue
		}
		if !sameCodexAppAuthID(msg.ID, id) {
			return nil, fmt.Errorf("unexpected app-server response id %s while waiting for %d", string(msg.ID), id)
		}
		if msg.Error != nil {
			return nil, codexAppAuthResponseError(msg.Error)
		}
		return msg.Result, nil
	}
}

func (c *codexAppAuthClient) writeNotification(ctx context.Context, method string, params any) error {
	line, err := json.Marshal(codexAppAuthNotification{JSONRPC: "2.0", Method: method, Params: params})
	if err != nil {
		return err
	}
	return c.transport.WriteLine(ctx, line)
}

func (c *codexAppAuthClient) readMessage(ctx context.Context) (codexAppAuthMessage, error) {
	line, err := c.transport.ReadLine(ctx)
	if err != nil {
		return codexAppAuthMessage{}, err
	}
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return codexAppAuthMessage{}, fmt.Errorf("empty app-server JSON line")
	}
	var msg codexAppAuthMessage
	if err := json.Unmarshal(line, &msg); err != nil {
		return codexAppAuthMessage{}, fmt.Errorf("invalid app-server JSON line: %w", err)
	}
	return msg, nil
}

func (c *codexAppAuthClient) isServerRequest(msg codexAppAuthMessage) bool {
	return strings.TrimSpace(msg.Method) != "" && len(bytes.TrimSpace(msg.ID)) > 0
}

func (c *codexAppAuthClient) writeUnsupportedServerRequest(ctx context.Context, msg codexAppAuthMessage) error {
	method := strings.TrimSpace(msg.Method)
	if method == "" {
		method = "server request"
	}
	line, err := json.Marshal(codexAppAuthErrorResponse{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Error: codexAppAuthErrorField{
			Code:    json.RawMessage(`-32601`),
			Message: method + " is not supported by codex-helper app auth",
		},
	})
	if err != nil {
		return err
	}
	return c.transport.WriteLine(ctx, line)
}

func codexAppAuthResponseError(field *codexAppAuthErrorField) error {
	message := strings.TrimSpace(field.Message)
	if message == "" {
		message = "codex app-server returned an error response"
	}
	if code := codexAppAuthErrorCodeString(field.Code); code != "" {
		message = code + ": " + message
	}
	return fmt.Errorf("%s", message)
}

func codexAppAuthErrorCodeString(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return strings.TrimSpace(text)
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		return number.String()
	}
	return string(raw)
}

func sameCodexAppAuthID(raw json.RawMessage, id int64) bool {
	var num json.Number
	if err := json.Unmarshal(raw, &num); err == nil {
		got, err := strconv.ParseInt(num.String(), 10, 64)
		return err == nil && got == id
	}
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		return str == strconv.FormatInt(id, 10)
	}
	return false
}

func openCodexAppAuthURL(ctx context.Context, rawURL string, proxyURL string, identity *execIdentity, log io.Writer) error {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return fmt.Errorf("auth URL is empty")
	}
	if strings.TrimSpace(proxyURL) != "" {
		if err := openCodexAppAuthURLWithProxy(ctx, rawURL, proxyURL, identity); err == nil {
			return nil
		} else {
			codexAppWarn(log, "could not open a proxy-managed browser automatically: %v", err)
			return err
		}
	}
	return defaultTeamsServiceOpenURL(ctx, rawURL)
}

func openCodexAppAuthURLWithProxy(ctx context.Context, rawURL string, proxyURL string, identity *execIdentity) error {
	switch codexAppGOOS() {
	case "darwin":
		return openCodexAppAuthURLWithMacProxyBrowser(ctx, rawURL, proxyURL, identity)
	case "windows":
		return openCodexAppAuthURLWithWindowsProxyBrowser(ctx, rawURL, proxyURL)
	case "linux":
		if codexAppIsWSL() {
			if err := codexAppAuthWindowsProxyReachableFn(ctx, proxyURL); err != nil {
				return fmt.Errorf("Windows browser cannot reach the selected WSL proxy %s: %w", proxyURL, err)
			}
			return openCodexAppAuthURLWithWindowsProxyBrowser(ctx, rawURL, proxyURL)
		}
	}
	return fmt.Errorf("proxy-managed browser opening is not supported on %s", codexAppGOOS())
}

func openCodexAppAuthURLWithMacProxyBrowser(ctx context.Context, rawURL string, proxyURL string, identity *execIdentity) error {
	if identity != nil && identity.UID != 0 {
		return fmt.Errorf("automatic proxy browser opening is disabled when cxp is running with an effective target identity; open the printed URL manually in that user's browser with proxy %s", proxyURL)
	}
	runID := codexAppAuthBrowserRunIDFn(proxyURL)
	type browserCandidate struct {
		AppName string
		ID      string
	}
	candidates := []browserCandidate{
		{AppName: "Google Chrome", ID: "google-chrome"},
		{AppName: "Microsoft Edge", ID: "microsoft-edge"},
		{AppName: "Chromium", ID: "chromium"},
	}
	var lastErr error
	for _, browser := range candidates {
		profileDir, err := codexAppAuthBrowserProfileDir(browser.ID, proxyURL, runID, identity)
		if err != nil {
			return err
		}
		cmd := codexAppCommandContext(ctx, "open",
			"-na", browser.AppName,
			"--args",
			"--user-data-dir="+profileDir,
			"--proxy-server="+proxyURL,
			"--new-window",
			"--no-first-run",
			"--disable-extensions",
			rawURL,
		)
		if err := cmd.Run(); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no supported Chromium browser was found")
	}
	return lastErr
}

func openCodexAppAuthURLWithWindowsProxyBrowser(ctx context.Context, rawURL string, proxyURL string) error {
	name := teamsServicePowerShellExecutable()
	if _, err := codexAppLookPath(name); err != nil {
		return fmt.Errorf("PowerShell is required to open a proxy-managed auth browser: %s not found: %w", name, err)
	}
	script := codexAppAuthWindowsProxyBrowserScript(rawURL, proxyURL, codexAppAuthBrowserRunIDFn(proxyURL))
	if _, err := codexAppCommandOutput(ctx, name, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script); err != nil {
		return fmt.Errorf("open proxy-managed Windows browser: %w", err)
	}
	return nil
}

func codexAppAuthBrowserProfileDir(browserID string, proxyURL string, runID string, identity *execIdentity) (string, error) {
	base, err := codexAppAuthBrowserCacheBase(identity)
	if err != nil || strings.TrimSpace(base) == "" {
		base = os.TempDir()
	}
	managedRoot := filepath.Join(base, "codex-helper", "app-auth-browser")
	dir := filepath.Join(managedRoot, codexAppAuthSafePathPart(browserID), codexAppAuthSafePathPart(runID)+"-"+codexAppAuthProxyHash(proxyURL))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	if err := ensureTreeOwnedByIdentity(managedRoot, identity); err != nil {
		return "", fmt.Errorf("set proxy browser profile ownership: %w", err)
	}
	return dir, nil
}

func codexAppAuthBrowserCacheBase(identity *execIdentity) (string, error) {
	if identity != nil && strings.TrimSpace(identity.Home) != "" {
		if codexAppGOOS() == "darwin" {
			return filepath.Join(identity.Home, "Library", "Caches"), nil
		}
		return filepath.Join(identity.Home, ".cache"), nil
	}
	return os.UserCacheDir()
}

func defaultCodexAppAuthBrowserRunID(proxyURL string) string {
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

var codexAppAuthUnsafePathPart = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func codexAppAuthSafePathPart(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "default"
	}
	safe := strings.Trim(codexAppAuthUnsafePathPart.ReplaceAllString(raw, "-"), ".-")
	if safe == "" {
		return "default"
	}
	if len(safe) > 64 {
		return safe[:64]
	}
	return safe
}

func codexAppAuthProxyHash(proxyURL string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(proxyURL)))
	return hex.EncodeToString(sum[:])[:12]
}

func codexAppAuthWindowsProxyReachable(ctx context.Context, proxyURL string) error {
	host, port, err := codexAppAuthProxyHostPort(proxyURL)
	if err != nil {
		return err
	}
	name := teamsServicePowerShellExecutable()
	if _, err := codexAppLookPath(name); err != nil {
		return fmt.Errorf("PowerShell is required to test Windows access to the WSL proxy: %s not found: %w", name, err)
	}
	script := codexAppAuthWindowsProxyReachabilityScript(host, port)
	if _, err := codexAppCommandOutput(ctx, name, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script); err != nil {
		return err
	}
	return nil
}

func codexAppAuthProxyHostPort(proxyURL string) (string, string, error) {
	parsed, err := url.Parse(strings.TrimSpace(proxyURL))
	if err != nil {
		return "", "", fmt.Errorf("parse proxy URL: %w", err)
	}
	host := parsed.Hostname()
	port := parsed.Port()
	if host == "" || port == "" {
		return "", "", fmt.Errorf("proxy URL must include host and port")
	}
	return host, port, nil
}

func codexAppAuthWindowsProxyReachabilityScript(host string, port string) string {
	healthURL := "http://" + net.JoinHostPort(host, port) + "/_codex_proxy/health"
	return strings.Join([]string{
		"$ErrorActionPreference = 'Stop'",
		"$healthUrl = " + powershellSingleQuote(healthURL),
		"$response = Invoke-RestMethod -TimeoutSec 2 -Uri $healthUrl",
		"if ($null -eq $response -or -not $response.ok) { throw ('proxy health check failed at ' + $healthUrl) }",
	}, "; ")
}

func codexAppAuthWindowsProxyBrowserScript(rawURL string, proxyURL string, runID string) string {
	url := powershellSingleQuote(rawURL)
	proxy := powershellSingleQuote(proxyURL)
	run := powershellSingleQuote(codexAppAuthSafePathPart(runID) + "-" + codexAppAuthProxyHash(proxyURL))
	return strings.Join([]string{
		"$ErrorActionPreference = 'Stop'",
		"$url = " + url,
		"$proxy = " + proxy,
		"$runId = " + run,
		"$profileRoot = Join-Path $env:LOCALAPPDATA 'codex-helper\\app-auth-browser'",
		"$candidates = @()",
		"if ($env:LOCALAPPDATA) { $candidates += (Join-Path $env:LOCALAPPDATA 'Microsoft\\Edge\\Application\\msedge.exe') }",
		"if ($env:LOCALAPPDATA) { $candidates += (Join-Path $env:LOCALAPPDATA 'Google\\Chrome\\Application\\chrome.exe') }",
		"if ($env:ProgramFiles) { $candidates += (Join-Path $env:ProgramFiles 'Microsoft\\Edge\\Application\\msedge.exe') }",
		"if (${env:ProgramFiles(x86)}) { $candidates += (Join-Path ${env:ProgramFiles(x86)} 'Microsoft\\Edge\\Application\\msedge.exe') }",
		"if ($env:ProgramFiles) { $candidates += (Join-Path $env:ProgramFiles 'Google\\Chrome\\Application\\chrome.exe') }",
		"if (${env:ProgramFiles(x86)}) { $candidates += (Join-Path ${env:ProgramFiles(x86)} 'Google\\Chrome\\Application\\chrome.exe') }",
		"$cmd = Get-Command msedge.exe -ErrorAction SilentlyContinue; if ($null -ne $cmd) { $candidates += $cmd.Source }",
		"$cmd = Get-Command chrome.exe -ErrorAction SilentlyContinue; if ($null -ne $cmd) { $candidates += $cmd.Source }",
		"$browserPaths = @($candidates | Where-Object { $_ -and (Test-Path -LiteralPath $_) } | Select-Object -Unique)",
		"if ($browserPaths.Count -eq 0) { throw 'No supported Edge or Chrome browser was found. Install Microsoft Edge or Chrome, or open the printed URL manually in a browser that uses the selected proxy.' }",
		"$failures = @()",
		"foreach ($browser in $browserPaths) { $browserId = [IO.Path]::GetFileNameWithoutExtension($browser); $profile = Join-Path $profileRoot ($runId + '-' + $browserId); New-Item -ItemType Directory -Force -Path $profile | Out-Null; $args = @('--user-data-dir=' + $profile, '--proxy-server=' + $proxy, '--new-window', '--no-first-run', '--disable-extensions', $url); try { & $browser @args | Out-Null; return } catch { $failures += ($browser + ': ' + $_.Exception.Message) } }",
		"throw ('Could not launch a proxy-managed Edge or Chrome browser: ' + ($failures -join '; '))",
	}, "; ")
}
