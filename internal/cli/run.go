package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/baaaaaaaka/codex-helper/internal/cloudgate"
	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/env"
	"github.com/baaaaaaaka/codex-helper/internal/ids"
	"github.com/baaaaaaaka/codex-helper/internal/manager"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/proc"
	"github.com/baaaaaaaka/codex-helper/internal/stack"
)

var stackStart = stack.Start

var (
	runWithProfileFn                   = runWithProfile
	runWithProfileOptionsFn            = runWithProfileOptions
	runTargetWithFallbackWithOptionsFn = runTargetWithFallbackWithOptions
	runTargetCommand                   = exec.Command
	ensureProxyPreferenceRunFn         = ensureProxyPreference
	ensureProfileRunFn                 = ensureProfile
	persistProxyPreferenceRunFn        = persistProxyPreference
	runTargetHealthCheckInterval       = 5 * time.Second
)

func newRunCmd(root *rootOptions) *cobra.Command {
	var modelProfile string
	cmd := &cobra.Command{
		Use:   "run [profile] -- [cmd args...]",
		Short: "Run a command using direct mode or an SSH-backed local proxy",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Keep `run` working, but also auto-init when no profiles exist.
			return runLike(cmd, root, true)
		},
	}
	cmd.Flags().Bool("yolo", false, "Launch Codex with helper-managed yolo mode")
	cmd.Flags().StringVar(&modelProfile, "model-profile", "", "Model profile id or name for Codex launches")
	return cmd
}

func runLike(cmd *cobra.Command, root *rootOptions, autoInit bool) error {
	all := cmd.Flags().Args()
	dash := cmd.Flags().ArgsLenAtDash()

	before := all
	after := []string{}
	if dash >= 0 {
		before = all[:dash]
		after = all[dash:]
	}

	var profileRef string
	if len(before) > 0 {
		profileRef = before[0]
	}
	if len(before) > 1 {
		return fmt.Errorf("unexpected args before -- (only profile is allowed)")
	}
	if len(after) == 0 {
		after = []string{"codex"}
	}
	runOpts := runTargetOptionsFromRunFlags(cmd)
	if strings.TrimSpace(runOpts.ModelProfileRef) != "" && (len(after) == 0 || !isCodexCommand(after[0])) {
		return fmt.Errorf("--model-profile only applies to Codex launches")
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	modelProfileLaunchRequested := false
	if runOpts.ModelProfileRef != "" || (len(after) > 0 && isCodexCommand(after[0])) {
		cfgForModelProfile, err := store.Load()
		if err != nil {
			return err
		}
		resolvedModelProfile, err := modelprofile.Resolve(cfgForModelProfile, runOpts.ModelProfileRef)
		if err != nil {
			return err
		}
		modelProfileLaunchRequested = strings.TrimSpace(runOpts.ModelProfileRef) != "" || strings.TrimSpace(cfgForModelProfile.DefaultModelProfile) != ""
		if resolvedModelProfile.SSHProfile != nil {
			if profileRef != "" {
				selected, ok := cfgForModelProfile.FindProfile(profileRef)
				if !ok {
					return fmt.Errorf("profile %q not found", profileRef)
				}
				if selected.ID != resolvedModelProfile.SSHProfile.ID {
					return fmt.Errorf("model profile %q requires ssh proxy %q, but run selected proxy profile %q", resolvedModelProfile.Name, resolvedModelProfile.SSHProfile.Name, profileRef)
				}
			} else {
				profileRef = resolvedModelProfile.SSHProfile.Name
			}
		}
	}

	if profileRef != "" {
		profile, cfg, err := ensureProfileRunFn(ctx, store, profileRef, autoInit, cmd.OutOrStdout())
		if err != nil {
			return err
		}
		if cfg.ProxyEnabled == nil {
			enabled := true
			if err := persistProxyPreferenceRunFn(store, enabled); err != nil {
				return err
			}
			cfg.ProxyEnabled = &enabled
		}
		if runOpts.YoloEnabled || modelProfileLaunchRequested {
			runOpts.Log = cmd.ErrOrStderr()
			return runWithProfileOptionsFn(ctx, store, profile, cfg.Instances, after, runOpts)
		}
		return runWithProfileFn(ctx, store, profile, cfg.Instances, after)
	}

	useProxy := false
	useProxy, _, err = ensureProxyPreferenceRunFn(ctx, store, "", cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	if useProxy {
		profile, cfgWithProfile, err := ensureProfileRunFn(ctx, store, "", autoInit, cmd.OutOrStdout())
		if err != nil {
			return err
		}
		if cfgWithProfile.ProxyEnabled == nil {
			enabled := true
			if err := persistProxyPreferenceRunFn(store, enabled); err != nil {
				return err
			}
			cfgWithProfile.ProxyEnabled = &enabled
		}
		if runOpts.YoloEnabled || modelProfileLaunchRequested {
			runOpts.Log = cmd.ErrOrStderr()
			return runWithProfileOptionsFn(ctx, store, profile, cfgWithProfile.Instances, after, runOpts)
		}
		return runWithProfileFn(ctx, store, profile, cfgWithProfile.Instances, after)
	}

	log := cmd.ErrOrStderr()
	resolvedCmd, err := resolveRunCommandWithInstallOptions(ctx, after, log, codexInstallOptions{})
	if err != nil {
		return err
	}

	opts := runOpts
	opts.UseProxy = false
	opts.Log = log
	if isCodexCommand(resolvedCmd[0]) {
		extraEnv, execIdentity, err := codexExecutionContextForRun("")
		if err != nil {
			return err
		}
		opts.ExtraEnv = append(opts.ExtraEnv, extraEnv...)
		opts.ExecIdentity = execIdentity
		var modelCleanup func()
		resolvedCmd, modelCleanup, err = prepareCodexModelProfileForRun(ctx, store, resolvedCmd, &opts, "")
		if err != nil {
			return err
		}
		if modelCleanup != nil {
			defer modelCleanup()
		}
		var cleanup func()
		resolvedCmd, cleanup = prepareYoloCodexCommandForRun(store, resolvedCmd, &opts)
		if cleanup != nil {
			defer cleanup()
		}
		if err := requireYoloLaunchArgs(resolvedCmd, opts); err != nil {
			return err
		}
	}
	return runTargetWithFallbackWithOptionsFn(ctx, resolvedCmd, "", nil, nil, opts)
}

func runTargetOptionsFromRunFlags(cmd *cobra.Command) runTargetOptions {
	opts := defaultRunTargetOptions()
	if cmd == nil {
		return opts
	}
	yolo, err := cmd.Flags().GetBool("yolo")
	if err == nil && yolo {
		opts.YoloEnabled = true
		opts.RequireYolo = true
	}
	if flag := cmd.Flags().Lookup("model-profile"); flag != nil {
		opts.ModelProfileRef = strings.TrimSpace(flag.Value.String())
	}
	return opts
}

func selectProfile(cfg config.Config, ref string) (config.Profile, error) {
	if ref != "" {
		if p, ok := cfg.FindProfile(ref); ok {
			return p, nil
		}
		return config.Profile{}, fmt.Errorf("profile %q not found", ref)
	}
	if len(cfg.Profiles) == 0 {
		return config.Profile{}, fmt.Errorf("no profiles found; run `codex-proxy init` (or run `codex-proxy` to create one)")
	}
	if len(cfg.Profiles) == 1 {
		return cfg.Profiles[0], nil
	}
	return config.Profile{}, fmt.Errorf("multiple profiles exist; specify one: `codex-proxy <profile>` or `codex-proxy run <profile> -- ...`")
}

func runWithExistingInstance(ctx context.Context, hc manager.HealthClient, inst config.Instance, cmdArgs []string) error {
	return runWithExistingInstanceOptions(ctx, nil, hc, inst, cmdArgs, defaultRunTargetOptions())
}

func runWithExistingInstanceOptions(
	ctx context.Context,
	store *config.Store,
	hc manager.HealthClient,
	inst config.Instance,
	cmdArgs []string,
	opts runTargetOptions,
) error {
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d", inst.HTTPPort)
	log := opts.Log
	if log == nil {
		log = os.Stderr
	}
	resolvedCmd, err := resolveRunCommandWithInstallOptions(ctx, cmdArgs, log, codexInstallOptions{
		installerEnv: env.WithProxy(os.Environ(), proxyURL),
	})
	if err != nil {
		return codexResolveError{err: err}
	}
	cmdArgs = resolvedCmd
	if isCodexCommand(cmdArgs[0]) {
		if err := applyDefaultCodexExecutionContext(&opts); err != nil {
			return err
		}
		var modelCleanup func()
		cmdArgs, modelCleanup, err = prepareCodexModelProfileForRun(ctx, store, cmdArgs, &opts, proxyURL)
		if err != nil {
			return err
		}
		if modelCleanup != nil {
			defer modelCleanup()
		}
		var cleanup func()
		cmdArgs, cleanup = prepareYoloCodexCommandForRun(store, cmdArgs, &opts)
		if cleanup != nil {
			defer cleanup()
		}
		if err := requireYoloLaunchArgs(cmdArgs, opts); err != nil {
			return err
		}
	}

	return runTargetSupervisedWithOptions(ctx, cmdArgs, proxyURL, func() error {
		return hc.CheckHTTPProxy(inst.HTTPPort, inst.ID)
	}, nil, opts)
}

func runWithNewStack(ctx context.Context, store *config.Store, profile config.Profile, cmdArgs []string) error {
	return runWithNewStackOptions(ctx, store, profile, cmdArgs, defaultRunTargetOptions())
}

func runWithNewStackOptions(
	ctx context.Context,
	store *config.Store,
	profile config.Profile,
	cmdArgs []string,
	opts runTargetOptions,
) error {
	instanceID, err := ids.New()
	if err != nil {
		return err
	}

	st, err := stackStart(profile, instanceID, stack.Options{})
	if err != nil {
		return err
	}
	stopContextClose := closeStackWhenContextDone(ctx, st)
	defer func() { _ = st.Close(context.Background()) }()
	defer stopContextClose()

	proxyURL := st.HTTPProxyURL()

	if len(cmdArgs) == 0 {
		return fmt.Errorf("missing command")
	}
	log := opts.Log
	if log == nil {
		log = os.Stderr
	}
	resolvedCmd, err := resolveRunCommandWithInstallOptions(ctx, cmdArgs, log, codexInstallOptions{
		installerEnv: env.WithProxy(os.Environ(), proxyURL),
	})
	if err != nil {
		return err
	}
	cmdArgs = resolvedCmd
	if isCodexCommand(cmdArgs[0]) {
		if err := applyDefaultCodexExecutionContext(&opts); err != nil {
			return err
		}
		var modelCleanup func()
		cmdArgs, modelCleanup, err = prepareCodexModelProfileForRun(ctx, store, cmdArgs, &opts, proxyURL)
		if err != nil {
			return err
		}
		if modelCleanup != nil {
			defer modelCleanup()
		}
		var cleanup func()
		cmdArgs, cleanup = prepareYoloCodexCommandForRun(store, cmdArgs, &opts)
		if cleanup != nil {
			defer cleanup()
		}
		if err := requireYoloLaunchArgs(cmdArgs, opts); err != nil {
			return err
		}
	}

	hc := manager.HealthClient{Timeout: 1 * time.Second}
	return runTargetSupervisedWithOptions(ctx, cmdArgs, proxyURL, func() error {
		return hc.CheckHTTPProxy(st.HTTPPort, instanceID)
	}, st.Fatal(), opts)
}

func closeStackWhenContextDone(ctx context.Context, st *stack.Stack) func() {
	if ctx == nil || st == nil {
		return func() {}
	}
	done := make(chan struct{})
	exited := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			_ = st.Close(context.Background())
		case <-done:
		}
	}()
	return func() {
		stopOnce.Do(func() {
			close(done)
		})
		<-exited
	}
}

func runWithProfile(
	ctx context.Context,
	store *config.Store,
	profile config.Profile,
	instances []config.Instance,
	cmdArgs []string,
) error {
	return runWithProfileOptions(ctx, store, profile, instances, cmdArgs, defaultRunTargetOptions())
}

func runWithProfileOptions(
	ctx context.Context,
	store *config.Store,
	profile config.Profile,
	instances []config.Instance,
	cmdArgs []string,
	opts runTargetOptions,
) error {
	hc := manager.HealthClient{Timeout: 1 * time.Second}
	if inst, err := manager.FindReusableInstanceContext(ctx, instances, profile.ID, hc); err != nil {
		return err
	} else if inst != nil {
		err := runWithExistingInstanceOptions(ctx, store, hc, *inst, cmdArgs, opts)
		if err == nil {
			return nil
		}
		if !isCodexResolveError(err) {
			return err
		}
		if opts.Log != nil {
			_, _ = fmt.Fprintln(opts.Log, "reusable proxy instance failed during codex install; starting a fresh proxy instance...")
		}
	}
	// Re-read config from disk to catch instances recorded by other
	// processes after our initial snapshot was loaded.
	if freshCfg, err := store.Load(); err == nil {
		if inst, err := manager.FindReusableInstanceContext(ctx, freshCfg.Instances, profile.ID, hc); err != nil {
			return err
		} else if inst != nil {
			err := runWithExistingInstanceOptions(ctx, store, hc, *inst, cmdArgs, opts)
			if err == nil {
				return nil
			}
			if !isCodexResolveError(err) {
				return err
			}
			if opts.Log != nil {
				_, _ = fmt.Fprintln(opts.Log, "reusable proxy instance failed during codex install; starting a fresh proxy instance...")
			}
		}
	}
	return runWithNewStackOptions(ctx, store, profile, cmdArgs, opts)
}

type runTargetOptions struct {
	Cwd          string
	ExtraEnv     []string
	UseProxy     bool
	Log          io.Writer
	ExecIdentity *execIdentity
	Stdin        io.Reader
	Stdout       io.Writer
	Stderr       io.Writer
	// PreserveTTY keeps the target in the foreground terminal process group for interactive CLIs.
	PreserveTTY    bool
	YoloEnabled    bool
	RequireYolo    bool
	OnYoloFallback func() error
	// PatchInfo, when set, records patch failure on startup crash.
	PatchInfo            *patchRunInfo
	ModelProfileRef      string
	ModelProfileSnapshot modelprofile.Snapshot
}

type codexResolveError struct {
	err error
}

func codexExecutionContextForRun(workingDir string) ([]string, *execIdentity, error) {
	paths, err := resolveEffectiveLaunchPaths("", "", workingDir)
	if err != nil || strings.TrimSpace(paths.CodexDir) == "" {
		return nil, nil, err
	}
	codexHome, err := resolveCodexHomePath(paths.CodexDir, workingDir)
	if err != nil || strings.TrimSpace(codexHome) == "" {
		return nil, nil, err
	}
	return codexHomeEnv(codexHome), paths.ExecIdentity, nil
}

func hasExplicitCodexHomeEnv(extraEnv []string) bool {
	return strings.TrimSpace(envValue(extraEnv, envCodexHome)) != "" ||
		strings.TrimSpace(envValue(extraEnv, codexhistory.EnvCodexDir)) != ""
}

func applyDefaultCodexExecutionContext(opts *runTargetOptions) error {
	if opts == nil {
		return nil
	}
	if opts.ExecIdentity != nil || hasExplicitCodexHomeEnv(opts.ExtraEnv) {
		return nil
	}
	extraEnv, execIdentity, err := codexExecutionContextForRun(opts.Cwd)
	if err != nil {
		return err
	}
	opts.ExtraEnv = append(opts.ExtraEnv, extraEnv...)
	opts.ExecIdentity = execIdentity
	return nil
}

func (e codexResolveError) Error() string {
	if e.err == nil {
		return "codex resolve failed"
	}
	return e.err.Error()
}

func (e codexResolveError) Unwrap() error {
	return e.err
}

func isCodexResolveError(err error) bool {
	var target codexResolveError
	return errors.As(err, &target)
}

// patchRunInfo carries context for recording patch failures.
type patchRunInfo struct {
	OrigBinaryPath string
	OrigSHA256     string
	ConfigDir      string
}

func defaultRunTargetOptions() runTargetOptions {
	return runTargetOptions{UseProxy: true}
}

func prepareYoloCodexCommandForRun(store *config.Store, cmdArgs []string, opts *runTargetOptions) ([]string, func()) {
	if opts == nil || !opts.YoloEnabled || len(cmdArgs) == 0 || !isCodexCommand(cmdArgs[0]) {
		return cmdArgs, nil
	}
	log := opts.Log
	if log == nil {
		log = io.Discard
	}
	var cleanupFns []func()
	addCleanup := func(fn func()) {
		if fn != nil {
			cleanupFns = append(cleanupFns, fn)
		}
	}
	cleanup := func() {
		for i := len(cleanupFns) - 1; i >= 0; i-- {
			cleanupFns[i]()
		}
	}
	codexHome := envValue(opts.ExtraEnv, envCodexHome)
	if strings.TrimSpace(codexHome) == "" {
		codexHome = envValue(opts.ExtraEnv, codexhistory.EnvCodexDir)
	}
	forceFileAuthStore := false
	if strings.TrimSpace(codexHome) != "" {
		authOverride, authErr := prepareYoloAuthOverride(codexHome, opts.ExecIdentity)
		logYoloAuthStatus(log, authOverride, authErr)
		if authOverride != nil {
			forceFileAuthStore = true
			addCleanup(authOverride.Cleanup)
		}
		if cacheBypass, err := cloudgate.InstallYoloCloudRequirementsBypass(codexHome, cmdArgs[0]); err != nil {
			_, _ = fmt.Fprintf(log, "yolo cloud requirements bypass warning: %v\n", err)
		} else if cacheBypass.Installed {
			forceFileAuthStore = true
			addCleanup(func() { _ = cloudgate.RemoveCloudRequirementsCache(codexHome) })
		}
	}
	if store != nil {
		historyDir := filepath.Dir(store.Path())
		patchResult, patchEnv, info, skipped, patchErr := preparePatchedBinaryForLaunch(cmdArgs[0], historyDir, opts.ExecIdentity)
		logYoloPatchStatus(log, patchResult, skipped, patchErr)
		if !skipped && patchResult != nil && patchResult.PatchedBinary != "" {
			cmdArgs = append([]string{}, cmdArgs...)
			cmdArgs[0] = patchResult.PatchedBinary
			opts.ExtraEnv = append(opts.ExtraEnv, patchEnv...)
			opts.PatchInfo = info
			addCleanup(patchResult.Cleanup)
		}
		if opts.OnYoloFallback == nil {
			opts.OnYoloFallback = func() error {
				return persistYoloEnabled(store, false)
			}
		}
	}
	if !commandArgsHaveYolo(cmdArgs[1:]) {
		if yoloFlags := codexYoloLaunchArgsWithOptions(cmdArgs[0], yoloLaunchOptions{ForceFileAuthStore: forceFileAuthStore}); len(yoloFlags) > 0 {
			out := make([]string, 0, 1+len(yoloFlags)+len(cmdArgs[1:]))
			out = append(out, cmdArgs[0])
			out = append(out, yoloFlags...)
			out = append(out, cmdArgs[1:]...)
			cmdArgs = out
		}
	} else if forceFileAuthStore && !commandArgsHaveFileAuthStoreConfig(cmdArgs[1:]) {
		out := make([]string, 0, len(cmdArgs)+2)
		out = append(out, cmdArgs[0], "-c", `cli_auth_credentials_store="file"`)
		out = append(out, cmdArgs[1:]...)
		cmdArgs = out
	}
	if len(cleanupFns) == 0 {
		return cmdArgs, nil
	}
	return cmdArgs, cleanup
}

func commandArgsHaveFileAuthStoreConfig(args []string) bool {
	for i, arg := range args {
		trimmed := strings.TrimSpace(arg)
		if (trimmed == "-c" || trimmed == "--config") && i+1 < len(args) && strings.TrimSpace(args[i+1]) == `cli_auth_credentials_store="file"` {
			return true
		}
	}
	return false
}

func commandArgsHaveYolo(args []string) bool {
	for i, arg := range args {
		switch strings.TrimSpace(arg) {
		case "--yolo", "--dangerously-bypass-approvals-and-sandbox":
			return true
		case "--ask-for-approval":
			if i+1 < len(args) && strings.TrimSpace(args[i+1]) == "never" {
				return true
			}
		default:
			if strings.TrimSpace(arg) == "--ask-for-approval=never" {
				return true
			}
		}
	}
	return false
}

func requireYoloLaunchArgs(cmdArgs []string, opts runTargetOptions) error {
	if !opts.YoloEnabled || !opts.RequireYolo {
		return nil
	}
	if len(cmdArgs) < 2 || !commandArgsHaveYolo(cmdArgs[1:]) {
		return fmt.Errorf("yolo mode is required but no supported Codex yolo launch flag was detected")
	}
	// NOTE: we intentionally do NOT require the binary patch to have matched
	// here. Yolo can also be enabled via the cloud-requirements cache bypass, so
	// "binary patched" is not the right invariant. Whether yolo is actually
	// effective is verified behaviorally by the runtime isYoloFailure fallback
	// (and, later, the dedicated validation probe). PatchResult.MatchCounts is
	// exposed for diagnostics.
	return nil
}

func withProfileInstallEnv(
	ctx context.Context,
	store *config.Store,
	profile config.Profile,
	instances []config.Instance,
	runInstall func([]string) error,
) error {
	installViaProxy := func(proxyURL string) error {
		return runInstall(env.WithProxy(os.Environ(), proxyURL))
	}

	var reuseErr error
	hc := manager.HealthClient{Timeout: 1 * time.Second}
	if inst, err := manager.FindReusableInstanceContext(ctx, instances, profile.ID, hc); err != nil {
		return err
	} else if inst != nil {
		if err := installViaProxy(fmt.Sprintf("http://127.0.0.1:%d", inst.HTTPPort)); err == nil {
			return nil
		} else {
			reuseErr = err
		}
	}
	if store != nil {
		if freshCfg, err := store.Load(); err == nil {
			if inst, err := manager.FindReusableInstanceContext(ctx, freshCfg.Instances, profile.ID, hc); err != nil {
				return err
			} else if inst != nil {
				if err := installViaProxy(fmt.Sprintf("http://127.0.0.1:%d", inst.HTTPPort)); err == nil {
					return nil
				} else if reuseErr == nil {
					reuseErr = err
				}
			}
		}
	}

	instanceID, err := ids.New()
	if err != nil {
		return err
	}
	st, err := stackStart(profile, instanceID, stack.Options{})
	if err != nil {
		if reuseErr != nil {
			return fmt.Errorf("reusable proxy install failed (%v) and fallback stack startup failed: %w", reuseErr, err)
		}
		return err
	}
	stopContextClose := closeStackWhenContextDone(ctx, st)
	defer func() { _ = st.Close(context.Background()) }()
	defer stopContextClose()
	if err := installViaProxy(st.HTTPProxyURL()); err != nil {
		if reuseErr != nil {
			return fmt.Errorf("reusable proxy install failed (%v) and fallback stack install failed: %w", reuseErr, err)
		}
		return err
	}
	return nil
}

func runTargetSupervised(
	ctx context.Context,
	cmdArgs []string,
	proxyURL string,
	healthCheck func() error,
	fatalCh <-chan error,
) error {
	return runTargetSupervisedWithOptions(ctx, cmdArgs, proxyURL, healthCheck, fatalCh, defaultRunTargetOptions())
}

func runTargetSupervisedWithOptions(
	ctx context.Context,
	cmdArgs []string,
	proxyURL string,
	healthCheck func() error,
	fatalCh <-chan error,
	opts runTargetOptions,
) error {
	if len(cmdArgs) == 0 {
		return fmt.Errorf("missing command")
	}
	return runTargetWithFallbackWithOptions(ctx, cmdArgs, proxyURL, healthCheck, fatalCh, opts)
}

func terminateProcess(p *os.Process, grace time.Duration) error {
	if p == nil {
		return nil
	}

	_ = p.Signal(os.Interrupt)

	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if !proc.IsAlive(p.Pid) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return p.Kill()
}

const maxOutputCaptureBytes = 64 * 1024

type limitedBuffer struct {
	buf []byte
	max int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.max <= 0 || len(p) == 0 {
		return len(p), nil
	}
	if len(p) >= b.max {
		b.buf = append(b.buf[:0], p[len(p)-b.max:]...)
		return len(p), nil
	}
	if len(b.buf)+len(p) > b.max {
		overflow := len(b.buf) + len(p) - b.max
		b.buf = append(b.buf[overflow:], p...)
		return len(p), nil
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *limitedBuffer) String() string {
	return string(b.buf)
}

func runTargetWithFallbackWithOptions(
	ctx context.Context,
	cmdArgs []string,
	proxyURL string,
	healthCheck func() error,
	fatalCh <-chan error,
	opts runTargetOptions,
) error {
	yoloRetried := false
	for {
		stdoutBuf := &limitedBuffer{max: maxOutputCaptureBytes}
		stderrBuf := &limitedBuffer{max: maxOutputCaptureBytes}
		err := runTargetOnceWithOptions(ctx, cmdArgs, proxyURL, healthCheck, fatalCh, stdoutBuf, stderrBuf, opts)
		if err == nil {
			// A clean run clears any accumulated patch-failure count so a
			// future transient blip can't push a good binary over the threshold.
			if opts.PatchInfo != nil {
				recordPatchSuccess(opts.PatchInfo)
			}
			return nil
		}
		out := stdoutBuf.String() + stderrBuf.String()
		if opts.YoloEnabled && !yoloRetried && isYoloFailure(err, out) {
			if opts.RequireYolo {
				return fmt.Errorf("yolo mode is required but Codex rejected yolo launch: %w", err)
			}
			yoloRetried = true
			if opts.OnYoloFallback != nil {
				_ = opts.OnYoloFallback()
			}
			cmdArgs = stripYoloArgs(cmdArgs)
			opts.YoloEnabled = false
			continue
		}
		// Record patch failure only if the patched binary itself is broken —
		// isPatchedBinaryStartupFailure already excludes transient kills.
		if opts.PatchInfo != nil && isPatchedBinaryStartupFailure(err, out) {
			recordPatchFailure(opts.PatchInfo, err, out)
		}
		return err
	}
}

func runTargetOnce(
	ctx context.Context,
	cmdArgs []string,
	proxyURL string,
	healthCheck func() error,
	fatalCh <-chan error,
	stdoutBuf io.Writer,
	stderrBuf io.Writer,
) error {
	return runTargetOnceWithOptions(ctx, cmdArgs, proxyURL, healthCheck, fatalCh, stdoutBuf, stderrBuf, defaultRunTargetOptions())
}

func runTargetOnceWithOptions(
	ctx context.Context,
	cmdArgs []string,
	proxyURL string,
	healthCheck func() error,
	fatalCh <-chan error,
	stdoutBuf io.Writer,
	stderrBuf io.Writer,
	opts runTargetOptions,
) error {
	cmd := runTargetCommand(cmdArgs[0], cmdArgs[1:]...)
	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
	}
	envVars := os.Environ()
	if opts.UseProxy {
		envVars = env.WithProxy(envVars, proxyURL)
	}
	if len(opts.ExtraEnv) > 0 {
		envVars = append(envVars, opts.ExtraEnv...)
	}
	envVars = applyExecIdentityEnv(envVars, opts.ExecIdentity)
	guardCleanup := func() {}
	guardCodexPath := ""
	if isCodexCommand(cmdArgs[0]) {
		guardCodexPath = cmdArgs[0]
	} else if opts.PatchInfo != nil && isCodexCommand(opts.PatchInfo.OrigBinaryPath) {
		guardCodexPath = opts.PatchInfo.OrigBinaryPath
	}
	if guardCodexPath != "" {
		guardEnv, cleanup, err := prepareCodexSelfUpdateGuardEnv(ctx, guardCodexPath, envVars, opts.ExecIdentity)
		if err != nil {
			if opts.Log != nil {
				_, _ = fmt.Fprintf(opts.Log, "failed to arm codex self-update guard: %v\n", err)
			}
		} else {
			envVars = guardEnv
			guardCleanup = cleanup
		}
	}
	defer guardCleanup()
	updatedEnv, identityErr := applyExecIdentity(cmd, envVars, opts.ExecIdentity)
	if identityErr != nil {
		return identityErr
	}
	cmd.Env = updatedEnv
	preserveTTY := shouldPreserveTargetTTY(opts)
	useProcessGroup := !preserveTTY
	if useProcessGroup {
		configureTargetProcessGroup(cmd)
	}
	terminateTarget := func() {
		if useProcessGroup {
			_ = terminateTargetCommand(cmd, 2*time.Second)
			return
		}
		_ = terminateProcess(cmd.Process, 2*time.Second)
	}
	if opts.Stdin != nil {
		cmd.Stdin = opts.Stdin
	} else {
		cmd.Stdin = os.Stdin
	}
	if preserveTTY {
		stdout := os.Stdout
		if opts.Stdout != nil {
			cmd.Stdout = opts.Stdout
		} else {
			cmd.Stdout = stdout
		}
		stderr := io.Writer(os.Stderr)
		if opts.Stderr != nil {
			stderr = opts.Stderr
		}
		// Keep stderr capture in interactive mode so yolo fallback and startup
		// failure classification still see early Codex errors. The TUI itself
		// requires stdin/stdout to be terminals; stderr is only mirrored.
		if stderrBuf != nil {
			cmd.Stderr = io.MultiWriter(stderr, stderrBuf)
		} else {
			cmd.Stderr = stderr
		}
	} else {
		stdout := io.Writer(os.Stdout)
		if opts.Stdout != nil {
			stdout = opts.Stdout
		}
		if stdoutBuf != nil {
			cmd.Stdout = io.MultiWriter(stdout, stdoutBuf)
		} else {
			cmd.Stdout = stdout
		}
		stderr := io.Writer(os.Stderr)
		if opts.Stderr != nil {
			stderr = opts.Stderr
		}
		if stderrBuf != nil {
			cmd.Stderr = io.MultiWriter(stderr, stderrBuf)
		} else {
			cmd.Stderr = stderr
		}
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	ticker := time.NewTicker(runTargetHealthCheckInterval)
	defer ticker.Stop()

	failures := 0
	for {
		select {
		case err := <-done:
			return err
		case err := <-fatalCh:
			terminateTarget()
			<-done
			return fmt.Errorf("proxy stack failed; terminated target: %w", err)
		case <-ctx.Done():
			terminateTarget()
			<-done
			return ctx.Err()
		case <-ticker.C:
			if healthCheck == nil {
				continue
			}
			if err := healthCheck(); err != nil {
				failures++
				if failures >= 3 {
					terminateTarget()
					<-done
					return fmt.Errorf("proxy unhealthy; terminated target: %w", err)
				}
				continue
			}
			failures = 0
		}
	}
}

func shouldPreserveTargetTTY(opts runTargetOptions) bool {
	if opts.PreserveTTY {
		return true
	}
	if opts.Stdin != nil || opts.Stdout != nil || opts.Stderr != nil {
		return false
	}
	return isTerminalFile(os.Stdin) && isTerminalFile(os.Stdout) && isTerminalFile(os.Stderr)
}

func isTerminalFile(f *os.File) bool {
	if f == nil {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}

// recordPatchFailure increments the consecutive-failure count for the patched
// binary. Patching is skipped only after PatchFailureThreshold consecutive
// failures (see PatchHistoryStore.IsFailed), so a single crash does not latch.
func recordPatchFailure(info *patchRunInfo, err error, output string) {
	if info == nil || info.ConfigDir == "" {
		return
	}
	phs, phsErr := config.NewPatchHistoryStore(info.ConfigDir)
	if phsErr != nil {
		return
	}
	_ = phs.RecordFailure(info.OrigBinaryPath, info.OrigSHA256, currentProxyVersion(), formatFailureReason(err, output))
}

// recordPatchSuccess marks the patched binary known-good after a clean run: it
// clears any accumulated failure count (so transient failures never reach the
// threshold) and records this codex version as a safe fallback target. The
// codex --version probe runs only on the first clean run of a given hash (or
// after a prior failure), so steady-state runs stay cheap.
func recordPatchSuccess(info *patchRunInfo) {
	if info == nil || info.ConfigDir == "" {
		return
	}
	phs, phsErr := config.NewPatchHistoryStore(info.ConfigDir)
	if phsErr != nil {
		return
	}
	if e, _ := phs.Find(info.OrigBinaryPath, info.OrigSHA256); e != nil && e.KnownGood && !e.Failed && e.FailureCount == 0 {
		return
	}
	_ = phs.RecordKnownGood(info.OrigBinaryPath, info.OrigSHA256, resolveCodexVersion(info.OrigBinaryPath))
}
