package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/env"
	"github.com/baaaaaaaka/codex-helper/internal/ids"
	"github.com/baaaaaaaka/codex-helper/internal/manager"
	"github.com/baaaaaaaka/codex-helper/internal/migration"
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
	runCodexCLIInvocationFn            = runCodexCLIInvocation
	runTargetHealthCheckInterval       = 5 * time.Second
)

func newRunCmd(root *rootOptions) *cobra.Command {
	var modelProfile string
	var agentAutoApprove bool
	var legacyMode bool
	cmd := &cobra.Command{
		Use:   "run [profile] -- [cmd args...]",
		Short: "Run a command using direct mode or an SSH-backed local proxy",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Keep `run` working, but also auto-init when no profiles exist.
			return runLike(cmd, root, true)
		},
	}
	cmd.Flags().StringVar(&modelProfile, "model-profile", "", "Model profile id or name for Codex launches")
	cmd.Flags().BoolVar(&agentAutoApprove, "aaa", false, "Automatically approve Codex agent requests for this run")
	cmd.Flags().BoolVar(&legacyMode, migration.LegacyRunModeFlagName, false, "")
	_ = cmd.Flags().MarkHidden(migration.LegacyRunModeFlagName)
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
	if runOpts.AgentAutoApprove && (len(after) == 0 || !isCodexCommand(after[0])) {
		return fmt.Errorf("--aaa only applies to Codex launches")
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
		if len(after) > 0 && isCodexCommand(after[0]) {
			runOpts.Log = cmd.ErrOrStderr()
			return runCodexCLIInvocationFn(ctx, root, store, &profile, cfg.Instances, after, true, runOpts)
		}
		if isCodexCommand(after[0]) || modelProfileLaunchRequested {
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
		if len(after) > 0 && isCodexCommand(after[0]) {
			runOpts.Log = cmd.ErrOrStderr()
			return runCodexCLIInvocationFn(ctx, root, store, &profile, cfgWithProfile.Instances, after, true, runOpts)
		}
		if isCodexCommand(after[0]) || modelProfileLaunchRequested {
			runOpts.Log = cmd.ErrOrStderr()
			return runWithProfileOptionsFn(ctx, store, profile, cfgWithProfile.Instances, after, runOpts)
		}
		return runWithProfileFn(ctx, store, profile, cfgWithProfile.Instances, after)
	}

	log := cmd.ErrOrStderr()
	opts := runOpts
	opts.UseProxy = false
	opts.Log = log
	if isCodexCommand(after[0]) {
		return runCodexCLIInvocationFn(ctx, root, store, nil, nil, after, false, opts)
	}
	resolvedCmd, err := resolveRunCommandWithInstallOptions(ctx, after, log, codexInstallOptions{})
	if err != nil {
		return err
	}
	return runTargetWithFallbackWithOptionsFn(ctx, resolvedCmd, "", nil, nil, opts)
}

func runTargetOptionsFromRunFlags(cmd *cobra.Command) runTargetOptions {
	opts := defaultRunTargetOptions()
	if cmd == nil {
		return opts
	}
	if flag := cmd.Flags().Lookup("model-profile"); flag != nil {
		opts.ModelProfileRef = strings.TrimSpace(flag.Value.String())
	}
	if flag := cmd.Flags().Lookup("aaa"); flag != nil {
		opts.AgentAutoApprove = strings.EqualFold(strings.TrimSpace(flag.Value.String()), "true")
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
	PreserveTTY          bool
	ModelProfileRef      string
	ModelProfileSnapshot modelprofile.Snapshot
	AgentAutoApprove     bool
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

func defaultRunTargetOptions() runTargetOptions {
	return runTargetOptions{UseProxy: true}
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
	buf       []byte
	max       int
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.max <= 0 || len(p) == 0 {
		if len(p) > 0 {
			b.truncated = true
		}
		return len(p), nil
	}
	if len(p) >= b.max {
		b.buf = append(b.buf[:0], p[len(p)-b.max:]...)
		b.truncated = true
		return len(p), nil
	}
	if len(b.buf)+len(p) > b.max {
		overflow := len(b.buf) + len(p) - b.max
		b.buf = append(b.buf[overflow:], p...)
		b.truncated = true
		return len(p), nil
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *limitedBuffer) Bytes() []byte {
	if len(b.buf) == 0 {
		return nil
	}
	return append([]byte(nil), b.buf...)
}

func (b *limitedBuffer) String() string {
	return string(b.buf)
}

func (b *limitedBuffer) Truncated() bool {
	return b.truncated
}

func runTargetWithFallbackWithOptions(
	ctx context.Context,
	cmdArgs []string,
	proxyURL string,
	healthCheck func() error,
	fatalCh <-chan error,
	opts runTargetOptions,
) error {
	stdoutBuf := &limitedBuffer{max: maxOutputCaptureBytes}
	stderrBuf := &limitedBuffer{max: maxOutputCaptureBytes}
	return runTargetOnceWithOptions(ctx, cmdArgs, proxyURL, healthCheck, fatalCh, stdoutBuf, stderrBuf, opts)
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
		envVars = mergeCLIEnvironment(envVars, opts.ExtraEnv)
	}
	envVars = applyExecIdentityEnv(envVars, opts.ExecIdentity)
	guardCleanup := func() {}
	guardCodexPath := ""
	if isCodexCommand(cmdArgs[0]) {
		guardCodexPath = cmdArgs[0]
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
	cmd.Env = mergeCLIEnvironment(nil, updatedEnv)
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
		// The TUI requires stdin/stdout to be terminals; stderr is mirrored so
		// launch diagnostics remain visible to the caller.
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
