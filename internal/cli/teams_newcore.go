package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/asr"
	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/core/codexagent"
	"github.com/baaaaaaaka/codex-helper/internal/core/delegate"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	"github.com/baaaaaaaka/codex-helper/internal/core/skillcmd"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/voice"
)

// `cxp teams run` with the rebuilt core (internal/core/engine). It is
// selected by the hidden --core=new flag or CODEX_HELPER_TEAMS_CORE=new;
// the default stays the old core.
const (
	envTeamsCore = "CODEX_HELPER_TEAMS_CORE"
	// envTeamsCoreStore names the core.sqlite to use when several exist.
	envTeamsCoreStore = "CODEX_HELPER_TEAMS_CORE_STORE"
	// teamsRestartExitCode asks the service manager to start a new
	// instance on the same state (`helper restart`).
	teamsRestartExitCode = 75
)

// exitCodeError makes Execute exit with code instead of 1.
type exitCodeError struct {
	code int
	err  error
}

func (e *exitCodeError) Error() string { return e.err.Error() }
func (e *exitCodeError) Unwrap() error { return e.err }

// teamsNewCoreSelected reports whether flag (or, when the flag is empty,
// the environment) selects the new core.
func teamsNewCoreSelected(flag string) (bool, error) {
	v := strings.ToLower(strings.TrimSpace(flag))
	if v == "" {
		v = strings.ToLower(strings.TrimSpace(os.Getenv(envTeamsCore)))
	}
	switch v {
	case "", "old":
		return false, nil
	case "new":
		return true, nil
	}
	return false, fmt.Errorf("unknown Teams core %q (use old or new)", v)
}

// teamsEnvSelectsNewCore reports whether a service environment selects the
// new core. The service, its local supervisor and the watchdog use it.
func teamsEnvSelectsNewCore(env map[string]string) bool {
	return strings.EqualFold(strings.TrimSpace(env[envTeamsCore]), "new")
}

// teamsNewCoreOptions are the `teams run` flags the new core uses.
type teamsNewCoreOptions struct {
	codexPath  string
	asrCommand string
	asrArgs    []string
	registry   bool
	// autoUpdate enables helper self-update (--auto-update), from
	// autoUpdateRepo, with pre-releases when autoUpdatePrerelease.
	autoUpdate           bool
	autoUpdateRepo       string
	autoUpdatePrerelease bool
	// codexArgs are the --codex-arg values (teamsNewCoreCodexArgs).
	codexArgs []string
}

// teamsNewCoreOverrides replace production wiring in the e2e build
// (teams_newcore_e2e.go); production leaves teamsNewCoreTestOverrides nil.
type teamsNewCoreOverrides struct {
	// HTTP is the client for Graph and the token endpoint.
	HTTP *http.Client
	// Timing replaces the production timing (and ReadRate the read budget).
	Timing   *engine.Timing
	ReadRate float64
	// Prepare runs on the acquired store before the engine is built.
	Prepare func(st *corestore.Store, dir string) error
}

var teamsNewCoreTestOverrides func(stderr io.Writer) (teamsNewCoreOverrides, error)

var migratedMarkerRE = regexp.MustCompile(`^migrated to (.+) at \S+ by cxp teams migrate`)

// resolveTeamsCoreStorePath finds the core.sqlite to run on under the state
// root. A legacy store that was never migrated is refused: conversion is
// an explicit, offline step (`cxp teams migrate`), never automatic.
func resolveTeamsCoreStorePath(stateDir string) (string, error) {
	if p := strings.TrimSpace(os.Getenv(envTeamsCoreStore)); p != "" {
		return filepath.Abs(p)
	}
	exists := func(p string) bool { _, err := os.Stat(p); return err == nil }
	var found, unmigrated []string
	add := func(p string) {
		for _, f := range found {
			if f == p {
				return
			}
		}
		found = append(found, p)
	}
	for _, dir := range migrate.Candidates(stateDir) {
		if p := filepath.Join(dir, migrate.TargetName); exists(p) {
			add(p)
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, migrate.MarkerName))
		if err != nil {
			unmigrated = append(unmigrated, dir)
			continue
		}
		// Migrated with --out elsewhere: the marker names the target.
		if m := migratedMarkerRE.FindStringSubmatch(strings.TrimSpace(string(raw))); m != nil && exists(m[1]) {
			add(m[1])
		}
	}
	fresh := filepath.Join(stateDir, "teams", migrate.TargetName)
	if exists(fresh) {
		add(fresh)
	}
	switch {
	case len(found) == 1:
		return found[0], nil
	case len(found) > 1:
		return "", fmt.Errorf("several Teams core stores exist (%s); set %s to the one to use", strings.Join(found, ", "), envTeamsCoreStore)
	case len(unmigrated) > 0:
		return "", fmt.Errorf("a Teams store from an earlier helper version is at %s and has not been converted; stop the helper and run `cxp teams migrate --source %s` first", unmigrated[0], unmigrated[0])
	}
	return fresh, nil
}

// teamsNewCoreWorkflowURL reads the Workflow (owner card) webhook from the
// store's configuration ("" = cards off, @mentions instead).
func teamsNewCoreWorkflowURL(st *corestore.Store, storeDir string) (string, error) {
	enabled, _, err := st.Meta(migrate.MetaWorkflowEnabled)
	if err != nil || !strings.EqualFold(strings.TrimSpace(enabled), "true") {
		return "", err
	}
	file, _, err := st.Meta(migrate.MetaWorkflowSecretFile)
	if err != nil || strings.TrimSpace(file) == "" {
		return "", err
	}
	if !filepath.IsAbs(file) {
		file = filepath.Join(storeDir, filepath.Base(file))
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("Workflow webhook secret: %w", err)
	}
	return strings.TrimSpace(string(raw)), nil
}

func teamsBootID() string {
	if runtime.GOOS != "linux" {
		return ""
	}
	raw, _ := os.ReadFile("/proc/sys/kernel/random/boot_id")
	return strings.TrimSpace(string(raw))
}

// runTeamsNewCore runs the new core until it is stopped. engine.ErrRestart
// becomes exit code 75.
func runTeamsNewCore(cmd *cobra.Command, root *rootOptions, opts teamsNewCoreOptions) error {
	out, errOut := cmd.OutOrStdout(), cmd.ErrOrStderr()
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var ov teamsNewCoreOverrides
	if teamsNewCoreTestOverrides != nil {
		var err error
		if ov, err = teamsNewCoreTestOverrides(errOut); err != nil {
			return err
		}
	}

	stateDir, err := appdirs.StateDir()
	if err != nil {
		return err
	}
	path, err := resolveTeamsCoreStorePath(stateDir)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	st, err := corestore.Open(path)
	if err != nil {
		return fmt.Errorf("open Teams core store: %w", err)
	}
	defer st.Close()
	host, _ := os.Hostname()
	if _, err := st.Acquire(corestore.Ident{Host: host, PID: os.Getpid(), BootID: teamsBootID()}); err != nil {
		if errors.Is(err, corestore.ErrDisabled) {
			return fmt.Errorf("the Teams helper is disabled (%s exists next to %s)", corestore.DisabledFile, path)
		}
		return fmt.Errorf("acquire Teams core store: %w", err)
	}
	defer st.Release()
	if ov.Prepare != nil {
		if err := ov.Prepare(st, dir); err != nil {
			return err
		}
	}

	// Graph: the real adapter with the delegated token cache.
	client := ov.HTTP
	if client == nil {
		lease, err := newTeamsGraphHTTPClientLease(ctx, root, errOut)
		if err != nil {
			return err
		}
		defer func() { _ = lease.Close(context.Background()) }()
		client = lease.Client
	}
	authCfg, err := chteams.ResolveAuthConfig("")
	if err != nil {
		return fmt.Errorf("Teams auth: %w", err)
	}
	ch, err := chteams.New(chteams.Config{HTTP: client, Tokens: chteams.NewDelegatedAuth(authCfg, client), TenantID: authCfg.TenantID,
		SharePointHosts: chteams.SharePointHostsFromEnv(os.Getenv("CODEX_HELPER_TEAMS_ALLOWED_SHAREPOINT_HOSTS"))})
	if err != nil {
		return err
	}

	// Codex: an explicit --codex-path, else the discovered installation.
	_, paths, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	codexBin := strings.TrimSpace(opts.codexPath)
	if codexBin == "" {
		if codexBin, err = ensureCodexInstalled(ctx, "", errOut); err != nil {
			return err
		}
	}
	server := codex.Config{
		Bin:  codexBin,
		Args: []string{"--analytics-default-enabled"},
		Env:  []string{"CODEX_HOME=" + paths.CodexDir},
	}
	extra, err := teamsNewCoreCodexArgs(opts.codexArgs, errOut)
	if err != nil {
		return err
	}
	server.Args = append(server.Args, extra...)
	var gatewayDone <-chan error
	if teamsNewCoreTestOverrides == nil {
		launch, err := newTeamsNewCoreCodexLaunch(ctx, root, codexBin, errOut)
		if err != nil {
			return err
		}
		defer launch.Close()
		server.Bin, server.Configure, gatewayDone = launch.Bin, launch.Configure, launch.Done
		server.Args = append(server.Args, launch.Args...)
	}
	ag := codexagent.New(codexagent.Config{
		Server:  server,
		PIDFile: filepath.Join(dir, "core-app-servers.pid"),
		Idle:    10 * time.Minute,
		Poll:    2 * time.Second,
		Log:     out,
	})

	timing, readRate := engine.ProductionTiming(), 0.0
	if ov.Timing != nil {
		timing, readRate = *ov.Timing, ov.ReadRate
	}
	machine := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_MACHINE_LABEL"))
	if machine == "" {
		// `cxp teams migrate` keeps the old hostname override here.
		machine, _, _ = st.Meta("machine_label")
		machine = strings.TrimSpace(machine)
	}
	if machine == "" {
		machine = host
	}
	skillsCache, err := defaultSkillsCacheDir()
	if err != nil {
		return err
	}
	cfg := engine.Config{
		Store: st, Channel: ch, ChannelName: "teams", Agent: ag,
		Log: out, Timing: timing, ReadRate: readRate, Machine: machine, Version: buildVersion(),
		SessionsDir:  filepath.Join(paths.CodexDir, "sessions"),
		FilesDir:     filepath.Join(dir, "core-files"),
		ActivityScan: true,
		Registry:     opts.registry,
		Skills: skillcmd.New(skillcmd.Options{
			ConfigDir: filepath.Dir(paths.ConfigPath),
			CacheDir:  skillsCache,
			CodexDir:  paths.CodexDir,
			HomeDir:   paths.Home,
		}),
	}
	if opts.autoUpdate && teamsNewCoreTestOverrides == nil {
		cfg.Updater = newTeamsNewCoreUpdater(opts.autoUpdateRepo, opts.autoUpdatePrerelease, cfg.Version, errOut)
		cfg.UpdatePrerelease = opts.autoUpdatePrerelease
	}
	if strings.TrimSpace(opts.codexPath) == "" && teamsNewCoreTestOverrides == nil {
		cfg.CodexUpgrader = teamsNewCoreCodexUpgrader{root: root, log: errOut}
	}
	if cfg.Registry {
		// The registry keys the inbox by the account id: read once (and
		// kept), so a start without network fails only the first time.
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := delegate.Configure(rctx, &cfg, ch, authCfg.TenantID, host, os.Getenv("CODEX_HELPER_TEAMS_PROFILE"), "")
		cancel()
		if err != nil {
			_, _ = fmt.Fprintf(errOut, "Machine registry is off until the next start: %v\n", err)
			cfg.Registry = false
		}
	}
	asrArgs, err := teamsASRArgsFromFlagsOrEnv(opts.asrArgs, strings.TrimSpace(opts.asrCommand) != "")
	if err != nil {
		return err
	}
	tr := voice.New(asr.New(opts.asrCommand, asrArgs, teamsNewCoreASRConfig()), "")
	defer func() { _ = tr.Close() }()
	cfg.Transcriber = tr
	notifier := &teamsNewCoreNotifier{st: st, path: path, client: client, log: errOut}
	notifier.url() // applies a pending change and reports a broken setting once
	cfg.Notifier = notifier

	e, err := engine.New(cfg)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "Teams helper (new core) running on %s\n", path)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go notifier.watch(runCtx)
	gatewayFailed := make(chan struct{})
	if gatewayDone != nil {
		go func() {
			select {
			case gerr := <-gatewayDone:
				// Every app-server reaches the model through the gateway:
				// start again on the same state rather than fail turns.
				_, _ = fmt.Fprintf(errOut, "Codex policy gateway stopped: %v; restarting\n", gerr)
				close(gatewayFailed)
				cancelRun()
			case <-runCtx.Done():
			}
		}()
	}
	err = e.Run(runCtx)
	select {
	case <-gatewayFailed:
		if ctx.Err() == nil {
			err = engine.ErrRestart
		}
	default:
	}
	switch {
	case errors.Is(err, engine.ErrRestart):
		return &exitCodeError{code: teamsRestartExitCode, err: errors.New("Teams helper restart requested")}
	case errors.Is(err, context.Canceled):
		return nil
	}
	return err
}

// teamsNewCoreASRConfig maps the managed-ASR environment of the old core.
func teamsNewCoreASRConfig() asr.ManagedConfig {
	old := teamsASRManagedConfigFromEnv()
	return asr.ManagedConfig{
		Device:     old.LlamaDevice,
		ServerPath: old.LlamaBinaryPath,
		ModelPath:  old.LlamaModelPath,
		MMProjPath: old.LlamaMMProjPath,
		FFmpegPath: old.FFmpegPath,
	}
}
