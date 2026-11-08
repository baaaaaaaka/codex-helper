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
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/asr"
	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/core/codexagent"
	"github.com/baaaaaaaka/codex-helper/internal/core/delegate"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	"github.com/baaaaaaaka/codex-helper/internal/core/skillcmd"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/tuilease"
	"github.com/baaaaaaaka/codex-helper/internal/core/voice"
)

// `cxp teams run`: the Teams helper core (internal/core/engine).
const (
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
	// managedServiceChild is set for a helper run by the service
	// (--managed-service-child): it paces fast restarts and stands by on an
	// expected refusal instead of exiting (teams_service_backoff.go).
	managedServiceChild bool
	// interval is --interval (0: the production read tiers); once is
	// --once (F-CLI-10: one cycle, then exit).
	interval time.Duration
	once     bool
}

// teamsNewCoreChannelConfig is the Teams adapter configuration. Display
// formulas are drawn with the managed MathJax renderer (NodeMath, as in
// v0.1.20); while it installs, or when Node.js is missing, they show as TeX
// code.
func teamsNewCoreChannelConfig(client *http.Client, authCfg chteams.AuthConfig) chteams.Config {
	return chteams.Config{HTTP: client, Tokens: chteams.NewDelegatedAuth(authCfg, client), TenantID: authCfg.TenantID,
		SharePointHosts: chteams.SharePointHostsFromEnv(os.Getenv("CODEX_HELPER_TEAMS_ALLOWED_SHAREPOINT_HOSTS")),
		Math:            &chteams.NodeMath{}}
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

// teamsBootID identifies this boot of the machine for the lease: the
// kernel boot id on Linux, the boot session UUID on macOS, the boot counter
// on Windows (teamsOSBootID). "" means unknown: the lease then treats every
// predecessor as possibly another machine, so it copies the store instead
// of opening it in place and waits out a crashed owner's lease.
func teamsBootID() string {
	if runtime.GOOS != "linux" {
		return teamsOSBootID()
	}
	raw, _ := os.ReadFile("/proc/sys/kernel/random/boot_id")
	return strings.TrimSpace(string(raw))
}

// teamsCorePIDFile is this host's record of its Codex app-server process
// groups. It is per host: on a shared (NFS) home a helper that takes over
// from another host must not kill pids that only mean something there, and
// two hosts must not overwrite each other's record.
func teamsCorePIDFile(dir, host string) string {
	name := strings.Map(func(r rune) rune {
		if r == '-' || r == '.' || r == '_' || ('0' <= r && r <= '9') || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') {
			return r
		}
		return '_'
	}, strings.TrimSpace(host))
	if name == "" {
		return filepath.Join(dir, "core-app-servers.pid")
	}
	return filepath.Join(dir, "core-app-servers."+name+".pid")
}

// runTeamsNewCore runs the new core until it is stopped. engine.ErrRestart
// becomes exit code 75.
func runTeamsNewCore(cmd *cobra.Command, root *rootOptions, opts teamsNewCoreOptions) error {
	out, errOut := cmd.OutOrStdout(), cmd.ErrOrStderr()
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	seam, err := loadTeamsTestSeam()
	if err != nil {
		return err
	}
	if seam != nil {
		_, _ = fmt.Fprintln(errOut, seam.describe())
		if d := seam.refusalRecheck(); d > 0 {
			teamsRefusalRecheck = d
		}
		defer seam.startCPUProfile(errOut)()
	}

	stateDir, err := appdirs.StateDir()
	if err != nil {
		return err
	}
	path, err := planTeamsCoreStore(stateDir)
	if err != nil {
		return err
	}
	cfgStore, paths, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	host, err := teamsHostname()
	if err != nil {
		host = ""
	}
	if opts.managedServiceChild {
		if err := teamsServiceStartBackoff(ctx, teamsServiceRestartRecordPath(dir, host), errOut); err != nil {
			return nil // stopped while waiting
		}
	}
	for {
		err := serveTeamsNewCore(ctx, out, errOut, root, opts, seam, cfgStore, paths, path, host)
		var stopped errTeamsStoppedEverywhereNow
		if !opts.managedServiceChild || !errors.As(err, &stopped) || ctx.Err() != nil {
			return err
		}
		// Stopped on all machines (`cxp teams stop`, `shutdown`): a
		// service-managed helper stands by in-process, as for a refusal
		// at start, instead of exiting into its supervisor's restarts.
		_, _ = fmt.Fprintf(errOut, "%v\nTeams helper standing by until `cxp teams start`\n", err)
	}
}

// teamsPollIntervalTiming applies `cxp teams run --interval d` (F-CLI-3):
// the control chat, the activity scan and recently active chats are read
// every d; the fastest tier never waits longer than d and the idle tiers
// never read more often than d.
func teamsPollIntervalTiming(t engine.Timing, d time.Duration) engine.Timing {
	d = max(d, t.Tick)
	t.Control, t.ScanInterval, t.Warm = d, d, d
	t.Hot = min(t.Hot, d)
	t.Cool, t.Cold = max(t.Cool, d), max(t.Cold, d)
	return t
}

// teamsOnceCheck is how often `run --once` asks whether its cycle is
// over (only while --once runs).
const teamsOnceCheck = 500 * time.Millisecond

// watchTeamsOnceCycle stops the engine once its first cycle is over
// (engine.CycleDone twice in a row, so work queued in between is seen).
func watchTeamsOnceCycle(ctx context.Context, e *engine.Engine, requests *teamsCLIRequests, stop context.CancelFunc, done chan<- struct{}) {
	t := time.NewTicker(teamsOnceCheck)
	defer t.Stop()
	quiet := 0
	for {
		var now time.Time
		select {
		case <-ctx.Done():
			return
		case now = <-t.C:
		}
		// The requests commands left while no helper ran are run (and
		// finished) before the cycle ends.
		requests.tick(now)
		if requests.pending() || !e.CycleDone() {
			quiet = 0
			continue
		}
		if quiet++; quiet >= 2 {
			close(done)
			stop()
			return
		}
	}
}

// errTeamsStoppedEverywhereNow: the running helper stopped because the
// kill-switch file appeared.
type errTeamsStoppedEverywhereNow struct{ desc string }

func (e errTeamsStoppedEverywhereNow) Error() string {
	return "the Teams helper is disabled and stopped. " + e.desc
}

// teamsDisabledDesc describes the kill-switch file next to the store at
// path: who stopped the helper everywhere and how to start it again.
func teamsDisabledDesc(path string) string {
	desc, ok := teamsStopState(path)
	if !ok {
		desc = corestore.StopRecord{}.Describe(corestore.DisabledPath(filepath.Dir(path)))
	}
	return desc
}

// errTeamsDisabledAtStart refuses a start while the helper is stopped on
// all machines.
func errTeamsDisabledAtStart(path string) error {
	return fmt.Errorf("the Teams helper is disabled and was not started. %s", teamsDisabledDesc(path))
}

// serveTeamsNewCore takes the store and serves until stopped.
func serveTeamsNewCore(ctx context.Context, out, errOut io.Writer, root *rootOptions, opts teamsNewCoreOptions, seam *teamsTestSeam,
	cfgStore *config.Store, paths effectivePaths, path, host string) error {
	dir := filepath.Dir(path)
	// The lease decides which host owns the store; nothing opens the store
	// with SQLite before it is held (teams_lease.go).
	anchor := path
	holder, err := teamsAcquireLease(ctx, anchor, host, teamsLeaseTiming(seam), opts.managedServiceChild, errOut)
	if err != nil {
		if ctx.Err() != nil {
			return nil // stopped while standing by
		}
		if errors.Is(err, lease.ErrDisabled) {
			return errTeamsDisabledAtStart(path)
		}
		return fmt.Errorf("acquire Teams core store: %w", err)
	}
	defer holder.Release() // after the store is closed (deferred below)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		// Superseded or disabled: stop serving, close the store, exit.
		select {
		case <-holder.Lost():
			cancelRun()
		case <-runCtx.Done():
		}
	}()
	plan := holder.Plan()
	if plan.Quiet {
		_, _ = fmt.Fprintf(errOut, "Teams helper taking over generation %d from %s (%s); waiting %s for it to stop\n",
			plan.Gen, firstNonEmptyString(plan.From.Host, "an unfinished takeover"), plan.Why, teamsLeaseTiming(seam).Quiet)
	}
	if err := holder.WaitQuiet(runCtx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return teamsLeaseLostError(err, anchor)
	}
	path = plan.DB
	st, err := openTeamsCoreStore(runCtx, holder, filepath.Join(paths.CodexDir, "sessions"), errOut)
	if err != nil {
		if lerr := holder.Err(); lerr != nil && ctx.Err() == nil {
			return teamsLeaseLostError(lerr, anchor)
		}
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer st.Close()
	if err := teamsAcquireOrWait(runCtx, func() error {
		_, err := st.Acquire(corestore.Ident{Host: host, PID: os.Getpid(), BootID: teamsBootID()})
		return err
	}, opts.managedServiceChild, errOut); err != nil {
		if ctx.Err() != nil {
			return nil // stopped while standing by
		}
		if errors.Is(err, corestore.ErrDisabled) {
			return errTeamsDisabledAtStart(path)
		}
		return fmt.Errorf("acquire Teams core store: %w", err)
	}
	defer st.Release()

	// Graph: the real adapter with the delegated token cache.
	graphLease, err := newTeamsGraphHTTPClientLease(runCtx, root, errOut)
	if err != nil {
		return err
	}
	defer func() { _ = graphLease.Close(context.Background()) }()
	client := teamsFencedHTTPClient(graphLease.Client, st.Fence)
	authCfg, err := chteams.ResolveAuthConfig("")
	if err != nil {
		return fmt.Errorf("Teams auth: %w", err)
	}
	ch, err := chteams.New(teamsNewCoreChannelConfig(client, authCfg))
	if err != nil {
		return err
	}

	// Codex: an explicit --codex-path, else the CXP-managed install (the
	// launch resolves or installs it; SPEC R-24).
	codexBin := strings.TrimSpace(opts.codexPath)
	server := codex.Config{
		Args: []string{"--analytics-default-enabled"},
		Env:  []string{"CODEX_HOME=" + paths.CodexDir},
		// A changed config.toml (e.g. MCP servers) applies from the next
		// turn: the conversation's idle app-server is replaced.
		ConfigFile:       filepath.Join(paths.CodexDir, "config.toml"),
		SharedDir:        dir,
		SecureSharedFile: func(path string) error { return ensurePathOwnedByIdentity(path, paths.ExecIdentity) },
	}
	extra, err := teamsNewCoreCodexArgs(opts.codexArgs, errOut)
	if err != nil {
		return err
	}
	server.Args = append(server.Args, extra...)
	launch, err := newTeamsNewCoreCodexLaunch(runCtx, root, codexBin, errOut)
	if err != nil {
		return err
	}
	defer launch.Close()
	server.Bin, server.Configure = launch.Bin, launch.Configure
	server.Args = append(server.Args, launch.Args...)
	gatewayDone := launch.Done
	ag := codexagent.New(codexagent.Config{
		Server:  server,
		PIDFile: teamsCorePIDFile(dir, host),
		Idle:    10 * time.Minute,
		Poll:    2 * time.Second,
		Log:     out,
	})

	timing, readRate := engine.ProductionTiming(), 0.0
	if seam != nil {
		timing, readRate = seam.timing()
	}
	if opts.interval > 0 {
		timing = teamsPollIntervalTiming(timing, opts.interval)
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
		Host: host, TakeoverWhy: plan.Why,
		SessionsDir:  filepath.Join(paths.CodexDir, "sessions"),
		FilesDir:     filepath.Join(dir, "core-files"),
		ActivityScan: true,
		Registry:     opts.registry,
		Defaults:     teamsConfigDefaults{cfgStore},
		Skills: skillcmd.New(skillcmd.Options{
			ConfigDir: filepath.Dir(paths.ConfigPath),
			CacheDir:  skillsCache,
			CodexDir:  paths.CodexDir,
			HomeDir:   paths.Home,
		}),
	}
	if opts.autoUpdate {
		cfg.Updater = newTeamsNewCoreUpdater(opts.autoUpdateRepo, opts.autoUpdatePrerelease, cfg.Version, errOut)
		cfg.UpdatePrerelease = opts.autoUpdatePrerelease
	}
	if strings.TrimSpace(opts.codexPath) == "" {
		cfg.CodexUpgrader = teamsNewCoreCodexUpgrader{root: root, bin: launch.Bin, identity: launch.Identity, log: errOut}
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
	tr := voice.New(asr.New(opts.asrCommand, asrArgs, teamsASRManagedConfig()), "")
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
	// The state read-only commands show (`cxp teams status`, `teams
	// control`, the bootstrap, upgrades): they never open the store while
	// this helper holds it.
	publisher := newTeamsStatusPublisher(st, filepath.Dir(anchor), holder.Gen(), host, errOut)
	published := make(chan struct{})
	go func() { defer close(published); publisher.run(runCtx) }()
	defer func() { cancelRun(); <-published }() // before the store closes
	stamper := &teamsPollStamper{dir: dir, host: host, version: cfg.Version, lastRead: func() time.Time { return e.Snapshot().LastRead }}
	tuiServer, err := tuilease.Listen(teamsTUIEndpointPath(anchor, host), paths.CodexDir, e, func(path string) error { return ensurePathOwnedByIdentity(path, paths.ExecIdentity) })
	if err != nil {
		return err
	}
	defer tuiServer.Close()
	requests := newTeamsCLIRequests(runCtx, anchor, e, errOut)
	defer func() { cancelRun(); requests.wait(10 * time.Second) }() // before the store closes
	go notifier.watch(runCtx, stamper.tick, requests.tick)
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
	var cycleDone chan struct{}
	if opts.once {
		cycleDone = make(chan struct{})
		go watchTeamsOnceCycle(runCtx, e, requests, cancelRun, cycleDone)
	}
	err = e.Run(runCtx)
	if cycleDone != nil {
		select {
		case <-cycleDone:
			_, _ = fmt.Fprintln(out, "Teams poll cycle complete (--once); exiting")
			return nil
		default:
		}
	}
	if lerr := holder.Err(); lerr != nil && ctx.Err() == nil {
		return teamsLeaseLostError(lerr, anchor)
	}
	if errors.Is(err, corestore.ErrDisabled) {
		return errTeamsStoppedEverywhereNow{desc: teamsDisabledDesc(path)}
	}
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
