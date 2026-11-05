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
	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	"github.com/baaaaaaaka/codex-helper/internal/core/skillcmd"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
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

var migratedMarkerRE = regexp.MustCompile(`^migrated to (.+) at \S+ by cxp teams migrate`)

// errTeamsStoreNotConverted: the store under the state root is from an
// earlier helper version and is converted when the helper next starts.
type errTeamsStoreNotConverted struct{ source string }

func (e errTeamsStoreNotConverted) Error() string {
	return fmt.Sprintf("the Teams store at %s is from an earlier helper version; the helper converts it when it starts (`cxp teams run` or the background service)", e.source)
}

// resolveTeamsCoreStorePath finds the core.sqlite the helper runs on, for
// commands that only read or configure it: a store still to be converted
// is an errTeamsStoreNotConverted.
func resolveTeamsCoreStorePath(stateDir string) (string, error) {
	path, from, err := planTeamsCoreStore(stateDir)
	if err == nil && from != "" {
		return "", errTeamsStoreNotConverted{source: from}
	}
	return path, err
}

// planTeamsCoreStore finds the core.sqlite to run on under the state root.
// migrateFrom is set when no converted store exists yet and a single
// supported older store (v0.1.20 or rc.59) does: the helper converts it
// at its first start into path (next to it). Stores of an intermediate
// release, or several candidates, are refused with what to do.
func planTeamsCoreStore(stateDir string) (path, migrateFrom string, err error) {
	if p := strings.TrimSpace(os.Getenv(envTeamsCoreStore)); p != "" {
		p, err := filepath.Abs(p)
		return p, "", err
	}
	exists := func(p string) bool { _, err := os.Stat(p); return err == nil }
	var found []string
	add := func(p string) {
		for _, f := range found {
			if f == p {
				return
			}
		}
		found = append(found, p)
	}
	for _, dir := range migrate.Candidates(stateDir) {
		// A lease file means the store is managed there even while the
		// file in use has another name (a takeover's copy).
		if p := filepath.Join(dir, migrate.TargetName); exists(p) || exists(lease.Path(p)) {
			add(p)
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, migrate.MarkerName))
		if err != nil {
			continue
		}
		// Migrated with --out elsewhere: the marker names the target.
		if m := migratedMarkerRE.FindStringSubmatch(strings.TrimSpace(string(raw))); m != nil && exists(m[1]) {
			add(m[1])
		}
	}
	fresh := filepath.Join(stateDir, "teams", migrate.TargetName)
	if exists(fresh) || exists(lease.Path(fresh)) {
		add(fresh)
	}
	switch {
	case len(found) == 1:
		return found[0], "", nil
	case len(found) > 1:
		return "", "", fmt.Errorf("several Teams core stores exist (%s); set %s to the one to use", strings.Join(found, ", "), envTeamsCoreStore)
	}
	src, err := migrate.ChooseSource(migrate.Pending(stateDir))
	if err != nil {
		return "", "", err
	}
	if src != "" {
		return filepath.Join(src, migrate.TargetName), src, nil
	}
	return fresh, "", nil
}

// teamsMigrateRetry is the first wait after a failed conversion at start;
// it doubles up to teamsMigrateRetryMax. The helper waits in-process so a
// failing conversion of a large store is never repeated at the service
// manager's restart rate.
var (
	teamsMigrateRetry    = 30 * time.Second
	teamsMigrateRetryMax = 30 * time.Minute
)

// migrateTeamsStoreAtStart converts the older store at src into
// src/core.sqlite (read-only on the old store, which stays as the backup;
// the result is renamed into place only when complete). The conversion
// leaves a summary in the new store that the engine posts once.
func migrateTeamsStoreAtStart(ctx context.Context, src, sessionsDir string, log io.Writer) error {
	wait := teamsMigrateRetry
	for {
		if _, err := os.Stat(filepath.Join(src, migrate.TargetName)); err == nil {
			return nil // converted meanwhile (another start, or `cxp teams migrate`)
		}
		_, _ = fmt.Fprintf(log, "Converting the Teams store at %s (first start of this version; the old store is kept unchanged)\n", src)
		started := time.Now()
		rep, err := migrate.Run(migrate.Options{Source: src, CodexSessions: sessionsDir, CountBacklog: true})
		if err == nil {
			_, _ = fmt.Fprintf(log, "Teams store converted in %s; report: %s\n", time.Since(started).Round(time.Second), filepath.Join(src, migrate.ReportName))
			return nil
		}
		if rep != nil {
			err = fmt.Errorf("%w (report: %s)", err, filepath.Join(src, migrate.ReportName))
		}
		_, _ = fmt.Fprintf(log, "Teams store conversion failed: %v; retrying in %s\n", err, wait)
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
		if wait *= 2; wait > teamsMigrateRetryMax {
			wait = teamsMigrateRetryMax
		}
	}
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
	}

	stateDir, err := appdirs.StateDir()
	if err != nil {
		return err
	}
	path, migrateFrom, err := planTeamsCoreStore(stateDir)
	if err != nil {
		return err
	}
	_, paths, err := newRootStore(root, "")
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
	// The lease decides which host owns the store; nothing opens the store
	// with SQLite before it is held (teams_lease.go).
	anchor := path
	holder, err := teamsAcquireLease(ctx, anchor, host, teamsLeaseTiming(seam), opts.managedServiceChild, errOut)
	if err != nil {
		if ctx.Err() != nil {
			return nil // stopped while standing by
		}
		if errors.Is(err, lease.ErrDisabled) {
			return fmt.Errorf("the Teams helper is disabled (%s exists next to %s)", corestore.DisabledFile, path)
		}
		return fmt.Errorf("acquire Teams core store: %w", err)
	}
	defer holder.Release() // after the store is closed (deferred below)
	plan := holder.Plan()
	if plan.Quiet {
		_, _ = fmt.Fprintf(errOut, "Teams helper taking over generation %d from %s (%s); waiting %s for it to stop\n",
			plan.Gen, firstNonEmptyString(plan.From.Host, "an unfinished takeover"), plan.Why, teamsLeaseTiming(seam).Quiet)
	}
	if err := holder.WaitQuiet(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return teamsLeaseLostError(err, anchor)
	}
	switch {
	case !plan.InPlace:
		_, _ = fmt.Fprintf(errOut, "Teams helper copying the store %s to %s\n", plan.Src, plan.DB)
		if err := corestore.CopySnapshot(plan.Src, plan.DB); err != nil {
			return fmt.Errorf("take over the Teams core store: %w", err)
		}
	case migrateFrom != "" && plan.FromGen == 0:
		if err := migrateTeamsStoreAtStart(ctx, migrateFrom, filepath.Join(paths.CodexDir, "sessions"), errOut); err != nil {
			return err
		}
	}
	if err := holder.Activate(); err != nil {
		return teamsLeaseLostError(err, anchor)
	}
	path = plan.DB
	st, err := corestore.OpenWith(path, corestore.Options{Lease: holder})
	if err != nil {
		return fmt.Errorf("open Teams core store: %w", err)
	}
	defer st.Close()
	if err := teamsAcquireOrWait(ctx, func() error {
		_, err := st.Acquire(corestore.Ident{Host: host, PID: os.Getpid(), BootID: teamsBootID()})
		return err
	}, opts.managedServiceChild, errOut); err != nil {
		if ctx.Err() != nil {
			return nil // stopped while standing by
		}
		if errors.Is(err, corestore.ErrDisabled) {
			return fmt.Errorf("the Teams helper is disabled (%s exists next to %s)", corestore.DisabledFile, path)
		}
		return fmt.Errorf("acquire Teams core store: %w", err)
	}
	defer st.Release()

	// Graph: the real adapter with the delegated token cache.
	graphLease, err := newTeamsGraphHTTPClientLease(ctx, root, errOut)
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

	// Codex: an explicit --codex-path, else the discovered installation.
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
	launch, err := newTeamsNewCoreCodexLaunch(ctx, root, codexBin, errOut)
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
	if lerr := holder.Err(); lerr != nil && ctx.Err() == nil {
		return teamsLeaseLostError(lerr, anchor)
	}
	if errors.Is(err, corestore.ErrDisabled) {
		return fmt.Errorf("the Teams helper is disabled (%s exists next to %s); stopped", corestore.DisabledFile, path)
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
