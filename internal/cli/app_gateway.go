package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/appgateway"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/ids"
	"github.com/baaaaaaaka/codex-helper/internal/stack"
)

const (
	appGatewayBackendRetryWindow = 10 * time.Minute
	appGatewayBackendMaxAttempts = 8
	appGatewayBackendMaxBackoff  = time.Minute
	appGatewayLogMaxBytes        = 2 << 20
)

var appGatewayProbeHostNetwork = stack.ProbeHostNetwork

// These seams keep migration tests in-process and deterministic. Production
// defaults are the real state-directory resolver, process discovery, service
// installer and detached daemon launcher; tests must never start a source CXP
// process on the development host.
var (
	appGatewayRegistryDirFn             = appGatewayRegistryDir
	appGatewayDiscoverDesktopProxyPorts = discoverDesktopProxyPorts
	appGatewayEnsureServiceFn           = ensureAppGatewayService
	appGatewayStartDetachedFn           = startAppGatewayDetached
	appGatewayWaitFn                    = waitForAppGateway
)

// appGatewayTicker is the small clock seam used by the daemon tests. The
// production implementation still uses time.Ticker; tests can drive the
// backend-generation monitor without sleeping for a real second.
type appGatewayTicker struct {
	C    <-chan time.Time
	stop func()
}

func (t appGatewayTicker) Stop() {
	if t.stop != nil {
		t.stop()
	}
}

type appGatewayDaemonDeps struct {
	probeHostNetwork func(context.Context, config.Profile) error
	startStack       func(config.Profile, string, stack.Options) (proxyStartedStack, error)
	newFrontend      func(string, int) (*appgateway.Frontend, error)
	now              func() time.Time
	sleep            func(context.Context, time.Duration) error
	newTicker        func(time.Duration) appGatewayTicker
}

func defaultAppGatewayDaemonDeps() appGatewayDaemonDeps {
	return appGatewayDaemonDeps{
		probeHostNetwork: appGatewayProbeHostNetwork,
		startStack:       proxyStartStack,
		newFrontend:      appgateway.NewFrontend,
		now:              time.Now,
		sleep:            appGatewaySleepContext,
		newTicker: func(duration time.Duration) appGatewayTicker {
			ticker := time.NewTicker(duration)
			return appGatewayTicker{C: ticker.C, stop: ticker.Stop}
		},
	}
}

func normalizeAppGatewayDaemonDeps(deps appGatewayDaemonDeps) appGatewayDaemonDeps {
	defaults := defaultAppGatewayDaemonDeps()
	if deps.probeHostNetwork == nil {
		deps.probeHostNetwork = defaults.probeHostNetwork
	}
	if deps.startStack == nil {
		deps.startStack = defaults.startStack
	}
	if deps.newFrontend == nil {
		deps.newFrontend = defaults.newFrontend
	}
	if deps.now == nil {
		deps.now = defaults.now
	}
	if deps.sleep == nil {
		deps.sleep = defaults.sleep
	}
	if deps.newTicker == nil {
		deps.newTicker = defaults.newTicker
	}
	return deps
}

func newProxyAppGatewayCmd(root *rootOptions) *cobra.Command {
	var profileID string
	var stateDir string

	run := &cobra.Command{
		Use:    "run",
		Short:  "Run the stable app gateway (internal)",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			if strings.TrimSpace(stateDir) == "" {
				stateDir, err = appGatewayRegistryDirFn()
				if err != nil {
					return err
				}
			}
			return runAppGatewayDaemon(cmd.Context(), store, profileID, stateDir)
		},
	}
	run.Flags().StringVar(&profileID, "profile-id", "", "Proxy profile id")
	run.Flags().StringVar(&stateDir, "state-dir", "", "App gateway registration directory")
	_ = run.MarkFlagRequired("profile-id")

	parent := &cobra.Command{
		Use:    "app-gateway",
		Short:  "Manage the stable desktop app gateway",
		Hidden: true,
		Args:   cobra.NoArgs,
	}
	parent.AddCommand(run)
	return parent
}

func appGatewayRegistryDir() (string, error) {
	return appdirs.StatePath("app-gateway", "registrations")
}

func appGatewayProfileFingerprint(profile config.Profile) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s\x00%d\x00%s\x00%s\x00%s\x00%d\x00%s", profile.Host, profile.Port, profile.User, profile.Name, profile.ID, profile.RouteTargetPort, profile.RouteTargetHost)
	for _, arg := range profile.SSHArgs {
		_, _ = fmt.Fprintf(h, "\x00%s", arg)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func appGatewayLogPath(stateDir, profileID string) string {
	return filepath.Join(stateDir, "logs", appGatewayProfileFingerprint(config.Profile{ID: profileID})+".log")
}

func recordAppGatewayEvent(registry *appgateway.Registry, reg appgateway.Registration, name, details string) {
	if registry == nil {
		return
	}
	_ = registry.AppendEvent(appgateway.Event{
		RegistrationID:    reg.ID,
		ProfileID:         reg.ProfileID,
		Event:             name,
		HTTPPort:          reg.HTTPPort,
		BackendGeneration: reg.BackendGeneration,
		State:             reg.State,
		Error:             reg.LastError,
		Details:           details,
	})
}

func warnIfAppGatewayPending(log io.Writer, health appgateway.Health) {
	if log == nil || health.OK {
		return
	}
	if health.Recovery == appgateway.StateBlocked {
		_, _ = fmt.Fprintf(log, "warning: stable App Gateway is listening but backend recovery is temporarily blocked; requests will resume automatically after cooldown\n")
		return
	}
	_, _ = fmt.Fprintf(log, "App Gateway is listening while its SSH backend recovers (%s); the client-visible port will remain unchanged\n", strings.TrimSpace(health.Error))
}

func ensureCodexAppGatewayURL(ctx context.Context, store *config.Store, profile config.Profile, legacyInstances []config.Instance, log io.Writer) (string, error) {
	return withProxyStartupLock(ctx, store, func() (string, error) {
		return ensureCodexAppGatewayURLLocked(ctx, store, profile, legacyInstances, log)
	})
}

func ensureCodexAppGatewayURLLocked(ctx context.Context, store *config.Store, profile config.Profile, legacyInstances []config.Instance, log io.Writer) (string, error) {
	if store == nil {
		return "", errors.New("app gateway config store is required")
	}
	// Re-read under the startup election so an upgrade or another app launch
	// cannot leave us with a stale instance snapshot while choosing a port.
	if store != nil {
		freshCfg, loadErr := store.Load()
		if loadErr != nil {
			return "", loadErr
		}
		legacyInstances = freshCfg.Instances
		for _, freshProfile := range freshCfg.Profiles {
			if freshProfile.ID == profile.ID {
				profile = freshProfile
				break
			}
		}
	}
	stateDir, err := appGatewayRegistryDirFn()
	if err != nil {
		return "", err
	}
	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		return "", err
	}
	fingerprint := appGatewayProfileFingerprint(profile)
	// Preserve an already-running legacy session. Its port is retained in the
	// new registration so a later daemon exit can be adopted without a random
	// client-visible port change.
	var legacyPort int
	var discoveredLegacyOwner string
	var occupiedDesktopPort int
	instanceConfig := config.Config{Profiles: []config.Profile{profile}, Instances: legacyInstances}
	for _, inst := range legacyInstances {
		if inst.ProfileID != profile.ID || inst.Kind == config.InstanceKindModelAdapter || inst.HTTPPort <= 0 {
			continue
		}
		hc := healthClientForProxyInstance(instanceConfig, inst, time.Second)
		if err := hc.CheckHTTPProxyContext(ctx, inst.HTTPPort, inst.ID); err == nil {
			legacyPort = inst.HTTPPort
			break
		}
	}
	if legacyPort == 0 {
		if ports, discoverErr := appGatewayDiscoverDesktopProxyPorts(ctx); discoverErr == nil {
			if len(ports) > 1 {
				return "", fmt.Errorf("ChatGPT/Codex exposes multiple proxy ports %v and no profile mapping is available; refusing an ambiguous migration", ports)
			}
			for _, port := range ports {
				// The old config entry may already have been pruned. A health
				// response on a port explicitly referenced by ChatGPT is enough to
				// identify the current local proxy and preserve that port; it is
				// intentionally checked before PortAvailable because the old daemon
				// is expected to be listening during an upgrade.
				if health, probeErr := appgateway.Probe(ctx, port, 500*time.Millisecond); probeErr == nil {
					legacyPort = port
					discoveredLegacyOwner = health.InstanceID
					break
				}
				if appgateway.PortAvailable(port) {
					legacyPort = port
					break
				}
				if occupiedDesktopPort == 0 {
					occupiedDesktopPort = port
				}
			}
		}
	}
	if legacyPort == 0 && occupiedDesktopPort > 0 {
		return "", fmt.Errorf("ChatGPT references occupied proxy port %d but no CXP health identity was returned; refusing to allocate a replacement port", occupiedDesktopPort)
	}
	reg, err := registry.EnsureWithPort(ctx, profile.ID, fingerprint, legacyPort)
	if err != nil {
		return "", err
	}
	recordAppGatewayEvent(registry, reg, "registration-ensured", "cxp app requested the stable gateway")
	if discoveredLegacyOwner != "" && discoveredLegacyOwner != reg.ID {
		// This is the upgrade case in which config.json no longer contains the
		// old instance, but the running desktop process still points at its
		// listener. Install exactly one waiter for the same port and keep the
		// current ChatGPT process untouched until the old owner exits.
		reg.ReplacedInstanceID = discoveredLegacyOwner
		_ = registry.Save(reg)
		if installed, serviceErr := appGatewayEnsureServiceFn(ctx, store, profile, reg, stateDir); installed {
			if serviceErr != nil && log != nil {
				_, _ = fmt.Fprintf(log, "warning: App Gateway service is active but its Start Menu launcher was not updated: %v\n", serviceErr)
			}
		} else {
			if _, startErr := appGatewayStartDetachedFn(store, profile, registry, reg, stateDir, log); startErr != nil {
				return "", fmt.Errorf("start legacy-port takeover watcher: %w", startErr)
			}
		}
		recordAppGatewayEvent(registry, reg, "legacy-port-preserved", fmt.Sprintf("port=%d; discovered owner=%s", legacyPort, discoveredLegacyOwner))
		return fmt.Sprintf("http://127.0.0.1:%d", legacyPort), nil
	}
	if legacyPort > 0 {
		for _, inst := range legacyInstances {
			if inst.ProfileID != profile.ID || inst.Kind == config.InstanceKindModelAdapter || inst.HTTPPort != legacyPort {
				continue
			}
			hc := healthClientForProxyInstance(instanceConfig, inst, time.Second)
			if err := hc.CheckHTTPProxyContext(ctx, inst.HTTPPort, inst.ID); err == nil {
				// Keep the old listener for the current app, but install a single
				// waiting Gateway owner that can take the same port after the old
				// daemon exits. It never opens a second client-visible port.
				reg.ReplacedInstanceID = inst.ID
				_ = registry.Save(reg)
				if installed, serviceErr := appGatewayEnsureServiceFn(ctx, store, profile, reg, stateDir); installed {
					if serviceErr != nil && log != nil {
						_, _ = fmt.Fprintf(log, "warning: App Gateway service is active but its Start Menu launcher was not updated: %v\n", serviceErr)
					}
				} else {
					if _, startErr := appGatewayStartDetachedFn(store, profile, registry, reg, stateDir, log); startErr != nil && log != nil {
						_, _ = fmt.Fprintf(log, "warning: could not install legacy-port takeover watcher: %v\n", startErr)
					}
				}
				if log != nil {
					_, _ = fmt.Fprintf(log, "using existing legacy proxy port %d until the desktop app next restarts; stable App Gateway takeover is pending\n", inst.HTTPPort)
				}
				recordAppGatewayEvent(registry, reg, "legacy-port-preserved", fmt.Sprintf("port=%d", inst.HTTPPort))
				return fmt.Sprintf("http://127.0.0.1:%d", inst.HTTPPort), nil
			}
		}
	}

	// A gateway health response, including a 503 while DNS/VPN is recovering,
	// proves that the stable listener belongs to this registration. Never start
	// a second gateway just because the backend is temporarily unavailable.
	if health, probeErr := appgateway.Probe(ctx, reg.HTTPPort, time.Second); probeErr == nil {
		warnIfAppGatewayPending(log, health)
		if health.InstanceID != reg.ID {
			for _, inst := range legacyInstances {
				if inst.ProfileID == profile.ID && inst.ID == health.InstanceID {
					reg.ReplacedInstanceID = inst.ID
					_ = registry.Save(reg)
					if log != nil {
						_, _ = fmt.Fprintf(log, "using existing legacy proxy port %d until the desktop app next restarts; stable App Gateway migration is pending\n", inst.HTTPPort)
					}
					recordAppGatewayEvent(registry, reg, "legacy-port-preserved", fmt.Sprintf("port=%d; health identity=%s", reg.HTTPPort, health.InstanceID))
					return fmt.Sprintf("http://127.0.0.1:%d", reg.HTTPPort), nil
				}
			}
			return "", fmt.Errorf("stable app gateway port %d is owned by %q, want %q", reg.HTTPPort, health.InstanceID, reg.ID)
		}
		return fmt.Sprintf("http://127.0.0.1:%d", reg.HTTPPort), nil
	}

	// A healthy legacy daemon is left untouched. It cannot share its listener
	// with the new Frontend; a later launch or daemon exit will complete the
	// handoff. Returning its URL preserves a running ChatGPT session instead of
	// forcing a visible restart during an upgrade.
	for _, inst := range legacyInstances {
		if inst.ProfileID != profile.ID || inst.Kind == config.InstanceKindModelAdapter || inst.HTTPPort <= 0 {
			continue
		}
		hc := healthClientForProxyInstance(instanceConfig, inst, time.Second)
		if err := hc.CheckHTTPProxyContext(ctx, inst.HTTPPort, inst.ID); err == nil {
			reg.ReplacedInstanceID = inst.ID
			_ = registry.Save(reg)
			if log != nil {
				_, _ = fmt.Fprintf(log, "using existing legacy proxy port %d until the desktop app next restarts; stable App Gateway migration is pending\n", inst.HTTPPort)
			}
			recordAppGatewayEvent(registry, reg, "legacy-port-preserved", fmt.Sprintf("port=%d", inst.HTTPPort))
			return fmt.Sprintf("http://127.0.0.1:%d", inst.HTTPPort), nil
		}
	}

	return ensureAppGatewayProcess(ctx, store, profile, registry, reg, stateDir, log)
}

func ensureAppGatewayProcess(ctx context.Context, store *config.Store, profile config.Profile, registry *appgateway.Registry, reg appgateway.Registration, stateDir string, log io.Writer) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if health, err := appgateway.Probe(ctx, reg.HTTPPort, 300*time.Millisecond); err == nil {
		warnIfAppGatewayPending(log, health)
		if health.InstanceID != reg.ID {
			return "", fmt.Errorf("stable app gateway port %d is owned by %q, want %q", reg.HTTPPort, health.InstanceID, reg.ID)
		}
		return fmt.Sprintf("http://127.0.0.1:%d", reg.HTTPPort), nil
	}
	if installed, serviceErr := appGatewayEnsureServiceFn(ctx, store, profile, reg, stateDir); installed {
		if err := appGatewayWaitFn(ctx, reg.HTTPPort, reg.ID); err != nil {
			return "", err
		}
		if health, probeErr := appgateway.Probe(ctx, reg.HTTPPort, 500*time.Millisecond); probeErr == nil {
			warnIfAppGatewayPending(log, health)
		}
		return fmt.Sprintf("http://127.0.0.1:%d", reg.HTTPPort), nil
	} else if serviceErr != nil && log != nil {
		_, _ = fmt.Fprintf(log, "warning: user-level App Gateway service was not installed; using a detached process for this session: %v\n", serviceErr)
	}

	if log != nil {
		_, _ = fmt.Fprintf(log, "starting stable App Gateway for profile %s on port %d...\n", profile.ID, reg.HTTPPort)
	}
	recordAppGatewayEvent(registry, reg, "frontend-start-requested", "stable listener was not already healthy")
	_, err := appGatewayStartDetachedFn(store, profile, registry, reg, stateDir, log)
	if err != nil {
		return "", err
	}
	if err := appGatewayWaitFn(ctx, reg.HTTPPort, reg.ID); err != nil {
		return "", err
	}
	if health, probeErr := appgateway.Probe(ctx, reg.HTTPPort, 500*time.Millisecond); probeErr == nil {
		warnIfAppGatewayPending(log, health)
	}
	return fmt.Sprintf("http://127.0.0.1:%d", reg.HTTPPort), nil
}

func startAppGatewayDetached(store *config.Store, profile config.Profile, registry *appgateway.Registry, reg appgateway.Registration, stateDir string, log io.Writer) (*detachedProcess, error) {
	exe, err := proxyExecutable()
	if err != nil {
		return nil, err
	}
	resolved, err := helperpath.StableRunnablePathFromSources(exe, restartArgv0(), helperpath.Options{})
	if err != nil {
		return nil, err
	}
	args := []string{
		"--config", store.Path(),
		"proxy", "app-gateway", "run",
		"--profile-id", profile.ID,
		"--state-dir", stateDir,
	}
	cmd := proxyCommand(resolved.Path, args...)
	cmd.Stdin = nil
	logPath := appGatewayLogPath(stateDir, profile.ID)
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return nil, fmt.Errorf("create app gateway log directory: %w", err)
	}
	if err := rotateAppGatewayLog(logPath); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open app gateway log: %w", err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	configureTeamsServiceDetachedCommand(cmd)
	detached, err := startDetachedProcess(cmd)
	_ = logFile.Close()
	if err != nil {
		return nil, fmt.Errorf("start app gateway: %w", err)
	}
	// The child records its PID and owner generation only after it acquires the
	// lifetime lease and binds the stable listener. Writing a stale parent copy
	// here would race that commit and erase the generation used to fence a
	// duplicate scheduler invocation.
	return detached, nil
}

func rotateAppGatewayLog(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat app gateway log: %w", err)
	}
	if info.Size() < appGatewayLogMaxBytes {
		return nil
	}
	backup := path + ".1"
	_ = os.Remove(backup)
	if err := os.Rename(path, backup); err != nil {
		return fmt.Errorf("rotate app gateway log: %w", err)
	}
	return nil
}

func waitForAppGateway(ctx context.Context, port int, id string) error {
	deadline := time.NewTimer(codexAppProxyReadyTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(codexAppProxyPollInterval)
	defer ticker.Stop()
	for {
		if health, err := appgateway.Probe(ctx, port, 500*time.Millisecond); err == nil {
			if health.InstanceID != id {
				return fmt.Errorf("app gateway port %d is owned by %q, want %q", port, health.InstanceID, id)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("app gateway %s did not become ready within %s", id, codexAppProxyReadyTimeout)
		case <-ticker.C:
		}
	}
}

func findProfile(cfg config.Config, profileID string) (config.Profile, error) {
	for _, profile := range cfg.Profiles {
		if profile.ID == profileID {
			return profile, nil
		}
	}
	return config.Profile{}, fmt.Errorf("profile %q not found", profileID)
}

func runAppGatewayDaemon(parentCtx context.Context, store *config.Store, profileID, stateDir string) error {
	return runAppGatewayDaemonWithDeps(parentCtx, store, profileID, stateDir, defaultAppGatewayDaemonDeps())
}

func runAppGatewayDaemonWithDeps(parentCtx context.Context, store *config.Store, profileID, stateDir string, deps appGatewayDaemonDeps) error {
	deps = normalizeAppGatewayDaemonDeps(deps)
	if store == nil {
		return errors.New("app gateway config store is required")
	}
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, stop := withSignalContext(parentCtx)
	defer stop()
	cfg, err := store.Load()
	if err != nil {
		return err
	}
	profile, err := findProfile(cfg, strings.TrimSpace(profileID))
	if err != nil {
		return err
	}
	if strings.TrimSpace(stateDir) == "" {
		return errors.New("app gateway state directory is required")
	}
	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		return err
	}
	reg, err := registry.Ensure(ctx, profile.ID, appGatewayProfileFingerprint(profile))
	if err != nil {
		return err
	}
	lease, err := registry.AcquireLease(ctx, profile.ID)
	if err != nil {
		return err
	}
	defer lease()

	var frontend *appgateway.Frontend
	for {
		frontend, err = deps.newFrontend(reg.ID, reg.HTTPPort)
		if err == nil {
			break
		}
		if !errors.Is(err, appgateway.ErrPortInUse) {
			return err
		}
		// A legacy ChatGPT/daemon pair may still own the port. Keep one
		// migration process alive and retry slowly; do not let a service
		// supervisor turn this into a bind/restart storm.
		reg.MigrationStage = "waiting-legacy-port"
		reg.State = appgateway.StatePending
		reg.LastError = err.Error()
		_ = registry.Save(reg)
		recordAppGatewayEvent(registry, reg, "waiting-legacy-port", "stable port is still owned by an older listener")
		if err := deps.sleep(ctx, 30*time.Second); err != nil {
			return nil
		}
		if latest, loadErr := registry.Load(profile.ID); loadErr == nil {
			reg = latest
		}
	}
	defer frontend.Close(context.Background())
	reg.OwnerPID = os.Getpid()
	reg.OwnerStartedAt = deps.now()
	reg.OwnerExecutable, _ = helperpath.RawExecutable()
	reg.OwnerCommandLine = strings.Join(os.Args, " ")
	reg.OwnerGeneration, _ = ids.New()
	reg.State = appgateway.StatePending
	if err := registry.Save(reg); err != nil {
		return err
	}
	recordAppGatewayEvent(registry, reg, "frontend-started", "stable listener acquired")
	defer func() {
		reg.OwnerPID = 0
		reg.OwnerExecutable = ""
		reg.OwnerCommandLine = ""
		reg.State = appgateway.StatePending
		reg.RecoveryBlocked = false
		reg.LastError = "gateway stopped"
		_ = registry.Save(reg)
		recordAppGatewayEvent(registry, reg, "frontend-stopped", "daemon exited")
	}()

	attempts := 0
	windowAt := deps.now()
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		now := deps.now()
		if now.Sub(windowAt) > appGatewayBackendRetryWindow {
			attempts = 0
			windowAt = now
		}
		if attempts >= appGatewayBackendMaxAttempts {
			reg.RecoveryBlocked = true
			reg.State = appgateway.StateBlocked
			reg.LastError = "backend recovery cooldown"
			_ = registry.Save(reg)
			recordAppGatewayEvent(registry, reg, "recovery-blocked", "bounded backend recovery cooldown; frontend remains listening")
			if err := deps.sleep(ctx, appGatewayBackendMaxBackoff); err != nil {
				return nil
			}
			attempts = 0
			windowAt = deps.now()
			reg.RecoveryBlocked = false
			reg.State = appgateway.StatePending
			continue
		}
		attempts++
		reg.RecoveryWindowAt = windowAt
		probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
		hostErr := deps.probeHostNetwork(probeCtx, profile)
		probeCancel()
		if hostErr != nil {
			_ = frontend.SetUnavailable(ctx, hostErr)
			reg.State = appgateway.StatePending
			reg.LastError = hostErr.Error()
			reg.RecoveryAttempts = attempts
			_ = registry.Save(reg)
			recordAppGatewayEvent(registry, reg, "network-not-ready", hostErr.Error())
			if err := deps.sleep(ctx, appGatewayBackoff(attempts)); err != nil {
				return nil
			}
			continue
		}

		child, startErr := deps.startStack(profile, reg.ID, stack.Options{
			HTTPListenAddr: "127.0.0.1:0",
			// The controller, rather than the child stack, owns the retry budget.
			// Keep one child conservative so a DNS/VPN outage cannot create an SSH
			// restart storm inside a single controller attempt.
			MaxRestarts:                 3,
			RestartBackoff:              2 * time.Second,
			MaxRequestFailureRecoveries: 3,
			RequestFailureConfirmations: 2,
			RestartWindow:               appGatewayBackendRetryWindow,
			RequestFailureWindow:        appGatewayBackendRetryWindow,
		})
		if startErr != nil {
			_ = frontend.SetUnavailable(ctx, startErr)
			reg.State = appgateway.StatePending
			reg.LastError = startErr.Error()
			reg.RecoveryAttempts = attempts
			_ = registry.Save(reg)
			recordAppGatewayEvent(registry, reg, "backend-start-failed", startErr.Error())
			if err := deps.sleep(ctx, appGatewayBackoff(attempts)); err != nil {
				return nil
			}
			continue
		}

		generation, _ := ids.New()
		activeSocks := child.currentSocksPort()
		if activeSocks <= 0 {
			_ = child.close(context.Background())
			reg.LastError = "backend reported no SOCKS port"
			_ = registry.Save(reg)
			recordAppGatewayEvent(registry, reg, "backend-no-port", "SSH worker did not report a SOCKS port")
			if sleepErr := deps.sleep(ctx, appGatewayBackoff(attempts)); sleepErr != nil {
				return nil
			}
			continue
		}
		if err := frontend.SwapBackend(ctx, fmt.Sprintf("127.0.0.1:%d", activeSocks), generation); err != nil {
			_ = child.close(context.Background())
			reg.LastError = err.Error()
			_ = registry.Save(reg)
			if sleepErr := deps.sleep(ctx, appGatewayBackoff(attempts)); sleepErr != nil {
				return nil
			}
			continue
		}
		reg.State = appgateway.StateReady
		reg.RecoveryBlocked = false
		reg.RecoveryAttempts = 0
		reg.LastError = ""
		reg.BackendGeneration = generation
		reg.LastReadyAt = deps.now()
		_ = registry.Save(reg)
		recordAppGatewayEvent(registry, reg, "backend-ready", fmt.Sprintf("socks-port=%d", activeSocks))

		ticker := deps.newTicker(time.Second)
		childDone := false
		for !childDone {
			select {
			case err := <-child.fatalCh:
				if err == nil {
					err = errors.New("backend stack exited")
				}
				_ = frontend.SetUnavailable(ctx, err)
				reg.State = appgateway.StatePending
				reg.LastError = err.Error()
				_ = registry.Save(reg)
				recordAppGatewayEvent(registry, reg, "backend-exited", err.Error())
				childDone = true
			case <-ctx.Done():
				_ = child.close(context.Background())
				ticker.Stop()
				return nil
			case <-ticker.C:
				if current := child.currentSocksPort(); current > 0 && current != activeSocks {
					nextGeneration, _ := ids.New()
					if err := frontend.SwapBackend(ctx, fmt.Sprintf("127.0.0.1:%d", current), nextGeneration); err != nil {
						_ = frontend.SetUnavailable(ctx, err)
						reg.State = appgateway.StatePending
						reg.LastError = err.Error()
						_ = registry.Save(reg)
						recordAppGatewayEvent(registry, reg, "backend-swap-failed", err.Error())
						childDone = true
					} else {
						activeSocks = current
						generation = nextGeneration
						reg.BackendGeneration = generation
						_ = registry.Save(reg)
						recordAppGatewayEvent(registry, reg, "backend-swapped", fmt.Sprintf("socks-port=%d", current))
					}
				}
			}
		}
		ticker.Stop()
		_ = child.close(context.Background())
		if err := deps.sleep(ctx, appGatewayBackoff(attempts)); err != nil {
			return nil
		}
	}
}

func appGatewayBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return time.Second
	}
	d := time.Duration(1<<minInt(attempt-1, 6)) * time.Second
	if d > appGatewayBackendMaxBackoff {
		return appGatewayBackendMaxBackoff
	}
	return d
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func appGatewaySleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
