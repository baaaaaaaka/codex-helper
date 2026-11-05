package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/gofrs/flock"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

var teamsUpgradePollInterval = 500 * time.Millisecond

type teamsUpgradeServiceRestartMode int

const (
	teamsUpgradeRestartNone teamsUpgradeServiceRestartMode = iota
	teamsUpgradeRestartImmediate
	teamsUpgradeRestartDelayed
)

type teamsUpgradeFinishOptions struct {
	Success            bool
	ServiceRestart     teamsUpgradeServiceRestartMode
	InstallPath        string
	PendingReplacePath string
}

type teamsUpgradeFinalizer func(context.Context, teamsUpgradeFinishOptions) error

var teamsServiceStartDetached = defaultTeamsServiceStartDetached

// ensureTeamsIdleBeforeCodexUpgrade refuses `teams run --upgrade-codex`
// while a helper owns the store (its Codex app-servers use the binary).
func ensureTeamsIdleBeforeCodexUpgrade(context.Context) error {
	live, err := teamsHelperOwnerLive(time.Now())
	if err != nil {
		return err
	}
	if live {
		return fmt.Errorf("the Teams helper is already running; stop it (`cxp teams service stop`) before using --upgrade-codex")
	}
	return nil
}

// teamsHelperOwnerLive reports whether a helper holds the store with a
// fresh heartbeat (read-only).
func teamsHelperOwnerLive(now time.Time) (bool, error) {
	view, err := teamsReadStoreView()
	if err != nil {
		var notConverted *errTeamsStoreNotConverted
		if errors.As(err, &notConverted) {
			return false, nil
		}
		return false, err
	}
	if view.Info.Exists {
		return view.Info.Live(now, lease.ProductionStale), nil
	}
	if !view.exists() {
		return false, nil
	}
	st, err := view.openReadOnly()
	if err != nil {
		return false, err
	}
	defer st.Close()
	o, ok, err := st.OwnerInfo()
	if err != nil || !ok || o.HeartbeatAt == 0 {
		return false, err
	}
	return now.Sub(time.UnixMilli(o.HeartbeatAt)) < corestore.OwnerStaleAfter, nil
}

// teamsRunningCodexWork counts the requests the helper is running (read
// from its store; 0 without a store).
func teamsRunningCodexWork() (int, error) {
	view, err := teamsReadStoreView()
	if err != nil {
		var notConverted *errTeamsStoreNotConverted
		if errors.As(err, &notConverted) {
			return 0, nil
		}
		return 0, err
	}
	if !view.exists() || view.Remote != "" {
		return 0, nil // another host's helper runs that work, not this host's service
	}
	st, err := view.openReadOnly()
	if err != nil {
		return 0, err
	}
	defer st.Close()
	tc, err := st.TurnCounts()
	if err != nil {
		return 0, err
	}
	return tc[corestore.TurnStarting] + tc[corestore.TurnRunning], nil
}

// prepareTeamsForHelperUpgrade stops the background service for a local
// helper upgrade and returns the finalizer that starts it again. Running
// Codex work is never interrupted by an upgrade: it waits (up to timeout)
// until the helper runs no request before stopping the service.
func prepareTeamsForHelperUpgrade(ctx context.Context, in io.Reader, out io.Writer, timeout time.Duration) (teamsUpgradeFinalizer, error) {
	serviceWasActive, err := teamsUpgradeServiceActive(ctx)
	if err != nil || !serviceWasActive {
		return nil, err
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	waiting := false
	for {
		n, err := teamsRunningCodexWork()
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		if !waiting && out != nil {
			_, _ = fmt.Fprintf(out, "Waiting for %d running Teams request(s) to finish before upgrading...\n", n)
			waiting = true
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("the Teams helper is still running %d request(s); retry the upgrade later, or send `helper update` in the Teams control chat to update when they finish", n)
		}
		if err := sleepContext(ctx, teamsUpgradePollInterval); err != nil {
			return nil, err
		}
	}
	return stopTeamsServiceForHelperUpgrade(ctx, in, out, nil)
}

func stopTeamsServiceForHelperUpgrade(ctx context.Context, in io.Reader, out io.Writer, beforeRestart func(context.Context, bool) error) (teamsUpgradeFinalizer, error) {
	if out != nil {
		_, _ = fmt.Fprintln(out, "Stopping Teams service before upgrade...")
	}
	backend, backendErr := teamsServiceBackendForCurrentPlatform()
	if backendErr != nil {
		return nil, backendErr
	}
	if err := stopTeamsServiceRaw(ctx, backend); err != nil {
		return nil, err
	}
	if _, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
		spec, specErr := buildTeamsServiceSpec()
		if specErr != nil {
			return nil, specErr
		}
		if _, retireErr := teamsServiceRetireLocalDuplicateProcesses(ctx, spec); retireErr != nil {
			return nil, fmt.Errorf("could not stop old local Teams helper process(es) before upgrade restart: %w", retireErr)
		}
	}
	return func(ctx context.Context, opts teamsUpgradeFinishOptions) error {
		if beforeRestart != nil {
			if err := beforeRestart(ctx, opts.Success); err != nil {
				return err
			}
		}
		if opts.ServiceRestart == teamsUpgradeRestartNone {
			return nil
		}
		refresh := teamsUpgradeServiceRefreshResult{}
		refreshService := opts.Success || (teamsServiceGOOS() == "windows" && strings.TrimSpace(opts.PendingReplacePath) != "")
		if refreshService {
			if out != nil {
				_, _ = fmt.Fprintln(out, "Refreshing Teams service config before restart...")
			}
			var err error
			refresh, err = refreshTeamsServiceForHelperUpgrade(ctx, in, out)
			if err != nil {
				return err
			}
		}
		if opts.ServiceRestart == teamsUpgradeRestartDelayed {
			if out != nil {
				if opts.Success {
					_, _ = fmt.Fprintln(out, "Scheduling Teams service restart after the updated helper is ready...")
				} else {
					_, _ = fmt.Fprintln(out, "Scheduling Teams service restart after the pending helper replacement...")
				}
			}
			return scheduleDelayedTeamsServiceStartAfterUpgrade(ctx, refresh, opts.PendingReplacePath, opts.InstallPath)
		}
		if out != nil {
			_, _ = fmt.Fprintln(out, "Restarting Teams service after upgrade...")
		}
		return startTeamsServiceAfterUpgrade(ctx, refresh)
	}, nil
}

func withTeamsHelperUpgradeInstallLock(ctx context.Context, installPath string, fn func() error) error {
	resolved, err := resolveInstallPathForCLI(installPath)
	if err != nil {
		return err
	}
	lock, ok, err := acquireHelperInstallLock(ctx, resolved)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("another codex-helper upgrade is already using install path %s", resolved)
	}
	defer func() { _ = lock.Unlock() }()
	return fn()
}

func acquireHelperInstallLock(ctx context.Context, resolved string) (*flock.Flock, bool, error) {
	paths := helperInstallLockPaths(resolved)
	var lastErr error
	for i, lockPath := range paths {
		lock, ok, err := tryLockHelperInstallPath(ctx, lockPath)
		if err != nil {
			lastErr = err
			if i+1 < len(paths) && helperInstallLockUnavailable(err) {
				continue
			}
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		return lock, true, nil
	}
	if lastErr != nil {
		return nil, false, lastErr
	}
	return nil, false, fmt.Errorf("could not create codex-helper upgrade lock for install path %s", resolved)
}

func helperInstallLockPaths(resolvedInstallPath string) []string {
	paths := []string{resolvedInstallPath + ".auto-update.lock"}
	cacheDir, err := os.UserCacheDir()
	if err != nil || strings.TrimSpace(cacheDir) == "" {
		return paths
	}
	lockIdentity := resolvedInstallPath
	if key, ok := helperInstallLocationKey(resolvedInstallPath, runtime.GOOS); ok {
		lockIdentity = key
	}
	sum := sha256.Sum256([]byte(lockIdentity))
	base := filepath.Base(lockIdentity)
	if strings.TrimSpace(base) == "" || base == "." || base == string(filepath.Separator) {
		base = "codex-proxy"
	}
	paths = append(paths, filepath.Join(cacheDir, "codex-helper", "locks", hex.EncodeToString(sum[:8])+"-"+base+".auto-update.lock"))
	return paths
}

func tryLockHelperInstallPath(ctx context.Context, lockPath string) (*flock.Flock, bool, error) {
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return nil, false, err
	}
	lock := flock.New(lockPath)
	lockCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	ok, err := lock.TryLockContext(lockCtx, 10*time.Millisecond)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, false, ctxErr
			}
			return nil, false, nil
		}
		return nil, false, err
	}
	if !ok {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, false, ctxErr
		}
	}
	return lock, ok, nil
}

func helperInstallLockUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if os.IsPermission(err) || os.IsNotExist(err) {
		return true
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "read-only file system") || strings.Contains(text, "access is denied")
}

type teamsUpgradeServiceRefreshResult struct {
	StartupFallback bool
}

func refreshTeamsServiceForHelperUpgrade(ctx context.Context, in io.Reader, out io.Writer) (teamsUpgradeServiceRefreshResult, error) {
	matches, err := teamsServiceConfigAlreadyMatchesForQuietUpgrade(ctx)
	if err != nil {
		return teamsUpgradeServiceRefreshResult{}, err
	}
	if matches {
		return teamsUpgradeServiceRefreshResult{}, nil
	}
	repairOpts := teamsServiceRepairOptions{Enable: true, Start: false}
	if backend, backendErr := teamsServiceBackendForCurrentPlatform(); backendErr != nil {
		return teamsUpgradeServiceRefreshResult{}, backendErr
	} else if backend.ID() == teamsServiceLocalSupervisorID {
		active, activeErr := backend.Active(ctx)
		if activeErr != nil {
			return teamsUpgradeServiceRefreshResult{}, activeErr
		}
		repairOpts.Start = active
	}
	if _, err := repairTeamsService(ctx, repairOpts); err == nil {
		return teamsUpgradeServiceRefreshResult{}, nil
	} else {
		return recoverWSLTeamsServiceRefreshAccessDenied(ctx, in, out, err)
	}
}

func teamsServiceConfigAlreadyMatchesForQuietUpgrade(ctx context.Context) (bool, error) {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return false, err
	}
	wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
	if !ok {
		return false, nil
	}
	spec, err := buildTeamsServiceSpec()
	if err != nil {
		return false, err
	}
	matches, err := wslBackend.TaskConfigMatches(ctx, spec)
	if err != nil || !matches {
		return false, err
	}
	if _, err := wslBackend.writeTaskConfig(buildTeamsServiceWSLArguments(spec)); err != nil {
		return false, err
	}
	_ = wslBackend.removeStartupFallbackMarker(ctx)
	return true, nil
}

func recoverWSLTeamsServiceRefreshAccessDenied(ctx context.Context, in io.Reader, out io.Writer, repairErr error) (teamsUpgradeServiceRefreshResult, error) {
	backend, backendErr := teamsServiceBackendForCurrentPlatform()
	if backendErr != nil {
		return teamsUpgradeServiceRefreshResult{}, backendErr
	}
	wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
	if !ok || !isTeamsServiceWindowsAccessDeniedError(repairErr) {
		return teamsUpgradeServiceRefreshResult{}, repairErr
	}
	spec, specErr := buildTeamsServiceSpec()
	if specErr != nil {
		return teamsUpgradeServiceRefreshResult{}, specErr
	}
	fallbackReason := "Windows Scheduled Task setup could not be completed: " + teamsServiceBootstrapErrorSummary(repairErr)
	retireErr := wslBackend.RetireScheduledTasks(ctx)
	if retireErr == nil {
		return installWSLTeamsServiceUpgradeStartupFallback(ctx, wslBackend, spec, out, fallbackReason, repairErr)
	}
	if !confirmTeamsServiceUACPrompt(in, out, false) {
		return teamsUpgradeServiceRefreshResult{}, fmt.Errorf("Windows Startup fallback is unsafe because old WSL Scheduled Tasks could not be disabled after Scheduled Task refresh failed (%s): normal cleanup failed: %s; UAC was not confirmed", teamsServiceBootstrapErrorSummary(repairErr), teamsServiceBootstrapErrorSummary(retireErr))
	}
	elevatedReason := ""
	principalUser, userErr := teamsServiceCurrentWindowsUser(ctx)
	if userErr != nil {
		elevatedReason = "Could not identify the current Windows user for UAC setup: " + teamsServiceBootstrapErrorSummary(userErr)
	} else if _, elevatedErr := wslBackend.RepairElevated(ctx, spec, teamsServiceRepairOptions{Enable: true, Start: false}, principalUser); elevatedErr == nil {
		_ = wslBackend.removeStartupFallbackMarker(ctx)
		return teamsUpgradeServiceRefreshResult{}, nil
	} else {
		elevatedReason = "UAC Scheduled Task setup failed: " + teamsServiceBootstrapErrorSummary(elevatedErr)
	}
	if elevatedRetireErr := wslBackend.RetireScheduledTasksElevated(ctx); elevatedRetireErr != nil {
		return teamsUpgradeServiceRefreshResult{}, fmt.Errorf("Windows Startup fallback is unsafe because old WSL Scheduled Tasks could not be disabled after Scheduled Task refresh failed (%s): normal cleanup failed: %s; elevated cleanup failed: %s", teamsServiceBootstrapErrorSummary(repairErr), teamsServiceBootstrapErrorSummary(retireErr), teamsServiceBootstrapErrorSummary(elevatedRetireErr))
	}
	if strings.TrimSpace(elevatedReason) != "" {
		fallbackReason = elevatedReason
	}
	fallbackReason += " Old WSL Scheduled Tasks were disabled using Windows permission before installing the fallback."
	return installWSLTeamsServiceUpgradeStartupFallback(ctx, wslBackend, spec, out, fallbackReason, repairErr)
}

func installWSLTeamsServiceUpgradeStartupFallback(ctx context.Context, wslBackend teamsServiceWSLWindowsTaskBackend, spec teamsServiceSpec, out io.Writer, fallbackReason string, repairErr error) (teamsUpgradeServiceRefreshResult, error) {
	printTeamsServiceBootstrapTaskFallback(out, fallbackReason)
	if _, fallbackErr := wslBackend.InstallStartupFallback(ctx, spec, false); fallbackErr != nil {
		return teamsUpgradeServiceRefreshResult{}, fmt.Errorf("Windows Startup fallback failed after Scheduled Task refresh failed (%s): %s", teamsServiceBootstrapErrorSummary(repairErr), teamsServiceBootstrapErrorSummary(fallbackErr))
	}
	_ = wslBackend.removeTaskConfig()
	return teamsUpgradeServiceRefreshResult{StartupFallback: true}, nil
}

// After a Startup fallback refresh the local supervisor is the configured
// backend, so the normal start paths apply.
func startTeamsServiceAfterUpgrade(ctx context.Context, _ teamsUpgradeServiceRefreshResult) error {
	return startTeamsService(ctx, false)
}

func scheduleDelayedTeamsServiceStartAfterUpgrade(ctx context.Context, _ teamsUpgradeServiceRefreshResult, pendingReplacePath string, installPath string) error {
	if teamsServiceGOOS() == "windows" && strings.TrimSpace(pendingReplacePath) != "" {
		return scheduleTeamsPendingHelperActivationForReplacement(ctx, pendingReplacePath, installPath)
	}
	return scheduleDelayedTeamsServiceStart(ctx, pendingReplacePath)
}

func scheduleDelayedTeamsServiceStart(ctx context.Context, pendingReplacePath string) error {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return err
	}
	name, args, err := delayedTeamsServiceStartCommand(backend, pendingReplacePath)
	if err != nil {
		return err
	}
	return teamsServiceStartDetached(ctx, name, args...)
}

func defaultTeamsServiceStartDetached(_ context.Context, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	configureTeamsServiceDetachedCommand(cmd)
	return cmd.Start()
}

func delayedTeamsServiceStartCommand(backend teamsServiceBackend, pendingReplacePath string) (string, []string, error) {
	if backend.ID() == teamsServiceLocalSupervisorID {
		return delayedLocalSupervisorServiceCommand(backend, "start")
	}
	if backend.ID() == "wsl-windows-task-scheduler" {
		wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
		if !ok {
			return "", nil, fmt.Errorf("WSL Teams service backend has unexpected type %T", backend)
		}
		command := "Start-Sleep -Seconds 3; " +
			teamsServiceStartScheduledTaskIfStoppedFunctionPowerShell() +
			teamsServiceWSLResolveTaskPowerShell(wslBackend.Name()) + "Enable-ScheduledTask -TaskName $taskName | Out-Null; Start-CodexHelperScheduledTaskIfStopped $taskName; " +
			teamsServiceWSLResolveOptionalTaskPowerShell(wslBackend.watchdogName()) + "if ($null -ne $task) { if ($task.State -eq 'Disabled') { Enable-ScheduledTask -TaskName $taskName | Out-Null }; Start-CodexHelperScheduledTaskIfStopped $taskName }"
		return teamsServicePowerShellExecutable(), []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-WindowStyle", "Hidden", "-Command", command}, nil
	}
	switch teamsServiceGOOS() {
	case "windows":
		task := powershellSingleQuote(teamsServiceWindowsTaskName)
		command := delayedWindowsTeamsServiceStartPowerShell(pendingReplacePath) + teamsServiceStartScheduledTaskIfStoppedFunctionPowerShell() + "Enable-ScheduledTask -TaskName " + task + " | Out-Null; Start-CodexHelperScheduledTaskIfStopped " + task
		return teamsServicePowerShellExecutable(), []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-WindowStyle", "Hidden", "-Command", command}, nil
	case "darwin":
		path, err := backend.Path()
		if err != nil {
			return "", nil, err
		}
		script := "sleep 3; launchctl bootstrap " + shellQuote(teamsServiceLaunchctlUserTarget()) + " " + shellQuote(path) + " >/dev/null 2>&1 || launchctl kickstart -k " + shellQuote(teamsServiceLaunchctlServiceTarget()) + " >/dev/null 2>&1"
		return "sh", []string{"-c", script}, nil
	default:
		script := "sleep 3; systemctl --user start " + shellQuote(backend.Name()) + " >/dev/null 2>&1"
		return "sh", []string{"-c", script}, nil
	}
}

func delayedLocalSupervisorServiceCommand(backend teamsServiceBackend, action string) (string, []string, error) {
	action = strings.TrimSpace(action)
	switch action {
	case "start", "restart":
	default:
		return "", nil, fmt.Errorf("unsupported delayed local-supervisor action %q", action)
	}
	path, err := backend.Path()
	if err != nil {
		return "", nil, err
	}
	cfg, err := readTeamsServiceLocalSupervisorConfig(path)
	if err != nil {
		return "", nil, err
	}
	exe := strings.TrimSpace(cfg.Spec.Executable)
	if exe == "" {
		return "", nil, fmt.Errorf("local-supervisor executable is required for delayed Teams service %s", action)
	}
	script := "sleep 3; " +
		envTeamsLinuxServiceBackend + "=local-supervisor " +
		envTeamsWSLServiceBackend + "=local-supervisor " +
		shellQuote(exe) + " teams service " + action + " >/dev/null 2>&1"
	return "sh", []string{"-c", script}, nil
}

func delayedWindowsTeamsServiceStartPowerShell(pendingReplacePath string) string {
	path := strings.TrimSpace(pendingReplacePath)
	if path == "" {
		return "Start-Sleep -Seconds 3; "
	}
	return "$pendingReplace = " + powershellSingleQuote(path) + "; " +
		"for ($i = 0; $i -lt 240 -and (Test-Path -LiteralPath $pendingReplace); $i++) { Start-Sleep -Milliseconds 500 }; " +
		"Start-Sleep -Seconds 1; "
}

func teamsUpgradeServiceActive(ctx context.Context) (bool, error) {
	active, err := teamsServiceActive(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "unsupported platform") {
			return false, nil
		}
		return false, err
	}
	return active, nil
}
