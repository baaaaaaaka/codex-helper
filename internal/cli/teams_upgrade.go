package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/gofrs/flock"

	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

var teamsUpgradePollInterval = 500 * time.Millisecond

const teamsUpgradeDrainReason = teamsstore.HelperUpgradeReason

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

func ensureTeamsIdleBeforeCodexUpgrade(ctx context.Context) error {
	paths, err := existingTeamsStorePaths()
	if err != nil {
		return err
	}
	for _, path := range paths {
		st, err := teamsstore.Open(path)
		if err != nil {
			return err
		}
		state, err := st.Load(ctx)
		if err != nil {
			return err
		}
		owner, hasOwner := stateOwner(state)
		if hasOwner {
			if teamsstore.IsStale(owner, 2*time.Minute, time.Now()) {
				return fmt.Errorf("Teams bridge owner appears stale in %s; run `codex-proxy teams recover` before using --upgrade-codex", path)
			}
			return fmt.Errorf("Teams bridge is already running for %s; stop it or run `codex-proxy teams drain` before using --upgrade-codex", path)
		}
		if blockers := teamsUpgradeBlockers(state); len(blockers) > 0 {
			return fmt.Errorf("Teams state has upgrade-blocking work in %s but no active owner: %s; run `codex-proxy teams recover` before using --upgrade-codex", path, teamsUpgradeBlockerSummary(blockers))
		}
	}
	return nil
}

func prepareTeamsForHelperUpgrade(ctx context.Context, in io.Reader, out io.Writer, timeout time.Duration, registryPath *string) (teamsUpgradeFinalizer, error) {
	paths, err := existingTeamsStorePaths()
	if err != nil {
		return nil, err
	}
	serviceWasActive, err := teamsUpgradeServiceActive(ctx)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		if serviceWasActive {
			return stopTeamsServiceForHelperUpgrade(ctx, in, out, nil, registryPath)
		}
		return nil, nil
	}
	type upgradeStore struct {
		Path string
		St   *teamsstore.Store
		Req  teamsstore.UpgradeRequest
	}
	var stores []upgradeStore
	for _, path := range paths {
		st, err := teamsstore.Open(path)
		if err != nil {
			return nil, err
		}
		state, err := st.Load(ctx)
		if err != nil {
			return nil, err
		}
		owner, hasOwner := stateOwner(state)
		if !hasOwner {
			if helperUpgradeNeedsRescue(state) {
				report, rescueErr := st.RescueForUpgrade(ctx, teamsstore.UpgradeRescueOptions{Reason: teamsUpgradeDrainReason, StaleAfter: 2 * time.Minute})
				if rescueErr != nil {
					return nil, rescueErr
				}
				if out != nil {
					printTeamsUpgradeRescueReport(out, path, report)
				}
				state, err = st.Load(ctx)
				if err != nil {
					return nil, err
				}
				if blockers := teamsHelperUpgradeBlockers(state); len(blockers) > 0 {
					_, _ = st.AbortUpgrade(context.Background(), report.Upgrade.ID, "helper upgrade rescue left protected Teams work")
					return nil, fmt.Errorf("Teams upgrade paused because protected Teams work remains in %s: %s", path, teamsUpgradeBlockerSummary(blockers))
				}
				if report.Upgrade.ID != "" {
					stores = append(stores, upgradeStore{Path: path, St: st, Req: report.Upgrade})
				}
			}
			continue
		}
		if teamsstore.IsStale(owner, 2*time.Minute, time.Now()) {
			report, rescueErr := st.RescueForUpgrade(ctx, teamsstore.UpgradeRescueOptions{Reason: teamsUpgradeDrainReason, StaleAfter: 2 * time.Minute})
			if rescueErr != nil {
				return nil, rescueErr
			}
			if out != nil {
				printTeamsUpgradeRescueReport(out, path, report)
			}
			state, err = st.Load(ctx)
			if err != nil {
				return nil, err
			}
			if blockers := teamsHelperUpgradeBlockers(state); len(blockers) > 0 {
				_, _ = st.AbortUpgrade(context.Background(), report.Upgrade.ID, "helper upgrade rescue left protected Teams work")
				return nil, fmt.Errorf("Teams upgrade paused because protected Teams work remains in %s: %s", path, teamsUpgradeBlockerSummary(blockers))
			}
			stores = append(stores, upgradeStore{Path: path, St: st, Req: report.Upgrade})
			continue
		}
		req, err := st.BeginUpgrade(ctx, teamsUpgradeDrainReason, timeout)
		if err != nil {
			return nil, err
		}
		stores = append(stores, upgradeStore{Path: path, St: st, Req: req})
	}
	if len(stores) == 0 {
		if serviceWasActive {
			return stopTeamsServiceForHelperUpgrade(ctx, in, out, nil, registryPath)
		}
		return nil, nil
	}
	if out != nil {
		_, _ = fmt.Fprintln(out, "Waiting for active Teams bridge to drain before upgrading...")
	}
	finish := func(ctx context.Context, success bool) error {
		var firstErr error
		for _, item := range stores {
			var err error
			if success {
				_, err = item.St.CompleteUpgrade(ctx, item.Req.ID)
			} else {
				_, err = item.St.AbortUpgrade(ctx, item.Req.ID, "helper upgrade did not complete")
			}
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(teamsUpgradePollInterval)
	defer tick.Stop()
	for {
		drained := true
		for _, item := range stores {
			itemDrained, err := teamsUpgradeStateDrained(ctx, item.St)
			if err != nil {
				return nil, err
			}
			if !itemDrained {
				drained = false
				break
			}
		}
		if drained {
			if out != nil {
				_, _ = fmt.Fprintln(out, "Teams bridge drained.")
			}
			for _, item := range stores {
				if _, err := item.St.MarkUpgradeReady(ctx, item.Req.ID); err != nil {
					_ = finish(context.Background(), false)
					return nil, err
				}
			}
			if serviceWasActive {
				restart, err := stopTeamsServiceForHelperUpgrade(ctx, in, out, finish, registryPath)
				if err != nil {
					_ = finish(context.Background(), false)
					return nil, err
				}
				return restart, nil
			}
			return func(ctx context.Context, opts teamsUpgradeFinishOptions) error {
				return finish(ctx, opts.Success)
			}, nil
		}
		select {
		case <-ctx.Done():
			_ = finish(context.Background(), false)
			return nil, ctx.Err()
		case <-deadline.C:
			_ = finish(context.Background(), false)
			return nil, fmt.Errorf("timed out waiting for Teams bridge to drain; run `codex-proxy teams status` or `codex-proxy teams recover --force` if the owner is gone")
		case <-tick.C:
		}
	}
}

func rescueTeamsForNoopHelperUpgrade(ctx context.Context, in io.Reader, out io.Writer, timeout time.Duration, installPath string) error {
	needsRescue, err := helperUpgradeNeedsNoopRescue(ctx)
	if err != nil {
		return err
	}
	if !needsRescue {
		return nil
	}
	return withTeamsHelperUpgradeInstallLock(ctx, installPath, func() error {
		finishTeams, err := prepareTeamsForHelperUpgrade(ctx, in, out, timeout, nil)
		if err != nil {
			return err
		}
		if finishTeams == nil {
			return nil
		}
		return finishTeams(context.Background(), teamsUpgradeFinishOptions{
			Success:        true,
			ServiceRestart: teamsUpgradeRestartImmediate,
		})
	})
}

func helperUpgradeNeedsNoopRescue(ctx context.Context) (bool, error) {
	paths, err := existingTeamsStorePaths()
	if err != nil {
		return false, err
	}
	now := time.Now()
	for _, path := range paths {
		st, err := teamsstore.Open(path)
		if err != nil {
			return false, err
		}
		state, err := st.Load(ctx)
		if err != nil {
			return false, err
		}
		owner, hasOwner := stateOwner(state)
		if !hasOwner {
			if helperUpgradeNeedsRescue(state) {
				return true, nil
			}
			continue
		}
		if teamsstore.IsStale(owner, 2*time.Minute, now) {
			return true, nil
		}
	}
	return false, nil
}

func stopTeamsServiceForHelperUpgrade(ctx context.Context, in io.Reader, out io.Writer, beforeRestart func(context.Context, bool) error, registryPath *string) (teamsUpgradeFinalizer, error) {
	if out != nil {
		_, _ = fmt.Fprintln(out, "Stopping Teams service before upgrade...")
	}
	if err := stopTeamsService(ctx); err != nil {
		return nil, err
	}
	if backend, backendErr := teamsServiceBackendForCurrentPlatform(); backendErr == nil {
		if _, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
			spec, specErr := buildTeamsServiceSpec(registryPath)
			if specErr != nil {
				return nil, specErr
			}
			if _, retireErr := teamsServiceRetireLocalDuplicateProcesses(ctx, spec); retireErr != nil {
				return nil, fmt.Errorf("could not stop old local Teams helper process(es) before upgrade restart: %w", retireErr)
			}
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
			refresh, err = refreshTeamsServiceForHelperUpgrade(ctx, registryPath, in, out)
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
			return scheduleDelayedTeamsServiceStartAfterUpgrade(ctx, registryPath, refresh, opts.PendingReplacePath, opts.InstallPath)
		}
		if out != nil {
			_, _ = fmt.Fprintln(out, "Restarting Teams service after upgrade...")
		}
		return startTeamsServiceAfterUpgrade(ctx, registryPath, refresh)
	}, nil
}

func withTeamsHelperUpgradeInstallLock(ctx context.Context, installPath string, fn func() error) error {
	resolved, err := update.ResolveInstallPath(installPath)
	if err != nil {
		return err
	}
	lock := flock.New(resolved + ".auto-update.lock")
	ok, err := tryLockHelperInstallPath(ctx, lock)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("another codex-helper upgrade is already using install path %s", resolved)
	}
	defer func() { _ = lock.Unlock() }()
	return fn()
}

func tryLockHelperInstallPath(ctx context.Context, lock *flock.Flock) (bool, error) {
	lockCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	ok, err := lock.TryLockContext(lockCtx, 10*time.Millisecond)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return false, ctxErr
			}
			return false, nil
		}
		return false, err
	}
	if !ok {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
	}
	return ok, nil
}

type teamsUpgradeServiceRefreshResult struct {
	StartupFallback bool
}

func refreshTeamsServiceForHelperUpgrade(ctx context.Context, registryPath *string, in io.Reader, out io.Writer) (teamsUpgradeServiceRefreshResult, error) {
	matches, err := teamsServiceConfigAlreadyMatchesForQuietUpgrade(ctx, registryPath)
	if err != nil {
		return teamsUpgradeServiceRefreshResult{}, err
	}
	if matches {
		return teamsUpgradeServiceRefreshResult{}, nil
	}
	if _, err := repairTeamsService(ctx, registryPath, teamsServiceRepairOptions{Enable: true, Start: false}); err == nil {
		return teamsUpgradeServiceRefreshResult{}, nil
	} else {
		return recoverWSLTeamsServiceRefreshAccessDenied(ctx, registryPath, in, out, err)
	}
}

func teamsServiceConfigAlreadyMatchesForQuietUpgrade(ctx context.Context, registryPath *string) (bool, error) {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return false, err
	}
	wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
	if !ok {
		return false, nil
	}
	spec, err := buildTeamsServiceSpec(registryPath)
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
	_ = wslBackend.RemoveStartupFallbackMarker()
	return true, nil
}

func recoverWSLTeamsServiceRefreshAccessDenied(ctx context.Context, registryPath *string, in io.Reader, out io.Writer, repairErr error) (teamsUpgradeServiceRefreshResult, error) {
	backend, backendErr := teamsServiceBackendForCurrentPlatform()
	if backendErr != nil {
		return teamsUpgradeServiceRefreshResult{}, backendErr
	}
	wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
	if !ok || !isTeamsServiceWindowsAccessDeniedError(repairErr) {
		return teamsUpgradeServiceRefreshResult{}, repairErr
	}
	spec, specErr := buildTeamsServiceSpec(registryPath)
	if specErr != nil {
		return teamsUpgradeServiceRefreshResult{}, specErr
	}
	fallbackReason := "Windows Scheduled Task setup could not be completed: " + teamsServiceBootstrapErrorSummary(repairErr)
	retireErr := wslBackend.RetireScheduledTasks(ctx)
	if retireErr == nil {
		return installWSLTeamsServiceUpgradeStartupFallback(ctx, wslBackend, spec, out, fallbackReason, repairErr)
	}
	if !confirmTeamsServiceUACPrompt(in, out, false) {
		return teamsUpgradeServiceRefreshResult{}, fmt.Errorf("Windows Startup watchdog fallback is unsafe because old WSL Scheduled Tasks could not be disabled after Scheduled Task refresh failed (%s): normal cleanup failed: %s; UAC was not confirmed", teamsServiceBootstrapErrorSummary(repairErr), teamsServiceBootstrapErrorSummary(retireErr))
	}
	elevatedReason := ""
	principalUser, userErr := teamsServiceCurrentWindowsUser(ctx)
	if userErr != nil {
		elevatedReason = "Could not identify the current Windows user for UAC setup: " + teamsServiceBootstrapErrorSummary(userErr)
	} else if _, elevatedErr := wslBackend.RepairElevated(ctx, spec, teamsServiceRepairOptions{Enable: true, Start: false}, principalUser); elevatedErr == nil {
		_ = wslBackend.RemoveStartupFallbackMarker()
		return teamsUpgradeServiceRefreshResult{}, nil
	} else {
		elevatedReason = "UAC Scheduled Task setup failed: " + teamsServiceBootstrapErrorSummary(elevatedErr)
	}
	if elevatedRetireErr := wslBackend.RetireScheduledTasksElevated(ctx); elevatedRetireErr != nil {
		return teamsUpgradeServiceRefreshResult{}, fmt.Errorf("Windows Startup watchdog fallback is unsafe because old WSL Scheduled Tasks could not be disabled after Scheduled Task refresh failed (%s): normal cleanup failed: %s; elevated cleanup failed: %s", teamsServiceBootstrapErrorSummary(repairErr), teamsServiceBootstrapErrorSummary(retireErr), teamsServiceBootstrapErrorSummary(elevatedRetireErr))
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
		return teamsUpgradeServiceRefreshResult{}, fmt.Errorf("Windows Startup watchdog fallback failed after Scheduled Task refresh failed (%s): %s", teamsServiceBootstrapErrorSummary(repairErr), teamsServiceBootstrapErrorSummary(fallbackErr))
	}
	_ = wslBackend.removeTaskConfig()
	return teamsUpgradeServiceRefreshResult{StartupFallback: true}, nil
}

func startTeamsServiceAfterUpgrade(ctx context.Context, registryPath *string, refresh teamsUpgradeServiceRefreshResult) error {
	if refresh.StartupFallback {
		backend, err := teamsServiceBackendForCurrentPlatform()
		if err != nil {
			return err
		}
		wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
		if !ok {
			return fmt.Errorf("Teams service Startup fallback is only supported by the WSL Windows service backend")
		}
		spec, err := buildTeamsServiceSpec(registryPath)
		if err != nil {
			return err
		}
		_, err = wslBackend.InstallStartupFallback(ctx, spec, true)
		return err
	}
	return startTeamsService(ctx, false)
}

func scheduleDelayedTeamsServiceStartAfterUpgrade(ctx context.Context, registryPath *string, refresh teamsUpgradeServiceRefreshResult, pendingReplacePath string, installPath string) error {
	if refresh.StartupFallback {
		return scheduleDelayedTeamsStartupFallbackStart(ctx, registryPath)
	}
	if teamsServiceGOOS() == "windows" && strings.TrimSpace(pendingReplacePath) != "" {
		return scheduleTeamsPendingHelperActivationForReplacement(ctx, pendingReplacePath, installPath)
	}
	return scheduleDelayedTeamsServiceStart(ctx, pendingReplacePath)
}

func scheduleDelayedTeamsStartupFallbackStart(ctx context.Context, registryPath *string) error {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return err
	}
	wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
	if !ok {
		return fmt.Errorf("Teams service Startup fallback is only supported by the WSL Windows service backend")
	}
	spec, err := buildTeamsServiceSpec(registryPath)
	if err != nil {
		return err
	}
	markerPath, err := wslBackend.startupFallbackMarkerPath()
	if err != nil {
		return err
	}
	fallbackSpec := buildTeamsServiceWSLStartupFallbackSpec(spec, teamsServiceWSLStartupFallbackStopPath(markerPath))
	args := buildTeamsServiceWSLArguments(fallbackSpec)
	command := "Start-Sleep -Seconds 3; " + buildTeamsServiceWSLStartupFallbackCommand(wslBackend.Name(), args, true)
	return teamsServiceStartDetached(ctx, teamsServicePowerShellExecutable(), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-WindowStyle", "Hidden", "-Command", command)
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
	return cmd.Start()
}

func delayedTeamsServiceStartCommand(backend teamsServiceBackend, pendingReplacePath string) (string, []string, error) {
	if backend.ID() == "wsl-windows-task-scheduler" {
		wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
		if !ok {
			return "", nil, fmt.Errorf("WSL Teams service backend has unexpected type %T", backend)
		}
		command := "Start-Sleep -Seconds 3; " +
			teamsServiceWSLResolveTaskPowerShell(wslBackend.Name()) + "Enable-ScheduledTask -TaskName $taskName | Out-Null; Start-ScheduledTask -TaskName $taskName; " +
			teamsServiceWSLResolveOptionalTaskPowerShell(wslBackend.watchdogName()) + "if ($null -ne $task) { if ($task.State -eq 'Disabled') { Enable-ScheduledTask -TaskName $taskName | Out-Null }; if ($task.State -ne 'Running') { Start-ScheduledTask -TaskName $taskName } }"
		return teamsServicePowerShellExecutable(), []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-WindowStyle", "Hidden", "-Command", command}, nil
	}
	switch teamsServiceGOOS() {
	case "windows":
		task := powershellSingleQuote(teamsServiceWindowsTaskName)
		command := delayedWindowsTeamsServiceStartPowerShell(pendingReplacePath) + "Enable-ScheduledTask -TaskName " + task + " | Out-Null; Start-ScheduledTask -TaskName " + task
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

func restoreTeamsUpgradeDrain(ctx context.Context, st *teamsstore.Store, previous teamsstore.ServiceControl) error {
	return st.Update(ctx, func(state *teamsstore.State) error {
		current := state.ServiceControl
		if !current.Draining || current.Reason != teamsUpgradeDrainReason || current.Paused != previous.Paused {
			return nil
		}
		restored := previous
		restored.UpdatedAt = time.Now()
		state.ServiceControl = restored
		return nil
	})
}

func teamsUpgradeStateDrained(ctx context.Context, st *teamsstore.Store) (bool, error) {
	state, err := st.Load(ctx)
	if err != nil {
		return false, err
	}
	if _, ok := stateOwner(state); ok {
		return false, nil
	}
	return len(teamsHelperUpgradeBlockers(state)) == 0, nil
}

func stateOwner(state teamsstore.State) (teamsstore.OwnerMetadata, bool) {
	if state.ServiceOwner != nil {
		if teamsstore.OwnerAppearsLocallyDead(*state.ServiceOwner) {
			return teamsstore.OwnerMetadata{}, false
		}
		return *state.ServiceOwner, true
	}
	if state.LockOwner != nil {
		if teamsstore.OwnerAppearsLocallyDead(*state.LockOwner) {
			return teamsstore.OwnerMetadata{}, false
		}
		return *state.LockOwner, true
	}
	return teamsstore.OwnerMetadata{}, false
}

func helperUpgradeNeedsRescue(state teamsstore.State) bool {
	for _, turn := range state.Turns {
		if turn.Status == teamsstore.TurnStatusRunning {
			return true
		}
	}
	for _, msg := range state.OutboxMessages {
		if msg.Status != teamsstore.OutboxStatusQueued && msg.Status != teamsstore.OutboxStatusSending {
			continue
		}
		if teamsstore.OutboxBlocksUpgrade(state, msg, time.Now()) {
			return true
		}
		if teamsstore.OutboxDeliveryTransient(msg) {
			return true
		}
	}
	return false
}

func teamsHelperUpgradeBlockers(state teamsstore.State) []teamsstore.UpgradeBlocker {
	now := time.Now()
	var blockers []teamsstore.UpgradeBlocker
	for _, turn := range state.Turns {
		if turn.Status != teamsstore.TurnStatusRunning {
			continue
		}
		blockers = append(blockers, teamsstore.UpgradeBlocker{
			Kind:      "turn",
			ID:        turn.ID,
			SessionID: turn.SessionID,
			Status:    string(turn.Status),
		})
	}
	for _, msg := range state.OutboxMessages {
		if teamsstore.OutboxBlocksUpgrade(state, msg, now) {
			blockers = append(blockers, teamsstore.UpgradeBlocker{
				Kind:      "outbox",
				ID:        msg.ID,
				SessionID: msg.SessionID,
				Status:    string(msg.Status),
				Detail:    msg.Kind,
			})
		}
	}
	return blockers
}

func teamsUpgradeBlockers(state teamsstore.State) []teamsstore.UpgradeBlocker {
	return teamsstore.UpgradeBlockers(state, time.Now())
}

func teamsUpgradeBlockerSummary(blockers []teamsstore.UpgradeBlocker) string {
	if len(blockers) == 0 {
		return "no queued/running turns or blocking outbox messages"
	}
	const max = 4
	parts := make([]string, 0, min(len(blockers), max)+1)
	for i, blocker := range blockers {
		if i >= max {
			parts = append(parts, fmt.Sprintf("+%d more", len(blockers)-max))
			break
		}
		segment := blocker.Kind
		if blocker.SessionID != "" {
			segment += " " + blocker.SessionID
		}
		if blocker.ID != "" {
			segment += " " + blocker.ID
		}
		if blocker.Status != "" {
			segment += " status=" + blocker.Status
		}
		if blocker.Detail != "" {
			segment += " kind=" + blocker.Detail
		}
		parts = append(parts, segment)
	}
	return strings.Join(parts, "; ")
}

func printTeamsUpgradeRescueReport(out io.Writer, path string, report teamsstore.UpgradeRescueReport) {
	if out == nil {
		return
	}
	clearedOwners := 0
	if report.ClearedOwner != nil {
		clearedOwners = 1
	}
	_, _ = fmt.Fprintf(
		out,
		"Teams upgrade rescue for %s: cleared stale helpers=%d preserved queued requests=%d interrupted abandoned requests=%d skipped stale notices=%d preserved saved replies/files=%d\n",
		path,
		clearedOwners,
		len(report.PreservedQueuedTurnIDs),
		len(report.InterruptedTurnIDs),
		len(report.SupersededOutboxIDs),
		len(report.PreservedOutboxBlockerIDs),
	)
}
