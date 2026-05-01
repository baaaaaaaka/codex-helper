package cli

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
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
	Success        bool
	ServiceRestart teamsUpgradeServiceRestartMode
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
		if teamsStateHasUnfinishedWork(state) {
			return fmt.Errorf("Teams state has unfinished turns in %s but no active owner; run `codex-proxy teams recover` before using --upgrade-codex", path)
		}
	}
	return nil
}

func prepareTeamsForHelperUpgrade(ctx context.Context, out io.Writer, timeout time.Duration) (teamsUpgradeFinalizer, error) {
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
			return stopTeamsServiceForHelperUpgrade(ctx, out, nil)
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
			if teamsStateHasUnfinishedWork(state) {
				return nil, fmt.Errorf("Teams state has unfinished turns in %s but no active owner; run `codex-proxy teams recover` before upgrading", path)
			}
			continue
		}
		if teamsstore.IsStale(owner, 2*time.Minute, time.Now()) {
			return nil, fmt.Errorf("Teams bridge owner appears stale in %s; run `codex-proxy teams recover` before upgrading", path)
		}
		req, err := st.BeginUpgrade(ctx, teamsUpgradeDrainReason, timeout)
		if err != nil {
			return nil, err
		}
		stores = append(stores, upgradeStore{Path: path, St: st, Req: req})
	}
	if len(stores) == 0 {
		if serviceWasActive {
			return stopTeamsServiceForHelperUpgrade(ctx, out, nil)
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
				restart, err := stopTeamsServiceForHelperUpgrade(ctx, out, finish)
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

func stopTeamsServiceForHelperUpgrade(ctx context.Context, out io.Writer, beforeRestart func(context.Context, bool) error) (teamsUpgradeFinalizer, error) {
	if out != nil {
		_, _ = fmt.Fprintln(out, "Stopping Teams service before upgrade...")
	}
	if err := stopTeamsService(ctx); err != nil {
		return nil, err
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
		if opts.ServiceRestart == teamsUpgradeRestartDelayed {
			if out != nil {
				_, _ = fmt.Fprintln(out, "Scheduling Teams service restart after the updated helper is ready...")
			}
			return scheduleDelayedTeamsServiceStart(ctx)
		}
		if out != nil {
			_, _ = fmt.Fprintln(out, "Restarting Teams service after upgrade...")
		}
		return startTeamsService(ctx, false)
	}, nil
}

func scheduleDelayedTeamsServiceStart(ctx context.Context) error {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return err
	}
	name, args, err := delayedTeamsServiceStartCommand(backend)
	if err != nil {
		return err
	}
	return teamsServiceStartDetached(ctx, name, args...)
}

func defaultTeamsServiceStartDetached(_ context.Context, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	return cmd.Start()
}

func delayedTeamsServiceStartCommand(backend teamsServiceBackend) (string, []string, error) {
	if backend.ID() == "wsl-windows-task-scheduler" {
		command := "Start-Sleep -Seconds 3; Start-ScheduledTask -TaskName " + powershellSingleQuote(backend.Name())
		return teamsServicePowerShellExecutable(), []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-WindowStyle", "Hidden", "-Command", command}, nil
	}
	switch teamsServiceGOOS() {
	case "windows":
		command := "Start-Sleep -Seconds 3; Start-ScheduledTask -TaskName " + powershellSingleQuote(teamsServiceWindowsTaskName)
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
	return !teamsStateHasUnfinishedWork(state), nil
}

func stateOwner(state teamsstore.State) (teamsstore.OwnerMetadata, bool) {
	if state.ServiceOwner != nil {
		return *state.ServiceOwner, true
	}
	if state.LockOwner != nil {
		return *state.LockOwner, true
	}
	return teamsstore.OwnerMetadata{}, false
}

func teamsStateHasUnfinishedWork(state teamsstore.State) bool {
	return teamsstore.HasUpgradeBlockingWork(state, time.Now())
}
