package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"golang.org/x/mod/semver"
)

// Old watchdogs cannot follow active runtime changes. Repair only that
// historical systemd case, before migration can wait out legacy heartbeats.
// Never use backend.Run("restart"): it also restarts our own service.
func refreshLegacyTeamsWatchdog(ctx context.Context) error {
	if runtime.GOOS != "linux" || os.Getenv("CODEX_HELPER_TEAMS_SERVICE") != "1" {
		return nil
	}
	current, ok := helperruntime.Current()
	if !ok {
		return nil
	}
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return err
	}
	if _, ok := backend.(teamsServiceSystemdBackend); !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	root, err := filepath.EvalSymlinks(current.Root)
	if err != nil {
		return err
	}
	return handoffLegacyTeamsWatchdog(ctx, root, version, os.Getpid(), teamsServiceRunSystemctl, os.Readlink, helperruntime.ReadActive)
}

func handoffLegacyTeamsWatchdog(ctx context.Context, root, build string, self int,
	run func(context.Context, ...string) ([]byte, error), readlink func(string) (string, error), active func(string) (string, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	running, ok := helperruntime.NormalizeVersion(build)
	if !ok {
		return fmt.Errorf("invalid helper build version %q", build)
	}
	selected, err := active(root)
	if err != nil {
		return err
	}
	if selected != running {
		return nil // another activation owns the transition
	}
	pid := func(unit string) (int, error) {
		out, err := run(ctx, "show", "--property=MainPID", "--value", unit)
		if err != nil {
			return 0, err
		}
		return strconv.Atoi(strings.TrimSpace(string(out)))
	}
	main, err := pid(teamsServiceUnitName)
	if err != nil {
		return err
	}
	if main != self {
		return nil // foreground runs and other service instances cannot repair
	}
	watchdog, err := pid(teamsServiceWatchdogUnitName)
	if err != nil {
		return err
	}
	if watchdog <= 0 {
		return nil // do not enable or start an intentionally stopped watchdog
	}
	exe, err := readlink(fmt.Sprintf("/proc/%d/exe", watchdog))
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(filepath.Join(root, "versions"), exe)
	parts := strings.Split(rel, string(filepath.Separator))
	if err != nil || len(parts) != 2 || parts[1] != "cxp" {
		return nil // never replace an unrelated installation
	}
	old, valid := helperruntime.NormalizeVersion(parts[0])
	if !valid || semver.Compare(old, "v0.1.22-rc.73") >= 0 || semver.Compare(old, running) >= 0 {
		return nil // modern watchdogs refresh themselves
	}
	// Revalidate after inspection, so a concurrent upgrade is not repaired
	// using an earlier instance or runtime selection.
	mainNow, err := pid(teamsServiceUnitName)
	if err != nil {
		return err
	}
	watchdogNow, err := pid(teamsServiceWatchdogUnitName)
	if err != nil {
		return err
	}
	selected, err = active(root)
	if err != nil {
		return err
	}
	exeNow, err := readlink(fmt.Sprintf("/proc/%d/exe", watchdogNow))
	if err != nil {
		return err
	}
	if mainNow != self || watchdogNow != watchdog || selected != running || exeNow != exe {
		return fmt.Errorf("service or runtime changed during watchdog inspection")
	}
	if _, err := run(ctx, "restart", teamsServiceWatchdogUnitName); err != nil {
		return err
	}
	// systemd reports started before the stable entry has exec'd the runtime.
	// Wait only within the caller's startup budget; never restart again here.
	for {
		newPID, err := pid(teamsServiceWatchdogUnitName)
		if err != nil {
			return err
		}
		newExe, err := readlink(fmt.Sprintf("/proc/%d/exe", newPID))
		if err == nil && newExe == filepath.Join(root, "versions", running, "cxp") {
			return nil
		}
		if err := sleepContext(ctx, 20*time.Millisecond); err != nil {
			return fmt.Errorf("watchdog restart did not select runtime %s: %w", running, err)
		}
	}
}
