package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func TestTeamsServiceWatchdogDefaultsAvoidTransientRestartLoops(t *testing.T) {
	if defaultTeamsServiceWatchdogOwnerStaleAfter != 90*time.Second {
		t.Fatalf("owner stale threshold = %s, want 90s", defaultTeamsServiceWatchdogOwnerStaleAfter)
	}
	if defaultTeamsServiceWatchdogConsecutiveStale != 3 {
		t.Fatalf("consecutive stale threshold = %d, want 3", defaultTeamsServiceWatchdogConsecutiveStale)
	}
	if defaultTeamsServiceWatchdogCooldown != 2*time.Minute {
		t.Fatalf("restart cooldown = %s, want 2m", defaultTeamsServiceWatchdogCooldown)
	}
	if teamsServiceExternalWatchdogInterval != 10*time.Second {
		t.Fatalf("watchdog interval = %s, want 10s", teamsServiceExternalWatchdogInterval)
	}
	if teamsServiceExternalWatchdogCheckTimeout != 45*time.Second {
		t.Fatalf("watchdog check timeout = %s, want 45s", teamsServiceExternalWatchdogCheckTimeout)
	}
}

// The snapshot comes from the store's owner row, read-only: a fresh
// heartbeat is healthy, an old one is stale, a released owner is missing,
// and reading never changes the store.
func TestCollectTeamsServiceWatchdogSnapshotReadsStoreOwner(t *testing.T) {
	lockCLITestHooks(t)
	path := filepath.Join(t.TempDir(), "core.sqlite")
	st, err := corestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if _, err := st.Acquire(corestore.Ident{Host: "box", PID: 4242, BootID: "b"}); err != nil {
		t.Fatal(err)
	}

	prevStore, prevInstalled, prevActive, prevChild := teamsServiceWatchdogStorePath, teamsServiceWatchdogInstalled, teamsServiceWatchdogActive, teamsServiceWatchdogManagedChild
	t.Cleanup(func() {
		teamsServiceWatchdogStorePath, teamsServiceWatchdogInstalled, teamsServiceWatchdogActive, teamsServiceWatchdogManagedChild = prevStore, prevInstalled, prevActive, prevChild
	})
	teamsServiceWatchdogStorePath = func() (string, error) { return path, nil }
	teamsServiceWatchdogInstalled = func() (bool, error) { return true, nil }
	teamsServiceWatchdogActive = func(context.Context) (bool, error) { return true, nil }
	teamsServiceWatchdogManagedChild = func() (teamsServiceWatchdogManagedChildIdentity, bool) {
		return teamsServiceWatchdogManagedChildIdentity{}, false
	}

	now := time.Now()
	snap, err := collectTeamsServiceWatchdogSnapshot(context.Background(), normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now}))
	if err != nil {
		t.Fatal(err)
	}
	if !snap.OwnerFound || !snap.OwnerFresh || snap.AuthoritativeStorePath != path || snap.StateFiles != 1 {
		t.Fatalf("fresh owner snapshot = %+v", snap)
	}
	if d := evaluateTeamsServiceWatchdog(snap, teamsServiceWatchdogState{ConsecutiveStale: 5}, teamsServiceWatchdogOptions{Now: now}); d.Action != teamsServiceWatchdogActionNoop || d.Stale {
		t.Fatalf("fresh owner decision = %+v", d)
	}

	later := now.Add(5 * time.Minute)
	snap, err = collectTeamsServiceWatchdogSnapshot(context.Background(), normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: later}))
	if err != nil || !snap.OwnerFound || snap.OwnerFresh {
		t.Fatalf("stale owner snapshot = %+v, %v", snap, err)
	}
	if d := evaluateTeamsServiceWatchdog(snap, teamsServiceWatchdogState{ConsecutiveStale: 2}, teamsServiceWatchdogOptions{Now: later}); d.Action != teamsServiceWatchdogActionRestart {
		t.Fatalf("stale owner decision = %+v", d)
	}

	// A live supervised child that does not own the store yet is waited for.
	teamsServiceWatchdogManagedChild = func() (teamsServiceWatchdogManagedChildIdentity, bool) {
		return teamsServiceWatchdogManagedChildIdentity{PID: 9999, StartedAt: later.Add(-time.Minute), ManagedBySupervisor: true}, true
	}
	snap, err = collectTeamsServiceWatchdogSnapshot(context.Background(), normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: later}))
	if err != nil || !snap.ManagedChildStarting {
		t.Fatalf("starting child snapshot = %+v, %v", snap, err)
	}

	if err := st.Release(); err != nil {
		t.Fatal(err)
	}
	teamsServiceWatchdogManagedChild = func() (teamsServiceWatchdogManagedChildIdentity, bool) {
		return teamsServiceWatchdogManagedChildIdentity{}, false
	}
	snap, err = collectTeamsServiceWatchdogSnapshot(context.Background(), normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now}))
	if err != nil || snap.OwnerFound {
		t.Fatalf("released owner snapshot = %+v, %v", snap, err)
	}
	if _, reason := teamsServiceWatchdogStaleReason(snap); reason != "helper owner is missing" {
		t.Fatalf("released owner reason = %q", reason)
	}
}

// No store yet (first start, or the store is still being converted) is not
// a reason to restart.
func TestCollectTeamsServiceWatchdogSnapshotWithoutStore(t *testing.T) {
	lockCLITestHooks(t)
	missing := filepath.Join(t.TempDir(), "core.sqlite")
	prevStore, prevInstalled, prevActive := teamsServiceWatchdogStorePath, teamsServiceWatchdogInstalled, teamsServiceWatchdogActive
	t.Cleanup(func() {
		teamsServiceWatchdogStorePath, teamsServiceWatchdogInstalled, teamsServiceWatchdogActive = prevStore, prevInstalled, prevActive
	})
	teamsServiceWatchdogStorePath = func() (string, error) { return missing, nil }
	teamsServiceWatchdogInstalled = func() (bool, error) { return true, nil }
	teamsServiceWatchdogActive = func(context.Context) (bool, error) { return true, nil }
	snap, err := collectTeamsServiceWatchdogSnapshot(context.Background(), normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{}))
	if err != nil || snap.StateFiles != 0 {
		t.Fatalf("snapshot = %+v, %v", snap, err)
	}
	if d := evaluateTeamsServiceWatchdog(snap, teamsServiceWatchdogState{ConsecutiveStale: 9}, teamsServiceWatchdogOptions{}); d.Action != teamsServiceWatchdogActionNoop {
		t.Fatalf("decision = %+v", d)
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("watchdog created the store: %v", err)
	}
}

func TestTeamsServiceWatchdogStartsInactiveService(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})

	decision := evaluateTeamsServiceWatchdog(teamsServiceWatchdogSnapshot{Installed: true, Active: false}, teamsServiceWatchdogState{}, opts)
	if decision.Action != teamsServiceWatchdogActionStart {
		t.Fatalf("action = %q, want start; decision=%+v", decision.Action, decision)
	}

	decision = evaluateTeamsServiceWatchdog(
		teamsServiceWatchdogSnapshot{Installed: true, Active: false},
		teamsServiceWatchdogState{LastActionAt: now.Add(-10 * time.Second)},
		opts,
	)
	if decision.Action != teamsServiceWatchdogActionStart || !decision.CooldownUntil.IsZero() {
		t.Fatalf("cooldown decision = %+v, want start without cooldown when service is inactive", decision)
	}
}

func TestTeamsServiceWatchdogWaitsForConsecutiveStaleSamples(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:          true,
		Active:             true,
		StateFiles:         1,
		OwnerFound:         true,
		LastOwnerHeartbeat: now.Add(-2 * time.Minute),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, opts)
	if decision.Action != teamsServiceWatchdogActionNoop || !decision.Stale || decision.ConsecutiveStale != 1 {
		t.Fatalf("stale decision = %+v, want first stale sample without restart", decision)
	}

	decision = evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{ConsecutiveStale: 2}, opts)
	if decision.Action != teamsServiceWatchdogActionRestart || !decision.Stale || decision.ConsecutiveStale != 3 {
		t.Fatalf("stale decision = %+v, want restart on third consecutive stale sample", decision)
	}
}

func TestTeamsServiceWatchdogKeepsLiveManagedChildWaitingForOwnerPastPollThreshold(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:             true,
		Active:                true,
		StateFiles:            1,
		OwnerFound:            true,
		LastOwnerHeartbeat:    now.Add(-24 * time.Hour),
		ManagedChildStarting:  true,
		ManagedChildStartedAt: now.Add(-24 * time.Hour),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{ConsecutiveStale: 100}, opts)
	if decision.Action != teamsServiceWatchdogActionNoop || decision.Stale {
		t.Fatalf("decision = %+v, want non-stale noop for a live managed child awaiting owner", decision)
	}
	if !strings.Contains(decision.Reason, "child is alive") {
		t.Fatalf("reason = %q, want live-child diagnostic", decision.Reason)
	}
}

func TestTeamsServiceWatchdogStateRoundTripAndCorruptReset(t *testing.T) {
	lockCLITestHooks(t)

	path := filepath.Join(t.TempDir(), "watchdog.json")
	prevPath := teamsServiceWatchdogStatePath
	t.Cleanup(func() { teamsServiceWatchdogStatePath = prevPath })
	teamsServiceWatchdogStatePath = func() (string, error) { return path, nil }

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	want := teamsServiceWatchdogState{ConsecutiveStale: 2, LastReason: "stale", LastAction: teamsServiceWatchdogActionRestart, LastActionAt: now, UpdatedAt: now}
	if err := saveTeamsServiceWatchdogState(want); err != nil {
		t.Fatalf("saveTeamsServiceWatchdogState: %v", err)
	}
	got, err := loadTeamsServiceWatchdogState()
	if err != nil {
		t.Fatalf("loadTeamsServiceWatchdogState: %v", err)
	}
	if got.ConsecutiveStale != want.ConsecutiveStale || got.LastAction != want.LastAction || !got.LastActionAt.Equal(want.LastActionAt) {
		t.Fatalf("loaded state = %+v, want %+v", got, want)
	}

	if err := os.WriteFile(path, []byte("{broken"), 0o600); err != nil {
		t.Fatalf("write corrupt state: %v", err)
	}
	got, err = loadTeamsServiceWatchdogState()
	if err != nil {
		t.Fatalf("load corrupt state should reset without failing: %v", err)
	}
	if got != (teamsServiceWatchdogState{}) {
		t.Fatalf("corrupt state = %+v, want zero", got)
	}
}

func TestRunTeamsServiceWatchdogOnceStableNoopDoesNotRewriteState(t *testing.T) {
	lockCLITestHooks(t)

	firstNow := time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "watchdog.json")
	prevPath := teamsServiceWatchdogStatePath
	prevCollect := teamsServiceWatchdogCollectSnapshot
	t.Cleanup(func() {
		teamsServiceWatchdogStatePath = prevPath
		teamsServiceWatchdogCollectSnapshot = prevCollect
	})
	teamsServiceWatchdogStatePath = func() (string, error) { return path, nil }
	teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		return teamsServiceWatchdogSnapshot{Installed: false}, nil
	}

	first, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: firstNow})
	if err != nil {
		t.Fatalf("first watchdog run: %v", err)
	}
	if first.Decision.Action != teamsServiceWatchdogActionNoop {
		t.Fatalf("first watchdog action = %q, want noop", first.Decision.Action)
	}
	fixedModTime := firstNow.Add(-time.Hour)
	if err := os.Chtimes(path, fixedModTime, fixedModTime); err != nil {
		t.Fatalf("set watchdog sentinel mtime: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read watchdog state before stable run: %v", err)
	}

	second, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: firstNow.Add(5 * time.Second)})
	if err != nil {
		t.Fatalf("second watchdog run: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read watchdog state after stable run: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat watchdog state after stable run: %v", err)
	}
	if !bytes.Equal(before, after) || !info.ModTime().Equal(fixedModTime) {
		t.Fatalf("stable watchdog rewrote state: bytes_equal=%t mtime=%s want=%s", bytes.Equal(before, after), info.ModTime(), fixedModTime)
	}
	if !second.State.UpdatedAt.Equal(first.State.UpdatedAt) {
		t.Fatalf("stable watchdog advanced persisted timestamp: first=%s second=%s", first.State.UpdatedAt, second.State.UpdatedAt)
	}
}

func TestRunTeamsServiceWatchdogOnceStartsServiceAndDryRunDoesNot(t *testing.T) {
	lockCLITestHooks(t)

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "watchdog.json")
	prevPath := teamsServiceWatchdogStatePath
	prevCollect := teamsServiceWatchdogCollectSnapshot
	prevStart := teamsServiceWatchdogStartService
	t.Cleanup(func() {
		teamsServiceWatchdogStatePath = prevPath
		teamsServiceWatchdogCollectSnapshot = prevCollect
		teamsServiceWatchdogStartService = prevStart
	})
	teamsServiceWatchdogStatePath = func() (string, error) { return path, nil }
	teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		return teamsServiceWatchdogSnapshot{Installed: true, Active: false}, nil
	}

	startCalls := 0
	restartArg := false
	teamsServiceWatchdogStartService = func(_ context.Context, restart bool) error {
		startCalls++
		restartArg = restart
		return nil
	}

	result, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now, DryRun: true})
	if err != nil {
		t.Fatalf("dry-run watchdog: %v", err)
	}
	if result.Decision.Action != teamsServiceWatchdogActionStart {
		t.Fatalf("dry-run action = %q, want start", result.Decision.Action)
	}
	if startCalls != 0 {
		t.Fatalf("dry-run start calls = %d, want 0", startCalls)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("dry-run should not write watchdog state, stat err=%v", err)
	}

	result, err = runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now})
	if err != nil {
		t.Fatalf("watchdog: %v", err)
	}
	if result.Decision.Action != teamsServiceWatchdogActionStart {
		t.Fatalf("action = %q, want start", result.Decision.Action)
	}
	if startCalls != 1 || restartArg {
		t.Fatalf("start calls = %d restart=%t, want one non-restart start", startCalls, restartArg)
	}
	stored, err := loadTeamsServiceWatchdogState()
	if err != nil {
		t.Fatalf("load stored watchdog state: %v", err)
	}
	if stored.LastAction != teamsServiceWatchdogActionStart || !stored.LastActionAt.Equal(now) {
		t.Fatalf("stored state = %+v, want start at %s", stored, now)
	}
}

func TestRunTeamsServiceWatchdogOnceRestartsAfterConsecutiveStaleState(t *testing.T) {
	lockCLITestHooks(t)

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "watchdog.json")
	prevPath := teamsServiceWatchdogStatePath
	prevCollect := teamsServiceWatchdogCollectSnapshot
	prevStart := teamsServiceWatchdogStartService
	t.Cleanup(func() {
		teamsServiceWatchdogStatePath = prevPath
		teamsServiceWatchdogCollectSnapshot = prevCollect
		teamsServiceWatchdogStartService = prevStart
	})
	teamsServiceWatchdogStatePath = func() (string, error) { return path, nil }
	teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		return teamsServiceWatchdogSnapshot{
			Installed:          true,
			Active:             true,
			StateFiles:         1,
			OwnerFound:         true,
			LastOwnerHeartbeat: now.Add(-3 * time.Minute),
		}, nil
	}
	if err := saveTeamsServiceWatchdogState(teamsServiceWatchdogState{ConsecutiveStale: 2, LastReason: "previous stale", UpdatedAt: now.Add(-10 * time.Second)}); err != nil {
		t.Fatalf("seed watchdog state: %v", err)
	}

	restartCalls := 0
	restartArg := false
	teamsServiceWatchdogStartService = func(_ context.Context, restart bool) error {
		restartCalls++
		restartArg = restart
		return nil
	}

	result, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now, DryRun: true})
	if err != nil {
		t.Fatalf("dry-run watchdog: %v", err)
	}
	if result.Decision.Action != teamsServiceWatchdogActionRestart {
		t.Fatalf("dry-run action = %q, want restart", result.Decision.Action)
	}
	if restartCalls != 0 {
		t.Fatalf("dry-run restart calls = %d, want 0", restartCalls)
	}

	result, err = runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now})
	if err != nil {
		t.Fatalf("watchdog: %v", err)
	}
	if result.Decision.Action != teamsServiceWatchdogActionRestart {
		t.Fatalf("action = %q, want restart", result.Decision.Action)
	}
	if restartCalls != 1 || !restartArg {
		t.Fatalf("restart calls = %d restart=%t, want one restart", restartCalls, restartArg)
	}
	stored, err := loadTeamsServiceWatchdogState()
	if err != nil {
		t.Fatalf("load stored watchdog state: %v", err)
	}
	if stored.LastAction != teamsServiceWatchdogActionRestart || !stored.LastActionAt.Equal(now) || stored.ConsecutiveStale != 3 {
		t.Fatalf("stored state = %+v, want restart action at %s with consecutive stale 3", stored, now)
	}
}

func TestRunTeamsServiceWatchdogOnceSuccessfulRestartCooldownSuppressesRestartStorm(t *testing.T) {
	lockCLITestHooks(t)

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "watchdog.json")
	prevPath := teamsServiceWatchdogStatePath
	prevCollect := teamsServiceWatchdogCollectSnapshot
	prevStart := teamsServiceWatchdogStartService
	t.Cleanup(func() {
		teamsServiceWatchdogStatePath = prevPath
		teamsServiceWatchdogCollectSnapshot = prevCollect
		teamsServiceWatchdogStartService = prevStart
	})
	teamsServiceWatchdogStatePath = func() (string, error) { return path, nil }
	teamsServiceWatchdogCollectSnapshot = func(_ context.Context, opts teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		return teamsServiceWatchdogSnapshot{
			Installed:          true,
			Active:             true,
			StateFiles:         1,
			OwnerFound:         true,
			LastOwnerHeartbeat: opts.Now.Add(-3 * time.Minute),
		}, nil
	}
	if err := saveTeamsServiceWatchdogState(teamsServiceWatchdogState{ConsecutiveStale: 2, LastReason: "previous stale", UpdatedAt: now.Add(-10 * time.Second)}); err != nil {
		t.Fatalf("seed watchdog state: %v", err)
	}
	restartCalls := 0
	teamsServiceWatchdogStartService = func(_ context.Context, restart bool) error {
		restartCalls++
		if !restart {
			t.Fatalf("restart flag = false, want true")
		}
		return nil
	}

	first, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now})
	if err != nil {
		t.Fatalf("first watchdog: %v", err)
	}
	if first.Decision.Action != teamsServiceWatchdogActionRestart || restartCalls != 1 {
		t.Fatalf("first decision=%+v restartCalls=%d, want restart", first.Decision, restartCalls)
	}
	second, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now.Add(10 * time.Second)})
	if err != nil {
		t.Fatalf("second watchdog: %v", err)
	}
	if second.Decision.Action != teamsServiceWatchdogActionNoop || second.Decision.CooldownUntil.IsZero() {
		t.Fatalf("second decision=%+v, want cooldown noop", second.Decision)
	}
	if restartCalls != 1 {
		t.Fatalf("restart calls = %d, want cooldown to suppress second restart", restartCalls)
	}
}

func TestRunTeamsServiceWatchdogOnceDoesNotCooldownFailedRestart(t *testing.T) {
	lockCLITestHooks(t)

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "watchdog.json")
	prevPath := teamsServiceWatchdogStatePath
	prevCollect := teamsServiceWatchdogCollectSnapshot
	prevStart := teamsServiceWatchdogStartService
	t.Cleanup(func() {
		teamsServiceWatchdogStatePath = prevPath
		teamsServiceWatchdogCollectSnapshot = prevCollect
		teamsServiceWatchdogStartService = prevStart
	})
	teamsServiceWatchdogStatePath = func() (string, error) { return path, nil }
	teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		return teamsServiceWatchdogSnapshot{
			Installed:          true,
			Active:             true,
			StateFiles:         1,
			OwnerFound:         true,
			LastOwnerHeartbeat: now.Add(-3 * time.Minute),
		}, nil
	}
	if err := saveTeamsServiceWatchdogState(teamsServiceWatchdogState{ConsecutiveStale: 2, LastReason: "previous stale", UpdatedAt: now.Add(-10 * time.Second)}); err != nil {
		t.Fatalf("seed watchdog state: %v", err)
	}

	restartCalls := 0
	restartErr := errors.New("restart timed out")
	teamsServiceWatchdogStartService = func(_ context.Context, restart bool) error {
		restartCalls++
		if !restart {
			t.Fatalf("restart flag = false, want true")
		}
		if restartCalls == 1 {
			return restartErr
		}
		return nil
	}

	result, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now})
	if !errors.Is(err, restartErr) {
		t.Fatalf("watchdog err = %v, want %v", err, restartErr)
	}
	if result.State.LastActionAt.IsZero() == false || result.State.LastAction != "" {
		t.Fatalf("failed restart result state = %+v, want no action cooldown marker", result.State)
	}
	stored, err := loadTeamsServiceWatchdogState()
	if err != nil {
		t.Fatalf("load failed-restart state: %v", err)
	}
	if stored.ConsecutiveStale != 3 || stored.LastAction != "" || !stored.LastActionAt.IsZero() {
		t.Fatalf("failed-restart stored state = %+v, want consecutive stale without cooldown action", stored)
	}

	result, err = runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now.Add(10 * time.Second)})
	if err != nil {
		t.Fatalf("retry watchdog: %v", err)
	}
	if restartCalls != 2 || result.Decision.Action != teamsServiceWatchdogActionRestart {
		t.Fatalf("retry calls=%d decision=%+v, want immediate restart retry", restartCalls, result.Decision)
	}
	stored, err = loadTeamsServiceWatchdogState()
	if err != nil {
		t.Fatalf("load retry state: %v", err)
	}
	if stored.LastAction != teamsServiceWatchdogActionRestart || !stored.LastActionAt.Equal(now.Add(10*time.Second)) {
		t.Fatalf("retry stored state = %+v, want successful restart marker", stored)
	}
}

func TestRunTeamsServiceWatchdogLoopRepeatsAndLogsQuietErrors(t *testing.T) {
	lockCLITestHooks(t)

	path := filepath.Join(t.TempDir(), "watchdog.json")
	prevPath := teamsServiceWatchdogStatePath
	prevCollect := teamsServiceWatchdogCollectSnapshot
	prevStart := teamsServiceWatchdogStartService
	t.Cleanup(func() {
		teamsServiceWatchdogStatePath = prevPath
		teamsServiceWatchdogCollectSnapshot = prevCollect
		teamsServiceWatchdogStartService = prevStart
	})
	teamsServiceWatchdogStatePath = func() (string, error) { return path, nil }
	teamsServiceWatchdogStartService = func(context.Context, bool) error {
		t.Fatal("watchdog loop should not start service when snapshot collection fails")
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return teamsServiceWatchdogSnapshot{}, errors.New("snapshot unavailable")
	}

	var out strings.Builder
	var errOut strings.Builder
	err := runTeamsServiceWatchdogLoop(ctx, teamsServiceWatchdogOptions{}, 10*time.Millisecond, true, &out, &errOut)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("loop error = %v, want context canceled", err)
	}
	if calls != 2 {
		t.Fatalf("snapshot calls = %d, want 2", calls)
	}
	if out.String() != "" {
		t.Fatalf("quiet loop wrote stdout: %q", out.String())
	}
	if got := errOut.String(); strings.Count(got, "snapshot unavailable") != 2 {
		t.Fatalf("quiet loop stderr = %q, want two error logs", got)
	}
}

func TestStartTeamsPrimaryServiceDoesNotTouchWatchdogSchedule(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hooks teamsServiceTestHooks
	}{
		{
			name: "systemd",
			hooks: teamsServiceTestHooks{
				goos:    "linux",
				exe:     "/tmp/codex-proxy",
				cwd:     "/tmp",
				unitDir: filepath.Join(t.TempDir(), "systemd", "user"),
			},
		},
		{
			name: "launchagent",
			hooks: teamsServiceTestHooks{
				goos:           "darwin",
				exe:            "/tmp/codex-proxy",
				cwd:            "/tmp",
				launchAgentDir: filepath.Join(t.TempDir(), "LaunchAgents"),
				userID:         "501",
			},
		},
		{
			name: "windows",
			hooks: teamsServiceTestHooks{
				goos:           "windows",
				exe:            `C:\codex-proxy.exe`,
				cwd:            `C:\work`,
				windowsTaskDir: filepath.Join(t.TempDir(), "tasks"),
			},
		},
		{
			name: "wsl",
			hooks: teamsServiceTestHooks{
				goos:           "linux",
				exe:            "/home/alice/bin/codex-proxy",
				cwd:            "/home/alice/work",
				windowsTaskDir: filepath.Join(t.TempDir(), "wsl-task"),
				isWSL:          true,
				wslDistro:      "Ubuntu",
				wslLinuxUser:   "alice",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lockCLITestHooks(t)
			runner := &recordingTeamsServiceRunner{output: []byte("ok")}
			tc.hooks.runner = runner
			withTeamsServiceTestHooks(t, tc.hooks)

			if err := startTeamsPrimaryService(context.Background(), true); err != nil {
				t.Fatalf("startTeamsPrimaryService: %v", err)
			}
			if len(runner.calls) == 0 {
				t.Fatal("startTeamsPrimaryService made no supervisor calls")
			}
			joined := strings.ToLower(fmt.Sprint(runner.calls))
			for _, forbidden := range []string{
				strings.ToLower(teamsServiceWatchdogTimerName),
				strings.ToLower(teamsServiceLaunchAgentWatchdogLabel),
				strings.ToLower(teamsServiceWindowsWatchdogTaskName),
				"teams watchdog",
			} {
				if strings.Contains(joined, forbidden) {
					t.Fatalf("primary service restart touched watchdog schedule %q: %#v", forbidden, runner.calls)
				}
			}
		})
	}
}
