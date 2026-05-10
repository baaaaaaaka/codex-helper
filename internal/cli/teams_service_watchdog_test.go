package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestTeamsServiceWatchdogDefaultsFitThirtySecondRecoveryBudget(t *testing.T) {
	if got := defaultTeamsServiceWatchdogOwnerStaleAfter + teamsServiceExternalWatchdogInterval; got > 30*time.Second {
		t.Fatalf("owner stale recovery budget = %s, want <= 30s", got)
	}
	if got := defaultTeamsServiceWatchdogPollStaleAfter + teamsServiceExternalWatchdogInterval; got > 30*time.Second {
		t.Fatalf("poll stale recovery budget = %s, want <= 30s", got)
	}
	if teamsServiceExternalWatchdogInterval != 10*time.Second {
		t.Fatalf("watchdog interval = %s, want 10s", teamsServiceExternalWatchdogInterval)
	}
	if teamsServiceExternalWatchdogCheckTimeout > 20*time.Second {
		t.Fatalf("watchdog check timeout = %s, want <= 20s", teamsServiceExternalWatchdogCheckTimeout)
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

func TestTeamsServiceWatchdogRestartsOnFirstStaleSample(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:          true,
		Active:             true,
		StateFiles:         1,
		OwnerFound:         true,
		LastOwnerHeartbeat: now.Add(-19 * time.Second),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, opts)
	if decision.Action != teamsServiceWatchdogActionRestart || !decision.Stale || decision.ConsecutiveStale != 1 {
		t.Fatalf("stale decision = %+v, want immediate restart stale count 1", decision)
	}
}

func TestTeamsServiceWatchdogTreatsPollErrorAsRecentActivity(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:         true,
		Active:            true,
		StateFiles:        1,
		PollActivityFound: true,
		PollActivityAt:    now.Add(-10 * time.Second),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, opts)
	if decision.Action != teamsServiceWatchdogActionNoop || decision.Stale {
		t.Fatalf("decision = %+v, want noop because recent poll error/success means the helper is still moving", decision)
	}
	if !strings.Contains(decision.Reason, "recent") {
		t.Fatalf("reason = %q, want recent activity diagnostic", decision.Reason)
	}
}

func TestTeamsServiceWatchdogRestartsWhenControlPollStaleDespiteFreshOwner(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:           true,
		Active:              true,
		StateFiles:          1,
		OwnerFound:          true,
		OwnerFresh:          true,
		LastOwnerHeartbeat:  now.Add(-5 * time.Second),
		FreshOwnerStartedAt: now.Add(-30 * time.Minute),
		PollActivityFound:   true,
		PollActivityAt:      now.Add(-21 * time.Second),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, opts)
	if decision.Action != teamsServiceWatchdogActionRestart {
		t.Fatalf("decision = %+v, want restart for stale control polling", decision)
	}
}

func TestTeamsServiceWatchdogDoesNotRestartFreshOwnerWithActiveTurn(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:          true,
		Active:             true,
		StateFiles:         1,
		OwnerFound:         true,
		OwnerFresh:         true,
		OwnerActiveTurn:    true,
		LastOwnerHeartbeat: now.Add(-30 * time.Second),
		PollActivityFound:  true,
		PollActivityAt:     now.Add(-30 * time.Minute),
	}
	state := teamsServiceWatchdogState{ConsecutiveStale: 1}

	decision := evaluateTeamsServiceWatchdog(snapshot, state, opts)
	if decision.Action != teamsServiceWatchdogActionNoop || decision.Stale {
		t.Fatalf("decision = %+v, want noop while an active turn is heartbeating", decision)
	}
}

func TestTeamsServiceWatchdogPollActivityUsesControlPollError(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	state := teamsstore.State{
		ControlChat: teamsstore.ControlChatBinding{TeamsChatID: "control-chat"},
		ChatPolls: map[string]teamsstore.ChatPollState{
			"control-chat": {ChatID: "control-chat", LastErrorAt: now.Add(-time.Minute)},
			"work-chat":    {ChatID: "work-chat", LastSuccessfulPollAt: now.Add(time.Hour)},
		},
	}

	activity, ok := teamsServiceWatchdogPollActivity(state, now)
	if !ok || !activity.Equal(now.Add(-time.Minute)) {
		t.Fatalf("activity = %s ok=%t, want control poll error time", activity, ok)
	}
}

func TestTeamsServiceWatchdogPollActivityTreatsFutureBlockedUntilAsActivity(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	state := teamsstore.State{
		ControlChat: teamsstore.ControlChatBinding{TeamsChatID: "control-chat"},
		ChatPolls: map[string]teamsstore.ChatPollState{
			"control-chat": {ChatID: "control-chat", LastErrorAt: now.Add(-2 * time.Minute), BlockedUntil: now.Add(time.Minute)},
		},
	}

	activity, ok := teamsServiceWatchdogPollActivity(state, now)
	if !ok || !activity.Equal(now) {
		t.Fatalf("activity = %s ok=%t, want current time while poll is intentionally blocked", activity, ok)
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
