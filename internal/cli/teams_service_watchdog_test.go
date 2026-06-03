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

const wantTeamsServiceWatchdogLifecycleDrainClearAction = "clear"

func TestTeamsServiceWatchdogDefaultsAvoidTransientRestartLoops(t *testing.T) {
	if defaultTeamsServiceWatchdogOwnerStaleAfter != 90*time.Second {
		t.Fatalf("owner stale threshold = %s, want 90s", defaultTeamsServiceWatchdogOwnerStaleAfter)
	}
	if defaultTeamsServiceWatchdogPollStaleAfter != 2*time.Minute {
		t.Fatalf("poll stale threshold = %s, want 2m", defaultTeamsServiceWatchdogPollStaleAfter)
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
		PollActivityAt:      now.Add(-3 * time.Minute),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{ConsecutiveStale: 2}, opts)
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

func TestTeamsServiceWatchdogLifecycleStatePrecedenceMatrix(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	for _, tc := range []struct {
		name     string
		snapshot teamsServiceWatchdogSnapshot
		want     string
		reason   string
	}{
		{
			name:     "paused drain without recoverable evidence remains quiet",
			snapshot: teamsServiceWatchdogSnapshot{Installed: true, Active: true, StateFiles: 1, ServicePaused: true, ServiceDraining: true},
			want:     teamsServiceWatchdogActionNoop,
			reason:   "paused",
		},
		{
			name:     "fresh helper reload drain remains quiet",
			snapshot: teamsServiceWatchdogSnapshot{Installed: true, Active: true, StateFiles: 1, ServiceDraining: true},
			want:     teamsServiceWatchdogActionNoop,
			reason:   "draining",
		},
		{
			name:     "manual drain remains quiet",
			snapshot: teamsServiceWatchdogSnapshot{Installed: true, Active: true, StateFiles: 1, ServiceDraining: true, OwnerFound: true},
			want:     teamsServiceWatchdogActionNoop,
			reason:   "draining",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decision := evaluateTeamsServiceWatchdog(tc.snapshot, teamsServiceWatchdogState{}, opts)
			if decision.Action != tc.want || !strings.Contains(decision.Reason, tc.reason) {
				t.Fatalf("decision = %+v, want action=%s reason containing %q", decision, tc.want, tc.reason)
			}
		})
	}
}

func TestTeamsServiceWatchdogClearsExpiredHelperUpgradeDrainWithIdleLocalOwner(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:                         true,
		Active:                            true,
		StateFiles:                        1,
		ServiceDraining:                   true,
		HelperUpgradeDrainExpired:         true,
		HelperUpgradeDrainLocalOwnerFresh: true,
		OwnerFound:                        true,
		OwnerFresh:                        true,
		LastOwnerHeartbeat:                now.Add(-5 * time.Second),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{ConsecutiveStale: 2}, opts)
	if decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction || !decision.CooldownUntil.IsZero() {
		t.Fatalf("decision = %+v, want clear for expired helper upgrade drain without restart cooldown", decision)
	}
}

func TestTeamsServiceWatchdogDoesNotRestartExpiredHelperUpgradeDrainWithRemoteFreshOwner(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:                          true,
		Active:                             true,
		StateFiles:                         1,
		ServiceDraining:                    true,
		HelperUpgradeDrainExpired:          true,
		HelperUpgradeDrainRemoteOwnerFresh: true,
		OwnerFound:                         true,
		OwnerFresh:                         true,
		LastOwnerHeartbeat:                 now.Add(-5 * time.Second),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, opts)
	if decision.Action != teamsServiceWatchdogActionNoop || !strings.Contains(decision.Reason, "another machine") {
		t.Fatalf("decision = %+v, want noop for fresh remote owner", decision)
	}
}

func TestTeamsServiceWatchdogClearableUpgradeDrainWinsOverUnrelatedRemoteEvidence(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:                          true,
		Active:                             true,
		StateFiles:                         1,
		ServiceDraining:                    true,
		HelperUpgradeDrainExpired:          true,
		HelperUpgradeDrainLocalOwnerFresh:  true,
		HelperUpgradeDrainRemoteOwnerFresh: true,
		OwnerFound:                         true,
		OwnerFresh:                         true,
		LastOwnerHeartbeat:                 now.Add(-5 * time.Second),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, opts)
	if decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction {
		t.Fatalf("decision = %+v, want clearable local upgrade drain to win over unrelated remote evidence", decision)
	}
}

func TestTeamsServiceWatchdogClearableReloadDrainWinsOverUnrelatedRemoteEvidence(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:                         true,
		Active:                            true,
		StateFiles:                        2,
		ServiceDraining:                   true,
		HelperReloadDrainStale:            true,
		HelperReloadDrainLocalOwnerFresh:  true,
		HelperReloadDrainRemoteOwnerFresh: true,
		OwnerFound:                        true,
		OwnerFresh:                        true,
		LastOwnerHeartbeat:                now.Add(-5 * time.Second),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, opts)
	if decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction {
		t.Fatalf("decision = %+v, want clearable local reload drain to win over unrelated remote evidence", decision)
	}
}

func TestTeamsServiceWatchdogDoesNotRestartExpiredHelperUpgradeDrainWithActiveTurn(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:                         true,
		Active:                            true,
		StateFiles:                        1,
		ServiceDraining:                   true,
		HelperUpgradeDrainExpired:         true,
		HelperUpgradeDrainLocalOwnerFresh: true,
		OwnerFound:                        true,
		OwnerFresh:                        true,
		OwnerActiveTurn:                   true,
		LastOwnerHeartbeat:                now.Add(-5 * time.Second),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, opts)
	if decision.Action != teamsServiceWatchdogActionNoop || !strings.Contains(decision.Reason, "active turn") {
		t.Fatalf("decision = %+v, want noop for active turn", decision)
	}
}

func TestTeamsServiceWatchdogExpiredHelperUpgradeDrainIgnoresRestartCooldown(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:                         true,
		Active:                            true,
		StateFiles:                        1,
		ServiceDraining:                   true,
		HelperUpgradeDrainExpired:         true,
		HelperUpgradeDrainLocalOwnerFresh: true,
		OwnerFound:                        true,
		OwnerFresh:                        true,
		LastOwnerHeartbeat:                now.Add(-5 * time.Second),
	}
	state := teamsServiceWatchdogState{ConsecutiveStale: 2, LastActionAt: now.Add(-10 * time.Second)}

	decision := evaluateTeamsServiceWatchdog(snapshot, state, opts)
	if decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction || !decision.CooldownUntil.IsZero() {
		t.Fatalf("decision = %+v, want clear without restart cooldown", decision)
	}
}

func TestTeamsServiceWatchdogClearsStaleHelperReloadDrainWithIdleLocalOwner(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:                        true,
		Active:                           true,
		StateFiles:                       1,
		ServiceDraining:                  true,
		HelperReloadDrainStale:           true,
		HelperReloadDrainLocalOwnerFresh: true,
		OwnerFound:                       true,
		OwnerFresh:                       true,
		LastOwnerHeartbeat:               now.Add(-5 * time.Second),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{ConsecutiveStale: 2}, opts)
	if decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction || !decision.CooldownUntil.IsZero() {
		t.Fatalf("decision = %+v, want clear for stale helper reload drain without restart cooldown", decision)
	}
}

func TestTeamsServiceWatchdogDoesNotRestartStaleHelperReloadDrainWithRemoteFreshOwner(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:                         true,
		Active:                            true,
		StateFiles:                        1,
		ServiceDraining:                   true,
		HelperReloadDrainStale:            true,
		HelperReloadDrainRemoteOwnerFresh: true,
		OwnerFound:                        true,
		OwnerFresh:                        true,
		LastOwnerHeartbeat:                now.Add(-5 * time.Second),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, opts)
	if decision.Action != teamsServiceWatchdogActionNoop || !strings.Contains(decision.Reason, "another machine") {
		t.Fatalf("decision = %+v, want noop for fresh remote owner", decision)
	}
}

func TestTeamsServiceWatchdogDoesNotRestartStaleHelperReloadDrainWithActiveTurn(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:                        true,
		Active:                           true,
		StateFiles:                       1,
		ServiceDraining:                  true,
		HelperReloadDrainStale:           true,
		HelperReloadDrainLocalOwnerFresh: true,
		OwnerFound:                       true,
		OwnerFresh:                       true,
		OwnerActiveTurn:                  true,
		LastOwnerHeartbeat:               now.Add(-5 * time.Second),
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, opts)
	if decision.Action != teamsServiceWatchdogActionNoop || !strings.Contains(decision.Reason, "active turn") {
		t.Fatalf("decision = %+v, want noop for active turn", decision)
	}
}

func TestTeamsServiceWatchdogStaleHelperReloadDrainIgnoresRestartCooldown(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:                        true,
		Active:                           true,
		StateFiles:                       1,
		ServiceDraining:                  true,
		HelperReloadDrainStale:           true,
		HelperReloadDrainLocalOwnerFresh: true,
		OwnerFound:                       true,
		OwnerFresh:                       true,
		LastOwnerHeartbeat:               now.Add(-5 * time.Second),
	}
	state := teamsServiceWatchdogState{ConsecutiveStale: 2, LastActionAt: now.Add(-10 * time.Second)}

	decision := evaluateTeamsServiceWatchdog(snapshot, state, opts)
	if decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction || !decision.CooldownUntil.IsZero() {
		t.Fatalf("decision = %+v, want clear without restart cooldown", decision)
	}
}

func TestTeamsServiceWatchdogClearsLifecycleDrainBeforeStartingInactiveService(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	snapshot := teamsServiceWatchdogSnapshot{
		Installed:                 true,
		Active:                    false,
		StateFiles:                1,
		ServiceDraining:           true,
		HelperUpgradeDrainExpired: true,
	}

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, opts)
	if decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction {
		t.Fatalf("decision = %+v, want clear before inactive service start", decision)
	}
}

func TestTeamsServiceWatchdogMergeDetectsExpiredHelperUpgradeDrainLocalOwner(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname error: %v", err)
	}
	owner := teamsstore.OwnerMetadata{
		PID:             os.Getpid(),
		Hostname:        hostname,
		HelperVersion:   "v0.1.0-rc.87",
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeat:   now.Add(-5 * time.Second),
		ActiveSessionID: "s002",
	}
	state := teamsstore.State{
		ServiceControl: teamsstore.ServiceControl{Draining: true, Reason: teamsstore.HelperUpgradeReason},
		Upgrade: &teamsstore.UpgradeRequest{
			ID:         "upgrade-1",
			Phase:      teamsstore.UpgradePhaseDraining,
			Reason:     teamsstore.HelperUpgradeReason,
			DeadlineAt: now.Add(-time.Minute),
		},
		ServiceOwner: &owner,
		LockOwner:    &owner,
	}
	var snapshot teamsServiceWatchdogSnapshot
	snapshot.Installed = true
	snapshot.Active = true
	snapshot.StateFiles = 1

	mergeTeamsServiceWatchdogState(&snapshot, state, normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now}))
	if !snapshot.ServiceDraining || !snapshot.HelperUpgradeDrainExpired || !snapshot.HelperUpgradeDrainLocalOwnerFresh {
		t.Fatalf("snapshot did not detect local expired helper upgrade drain: %+v", snapshot)
	}
}

func TestTeamsServiceWatchdogMergeDetectsExpiredHelperUpgradeDrainRemoteOwner(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname error: %v", err)
	}
	owner := teamsstore.OwnerMetadata{
		PID:             4242,
		Hostname:        hostname + "-remote",
		HelperVersion:   "v0.1.0-rc.87",
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeat:   now.Add(-5 * time.Second),
		ActiveSessionID: "s002",
	}
	state := teamsstore.State{
		ServiceControl: teamsstore.ServiceControl{Draining: true, Reason: teamsstore.HelperUpgradeReason},
		Upgrade: &teamsstore.UpgradeRequest{
			ID:         "upgrade-1",
			Phase:      teamsstore.UpgradePhaseDraining,
			Reason:     teamsstore.HelperUpgradeReason,
			DeadlineAt: now.Add(-time.Minute),
		},
		ServiceOwner: &owner,
		LockOwner:    &owner,
	}
	var snapshot teamsServiceWatchdogSnapshot
	snapshot.Installed = true
	snapshot.Active = true
	snapshot.StateFiles = 1

	mergeTeamsServiceWatchdogState(&snapshot, state, normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now}))
	if !snapshot.ServiceDraining || !snapshot.HelperUpgradeDrainExpired || !snapshot.HelperUpgradeDrainRemoteOwnerFresh || snapshot.HelperUpgradeDrainLocalOwnerFresh {
		t.Fatalf("snapshot did not detect remote expired helper upgrade drain owner: %+v", snapshot)
	}
}

func TestTeamsServiceWatchdogUnrelatedActiveTurnDoesNotBlockZombieDrainClear(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname error: %v", err)
	}
	zombieRootState := teamsstore.State{
		ServiceControl: teamsstore.ServiceControl{Draining: true, Reason: teamsstore.HelperUpgradeReason},
		Upgrade: &teamsstore.UpgradeRequest{
			ID:         "upgrade-root-zombie",
			Phase:      teamsstore.UpgradePhaseDraining,
			Reason:     teamsstore.HelperUpgradeReason,
			DeadlineAt: now.Add(-time.Minute),
		},
	}
	activeScopeOwner := teamsstore.OwnerMetadata{
		PID:             os.Getpid(),
		Hostname:        hostname,
		HelperVersion:   "v0.1.0-rc.87",
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeat:   now.Add(-5 * time.Second),
		ActiveSessionID: "s002",
		ActiveTurnID:    "turn-active",
	}
	activeScopeState := teamsstore.State{
		ServiceOwner: &activeScopeOwner,
		LockOwner:    &activeScopeOwner,
	}
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	var snapshot teamsServiceWatchdogSnapshot
	snapshot.Installed = true
	snapshot.Active = true
	snapshot.StateFiles = 2
	mergeTeamsServiceWatchdogState(&snapshot, zombieRootState, opts)
	mergeTeamsServiceWatchdogState(&snapshot, activeScopeState, opts)

	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{ConsecutiveStale: 2}, opts)
	if decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction {
		t.Fatalf("decision = %+v snapshot=%+v, want clear for root zombie drain despite unrelated active turn", decision, snapshot)
	}
}

func TestTeamsServiceWatchdogMergeDetectsStaleHelperReloadDrainLocalOwner(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname error: %v", err)
	}
	owner := teamsstore.OwnerMetadata{
		PID:             os.Getpid(),
		Hostname:        hostname,
		HelperVersion:   "v0.1.0-rc.87",
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeat:   now.Add(-5 * time.Second),
		ActiveSessionID: "s002",
	}
	state := teamsstore.State{
		ServiceControl: teamsstore.ServiceControl{
			Draining:  true,
			Reason:    teamsstore.HelperReloadReason,
			UpdatedAt: now.Add(-defaultTeamsServiceWatchdogReloadStaleAfter - time.Minute),
		},
		ServiceOwner: &owner,
		LockOwner:    &owner,
	}
	var snapshot teamsServiceWatchdogSnapshot

	mergeTeamsServiceWatchdogState(&snapshot, state, normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now}))
	if !snapshot.ServiceDraining || !snapshot.HelperReloadDrainStale || !snapshot.HelperReloadDrainLocalOwnerFresh {
		t.Fatalf("snapshot did not detect local stale helper reload drain: %+v", snapshot)
	}
}

func TestTeamsServiceWatchdogMergeDetectsStaleHelperReloadDrainRemoteOwner(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname error: %v", err)
	}
	owner := teamsstore.OwnerMetadata{
		PID:             4242,
		Hostname:        hostname + "-remote",
		HelperVersion:   "v0.1.0-rc.87",
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeat:   now.Add(-5 * time.Second),
		ActiveSessionID: "s002",
	}
	state := teamsstore.State{
		ServiceControl: teamsstore.ServiceControl{
			Draining:  true,
			Reason:    teamsstore.HelperReloadReason,
			UpdatedAt: now.Add(-defaultTeamsServiceWatchdogReloadStaleAfter - time.Minute),
		},
		ServiceOwner: &owner,
		LockOwner:    &owner,
	}
	var snapshot teamsServiceWatchdogSnapshot

	mergeTeamsServiceWatchdogState(&snapshot, state, normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now}))
	if !snapshot.ServiceDraining || !snapshot.HelperReloadDrainStale || !snapshot.HelperReloadDrainRemoteOwnerFresh || snapshot.HelperReloadDrainLocalOwnerFresh {
		t.Fatalf("snapshot did not detect remote stale helper reload drain owner: %+v", snapshot)
	}
}

func TestTeamsServiceWatchdogMergeDoesNotRecoverPausedStateDrain(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname error: %v", err)
	}
	owner := teamsstore.OwnerMetadata{
		PID:           os.Getpid(),
		Hostname:      hostname,
		HelperVersion: "v0.1.0-rc.87",
		StartedAt:     now.Add(-time.Hour),
		LastHeartbeat: now.Add(-5 * time.Second),
	}
	state := teamsstore.State{
		ServiceControl: teamsstore.ServiceControl{
			Paused:    true,
			Draining:  true,
			Reason:    teamsstore.HelperReloadReason,
			UpdatedAt: now.Add(-defaultTeamsServiceWatchdogReloadStaleAfter - time.Minute),
		},
		ServiceOwner: &owner,
		LockOwner:    &owner,
	}
	var snapshot teamsServiceWatchdogSnapshot
	snapshot.Installed = true
	snapshot.Active = true
	snapshot.StateFiles = 1

	mergeTeamsServiceWatchdogState(&snapshot, state, normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now}))
	if !snapshot.ServicePaused || !snapshot.ServiceDraining {
		t.Fatalf("paused draining state was not reflected: %+v", snapshot)
	}
	if snapshot.HelperReloadDrainStale || snapshot.HelperReloadDrainLocalOwnerFresh || snapshot.HelperReloadDrainRemoteOwnerFresh {
		t.Fatalf("paused state drain should not be considered recoverable: %+v", snapshot)
	}
	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now}))
	if decision.Action != teamsServiceWatchdogActionNoop || !strings.Contains(decision.Reason, "paused") {
		t.Fatalf("decision = %+v, want paused noop", decision)
	}
}

func TestTeamsServiceWatchdogRecoverableDrainWinsOverSeparatePausedState(t *testing.T) {
	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname error: %v", err)
	}
	owner := teamsstore.OwnerMetadata{
		PID:           os.Getpid(),
		Hostname:      hostname,
		HelperVersion: "v0.1.0-rc.87",
		StartedAt:     now.Add(-time.Hour),
		LastHeartbeat: now.Add(-5 * time.Second),
	}
	pausedState := teamsstore.State{
		ServiceControl: teamsstore.ServiceControl{
			Paused:    true,
			Reason:    "manual pause",
			UpdatedAt: now.Add(-time.Minute),
		},
	}
	staleReloadState := teamsstore.State{
		ServiceControl: teamsstore.ServiceControl{
			Draining:  true,
			Reason:    teamsstore.HelperReloadReason,
			UpdatedAt: now.Add(-defaultTeamsServiceWatchdogReloadStaleAfter - time.Minute),
		},
		ServiceOwner: &owner,
		LockOwner:    &owner,
	}
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	var snapshot teamsServiceWatchdogSnapshot
	snapshot.Installed = true
	snapshot.Active = true
	snapshot.StateFiles = 2
	mergeTeamsServiceWatchdogState(&snapshot, pausedState, opts)
	mergeTeamsServiceWatchdogState(&snapshot, staleReloadState, opts)

	if !snapshot.ServicePaused || !snapshot.HelperReloadDrainStale || !snapshot.HelperReloadDrainLocalOwnerFresh {
		t.Fatalf("snapshot did not retain both paused and recoverable drain evidence: %+v", snapshot)
	}
	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{ConsecutiveStale: 2}, opts)
	if decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction {
		t.Fatalf("decision = %+v, want clear for recoverable drain despite separate paused state", decision)
	}
}

func TestTeamsServiceWatchdogManyStateFilesRecoveryMatrix(t *testing.T) {
	now := time.Date(2026, 5, 16, 2, 0, 0, 0, time.UTC)
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname error: %v", err)
	}
	localOwner := teamsstore.OwnerMetadata{
		PID:           os.Getpid(),
		Hostname:      hostname,
		HelperVersion: "v0.1.0-rc.87",
		StartedAt:     now.Add(-time.Hour),
		LastHeartbeat: now.Add(-5 * time.Second),
	}
	remoteOwner := teamsstore.OwnerMetadata{
		PID:           4242,
		Hostname:      hostname + "-remote",
		HelperVersion: "v0.1.0-rc.87",
		StartedAt:     now.Add(-time.Hour),
		LastHeartbeat: now.Add(-5 * time.Second),
	}
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: now})
	pausedState := teamsstore.State{
		ServiceControl: teamsstore.ServiceControl{
			Paused:    true,
			Reason:    "manual pause in unrelated scope",
			UpdatedAt: now.Add(-time.Minute),
		},
	}
	staleReloadState := func(owner teamsstore.OwnerMetadata) teamsstore.State {
		return teamsstore.State{
			ServiceControl: teamsstore.ServiceControl{
				Draining:  true,
				Reason:    teamsstore.HelperReloadReason,
				UpdatedAt: now.Add(-defaultTeamsServiceWatchdogReloadStaleAfter - time.Minute),
			},
			ServiceOwner: &owner,
			LockOwner:    &owner,
		}
	}

	for _, tc := range []struct {
		name             string
		localActiveTurn  bool
		includeRemote    bool
		wantAction       string
		wantReasonSubstr string
	}{
		{
			name:             "local stale reload drain recovers despite many paused scopes",
			wantAction:       wantTeamsServiceWatchdogLifecycleDrainClearAction,
			wantReasonSubstr: "helper reload drain is stale",
		},
		{
			name:             "fresh local active turn still prevents recovery",
			localActiveTurn:  true,
			wantAction:       teamsServiceWatchdogActionNoop,
			wantReasonSubstr: "active turn",
		},
		{
			name:             "fresh remote owner in another store does not block local drain clear",
			includeRemote:    true,
			wantAction:       wantTeamsServiceWatchdogLifecycleDrainClearAction,
			wantReasonSubstr: "helper reload drain is stale",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := teamsServiceWatchdogSnapshot{Installed: true, Active: true}
			merge := func(state teamsstore.State) {
				snapshot.StateFiles++
				mergeTeamsServiceWatchdogState(&snapshot, state, opts)
			}
			for i := 0; i < 250; i++ {
				merge(pausedState)
			}
			owner := localOwner
			if tc.localActiveTurn {
				owner.ActiveTurnID = "turn-active"
			}
			merge(staleReloadState(owner))
			if tc.includeRemote {
				merge(staleReloadState(remoteOwner))
			}

			decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{ConsecutiveStale: 2}, opts)
			if decision.Action != tc.wantAction || !strings.Contains(decision.Reason, tc.wantReasonSubstr) {
				t.Fatalf("decision = %+v snapshot=%+v, want action=%s reason containing %q", decision, snapshot, tc.wantAction, tc.wantReasonSubstr)
			}
		})
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

func TestRunTeamsServiceWatchdogOnceDryRunLifecycleDrainClearDoesNotWriteState(t *testing.T) {
	lockCLITestHooks(t)

	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
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
			Installed:                         true,
			Active:                            true,
			StateFiles:                        1,
			ServiceDraining:                   true,
			HelperUpgradeDrainExpired:         true,
			HelperUpgradeDrainLocalOwnerFresh: true,
			OwnerFound:                        true,
			OwnerFresh:                        true,
			LastOwnerHeartbeat:                now.Add(-5 * time.Second),
		}, nil
	}
	teamsServiceWatchdogStartService = func(context.Context, bool) error {
		t.Fatal("lifecycle drain clear dry-run must not start or restart service")
		return nil
	}

	result, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now, DryRun: true})
	if err != nil {
		t.Fatalf("dry-run watchdog: %v", err)
	}
	if result.Decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction {
		t.Fatalf("dry-run action = %q, want lifecycle drain clear", result.Decision.Action)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("dry-run lifecycle drain clear should not write watchdog state, stat err=%v", err)
	}
}

func TestRunTeamsServiceWatchdogOnceClearsExpiredHelperUpgradeDrainWithoutRestart(t *testing.T) {
	lockCLITestHooks(t)

	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	tmp := t.TempDir()
	watchdogPath := filepath.Join(tmp, "watchdog.json")
	storePath := filepath.Join(tmp, "teams", "state.json")
	store, err := teamsstore.Open(storePath)
	if err != nil {
		t.Fatalf("Open teams store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Update(context.Background(), func(state *teamsstore.State) error {
		state.ServiceControl = teamsstore.ServiceControl{
			Draining:  true,
			Reason:    teamsstore.HelperUpgradeReason,
			UpdatedAt: now.Add(-time.Minute),
		}
		state.Upgrade = &teamsstore.UpgradeRequest{
			ID:         "upgrade-zombie",
			Phase:      teamsstore.UpgradePhaseDraining,
			Reason:     teamsstore.HelperUpgradeReason,
			DeadlineAt: now.Add(-time.Minute),
			StartedAt:  now.Add(-10 * time.Minute),
			UpdatedAt:  now.Add(-time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed expired helper upgrade drain: %v", err)
	}

	prevPath := teamsServiceWatchdogStatePath
	prevCollect := teamsServiceWatchdogCollectSnapshot
	prevStorePaths := teamsServiceWatchdogStorePaths
	prevStart := teamsServiceWatchdogStartService
	t.Cleanup(func() {
		teamsServiceWatchdogStatePath = prevPath
		teamsServiceWatchdogCollectSnapshot = prevCollect
		teamsServiceWatchdogStorePaths = prevStorePaths
		teamsServiceWatchdogStartService = prevStart
	})
	teamsServiceWatchdogStatePath = func() (string, error) { return watchdogPath, nil }
	teamsServiceWatchdogStorePaths = func() ([]string, error) { return []string{storePath}, nil }
	teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		return teamsServiceWatchdogSnapshot{
			Installed:                 true,
			Active:                    true,
			StateFiles:                1,
			ServiceDraining:           true,
			HelperUpgradeDrainExpired: true,
		}, nil
	}
	teamsServiceWatchdogStartService = func(context.Context, bool) error {
		t.Fatal("lifecycle drain clear must not start or restart service")
		return nil
	}

	result, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now})
	if err != nil {
		t.Fatalf("watchdog clear expired helper upgrade drain: %v", err)
	}
	if result.Decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction {
		t.Fatalf("action = %q, want lifecycle drain clear", result.Decision.Action)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load teams store: %v", err)
	}
	if state.ServiceControl.Draining {
		t.Fatalf("helper upgrade drain remained after watchdog clear: %#v", state.ServiceControl)
	}
	if state.Upgrade == nil || state.Upgrade.Phase != teamsstore.UpgradePhaseAborted {
		t.Fatalf("upgrade = %#v, want aborted", state.Upgrade)
	}
	stored, err := loadTeamsServiceWatchdogState()
	if err != nil {
		t.Fatalf("load watchdog state: %v", err)
	}
	if stored.LastAction != "" || !stored.LastActionAt.IsZero() {
		t.Fatalf("watchdog clear stored restart cooldown marker: %+v", stored)
	}
}

func TestRunTeamsServiceWatchdogOnceClearsStaleHelperReloadDrainWithoutRestart(t *testing.T) {
	lockCLITestHooks(t)

	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	tmp := t.TempDir()
	watchdogPath := filepath.Join(tmp, "watchdog.json")
	storePath := filepath.Join(tmp, "teams", "state.json")
	store, err := teamsstore.Open(storePath)
	if err != nil {
		t.Fatalf("Open teams store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Update(context.Background(), func(state *teamsstore.State) error {
		state.ServiceControl = teamsstore.ServiceControl{
			Draining:  true,
			Reason:    teamsstore.HelperReloadReason,
			UpdatedAt: now.Add(-defaultTeamsServiceWatchdogReloadStaleAfter - time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed stale helper reload drain: %v", err)
	}

	prevPath := teamsServiceWatchdogStatePath
	prevCollect := teamsServiceWatchdogCollectSnapshot
	prevStorePaths := teamsServiceWatchdogStorePaths
	prevStart := teamsServiceWatchdogStartService
	t.Cleanup(func() {
		teamsServiceWatchdogStatePath = prevPath
		teamsServiceWatchdogCollectSnapshot = prevCollect
		teamsServiceWatchdogStorePaths = prevStorePaths
		teamsServiceWatchdogStartService = prevStart
	})
	teamsServiceWatchdogStatePath = func() (string, error) { return watchdogPath, nil }
	teamsServiceWatchdogStorePaths = func() ([]string, error) { return []string{storePath}, nil }
	teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		return teamsServiceWatchdogSnapshot{
			Installed:              true,
			Active:                 true,
			StateFiles:             1,
			ServiceDraining:        true,
			HelperReloadDrainStale: true,
		}, nil
	}
	teamsServiceWatchdogStartService = func(context.Context, bool) error {
		t.Fatal("lifecycle drain clear must not start or restart service")
		return nil
	}

	result, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now})
	if err != nil {
		t.Fatalf("watchdog clear stale helper reload drain: %v", err)
	}
	if result.Decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction {
		t.Fatalf("action = %q, want lifecycle drain clear", result.Decision.Action)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load teams store: %v", err)
	}
	if state.ServiceControl.Draining {
		t.Fatalf("helper reload drain remained after watchdog clear: %#v", state.ServiceControl)
	}
	stored, err := loadTeamsServiceWatchdogState()
	if err != nil {
		t.Fatalf("load watchdog state: %v", err)
	}
	if stored.LastAction != "" || !stored.LastActionAt.IsZero() {
		t.Fatalf("watchdog clear stored restart cooldown marker: %+v", stored)
	}
}

func TestRunTeamsServiceWatchdogOnceDoesNotClearFreshRemoteStoreDuringLocalDrainClear(t *testing.T) {
	lockCLITestHooks(t)

	now := time.Date(2026, 5, 16, 1, 30, 0, 0, time.UTC)
	tmp := t.TempDir()
	watchdogPath := filepath.Join(tmp, "watchdog.json")
	localStorePath := filepath.Join(tmp, "teams", "state.json")
	remoteStorePath := filepath.Join(tmp, "teams", "scopes", "remote", "state.json")
	localStore, err := teamsstore.Open(localStorePath)
	if err != nil {
		t.Fatalf("Open local teams store: %v", err)
	}
	t.Cleanup(func() { _ = localStore.Close() })
	if err := localStore.Update(context.Background(), func(state *teamsstore.State) error {
		state.ServiceControl = teamsstore.ServiceControl{
			Draining:  true,
			Reason:    teamsstore.HelperReloadReason,
			UpdatedAt: now.Add(-defaultTeamsServiceWatchdogReloadStaleAfter - time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed local stale helper reload drain: %v", err)
	}
	remoteStore, err := teamsstore.Open(remoteStorePath)
	if err != nil {
		t.Fatalf("Open remote teams store: %v", err)
	}
	t.Cleanup(func() { _ = remoteStore.Close() })
	remoteOwner := teamsstore.OwnerMetadata{
		PID:           4242,
		Hostname:      "remote-shared-home-host",
		StartedAt:     now.Add(-time.Hour),
		LastHeartbeat: now.Add(-time.Second),
	}
	if err := remoteStore.Update(context.Background(), func(state *teamsstore.State) error {
		state.ServiceControl = teamsstore.ServiceControl{
			Draining:  true,
			Reason:    teamsstore.HelperReloadReason,
			UpdatedAt: now.Add(-defaultTeamsServiceWatchdogReloadStaleAfter - time.Minute),
		}
		state.ServiceOwner = &remoteOwner
		state.LockOwner = &remoteOwner
		return nil
	}); err != nil {
		t.Fatalf("seed remote stale helper reload drain: %v", err)
	}

	prevPath := teamsServiceWatchdogStatePath
	prevCollect := teamsServiceWatchdogCollectSnapshot
	prevStorePaths := teamsServiceWatchdogStorePaths
	prevStart := teamsServiceWatchdogStartService
	t.Cleanup(func() {
		teamsServiceWatchdogStatePath = prevPath
		teamsServiceWatchdogCollectSnapshot = prevCollect
		teamsServiceWatchdogStorePaths = prevStorePaths
		teamsServiceWatchdogStartService = prevStart
	})
	teamsServiceWatchdogStatePath = func() (string, error) { return watchdogPath, nil }
	teamsServiceWatchdogStorePaths = func() ([]string, error) { return []string{localStorePath, remoteStorePath}, nil }
	teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		return teamsServiceWatchdogSnapshot{
			Installed:                         true,
			Active:                            true,
			StateFiles:                        2,
			ServiceDraining:                   true,
			HelperReloadDrainStale:            true,
			HelperReloadDrainLocalOwnerFresh:  true,
			HelperReloadDrainRemoteOwnerFresh: true,
			OwnerFound:                        true,
			OwnerFresh:                        true,
			LastOwnerHeartbeat:                now.Add(-time.Second),
		}, nil
	}
	teamsServiceWatchdogStartService = func(context.Context, bool) error {
		t.Fatal("lifecycle drain clear must not start or restart service")
		return nil
	}

	result, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now})
	if err != nil {
		t.Fatalf("watchdog clear local drain with remote store present: %v", err)
	}
	if result.Decision.Action != wantTeamsServiceWatchdogLifecycleDrainClearAction {
		t.Fatalf("action = %q, want lifecycle drain clear", result.Decision.Action)
	}
	localState, err := localStore.Load(context.Background())
	if err != nil {
		t.Fatalf("Load local teams store: %v", err)
	}
	if localState.ServiceControl.Draining {
		t.Fatalf("local helper reload drain remained after watchdog clear: %#v", localState.ServiceControl)
	}
	remoteState, err := remoteStore.Load(context.Background())
	if err != nil {
		t.Fatalf("Load remote teams store: %v", err)
	}
	if !remoteState.ServiceControl.Draining || remoteState.ServiceControl.Reason != teamsstore.HelperReloadReason {
		t.Fatalf("fresh remote helper reload drain was cleared: %#v", remoteState.ServiceControl)
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
			Installed:           true,
			Active:              true,
			StateFiles:          1,
			OwnerFound:          true,
			OwnerFresh:          true,
			LastOwnerHeartbeat:  now.Add(-5 * time.Second),
			FreshOwnerStartedAt: now.Add(-30 * time.Minute),
			PollActivityFound:   true,
			PollActivityAt:      now.Add(-3 * time.Minute),
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

func TestRunTeamsServiceWatchdogOnceRequiresPersistentStaleAndResetsOnFreshPoll(t *testing.T) {
	lockCLITestHooks(t)

	start := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
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
	staleSamples := []bool{true, true, false, true, true, true}
	sample := 0
	teamsServiceWatchdogCollectSnapshot = func(_ context.Context, opts teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		if sample >= len(staleSamples) {
			t.Fatalf("unexpected watchdog snapshot collection %d", sample)
		}
		stale := staleSamples[sample]
		sample++
		pollAt := opts.Now.Add(-10 * time.Second)
		if stale {
			pollAt = opts.Now.Add(-3 * time.Minute)
		}
		return teamsServiceWatchdogSnapshot{
			Installed:           true,
			Active:              true,
			StateFiles:          1,
			OwnerFound:          true,
			OwnerFresh:          true,
			LastOwnerHeartbeat:  opts.Now.Add(-5 * time.Second),
			FreshOwnerStartedAt: opts.Now.Add(-30 * time.Minute),
			PollActivityFound:   true,
			PollActivityAt:      pollAt,
		}, nil
	}
	restartCalls := 0
	teamsServiceWatchdogStartService = func(_ context.Context, restart bool) error {
		restartCalls++
		if !restart {
			t.Fatalf("restart flag = false, want true")
		}
		return nil
	}

	wantActions := []string{
		teamsServiceWatchdogActionNoop,
		teamsServiceWatchdogActionNoop,
		teamsServiceWatchdogActionNoop,
		teamsServiceWatchdogActionNoop,
		teamsServiceWatchdogActionNoop,
		teamsServiceWatchdogActionRestart,
	}
	for i, wantAction := range wantActions {
		now := start.Add(time.Duration(i) * teamsServiceExternalWatchdogInterval)
		result, err := runTeamsServiceWatchdogOnce(context.Background(), teamsServiceWatchdogOptions{Now: now})
		if err != nil {
			t.Fatalf("watchdog sample %d: %v", i, err)
		}
		if result.Decision.Action != wantAction {
			t.Fatalf("sample %d action = %q, want %q decision=%+v", i, result.Decision.Action, wantAction, result.Decision)
		}
	}
	if restartCalls != 1 {
		t.Fatalf("restart calls = %d, want one restart after fresh sample reset and three stale samples", restartCalls)
	}
	stored, err := loadTeamsServiceWatchdogState()
	if err != nil {
		t.Fatalf("load stored watchdog state: %v", err)
	}
	if stored.ConsecutiveStale != 3 || stored.LastAction != teamsServiceWatchdogActionRestart {
		t.Fatalf("stored state = %+v, want restarted third stale generation", stored)
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
			Installed:           true,
			Active:              true,
			StateFiles:          1,
			OwnerFound:          true,
			OwnerFresh:          true,
			LastOwnerHeartbeat:  opts.Now.Add(-5 * time.Second),
			FreshOwnerStartedAt: opts.Now.Add(-30 * time.Minute),
			PollActivityFound:   true,
			PollActivityAt:      opts.Now.Add(-3 * time.Minute),
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
			Installed:           true,
			Active:              true,
			StateFiles:          1,
			OwnerFound:          true,
			OwnerFresh:          true,
			LastOwnerHeartbeat:  now.Add(-5 * time.Second),
			FreshOwnerStartedAt: now.Add(-30 * time.Minute),
			PollActivityFound:   true,
			PollActivityAt:      now.Add(-3 * time.Minute),
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
