package teams

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// Listen claims the control lease before recording the first service owner
// heartbeat.  A startup error at that boundary must not leave the lease held
// by a process that never became an owner, or every later service will remain
// in standby until the lease expires.
func TestBridgeListenDoesNotLeakControlLeaseWhenInitialOwnerHeartbeatFails(t *testing.T) {
	isolateTeamsUserDirsForTest(t, t.TempDir())
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, EchoExecutor{})
	now := time.Now()
	previous := teamstore.OwnerMetadata{
		PID:             os.Getpid() + 100000,
		Hostname:        "previous-teams-owner",
		MachineID:       bridge.machine.ID + "-previous",
		ScopeID:         bridge.scope.ID,
		LeaseGeneration: 7,
		StartedAt:       now.Add(-time.Minute),
		LastHeartbeat:   now,
	}
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.Scope = bridge.scope
		state.ServiceOwner = &previous
		state.LockOwner = &previous
		state.ControlLease = teamstore.ControlLease{}
		return nil
	}); err != nil {
		t.Fatalf("seed previous owner: %v", err)
	}

	err := bridge.Listen(context.Background(), BridgeOptions{
		Store:           store,
		Once:            true,
		Interval:        time.Millisecond,
		OwnerStaleAfter: time.Hour,
		Executor:        EchoExecutor{},
		HelperVersion:   "initial-heartbeat-failure",
	})
	var ownerConflict *teamstore.OwnerConflictError
	if !errors.As(err, &ownerConflict) {
		t.Fatalf("Listen error = %v, want initial owner heartbeat conflict", err)
	}
	state, loadErr := store.Load(context.Background())
	if loadErr != nil {
		t.Fatalf("load state after failed Listen: %v", loadErr)
	}
	if state.ControlLease.HolderMachineID != "" || state.ControlLease.Status == teamstore.ControlLeaseStatusActive || state.ControlLease.LeaseUntil.After(time.Now()) {
		t.Fatalf("initial owner heartbeat failure left an active control lease behind: lease=%#v", state.ControlLease)
	}
	if state.ServiceOwner == nil || state.ServiceOwner.MachineID != previous.MachineID || state.ServiceOwner.LeaseGeneration != previous.LeaseGeneration {
		t.Fatalf("failed listener overwrote the previous service owner: got=%#v want=%#v", state.ServiceOwner, previous)
	}
	if state.LockOwner == nil || state.LockOwner.MachineID != previous.MachineID || state.LockOwner.LeaseGeneration != previous.LeaseGeneration {
		t.Fatalf("failed listener overwrote the previous lock owner: got=%#v want=%#v", state.LockOwner, previous)
	}

	// The failed listener must not leave the next legitimate owner waiting for
	// the leaked lease to expire. Exercise the same store-level acquisition
	// path used by a replacement bridge rather than only inspecting the row.
	replacementMachine := bridge.machine
	replacementMachine.ID += "-replacement"
	replacementOwner, ownerErr := teamstore.CurrentOwner("replacement-owner", "", "", now.Add(time.Second))
	if ownerErr != nil {
		t.Fatalf("CurrentOwner for replacement: %v", ownerErr)
	}
	replacementOwner.ScopeID = bridge.scope.ID
	replacementOwner.MachineID = replacementMachine.ID
	replacementOwner.LeaseGeneration = 0
	decision, claimErr := store.ClaimControlLease(context.Background(), teamstore.ControlLeaseClaim{
		Scope:    bridge.scope,
		Machine:  replacementMachine,
		Owner:    replacementOwner,
		Duration: time.Minute,
		Now:      now.Add(time.Second),
	})
	if claimErr != nil {
		t.Fatalf("replacement control lease claim: %v", claimErr)
	}
	if decision.Mode != teamstore.LeaseModeActive || decision.Lease.HolderMachineID != replacementMachine.ID {
		t.Fatalf("replacement control lease decision = %#v, want immediate active takeover", decision)
	}
	if released, releaseErr := store.ReleaseControlLeaseIfHolder(context.Background(), replacementMachine.ID, decision.Lease.Generation); releaseErr != nil || !released {
		t.Fatalf("release replacement control lease: released=%v err=%v", released, releaseErr)
	}
}
