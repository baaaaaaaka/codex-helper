package teams

import (
	"context"
	"os"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestAsyncTurnLifecycleSignalDoesNotMisclassifyRequestCancellation(t *testing.T) {
	bridge := &Bridge{}
	generation := bridge.beginAsyncTurnLifecycle(time.Second)
	bridge.asyncTurnStateMu.Lock()
	stopSignal := bridge.asyncTurnLifecycleStop
	bridge.asyncTurnStateMu.Unlock()
	if stopSignal == nil {
		t.Fatal("listener lifecycle did not create a stop signal")
	}

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	workerCtx := withTeamsAsyncTurnLifecycleContext(requestCtx, stopSignal)
	cancelRequest()
	if bridge.asyncTurnLifecycleStoppedForContext(workerCtx) {
		t.Fatal("request-scoped cancellation was classified as listener shutdown")
	}

	bridge.markAsyncTurnLifecycleStopping(generation)
	if !bridge.asyncTurnLifecycleStoppedForContext(workerCtx) {
		t.Fatal("listener generation stop was not visible to an admitted worker")
	}
	bridge.stopAsyncTurnLifecycle(generation)
}

// A pre-generation owner row without lease history is still a live ownership
// witness.  Listen must remain in standby rather than claiming first and
// relying on a later heartbeat conflict; neither path may leave a new lease
// behind or overwrite the legacy owner.
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
	if err != nil {
		t.Fatalf("Listen with fresh legacy owner = %v, want clean Once standby return", err)
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

	// A second process must also remain standby while the legacy owner is fresh.
	// Once that owner is stale, the same store-level acquisition path must allow
	// a legitimate replacement without waiting on a leaked lease.
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
	if decision.Mode != teamstore.LeaseModeStandby {
		t.Fatalf("fresh-owner replacement decision = %#v, want standby", decision)
	}
	decision, claimErr = store.ClaimControlLease(context.Background(), teamstore.ControlLeaseClaim{
		Scope:    bridge.scope,
		Machine:  replacementMachine,
		Owner:    replacementOwner,
		Duration: time.Minute,
		Now:      now.Add(2 * time.Minute),
	})
	if claimErr != nil {
		t.Fatalf("stale legacy owner replacement claim: %v", claimErr)
	}
	if decision.Mode != teamstore.LeaseModeActive || decision.Lease.HolderMachineID != replacementMachine.ID {
		t.Fatalf("stale-owner replacement control lease decision = %#v, want active takeover", decision)
	}
	if released, releaseErr := store.ReleaseControlLeaseIfHolder(context.Background(), replacementMachine.ID, decision.Lease.Generation); releaseErr != nil || !released {
		t.Fatalf("release replacement control lease: released=%v err=%v", released, releaseErr)
	}
}
