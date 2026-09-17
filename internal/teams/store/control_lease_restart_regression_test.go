package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// A listener restart reclaims the same disposable machine in a new process,
// then performs several cold/runtime writes during startup and the first poll
// cycle. Those writes must not erase the owner witness or cause the same
// process to manufacture a new lease generation on its next refresh.
func TestSQLiteListenerRestartKeepsOwnerWitnessAcrossColdWrites(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	scope := ScopeIdentity{ID: "scope-listener-restart", AccountID: "account-listener-restart", Profile: "default"}
	machine := MachineRecord{
		ID: "machine-listener-restart", ScopeID: scope.ID,
		Kind: MachineKindEphemeral, Priority: DefaultMachinePriority(MachineKindEphemeral),
	}
	now := testOwnerStart()
	owner, err := CurrentOwner("listener-restart-test", "", "", now)
	if err != nil {
		t.Fatalf("CurrentOwner: %v", err)
	}
	owner.ScopeID = scope.ID
	owner.MachineID = machine.ID

	first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope: scope, Machine: machine, Owner: owner, Duration: time.Hour, Now: now,
	})
	if err != nil || first.Mode != LeaseModeActive {
		t.Fatalf("initial lease claim = %#v err=%v", first, err)
	}
	owner.LeaseGeneration = first.Lease.Generation
	if _, err := store.RecordOwnerHeartbeatForLease(ctx, owner, time.Minute, time.Hour, now); err != nil {
		t.Fatalf("initial owner heartbeat: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	for index := 1; index <= 8; index++ {
		writeAt := now.Add(time.Duration(index) * time.Second)
		if err := store.Update(ctx, func(state *State) error {
			state.ServiceControl.Reason = fmt.Sprintf("startup cold write %d", index)
			state.ServiceControl.UpdatedAt = writeAt
			return nil
		}); err != nil {
			t.Fatalf("cold write %d: %v", index, err)
		}

		claimOwner, err := CurrentOwner("listener-restart-test", "", "", writeAt)
		if err != nil {
			t.Fatalf("CurrentOwner %d: %v", index, err)
		}
		claimOwner.ScopeID = scope.ID
		claimOwner.MachineID = machine.ID
		decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
			Scope: scope, Machine: machine, Owner: claimOwner, Duration: time.Hour, Now: writeAt,
		})
		if err != nil {
			t.Fatalf("lease refresh %d: %v", index, err)
		}
		if decision.Mode != LeaseModeActive || decision.Lease.Generation != first.Lease.Generation {
			t.Fatalf("lease refresh %d = %#v, want active generation %d", index, decision, first.Lease.Generation)
		}

		owner.LeaseGeneration = decision.Lease.Generation
		if _, err := store.RecordOwnerHeartbeatForLease(ctx, owner, time.Minute, time.Hour, writeAt); err != nil {
			t.Fatalf("owner heartbeat %d: %v", index, err)
		}
	}

	persisted, found, err := store.ReadOwner(ctx)
	if err != nil || !found {
		t.Fatalf("read owner after restart writes: found=%t err=%v", found, err)
	}
	if persisted.InstanceID != owner.InstanceID || persisted.MachineID != machine.ID || persisted.LeaseGeneration != first.Lease.Generation {
		t.Fatalf("owner after restart writes = %#v, want same instance/machine/generation as %#v", persisted, owner)
	}
	lease, err := store.ValidateControlLease(ctx, machine.ID, first.Lease.Generation, now.Add(8*time.Second))
	if err != nil {
		t.Fatalf("validate lease after restart writes: %v", err)
	}
	if lease.Generation != first.Lease.Generation || lease.HolderMachineID != machine.ID {
		t.Fatalf("lease after restart writes = %#v, want generation %d on %s", lease, first.Lease.Generation, machine.ID)
	}
}

func TestClearOwnerIfSameDoesNotClearReplacementGeneration(t *testing.T) {
	ctx := context.Background()
	for _, migrate := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", migrate), func(t *testing.T) {
			store := newTestStore(t)
			scope := ScopeIdentity{ID: "scope-owner-cleanup", AccountID: "account-owner-cleanup", Profile: "default"}
			machine := MachineRecord{
				ID: "machine-owner-cleanup", ScopeID: scope.ID,
				Kind: MachineKindEphemeral, Priority: DefaultMachinePriority(MachineKindEphemeral),
			}
			now := testOwnerStart()
			oldOwner, err := CurrentOwner("owner-cleanup-test", "", "", now)
			if err != nil {
				t.Fatalf("CurrentOwner: %v", err)
			}
			oldOwner.ScopeID = scope.ID
			oldOwner.MachineID = machine.ID

			first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope: scope, Machine: machine, Owner: oldOwner, Duration: time.Hour, Now: now,
			})
			if err != nil || first.Mode != LeaseModeActive {
				t.Fatalf("initial lease claim = %#v err=%v", first, err)
			}
			oldOwner.LeaseGeneration = first.Lease.Generation
			if _, err := store.RecordOwnerHeartbeatForLease(ctx, oldOwner, time.Minute, time.Hour, now); err != nil {
				t.Fatalf("initial owner heartbeat: %v", err)
			}
			if migrate {
				migrateStoreToSQLiteForTest(t, store)
			}

			released, err := store.ReleaseControlLeaseIfHolder(ctx, machine.ID, first.Lease.Generation)
			if err != nil || !released {
				t.Fatalf("release first lease: released=%t err=%v", released, err)
			}
			claimOwner := oldOwner
			claimOwner.LeaseGeneration = 0
			nextAt := now.Add(time.Minute)
			second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope: scope, Machine: machine, Owner: claimOwner, Duration: time.Hour, Now: nextAt,
			})
			if err != nil || second.Mode != LeaseModeActive {
				t.Fatalf("replacement lease claim = %#v err=%v", second, err)
			}
			if second.Lease.Generation <= first.Lease.Generation {
				t.Fatalf("replacement generation = %d, want > %d", second.Lease.Generation, first.Lease.Generation)
			}
			replacementOwner := claimOwner
			replacementOwner.LeaseGeneration = second.Lease.Generation
			if _, err := store.RecordOwnerHeartbeatForLease(ctx, replacementOwner, time.Minute, time.Hour, nextAt); err != nil {
				t.Fatalf("replacement owner heartbeat: %v", err)
			}

			cleared, err := store.ClearOwnerIfSame(ctx, oldOwner)
			if err != nil {
				t.Fatalf("stale ClearOwnerIfSame: %v", err)
			}
			if cleared {
				t.Fatal("stale cleanup cleared replacement owner witness")
			}
			persisted, found, err := store.ReadOwner(ctx)
			if err != nil || !found {
				t.Fatalf("ReadOwner after stale cleanup: found=%t err=%v", found, err)
			}
			if persisted.InstanceID != replacementOwner.InstanceID || persisted.MachineID != machine.ID || persisted.LeaseGeneration != second.Lease.Generation {
				t.Fatalf("owner after stale cleanup = %#v, want replacement %#v", persisted, replacementOwner)
			}
		})
	}
}

// Recovery must reserve the observed owner and apply the turn/outbox
// disposition in the same durable critical section. A replacement owner in
// the meantime must therefore produce no recovery mutation at all.
func TestRecoverIfOwnerSameFencesReplacementBeforeRecovery(t *testing.T) {
	ctx := context.Background()
	for _, migrate := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", migrate), func(t *testing.T) {
			store := newTestStore(t)
			now := time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC)
			oldOwner := OwnerMetadata{
				PID: 501, Hostname: "recovery-owner", ExecutablePath: "/opt/cxp",
				InstanceID: "recovery-old", ScopeID: "recovery-scope", MachineID: "recovery-machine", LeaseGeneration: 1,
			}
			replacement := oldOwner
			replacement.PID = 502
			replacement.InstanceID = "recovery-replacement"
			replacement.LeaseGeneration = 2
			if err := store.Update(ctx, func(state *State) error {
				state.Scope = ScopeIdentity{ID: "recovery-scope", AccountID: "recovery-account"}
				state.ControlLease = ControlLease{
					ScopeID: "recovery-scope", HolderMachineID: replacement.MachineID,
					Generation: replacement.LeaseGeneration, Status: ControlLeaseStatusActive,
					LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
				}
				state.ServiceOwner = &replacement
				state.LockOwner = &replacement
				state.Turns["turn:recovery-fence"] = Turn{ID: "turn:recovery-fence", SessionID: "session:recovery-fence", Status: TurnStatusQueued, CreatedAt: now}
				state.OutboxMessages["outbox:recovery-fence"] = OutboxMessage{ID: "outbox:recovery-fence", SessionID: "session:recovery-fence", TeamsChatID: "chat:recovery-fence", Kind: "queued-status", Status: OutboxStatusQueued, CreatedAt: now}
				return nil
			}); err != nil {
				t.Fatalf("seed recovery fence state: %v", err)
			}
			if migrate {
				migrateStoreToSQLiteForTest(t, store)
			}

			report, applied, err := store.RecoverIfOwnerSame(ctx, oldOwner, true)
			if err != nil {
				t.Fatalf("stale recovery error: %v", err)
			}
			if applied || len(report.InterruptedTurnIDs) != 0 || len(report.SupersededOutboxIDs) != 0 {
				t.Fatalf("stale recovery report=%#v applied=%t, want no mutation", report, applied)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load after stale recovery: %v", err)
			}
			if state.Turns["turn:recovery-fence"].Status != TurnStatusQueued || state.OutboxMessages["outbox:recovery-fence"].Status != OutboxStatusQueued {
				t.Fatalf("stale recovery changed queued work: turn=%#v outbox=%#v", state.Turns["turn:recovery-fence"], state.OutboxMessages["outbox:recovery-fence"])
			}
			owner, ok := state.readOwner()
			if !ok || owner.InstanceID != replacement.InstanceID || owner.LeaseGeneration != replacement.LeaseGeneration {
				t.Fatalf("replacement owner after stale recovery = %#v found=%t", owner, ok)
			}

			report, applied, err = store.RecoverIfOwnerSame(ctx, replacement, true)
			if err != nil || !applied {
				t.Fatalf("current recovery applied=%t err=%v report=%#v", applied, err, report)
			}
			if len(report.InterruptedTurnIDs) != 1 || len(report.SupersededOutboxIDs) != 1 {
				t.Fatalf("current recovery report=%#v, want one turn and one outbox", report)
			}
			state, err = store.Load(ctx)
			if err != nil {
				t.Fatalf("load after current recovery: %v", err)
			}
			if _, ok := state.readOwner(); ok {
				t.Fatal("current recovery did not clear the reserved owner")
			}
			if state.Turns["turn:recovery-fence"].Status != TurnStatusInterrupted || state.OutboxMessages["outbox:recovery-fence"].Status != OutboxStatusSkipped {
				t.Fatalf("current recovery did not dispose work: turn=%#v outbox=%#v", state.Turns["turn:recovery-fence"], state.OutboxMessages["outbox:recovery-fence"])
			}
		})
	}
}

// A process restart can leave a turn queued with the previous listener's
// generation. The new owner must adopt that row at the queued->running CAS;
// otherwise the listener treats a normal restart as a lost control lease and
// repeatedly creates new generations. Adoption is safe only for a queued row
// and only after the current control lease has been validated. A callback that
// still carries the old generation must remain fenced after the adoption.
func TestCurrentOwnerAdoptsQueuedTurnFromPreviousLeaseGeneration(t *testing.T) {
	ctx := context.Background()
	for _, migrate := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", migrate), func(t *testing.T) {
			store := newTestStore(t)
			defer store.Close()
			const (
				scopeID   = "scope-queued-restart-adoption"
				sessionID = "session-queued-restart-adoption"
				turnID    = "turn-queued-restart-adoption"
			)
			scope := ScopeIdentity{ID: scopeID, AccountID: "account-queued-restart-adoption", Profile: "default"}
			machine := MachineRecord{ID: "machine-queued-restart-adoption", ScopeID: scopeID, Kind: MachineKindEphemeral}
			if err := store.Update(ctx, func(state *State) error {
				state.Scope = scope
				state.Sessions[sessionID] = SessionContext{
					ID: sessionID, Status: SessionStatusActive, TeamsChatID: "chat-queued-restart-adoption",
				}
				return nil
			}); err != nil {
				t.Fatalf("seed queued restart session: %v", err)
			}
			if migrate {
				migrateStoreToSQLiteForTest(t, store)
			}

			// Capability validation uses the real clock. Keep the fixture's
			// timestamps deterministic relative to the current clock so the
			// one-hour lease is still active when QueueTurn validates it.
			now := time.Now().UTC()
			oldOwner, err := CurrentOwner("queued-restart-old", "", "", now)
			if err != nil {
				t.Fatalf("CurrentOwner old: %v", err)
			}
			oldOwner.ScopeID = scopeID
			oldOwner.MachineID = machine.ID
			first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope: scope, Machine: machine, Owner: oldOwner, Duration: time.Hour, Now: now,
			})
			if err != nil || first.Mode != LeaseModeActive {
				t.Fatalf("claim old owner: mode=%v err=%v", first.Mode, err)
			}
			oldOwner.LeaseGeneration = first.Lease.Generation
			if _, err := store.RecordOwnerHeartbeatForLease(ctx, oldOwner, time.Minute, time.Hour, now); err != nil {
				t.Fatalf("old owner heartbeat: %v", err)
			}

			queued, created, err := store.QueueTurn(ctx, Turn{
				ID: turnID, SessionID: sessionID, Status: TurnStatusQueued,
				MachineID: machine.ID, LeaseGeneration: first.Lease.Generation,
				QueuedAt: now, CreatedAt: now, UpdatedAt: now,
			})
			if err != nil || !created {
				t.Fatalf("queue old-generation turn: created=%t err=%v turn=%#v", created, err, queued)
			}
			if queued.LeaseGeneration != first.Lease.Generation || queued.MachineID != machine.ID {
				t.Fatalf("queued old-generation turn = %#v, want machine=%q generation=%d", queued, machine.ID, first.Lease.Generation)
			}

			// Model the previous process completing its shutdown boundary. The
			// queued row remains durable, but its owner and lease are retired.
			if cleared, err := store.ClearOwnerIfSame(ctx, oldOwner); err != nil || !cleared {
				t.Fatalf("clear old owner: cleared=%t err=%v", cleared, err)
			}
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machine.ID, first.Lease.Generation); err != nil || !released {
				t.Fatalf("release old lease: released=%t err=%v", released, err)
			}

			newOwner := oldOwner
			newOwner.InstanceID = oldOwner.InstanceID + "-replacement"
			newOwner.HelperVersion = "queued-restart-new"
			newOwner.StartedAt = now.Add(time.Second)
			newOwner.LastHeartbeat = newOwner.StartedAt
			newLease, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope: scope, Machine: machine, Owner: newOwner, Duration: time.Hour, Now: now.Add(time.Second),
			})
			if err != nil || newLease.Mode != LeaseModeActive {
				t.Fatalf("claim replacement owner: mode=%v err=%v", newLease.Mode, err)
			}
			if newLease.Lease.Generation <= first.Lease.Generation {
				t.Fatalf("replacement generation=%d, want > %d", newLease.Lease.Generation, first.Lease.Generation)
			}

			claimed, ok, err := store.ClaimNextQueuedTurnForOwner(ctx, sessionID, machine.ID, newLease.Lease.Generation)
			if err != nil || !ok {
				t.Fatalf("claim previous-generation queued turn: claimed=%t err=%v turn=%#v", ok, err, claimed)
			}
			if claimed.ID != turnID || claimed.Status != TurnStatusRunning || claimed.MachineID != machine.ID || claimed.LeaseGeneration != newLease.Lease.Generation {
				t.Fatalf("adopted queued turn = %#v, want running replacement capability machine=%q generation=%d", claimed, machine.ID, newLease.Lease.Generation)
			}

			if _, err := store.RequeueTurnForOwner(ctx, turnID, machine.ID, first.Lease.Generation); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("old-generation requeue error=%v, want ErrControlLeaseNotHeld", err)
			}
			current, found, err := store.TurnByID(ctx, turnID)
			if err != nil || !found {
				t.Fatalf("read adopted queued turn: found=%t err=%v", found, err)
			}
			if current.Status != TurnStatusRunning || current.LeaseGeneration != newLease.Lease.Generation {
				t.Fatalf("old generation changed adopted turn: %#v", current)
			}
		})
	}
}

func TestQueuedTurnOwnerGenerationBoundaries(t *testing.T) {
	capability, err := newStoreOwnerCapability("machine-current", 7)
	if err != nil {
		t.Fatalf("newStoreOwnerCapability: %v", err)
	}
	tests := []struct {
		name    string
		turn    Turn
		wantErr error
	}{
		{name: "legacy unbound", turn: Turn{Status: TurnStatusQueued}},
		{name: "legacy machine marker", turn: Turn{Status: TurnStatusQueued, MachineID: "machine-old"}},
		{name: "same generation", turn: Turn{Status: TurnStatusQueued, MachineID: "machine-current", LeaseGeneration: 7}},
		{name: "older generation with owner", turn: Turn{Status: TurnStatusQueued, MachineID: "machine-old", LeaseGeneration: 6}},
		{name: "older generation without owner", turn: Turn{Status: TurnStatusQueued, LeaseGeneration: 6}, wantErr: ErrControlLeaseNotHeld},
		{name: "different current owner", turn: Turn{Status: TurnStatusQueued, MachineID: "machine-other", LeaseGeneration: 7}, wantErr: ErrControlLeaseNotHeld},
		{name: "future generation", turn: Turn{Status: TurnStatusQueued, MachineID: "machine-current", LeaseGeneration: 8}, wantErr: ErrControlLeaseNotHeld},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateQueuedTurnOwnerForClaim(test.turn, capability)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("validation error=%v, want %v", err, test.wantErr)
			}
		})
	}
}
