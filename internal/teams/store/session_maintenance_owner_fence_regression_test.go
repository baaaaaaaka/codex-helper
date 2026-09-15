package store

import (
	"context"
	"testing"
	"time"
)

// TestSessionMaintenanceOwnerFenceRejectsReplacementOwner protects the CLI
// maintenance boundary.  Reading and clearing a stale owner in two separate
// calls is not a reservation: a new listener may claim the store in between.
// The owner-fenced lifecycle APIs must reject the old snapshot and allow the
// current owner (or a still-ownerless store) to proceed atomically.
func TestSessionMaintenanceOwnerFenceRejectsReplacementOwner(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.Update(ctx, func(state *State) error {
				state.Scope = ScopeIdentity{ID: "scope-maintenance-fence-" + name}
				state.Sessions["session-maintenance-fence"] = SessionContext{
					ID: "session-maintenance-fence", Status: SessionStatusActive, TeamsChatID: "chat-maintenance-fence",
				}
				return nil
			}); err != nil {
				t.Fatalf("seed session: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, st)
			}

			scope := ScopeIdentity{ID: "scope-maintenance-fence-" + name}
			machineA := MachineRecord{ID: "machine-maintenance-a-" + name, ScopeID: scope.ID, Kind: MachineKindPrimary}
			ownerA, err := CurrentOwner("maintenance-a", "", "", now)
			if err != nil {
				t.Fatalf("create owner A: %v", err)
			}
			ownerA.ScopeID, ownerA.MachineID = scope.ID, machineA.ID
			first, err := st.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineA, Owner: ownerA, Duration: time.Hour, Now: now})
			if err != nil || first.Mode != LeaseModeActive {
				t.Fatalf("claim owner A: decision=%#v err=%v", first, err)
			}
			ownerA.LeaseGeneration = first.Lease.Generation
			if _, err := st.RecordOwnerHeartbeatForLease(ctx, ownerA, time.Minute, time.Hour, now); err != nil {
				t.Fatalf("record owner A heartbeat: %v", err)
			}
			expectedA, found, err := st.ReadOwner(ctx)
			if err != nil || !found {
				t.Fatalf("read owner A: found=%v err=%v owner=%#v", found, err, expectedA)
			}
			if released, err := st.ReleaseControlLeaseIfHolder(ctx, machineA.ID, first.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A: released=%v err=%v", released, err)
			}

			machineB := MachineRecord{ID: "machine-maintenance-b-" + name, ScopeID: scope.ID, Kind: MachineKindPrimary}
			ownerB, err := CurrentOwner("maintenance-b", "", "", now.Add(time.Second))
			if err != nil {
				t.Fatalf("create owner B: %v", err)
			}
			ownerB.ScopeID, ownerB.MachineID = scope.ID, machineB.ID
			second, err := st.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineB, Owner: ownerB, Duration: time.Hour, Now: now.Add(time.Second)})
			if err != nil || second.Mode != LeaseModeActive {
				t.Fatalf("claim owner B: decision=%#v err=%v", second, err)
			}
			ownerB.LeaseGeneration = second.Lease.Generation
			if _, err := st.RecordOwnerHeartbeatForLease(ctx, ownerB, time.Minute, time.Hour, now.Add(time.Second)); err != nil {
				t.Fatalf("record owner B heartbeat: %v", err)
			}

			if report, applied, err := st.QuarantineSessionIfOwnerSame(ctx, expectedA, true, SessionQuarantineRequest{
				SessionID: "session-maintenance-fence", Reason: "stale owner test", Source: "test", Now: now.Add(2 * time.Second),
			}); err != nil {
				t.Fatalf("old-owner quarantine: %v", err)
			} else if applied || report.Changed {
				t.Fatalf("old-owner quarantine applied across takeover: applied=%v report=%#v", applied, report)
			}
			state, err := st.Load(ctx)
			if err != nil {
				t.Fatalf("load after old-owner rejection: %v", err)
			}
			if state.Sessions["session-maintenance-fence"].Status != SessionStatusActive {
				t.Fatalf("old-owner rejection changed session: %#v", state.Sessions["session-maintenance-fence"])
			}

			expectedB, found, err := st.ReadOwner(ctx)
			if err != nil || !found {
				t.Fatalf("read owner B: found=%v err=%v owner=%#v", found, err, expectedB)
			}
			if report, applied, err := st.QuarantineSessionIfOwnerSame(ctx, expectedB, true, SessionQuarantineRequest{
				SessionID: "session-maintenance-fence", Reason: "current owner test", Source: "test", Now: now.Add(3 * time.Second),
			}); err != nil || !applied || !report.Changed {
				t.Fatalf("current-owner quarantine: applied=%v report=%#v err=%v", applied, report, err)
			}
			if owner, found, err := st.ReadOwner(ctx); err != nil || found {
				t.Fatalf("current-owner quarantine did not clear stale maintenance owner: found=%v err=%v owner=%#v", found, err, owner)
			}

			if report, applied, err := st.UnquarantineSessionIfOwnerSame(ctx, OwnerMetadata{}, false, SessionUnquarantineRequest{
				SessionID: "session-maintenance-fence", Now: now.Add(4 * time.Second),
			}); err != nil || !applied || !report.Changed {
				t.Fatalf("ownerless unquarantine: applied=%v report=%#v err=%v", applied, report, err)
			}
		})
	}
}
