package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// A queued row can be safely adopted by the current control owner for an
// explicit recovery disposition. This is deliberately narrower than running
// takeover: once another owner claims the row, the same API must become a
// durable no-op rather than interrupting an unresolved execution.
func TestMarkQueuedTurnInterruptedForOwnerPreservesExecutionFence(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			store := newTestStore(t)
			t.Cleanup(func() { _ = store.Close() })
			now := time.Now().UTC().Truncate(time.Microsecond)
			scope := ScopeIdentity{ID: "scope-queued-disposition", AccountID: "account-queued-disposition", Profile: "default"}
			machineA := MachineRecord{ID: "machine-queued-disposition-a", ScopeID: scope.ID, Kind: MachineKindPrimary}
			machineB := MachineRecord{ID: "machine-queued-disposition-b", ScopeID: scope.ID, Kind: MachineKindPrimary}
			if err := store.Update(ctx, func(state *State) error {
				state.Scope = scope
				state.Sessions["session-queued-disposition"] = SessionContext{
					ID: "session-queued-disposition", Status: SessionStatusActive, TeamsChatID: "chat-queued-disposition",
				}
				state.Turns["turn-queued-disposition"] = Turn{
					ID: "turn-queued-disposition", SessionID: "session-queued-disposition", Status: TurnStatusQueued,
					MachineID: machineA.ID, LeaseGeneration: 1, CreatedAt: now, UpdatedAt: now,
				}
				state.Turns["turn-queued-race"] = Turn{
					ID: "turn-queued-race", SessionID: "session-queued-disposition", Status: TurnStatusQueued,
					MachineID: machineA.ID, LeaseGeneration: 1, CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed queued disposition fixture: %v", err)
			}
			ownerA, err := CurrentOwner("queued-disposition-a", "", "", now)
			if err != nil {
				t.Fatalf("CurrentOwner A: %v", err)
			}
			ownerA.ScopeID, ownerA.MachineID = scope.ID, machineA.ID
			first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineA, Owner: ownerA, Duration: time.Hour, Now: now})
			if err != nil || first.Mode != LeaseModeActive {
				t.Fatalf("claim owner A: decision=%#v err=%v", first, err)
			}
			if err := store.Update(ctx, func(state *State) error {
				turn := state.Turns["turn-queued-disposition"]
				turn.LeaseGeneration = first.Lease.Generation
				state.Turns[turn.ID] = turn
				turn = state.Turns["turn-queued-race"]
				turn.LeaseGeneration = first.Lease.Generation
				state.Turns[turn.ID] = turn
				return nil
			}); err != nil {
				t.Fatalf("bind queued fixture to owner A: %v", err)
			}
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, first.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A: released=%v err=%v", released, err)
			}
			ownerB, err := CurrentOwner("queued-disposition-b", "", "", now.Add(time.Second))
			if err != nil {
				t.Fatalf("CurrentOwner B: %v", err)
			}
			ownerB.ScopeID, ownerB.MachineID = scope.ID, machineB.ID
			second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineB, Owner: ownerB, Duration: time.Hour, Now: now.Add(time.Second)})
			if err != nil || second.Mode != LeaseModeActive {
				t.Fatalf("claim owner B: decision=%#v err=%v", second, err)
			}

			retired, err := store.MarkQueuedTurnInterruptedForOwner(ctx, "turn-queued-disposition", "malformed recovery input", machineB.ID, second.Lease.Generation)
			if err != nil {
				t.Fatalf("current owner queued disposition: %v", err)
			}
			if retired.Status != TurnStatusInterrupted || retired.MachineID != machineB.ID || retired.LeaseGeneration != second.Lease.Generation {
				t.Fatalf("retired queued turn = %#v, want current owner interrupted row", retired)
			}
			if _, err := store.MarkTurnInterruptedForOwner(ctx, "turn-queued-disposition", "stale callback", machineA.ID, first.Lease.Generation); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale callback error=%v, want owner fence", err)
			}

			claimed, ok, err := store.ClaimNextQueuedTurnForOwner(ctx, "session-queued-disposition", machineB.ID, second.Lease.Generation)
			if err != nil || !ok || claimed.ID != "turn-queued-race" || claimed.Status != TurnStatusRunning {
				t.Fatalf("claim race turn = %#v claimed=%v err=%v, want running", claimed, ok, err)
			}
			unchanged, err := store.MarkQueuedTurnInterruptedForOwner(ctx, "turn-queued-race", "late recovery disposition", machineB.ID, second.Lease.Generation)
			if err != nil {
				t.Fatalf("running row recovery disposition: %v", err)
			}
			if unchanged.Status != TurnStatusRunning || unchanged.LeaseGeneration != second.Lease.Generation {
				t.Fatalf("running row after queued disposition = %#v, want unchanged running owner", unchanged)
			}
		})
	}
}
