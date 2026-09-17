package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSetChatRateLimitForOwnerFencesTakeoverAcrossBackends(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			// ClaimControlLease uses the supplied logical time, while the owner
			// capability fence validates expiry against the wall clock.  Keep the
			// seed lease live for the immediate capability write; using a fixed
			// historical timestamp makes this regression test fail once the clock
			// moves past that timestamp.
			now := time.Now().UTC()
			scope := ScopeIdentity{ID: "rate-limit-owner-scope", AccountID: "account-1", Profile: "default"}
			machineA := MachineRecord{ID: "rate-limit-machine-a", ScopeID: scope.ID, Kind: MachineKindPrimary}
			ownerA := testOwner("rate-limit-session-a", "rate-limit-turn-a", now)
			ownerA.ScopeID = scope.ID
			ownerA.MachineID = machineA.ID
			first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope: scope, Machine: machineA, Owner: ownerA, Duration: time.Minute, Now: now,
			})
			if err != nil {
				t.Fatalf("claim machine A: %v", err)
			}
			if first.Mode != LeaseModeActive || first.Lease.Generation <= 0 {
				t.Fatalf("machine A lease = %#v, want active positive generation", first)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}

			firstDeadline := now.Add(10 * time.Minute)
			if _, err := store.SetChatRateLimitForOwner(ctx, "rate-limit-chat", firstDeadline, "account 429", machineA.ID, first.Lease.Generation); err != nil {
				t.Fatalf("owner-bound rate limit before takeover: %v", err)
			}

			takeoverAt := now.Add(2 * time.Minute)
			machineB := MachineRecord{ID: "rate-limit-machine-b", ScopeID: scope.ID, Kind: MachineKindPrimary}
			ownerB := testOwner("rate-limit-session-b", "rate-limit-turn-b", takeoverAt)
			ownerB.ScopeID = scope.ID
			ownerB.MachineID = machineB.ID
			second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope: scope, Machine: machineB, Owner: ownerB, Duration: time.Minute, Now: takeoverAt,
			})
			if err != nil {
				t.Fatalf("claim machine B takeover: %v", err)
			}
			if second.Mode != LeaseModeActive || second.Lease.HolderMachineID != machineB.ID || second.Lease.Generation <= first.Lease.Generation {
				t.Fatalf("machine B lease = %#v, want newer active lease", second)
			}

			staleDeadline := now.Add(20 * time.Minute)
			if _, err := store.SetChatRateLimitForOwner(ctx, "rate-limit-chat", staleDeadline, "stale owner must not extend gate", machineA.ID, first.Lease.Generation); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale owner rate-limit write error = %v, want ErrControlLeaseNotHeld", err)
			}
			limit, ok, err := store.ChatRateLimit(ctx, "rate-limit-chat")
			if err != nil || !ok {
				t.Fatalf("read rate-limit after stale write: ok=%v err=%v limit=%#v", ok, err, limit)
			}
			if !limit.BlockedUntil.Equal(firstDeadline) {
				t.Fatalf("stale owner changed durable deadline to %s, want %s", limit.BlockedUntil, firstDeadline)
			}

			secondDeadline := now.Add(30 * time.Minute)
			if _, err := store.SetChatRateLimitForOwner(ctx, "rate-limit-chat", secondDeadline, "new owner account 429", machineB.ID, second.Lease.Generation); err != nil {
				t.Fatalf("current owner rate-limit write: %v", err)
			}
			limit, ok, err = store.ChatRateLimit(ctx, "rate-limit-chat")
			if err != nil || !ok || !limit.BlockedUntil.Equal(secondDeadline) {
				t.Fatalf("current owner durable deadline = %#v ok=%v err=%v, want %s", limit, ok, err, secondDeadline)
			}
		})
	}
}

func TestClearChatRateLimitIfExpiredDoesNotDeleteNewerGate(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			oldDeadline := time.Now().Add(-time.Minute).UTC()
			newDeadline := time.Now().Add(10 * time.Minute).UTC()
			if _, err := store.SetChatRateLimit(ctx, "clear-race-chat", oldDeadline, "old"); err != nil {
				t.Fatalf("set expired gate: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			if _, err := store.SetChatRateLimit(ctx, "clear-race-chat", newDeadline, "new owner"); err != nil {
				t.Fatalf("set newer gate: %v", err)
			}
			if err := store.ClearChatRateLimitIfExpired(ctx, "clear-race-chat", oldDeadline); err != nil {
				t.Fatalf("conditional clear: %v", err)
			}
			got, ok, err := store.ChatRateLimit(ctx, "clear-race-chat")
			if err != nil || !ok || !got.BlockedUntil.Equal(newDeadline) {
				t.Fatalf("conditional clear changed newer gate: got=%#v ok=%v err=%v want=%s", got, ok, err, newDeadline)
			}
		})
	}
}
