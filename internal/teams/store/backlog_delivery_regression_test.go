package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"
)

func TestStoreJSONSQLiteDispositionParity(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			queuedAt := now.Add(-2 * time.Minute)
			for _, msg := range []OutboxMessage{
				{ID: "outbox:parity:due", TeamsChatID: "parity-chat", Kind: "helper", Body: "due", CreatedAt: queuedAt},
				{ID: "outbox:parity:future", TeamsChatID: "parity-chat", Kind: "helper", Body: "future", CreatedAt: queuedAt.Add(time.Second)},
			} {
				if _, created, err := store.QueueOutbox(ctx, msg); err != nil || !created {
					t.Fatalf("QueueOutbox %s created=%v err=%v", msg.ID, created, err)
				}
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			future := now.Add(time.Hour)
			if _, err := store.DeferOutboxDeliveryUntil(ctx, "outbox:parity:future", future); err != nil {
				t.Fatalf("DeferOutboxDeliveryUntil: %v", err)
			}

			page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 10})
			if err != nil {
				t.Fatalf("PendingOutboxPageAt due: %v", err)
			}
			if got := backlogOutboxIDs(page.Messages); len(got) != 1 || got[0] != "outbox:parity:due" {
				t.Fatalf("due pending ids = %#v, want only due row", got)
			}
			all, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 10, IgnoreRetryGate: true})
			if err != nil {
				t.Fatalf("PendingOutboxPageAt ignore gate: %v", err)
			}
			if got := backlogOutboxIDs(all.Messages); len(got) != 2 {
				t.Fatalf("ignore-gate pending ids = %#v, want both rows", got)
			}

			if _, err := store.SetChatRateLimit(ctx, "parity-chat", now.Add(2*time.Hour), "test rate limit"); err != nil {
				t.Fatalf("SetChatRateLimit: %v", err)
			}
			limited, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 10})
			if err != nil {
				t.Fatalf("PendingOutboxPageAt rate limit: %v", err)
			}
			if len(limited.Messages) != 0 {
				t.Fatalf("rate-limited pending rows = %#v, want none", backlogOutboxIDs(limited.Messages))
			}
			if err := store.ClearChatRateLimit(ctx, "parity-chat"); err != nil {
				t.Fatalf("ClearChatRateLimit: %v", err)
			}
			woken, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 10})
			if err != nil {
				t.Fatalf("PendingOutboxPageAt after clear: %v", err)
			}
			if got := backlogOutboxIDs(woken.Messages); len(got) != 2 {
				t.Fatalf("woken pending ids = %#v, want both rows", got)
			}

			futureRow, err := store.OutboxMessageByID(ctx, "outbox:parity:future")
			if err != nil {
				t.Fatalf("OutboxMessageByID future row: %v", err)
			}
			if !futureRow.NextAttemptAt.IsZero() {
				t.Fatalf("future row retry gate = %s, want cleared by chat wake-up", futureRow.NextAttemptAt)
			}
		})
	}
}

func TestRetryableOutboxErrorRetainsTerminalInFlightAttemptAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			row, created, err := store.QueueOutbox(ctx, OutboxMessage{
				ID: "outbox:retryable-terminal-fence", TeamsChatID: "chat:retryable-terminal-fence",
				Kind: "final", Body: "retryable response",
			})
			if err != nil || !created {
				t.Fatalf("queue row created=%v err=%v", created, err)
			}
			row, err = store.MarkOutboxSendAttempt(ctx, row.ID)
			if err != nil || row.SendAttemptToken == "" {
				t.Fatalf("claim row=%#v err=%v", row, err)
			}
			if err := store.Update(ctx, func(state *State) error {
				message := state.OutboxMessages[row.ID]
				message.BlockedByTerminalFailure = true
				state.OutboxMessages[row.ID] = message
				return nil
			}); err != nil {
				t.Fatalf("install terminal fence: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}

			updated, err := store.MarkOutboxRetryableSendErrorForAttempt(ctx, row.ID, row.SendAttemptToken, "HTTP 429")
			if err != nil || updated.Status != OutboxStatusSending || !OutboxSendIsAmbiguous(updated) {
				t.Fatalf("retryable terminal row=%#v err=%v, want ambiguous Sending", updated, err)
			}
		})
	}
}

func TestOwnerHeartbeatForLeaseRejectsStaleGenerationAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			store := newTestStore(t)
			scope := ScopeIdentity{ID: "scope-owner-heartbeat", AccountID: "user-1", OSUser: "tester", Profile: "default"}
			machineA := MachineRecord{ID: "machine-owner-a", ScopeID: scope.ID, Kind: MachineKindPrimary, Priority: DefaultMachinePriority(MachineKindPrimary)}
			machineB := MachineRecord{ID: "machine-owner-b", ScopeID: scope.ID, Kind: MachineKindPrimary, Priority: DefaultMachinePriority(MachineKindPrimary)}
			now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
			ownerA := OwnerMetadata{PID: 41001, Hostname: "owner-a", ExecutablePath: "/opt/cxp-a", InstanceID: "instance-owner-a", ScopeID: scope.ID, MachineID: machineA.ID, LeaseGeneration: 0}
			ownerB := OwnerMetadata{PID: 41002, Hostname: "owner-b", ExecutablePath: "/opt/cxp-b", InstanceID: "instance-owner-b", ScopeID: scope.ID, MachineID: machineB.ID, LeaseGeneration: 0}

			first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineA, Owner: ownerA, Duration: time.Hour, Now: now})
			if err != nil || first.Mode != LeaseModeActive {
				t.Fatalf("claim first owner = %#v err=%v", first, err)
			}
			ownerA.LeaseGeneration = first.Lease.Generation
			if _, err := store.RecordOwnerHeartbeatForLease(ctx, ownerA, time.Minute, time.Hour, now); err != nil {
				t.Fatalf("record first owner heartbeat: %v", err)
			}
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}

			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, first.Lease.Generation); err != nil || !released {
				t.Fatalf("release first lease = %v err=%v", released, err)
			}
			second, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineB, Owner: ownerB, Duration: time.Hour, Now: now.Add(time.Second)})
			if err != nil || second.Mode != LeaseModeActive || second.Lease.Generation <= first.Lease.Generation {
				t.Fatalf("claim replacement owner = %#v err=%v", second, err)
			}
			ownerB.LeaseGeneration = second.Lease.Generation
			if _, err := store.RecordOwnerHeartbeatForLease(ctx, ownerB, time.Minute, time.Hour, now.Add(2*time.Second)); err != nil {
				t.Fatalf("record replacement owner heartbeat: %v", err)
			}

			if _, err := store.RecordOwnerHeartbeatForLease(ctx, ownerA, time.Minute, time.Hour, now.Add(3*time.Second)); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale owner heartbeat error = %v, want ErrControlLeaseNotHeld", err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load after stale heartbeat: %v", err)
			}
			currentOwner, ok := state.readOwner()
			if !ok || currentOwner.InstanceID != ownerB.InstanceID || currentOwner.MachineID != machineB.ID || currentOwner.LeaseGeneration != second.Lease.Generation {
				t.Fatalf("stale heartbeat replaced current owner: owner=%#v ok=%v lease=%#v", currentOwner, ok, state.ControlLease)
			}
			if state.ControlLease.HolderMachineID != machineB.ID || state.ControlLease.Generation != second.Lease.Generation {
				t.Fatalf("stale heartbeat changed current lease: %#v", state.ControlLease)
			}
		})
	}
}

func TestOwnerBoundChatPollMutationRejectsStaleOwnerAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC()
			scope := ScopeIdentity{ID: "scope:poll-normalize-owner", AccountID: "user-1", OSUser: "tester", Profile: "default"}
			machineA := MachineRecord{ID: "machine:poll-normalize-a", ScopeID: scope.ID, Kind: MachineKindPrimary}
			machineB := MachineRecord{ID: "machine:poll-normalize-b", ScopeID: scope.ID, Kind: MachineKindPrimary}
			path := "/chats/poll-normalize/messages?$skiptoken=opaque"
			if _, _, err := store.UpdateChatPoll(ctx, "poll-normalize-chat", func(poll *ChatPollState) error {
				poll.ChatID = "poll-normalize-chat"
				poll.Seeded = true
				poll.ContinuationPath = path
				poll.DeferredContinuationPath = path
				return nil
			}); err != nil {
				t.Fatalf("seed poll normalization row: %v", err)
			}
			leaseA, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineA, Duration: time.Minute, Now: now})
			if err != nil || leaseA.Mode != LeaseModeActive {
				t.Fatalf("claim owner A: %#v err=%v", leaseA, err)
			}
			if sqliteMode {
				migrateStoreToSQLiteForTest(t, store)
			}
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, leaseA.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A: released=%v err=%v", released, err)
			}
			leaseB, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineB, Duration: time.Minute, Now: now.Add(time.Second)})
			if err != nil || leaseB.Mode != LeaseModeActive || leaseB.Lease.Generation <= leaseA.Lease.Generation {
				t.Fatalf("claim owner B: %#v err=%v", leaseB, err)
			}
			if _, changed, err := store.UpdateChatPollForOwner(ctx, "poll-normalize-chat", machineA.ID, leaseA.Lease.Generation, func(poll *ChatPollState) error {
				poll.DeferredContinuationPath = "stale-owner-must-not-win"
				return nil
			}); err != nil || changed {
				t.Fatalf("stale owner poll mutation changed=%v err=%v, want an owner-fenced no-op", changed, err)
			}
			current, found, err := store.ChatPoll(ctx, "poll-normalize-chat")
			if err != nil || !found || current.DeferredContinuationPath != path {
				t.Fatalf("stale owner changed poll row: found=%v err=%v poll=%#v", found, err, current)
			}
			if _, changed, err := store.UpdateChatPollForOwner(ctx, "poll-normalize-chat", machineB.ID, leaseB.Lease.Generation, func(poll *ChatPollState) error {
				poll.DeferredContinuationPath = ""
				return nil
			}); err != nil || !changed {
				t.Fatalf("current owner poll mutation: changed=%v err=%v", changed, err)
			}
			current, found, err = store.ChatPoll(ctx, "poll-normalize-chat")
			if err != nil || !found || current.DeferredContinuationPath != "" {
				t.Fatalf("current owner did not mutate poll row: found=%v err=%v poll=%#v", found, err, current)
			}
		})
	}
}

func TestPendingOutboxAmbiguousRecoveryQueryIsSeparateAndDurablyGatedAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			now := time.Date(2026, 8, 30, 12, 30, 0, 0, time.UTC)
			expiredAt := now.Add(-outboxSendLease - time.Second)
			if err := store.Update(ctx, func(state *State) error {
				state.OutboxMessages["outbox:ambiguous-expired"] = OutboxMessage{
					ID: "outbox:ambiguous-expired", TeamsChatID: "chat:ambiguous",
					Kind: "helper", Body: "unknown external outcome",
					Status: OutboxStatusSending, LastSendError: "ambiguous Graph send; response lost",
					LastSendAttempt: expiredAt, CreatedAt: expiredAt,
				}
				state.OutboxMessages["outbox:ambiguous-fresh"] = OutboxMessage{
					ID: "outbox:ambiguous-fresh", TeamsChatID: "chat:ambiguous",
					Kind: "helper", Body: "still inside send lease",
					Status: OutboxStatusSending, LastSendError: "ambiguous Graph send; response lost",
					LastSendAttempt: now.Add(-time.Second), NextAttemptAt: now.Add(2 * time.Hour), CreatedAt: now.Add(-time.Second),
				}
				state.OutboxMessages["outbox:ordinary-queued"] = OutboxMessage{
					ID: "outbox:ordinary-queued", TeamsChatID: "chat:ambiguous",
					Kind: "helper", Body: "ordinary queued work",
					Status: OutboxStatusQueued, CreatedAt: now.Add(time.Second),
				}
				// These rows are deliberately earlier in the FIFO than the
				// expired candidate, but are still inside their send lease.  A
				// recovery query must filter eligibility before applying LIMIT;
				// otherwise SQLite's raw page can hide the only row that may be
				// reconciled by the cold recovery lane.
				for i := 0; i < 3; i++ {
					id := fmt.Sprintf("outbox:ambiguous-fresh-prefix:%d", i)
					state.OutboxMessages[id] = OutboxMessage{
						ID: id, TeamsChatID: "chat:ambiguous",
						Kind: "helper", Body: "fresh prefix",
						Status: OutboxStatusSending, LastSendError: "ambiguous Graph send; response lost",
						LastSendAttempt: now.Add(-time.Second), NextAttemptAt: now.Add(2 * time.Hour),
						CreatedAt: expiredAt.Add(-time.Duration(i+1) * time.Second),
					}
				}
				return nil
			}); err != nil {
				t.Fatalf("seed outbox rows: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}

			ordinary, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 20})
			if err != nil {
				t.Fatalf("ordinary pending query: %v", err)
			}
			if got := backlogOutboxIDs(ordinary.Messages); len(got) != 1 || got[0] != "outbox:ordinary-queued" {
				t.Fatalf("ordinary query = %#v, want only ordinary queued row", got)
			}

			limitedRecovery, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{
				Now: now, Limit: 1, IncludeAmbiguous: true, AmbiguousOnly: true,
			})
			if err != nil {
				t.Fatalf("limited ambiguous recovery query: %v", err)
			}
			if got := backlogOutboxIDs(limitedRecovery.Messages); len(got) != 1 || got[0] != "outbox:ambiguous-expired" {
				t.Fatalf("limited ambiguous recovery query = %#v, want expired candidate despite fresh prefix", got)
			}

			recoveryQuery := PendingOutboxQuery{
				Now: now, Limit: 20, IncludeAmbiguous: true, AmbiguousOnly: true,
			}
			recovery, err := store.PendingOutboxPageAt(ctx, recoveryQuery)
			if err != nil {
				t.Fatalf("ambiguous recovery query: %v", err)
			}
			if got := backlogOutboxIDs(recovery.Messages); len(got) != 1 || got[0] != "outbox:ambiguous-expired" {
				t.Fatalf("ambiguous recovery query = %#v, want only expired ambiguous row", got)
			}

			deferredUntil := now.Add(time.Hour)
			if _, err := store.DeferOutboxDeliveryUntil(ctx, "outbox:ambiguous-expired", deferredUntil); err != nil {
				t.Fatalf("defer ambiguous recovery: %v", err)
			}
			gated, err := store.PendingOutboxPageAt(ctx, recoveryQuery)
			if err != nil {
				t.Fatalf("gated ambiguous recovery query: %v", err)
			}
			if len(gated.Messages) != 0 {
				t.Fatalf("gated ambiguous recovery query = %#v, want no rows", gated.Messages)
			}
			recoveryQuery.Now = deferredUntil
			woken, err := store.PendingOutboxPageAt(ctx, recoveryQuery)
			if err != nil {
				t.Fatalf("woken ambiguous recovery query: %v", err)
			}
			if got := backlogOutboxIDs(woken.Messages); len(got) != 1 || got[0] != "outbox:ambiguous-expired" {
				t.Fatalf("woken ambiguous recovery query = %#v, want expired row", got)
			}
		})
	}
}

func TestPendingOutboxRecoveryAdoptsExpiredMarkerlessSendingAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			scope := ScopeIdentity{ID: "scope:markerless-recovery", AccountID: "user-1", OSUser: "tester", Profile: "default"}
			machine := MachineRecord{ID: "machine:markerless-recovery", ScopeID: scope.ID, Kind: MachineKindPrimary, Priority: DefaultMachinePriority(MachineKindPrimary)}
			lease, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Duration: time.Hour, Now: now})
			if err != nil || lease.Mode != LeaseModeActive {
				t.Fatalf("claim recovery owner = %#v err=%v", lease, err)
			}
			expiredAt := now.Add(-outboxSendLease - time.Minute)
			if err := store.Update(ctx, func(state *State) error {
				state.OutboxMessages["outbox:markerless-expired"] = OutboxMessage{
					ID: "outbox:markerless-expired", TeamsChatID: "chat:markerless", Kind: "final",
					Body: "legacy unknown outcome", Status: OutboxStatusSending,
					MachineID: machine.ID, LeaseGeneration: lease.Lease.Generation,
					LastSendAttempt: expiredAt, NextAttemptAt: now.Add(time.Hour), CreatedAt: expiredAt,
				}
				state.OutboxMessages["outbox:markerless-fresh"] = OutboxMessage{
					ID: "outbox:markerless-fresh", TeamsChatID: "chat:markerless", Kind: "final",
					Body: "still possibly in flight", Status: OutboxStatusSending,
					LastSendAttempt: now.Add(-time.Second), NextAttemptAt: now.Add(2 * time.Hour), CreatedAt: now.Add(-time.Second),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed markerless rows: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}

			ordinary, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 10})
			if err != nil {
				t.Fatalf("ordinary markerless query: %v", err)
			}
			if got := backlogOutboxIDs(ordinary.Messages); len(got) != 0 {
				t.Fatalf("ordinary query admitted markerless Sending rows: %v", got)
			}
			recoveryQuery := PendingOutboxQuery{
				Now: now, Limit: 10, IncludeAmbiguous: true, AmbiguousOnly: true,
			}
			recovery, err := store.PendingOutboxPageAt(ctx, recoveryQuery)
			if err != nil {
				t.Fatalf("markerless recovery query: %v", err)
			}
			if got := backlogOutboxIDs(recovery.Messages); len(got) != 0 {
				t.Fatalf("markerless recovery query before retry gate = %v, want no rows", got)
			}
			recoveryQuery.Now = now.Add(time.Hour)
			recovery, err = store.PendingOutboxPageAt(ctx, recoveryQuery)
			if err != nil {
				t.Fatalf("markerless recovery query after retry gate: %v", err)
			}
			if got := backlogOutboxIDs(recovery.Messages); len(got) != 1 || got[0] != "outbox:markerless-expired" {
				t.Fatalf("markerless recovery query after retry gate = %v, want expired row only", got)
			}
			if !OutboxSendRecoveryEligible(recovery.Messages[0], recoveryQuery.Now) {
				t.Fatalf("expired markerless row was not classified as recovery eligible: %#v", recovery.Messages[0])
			}

			bound, err := store.BindOutboxRecoveryAttemptForOwner(ctx, recovery.Messages[0].ID, machine.ID, lease.Lease.Generation)
			if err != nil || bound.SendAttemptToken == "" || !OutboxSendIsAmbiguous(bound) || bound.MachineID != machine.ID || bound.LeaseGeneration != lease.Lease.Generation {
				t.Fatalf("markerless recovery adoption = %#v err=%v, want owner-bound ambiguous row", bound, err)
			}
			ordinary, err = store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, Limit: 10})
			if err != nil {
				t.Fatalf("ordinary query after adoption: %v", err)
			}
			if got := backlogOutboxIDs(ordinary.Messages); len(got) != 0 {
				t.Fatalf("ordinary query admitted adopted ambiguous row: %v", got)
			}
		})
	}
}

func TestPendingOutboxRecoveryAdoptsTokenfulSendingWithoutPOSTProofAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			scope := ScopeIdentity{ID: "scope:tokenful-recovery", AccountID: "user-1", OSUser: "tester", Profile: "default"}
			machineA := MachineRecord{ID: "machine:tokenful-recovery-a", ScopeID: scope.ID, Kind: MachineKindPrimary, Priority: DefaultMachinePriority(MachineKindPrimary)}
			machineB := MachineRecord{ID: "machine:tokenful-recovery-b", ScopeID: scope.ID, Kind: MachineKindPrimary, Priority: DefaultMachinePriority(MachineKindPrimary)}
			leaseA, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineA, Duration: time.Hour, Now: now})
			if err != nil || leaseA.Mode != LeaseModeActive {
				t.Fatalf("claim owner A = %#v err=%v", leaseA, err)
			}
			if err := store.Update(ctx, func(state *State) error {
				state.OutboxMessages["outbox:tokenful-recovery"] = OutboxMessage{
					ID: "outbox:tokenful-recovery", TeamsChatID: "chat:tokenful-recovery", Kind: "final",
					Body: "outcome may already exist", Status: OutboxStatusSending,
					MachineID: machineA.ID, LeaseGeneration: leaseA.Lease.Generation,
					SendAttemptToken: "old-attempt-token", LastSendAttempt: now.Add(-outboxSendLease - time.Minute), CreatedAt: now.Add(-outboxSendLease - time.Minute),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed tokenful Sending row: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			recovery, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{
				Now: now.Add(time.Second), Limit: 10, IncludeAmbiguous: true, AmbiguousOnly: true,
			})
			if err != nil {
				t.Fatalf("tokenful recovery query: %v", err)
			}
			if got := backlogOutboxIDs(recovery.Messages); len(got) != 1 || got[0] != "outbox:tokenful-recovery" {
				t.Fatalf("tokenful recovery query = %v, want stale tokenful row", got)
			}
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, leaseA.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A = %v err=%v", released, err)
			}
			leaseB, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineB, Duration: time.Hour, Now: now.Add(time.Second)})
			if err != nil || leaseB.Mode != LeaseModeActive {
				t.Fatalf("claim owner B = %#v err=%v", leaseB, err)
			}
			bound, err := store.BindOutboxRecoveryAttemptForOwner(ctx, "outbox:tokenful-recovery", machineB.ID, leaseB.Lease.Generation)
			if err != nil {
				t.Fatalf("bind tokenful recovery = %#v err=%v", bound, err)
			}
			if bound.SendAttemptToken == "old-attempt-token" || !OutboxSendIsAmbiguous(bound) || bound.MachineID != machineB.ID || bound.LeaseGeneration != leaseB.Lease.Generation {
				t.Fatalf("tokenful recovery = %#v, want rotated explicit ambiguous owner-bound row", bound)
			}
			ordinary, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now.Add(time.Second), Limit: 10})
			if err != nil {
				t.Fatalf("ordinary query after tokenful recovery = %v", err)
			}
			if len(ordinary.Messages) != 0 {
				t.Fatalf("ambiguous tokenful row entered ordinary FIFO: %#v", ordinary.Messages)
			}
		})
	}
}

func TestStoreOwnerBoundOutboxAdmissionRejectsStaleOwnerAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			scope := ScopeIdentity{ID: "scope:outbox-owner-admission", AccountID: "user-1", OSUser: "tester", Profile: "default"}
			machineA := MachineRecord{ID: "machine:outbox-owner-a", ScopeID: scope.ID, Kind: MachineKindPrimary, Priority: DefaultMachinePriority(MachineKindPrimary)}
			machineB := MachineRecord{ID: "machine:outbox-owner-b", ScopeID: scope.ID, Kind: MachineKindPrimary, Priority: DefaultMachinePriority(MachineKindPrimary)}
			leaseA, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineA, Duration: time.Hour, Now: now})
			if err != nil || leaseA.Mode != LeaseModeActive {
				t.Fatalf("claim owner A = %#v err=%v", leaseA, err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}

			if _, created, err := store.QueueOutboxForOwner(ctx, OutboxMessage{
				ID: "outbox:stale-before-takeover", TeamsChatID: "chat:owner-admission", Kind: "helper", Body: "must not be admitted",
			}, machineB.ID, leaseA.Lease.Generation); !errors.Is(err, ErrControlLeaseNotHeld) || created {
				t.Fatalf("wrong-machine admission before takeover: created=%v err=%v, want lease rejection", created, err)
			}
			fresh, created, err := store.QueueOutboxForOwner(ctx, OutboxMessage{
				ID: "outbox:owner-a", TeamsChatID: "chat:owner-admission", Kind: "helper", Body: "owner A work",
			}, machineA.ID, leaseA.Lease.Generation)
			if err != nil || !created || fresh.MachineID != machineA.ID || fresh.LeaseGeneration != leaseA.Lease.Generation {
				t.Fatalf("owner A admission = %#v created=%v err=%v", fresh, created, err)
			}

			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, leaseA.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A = %v err=%v", released, err)
			}
			leaseB, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineB, Duration: time.Hour, Now: now.Add(time.Second)})
			if err != nil || leaseB.Mode != LeaseModeActive || leaseB.Lease.Generation <= leaseA.Lease.Generation {
				t.Fatalf("claim owner B = %#v err=%v", leaseB, err)
			}
			if _, created, err := store.QueueOutboxForOwner(ctx, OutboxMessage{
				ID: "outbox:stale-after-takeover", TeamsChatID: "chat:owner-admission", Kind: "helper", Body: "stale callback",
			}, machineA.ID, leaseA.Lease.Generation); !errors.Is(err, ErrControlLeaseNotHeld) || created {
				t.Fatalf("stale admission after takeover: created=%v err=%v, want lease rejection", created, err)
			}
			replacement, created, err := store.QueueOutboxForOwner(ctx, OutboxMessage{
				ID: "outbox:owner-b", TeamsChatID: "chat:owner-admission", Kind: "helper", Body: "owner B work",
			}, machineB.ID, leaseB.Lease.Generation)
			if err != nil || !created || replacement.MachineID != machineB.ID || replacement.LeaseGeneration != leaseB.Lease.Generation {
				t.Fatalf("owner B admission = %#v created=%v err=%v", replacement, created, err)
			}
		})
	}
}

func TestStoreOwnerFencedOutboxRecoveryAndQueuedCleanupAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			scope := ScopeIdentity{ID: "scope:outbox-fenced-mutations", AccountID: "user-1", OSUser: "tester", Profile: "default"}
			machineA := MachineRecord{ID: "machine:fenced-a", ScopeID: scope.ID, Kind: MachineKindPrimary, Priority: DefaultMachinePriority(MachineKindPrimary)}
			machineB := MachineRecord{ID: "machine:fenced-b", ScopeID: scope.ID, Kind: MachineKindPrimary, Priority: DefaultMachinePriority(MachineKindPrimary)}
			leaseA, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineA, Duration: time.Hour, Now: now})
			if err != nil || leaseA.Mode != LeaseModeActive {
				t.Fatalf("claim owner A = %#v err=%v", leaseA, err)
			}
			queued, created, err := store.QueueOutboxForOwner(ctx, OutboxMessage{
				ID: "outbox:fenced-ambiguous", TeamsChatID: "chat:fenced", Kind: "helper", Body: "unknown external outcome",
			}, machineA.ID, leaseA.Lease.Generation)
			if err != nil || !created {
				t.Fatalf("queue owner-A outbox created=%v err=%v", created, err)
			}
			claimed, err := store.MarkOutboxSendAttemptForOwner(ctx, queued.ID, machineA.ID, leaseA.Lease.Generation)
			if err != nil || claimed.SendAttemptToken == "" {
				t.Fatalf("claim owner-A outbox = %#v err=%v", claimed, err)
			}
			if _, err := store.MarkOutboxAmbiguousSendErrorForAttempt(ctx, claimed.ID, claimed.SendAttemptToken, "Graph response lost"); err != nil {
				t.Fatalf("mark ambiguous owner-A outbox: %v", err)
			}
			if _, _, err := store.QueueOutboxForOwner(ctx, OutboxMessage{
				ID: "outbox:fenced-queued", TeamsChatID: "chat:fenced", Kind: "helper", Body: "queued cleanup",
			}, machineA.ID, leaseA.Lease.Generation); err != nil {
				t.Fatalf("queue owner-A cleanup row: %v", err)
			}
			if _, _, err := store.QueueOutboxForOwner(ctx, OutboxMessage{
				ID: "outbox:fenced-source", TeamsChatID: "chat:fenced", Kind: "final", Body: "source changed", TeamsMessageID: "teams-source-fenced",
			}, machineA.ID, leaseA.Lease.Generation); err != nil {
				t.Fatalf("queue owner-A source row: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}

			oldToken := claimed.SendAttemptToken
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, leaseA.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A = %v err=%v", released, err)
			}
			leaseB, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineB, Duration: time.Hour, Now: now.Add(time.Second)})
			if err != nil || leaseB.Mode != LeaseModeActive || leaseB.Lease.Generation <= leaseA.Lease.Generation {
				t.Fatalf("claim owner B = %#v err=%v", leaseB, err)
			}
			bound, err := store.BindOutboxRecoveryAttemptForOwner(ctx, claimed.ID, machineB.ID, leaseB.Lease.Generation)
			if err != nil || bound.SendAttemptToken == "" || bound.SendAttemptToken == oldToken {
				t.Fatalf("bind replacement recovery = %#v err=%v", bound, err)
			}

			before, err := store.OutboxMessageByID(ctx, claimed.ID)
			if err != nil {
				t.Fatalf("read before stale defer: %v", err)
			}
			if _, err := store.DeferOutboxDeliveryUntilForOwner(ctx, claimed.ID, now.Add(time.Hour), machineA.ID, leaseA.Lease.Generation, oldToken); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale owner defer error = %v, want ErrControlLeaseNotHeld", err)
			}
			after, err := store.OutboxMessageByID(ctx, claimed.ID)
			if err != nil {
				t.Fatalf("read after stale defer: %v", err)
			}
			if after.NextAttemptAt != before.NextAttemptAt || after.MachineID != machineB.ID || after.LeaseGeneration != leaseB.Lease.Generation || after.SendAttemptToken != bound.SendAttemptToken {
				t.Fatalf("stale owner changed replacement row: before=%#v after=%#v", before, after)
			}
			deferred, err := store.DeferOutboxDeliveryUntilForOwner(ctx, claimed.ID, now.Add(time.Hour), machineB.ID, leaseB.Lease.Generation, bound.SendAttemptToken)
			if err != nil || !deferred.NextAttemptAt.Equal(now.Add(time.Hour)) {
				t.Fatalf("current owner defer = %#v err=%v, want durable gate", deferred, err)
			}

			if _, _, err := store.MarkOutboxSkippedIfQueuedForOwner(ctx, "outbox:fenced-queued", "obsolete queued row", machineA.ID, leaseA.Lease.Generation); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale queued cleanup error = %v, want ErrControlLeaseNotHeld", err)
			}
			retired, changed, err := store.MarkOutboxSkippedIfQueuedForOwner(ctx, "outbox:fenced-queued", "obsolete queued row", machineB.ID, leaseB.Lease.Generation)
			if err != nil || !changed || retired.Status != OutboxStatusSkipped {
				t.Fatalf("replacement queued cleanup = %#v changed=%v err=%v", retired, changed, err)
			}
			if _, _, err := store.MarkOutboxSourceRewriteFenceIfQueuedForOwner(ctx, "outbox:fenced-source", "teams-source-fenced", machineA.ID, leaseA.Lease.Generation); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale source fence error = %v, want ErrControlLeaseNotHeld", err)
			}
			fenced, changed, err := store.MarkOutboxSourceRewriteFenceIfQueuedForOwner(ctx, "outbox:fenced-source", "teams-source-fenced", machineB.ID, leaseB.Lease.Generation)
			if err != nil || !changed || fenced.Status != OutboxStatusAccepted || !fenced.BlockedBySourceRewrite || fenced.MachineID != machineB.ID || fenced.LeaseGeneration != leaseB.Lease.Generation {
				t.Fatalf("replacement source fence = %#v changed=%v err=%v", fenced, changed, err)
			}
		})
	}
}

func TestStoreTakeOverRunningTurnWithAnchorRebindsOnlyExpectedTurnAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			const (
				sessionID = "session:takeover-anchor"
				turnID    = "turn:takeover-anchor"
				threadID  = "thread:takeover-anchor"
				codexID   = "codex:takeover-anchor"
			)
			scope := ScopeIdentity{ID: "scope:takeover-anchor", AccountID: "user-1", OSUser: "tester", Profile: "default"}
			machineA := MachineRecord{ID: "machine:takeover-anchor-a", ScopeID: scope.ID, Kind: MachineKindPrimary, Priority: DefaultMachinePriority(MachineKindPrimary)}
			machineB := MachineRecord{ID: "machine:takeover-anchor-b", ScopeID: scope.ID, Kind: MachineKindPrimary, Priority: DefaultMachinePriority(MachineKindPrimary)}
			leaseA, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineA, Duration: time.Hour, Now: now})
			if err != nil || leaseA.Mode != LeaseModeActive {
				t.Fatalf("claim owner A = %#v err=%v", leaseA, err)
			}
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, TeamsChatID: "chat:takeover-anchor", Status: SessionStatusActive, CreatedAt: now, UpdatedAt: now}
				state.Turns[turnID] = Turn{
					ID: turnID, SessionID: sessionID, ScopeID: scope.ID,
					MachineID: machineA.ID, LeaseGeneration: leaseA.Lease.Generation,
					Status: TurnStatusRunning, CodexThreadID: threadID, CodexTurnID: codexID,
					StartedAt: now, CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed running turn: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, leaseA.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A = %v err=%v", released, err)
			}
			leaseB, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineB, Duration: time.Hour, Now: now.Add(time.Second)})
			if err != nil || leaseB.Mode != LeaseModeActive || leaseB.Lease.Generation <= leaseA.Lease.Generation {
				t.Fatalf("claim owner B = %#v err=%v", leaseB, err)
			}

			request := PersistInterruptedTurnWithAnchorRequest{
				SessionID: sessionID, TurnID: turnID, CheckpointID: sessionTranscriptCheckpointID(sessionID),
				CodexThreadID: threadID, CodexTurnID: codexID, RecoveryReason: "owner takeover",
				Anchor: ExecutionAnchor{ThreadID: threadID, CodexTurnID: codexID, Reason: "owner takeover", Provenance: ExecutionAnchorProvenanceRuntime},
			}
			if _, err := store.TakeOverRunningTurnWithAnchorForOwner(ctx, request, machineB.ID, leaseB.Lease.Generation, "machine:wrong", leaseA.Lease.Generation); !errors.Is(err, ErrStaleExecutionCallback) {
				t.Fatalf("wrong expected previous owner error = %v, want stale callback", err)
			}
			before, found, err := store.TurnByID(ctx, turnID)
			if err != nil || !found || before.Status != TurnStatusRunning || before.MachineID != machineA.ID || before.LeaseGeneration != leaseA.Lease.Generation {
				t.Fatalf("wrong-expectation mutation = %#v found=%v err=%v", before, found, err)
			}

			result, err := store.TakeOverRunningTurnWithAnchorForOwner(ctx, request, machineB.ID, leaseB.Lease.Generation, machineA.ID, leaseA.Lease.Generation)
			if err != nil || !result.Changed || result.Turn.Status != TurnStatusInterrupted || result.Turn.MachineID != machineB.ID || result.Turn.LeaseGeneration != leaseB.Lease.Generation {
				t.Fatalf("takeover result = %#v err=%v", result, err)
			}
			checkpoint, found, err := store.ImportCheckpoint(ctx, sessionTranscriptCheckpointID(sessionID))
			if err != nil || !found || checkpoint.UnresolvedExecution == nil || checkpoint.UnresolvedExecution.Generation <= 0 || checkpoint.UnresolvedExecution.ThreadID != threadID {
				t.Fatalf("takeover checkpoint = %#v found=%v err=%v, want unresolved anchor", checkpoint, found, err)
			}
			if _, err := store.TakeOverRunningTurnWithAnchorForOwner(ctx, request, machineB.ID, leaseB.Lease.Generation, machineA.ID, leaseA.Lease.Generation); !errors.Is(err, ErrStaleExecutionCallback) {
				t.Fatalf("repeat takeover error = %v, want stale callback for non-running turn", err)
			}
		})
	}
}

func TestStorePollFrontierAndScheduleRevisionRace(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			old, acquired, err := store.BeginChatPollAttempt(ctx, ChatPollAttemptRequest{
				ChatID:             "poll-race-chat",
				Owner:              "old-owner",
				ProcessIncarnation: "old-process",
				LeaseGeneration:    11,
				Now:                now,
				TTL:                time.Minute,
			})
			if err != nil || !acquired || old.Attempt == nil {
				t.Fatalf("begin old attempt acquired=%v attempt=%#v err=%v", acquired, old.Attempt, err)
			}
			oldCapability := ChatPollAttemptCapability{
				ID:                 old.Attempt.ID,
				Owner:              old.Attempt.Owner,
				ProcessIncarnation: old.Attempt.ProcessIncarnation,
				LeaseGeneration:    old.Attempt.LeaseGeneration,
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			// Model an owner handoff that preserves the attempt ID but changes the
			// durable identity. Attempt-ID-only CAS would accept a delayed callback
			// if it learned the new revision; the capability CAS must reject it.
			if _, changed, err := store.UpdateChatPoll(ctx, "poll-race-chat", func(poll *ChatPollState) error {
				if poll.Attempt == nil {
					t.Fatalf("poll attempt disappeared during handoff")
				}
				poll.Attempt.Owner = "new-owner"
				poll.Attempt.ProcessIncarnation = "new-process"
				poll.Attempt.LeaseGeneration = 12
				poll.Attempt.ExpectedPollRevision = poll.PollRevision + 1
				return nil
			}); err != nil || !changed {
				t.Fatalf("seed replacement capability changed=%v err=%v", changed, err)
			}
			current, found, err := store.ChatPoll(ctx, "poll-race-chat")
			if err != nil || !found || current.Attempt == nil {
				t.Fatalf("current poll after handoff found=%v attempt=%#v err=%v", found, current.Attempt, err)
			}
			if _, committed, err := store.CommitChatPollAttemptWithCapability(ctx, "poll-race-chat", oldCapability, current.PollRevision, func(poll *ChatPollState) error {
				poll.LastError = "stale callback must not win"
				return nil
			}); err != nil || committed {
				t.Fatalf("old capability commit committed=%v err=%v, want rejected", committed, err)
			}
			current, found, err = store.ChatPoll(ctx, "poll-race-chat")
			if err != nil || !found || current.Attempt == nil {
				t.Fatalf("current poll found=%v attempt=%#v err=%v", found, current.Attempt, err)
			}
			newCapability := oldCapability
			newCapability.Owner = "new-owner"
			newCapability.ProcessIncarnation = "new-process"
			newCapability.LeaseGeneration = 12
			if _, committed, err := store.CommitChatPollAttemptWithCapability(ctx, "poll-race-chat", newCapability, current.PollRevision, func(poll *ChatPollState) error {
				poll.LastError = "replacement callback committed"
				return nil
			}); err != nil || !committed {
				t.Fatalf("new capability commit committed=%v err=%v, want committed", committed, err)
			}
			final, found, err := store.ChatPoll(ctx, "poll-race-chat")
			if err != nil || !found {
				t.Fatalf("final poll found=%v err=%v", found, err)
			}
			if final.LastError != "replacement callback committed" || final.Attempt != nil {
				t.Fatalf("final poll = %#v, want replacement result without attempt", final)
			}
		})
	}
}

func TestStoreOwnerFencesPollInboundAndTurnAfterControlLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["owner-fence-session"] = SessionContext{
					ID: "owner-fence-session", Status: SessionStatusActive, TeamsChatID: "owner-fence-chat",
				}
				return nil
			}); err != nil {
				t.Fatalf("seed owner-fence session: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			now := time.Now().UTC()
			scope := ScopeIdentity{ID: "scope:owner-fence"}
			machineA := MachineRecord{ID: "machine:owner-fence-a", ScopeID: scope.ID, Kind: MachineKindEphemeral}
			machineB := MachineRecord{ID: "machine:owner-fence-b", ScopeID: scope.ID, Kind: MachineKindPrimary}
			leaseA, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope: scope, Machine: machineA, Duration: time.Minute, Now: now,
			})
			if err != nil || leaseA.Mode != LeaseModeActive {
				t.Fatalf("claim owner A: mode=%v err=%v", leaseA.Mode, err)
			}
			poll, acquired, err := store.BeginChatPollAttempt(ctx, ChatPollAttemptRequest{
				ChatID: "owner-fence-chat", Owner: machineA.ID, ProcessIncarnation: "process-a",
				LeaseGeneration: leaseA.Lease.Generation, Now: now, TTL: time.Minute,
			})
			if err != nil || !acquired || poll.Attempt == nil {
				t.Fatalf("begin owner A poll: acquired=%v poll=%#v err=%v", acquired, poll, err)
			}
			oldCapability := ChatPollAttemptCapability{
				ID: poll.Attempt.ID, Owner: poll.Attempt.Owner,
				ProcessIncarnation: poll.Attempt.ProcessIncarnation,
				LeaseGeneration:    leaseA.Lease.Generation,
			}
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, leaseA.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A: released=%v err=%v", released, err)
			}
			leaseB, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope: scope, Machine: machineB, Duration: time.Minute, Now: now.Add(time.Second),
			})
			if err != nil || leaseB.Mode != LeaseModeActive {
				t.Fatalf("claim owner B: mode=%v err=%v", leaseB.Mode, err)
			}

			if _, committed, err := store.CommitChatPollAttemptWithCapability(ctx, "owner-fence-chat", oldCapability, poll.PollRevision, func(poll *ChatPollState) error {
				poll.LastError = "stale poll writer must not commit"
				return nil
			}); err != nil || committed {
				t.Fatalf("stale poll commit: committed=%v err=%v", committed, err)
			}
			if _, created, err := store.PersistInbound(ctx, InboundEvent{
				ID: "inbound:owner-fence-stale", SessionID: "owner-fence-session",
				TeamsChatID: "owner-fence-chat", TeamsMessageID: "stale-message",
				MachineID: machineA.ID, LeaseGeneration: leaseA.Lease.Generation,
				Status: InboundStatusPersisted,
			}); !errors.Is(err, ErrControlLeaseNotHeld) || created {
				t.Fatalf("stale inbound: created=%v err=%v, want owner fence", created, err)
			}
			if _, created, err := store.QueueTurn(ctx, Turn{
				ID: "turn:owner-fence-stale", SessionID: "owner-fence-session",
				MachineID: machineA.ID, LeaseGeneration: leaseA.Lease.Generation,
			}); !errors.Is(err, ErrControlLeaseNotHeld) || created {
				t.Fatalf("stale turn: created=%v err=%v, want owner fence", created, err)
			}

			freshInbound, created, err := store.PersistInbound(ctx, InboundEvent{
				ID: "inbound:owner-fence-fresh", SessionID: "owner-fence-session",
				TeamsChatID: "owner-fence-chat", TeamsMessageID: "fresh-message",
				MachineID: machineB.ID, LeaseGeneration: leaseB.Lease.Generation,
				Status: InboundStatusPersisted,
			})
			if err != nil || !created {
				t.Fatalf("fresh inbound: created=%v err=%v", created, err)
			}
			if _, created, err := store.QueueTurn(ctx, Turn{
				ID: "turn:owner-fence-fresh", SessionID: "owner-fence-session",
				InboundEventID: freshInbound.ID, MachineID: machineB.ID,
				LeaseGeneration: leaseB.Lease.Generation,
			}); err != nil || !created {
				t.Fatalf("fresh turn: created=%v err=%v", created, err)
			}
		})
	}
}

func TestStoreOwnerFencesLateTurnCompletionAfterControlLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "late-completion-session"
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{
					ID: sessionID, Status: SessionStatusActive, TeamsChatID: "late-completion-chat",
				}
				return nil
			}); err != nil {
				t.Fatalf("seed session: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			now := time.Now().UTC()
			scope := ScopeIdentity{ID: "scope:late-completion"}
			machineA := MachineRecord{ID: "machine:late-completion-a", ScopeID: scope.ID, Kind: MachineKindEphemeral}
			machineB := MachineRecord{ID: "machine:late-completion-b", ScopeID: scope.ID, Kind: MachineKindPrimary}
			leaseA, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope: scope, Machine: machineA, Duration: time.Minute, Now: now,
			})
			if err != nil || leaseA.Mode != LeaseModeActive {
				t.Fatalf("claim owner A: mode=%v err=%v", leaseA.Mode, err)
			}
			turn, created, err := store.QueueTurn(ctx, Turn{
				ID: "turn:late-completion", SessionID: sessionID,
				MachineID: machineA.ID, LeaseGeneration: leaseA.Lease.Generation,
				CodexThreadID: "thread:late-completion",
			})
			if err != nil || !created {
				t.Fatalf("queue owner A turn: created=%v err=%v turn=%#v", created, err, turn)
			}
			turn, err = store.MarkTurnRunning(ctx, turn.ID, turn.CodexThreadID, "codex:late-completion")
			if err != nil {
				t.Fatalf("mark turn running: %v", err)
			}
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, leaseA.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A: released=%v err=%v", released, err)
			}
			leaseB, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope: scope, Machine: machineB, Duration: time.Minute, Now: now.Add(time.Second),
			})
			if err != nil || leaseB.Mode != LeaseModeActive {
				t.Fatalf("claim owner B: mode=%v err=%v", leaseB.Mode, err)
			}
			// These two compatibility APIs do not take an explicit capability.
			// They still must fence a callback from owner A after the control
			// lease has moved to B; otherwise a delayed recovery callback could
			// rebind the old turn or rewrite its recovery state.
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID+"-isolated"] = SessionContext{
					ID: sessionID + "-isolated", Status: SessionStatusActive,
					TeamsChatID: "late-completion-isolated-chat",
				}
				state.Turns["turn:late-isolation"] = Turn{
					ID: "turn:late-isolation", SessionID: sessionID + "-isolated",
					Status: TurnStatusQueued, MachineID: machineA.ID,
					LeaseGeneration: leaseA.Lease.Generation,
				}
				turn.RecoveryReason = "stale recovery reason"
				state.Turns[turn.ID] = turn
				return nil
			}); err != nil {
				t.Fatalf("seed stale compatibility callbacks: %v", err)
			}
			if _, err := store.MarkTurnForIsolatedCodexThread(ctx, "turn:late-isolation"); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("late isolated-thread mark error = %v, want ErrControlLeaseNotHeld", err)
			}
			if _, changed, err := store.UpdateTurnRecoveryReasonIfMatches(ctx, turn.ID, TurnStatusRunning, "stale recovery reason", "late callback"); !errors.Is(err, ErrControlLeaseNotHeld) || changed {
				t.Fatalf("late recovery reason update changed=%v err=%v, want owner fence", changed, err)
			}
			if _, err := store.MarkTurnRunning(ctx, turn.ID, turn.CodexThreadID, turn.CodexTurnID); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("late unscoped running callback error = %v, want ErrControlLeaseNotHeld", err)
			}
			if _, err := store.RequeueTurn(ctx, turn.ID); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("late unscoped requeue callback error = %v, want ErrControlLeaseNotHeld", err)
			}
			if _, err := store.MarkTurnInterrupted(ctx, turn.ID, "late interruption must not commit"); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("late unscoped interruption error = %v, want ErrControlLeaseNotHeld", err)
			}

			final := OutboxMessage{
				ID: "outbox:late-completion-final", SessionID: sessionID, TurnID: turn.ID,
				TeamsChatID: "late-completion-chat", Kind: "final", Body: "late completion must not commit",
			}
			_, err = store.CompleteTurnWithFinal(ctx, CompleteTurnWithFinalRequest{
				SessionID: sessionID, TurnID: turn.ID,
				CodexThreadID: turn.CodexThreadID, CodexTurnID: turn.CodexTurnID,
				Progress:    TranscriptCheckpointProgress{ID: sessionTranscriptCheckpointID(sessionID), SessionID: sessionID},
				FinalOutbox: []OutboxMessage{final},
			})
			if !errors.Is(err, ErrCompletionOwnerLost) {
				t.Fatalf("late completion error = %v, want ErrCompletionOwnerLost", err)
			}
			if _, err := store.MarkTurnFailedForExecution(ctx, ExecutionFailureIdentity{
				SessionID: sessionID, TurnID: turn.ID,
				ThreadID: turn.CodexThreadID, CodexTurnID: turn.CodexTurnID,
			}, "late failure must not commit"); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("late failure error = %v, want ErrControlLeaseNotHeld", err)
			}

			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load state after stale callbacks: %v", err)
			}
			if got := state.Turns[turn.ID]; got.Status != TurnStatusRunning || got.MachineID != machineA.ID || got.LeaseGeneration != leaseA.Lease.Generation {
				t.Fatalf("stale callback changed turn: %#v", got)
			}
			if _, ok := state.OutboxMessages[final.ID]; ok {
				t.Fatalf("stale completion created final outbox row: %#v", state.OutboxMessages[final.ID])
			}
			if checkpoint, ok := state.ImportCheckpoints[sessionTranscriptCheckpointID(sessionID)]; ok &&
				(checkpoint.LastRecordID != "" || checkpoint.LastOffset != 0 || checkpoint.Status == importCheckpointStatusComplete) {
				t.Fatalf("stale completion advanced transcript checkpoint: %#v", checkpoint)
			}
		})
	}
}

func TestStoreOwnerBindsLegacyQueuedTurnAndRejectsPreviousOwnerCallbacks(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			const sessionID = "legacy-queued-owner-session"
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{
					ID: sessionID, Status: SessionStatusActive, TeamsChatID: "legacy-queued-owner-chat",
				}
				return nil
			}); err != nil {
				t.Fatalf("seed session: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			now := time.Now().UTC()
			scope := ScopeIdentity{ID: "scope:legacy-queued-owner"}
			machineA := MachineRecord{ID: "machine:legacy-queued-owner-a", ScopeID: scope.ID, Kind: MachineKindEphemeral}
			machineB := MachineRecord{ID: "machine:legacy-queued-owner-b", ScopeID: scope.ID, Kind: MachineKindPrimary}
			leaseA, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineA, Duration: time.Minute, Now: now})
			if err != nil || leaseA.Mode != LeaseModeActive {
				t.Fatalf("claim owner A: mode=%v err=%v", leaseA.Mode, err)
			}
			// This simulates a queued row written by an older listener before
			// queued rows carried a lease capability.
			legacy, created, err := store.QueueTurn(ctx, Turn{ID: "turn:legacy-queued-owner", SessionID: sessionID, MachineID: machineA.ID})
			if err != nil || !created || legacy.MachineID != machineA.ID || legacy.LeaseGeneration != 0 {
				t.Fatalf("queue legacy turn: created=%v err=%v turn=%#v", created, err, legacy)
			}
			if _, err := store.MarkTurnForIsolatedCodexThread(ctx, legacy.ID); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("unscoped legacy isolation mark error = %v, want ErrControlLeaseNotHeld", err)
			}
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, leaseA.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A: released=%v err=%v", released, err)
			}
			leaseB, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineB, Duration: time.Minute, Now: now.Add(time.Second)})
			if err != nil || leaseB.Mode != LeaseModeActive {
				t.Fatalf("claim owner B: mode=%v err=%v", leaseB.Mode, err)
			}
			prepared, err := store.MarkTurnForIsolatedCodexThreadForOwner(ctx, legacy.ID, machineB.ID, leaseB.Lease.Generation)
			if err != nil {
				t.Fatalf("current owner adopts legacy queued turn: %v", err)
			}
			if prepared.Status != TurnStatusQueued || prepared.MachineID != machineA.ID || prepared.LeaseGeneration != 0 {
				t.Fatalf("legacy queued turn changed during safe adoption: %#v", prepared)
			}
			claimed, ok, err := store.ClaimNextQueuedTurnForOwner(ctx, sessionID, machineB.ID, leaseB.Lease.Generation)
			if err != nil || !ok {
				t.Fatalf("claim legacy queued turn for B: claimed=%v err=%v turn=%#v", ok, err, claimed)
			}
			if claimed.Status != TurnStatusRunning || claimed.MachineID != machineB.ID || claimed.LeaseGeneration != leaseB.Lease.Generation {
				t.Fatalf("claimed legacy turn = %#v, want B capability", claimed)
			}
			if _, err := store.RequeueTurnForOwner(ctx, claimed.ID, machineA.ID, leaseA.Lease.Generation); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("previous owner requeue error = %v, want ErrControlLeaseNotHeld", err)
			}
			if _, err := store.MarkTurnInterruptedForOwner(ctx, claimed.ID, "previous owner interruption", machineA.ID, leaseA.Lease.Generation); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("previous owner interruption error = %v, want ErrControlLeaseNotHeld", err)
			}
			if _, err := store.MarkTurnRunningForOwner(ctx, claimed.ID, "thread:stale", "codex:stale", machineA.ID, leaseA.Lease.Generation); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("previous owner running callback error = %v, want ErrControlLeaseNotHeld", err)
			}
			current, found, err := store.TurnByID(ctx, claimed.ID)
			if err != nil || !found {
				t.Fatalf("read rebound turn: found=%v err=%v", found, err)
			}
			if current.Status != TurnStatusRunning || current.MachineID != machineB.ID || current.LeaseGeneration != leaseB.Lease.Generation || current.CodexThreadID == "thread:stale" {
				t.Fatalf("previous owner changed rebound turn: %#v", current)
			}
		})
	}
}

func TestStoreLegacyMachineOnlyRunningTurnRejectsUnscopedCallbacksAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			defer store.Close()
			const (
				sessionID = "legacy-running-owner-session"
				turnID    = "legacy-running-owner-turn"
			)
			now := time.Now().UTC()
			if err := store.Update(ctx, func(state *State) error {
				state.Scope = ScopeIdentity{ID: "scope:legacy-running-owner"}
				state.ControlLease = ControlLease{
					ScopeID: "scope:legacy-running-owner", HolderMachineID: "machine:current-owner",
					Generation: 9, Status: ControlLeaseStatusActive,
					LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
				}
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				// A running row with only MachineID is a pre-generation legacy
				// representation. It cannot prove which process owns the active
				// lease, so an unscoped callback must not mutate it.
				state.Turns[turnID] = Turn{
					ID: turnID, SessionID: sessionID, Status: TurnStatusRunning,
					MachineID: "machine:old-owner", LeaseGeneration: 0,
					CodexThreadID: "thread:legacy-running", CodexTurnID: "codex:legacy-running",
					StartedAt: now,
				}
				// Some old rows have neither a MachineID nor a generation.  Once
				// any control lease is active, an unscoped callback cannot prove
				// that it belongs to that lease and must be fenced just like a
				// MachineID-only legacy row.
				state.Turns[turnID+"-empty-owner"] = Turn{
					ID: turnID + "-empty-owner", SessionID: sessionID, Status: TurnStatusQueued,
					CodexThreadID: "thread:legacy-empty", CodexTurnID: "codex:legacy-empty",
					QueuedAt: now, CreatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed legacy running turn: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			if _, err := store.MarkTurnInterrupted(ctx, turnID, "legacy callback must not interrupt"); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("legacy interruption error = %v, want ErrControlLeaseNotHeld", err)
			}
			if _, err := store.MarkTurnFailedWithCodexIDs(ctx, turnID, "legacy callback must not fail", "thread:legacy-running", "codex:legacy-running"); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("legacy failure error = %v, want ErrControlLeaseNotHeld", err)
			}
			if _, err := store.MarkTurnCompletedWithTranscriptCheckpoint(ctx, turnID, "thread:legacy-running", "codex:legacy-running", TranscriptCheckpointProgress{
				ID: sessionTranscriptCheckpointID(sessionID), SessionID: sessionID, LastRecordID: "legacy-final", LastOffset: 128,
			}); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("legacy completion error = %v, want ErrControlLeaseNotHeld", err)
			}
			if _, err := store.MarkTurnInterrupted(ctx, turnID+"-empty-owner", "empty-owner callback must not interrupt"); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("empty-owner interruption error = %v, want ErrControlLeaseNotHeld", err)
			}
			turn, found, err := store.TurnByID(ctx, turnID)
			if err != nil || !found || turn.Status != TurnStatusRunning || turn.MachineID != "machine:old-owner" || turn.LeaseGeneration != 0 {
				t.Fatalf("legacy callback changed turn: turn=%#v found=%v err=%v", turn, found, err)
			}
			if checkpoint, found, err := store.ImportCheckpoint(ctx, sessionTranscriptCheckpointID(sessionID)); err != nil || found {
				t.Fatalf("legacy callback created checkpoint: checkpoint=%#v found=%v err=%v", checkpoint, found, err)
			}
			emptyTurn, found, err := store.TurnByID(ctx, turnID+"-empty-owner")
			if err != nil || !found || emptyTurn.Status != TurnStatusQueued || emptyTurn.LeaseGeneration != 0 || emptyTurn.MachineID != "" {
				t.Fatalf("empty-owner callback changed turn: turn=%#v found=%v err=%v", emptyTurn, found, err)
			}
		})
	}
}

func TestStoreOwnerFencesAndRebindsDeferredInboundAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			defer store.Close()
			scope := ScopeIdentity{ID: "scope:inbound-owner-fence"}
			machineA := MachineRecord{ID: "machine:inbound-owner-a", ScopeID: scope.ID, Kind: MachineKindEphemeral}
			machineB := MachineRecord{ID: "machine:inbound-owner-b", ScopeID: scope.ID, Kind: MachineKindPrimary}
			now := time.Now().UTC()
			leaseA, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineA, Duration: time.Minute, Now: now})
			if err != nil || leaseA.Mode != LeaseModeActive {
				t.Fatalf("claim owner A: mode=%v err=%v", leaseA.Mode, err)
			}
			inbound := InboundEvent{
				ID: "inbound:owner-fence", SessionID: "session:owner-fence",
				TeamsChatID: "chat:owner-fence", TeamsMessageID: "message:owner-fence",
				ScopeID: scope.ID, MachineID: machineA.ID, LeaseGeneration: leaseA.Lease.Generation,
				Text: "deferred prompt", Source: "teams_session_import_deferred", Status: InboundStatusDeferred,
				CreatedAt: now, ReceivedAt: now,
			}
			if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
				t.Fatalf("persist deferred inbound: created=%v err=%v", created, err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, leaseA.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A: released=%v err=%v", released, err)
			}
			leaseB, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineB, Duration: time.Minute, Now: now.Add(time.Second)})
			if err != nil || leaseB.Mode != LeaseModeActive || leaseB.Lease.Generation <= leaseA.Lease.Generation {
				t.Fatalf("claim owner B: mode=%v generation=%d old=%d err=%v", leaseB.Mode, leaseB.Lease.Generation, leaseA.Lease.Generation, err)
			}
			update := func(current InboundEvent, found bool, updateNow time.Time) (InboundEvent, bool, error) {
				if !found || current.Status != InboundStatusDeferred {
					return current, false, nil
				}
				current.Status = InboundStatusIgnored
				current.Source += " recovered"
				current.UpdatedAt = updateNow
				return current, true, nil
			}
			if _, _, err := store.UpdateInboundEvent(ctx, inbound.ID, update); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("unscoped stale inbound update error = %v, want %v", err, ErrControlLeaseNotHeld)
			}
			if _, _, err := store.UpdateInboundEventForOwner(ctx, inbound.ID, machineA.ID, leaseA.Lease.Generation, update); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale owner inbound update error = %v, want %v", err, ErrControlLeaseNotHeld)
			}
			updated, changed, err := store.UpdateInboundEventForOwner(ctx, inbound.ID, machineB.ID, leaseB.Lease.Generation, update)
			if err != nil || !changed || updated.Status != InboundStatusIgnored || updated.MachineID != machineB.ID || updated.LeaseGeneration != leaseB.Lease.Generation {
				t.Fatalf("replacement owner inbound update = %#v changed=%v err=%v", updated, changed, err)
			}
			final, found, err := store.InboundEventByID(ctx, inbound.ID)
			if err != nil || !found || final.Status != InboundStatusIgnored || final.MachineID != machineB.ID || final.LeaseGeneration != leaseB.Lease.Generation {
				t.Fatalf("final deferred inbound = %#v found=%v err=%v", final, found, err)
			}
		})
	}
}

func TestStoreHistoryWatchOwnerCapabilityFencesTakeoverAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			defer store.Close()
			const checkpointID = "history-watch:owner-fence"
			now := time.Now().UTC()
			if err := store.Update(ctx, func(state *State) error {
				state.HistoryWatch[checkpointID] = HistoryWatchCheckpoint{
					ID: checkpointID, Path: "/tmp/history-owner-fence.jsonl",
					Size: 64, Offset: 64, Line: 4, SessionID: "history-owner-fence-session",
					UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed history-watch checkpoint: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			scope := ScopeIdentity{ID: "scope:history-owner-fence"}
			machineA := MachineRecord{ID: "machine:history-owner-fence-a", ScopeID: scope.ID, Kind: MachineKindEphemeral}
			machineB := MachineRecord{ID: "machine:history-owner-fence-b", ScopeID: scope.ID, Kind: MachineKindPrimary}
			leaseA, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineA, Duration: time.Minute, Now: now})
			if err != nil || leaseA.Mode != LeaseModeActive {
				t.Fatalf("claim owner A: mode=%v err=%v", leaseA.Mode, err)
			}
			state, err := store.HistoryWatchState(ctx)
			if err != nil {
				t.Fatalf("load history-watch state: %v", err)
			}
			expected := state.HistoryWatch[checkpointID]
			first := expected
			first.Offset = 96
			first.Size = 96
			first.Line = 6
			if err := store.UpdateHistoryWatchCheckpointIfCurrentForOwner(ctx, checkpointID, &expected, first, machineA.ID, leaseA.Lease.Generation); err != nil {
				t.Fatalf("owner A checkpoint CAS: %v", err)
			}
			if err := store.UpdateHistoryWatchForOwner(ctx, machineA.ID, leaseA.Lease.Generation, func(history map[string]HistoryWatchCheckpoint, _ *time.Time) error {
				checkpoint := history[checkpointID]
				checkpoint.LastFinalID = "owner-a-final"
				history[checkpointID] = checkpoint
				return nil
			}); err != nil {
				t.Fatalf("owner A history-watch update: %v", err)
			}
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, leaseA.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A: released=%v err=%v", released, err)
			}
			leaseB, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machineB, Duration: time.Minute, Now: now.Add(time.Second)})
			if err != nil || leaseB.Mode != LeaseModeActive {
				t.Fatalf("claim owner B: mode=%v err=%v", leaseB.Mode, err)
			}

			stale := first
			stale.Offset = 128
			stale.Size = 128
			if err := store.UpdateHistoryWatchCheckpointIfCurrentForOwner(ctx, checkpointID, &first, stale, machineA.ID, leaseA.Lease.Generation); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale history-watch CAS error = %v, want %v", err, ErrControlLeaseNotHeld)
			}
			if err := store.UpdateHistoryWatchForOwner(ctx, machineA.ID, leaseA.Lease.Generation, func(history map[string]HistoryWatchCheckpoint, _ *time.Time) error {
				checkpoint := history[checkpointID]
				checkpoint.LastFinalID = "stale-owner-final"
				history[checkpointID] = checkpoint
				return nil
			}); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale history-watch update error = %v, want %v", err, ErrControlLeaseNotHeld)
			}
			if err := store.DeleteHistoryWatchCheckpointIfCurrentForOwner(ctx, checkpointID, &first, machineA.ID, leaseA.Lease.Generation); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale history-watch delete error = %v, want %v", err, ErrControlLeaseNotHeld)
			}

			current, err := store.HistoryWatchState(ctx)
			if err != nil {
				t.Fatalf("load state after stale history watcher: %v", err)
			}
			if got := current.HistoryWatch[checkpointID]; got.Offset != first.Offset || got.LastFinalID != "owner-a-final" {
				t.Fatalf("stale history watcher changed checkpoint: %#v", got)
			}
			if err := store.UpdateHistoryWatchForOwner(ctx, machineB.ID, leaseB.Lease.Generation, func(history map[string]HistoryWatchCheckpoint, _ *time.Time) error {
				checkpoint := history[checkpointID]
				checkpoint.LastFinalID = "owner-b-final"
				history[checkpointID] = checkpoint
				return nil
			}); err != nil {
				t.Fatalf("owner B history-watch update: %v", err)
			}
			current, err = store.HistoryWatchState(ctx)
			if err != nil {
				t.Fatalf("load final history-watch state: %v", err)
			}
			if got := current.HistoryWatch[checkpointID]; got.LastFinalID != "owner-b-final" {
				t.Fatalf("owner B history watcher did not commit: %#v", got)
			}
		})
	}
}

func backlogOutboxIDs(messages []OutboxMessage) []string {
	ids := make([]string, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.ID)
	}
	sort.Strings(ids)
	return ids
}
