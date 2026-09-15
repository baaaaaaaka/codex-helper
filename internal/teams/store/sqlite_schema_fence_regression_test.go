package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gofrs/flock"
)

func seedSQLiteSchemaFenceHeartbeatStore(t *testing.T) (*Store, OwnerMetadata, time.Time) {
	t.Helper()
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	owner := testOwner("schema-fence-owner", "schema-fence-turn", now)
	owner.ScopeID = "schema-fence-scope"
	owner.MachineID = "schema-fence-machine"
	owner.LeaseGeneration = 7
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = ScopeIdentity{ID: owner.ScopeID, AccountID: "schema-fence-account", Profile: "default"}
		state.ServiceOwner = &owner
		state.LockOwner = &owner
		state.ControlLease = ControlLease{
			ScopeID:         owner.ScopeID,
			HolderMachineID: owner.MachineID,
			Generation:      owner.LeaseGeneration,
			Status:          ControlLeaseStatusActive,
			LeaseUntil:      now.Add(time.Hour),
			LastHeartbeat:   now,
			UpdatedAt:       now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed schema-fence owner: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	return store, owner, now
}

func writeSQLiteSchemaClaimRawForTest(t *testing.T, store *Store, raw []byte) {
	t.Helper()
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `INSERT INTO state_meta(key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, sqliteSchemaPreparationClaimKey, raw)
		return err
	})
}

func clearSQLiteSchemaClaimRawForTest(t *testing.T, store *Store) {
	t.Helper()
	withSQLiteUnpreparedTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `DELETE FROM state_meta WHERE key = ?`, sqliteSchemaPreparationClaimKey)
		return err
	})
}

func readSQLiteSchemaClaimRawForTest(t *testing.T, store *Store) []byte {
	t.Helper()
	var raw []byte
	if err := withSQLiteUnpreparedRawQueryForTest(store, `SELECT value FROM state_meta WHERE key = ?`, &raw, sqliteSchemaPreparationClaimKey); err != nil {
		t.Fatalf("read raw schema claim: %v", err)
	}
	return raw
}

// A preparation claim is a safety fence, not a best-effort retry hint.  In
// particular, an old timestamp can describe a still-running bounded repair,
// a future timestamp can come from clock skew, and malformed/empty bytes can
// be a torn write.  None of these forms may let a heartbeat renew ownership.
func TestSQLiteSchemaPreparationClaimFencesHeartbeatForMalformedFutureAndExpiredClaims(t *testing.T) {
	ctx := context.Background()
	claimTimes := []struct {
		name string
		raw  []byte
	}{
		{name: "empty", raw: []byte{}},
		{name: "malformed", raw: []byte(`{"claim_id"`)},
		{
			name: "future",
			raw: []byte(mustMarshalSQLiteSchemaPreparationClaim(sqliteSchemaPreparationClaim{
				ClaimID: "future-claim", DBPath: "/future/store.sqlite", ClaimedAt: time.Now().UTC().Add(time.Hour),
			})),
		},
		{
			name: "expired",
			raw: []byte(mustMarshalSQLiteSchemaPreparationClaim(sqliteSchemaPreparationClaim{
				ClaimID: "expired-claim", DBPath: "/expired/store.sqlite", ClaimedAt: time.Now().UTC().Add(-time.Hour),
			})),
		},
	}
	for _, tc := range claimTimes {
		t.Run(tc.name, func(t *testing.T) {
			store, owner, now := seedSQLiteSchemaFenceHeartbeatStore(t)
			writeSQLiteSchemaClaimRawForTest(t, store, tc.raw)

			beforeOwner, found, err := store.ReadOwner(ctx)
			if err != nil || !found {
				t.Fatalf("read owner before fenced heartbeat: found=%v err=%v", found, err)
			}
			beforeLease, err := readSQLiteControlLeaseForTest(t, store)
			if err != nil {
				t.Fatalf("read lease before fenced heartbeat: %v", err)
			}
			_, err = store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now.Add(time.Second))
			if !errors.Is(err, ErrSQLiteSchemaPreparationInProgress) {
				t.Fatalf("heartbeat with %s preparation claim error = %v, want preparation fence", tc.name, err)
			}

			if raw := readSQLiteSchemaClaimRawForTest(t, store); string(raw) != string(tc.raw) {
				t.Fatalf("heartbeat changed %s claim from %q to %q", tc.name, tc.raw, raw)
			}
			clearSQLiteSchemaClaimRawForTest(t, store)
			afterOwner, found, err := store.ReadOwner(ctx)
			if err != nil || !found {
				t.Fatalf("read owner after fenced heartbeat: found=%v err=%v", found, err)
			}
			if afterOwner.LastHeartbeat != beforeOwner.LastHeartbeat || afterOwner.LeaseGeneration != beforeOwner.LeaseGeneration {
				t.Fatalf("fenced heartbeat changed owner from %#v to %#v", beforeOwner, afterOwner)
			}
			afterLease, err := readSQLiteControlLeaseForTest(t, store)
			if err != nil {
				t.Fatalf("read lease after fenced heartbeat: %v", err)
			}
			if afterLease != beforeLease {
				t.Fatalf("fenced heartbeat changed lease from %#v to %#v", beforeLease, afterLease)
			}
		})
	}
}

// A published schema marker does not make a leftover preparation claim safe to
// ignore.  This is the crash window between the structural repair and its
// final claim cleanup; direct lease callers must observe the same fence as the
// listener startup path.
func TestSQLiteClaimControlLeaseRejectsOutstandingPreparationClaim(t *testing.T) {
	ctx := context.Background()
	claimCases := []struct {
		name string
		raw  []byte
	}{
		{name: "empty", raw: []byte{}},
		{name: "malformed", raw: []byte(`{"claim_id"`)},
		{name: "future", raw: []byte(mustMarshalSQLiteSchemaPreparationClaim(sqliteSchemaPreparationClaim{
			ClaimID: "claim-future", DBPath: "/future.sqlite", ClaimedAt: time.Now().UTC().Add(time.Hour),
		}))},
		{name: "expired", raw: []byte(mustMarshalSQLiteSchemaPreparationClaim(sqliteSchemaPreparationClaim{
			ClaimID: "claim-expired", DBPath: "/expired.sqlite", ClaimedAt: time.Now().UTC().Add(-time.Hour),
		}))},
	}
	for _, tc := range claimCases {
		t.Run(tc.name, func(t *testing.T) {
			store, owner, now := seedSQLiteSchemaFenceHeartbeatStore(t)
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, owner.MachineID, owner.LeaseGeneration); err != nil || !released {
				t.Fatalf("release seeded lease: released=%v err=%v", released, err)
			}
			writeSQLiteSchemaClaimRawForTest(t, store, tc.raw)
			replacement := OwnerMetadata{
				ScopeID: owner.ScopeID, MachineID: owner.MachineID + "-replacement",
				InstanceID: "replacement-" + tc.name, StartedAt: now,
			}
			_, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope:   ScopeIdentity{ID: owner.ScopeID, AccountID: "schema-fence-account", Profile: "default"},
				Machine: MachineRecord{ID: replacement.MachineID, ScopeID: owner.ScopeID, Status: MachineStatusActive},
				Owner:   replacement, Duration: time.Minute, Now: now,
			})
			if !errors.Is(err, ErrSQLiteSchemaPreparationInProgress) {
				t.Fatalf("claim with %s preparation residue error = %v, want preparation fence", tc.name, err)
			}
			if current, found, readErr := store.ReadOwner(ctx); readErr != nil {
				t.Fatalf("read owner after rejected claim: %v", readErr)
			} else if !found || current.MachineID != owner.MachineID {
				// ReleaseControlLease clears the lease but keeps the durable process
				// witness. It must never be replaced by the rejected claimant.
				t.Fatalf("rejected claim changed owner witness: found=%v owner=%#v", found, current)
			}
		})
	}
}

// A stale durable claim can be reclaimed only after the process-level
// preparation lock is available.  This prevents a slow preparation that has
// crossed the timestamp TTL from being mistaken for a crashed preparation.
func TestSQLiteExpiredSchemaPreparationClaimCannotBeReclaimedWhilePreparationLockIsHeld(t *testing.T) {
	ctx := context.Background()
	store, _, _ := seedSQLiteSchemaFenceHeartbeatStore(t)
	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("read SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("resolve SQLite path: %v", err)
	}
	claim := sqliteSchemaPreparationClaim{
		ClaimID: "expired-preparation-lock-claim", DBPath: dbPath, ClaimedAt: time.Now().UTC().Add(-time.Hour),
	}
	raw := []byte(mustMarshalSQLiteSchemaPreparationClaim(claim))
	writeSQLiteSchemaClaimRawForTest(t, store, raw)

	prepLock := flock.New(dbPath + sqliteSchemaPreparationLockSuffix)
	locked, err := prepLock.TryLockContext(ctx, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("hold schema preparation lock: %v", err)
	}
	if !locked {
		t.Fatalf("hold schema preparation lock: path already locked: %s", dbPath+sqliteSchemaPreparationLockSuffix)
	}
	t.Cleanup(func() { _ = prepLock.Unlock() })
	prepareCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := store.PrepareSQLiteSchemaBeforeOwner(prepareCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("preparation behind active process lock error = %v, want bounded context cancellation", err)
	}
	if got := readSQLiteSchemaClaimRawForTest(t, store); string(got) != string(raw) {
		t.Fatalf("blocked preparation replaced expired claim: got %q want %q", got, raw)
	}
}

// A preparer can die after publishing an opaque claim but before schema DDL or
// the ready marker.  Once the next process owns the exclusive preparation lock
// and observes no live control lease, that residue must be recoverable; otherwise
// every startup would remain fenced forever by an unreadable state_meta value.
func TestSQLiteSchemaPreparationReclaimsOpaqueClaimAfterExclusiveLockRecovery(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	migrateStoreToSQLiteForTest(t, store)

	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("read SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("resolve SQLite path: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, sqliteSchemaPreparationClaimKey, []byte(`{"claim_id"`))
		return err
	})

	if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
		t.Fatalf("recover opaque preparation claim: %v", err)
	}
	var marker string
	if err := withSQLiteRawQueryForTest(store, `SELECT value FROM state_meta WHERE key = ?`, func(raw []byte) error {
		marker = string(raw)
		return nil
	}, sqliteSchemaPreparationVersionKey); err != nil {
		t.Fatalf("read recovered schema marker: %v", err)
	} else if marker != sqliteSchemaPreparationVersion {
		t.Fatalf("recovered schema marker = %q, want %q", marker, sqliteSchemaPreparationVersion)
	}
	if err := withSQLiteRawQueryForTest(store, `SELECT value FROM state_meta WHERE key = ?`, func(raw []byte) error {
		return fmt.Errorf("opaque claim remains: %q", raw)
	}, sqliteSchemaPreparationClaimKey); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("recovered opaque claim lookup error = %v, want sql.ErrNoRows", err)
	}

	// The normal call above has released the database-specific lock.  Confirm it
	// is available again; a leaked lock would turn the next startup into a false
	// permanent fence.
	prepLock := flock.New(dbPath + sqliteSchemaPreparationLockSuffix)
	locked, err := prepLock.TryLockContext(ctx, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("reacquire released schema preparation lock: %v", err)
	}
	if !locked {
		t.Fatal("schema preparation lock remained held after recovery")
	}
	if err := prepLock.Unlock(); err != nil {
		t.Fatalf("release recovered schema preparation lock: %v", err)
	}
}

// The legacy heartbeat API has no positive generation capability.  It must
// not recreate an owner after owner-row cleanup while the durable lease is
// still active; otherwise a retired process can poison the next takeover.
func TestSQLiteUnscopedHeartbeatCannotWriteOverActiveLeaseAfterOwnerClear(t *testing.T) {
	ctx := context.Background()
	store, owner, now := seedSQLiteSchemaFenceHeartbeatStore(t)
	cleared, err := store.ClearOwnerIfSame(ctx, owner)
	if err != nil || !cleared {
		t.Fatalf("clear seeded owner: cleared=%v err=%v", cleared, err)
	}
	legacy := owner
	legacy.LeaseGeneration = 0
	legacy.InstanceID = "retired-unscoped-callback"
	legacy.MachineID = owner.MachineID
	if _, err := store.RecordOwnerHeartbeat(ctx, legacy, time.Minute, now.Add(time.Second)); !errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("unscoped heartbeat over active lease error = %v, want lease-not-held", err)
	}
	if current, found, err := store.ReadOwner(ctx); err != nil {
		t.Fatalf("read owner after rejected unscoped heartbeat: %v", err)
	} else if found {
		t.Fatalf("rejected unscoped heartbeat recreated owner: %#v", current)
	}
	lease, err := readSQLiteControlLeaseForTest(t, store)
	if err != nil {
		t.Fatalf("read active lease after rejected heartbeat: %v", err)
	}
	if lease.HolderMachineID != owner.MachineID || lease.Generation != owner.LeaseGeneration || !lease.LeaseUntil.After(now) {
		t.Fatalf("rejected unscoped heartbeat changed active lease: %#v", lease)
	}
}

// JSON null is syntactically valid for encoding/json but is not a valid value
// projection for required runtime objects.  Strict reads and lease recovery
// must reject it without replacing the raw evidence with a zero value.
func TestSQLiteNullRequiredRuntimeRowsFailClosed(t *testing.T) {
	ctx := context.Background()
	for _, key := range []string{sqliteRuntimeKeyScope, sqliteRuntimeKeyMachineIdentity, sqliteRuntimeKeyMachines} {
		t.Run(key, func(t *testing.T) {
			store, owner, now := seedSQLiteSchemaFenceHeartbeatStore(t)
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, []byte("null"), key)
				return err
			})

			if _, err := store.Load(ctx); err == nil {
				t.Fatalf("Load accepted null required runtime row %q", key)
			}
			claimOwner := owner
			claimOwner.MachineID = owner.MachineID + "-replacement"
			claimOwner.InstanceID = "replacement"
			_, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope:    ScopeIdentity{ID: "replacement-scope", AccountID: "replacement-account", Profile: "default"},
				Machine:  MachineRecord{ID: claimOwner.MachineID, ScopeID: "replacement-scope", Status: MachineStatusActive},
				Owner:    claimOwner,
				Duration: time.Minute,
				Now:      now.Add(2 * time.Hour),
			})
			if err == nil {
				t.Fatalf("ClaimControlLease accepted null required runtime row %q", key)
			}
			var raw []byte
			if err := withSQLiteRawQueryForTest(store, `SELECT json FROM runtime_state WHERE key = ?`, &raw, key); err != nil {
				t.Fatalf("read preserved null runtime row %q: %v", key, err)
			}
			if string(raw) != "null" {
				t.Fatalf("null runtime row %q was rewritten to %q", key, raw)
			}
		})
	}
}

// A cached runtime handle must re-check the durable readiness marker on every
// operation.  Otherwise a trigger repair can invalidate the marker while the
// old connection remains open and the next heartbeat can still mutate the
// unprepared schema.
func TestSQLiteCachedRuntimeHandleRejectsMarkerRevocation(t *testing.T) {
	ctx := context.Background()
	store, owner, now := seedSQLiteSchemaFenceHeartbeatStore(t)
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now.Add(time.Second)); err != nil {
		t.Fatalf("prime cached runtime handle: %v", err)
	}
	var beforeOwnerRaw []byte
	if err := withSQLiteRawQueryForTest(store, `SELECT json FROM runtime_state WHERE key = ?`, &beforeOwnerRaw, sqliteRuntimeKeyServiceOwner); err != nil {
		t.Fatalf("read owner projection after priming runtime handle: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey)
		return err
	})
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now.Add(2*time.Second)); !errors.Is(err, ErrSQLiteSchemaPreparationRequired) {
		t.Fatalf("heartbeat after readiness marker revocation error = %v, want preparation-required", err)
	}
	var afterOwnerRaw []byte
	if err := withSQLiteUnpreparedRawQueryForTest(store, `SELECT json FROM runtime_state WHERE key = ?`, &afterOwnerRaw, sqliteRuntimeKeyServiceOwner); err != nil {
		t.Fatalf("read owner projection after marker revocation: %v", err)
	}
	if string(afterOwnerRaw) != string(beforeOwnerRaw) {
		t.Fatalf("marker-revoked heartbeat changed owner projection from %q to %q", beforeOwnerRaw, afterOwnerRaw)
	}
	lease, err := readSQLiteControlLeaseWithoutSchemaMarkerForTest(t, store)
	if err != nil {
		t.Fatalf("read lease after marker revocation: %v", err)
	}
	if !lease.LastHeartbeat.Equal(now) {
		t.Fatalf("marker-revoked heartbeat changed lease heartbeat to %s", lease.LastHeartbeat)
	}
}

// Foreground hot reads share a cached connection with ordinary Store APIs.  A
// peer schema repair can revoke the durable marker after that connection was
// opened, so the cached path must stop both bounded admission and full-state
// loading until the fenced preparer republishes readiness.
func TestSQLiteCachedForegroundHandleRejectsMarkerRevocation(t *testing.T) {
	ctx := context.Background()
	store, _, _ := seedSQLiteSchemaFenceHeartbeatStore(t)
	if _, found, err := store.ChatPoll(ctx, "schema-fence-chat"); err != nil || found {
		t.Fatalf("prime cached foreground handle: found=%v err=%v", found, err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey)
		return err
	})

	if _, _, err := store.ChatPoll(ctx, "schema-fence-chat"); !errors.Is(err, ErrSQLiteSchemaPreparationRequired) {
		t.Fatalf("cached foreground hot read after readiness revocation error = %v, want preparation-required", err)
	}
	if _, err := store.Load(ctx); !errors.Is(err, ErrSQLiteSchemaPreparationRequired) {
		t.Fatalf("cached foreground full load after readiness revocation error = %v, want preparation-required", err)
	}

}

// A cached foreground handle must not treat a ready marker as sufficient when
// a durable preparation claim is still present. This is the crash window after
// schema publication and before claim cleanup; every ordinary path must remain
// behind the same fence as startup/heartbeat rather than using its in-memory
// readiness bit to cross it.
func TestSQLiteCachedForegroundHandleRejectsOutstandingPreparationClaim(t *testing.T) {
	ctx := context.Background()
	store, _, _ := seedSQLiteSchemaFenceHeartbeatStore(t)
	if _, found, err := store.ChatPoll(ctx, "schema-fence-cached-claim-chat"); err != nil || found {
		t.Fatalf("prime cached foreground handle: found=%v err=%v", found, err)
	}
	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("read SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("resolve SQLite path: %v", err)
	}
	// Deliberately use non-canonical formatting and a legacy claim without a
	// physical witness. json.Unmarshal still considers it valid; the exact raw
	// bytes are needed for safe cleanup and the claim must remain a hard fence.
	raw := []byte(fmt.Sprintf("{\n  \"claimed_at\": %q,\n  \"db_path\": %q,\n  \"claim_id\": \"cached-claim\"\n}",
		time.Now().UTC().Format(time.RFC3339Nano), dbPath))
	writeSQLiteSchemaClaimRawForTest(t, store, raw)

	if _, err := store.Load(ctx); !errors.Is(err, ErrSQLiteSchemaPreparationInProgress) {
		t.Fatalf("cached full load with outstanding claim error = %v, want preparation fence", err)
	}
	if _, _, err := store.ChatPoll(ctx, "schema-fence-cached-claim-chat"); !errors.Is(err, ErrSQLiteSchemaPreparationInProgress) {
		t.Fatalf("cached hot read with outstanding claim error = %v, want preparation fence", err)
	}
	callbackCalled := false
	if err := store.Update(ctx, func(state *State) error {
		callbackCalled = true
		return nil
	}); !errors.Is(err, ErrSQLiteSchemaPreparationInProgress) {
		t.Fatalf("cached update with outstanding claim error = %v, want preparation fence", err)
	}
	if callbackCalled {
		t.Fatal("cached update invoked its mutating callback through a preparation claim")
	}
}

// Claim cleanup must use the exact durable bytes that were observed. A legal
// non-canonical JSON encoding must not be re-marshaled into a different DELETE
// predicate and then reported as successfully cleared.
func TestSQLiteSchemaPreparationClaimCleanupUsesExactRawBytes(t *testing.T) {
	ctx := context.Background()
	store, _, _ := seedSQLiteSchemaFenceHeartbeatStore(t)
	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("read SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("resolve SQLite path: %v", err)
	}
	raw := []byte(fmt.Sprintf("{ \"db_path\": %q, \"claimed_at\": %q, \"claim_id\": \"raw-cleanup\" }",
		dbPath, time.Now().UTC().Format(time.RFC3339Nano)))
	writeSQLiteSchemaClaimRawForTest(t, store, raw)

	withSQLiteUnpreparedTxForTest(t, store, func(tx *sql.Tx) error {
		claim, present, valid, err := loadSQLiteSchemaPreparationClaim(ctx, tx)
		if err != nil {
			return err
		}
		if !present || !valid {
			return fmt.Errorf("loaded claim present=%v valid=%v", present, valid)
		}
		if string(claim.Raw) != string(raw) {
			return fmt.Errorf("loaded raw claim = %q, want %q", claim.Raw, raw)
		}
		return clearSQLiteSchemaPreparationClaim(ctx, tx, claim)
	})
	var got []byte
	if err := withSQLiteRawQueryForTest(store, `SELECT value FROM state_meta WHERE key = ?`, &got, sqliteSchemaPreparationClaimKey); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("exact raw claim cleanup lookup error = %v, want sql.ErrNoRows (value=%q)", err, got)
	}
}

// A live owner may clean a completed same-path claim after a crash, but it
// must not silently delete an opaque or foreign claim. The latter may belong
// to a preparer for another physical database and remains a hard fence unless
// the physical identity proves that it is copied-file residue.
func TestSQLiteLiveSchemaPreparationDoesNotClearForeignOrOpaqueClaim(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		raw  func(t *testing.T, dbPath string) []byte
	}{
		{name: "opaque", raw: func(t *testing.T, _ string) []byte { return []byte(`{"claim_id"`) }},
		{name: "foreign-same-physical", raw: func(t *testing.T, dbPath string) []byte {
			identity, err := sqliteReadOnlyFileIdentityForPath(dbPath)
			if err != nil {
				t.Fatalf("read SQLite physical identity: %v", err)
			}
			return []byte(fmt.Sprintf(`{"claim_id":"foreign-live-claim","db_path":"/foreign/preparer.sqlite","claimed_at":%q,"physical_revision":%q}`,
				time.Now().UTC().Format(time.RFC3339Nano), identity.Revision))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC()
			scope := ScopeIdentity{ID: "scope-live-claim-" + tc.name, AccountID: "account-live-claim-" + tc.name, OSUser: "tester", Profile: "default"}
			if err := store.Update(ctx, func(state *State) error {
				state.Scope = scope
				return nil
			}); err != nil {
				t.Fatalf("seed live-claim scope: %v", err)
			}
			migrateStoreToSQLiteForTest(t, store)
			if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
				t.Fatalf("prepare schema before live claim: %v", err)
			}
			machine := MachineRecord{ID: "machine-live-claim-" + tc.name, ScopeID: scope.ID, Kind: MachineKindPrimary, Status: MachineStatusActive}
			decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope: scope, Machine: machine, Owner: testOwner(machine.ID, "", now), Duration: time.Hour, Now: now,
			})
			if err != nil || decision.Mode != LeaseModeActive {
				t.Fatalf("claim live-claim owner: decision=%#v err=%v", decision, err)
			}
			lease, leaseErr := readSQLiteControlLeaseForTest(t, store)
			if leaseErr != nil {
				t.Fatalf("read live-claim lease: %v", leaseErr)
			}
			if lease.HolderMachineID == "" || !lease.LeaseUntil.After(now) {
				t.Fatalf("live-claim lease = %#v, want active lease after %s", lease, now)
			}
			pointer, ok, err := store.currentSQLitePointerReadOnly()
			if err != nil || !ok {
				t.Fatalf("read SQLite pointer: ok=%v err=%v", ok, err)
			}
			dbPath, err := store.storeSQLitePath(pointer)
			if err != nil {
				t.Fatalf("resolve SQLite path: %v", err)
			}
			raw := tc.raw(t, dbPath)
			writeSQLiteSchemaClaimRawForTest(t, store, raw)
			if got := readSQLiteSchemaClaimRawForTest(t, store); string(got) != string(raw) {
				t.Fatalf("seeded %s claim = %q, want %q", tc.name, got, raw)
			}
			var marker string
			if err := withSQLiteUnpreparedRawQueryForTest(store, `SELECT value FROM state_meta WHERE key = ?`, &marker, sqliteSchemaPreparationVersionKey); err != nil {
				t.Fatalf("read schema marker before live preparation: %v", err)
			}
			if marker != sqliteSchemaPreparationVersion {
				t.Fatalf("schema marker before live preparation = %q, want %q", marker, sqliteSchemaPreparationVersion)
			}

			if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); !errors.Is(err, ErrSQLiteSchemaPreparationInProgress) {
				t.Fatalf("live preparation with %s claim error = %v, want preparation fence", tc.name, err)
			}
			if got := readSQLiteSchemaClaimRawForTest(t, store); string(got) != string(raw) {
				t.Fatalf("live preparation changed %s claim from %q to %q", tc.name, raw, got)
			}
		})
	}
}

func readSQLiteControlLeaseForTest(t *testing.T, store *Store) (ControlLease, error) {
	t.Helper()
	var lease ControlLease
	err := withSQLiteUnpreparedRawQueryForTest(store, `SELECT json FROM runtime_state WHERE key = ?`, func(raw []byte) error {
		return json.Unmarshal(raw, &lease)
	}, sqliteRuntimeKeyControlLease)
	return lease, err
}

func withSQLiteRawQueryForTest(store *Store, query string, destination any, args ...any) error {
	return store.withStateLock(context.Background(), func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("store is not backed by SQLite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		if callback, ok := destination.(func([]byte) error); ok {
			var raw []byte
			if err := db.QueryRowContext(context.Background(), query, args...).Scan(&raw); err != nil {
				return err
			}
			return callback(raw)
		}
		return db.QueryRowContext(context.Background(), query, args...).Scan(destination)
	})
}

// withSQLiteUnpreparedTxForTest is a forensic-only helper. Once a durable
// preparation claim exists, ordinary Store APIs intentionally reject all
// cached reads/writes; tests that need to inspect or remove the claim must use
// the setup-free maintenance handle explicitly.
func withSQLiteUnpreparedTxForTest(t *testing.T, store *Store, fn func(*sql.Tx) error) {
	t.Helper()
	ctx := context.Background()
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("store is not backed by SQLite")
		}
		dbPath, err := store.storeSQLitePath(pointer)
		if err != nil {
			return err
		}
		return withSQLiteSchemaPreparationDB(ctx, dbPath, func(db *sql.DB) error {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if err := fn(tx); err != nil {
				return err
			}
			return tx.Commit()
		})
	}); err != nil {
		t.Fatalf("with unprepared sqlite tx: %v", err)
	}
}

// withSQLiteUnpreparedRawQueryForTest is only for inspecting forensic bytes
// after a test deliberately revokes the readiness marker. Production store
// APIs must continue to fail closed in that state; this helper opens the
// setup-free preparation handle and performs a read without invoking DDL.
func withSQLiteUnpreparedRawQueryForTest(store *Store, query string, destination any, args ...any) error {
	return store.withStateLock(context.Background(), func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("store is not backed by SQLite")
		}
		dbPath, err := store.storeSQLitePath(pointer)
		if err != nil {
			return err
		}
		return withSQLiteSchemaPreparationDB(context.Background(), dbPath, func(db *sql.DB) error {
			if callback, ok := destination.(func([]byte) error); ok {
				var raw []byte
				if err := db.QueryRowContext(context.Background(), query, args...).Scan(&raw); err != nil {
					return err
				}
				return callback(raw)
			}
			return db.QueryRowContext(context.Background(), query, args...).Scan(destination)
		})
	})
}

func readSQLiteControlLeaseWithoutSchemaMarkerForTest(t *testing.T, store *Store) (ControlLease, error) {
	t.Helper()
	var lease ControlLease
	err := withSQLiteUnpreparedRawQueryForTest(store, `SELECT json FROM runtime_state WHERE key = ?`, func(raw []byte) error {
		return json.Unmarshal(raw, &lease)
	}, sqliteRuntimeKeyControlLease)
	return lease, err
}
