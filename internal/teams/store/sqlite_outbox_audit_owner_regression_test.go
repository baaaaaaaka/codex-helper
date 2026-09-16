package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The ownership/concurrency tests pause the audit at explicit hook boundaries.
// They need one valid row to exercise the scan, not a large corpus: a large
// JSON fixture only turns the race build into an avoidable full-scan tail.
const sqliteOutboxAuditOwnershipRowsForTest = 1

func seedSQLiteOutboxAuditOwnerForTest(t *testing.T, store *Store, owner OwnerMetadata, rows int) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.Update(context.Background(), func(state *State) error {
		state.Scope = ScopeIdentity{ID: owner.ScopeID, AccountID: "audit-owner-account"}
		state.ControlLease = ControlLease{
			ScopeID: owner.ScopeID, HolderMachineID: owner.MachineID,
			Generation: owner.LeaseGeneration, Status: ControlLeaseStatusActive,
			LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
		}
		serviceOwner := owner
		lockOwner := owner
		state.ServiceOwner = &serviceOwner
		state.LockOwner = &lockOwner
		state.OutboxMessages = make(map[string]OutboxMessage, rows)
		for i := 0; i < rows; i++ {
			id := "outbox:audit-owner:" + string(rune('a'+i%26)) + ":" + time.Unix(int64(i), 0).UTC().Format("150405.000000000")
			state.OutboxMessages[id] = OutboxMessage{
				ID: id, TeamsChatID: "chat:audit-owner", Kind: "helper",
				Body: "audit owner test payload", Status: OutboxStatusQueued,
				Sequence: int64(i + 1), CreatedAt: now.Add(time.Duration(i) * time.Microsecond), UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed audit owner state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for _, key := range []string{
			sqliteOutboxProjectionTrustKey,
			sqliteOutboxSessionProjectionTrustKey,
			sqliteOutboxTurnProjectionTrustKey,
		} {
			if err := sqliteWriteMetaValueContext(context.Background(), tx, key, sqliteOutboxProjectionTrustDeferred); err != nil {
				return err
			}
		}
		return nil
	})
}

func installSQLiteOutboxAuditClaimForTest(t *testing.T, store *Store, owner OwnerMetadata, claimedAt time.Time) {
	t.Helper()
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		identity, err := sqliteReadMetaValueContext(context.Background(), tx, sqliteOutboxDatabaseIdentityKey)
		if err != nil {
			return err
		}
		generation, err := sqliteReadOutboxGenerationContext(context.Background(), tx)
		if err != nil {
			return err
		}
		dbPath := filepath.Join(filepath.Dir(store.Path()), SQLiteFileName)
		physical, err := sqliteReadOnlyFileIdentityForPath(dbPath)
		if err != nil {
			return err
		}
		claim, err := sqliteOutboxProjectionAuditClaimForOwner(owner, identity, physical.Revision, generation, sqliteOutboxProjectionAuditPendingFIFO|sqliteOutboxProjectionAuditPendingSession|sqliteOutboxProjectionAuditPendingTurn)
		if err != nil {
			return err
		}
		claim.ClaimedAt = claimedAt
		return writeSQLiteOutboxProjectionAuditClaim(context.Background(), tx, claim)
	})
}

func TestSQLiteOwnerOutboxProjectionAuditIsSingleFlightAcrossStores(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{
		PID: 92001, Hostname: "audit-owner-host", ExecutablePath: "/opt/cxp",
		InstanceID: "audit-owner-instance", ScopeID: "audit-owner-scope",
		MachineID: "audit-owner-machine", LeaseGeneration: 1,
	}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, sqliteOutboxAuditOwnershipRowsForTest)
	peer, err := Open(store.Path())
	if err != nil {
		t.Fatalf("open peer store: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })

	opened := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) {
		if stage != "opened" {
			return
		}
		once.Do(func() { close(opened) })
		<-release
	}
	t.Cleanup(func() {
		sqliteOutboxAuditTestHook = previousHook
		select {
		case <-release:
		default:
			close(release)
		}
	})

	firstDone := make(chan error, 1)
	go func() { firstDone <- store.RetryDeferredOutboxProjectionAuditForOwner(ctx, owner) }()
	select {
	case <-opened:
	case err := <-firstDone:
		t.Fatalf("first audit ended before opening: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("first audit did not open")
	}
	if err := peer.RetryDeferredOutboxProjectionAuditForOwner(ctx, owner); !errors.Is(err, ErrSQLiteOutboxProjectionAuditInProgress) {
		t.Fatalf("concurrent peer audit error = %v, want ErrSQLiteOutboxProjectionAuditInProgress", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first audit: %v", err)
	}
	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, store) {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("single-flight marker %s = %q, want trusted", key, marker)
		}
	}
}

func TestSQLiteOfflineOutboxProjectionAuditReclaimsStaleOwnerClaim(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-stale-owner-scope", MachineID: "audit-stale-owner-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	if _, err := store.ReleaseControlLeaseIfHolder(ctx, owner.MachineID, owner.LeaseGeneration); err != nil {
		t.Fatalf("release seeded owner lease: %v", err)
	}
	installSQLiteOutboxAuditClaimForTest(t, store, owner, time.Now().UTC().Add(-sqliteOutboxProjectionAuditClaimStaleAfter-time.Second))

	if err := store.RetryDeferredOutboxProjectionAudit(ctx); err != nil {
		t.Fatalf("offline audit should reclaim stale owner claim: %v", err)
	}
	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, store) {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("reclaimed stale claim marker %s = %q, want trusted", key, marker)
		}
	}
}

func TestSQLiteOfflineOutboxProjectionAuditDoesNotReclaimRecentOwnerClaim(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-recent-owner-scope", MachineID: "audit-recent-owner-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	if _, err := store.ReleaseControlLeaseIfHolder(ctx, owner.MachineID, owner.LeaseGeneration); err != nil {
		t.Fatalf("release seeded owner lease: %v", err)
	}
	installSQLiteOutboxAuditClaimForTest(t, store, owner, time.Now().UTC())

	if err := store.RetryDeferredOutboxProjectionAudit(ctx); !errors.Is(err, ErrSQLiteOutboxProjectionAuditInProgress) {
		t.Fatalf("offline audit recent owner claim error = %v, want InProgress", err)
	}
}

func TestSQLiteOutboxProjectionAuditClaimInvalidTimesAreRecoverable(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	base := sqliteOutboxProjectionAuditClaim{ClaimedAt: time.Time{}}
	if !sqliteOutboxProjectionAuditClaimIsStale(base, now) {
		t.Fatal("zero claimed-at was treated as live")
	}
	future := base
	future.ClaimedAt = now.Add(2 * time.Minute)
	if !sqliteOutboxProjectionAuditClaimIsStale(future, now) {
		t.Fatal("materially future claimed-at was treated as live")
	}
	recentFuture := base
	recentFuture.ClaimedAt = now.Add(30 * time.Second)
	if sqliteOutboxProjectionAuditClaimIsStale(recentFuture, now) {
		t.Fatal("small clock-skew claimed-at was treated as stale")
	}
}

func TestSQLiteOwnerOutboxProjectionAuditCannotPublishAfterTakeover(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	ownerA := OwnerMetadata{
		PID: 92002, Hostname: "audit-takeover-host", ExecutablePath: "/opt/cxp",
		InstanceID: "audit-owner-a", ScopeID: "audit-takeover-scope",
		MachineID: "audit-machine-a", LeaseGeneration: 1,
	}
	ownerB := ownerA
	ownerB.PID++
	ownerB.InstanceID = "audit-owner-b"
	ownerB.MachineID = "audit-machine-b"
	ownerB.LeaseGeneration = 2
	seedSQLiteOutboxAuditOwnerForTest(t, store, ownerA, sqliteOutboxAuditOwnershipRowsForTest)

	opened := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) {
		if stage != "opened" {
			return
		}
		once.Do(func() { close(opened) })
		<-release
	}
	t.Cleanup(func() {
		sqliteOutboxAuditTestHook = previousHook
		select {
		case <-release:
		default:
			close(release)
		}
	})

	firstDone := make(chan error, 1)
	go func() { firstDone <- store.RetryDeferredOutboxProjectionAuditForOwner(ctx, ownerA) }()
	select {
	case <-opened:
	case err := <-firstDone:
		t.Fatalf("old owner audit ended before opening: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("old owner audit did not open")
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		lease := ControlLease{
			ScopeID: ownerB.ScopeID, HolderMachineID: ownerB.MachineID,
			Generation: ownerB.LeaseGeneration, Status: ControlLeaseStatusActive,
			LeaseUntil: time.Now().Add(time.Hour), LastHeartbeat: time.Now(), UpdatedAt: time.Now(),
		}
		rawLease, err := json.Marshal(lease)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, rawLease, sqliteRuntimeKeyControlLease); err != nil {
			return err
		}
		rawOwner, err := json.Marshal(&ownerB)
		if err != nil {
			return err
		}
		for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
			if _, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, rawOwner, key); err != nil {
				return err
			}
		}
		return nil
	})
	close(release)
	if err := <-firstDone; !errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("old owner audit error = %v, want ErrControlLeaseNotHeld", err)
	}
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker == sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("old owner published trusted marker %s after takeover", key)
		}
	}

	sqliteOutboxAuditTestHook = nil
	if err := store.RetryDeferredOutboxProjectionAuditForOwner(ctx, ownerB); err != nil {
		t.Fatalf("new owner audit after takeover: %v", err)
	}
	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, store) {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("new owner marker %s = %q, want trusted", key, marker)
		}
	}
}

// The takeover must overlap the old audit's final transaction, not merely
// happen before a later retry. The durable claim token is what prevents the
// old owner from clearing or publishing over the replacement owner's claim.
func TestSQLiteOwnerOutboxProjectionAuditClaimTokenFencesTakeoverOverlap(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	ownerA := OwnerMetadata{
		PID: 92004, Hostname: "audit-overlap-host", ExecutablePath: "/opt/cxp",
		InstanceID: "audit-overlap-a", ScopeID: "audit-overlap-scope",
		MachineID: "audit-overlap-machine-a", LeaseGeneration: 1,
	}
	ownerB := ownerA
	ownerB.PID++
	ownerB.InstanceID = "audit-overlap-b"
	ownerB.MachineID = "audit-overlap-machine-b"
	ownerB.LeaseGeneration = 2
	seedSQLiteOutboxAuditOwnerForTest(t, store, ownerA, sqliteOutboxAuditOwnershipRowsForTest)
	peer, err := Open(store.Path())
	if err != nil {
		t.Fatalf("open replacement owner store: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })

	oldOwnerAtFinal := make(chan struct{})
	releaseOldOwner := make(chan struct{})
	var hookMu sync.Mutex
	beforeFinishCount := 0
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) {
		if stage != "before-finish" {
			return
		}
		hookMu.Lock()
		beforeFinishCount++
		count := beforeFinishCount
		hookMu.Unlock()
		if count == 1 {
			close(oldOwnerAtFinal)
			<-releaseOldOwner
		}
	}
	t.Cleanup(func() {
		sqliteOutboxAuditTestHook = previousHook
		select {
		case <-releaseOldOwner:
		default:
			close(releaseOldOwner)
		}
	})

	oldDone := make(chan error, 1)
	go func() { oldDone <- store.RetryDeferredOutboxProjectionAuditForOwner(ctx, ownerA) }()
	select {
	case <-oldOwnerAtFinal:
	case err := <-oldDone:
		t.Fatalf("old owner audit ended before final fence: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("old owner audit did not reach final fence")
	}

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		lease := ControlLease{
			ScopeID: ownerB.ScopeID, HolderMachineID: ownerB.MachineID,
			Generation: ownerB.LeaseGeneration, Status: ControlLeaseStatusActive,
			LeaseUntil: time.Now().Add(time.Hour), LastHeartbeat: time.Now(), UpdatedAt: time.Now(),
		}
		rawLease, err := json.Marshal(lease)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, rawLease, sqliteRuntimeKeyControlLease); err != nil {
			return err
		}
		rawOwner, err := json.Marshal(&ownerB)
		if err != nil {
			return err
		}
		for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
			if _, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, rawOwner, key); err != nil {
				return err
			}
		}
		return nil
	})

	newDone := make(chan error, 1)
	go func() { newDone <- peer.RetryDeferredOutboxProjectionAuditForOwner(ctx, ownerB) }()
	select {
	case err := <-newDone:
		if err != nil {
			t.Fatalf("replacement owner audit: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("replacement owner audit did not finish while old owner was fenced")
	}
	close(releaseOldOwner)
	if err := <-oldDone; !errors.Is(err, ErrControlLeaseNotHeld) && !errors.Is(err, ErrSQLiteOutboxProjectionAuditInProgress) {
		t.Fatalf("old owner final error = %v, want owner/claim-token fence", err)
	}
	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, store) {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("overlap marker %s = %q, want trusted", key, marker)
		}
	}
	claimPresent := false
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		var err error
		_, claimPresent, _, err = loadSQLiteOutboxProjectionAuditClaim(ctx, tx)
		return err
	})
	if claimPresent {
		t.Fatal("replacement owner claim was not cleared after successful publication")
	}
}

func TestSQLiteOwnerOutboxProjectionAuditCancellationLeavesAuditing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newTestStore(t)
	owner := OwnerMetadata{
		PID: 92003, Hostname: "audit-cancel-host", ExecutablePath: "/opt/cxp",
		InstanceID: "audit-cancel-instance", ScopeID: "audit-cancel-scope",
		MachineID: "audit-cancel-machine", LeaseGeneration: 1,
	}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, sqliteOutboxAuditOwnershipRowsForTest)
	var once sync.Once
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) {
		if stage == "row" {
			once.Do(cancel)
		}
	}
	t.Cleanup(func() { sqliteOutboxAuditTestHook = previousHook })

	err := store.RetryDeferredOutboxProjectionAuditForOwner(ctx, owner)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled owner audit error = %v, want context.Canceled", err)
	}
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustAuditing {
			t.Fatalf("canceled audit marker %s = %q, want auditing; all=%#v", key, marker, markers)
		}
	}

	// A new lease generation can reclaim the durable auditing claim and finish
	// the exact same local audit without resetting through unknown.
	owner.LeaseGeneration = 2
	owner.InstanceID = "audit-cancel-replacement"
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		lease := ControlLease{
			ScopeID: owner.ScopeID, HolderMachineID: owner.MachineID,
			Generation: owner.LeaseGeneration, Status: ControlLeaseStatusActive,
			LeaseUntil: time.Now().Add(time.Hour), LastHeartbeat: time.Now(), UpdatedAt: time.Now(),
		}
		rawLease, err := json.Marshal(lease)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(context.Background(), `UPDATE runtime_state SET json = ? WHERE key = ?`, rawLease, sqliteRuntimeKeyControlLease); err != nil {
			return err
		}
		rawOwner, err := json.Marshal(&owner)
		if err != nil {
			return err
		}
		for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
			if _, err := tx.ExecContext(context.Background(), `UPDATE runtime_state SET json = ? WHERE key = ?`, rawOwner, key); err != nil {
				return err
			}
		}
		return nil
	})
	sqliteOutboxAuditTestHook = nil
	if err := store.RetryDeferredOutboxProjectionAuditForOwner(context.Background(), owner); err != nil {
		t.Fatalf("replacement owner audit: %v", err)
	}
	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, store) {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("replacement marker %s = %q, want trusted", key, marker)
		}
	}
}

func TestSQLiteOutboxProjectionAuditRejectsNonTextNativeKeys(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		set  string
	}{
		{name: "id", set: `id = CAST(id AS BLOB)`},
		{name: "chat", set: `teams_chat_id = CAST(teams_chat_id AS BLOB)`},
		{name: "status", set: `status = CAST(status AS BLOB)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			owner := OwnerMetadata{ScopeID: "audit-type-scope", MachineID: "audit-type-machine", LeaseGeneration: 1}
			seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET `+tc.set); err != nil {
					return err
				}
				for _, key := range []string{sqliteOutboxProjectionTrustKey, sqliteOutboxSessionProjectionTrustKey, sqliteOutboxTurnProjectionTrustKey} {
					if err := sqliteWriteMetaValueContext(ctx, tx, key, sqliteOutboxProjectionTrustUnknown); err != nil {
						return err
					}
				}
				return nil
			})
			if err := store.PrepareOutboxProjectionForOwner(ctx, owner); err != nil {
				t.Fatalf("prepare type-corrupt projection: %v", err)
			}
			if marker := readOutboxProjectionTrustMarkersForTest(t, store)[sqliteOutboxProjectionTrustKey]; marker != sqliteOutboxProjectionTrustUntrusted {
				t.Fatalf("type-corrupt %s marker = %q, want untrusted", tc.name, marker)
			}
		})
	}
}

func TestSQLiteOutboxProjectionAuditRejectsDuplicateCanonicalKeys(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-duplicate-scope", MachineID: "audit-duplicate-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	const outboxID = "outbox:audit-owner:a:000000.000000000"
	duplicate := fmt.Sprintf(`{"id":%q,"teams_chat_id":"chat:audit-owner","status":"queued","status":"sent","sequence":1,"created_at":"2026-09-13T00:00:00Z","body":"duplicate"}`, outboxID)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`, duplicate, outboxID); err != nil {
			return err
		}
		for _, key := range []string{sqliteOutboxProjectionTrustKey, sqliteOutboxSessionProjectionTrustKey, sqliteOutboxTurnProjectionTrustKey} {
			if err := sqliteWriteMetaValueContext(ctx, tx, key, sqliteOutboxProjectionTrustUnknown); err != nil {
				return err
			}
		}
		return nil
	})
	if err := store.PrepareOutboxProjectionForOwner(ctx, owner); err != nil {
		t.Fatalf("prepare duplicate-key projection: %v", err)
	}
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustUntrusted {
			t.Fatalf("duplicate-key marker %s = %q, want untrusted; all=%#v", key, marker, markers)
		}
	}
}

func TestSQLiteOutboxProjectionAuditRejectsNestedDuplicateCanonicalKeys(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-nested-duplicate-scope", MachineID: "audit-nested-duplicate-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	const outboxID = "outbox:audit-owner:a:000000.000000000"
	row, err := store.OutboxMessageByID(ctx, outboxID)
	if err != nil {
		t.Fatalf("load nested-duplicate fixture row: %v", err)
	}
	raw := fmt.Sprintf(`{"id":%q,"teams_chat_id":"chat:audit-owner","status":"queued","sequence":1,"created_at":%q,"body":"nested duplicate","math_spans":[{"start":1,"start":2,"end":2,"index":0,"source":"fixture"}]}`,
		outboxID, row.CreatedAt.UTC().Format(time.RFC3339Nano))
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`, raw, outboxID); err != nil {
			return err
		}
		for _, key := range []string{sqliteOutboxProjectionTrustKey, sqliteOutboxSessionProjectionTrustKey, sqliteOutboxTurnProjectionTrustKey} {
			if err := sqliteWriteMetaValueContext(ctx, tx, key, sqliteOutboxProjectionTrustUnknown); err != nil {
				return err
			}
		}
		return nil
	})
	if err := store.PrepareOutboxProjectionForOwner(ctx, owner); err != nil {
		t.Fatalf("prepare nested duplicate-key projection: %v", err)
	}
	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, store) {
		if marker != sqliteOutboxProjectionTrustUntrusted {
			t.Fatalf("nested duplicate-key marker %s = %q, want untrusted", key, marker)
		}
	}
}

func TestSQLiteOutboxCanonicalFallbackRejectsPaddedDestinationProjection(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC)
	message := OutboxMessage{
		ID: "outbox:padded-destination", TeamsChatID: "chat:padded-destination",
		Kind: "helper", Body: "must remain quarantined", Status: OutboxStatusQueued,
		Sequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[message.ID] = message
		return nil
	}); err != nil {
		t.Fatalf("seed padded-destination row: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("prepare padded-destination projection: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET teams_chat_id = ? WHERE id = ?`, " "+message.TeamsChatID+" ", message.ID)
		return err
	})
	if marker := readOutboxProjectionTrustMarkersForTest(t, store)[sqliteOutboxProjectionTrustKey]; marker != sqliteOutboxProjectionTrustUntrusted {
		t.Fatalf("padded destination marker = %q, want untrusted", marker)
	}
	page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now.Add(time.Hour), Limit: 10})
	if err != nil {
		t.Fatalf("pending page with padded destination: %v", err)
	}
	if len(page.Messages) != 0 {
		t.Fatalf("padded destination entered canonical fallback page: %#v", page.Messages)
	}
}

func TestSQLiteNativeProjectionRevokesSideEffectCapabilityForMissingScalar(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 14, 13, 30, 0, 0, time.UTC)
	message := OutboxMessage{
		ID: "outbox:side-effect-missing-scalar", TeamsChatID: "chat:side-effect-missing-scalar",
		TeamsMessageID: "teams:side-effect-missing-scalar", Kind: "helper-final", Body: "pending effect",
		Status: OutboxStatusSent, PostSendEffectsPending: true, Sequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[message.ID] = message
		return nil
	}); err != nil {
		t.Fatalf("seed side-effect row: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("prepare side-effect projection: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET post_send_effects_pending = NULL WHERE id = ?`, message.ID)
		return err
	})
	if marker := readOutboxProjectionTrustMarkersForTest(t, store)[sqliteOutboxProjectionTrustKey]; marker != sqliteOutboxProjectionTrustUntrusted {
		t.Fatalf("missing side-effect scalar marker = %q, want untrusted", marker)
	}
	pending, err := store.PendingSentOutboxSideEffects(ctx, 10)
	if err != nil {
		t.Fatalf("pending side effects after scalar revocation: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != message.ID {
		t.Fatalf("pending side effects after scalar revocation = %#v, want canonical row", pending)
	}
}

func TestSQLiteContradictoryPostSendProjectionSurvivesUnrelatedRewrite(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 13, 45, 0, 0, time.UTC)
	for _, tc := range []struct {
		name           string
		canonicalValue string
		scalarValue    int64
		wantPending    bool
	}{
		{name: "canonical true scalar false", canonicalValue: "true", scalarValue: 0, wantPending: true},
		{name: "canonical false scalar true", canonicalValue: "false", scalarValue: 1, wantPending: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			message := OutboxMessage{
				ID:          "outbox:contradictory-post-send-" + strings.ReplaceAll(tc.name, " ", "-"),
				TeamsChatID: "chat:contradictory-post-send", TeamsMessageID: "teams:already-sent",
				Kind: "helper-final", Body: "must survive an unrelated rewrite", Status: OutboxStatusSent,
				PostSendEffectsPending: true, Sequence: 1, CreatedAt: now, UpdatedAt: now,
			}
			if err := store.Update(ctx, func(state *State) error {
				state.OutboxMessages[message.ID] = message
				return nil
			}); err != nil {
				t.Fatalf("seed contradictory projection: %v", err)
			}
			migrateStoreToSQLiteForTest(t, store)
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages
SET json = json_set(json, '$.post_send_effects_pending', json(?)), post_send_effects_pending = ?
WHERE id = ?`, tc.canonicalValue, tc.scalarValue, message.ID)
				return err
			})
			if marker := readOutboxProjectionTrustMarkersForTest(t, store)[sqliteOutboxProjectionTrustKey]; marker == sqliteOutboxProjectionTrustTrusted {
				t.Fatalf("contradictory projection marker remained trusted")
			}

			if err := store.Update(ctx, func(state *State) error {
				state.Scope.AccountID = "unrelated-state-change"
				return nil
			}); err != nil {
				t.Fatalf("unrelated state rewrite: %v", err)
			}

			var raw []byte
			var scalar int64
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				return tx.QueryRowContext(ctx, `SELECT json, post_send_effects_pending FROM outbox_messages WHERE id = ?`, message.ID).Scan(&raw, &scalar)
			})
			if !bytes.Contains(raw, []byte(`"post_send_effects_pending":`+tc.canonicalValue)) {
				t.Fatalf("canonical post-send value was not preserved: %s", raw)
			}
			if scalar != tc.scalarValue {
				t.Fatalf("post-send scalar after rewrite = %d, want %d", scalar, tc.scalarValue)
			}
			pending, err := store.PendingSentOutboxSideEffects(ctx, 10)
			if err != nil {
				t.Fatalf("read post-send side effects after rewrite: %v", err)
			}
			if tc.wantPending && (len(pending) != 1 || pending[0].ID != message.ID) {
				t.Fatalf("pending side effects after rewrite = %#v, want preserved row", pending)
			}
			if !tc.wantPending && len(pending) != 0 {
				t.Fatalf("false canonical pending bit returned side effects: %#v", pending)
			}
		})
	}
}

func TestSQLiteOutboxProjectionGuardRevokesTrustedMarkerAfterNestedDuplicateWrite(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-nested-post-trust-scope", MachineID: "audit-nested-post-trust-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	if err := store.RetryDeferredOutboxProjectionAuditForOwner(ctx, owner); err != nil {
		t.Fatalf("establish trusted projection: %v", err)
	}
	const outboxID = "outbox:audit-owner:a:000000.000000000"
	row, err := store.OutboxMessageByID(ctx, outboxID)
	if err != nil {
		t.Fatalf("load nested duplicate row: %v", err)
	}
	raw := fmt.Sprintf(`{"id":%q,"teams_chat_id":"chat:audit-owner","status":"queued","sequence":1,"created_at":%q,"body":"nested duplicate","math_spans":[{"start":1,"start":2,"end":2,"index":0,"source":"fixture"}]}`,
		outboxID, row.CreatedAt.UTC().Format(time.RFC3339Nano))
	rawDB, err := sql.Open("sqlite", filepath.Join(filepath.Dir(store.Path()), SQLiteFileName))
	if err != nil {
		t.Fatalf("open raw SQLite handle: %v", err)
	}
	t.Cleanup(func() { _ = rawDB.Close() })
	if _, err := rawDB.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`, raw, outboxID); err != nil {
		t.Fatalf("write nested duplicate row: %v", err)
	}
	if marker := readOutboxProjectionTrustMarkersForTest(t, store)[sqliteOutboxProjectionTrustKey]; marker != sqliteOutboxProjectionTrustUntrusted {
		t.Fatalf("nested duplicate post-trust marker = %q, want untrusted", marker)
	}
}

func TestSQLiteOutboxProjectionAuditRevokesTrustedMarkerAfterDuplicateWrite(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-post-trust-scope", MachineID: "audit-post-trust-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	if err := store.RetryDeferredOutboxProjectionAuditForOwner(ctx, owner); err != nil {
		t.Fatalf("establish trusted projection: %v", err)
	}
	const outboxID = "outbox:audit-owner:a:000000.000000000"
	db, err := sql.Open("sqlite", filepath.Join(filepath.Dir(store.Path()), SQLiteFileName))
	if err != nil {
		t.Fatalf("open raw SQLite handle: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	duplicate := fmt.Sprintf(`{"id":%q,"teams_chat_id":"chat:audit-owner","status":"queued","status":"sent","sequence":1,"created_at":"2026-09-13T00:00:00Z","body":"duplicate"}`, outboxID)
	if _, err := db.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`, duplicate, outboxID); err != nil {
		t.Fatalf("write duplicate canonical row: %v", err)
	}
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustUntrusted {
			t.Fatalf("post-trust duplicate marker %s = %q, want untrusted; all=%#v", key, marker, markers)
		}
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for _, key := range []string{
			sqliteOutboxProjectionProvenanceKey,
			sqliteOutboxSessionProjectionProvenanceKey,
			sqliteOutboxTurnProjectionProvenanceKey,
		} {
			var value string
			err := tx.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, key).Scan(&value)
			if !errors.Is(err, sql.ErrNoRows) {
				if err == nil {
					return fmt.Errorf("provenance %s survived trust revocation with value %q", key, value)
				}
				return err
			}
		}
		return nil
	})
}

func TestSQLiteOutboxProjectionAuditRejectsFullDecodeTypeMismatches(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		mutate func(*sql.Tx) error
	}{
		{
			name: "turn body",
			mutate: func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = json_set(json, '$.body', 17)`)
				return err
			},
		},
		{
			name: "send attempt token",
			mutate: func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = json_set(json, '$.send_attempt_token', 17)`)
				return err
			},
		},
		{
			name: "upload session post state",
			mutate: func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = json_set(json, '$.attachment_upload_session_post_state', 17)`)
				return err
			},
		},
		{
			name: "invalid upload expiry",
			mutate: func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = json_set(json, '$.attachment_upload_expiry', 'not-a-time')`)
				return err
			},
		},
		{
			name: "invalid math span field",
			mutate: func(tx *sql.Tx) error {
				const spans = `[{"start":"bad","end":2,"index":0,"source":"fixture"}]`
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = json_set(json, '$.math_spans', json(?))`, spans)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			owner := OwnerMetadata{ScopeID: "audit-decode-scope", MachineID: "audit-decode-machine", LeaseGeneration: 1}
			seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
			withSQLiteTxForTest(t, store, tc.mutate)
			if err := store.PrepareOutboxProjectionForOwner(ctx, owner); err != nil {
				t.Fatalf("prepare type-corrupt projection: %v", err)
			}
			markers := readOutboxProjectionTrustMarkersForTest(t, store)
			for key, marker := range markers {
				if marker != sqliteOutboxProjectionTrustUntrusted {
					t.Fatalf("%s marker %s = %q, want untrusted; all=%#v", tc.name, key, marker, markers)
				}
			}
		})
	}
}

func TestSQLiteTrustedOutboxProjectionRequiresCurrentProvenance(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-provenance-scope", MachineID: "audit-provenance-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	if err := store.RetryDeferredOutboxProjectionAuditForOwner(ctx, owner); err != nil {
		t.Fatalf("establish trusted projection: %v", err)
	}

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, `{"version":"1","database_identity":"stale","physical_revision":"stale","generation":1}`, sqliteOutboxProjectionProvenanceKey); err != nil {
			return err
		}
		return nil
	})
	var nativeReady bool
	err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		nativeReady, err = store.sqliteOutboxProjectionNativeMarkerReady(ctx, db, sqliteOutboxProjectionTrustKey)
		return err
	})
	if err != nil {
		t.Fatalf("check stale provenance: %v", err)
	}
	if nativeReady {
		t.Fatal("stale provenance incorrectly enabled native projection")
	}

	var opened bool
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) {
		if stage == "opened" {
			opened = true
		}
	}
	t.Cleanup(func() { sqliteOutboxAuditTestHook = previousHook })
	if err := store.PrepareOutboxProjectionForOwner(ctx, owner); err != nil {
		t.Fatalf("repair stale provenance: %v", err)
	}
	if !opened {
		t.Fatal("stale trusted marker was repaired without a fresh audit")
	}
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	if markers[sqliteOutboxProjectionTrustKey] != sqliteOutboxProjectionTrustTrusted {
		t.Fatalf("repaired FIFO marker = %q, want trusted", markers[sqliteOutboxProjectionTrustKey])
	}
}

// A trusted marker and an unchanged outbox generation are not sufficient if a
// raw writer removes one of the invalidation triggers.  Bind provenance to the
// SQLite schema cookie so DROP/CREATE of helper-owned DDL fails closed before a
// later contradictory row can be admitted through the scalar lane.
func TestSQLiteTrustedOutboxProjectionRequiresCurrentTriggerSchema(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-trigger-cookie-scope", MachineID: "audit-trigger-cookie-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	if err := store.RetryDeferredOutboxProjectionAuditForOwner(ctx, owner); err != nil {
		t.Fatalf("establish trusted projection: %v", err)
	}
	if got := sqliteMetaValueForTest(t, store, sqliteOutboxProjectionTrustKey); got != sqliteOutboxProjectionTrustTrusted {
		t.Fatalf("initial outbox projection marker = %q, want trusted", got)
	}

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DROP TRIGGER outbox_projection_guard_update`)
		return err
	})
	if got := sqliteMetaValueForTest(t, store, sqliteOutboxProjectionTrustKey); got != sqliteOutboxProjectionTrustTrusted {
		t.Fatalf("trigger removal unexpectedly changed durable marker to %q", got)
	}

	var nativeReady bool
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		nativeReady, err = store.sqliteOutboxProjectionNativeMarkerReady(ctx, db, sqliteOutboxProjectionTrustKey)
		return err
	}); err != nil {
		t.Fatalf("check trigger-cookie provenance: %v", err)
	}
	if nativeReady {
		t.Fatal("native projection remained trusted after helper trigger removal")
	}

	if err := store.PrepareOutboxProjectionForOwner(ctx, owner); err != nil {
		t.Fatalf("repair removed trigger and re-audit projection: %v", err)
	}
	if got := sqliteMetaValueForTest(t, store, sqliteOutboxProjectionTrustKey); got != sqliteOutboxProjectionTrustTrusted {
		t.Fatalf("repaired outbox projection marker = %q, want trusted", got)
	}
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		nativeReady, err = store.sqliteOutboxProjectionNativeMarkerReady(ctx, db, sqliteOutboxProjectionTrustKey)
		return err
	}); err != nil {
		t.Fatalf("check repaired trigger-cookie provenance: %v", err)
	}
	if !nativeReady {
		t.Fatal("native projection stayed disabled after explicit trigger repair and audit")
	}
}

// A helper-owned schema preparation may advance SQLite's schema cookie while
// leaving the outbox rows and guard triggers unchanged.  The preparation
// boundary must refresh the trusted provenance instead of making every later
// FIFO lookup fall back to the full JSON oracle.
func TestSQLiteSchemaPreparationRefreshesTrustedOutboxProvenanceCookie(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-schema-refresh-scope", MachineID: "audit-schema-refresh-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	if err := store.RetryDeferredOutboxProjectionAuditForOwner(ctx, owner); err != nil {
		t.Fatalf("establish trusted projection: %v", err)
	}

	beforeRaw := sqliteMetaValueForTest(t, store, sqliteOutboxProjectionProvenanceKey)
	var before sqliteOutboxProjectionTrustProvenance
	if err := json.Unmarshal([]byte(beforeRaw), &before); err != nil {
		t.Fatalf("decode initial outbox provenance: %v", err)
	}
	if before.SchemaVersion == nil {
		t.Fatal("initial outbox provenance has no schema cookie")
	}

	// Model a harmless helper-owned/index-only schema upgrade.  It changes the
	// SQLite cookie but not the canonical outbox data or the projection contract.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE INDEX outbox_schema_cookie_regression_idx ON outbox_messages(created_at)`)
		return err
	})

	var schemaVersion int64
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&schemaVersion)
	}); err != nil {
		t.Fatalf("read upgraded schema cookie: %v", err)
	}
	if schemaVersion == *before.SchemaVersion {
		t.Fatalf("schema cookie did not advance: before=%d after=%d", *before.SchemaVersion, schemaVersion)
	}

	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return ensureSQLiteSchemaContext(ctx, db)
	}); err != nil {
		t.Fatalf("complete helper-owned schema preparation: %v", err)
	}

	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, store) {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("schema preparation changed trusted marker %s to %q", key, marker)
		}
	}
	afterRaw := sqliteMetaValueForTest(t, store, sqliteOutboxProjectionProvenanceKey)
	var after sqliteOutboxProjectionTrustProvenance
	if err := json.Unmarshal([]byte(afterRaw), &after); err != nil {
		t.Fatalf("decode refreshed outbox provenance: %v", err)
	}
	if after.SchemaVersion == nil || *after.SchemaVersion != schemaVersion {
		t.Fatalf("refreshed outbox schema cookie = %v, want %d", after.SchemaVersion, schemaVersion)
	}

	var nativeReady bool
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		nativeReady, err = store.sqliteOutboxProjectionNativeMarkerReady(ctx, db, sqliteOutboxProjectionTrustKey)
		return err
	}); err != nil {
		t.Fatalf("check refreshed outbox capability: %v", err)
	}
	if !nativeReady {
		t.Fatal("trusted outbox projection remained disabled after schema preparation")
	}
}

// A schema cookie change that did not pass through helper-owned preparation
// must not be silently blessed.  Explicit outbox preparation must revoke the
// stale trusted marker and perform a fresh bounded audit, preventing the
// marker/fallback mismatch from becoming permanent.
func TestSQLiteTrustedOutboxProjectionSchemaMismatchIsReauditedAtPreparation(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-schema-reaudit-scope", MachineID: "audit-schema-reaudit-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	if err := store.RetryDeferredOutboxProjectionAuditForOwner(ctx, owner); err != nil {
		t.Fatalf("establish trusted projection: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE INDEX outbox_schema_cookie_opaque_regression_idx ON outbox_messages(sequence)`)
		return err
	})

	var audited bool
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) {
		if stage == "opened" {
			audited = true
		}
	}
	t.Cleanup(func() { sqliteOutboxAuditTestHook = previousHook })
	if err := store.PrepareOutboxProjectionForOwner(ctx, owner); err != nil {
		t.Fatalf("reaudit schema-mismatched projection: %v", err)
	}
	if !audited {
		t.Fatal("schema-mismatched trusted marker was accepted without a fresh audit")
	}

	var nativeReady bool
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		nativeReady, err = store.sqliteOutboxProjectionNativeMarkerReady(ctx, db, sqliteOutboxProjectionTrustKey)
		return err
	}); err != nil {
		t.Fatalf("check reaudited outbox capability: %v", err)
	}
	if !nativeReady {
		t.Fatal("reaudited outbox projection did not return to native lane")
	}
}

func TestSQLiteTrustedOutboxProjectionSurvivesValidOutboxWrite(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-valid-write-scope", MachineID: "audit-valid-write-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	if err := store.RetryDeferredOutboxProjectionAuditForOwner(ctx, owner); err != nil {
		t.Fatalf("establish trusted projection: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = json_set(json, '$.body', 'updated-valid-body')`)
		return err
	})
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("valid outbox write revoked marker %s to %q", key, marker)
		}
	}
	var nativeReady bool
	err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		nativeReady, err = store.sqliteOutboxProjectionNativeMarkerReady(ctx, db, sqliteOutboxProjectionTrustKey)
		return err
	})
	if err != nil {
		t.Fatalf("check native readiness after valid write: %v", err)
	}
	if !nativeReady {
		t.Fatal("valid outbox write forced native projection back to JSON fallback")
	}
}

func TestSQLiteUnownedOutboxProjectionAuditCannotPublishAfterLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	ownerA := OwnerMetadata{ScopeID: "audit-unowned-takeover-scope", MachineID: "audit-unowned-machine-a", LeaseGeneration: 1, InstanceID: "audit-unowned-a"}
	ownerB := ownerA
	ownerB.MachineID = "audit-unowned-machine-b"
	ownerB.InstanceID = "audit-unowned-b"
	ownerB.LeaseGeneration = 2
	seedSQLiteOutboxAuditOwnerForTest(t, store, ownerA, sqliteOutboxAuditOwnershipRowsForTest)
	peer, err := Open(store.Path())
	if err != nil {
		t.Fatalf("open peer store: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })

	// Release the seeded owner before entering the unowned API. The takeover
	// below deliberately occurs while the long read-only audit is paused.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, []byte(`{"scope_id":"audit-unowned-takeover-scope","generation":1}`), sqliteRuntimeKeyControlLease)
		if err != nil {
			return err
		}
		for _, key := range []string{
			sqliteOutboxProjectionTrustKey,
			sqliteOutboxSessionProjectionTrustKey,
			sqliteOutboxTurnProjectionTrustKey,
		} {
			if err := sqliteWriteMetaValueContext(ctx, tx, key, sqliteOutboxProjectionTrustUnknown); err != nil {
				return err
			}
		}
		return nil
	})

	opened := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) {
		if stage != "opened" {
			return
		}
		once.Do(func() { close(opened) })
		<-release
	}
	t.Cleanup(func() {
		sqliteOutboxAuditTestHook = previousHook
		select {
		case <-release:
		default:
			close(release)
		}
	})

	oldDone := make(chan error, 1)
	go func() { oldDone <- store.PrepareOutboxProjection(ctx) }()
	select {
	case <-opened:
	case err := <-oldDone:
		t.Fatalf("unowned audit ended before opening: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("unowned audit did not open")
	}
	withSQLiteTxForTest(t, peer, func(tx *sql.Tx) error {
		lease := ControlLease{
			ScopeID: ownerB.ScopeID, HolderMachineID: ownerB.MachineID,
			Generation: ownerB.LeaseGeneration, Status: ControlLeaseStatusActive,
			LeaseUntil: time.Now().Add(time.Hour), LastHeartbeat: time.Now(), UpdatedAt: time.Now(),
		}
		rawLease, err := json.Marshal(lease)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, rawLease, sqliteRuntimeKeyControlLease); err != nil {
			return err
		}
		rawOwner, err := json.Marshal(&ownerB)
		if err != nil {
			return err
		}
		for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
			if _, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, rawOwner, key); err != nil {
				return err
			}
		}
		return nil
	})
	close(release)
	if err := <-oldDone; !errors.Is(err, ErrSQLiteOutboxProjectionAuditOwnerRequired) {
		t.Fatalf("unowned audit error = %v, want owner-required fence", err)
	}
	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, store) {
		if marker == sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("unowned audit published trusted marker %s after takeover", key)
		}
	}

	sqliteOutboxAuditTestHook = nil
	if err := peer.RetryDeferredOutboxProjectionAuditForOwner(ctx, ownerB); err != nil {
		t.Fatalf("replacement owner audit: %v", err)
	}
	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, peer) {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("replacement marker %s = %q, want trusted", key, marker)
		}
	}
}

func TestSQLiteOwnerProjectionGuardRepairRequiresCurrentLease(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	ownerA := OwnerMetadata{
		ScopeID: "audit-guard-takeover-scope", MachineID: "audit-guard-machine-a",
		InstanceID: "audit-guard-a", LeaseGeneration: 1,
	}
	ownerB := ownerA
	ownerB.MachineID = "audit-guard-machine-b"
	ownerB.InstanceID = "audit-guard-b"
	ownerB.LeaseGeneration = 2
	seedSQLiteOutboxAuditOwnerForTest(t, store, ownerA, 1)

	// Make the helper-owned DDL incomplete, then install a replacement lease.
	// The stale owner must be rejected before it can recreate the trigger or
	// rewrite the fail-closed markers.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DROP TRIGGER outbox_projection_guard_insert`); err != nil {
			return err
		}
		lease := ControlLease{
			ScopeID: ownerB.ScopeID, HolderMachineID: ownerB.MachineID,
			Generation: ownerB.LeaseGeneration, Status: ControlLeaseStatusActive,
			LeaseUntil: time.Now().Add(time.Hour), LastHeartbeat: time.Now(), UpdatedAt: time.Now(),
		}
		rawLease, err := json.Marshal(lease)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, rawLease, sqliteRuntimeKeyControlLease); err != nil {
			return err
		}
		rawOwner, err := json.Marshal(&ownerB)
		if err != nil {
			return err
		}
		for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
			if _, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, rawOwner, key); err != nil {
				return err
			}
		}
		return nil
	})

	var staleErr error
	var triggerCount int
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return fmt.Errorf("SQLite pointer: ok=%t err=%v", ok, err)
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		staleErr = ensureSQLiteOutboxProjectionGuardContextForOwner(ctx, db, ownerA)
		return db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = 'outbox_projection_guard_insert'`).Scan(&triggerCount)
	}); err != nil {
		t.Fatalf("inspect stale owner guard repair: %v", err)
	}
	if !errors.Is(staleErr, ErrControlLeaseNotHeld) {
		t.Fatalf("stale owner guard repair error = %v, want ErrControlLeaseNotHeld", staleErr)
	}
	if triggerCount != 0 {
		t.Fatalf("stale owner recreated projection guard trigger, count=%d", triggerCount)
	}

	// The replacement owner can now perform the same repair under its lease.
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return fmt.Errorf("SQLite pointer: ok=%t err=%v", ok, err)
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return ensureSQLiteOutboxProjectionGuardContextForOwner(ctx, db, ownerB)
	}); err != nil {
		t.Fatalf("replacement owner guard repair: %v", err)
	}
	if markers := readOutboxProjectionTrustMarkersForTest(t, store); markers[sqliteOutboxProjectionTrustKey] != sqliteOutboxProjectionTrustUnknown {
		t.Fatalf("replacement owner guard repair marker = %q, want unknown pending audit", markers[sqliteOutboxProjectionTrustKey])
	}
}

func TestSQLiteProjectionAuditPublicationPermanentPathErrorIsBounded(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-publication-error-scope", MachineID: "audit-publication-error-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	snapshot := sqliteOutboxProjectionAuditSnapshot{dbPath: filepath.Join(t.TempDir(), "does-not-exist.sqlite"), claimID: "claim", databaseIdentity: "identity", physicalRevision: "revision", pendingMask: sqliteOutboxProjectionAuditPendingFIFO, projectionPending: true}
	started := time.Now()
	err := store.finishSQLiteOutboxProjectionAuditForOwnerWithRetry(ctx, owner, snapshot, true, false, false, false)
	if err == nil {
		t.Fatal("permanent path error unexpectedly succeeded")
	}
	if !errors.Is(err, ErrSQLiteOutboxProjectionAuditPermanent) {
		t.Fatalf("permanent path error = %v, want ErrSQLiteOutboxProjectionAuditPermanent", err)
	}
	if elapsed := time.Since(started); elapsed > sqliteProjectionAuditPermanentPathMaxDurationForTest() {
		t.Fatalf("permanent path error took %s; publication retry should stop immediately", elapsed)
	}
}

func TestSQLiteOutboxProjectionAuditResumesRecordedResultAfterPublicationFailure(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{
		ScopeID: "audit-resume-scope", MachineID: "audit-resume-machine",
		InstanceID: "audit-resume-instance", LeaseGeneration: 1,
	}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)

	snapshot, claimed, err := store.claimSQLiteOutboxProjectionAuditForOwner(ctx, owner)
	if err != nil || !claimed {
		t.Fatalf("claim audit result: snapshot=%#v claimed=%t err=%v", snapshot, claimed, err)
	}
	if err := store.recordSQLiteOutboxProjectionAuditResultForOwner(ctx, owner, snapshot, true, true, true, false); err != nil {
		t.Fatalf("record completed audit result: %v", err)
	}

	var opened bool
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) {
		if stage == "opened" {
			opened = true
		}
	}
	t.Cleanup(func() { sqliteOutboxAuditTestHook = previousHook })
	if err := store.RetryDeferredOutboxProjectionAuditForOwner(ctx, owner); err != nil {
		t.Fatalf("resume recorded audit result: %v", err)
	}
	if opened {
		t.Fatal("resumed completed audit unexpectedly rescanned the outbox")
	}
	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, store) {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("resumed marker %s = %q, want trusted", key, marker)
		}
	}
	claimPresent := false
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, claimPresent, _, err = loadSQLiteOutboxProjectionAuditClaim(ctx, tx)
		return err
	})
	if claimPresent {
		t.Fatal("resumed audit left a durable claim after publication")
	}
}

func TestSQLiteOutboxSessionAuditRejectsPaddedAbsentSessionScalar(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-session-padding-scope", MachineID: "audit-session-padding-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET session_id = ?`, " session-with-padding ")
		return err
	})
	if err := store.PrepareOutboxProjectionForOwner(ctx, owner); err != nil {
		t.Fatalf("prepare padded absent-session projection: %v", err)
	}
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	if markers[sqliteOutboxSessionProjectionTrustKey] != sqliteOutboxSessionProjectionTrustUntrusted {
		t.Fatalf("padded absent-session marker = %q, want untrusted; all=%#v", markers[sqliteOutboxSessionProjectionTrustKey], markers)
	}
}

func TestSQLiteOutboxSessionFallbackDoesNotFilterDuplicateCanonicalCandidate(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-session-duplicate-scope", MachineID: "audit-session-duplicate-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	const outboxID = "outbox:audit-owner:a:000000.000000000"
	duplicate := fmt.Sprintf(`{"id":%q,"session_id":"other-session","session_id":"target-session","teams_chat_id":"chat:audit-owner","status":"queued","sequence":1,"created_at":"2026-09-13T00:00:00Z","body":"duplicate"}`, outboxID)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ?, session_id = ? WHERE id = ?`, duplicate, "other-session", outboxID); err != nil {
			return err
		}
		if err := sqliteWriteMetaValueContext(ctx, tx, sqliteOutboxSessionProjectionTrustKey, sqliteOutboxSessionProjectionTrustUntrusted); err != nil {
			return err
		}
		return nil
	})
	var got map[string]OutboxMessage
	err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		got = make(map[string]OutboxMessage)
		return loadSQLiteOutboxSessionMapCanonical(ctx, tx, got, "target-session")
	})
	if !errors.Is(err, ErrSQLiteOutboxProjectionUntrusted) {
		t.Fatalf("duplicate canonical session fallback error = %v, want ErrSQLiteOutboxProjectionUntrusted", err)
	}
	if len(got) != 0 {
		t.Fatalf("duplicate canonical session fallback returned %#v instead of refusing an ambiguous row", got)
	}
}

func TestSQLiteOutboxSessionFallbackUsesByteBudgetAndPublishesAtomically(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-session-budget-scope", MachineID: "audit-session-budget-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	const outboxID = "outbox:audit-owner:a:000000.000000000"
	// Use multibyte UTF-8 so a rune-counting limit would incorrectly accept
	// this row. The SQL expression must enforce the limit in encoded bytes.
	payload := strings.Repeat("界", int(sqliteOutboxCanonicalLookupMaxJSONRowBytes)/3+1)
	oversized := `{"id":"` + outboxID + `","session_id":"target-session","teams_chat_id":"chat:audit-owner","status":"queued","sequence":1,"created_at":"2026-09-13T00:00:00Z","body":"` + payload + `"}`
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ?, session_id = ? WHERE id = ?`, oversized, "target-session", outboxID)
		return err
	})

	sentinel := OutboxMessage{ID: "sentinel", SessionID: "unrelated", Status: OutboxStatusSent}
	got := map[string]OutboxMessage{"sentinel": sentinel}
	err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		return loadSQLiteOutboxSessionMapCanonical(ctx, tx, got, "target-session")
	})
	if !errors.Is(err, ErrSQLiteOutboxProjectionUntrusted) {
		t.Fatalf("oversized canonical session fallback error = %v, want ErrSQLiteOutboxProjectionUntrusted", err)
	}
	if len(got) != 1 || got["sentinel"].ID != sentinel.ID || got["sentinel"].Status != sentinel.Status {
		t.Fatalf("oversized canonical session fallback partially published %#v", got)
	}
}

func TestSQLiteUnownedOutboxProjectionAuditCannotRunWithActiveLease(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-unowned-scope", MachineID: "audit-unowned-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	if err := store.PrepareOutboxProjection(ctx); !errors.Is(err, ErrSQLiteOutboxProjectionAuditOwnerRequired) {
		t.Fatalf("unowned prepare error = %v, want owner-required fence", err)
	}
	if err := store.RetryDeferredOutboxProjectionAudit(ctx); !errors.Is(err, ErrSQLiteOutboxProjectionAuditOwnerRequired) {
		t.Fatalf("unowned retry error = %v, want owner-required fence", err)
	}
	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, store) {
		if marker != sqliteOutboxProjectionTrustDeferred {
			t.Fatalf("unowned audit changed marker %s to %q, want deferred", key, marker)
		}
	}
}

// An unowned/offline preparation attempt must prove the absence of a live
// owner before sqliteDBUnlocked can run schema repair.  Dropping one helper
// trigger on a fresh Store makes the old ordering observable: the unsafe
// implementation recreated it before returning OwnerRequired.
func TestSQLiteUnownedProjectionPreparationDoesNotRepairSchemaWithActiveOwner(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-schema-fence-scope", MachineID: "audit-schema-fence-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DROP TRIGGER outbox_projection_guard_update`)
		return err
	})

	peer, err := Open(store.Path())
	if err != nil {
		t.Fatalf("open peer store: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	if err := peer.prepareOutboxProjectionSQLite(ctx, false); !errors.Is(err, ErrSQLiteOutboxProjectionAuditOwnerRequired) {
		t.Fatalf("unowned preparation error = %v, want owner-required fence", err)
	}

	db, err := openExistingSQLiteRuntimeStore(store.sqliteDBPath)
	if err != nil {
		t.Fatalf("open read-only trigger probe: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var triggerCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = ?`, "outbox_projection_guard_update").Scan(&triggerCount); err != nil {
		t.Fatalf("probe dropped trigger: %v", err)
	}
	if triggerCount != 0 {
		t.Fatalf("ownerless preparation recreated a helper trigger while owner was active")
	}
}

// Schema repair is allowed before ownership only while a durable preparation
// claim fences owner heartbeats.  This catches the check-then-ensure TOCTOU
// window: a heartbeat arriving after the no-owner check must be rejected until
// the DDL boundary has completed, rather than renewing a lease during an
// unowned schema mutation.
func TestSQLiteSchemaPreflightFencesOwnerHeartbeatDuringRepair(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "schema-preflight-scope", MachineID: "schema-preflight-machine", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	if _, err := store.ReleaseControlLeaseIfHolder(ctx, owner.MachineID, owner.LeaseGeneration); err != nil {
		t.Fatalf("release seeded lease: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DROP TRIGGER outbox_projection_guard_update`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey)
		return err
	})

	peer, err := Open(store.Path())
	if err != nil {
		t.Fatalf("open preflight peer: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	opened := make(chan struct{})
	release := make(chan struct{})
	previousHook := sqliteSchemaPreparationTestHook
	sqliteSchemaPreparationTestHook = func(stage string) {
		if stage != "opened" {
			return
		}
		select {
		case <-opened:
		default:
			close(opened)
		}
		<-release
	}
	t.Cleanup(func() {
		sqliteSchemaPreparationTestHook = previousHook
		select {
		case <-release:
		default:
			close(release)
		}
	})
	preflightDone := make(chan error, 1)
	go func() { preflightDone <- peer.PrepareSQLiteSchemaBeforeOwner(ctx) }()
	select {
	case <-opened:
	case err := <-preflightDone:
		t.Fatalf("schema preflight finished before maintenance boundary: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("schema preflight did not open maintenance handle")
	}
	heartbeatOwner := owner
	heartbeatOwner.InstanceID = "schema-preflight-heartbeat"
	heartbeatDone := make(chan error, 1)
	go func() {
		_, err := store.RecordOwnerHeartbeat(ctx, heartbeatOwner, time.Minute, time.Now().UTC())
		heartbeatDone <- err
	}()
	select {
	case err := <-heartbeatDone:
		if !errors.Is(err, ErrSQLiteSchemaPreparationInProgress) && !errors.Is(err, ErrSQLiteSchemaPreparationRequired) {
			t.Fatalf("heartbeat during schema preparation error = %v, want fail-closed preparation fence", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat remained blocked behind schema preparation")
	}
	close(release)
	if err := <-preflightDone; err != nil {
		t.Fatalf("schema preflight: %v", err)
	}

	dbPath := filepath.Join(filepath.Dir(store.Path()), SQLiteFileName)
	db, err := openExistingSQLiteRuntimeStore(dbPath)
	if err != nil {
		t.Fatalf("open trigger probe: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var triggerCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = ?`, "outbox_projection_guard_update").Scan(&triggerCount); err != nil {
		t.Fatalf("probe dropped trigger: %v", err)
	}
	if triggerCount != 1 {
		t.Fatalf("schema preflight trigger count = %d, want repaired trigger", triggerCount)
	}
	var marker string
	if err := db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey).Scan(&marker); err != nil {
		t.Fatalf("read schema preparation marker: %v", err)
	}
	if marker != sqliteSchemaPreparationVersion {
		t.Fatalf("schema preparation marker = %q, want %q", marker, sqliteSchemaPreparationVersion)
	}
}

// An already-DB migration invoked by a separate Store must not perform
// unowned DDL while another generation owns the control lease.  It may report
// that the explicit preparation boundary is required, but the helper-owned
// trigger must remain absent until the live owner is gone and a fenced startup
// repair is allowed to run.
func TestSQLiteMigrationDoesNotRunUnownedSchemaDDLWithActiveOwner(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedOfficialReleaseSQLiteStoreForTestWithOptions(t, store, "migration-owner-fence", officialReleaseSQLiteFixtureOptions{
		LegacyOutboxColumns: true,
	})
	if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
		t.Fatalf("prepare legacy schema before claim: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	owner := testOwner("migration-owner", "", now)
	decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope:   ScopeIdentity{ID: "migration-owner-scope", AccountID: "migration-owner-account", Profile: "default"},
		Machine: MachineRecord{ID: "migration-owner-machine", ScopeID: "migration-owner-scope", Status: MachineStatusActive},
		Owner:   owner, Duration: time.Minute, Now: now,
	})
	if err != nil || decision.Mode != LeaseModeActive {
		t.Fatalf("claim active owner: decision=%#v err=%v", decision, err)
	}
	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("read SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("resolve SQLite path: %v", err)
	}
	if _, err := store.withSQLiteRuntimeDB(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS outbox_projection_guard_update`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey); err != nil {
			return err
		}
		return tx.Commit()
	}); err != nil {
		t.Fatalf("remove schema marker and trigger: %v", err)
	}

	peer, err := Open(store.Path())
	if err != nil {
		t.Fatalf("open migration peer: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	if _, err := peer.Load(ctx); !errors.Is(err, ErrSQLiteSchemaPreparationRequired) {
		t.Fatalf("peer generic Load with active owner error = %v, want schema-preparation-required", err)
	}
	if err := peer.Update(ctx, func(state *State) error {
		state.ServiceControl.Reason = "must not write during owner schema fence"
		return nil
	}); !errors.Is(err, ErrSQLiteSchemaPreparationRequired) {
		t.Fatalf("peer generic Update with active owner error = %v, want schema-preparation-required", err)
	}
	if _, err := peer.MigrateLargeStateToSQLite(ctx, 0); !errors.Is(err, ErrSQLiteSchemaPreparationRequired) {
		t.Fatalf("peer migration with active owner error = %v, want schema-preparation-required", err)
	}

	db, err := openSQLiteHandle(dbPath, false)
	if err != nil {
		t.Fatalf("open trigger probe: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var triggerCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = ?`, "outbox_projection_guard_update").Scan(&triggerCount); err != nil {
		t.Fatalf("probe migration trigger: %v", err)
	}
	if triggerCount != 0 {
		t.Fatalf("peer migration repaired a trigger while another owner was active")
	}
}

// A setup-free SQLite foreground open must never repair an unready schema.  The
// public Load compatibility bridge is allowed to prepare a valid inherited DB,
// but only through the fenced preparation claim; keep both contracts visible in
// one regression so a future shortcut cannot reintroduce lazy DDL.
func TestSQLiteLazyOpenIsSetupFreeAndLoadUsesFencedPreparation(t *testing.T) {
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
		if _, err := tx.ExecContext(ctx, `DROP TABLE runtime_state`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey)
		return err
	})

	if _, err := openExistingSQLitePreparedStore(dbPath); !errors.Is(err, ErrSQLiteSchemaPreparationRequired) {
		t.Fatalf("setup-free foreground open error = %v, want schema-preparation-required", err)
	}
	// The setup-free probe above must not manufacture the missing projection.
	probe, err := openSQLiteHandle(dbPath, false)
	if err != nil {
		t.Fatalf("open schema probe: %v", err)
	}
	t.Cleanup(func() { _ = probe.Close() })
	var tableCount int
	if err := probe.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'runtime_state'`).Scan(&tableCount); err != nil {
		t.Fatalf("probe runtime_state before fenced Load: %v", err)
	}
	if tableCount != 0 {
		t.Fatalf("setup-free foreground open repaired runtime_state table")
	}
	_ = probe.Close()

	peer, err := Open(store.Path())
	if err != nil {
		t.Fatalf("open lazy-read peer: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	if _, err := peer.Load(ctx); err != nil {
		t.Fatalf("fenced compatibility Load error = %v", err)
	}
	probe, err = openSQLiteHandle(dbPath, false)
	if err != nil {
		t.Fatalf("open repaired schema probe: %v", err)
	}
	t.Cleanup(func() { _ = probe.Close() })
	if err := probe.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'runtime_state'`).Scan(&tableCount); err != nil {
		t.Fatalf("probe repaired runtime_state: %v", err)
	}
	if tableCount != 1 {
		t.Fatalf("fenced compatibility Load runtime_state table count = %d, want 1", tableCount)
	}
}

// The setup-free lazy path must refuse the damaged file, while the explicit
// fenced preparation path must still be able to recreate a missing runtime
// table and publish readiness for the next owner.
func TestSQLiteSchemaPreparationRepairsMissingRuntimeState(t *testing.T) {
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
		if _, err := tx.ExecContext(ctx, `DROP TABLE runtime_state`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey)
		return err
	})

	if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
		t.Fatalf("repair missing runtime_state schema: %v", err)
	}
	db, err := openExistingSQLiteRuntimeStore(dbPath)
	if err != nil {
		t.Fatalf("open repaired runtime store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var marker string
	if err := db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey).Scan(&marker); err != nil {
		t.Fatalf("read repaired schema marker: %v", err)
	}
	if marker != sqliteSchemaPreparationVersion {
		t.Fatalf("repaired schema marker = %q, want %q", marker, sqliteSchemaPreparationVersion)
	}
	var runtimeRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_state`).Scan(&runtimeRows); err != nil {
		t.Fatalf("count repaired runtime rows: %v", err)
	}
	if runtimeRows < len(sqliteRuntimeRequiredKeys) {
		t.Fatalf("repaired runtime rows = %d, want at least %d", runtimeRows, len(sqliteRuntimeRequiredKeys))
	}
}

// The public lease API must enforce the same schema gate as the listener's
// preflight.  Otherwise an alternate caller could acquire ownership and then
// let its first owner-scoped operation perform partial DDL.
func TestSQLiteClaimControlLeaseRequiresPreparedSchema(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey)
		return err
	})
	now := time.Now().UTC().Truncate(time.Microsecond)
	scope := ScopeIdentity{ID: "claim-schema-gate-scope", AccountID: "claim-schema-gate-account", Profile: "default"}
	_, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope:    scope,
		Machine:  MachineRecord{ID: "claim-schema-gate-machine", ScopeID: scope.ID, Status: MachineStatusActive},
		Duration: time.Minute,
		Now:      now,
	})
	if !errors.Is(err, ErrSQLiteSchemaPreparationRequired) {
		t.Fatalf("ClaimControlLease error = %v, want schema-preparation-required", err)
	}
}

// A process can die after schema DDL publishes the ready marker but before it
// clears the preparation claim.  Startup must treat that tuple as completed
// preparation and remove only the exact leftover claim; otherwise the next
// owner can be rejected by the stale heartbeat fence for the claim TTL.
func TestSQLiteSchemaPreflightRecoversClaimAfterMarkerPublication(t *testing.T) {
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
	claim := sqliteSchemaPreparationClaim{
		ClaimID:   "schema-claim-crash-recovery",
		DBPath:    dbPath,
		ClaimedAt: time.Now().UTC(),
	}
	rawClaim, err := json.Marshal(claim)
	if err != nil {
		t.Fatalf("marshal preparation claim: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, sqliteSchemaPreparationVersionKey, sqliteSchemaPreparationVersion); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, sqliteSchemaPreparationClaimKey, string(rawClaim))
		return err
	})
	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close pre-crash store: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen after published schema marker: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
		t.Fatalf("recover published schema claim: %v", err)
	}
	withSQLiteTxForTest(t, reopened, func(tx *sql.Tx) error {
		if _, present, valid, err := loadSQLiteSchemaPreparationClaim(ctx, tx); err != nil {
			return err
		} else if present || valid {
			return fmt.Errorf("published schema claim remains: present=%v valid=%v", present, valid)
		}
		return nil
	})

	now := time.Now().UTC().Truncate(time.Microsecond)
	owner := testOwner("schema-claim-recovery-owner", "", now)
	scope := ScopeIdentity{ID: "schema-claim-recovery-scope", AccountID: "schema-claim-recovery-account", Profile: "default"}
	decision, err := reopened.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope:   scope,
		Machine: MachineRecord{ID: "schema-claim-recovery-machine", ScopeID: scope.ID, Status: MachineStatusActive},
		Owner:   owner, Duration: time.Minute, Now: now,
	})
	if err != nil || decision.Mode != LeaseModeActive {
		t.Fatalf("claim owner after schema recovery: decision=%#v err=%v", decision, err)
	}
	owner.ScopeID = scope.ID
	owner.MachineID = "schema-claim-recovery-machine"
	owner.LeaseGeneration = decision.Lease.Generation
	if _, err := reopened.RecordOwnerHeartbeatForLease(ctx, owner, time.Minute, time.Minute, now.Add(time.Second)); err != nil {
		t.Fatalf("heartbeat after schema claim recovery: %v", err)
	}
}

// CREATE TRIGGER IF NOT EXISTS does not upgrade an existing trigger body.
// Schema preparation must replace stale state_json revision triggers before
// publishing/using the ready marker, otherwise cold-state revisions can stop
// advancing after an upgrade.
func TestSQLiteSchemaPreparationReplacesStaleStateJSONRevisionTriggers(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for _, name := range []string{"state_json_revision_insert", "state_json_revision_update"} {
			if _, err := tx.ExecContext(ctx, `DROP TRIGGER `+name); err != nil {
				return err
			}
		}
		stale := []string{
			`CREATE TRIGGER state_json_revision_insert AFTER INSERT ON state_meta WHEN NEW.key = 'state_json' BEGIN SELECT 1; END`,
			`CREATE TRIGGER state_json_revision_update AFTER UPDATE OF value ON state_meta WHEN NEW.key = 'state_json' BEGIN SELECT 1; END`,
		}
		for _, definition := range stale {
			if _, err := tx.ExecContext(ctx, definition); err != nil {
				return err
			}
		}
		return nil
	})
	if current, err := sqliteStateJSONRevisionTriggersCurrentForTest(store); err != nil {
		t.Fatalf("inspect stale state_json triggers: %v", err)
	} else if current {
		t.Fatal("stale state_json triggers unexpectedly matched current definitions")
	}

	if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
		t.Fatalf("repair stale state_json triggers: %v", err)
	}
	if current, err := sqliteStateJSONRevisionTriggersCurrentForTest(store); err != nil {
		t.Fatalf("inspect repaired state_json triggers: %v", err)
	} else if !current {
		t.Fatal("schema preparation did not install current state_json triggers")
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		var before string
		if err := tx.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteStateJSONRevisionKey).Scan(&before); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = value || 'x' WHERE key = 'state_json'`); err != nil {
			return err
		}
		var after string
		if err := tx.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteStateJSONRevisionKey).Scan(&after); err != nil {
			return err
		}
		var beforeRevision, afterRevision int64
		if _, err := fmt.Sscan(before, &beforeRevision); err != nil {
			return fmt.Errorf("parse state_json revision before update %q: %w", before, err)
		}
		if _, err := fmt.Sscan(after, &afterRevision); err != nil {
			return fmt.Errorf("parse state_json revision after update %q: %w", after, err)
		}
		if afterRevision != beforeRevision+1 {
			return fmt.Errorf("state_json revision after update = %d, want %d", afterRevision, beforeRevision+1)
		}
		return nil
	})
}

func sqliteStateJSONRevisionTriggersCurrentForTest(store *Store) (bool, error) {
	ctx := context.Background()
	var current bool
	err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		rows, err := db.QueryContext(ctx, `SELECT name, COALESCE(sql, '') FROM sqlite_master
WHERE type = 'trigger' AND name IN (?, ?)
ORDER BY name`, "state_json_revision_insert", "state_json_revision_update")
		if err != nil {
			return err
		}
		seen := map[string]string{}
		for rows.Next() {
			var name, definition string
			if err := rows.Scan(&name, &definition); err != nil {
				_ = rows.Close()
				return err
			}
			seen[name] = definition
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		want := []string{
			`CREATE TRIGGER state_json_revision_insert
AFTER INSERT ON state_meta
WHEN NEW.key = 'state_json'
BEGIN
  INSERT INTO state_meta(key, value) VALUES ('state_json_revision', '1')
  ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(COALESCE(value, '0') AS INTEGER) + 1 AS TEXT);
  DELETE FROM state_meta WHERE key IN ('chat_sequence_projection_version', 'chat_sequence_projection_version_backfill_cursor');
END`,
			`CREATE TRIGGER state_json_revision_update
AFTER UPDATE OF value ON state_meta
WHEN NEW.key = 'state_json'
BEGIN
  INSERT INTO state_meta(key, value) VALUES ('state_json_revision', '1')
  ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(COALESCE(value, '0') AS INTEGER) + 1 AS TEXT);
  DELETE FROM state_meta WHERE key IN ('chat_sequence_projection_version', 'chat_sequence_projection_version_backfill_cursor');
END`,
		}
		current = len(seen) == len(want)
		if current {
			for _, definition := range want {
				name := strings.Fields(definition)[2]
				if normalizeSQLiteDDL(seen[name]) != normalizeSQLiteDDL(definition) {
					current = false
					break
				}
			}
		}
		return nil
	})
	return current, err
}

// A table can have the expected name while still being too narrow for the
// current split-state schema.  Preparation must fail closed and remove a
// previous ready marker rather than claiming that ALTER/CREATE IF NOT EXISTS
// repaired a table it could not actually change.
func TestSQLiteSchemaPreparationRejectsIncompatibleExistingSchema(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	if err := store.Update(ctx, func(state *State) error {
		state.Scope.ID = "incompatible-schema-scope"
		return nil
	}); err != nil {
		t.Fatalf("seed incompatible-schema source: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DROP TABLE sessions`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `CREATE TABLE sessions (id INTEGER PRIMARY KEY, json BLOB NOT NULL)`)
		return err
	})
	if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err == nil {
		t.Fatal("incompatible sessions schema unexpectedly prepared successfully")
	}
	// Preparation deliberately revokes readiness before returning the schema
	// incompatibility.  Use the setup-free forensic reader here; ordinary
	// Store reads must remain fenced and cannot be used to inspect the marker
	// after that failure.
	var marker string
	err := withSQLiteUnpreparedRawQueryForTest(store, `SELECT value FROM state_meta WHERE key = ?`, &marker, sqliteSchemaPreparationVersionKey)
	if !errors.Is(err, sql.ErrNoRows) {
		if err != nil {
			t.Fatalf("inspect revoked schema marker: %v", err)
		}
		t.Fatalf("incompatible schema retained ready marker %q", marker)
	}
}

// runtime_state is part of the required schema, not an optional optimization
// table. A same-named table with an incompatible key type must not be accepted
// merely because CREATE TABLE IF NOT EXISTS succeeds; otherwise lease reads
// could use an untrusted ownership projection after startup.
func TestSQLiteSchemaPreparationRejectsIncompatibleRuntimeStateSchema(t *testing.T) {
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
		if _, err := tx.ExecContext(ctx, `DROP TABLE runtime_state`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `CREATE TABLE runtime_state (key INTEGER PRIMARY KEY, json BLOB NOT NULL)`)
		return err
	})
	probe, err := openSQLiteHandle(dbPath, false)
	if err != nil {
		t.Fatalf("open incompatible runtime probe: %v", err)
	}
	var runtimeType string
	if err := probe.QueryRowContext(ctx, `SELECT type FROM pragma_table_info('runtime_state') WHERE name = 'key'`).Scan(&runtimeType); err != nil {
		_ = probe.Close()
		t.Fatalf("inspect incompatible runtime key: %v", err)
	}
	if runtimeType != "INTEGER" {
		t.Fatalf("incompatible runtime key type = %q, want INTEGER", runtimeType)
	}
	if err := validateSQLiteRequiredColumns(probe); err == nil {
		t.Fatal("incompatible runtime schema unexpectedly passed column validation")
	}
	_ = probe.Close()

	prepareErr := store.PrepareSQLiteSchemaBeforeOwner(ctx)
	if prepareErr == nil {
		t.Fatal("incompatible runtime_state schema unexpectedly prepared successfully")
	}
	var marker string
	err = withSQLiteUnpreparedRawQueryForTest(store, `SELECT value FROM state_meta WHERE key = ?`, &marker, sqliteSchemaPreparationVersionKey)
	if !errors.Is(err, sql.ErrNoRows) {
		if err != nil {
			t.Fatalf("inspect revoked runtime schema marker: %v", err)
		}
		t.Fatalf("incompatible runtime_state schema retained ready marker %q", marker)
	}
}

// A scalar status that says queued while the canonical JSON says sent is
// opaque evidence, not a pending message.  The durable marker is revoked by
// the trigger, so distinct-chat admission must use the canonical lane and not
// return a phantom chat that would cause needless Graph polling.
func TestSQLitePendingChatAdmissionRequiresTrustedOutboxProjection(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	message := OutboxMessage{
		ID: "outbox:phantom-chat", TeamsChatID: "chat:phantom", Status: OutboxStatusQueued,
		Sequence: 1, CreatedAt: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC), Body: "opaque",
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[message.ID] = message
		return nil
	}); err != nil {
		t.Fatalf("seed phantom chat: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		raw, err := json.Marshal(message)
		if err != nil {
			return err
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return err
		}
		object["status"] = json.RawMessage(`"sent"`)
		raw, err = json.Marshal(object)
		if err != nil {
			return err
		}
		// Keep the scalar status as queued.  The trigger must revoke the
		// projection marker on this canonical-only write.
		_, err = tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`, raw, message.ID)
		return err
	})
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	if markers[sqliteOutboxProjectionTrustKey] != sqliteOutboxProjectionTrustUntrusted {
		t.Fatalf("outbox marker after canonical/scalar contradiction = %q, want untrusted; all=%#v", markers[sqliteOutboxProjectionTrustKey], markers)
	}
	ids, err := store.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: message.CreatedAt.Add(time.Hour)}, 2)
	if err != nil {
		t.Fatalf("pending chat admission: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("opaque sent row was admitted as pending chat: %v", ids)
	}
}

func TestSQLiteOwnerProjectionAuditCannotPublishWithoutResultReceipt(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := OwnerMetadata{ScopeID: "audit-receipt-scope", MachineID: "audit-receipt-machine", InstanceID: "audit-receipt-instance", LeaseGeneration: 1}
	seedSQLiteOutboxAuditOwnerForTest(t, store, owner, 1)
	snapshot, claimed, err := store.claimSQLiteOutboxProjectionAuditForOwner(ctx, owner)
	if err != nil || !claimed {
		t.Fatalf("claim audit: snapshot=%#v claimed=%v err=%v", snapshot, claimed, err)
	}
	if err := store.finishSQLiteOutboxProjectionAuditForOwner(ctx, owner, snapshot, true, true, true, false); !errors.Is(err, ErrSQLiteOutboxProjectionAuditInProgress) {
		t.Fatalf("finish without receipt error = %v, want in-progress fence", err)
	}
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustAuditing {
			t.Fatalf("finish without receipt changed marker %s to %q", key, marker)
		}
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		claim, present, valid, err := loadSQLiteOutboxProjectionAuditClaim(ctx, tx)
		if err != nil {
			return err
		}
		if !present || !valid || claim.ResultReady {
			t.Fatalf("finish without receipt changed claim: present=%v valid=%v claim=%+v", present, valid, claim)
		}
		return nil
	})
}
