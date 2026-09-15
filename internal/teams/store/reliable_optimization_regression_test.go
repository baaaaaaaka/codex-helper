package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func readOutboxProjectionTrustMarkersForTest(t *testing.T, store *Store) map[string]string {
	t.Helper()
	markers := make(map[string]string, 3)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for _, key := range []string{
			sqliteOutboxProjectionTrustKey,
			sqliteOutboxSessionProjectionTrustKey,
			sqliteOutboxTurnProjectionTrustKey,
		} {
			value, err := sqliteReadMetaValueContext(context.Background(), tx, key)
			if err != nil {
				return err
			}
			markers[key] = value
		}
		return nil
	})
	return markers
}

// A duplicate-key row is not a candidate. SQLite JSON1 and encoding/json use
// different duplicate-key semantics, so the canonical lane must reject it at
// the SQL boundary and continue to a healthy later chat rather than letting
// the corrupt row consume the distinct-chat page.
func TestSQLiteCanonicalPendingChatAdmissionRejectsDuplicateKeysBeforeGrouping(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	healthy := OutboxMessage{
		ID: "outbox:duplicate-key-healthy", TeamsChatID: "chat:duplicate-key-healthy",
		Kind: "helper", Body: "healthy after duplicate", Status: OutboxStatusQueued,
		Sequence: 2, CreatedAt: now.Add(time.Second), UpdatedAt: now,
	}
	if _, _, err := store.QueueOutbox(ctx, healthy); err != nil {
		t.Fatalf("seed healthy outbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		duplicate := []byte(`{"id":"outbox:duplicate-key","teams_chat_id":"chat:duplicate-key","status":"queued","status":"sent","sequence":1,"created_at":"2026-09-14T12:00:00Z","body":"corrupt"}`)
		if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			"outbox:duplicate-key", "", "", "chat:duplicate-key", "", string(OutboxStatusQueued), 1, sqliteTime(now), 0, 0, duplicate); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, sqliteOutboxProjectionTrustDeferred, sqliteOutboxProjectionTrustKey)
		return err
	})
	ids, err := store.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now.Add(time.Minute)}, 8)
	if err != nil {
		t.Fatalf("pending chat IDs with duplicate row: %v", err)
	}
	if len(ids) != 1 || ids[0] != healthy.TeamsChatID {
		t.Fatalf("pending chat IDs = %#v, want only healthy chat %q", ids, healthy.TeamsChatID)
	}
	page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now.Add(time.Minute), Limit: 8})
	if err != nil {
		t.Fatalf("pending page with duplicate row: %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != healthy.ID {
		t.Fatalf("pending page = %#v, want only healthy row %q", page, healthy.ID)
	}
}

// Canonical JSON fallback is intentionally cold, but it must not hold the
// Store state/file lock while it scans. Blocking the fallback hook and then
// completing an independent durable runtime write makes that boundary
// observable without depending on wall-clock performance.
func TestSQLiteCanonicalPendingFallbackReleasesStoreLockDuringRead(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 14, 12, 5, 0, 0, time.UTC)
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID: "outbox:canonical-lock-release", TeamsChatID: "chat:canonical-lock-release",
		Kind: "helper", Body: "fallback lock release", Status: OutboxStatusQueued,
		Sequence: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("seed fallback outbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, sqliteOutboxProjectionTrustDeferred, sqliteOutboxProjectionTrustKey)
		return err
	})

	entered := make(chan struct{})
	release := make(chan struct{})
	previousHook := sqliteOutboxPendingPageCanonicalFallbackTestHook
	sqliteOutboxPendingPageCanonicalFallbackTestHook = func() {
		close(entered)
		<-release
	}
	t.Cleanup(func() {
		sqliteOutboxPendingPageCanonicalFallbackTestHook = previousHook
		select {
		case <-release:
		default:
			close(release)
		}
	})

	pageDone := make(chan error, 1)
	go func() {
		_, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now.Add(time.Minute), Limit: 1})
		pageDone <- err
	}()
	select {
	case <-entered:
	case err := <-pageDone:
		t.Fatalf("fallback ended before hook: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("canonical fallback did not reach hook")
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := store.SetDraining(ctx, "canonical fallback lock probe")
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("runtime write while fallback read: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("runtime write blocked by canonical fallback read")
	}
	close(release)
	if err := <-pageDone; err != nil {
		t.Fatalf("canonical fallback page: %v", err)
	}
}

// A canonical fallback runs on a lock-free read handle, so a legitimate
// durable outbox append can race its snapshot witness. The read must retry a
// bounded number of times and return the stable result; returning
// errSQLiteOutboxReadSnapshotChanged immediately turns normal executor
// progress into a false admission failure.
func TestSQLiteCanonicalPendingFallbackRetriesAfterDurableAppend(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 14, 12, 10, 0, 0, time.UTC)
	seed := func(id, chat string, createdAt time.Time) {
		t.Helper()
		if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
			ID: id, TeamsChatID: chat, Kind: "helper", Body: id,
			Status: OutboxStatusQueued, Sequence: int64(createdAt.UnixNano()),
			CreatedAt: createdAt, UpdatedAt: createdAt,
		}); err != nil {
			t.Fatalf("seed outbox %q: %v", id, err)
		}
	}
	seed("outbox:canonical-retry-seed", "chat:canonical-retry-seed", now)
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, sqliteOutboxProjectionTrustDeferred, sqliteOutboxProjectionTrustKey)
		return err
	})

	var chatAppendErr error
	var chatAppendOnce sync.Once
	previousChatHook := sqliteOutboxPendingChatIDsCanonicalFallbackTestHook
	sqliteOutboxPendingChatIDsCanonicalFallbackTestHook = func() {
		chatAppendOnce.Do(func() {
			_, _, chatAppendErr = store.QueueOutbox(ctx, OutboxMessage{
				ID: "outbox:canonical-retry-chat", TeamsChatID: "chat:canonical-retry-chat",
				Kind: "helper", Body: "outbox:canonical-retry-chat", Status: OutboxStatusQueued,
				Sequence: 2, CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
			})
		})
	}
	t.Cleanup(func() { sqliteOutboxPendingChatIDsCanonicalFallbackTestHook = previousChatHook })
	chatIDs, err := store.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now.Add(time.Minute)}, 8)
	if err != nil {
		t.Fatalf("pending chat IDs after concurrent append: %v", err)
	}
	if chatAppendErr != nil {
		t.Fatalf("concurrent chat append: %v", chatAppendErr)
	}
	if len(chatIDs) != 2 {
		t.Fatalf("pending chat IDs after concurrent append = %#v, want two chats", chatIDs)
	}

	var pageAppendErr error
	var pageAppendOnce sync.Once
	previousPageHook := sqliteOutboxPendingPageCanonicalFallbackTestHook
	sqliteOutboxPendingPageCanonicalFallbackTestHook = func() {
		pageAppendOnce.Do(func() {
			_, _, pageAppendErr = store.QueueOutbox(ctx, OutboxMessage{
				ID: "outbox:canonical-retry-page", TeamsChatID: "chat:canonical-retry-page",
				Kind: "helper", Body: "outbox:canonical-retry-page", Status: OutboxStatusQueued,
				Sequence: 3, CreatedAt: now.Add(2 * time.Second), UpdatedAt: now.Add(2 * time.Second),
			})
		})
	}
	t.Cleanup(func() { sqliteOutboxPendingPageCanonicalFallbackTestHook = previousPageHook })
	page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now.Add(time.Minute), Limit: 8})
	if err != nil {
		t.Fatalf("pending page after concurrent append: %v", err)
	}
	if pageAppendErr != nil {
		t.Fatalf("concurrent page append: %v", pageAppendErr)
	}
	if len(page.Messages) != 3 {
		t.Fatalf("pending page after concurrent append = %#v, want three messages", page)
	}
}

// A large inherited outbox whose rows are still within the bounded startup
// audit envelope must establish native capability once, before foreground FIFO
// lookup. Row count alone must not force every send back through the O(N) JSON
// fallback.
func TestSQLiteLargeOutboxProjectionStartupAuditsWithinEnvelope(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 15, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < 4097; i++ {
			id := fmt.Sprintf("outbox:large-projection:%05d", i)
			state.OutboxMessages[id] = OutboxMessage{
				ID: id, TeamsChatID: "chat:large-projection", Kind: "helper",
				Body: "bounded projection payload", Status: OutboxStatusQueued,
				Sequence: int64(i + 1), CreatedAt: now.Add(time.Duration(i) * time.Nanosecond), UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed large outbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	var stages []string
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) { stages = append(stages, stage) }
	t.Cleanup(func() { sqliteOutboxAuditTestHook = previousHook })
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("PrepareOutboxProjection: %v", err)
	}
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("startup marker %s = %q, want trusted marker for bounded large outbox; rows=%d stages=%v all=%#v", key, marker, sqliteTableRowCountForTest(t, store, "outbox_messages"), stages, markers)
		}
	}
	if len(stages) == 0 {
		t.Fatal("startup did not establish native capability through the audit")
	}

	stages = nil
	if err := store.RetryDeferredOutboxProjectionAudit(ctx); err != nil {
		t.Fatalf("RetryDeferredOutboxProjectionAudit: %v", err)
	}
	markers = readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("explicit retry marker %s = %q, want unchanged trusted marker; all=%#v", key, marker, markers)
		}
	}
	if len(stages) != 0 {
		t.Fatalf("explicit retry unexpectedly repeated trusted audit: stages=%v", stages)
	}
}

// An individual pathological JSON record remains outside the synchronous
// startup envelope. It must leave a durable non-native marker, and an explicit
// maintenance retry must still be able to establish trust without changing the
// outbox rows.
func TestSQLiteOversizedOutboxProjectionStartupDefersAndExplicitRetryAudits(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 15, 5, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox:oversized-projection"] = OutboxMessage{
			ID: "outbox:oversized-projection", TeamsChatID: "chat:oversized-projection",
			Kind: "helper", Body: strings.Repeat("x", int(sqliteOutboxStartupAuditMaxJSONRowBytes)+1),
			Status: OutboxStatusQueued, Sequence: 1, CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed oversized outbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	var stages []string
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) { stages = append(stages, stage) }
	t.Cleanup(func() { sqliteOutboxAuditTestHook = previousHook })
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("PrepareOutboxProjection: %v", err)
	}
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustDeferred {
			t.Fatalf("oversized startup marker %s = %q, want deferred; stages=%v all=%#v", key, marker, stages, markers)
		}
	}
	if len(stages) != 1 || stages[0] != "deferred" {
		t.Fatalf("oversized startup audit stages = %v, want [deferred]", stages)
	}

	stages = nil
	if err := store.RetryDeferredOutboxProjectionAudit(ctx); err != nil {
		t.Fatalf("RetryDeferredOutboxProjectionAudit: %v", err)
	}
	markers = readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("oversized retry marker %s = %q, want trusted; all=%#v", key, marker, markers)
		}
	}
	if len(stages) == 0 {
		t.Fatal("explicit retry did not enter the full audit")
	}
}

// An interrupted audit leaves an auditing marker.  That marker is not proof
// that the next owner may skip the startup envelope: a large inherited store
// must be deferred again before the full JSON audit is opened.
func TestSQLiteAuditingMarkerStillHonorsStartupBudget(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 15, 10, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox:auditing-budget"] = OutboxMessage{
			ID: "outbox:auditing-budget", TeamsChatID: "chat:auditing-budget",
			Kind: "helper", Body: strings.Repeat("x", int(sqliteOutboxStartupAuditMaxJSONRowBytes)+1),
			Status: OutboxStatusQueued, Sequence: 1, CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed auditing-budget outbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for _, key := range []string{
			sqliteOutboxProjectionTrustKey,
			sqliteOutboxSessionProjectionTrustKey,
			sqliteOutboxTurnProjectionTrustKey,
		} {
			if err := sqliteWriteMetaValue(tx, key, sqliteOutboxProjectionTrustAuditing); err != nil {
				return err
			}
		}
		return nil
	})

	var stages []string
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) { stages = append(stages, stage) }
	t.Cleanup(func() { sqliteOutboxAuditTestHook = previousHook })
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("PrepareOutboxProjection from auditing marker: %v", err)
	}
	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, store) {
		if marker != sqliteOutboxProjectionTrustDeferred {
			t.Fatalf("auditing-budget marker %s = %q, want deferred; stages=%v", key, marker, stages)
		}
	}
	if len(stages) != 1 || stages[0] != "deferred" {
		t.Fatalf("auditing-budget audit stages = %v, want [deferred] without opening full audit", stages)
	}
}

// If an unowned/offline process is canceled after the durable audit-start
// marker but before the read-only scan completes, it must release its exact
// claim before returning. The next startup must not mistake the marker for
// proof or leave the store permanently on the full JSON fallback; it resumes
// the audit and publishes a trusted result without waiting for the stale TTL.
func TestSQLiteInterruptedOutboxProjectionAuditLeavesAuditingAndCanResume(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC()
	if _, _, err := store.QueueOutbox(context.Background(), OutboxMessage{
		ID: "outbox:interrupted-projection", TeamsChatID: "chat:interrupted-projection",
		Kind: "helper", Body: "audit interruption", Status: OutboxStatusSent,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("seed outbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

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

	ctx, cancel := context.WithCancel(context.Background())
	prepareDone := make(chan error, 1)
	go func() { prepareDone <- store.PrepareOutboxProjection(ctx) }()
	select {
	case <-opened:
	case err := <-prepareDone:
		t.Fatalf("projection preparation ended before audit opened: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("projection audit did not open")
	}
	cancel()
	close(release)
	if err := <-prepareDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled projection audit error = %v, want context.Canceled", err)
	}
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustAuditing {
			t.Fatalf("interrupted marker %s = %q, want auditing; all=%#v", key, marker, markers)
		}
	}
	var claimPresent bool
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		var err error
		_, claimPresent, _, err = loadSQLiteOutboxProjectionAuditClaim(context.Background(), tx)
		return err
	})
	if claimPresent {
		t.Fatal("canceled ownerless audit claim was not released")
	}

	// The test hook is only for the interrupted attempt. A normal next startup
	// must treat the auditing marker as incomplete and resume automatically;
	// otherwise one canceled owner would strand every later FIFO lookup on the
	// full JSON lane.
	sqliteOutboxAuditTestHook = nil
	if err := store.PrepareOutboxProjection(context.Background()); err != nil {
		t.Fatalf("resume interrupted projection audit on next startup: %v", err)
	}
	markers = readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("resumed marker %s = %q, want trusted; all=%#v", key, marker, markers)
		}
	}
}

// The startup cleanup is a compatibility mutation, so the listener's live
// owner must fence it just like every other durable write. Exercise both
// backends with a stale capability and verify that the obsolete row is still
// queued until the current owner performs the cleanup.
func TestLegacyHistoryGateStartupCleanupIsOwnerFencedAcrossBackends(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC()
			if err := store.Update(context.Background(), func(state *State) error {
				state.Scope = ScopeIdentity{ID: "cleanup-scope", AccountID: "cleanup-account", Profile: "default"}
				state.ControlLease = ControlLease{
					ScopeID: "cleanup-scope", HolderMachineID: "cleanup-owner-a", Generation: 7,
					Status: ControlLeaseStatusActive, LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
				}
				state.OutboxMessages["outbox:legacy-cleanup-owner-fence"] = OutboxMessage{
					ID: "outbox:legacy-cleanup-owner-fence", TeamsChatID: "chat:cleanup-owner-fence",
					Kind: "sync-status-backlog-blocked", Body: "obsolete history gate",
					Status: OutboxStatusQueued, Sequence: 1, CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed cleanup state: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}

			if _, err := store.RetireLegacyHistoryGateOutboxForStartupForOwner(context.Background(), 8, 2, "cleanup-owner-b", 8); !errors.Is(err, ErrControlLeaseNotHeld) {
				t.Fatalf("stale cleanup error = %v, want ErrControlLeaseNotHeld", err)
			}
			row, err := store.OutboxMessageByID(context.Background(), "outbox:legacy-cleanup-owner-fence")
			if err != nil {
				t.Fatalf("read stale-cleanup row: %v", err)
			}
			if row.Status != OutboxStatusQueued {
				t.Fatalf("stale cleanup changed row status to %q", row.Status)
			}

			retired, err := store.RetireLegacyHistoryGateOutboxForStartupForOwner(context.Background(), 8, 2, "cleanup-owner-a", 7)
			if err != nil || retired != 1 {
				t.Fatalf("current-owner cleanup retired=%d err=%v, want 1/nil", retired, err)
			}
			row, err = store.OutboxMessageByID(context.Background(), row.ID)
			if err != nil {
				t.Fatalf("read retired-cleanup row: %v", err)
			}
			if row.Status != OutboxStatusSkipped {
				t.Fatalf("current-owner cleanup status = %q, want skipped", row.Status)
			}
		})
	}
}

// The page boundary is also an ownership boundary. If the lease is handed to
// another process after one committed page, the old owner must stop before
// committing the next page rather than relying only on its in-memory owner
// fields.
func TestSQLiteLegacyHistoryGateCleanupStopsAfterPageBoundaryTakeover(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = ScopeIdentity{ID: "cleanup-page-scope", AccountID: "cleanup-page-account", Profile: "default"}
		state.ControlLease = ControlLease{
			ScopeID: "cleanup-page-scope", HolderMachineID: "cleanup-page-owner-a", Generation: 7,
			Status: ControlLeaseStatusActive, LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
		}
		for i := 0; i < 3; i++ {
			id := fmt.Sprintf("outbox:legacy-page-fence:%d", i)
			state.OutboxMessages[id] = OutboxMessage{
				ID: id, TeamsChatID: "chat:cleanup-page-fence", Kind: "sync-status-backlog-blocked",
				Body: "obsolete history gate", Status: OutboxStatusQueued,
				Sequence: int64(i + 1), CreatedAt: now.Add(time.Duration(i) * time.Second), UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed page-fence state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	takeoverErr := make(chan error, 1)
	var once sync.Once
	previousHook := sqliteLegacyHistoryGatePageTestHook
	sqliteLegacyHistoryGatePageTestHook = func() {
		once.Do(func() {
			if _, err := store.ReleaseControlLeaseIfHolder(ctx, "cleanup-page-owner-a", 7); err != nil {
				takeoverErr <- fmt.Errorf("release old owner: %w", err)
				return
			}
			ownerB := testOwner("", "", time.Now().UTC())
			ownerB.ScopeID = "cleanup-page-scope"
			ownerB.MachineID = "cleanup-page-owner-b"
			decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope:   ScopeIdentity{ID: "cleanup-page-scope", AccountID: "cleanup-page-account", Profile: "default"},
				Machine: MachineRecord{ID: "cleanup-page-owner-b", ScopeID: "cleanup-page-scope", Status: MachineStatusActive},
				Owner:   ownerB, Duration: time.Hour, Now: time.Now().UTC(),
			})
			if err != nil {
				takeoverErr <- fmt.Errorf("claim new owner: %w", err)
				return
			}
			if decision.Mode != LeaseModeActive || decision.Lease.Generation <= 7 {
				takeoverErr <- fmt.Errorf("new owner decision = %#v", decision)
				return
			}
			takeoverErr <- nil
		})
	}
	t.Cleanup(func() { sqliteLegacyHistoryGatePageTestHook = previousHook })

	retired, err := store.RetireLegacyHistoryGateOutboxForStartupForOwner(ctx, 1, 4, "cleanup-page-owner-a", 7)
	if !errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("cleanup after page takeover error = %v, want ErrControlLeaseNotHeld", err)
	}
	if retired != 2 {
		t.Fatalf("cleanup after page takeover retired=%d, want first page's 2 rows", retired)
	}
	if err := <-takeoverErr; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		row, readErr := store.OutboxMessageByID(ctx, fmt.Sprintf("outbox:legacy-page-fence:%d", i))
		if readErr != nil {
			t.Fatalf("read page-fence row %d: %v", i, readErr)
		}
		want := OutboxStatusSkipped
		if i == 2 {
			want = OutboxStatusQueued
		}
		if row.Status != want {
			t.Fatalf("page-fence row %d status = %q, want %q", i, row.Status, want)
		}
	}
}

// A repair callback is intentionally a row-local CAS.  Once another writer
// changes the recovery evidence, the stale callback must become a no-op on
// both backends rather than clearing the replacement marker or installing a
// gap for the wrong raw frontier.
func TestChatPollRevisionFenceRejectsStaleRepairAcrossBackends(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			const chatID = "chat-stale-repair"
			if err := store.Update(ctx, func(state *State) error {
				state.ChatPolls[chatID] = ChatPollState{
					ChatID:             chatID,
					Seeded:             true,
					PollState:          chatPollStateWarm,
					RecoveryRequired:   true,
					RecoveryReason:     chatPollOpaqueRecoveryReason,
					RecoverySourceHash: "repair-source-before",
					PollRevision:       7,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed stale-repair poll: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			before, found, err := store.ChatPoll(ctx, chatID)
			if err != nil || !found {
				t.Fatalf("read stale-repair snapshot: found=%v err=%v poll=%#v", found, err, before)
			}
			if _, changed, err := store.UpdateChatPoll(ctx, chatID, func(poll *ChatPollState) error {
				poll.RecoveryRequired = false
				poll.RecoveryReason = ""
				poll.RecoverySourceHash = ""
				poll.Gap = &ChatPollGap{Kind: "replacement-gap"}
				return nil
			}); err != nil || !changed {
				t.Fatalf("replace recovery evidence: changed=%v err=%v", changed, err)
			}

			_, changed, err := store.UpdateChatPollAtRevision(ctx, chatID, before.PollRevision, func(poll *ChatPollState) error {
				poll.RecoveryRequired = false
				poll.RecoveryReason = ""
				poll.RecoverySourceHash = ""
				poll.Gap = &ChatPollGap{Kind: "stale-repair-must-not-open-gap"}
				return nil
			})
			if err != nil {
				t.Fatalf("stale repair CAS: %v", err)
			}
			if changed {
				t.Fatalf("stale repair unexpectedly changed a newer poll revision: before=%#v", before)
			}
			after, found, err := store.ChatPoll(ctx, chatID)
			if err != nil || !found {
				t.Fatalf("read after stale repair: found=%v err=%v poll=%#v", found, err, after)
			}
			if after.PollRevision <= before.PollRevision || after.RecoverySourceHash != "" || after.RecoveryReason != "" || after.Gap == nil || after.Gap.Kind != "replacement-gap" {
				t.Fatalf("stale repair overwrote replacement evidence: %#v", after)
			}
		})
	}
}

func TestLegacyHistoryGateSendClassifierDoesNotSkipRealImportAttention(t *testing.T) {
	base := OutboxMessage{Kind: "import-needs-attention", TurnID: "import-bg:session-1"}
	base.Body = "local Codex history sync needs attention. I could not find the saved transcript checkpoint for this import."
	if !IsLegacyHistoryGateNoticeForSend(base) {
		t.Fatal("exact legacy import notice was not classified as obsolete")
	}

	realAttention := base
	realAttention.Body = "Local Codex history sync needs attention. Please choose a source file."
	if IsLegacyHistoryGateNoticeForSend(realAttention) {
		t.Fatal("real import-needs-attention message was incorrectly classified as obsolete")
	}

	withRemoteID := base
	withRemoteID.TeamsMessageID = "teams-message-1"
	if IsLegacyHistoryGateNoticeForSend(withRemoteID) {
		t.Fatal("already-delivered legacy notice was classified as pending send")
	}
}
