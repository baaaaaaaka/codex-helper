package teams

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestGlobalInboundLedgerPathForRegistry(t *testing.T) {
	tmp := t.TempDir()
	scopedRegistry := filepath.Join(tmp, "teams", "scopes", "scope-a", "registry.json")
	got, ok := globalInboundLedgerPathForRegistry(scopedRegistry)
	if !ok {
		t.Fatal("scoped registry should enable global inbound ledger")
	}
	want := filepath.Join(tmp, "teams", "global-inbound-ledger.json")
	if got != want {
		t.Fatalf("scoped global ledger path = %q, want %q", got, want)
	}

	plainRegistry := filepath.Join(tmp, "profile", "registry.json")
	got, ok = globalInboundLedgerPathForRegistry(plainRegistry)
	if !ok {
		t.Fatal("plain registry should enable global inbound ledger")
	}
	want = filepath.Join(tmp, "profile", "teams-global-inbound-ledger.json")
	if got != want {
		t.Fatalf("plain global ledger path = %q, want %q", got, want)
	}

	if got, ok := globalInboundLedgerPathForRegistry(""); ok || got != "" {
		t.Fatalf("empty registry should disable global inbound ledger, got path=%q ok=%v", got, ok)
	}
}

func TestGlobalInboundSQLiteWriterReopensReplacedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	sqlitePath := teamsLedgerSQLitePath(path)
	writer := &globalInboundSQLiteWriter{}
	t.Cleanup(func() { _ = writer.close() })
	first, err := writer.open(sqlitePath)
	if err != nil {
		t.Fatalf("open writer database: %v", err)
	}
	if _, err := first.ExecContext(context.Background(), `CREATE TABLE writer_probe (value TEXT NOT NULL)`); err != nil {
		t.Fatalf("seed writer database: %v", err)
	}
	// Unix permits replacing an open SQLite file, which is the production
	// recovery shape this test exercises. Windows holds the database handle
	// without delete sharing, so the same rename is rejected by the OS before
	// globalInboundSQLiteWriter can observe a new file identity. Close the
	// fixture connection there and keep the rest of the reopen/claim contract
	// covered on every platform.
	if runtime.GOOS == "windows" {
		if err := writer.close(); err != nil {
			t.Fatalf("close writer database before Windows replacement: %v", err)
		}
	}
	oldPath := sqlitePath + ".old"
	if err := os.Rename(sqlitePath, oldPath); err != nil {
		t.Fatalf("rename writer database: %v", err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Rename(sqlitePath+suffix, oldPath+suffix); err != nil && !os.IsNotExist(err) {
			t.Fatalf("rename writer database %s: %v", suffix, err)
		}
	}
	replacement, err := openTeamsLedgerSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("create replacement writer database: %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatalf("close replacement writer database: %v", err)
	}
	second, err := writer.open(sqlitePath)
	if err != nil {
		t.Fatalf("reopen replaced writer database: %v", err)
	}
	if second == first {
		t.Fatal("writer reused a connection to the replaced database")
	}
	if _, err := second.ExecContext(context.Background(), `SELECT 1`); err != nil {
		t.Fatalf("query replacement writer database: %v", err)
	}
	claim, claimed, err := claimGlobalInboundWithWriter(context.Background(), path, "chat-replaced", "message-replaced", "owner-reopened", time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC), writer)
	if err != nil {
		t.Fatalf("claim through reopened writer: %v", err)
	}
	if !claimed {
		t.Fatal("claim through reopened writer should win")
	}
	if err := completeGlobalInbound(context.Background(), claim); err != nil {
		t.Fatalf("complete through reopened writer: %v", err)
	}
	ledger, err := readGlobalInboundLedger(path)
	if err != nil {
		t.Fatalf("read reopened writer ledger: %v", err)
	}
	if item := ledger.Items[globalInboundKey("chat-replaced", "message-replaced")]; item.Status != "done" {
		t.Fatalf("reopened writer ledger item = %#v, want done", item)
	}
}

func TestGlobalInboundLedgerClaimLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)

	claim, claimed, err := claimGlobalInbound(ctx, path, "chat-1", "message-1", "owner-a", now)
	if err != nil {
		t.Fatalf("first claim error: %v", err)
	}
	if !claimed {
		t.Fatal("first claim should win")
	}
	if _, claimed, err := claimGlobalInbound(ctx, path, "chat-1", "message-1", "owner-b", now.Add(time.Second)); err != nil {
		t.Fatalf("second claim error: %v", err)
	} else if claimed {
		t.Fatal("second claim should lose while first claim is fresh")
	}

	releaseGlobalInbound(ctx, claim)
	claim, claimed, err = claimGlobalInbound(ctx, path, "chat-1", "message-1", "owner-b", now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("claim after release error: %v", err)
	}
	if !claimed {
		t.Fatal("claim after release should win")
	}
	if err := completeGlobalInbound(ctx, claim); err != nil {
		t.Fatalf("complete claim error: %v", err)
	}
	if _, claimed, err := claimGlobalInbound(ctx, path, "chat-1", "message-1", "owner-c", now.Add(3*time.Second)); err != nil {
		t.Fatalf("claim after done error: %v", err)
	} else if claimed {
		t.Fatal("done entry should suppress duplicate claims")
	}

	ledger, err := readGlobalInboundLedger(path)
	if err != nil {
		t.Fatalf("read ledger error: %v", err)
	}
	item := ledger.Items[globalInboundKey("chat-1", "message-1")]
	if item.Status != "done" || item.Owner != "owner-b" {
		t.Fatalf("completed ledger item = %#v, want done owner-b", item)
	}
}

func TestGlobalInboundLedgerWriterClaimLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	writer := &globalInboundSQLiteWriter{}
	t.Cleanup(func() { _ = writer.close() })
	now := time.Date(2026, 5, 11, 13, 0, 0, 0, time.UTC)

	claim, claimed, err := claimGlobalInboundWithWriter(ctx, path, "chat-writer", "message-1", "owner-a", now, writer)
	if err != nil {
		t.Fatalf("writer first claim error: %v", err)
	}
	if !claimed {
		t.Fatal("writer first claim should win")
	}
	if err := completeGlobalInbound(ctx, claim); err != nil {
		t.Fatalf("writer completion error: %v", err)
	}
	if _, claimed, err := claimGlobalInboundWithWriter(ctx, path, "chat-writer", "message-1", "owner-b", now.Add(time.Second), writer); err != nil {
		t.Fatalf("writer duplicate claim error: %v", err)
	} else if claimed {
		t.Fatal("writer completed entry should suppress duplicate claim")
	}

	ledger, err := readGlobalInboundLedger(path)
	if err != nil {
		t.Fatalf("read writer ledger: %v", err)
	}
	item := ledger.Items[globalInboundKey("chat-writer", "message-1")]
	if item.Status != "done" || item.Owner != "owner-a" {
		t.Fatalf("writer completed ledger item = %#v, want done owner-a", item)
	}
}

func TestGlobalInboundSQLiteWriterWaitHonorsContextCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	writer := &globalInboundSQLiteWriter{}
	t.Cleanup(func() { _ = writer.close() })

	holderStarted := make(chan struct{})
	holderRelease := make(chan struct{})
	var holderReleaseOnce sync.Once
	releaseHolder := func() { holderReleaseOnce.Do(func() { close(holderRelease) }) }
	// Opening the modernc SQLite sidecar and creating its schema can take more
	// than one second on a busy Windows hosted runner. Always release the
	// holder during cleanup as well, so a setup-time assertion cannot strand the
	// writer gate and make writer.close block the whole package.
	t.Cleanup(releaseHolder)
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- updateGlobalInboundSQLiteWithWriter(context.Background(), path, writer, func(*sql.Tx, time.Time) error {
			close(holderStarted)
			<-holderRelease
			return nil
		})
	}()
	select {
	case <-holderStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("writer holder did not enter its transaction")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := updateGlobalInboundSQLiteWithWriter(ctx, path, writer, func(*sql.Tx, time.Time) error {
		return errors.New("canceled waiter acquired the writer")
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled writer waiter error = %v, want context deadline exceeded", err)
	}
	releaseHolder()
	if err := <-holderDone; err != nil {
		t.Fatalf("writer holder error: %v", err)
	}
}

func TestGlobalInboundLedgerMigratesLegacyJSONWithoutRewrite(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	legacy := globalInboundLedger{
		Version: 1,
		Items: map[string]globalInboundItem{
			globalInboundKey("chat-1", "legacy-message"): {
				ChatID:    "chat-1",
				MessageID: "legacy-message",
				Owner:     "owner-a",
				Status:    "done",
				UpdatedAt: now.Add(-time.Hour),
			},
		},
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir legacy inbound ledger: %v", err)
	}
	raw, err := json.MarshalIndent(legacy, "", "  ")
	if err != nil {
		t.Fatalf("marshal legacy inbound ledger: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write legacy inbound ledger: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read legacy inbound before: %v", err)
	}

	if _, claimed, err := claimGlobalInbound(ctx, path, "chat-1", "new-message", "owner-b", now); err != nil {
		t.Fatalf("claim migrated inbound ledger: %v", err)
	} else if !claimed {
		t.Fatal("new inbound claim should win")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read legacy inbound after: %v", err)
	}
	if string(after) != string(before) {
		t.Fatal("claiming inbound ledger rewrote legacy JSON")
	}
	if _, err := os.Stat(teamsLedgerSQLitePath(path)); err != nil {
		t.Fatalf("stat inbound sqlite sidecar: %v", err)
	}
	got, err := readGlobalInboundLedger(path)
	if err != nil {
		t.Fatalf("read migrated inbound ledger: %v", err)
	}
	if item := got.Items[globalInboundKey("chat-1", "legacy-message")]; item.Status != "done" {
		t.Fatalf("migrated ledger legacy item = %#v, want done", item)
	}
	if item := got.Items[globalInboundKey("chat-1", "new-message")]; item.Status != "claimed" || item.Owner != "owner-b" {
		t.Fatalf("migrated ledger new item = %#v, want claimed owner-b", item)
	}
}

func TestGlobalInboundLegacyWrongMapKeyNormalizesToCanonicalKey(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	legacy := globalInboundLedger{
		Version: 1,
		Items: map[string]globalInboundItem{
			"wrong-legacy-index": {
				ChatID:    "chat-normalized-key",
				MessageID: "message-normalized-key",
				Owner:     "owner-legacy",
				Status:    "done",
				UpdatedAt: now,
			},
		},
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir legacy inbound ledger: %v", err)
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal wrong-key legacy ledger: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write wrong-key legacy ledger: %v", err)
	}
	if _, claimed, err := claimGlobalInbound(ctx, path, "chat-normalized-key", "message-new", "owner-new", now.Add(time.Minute)); err != nil || !claimed {
		t.Fatalf("claim after wrong-key legacy import: claimed=%v err=%v", claimed, err)
	}
	got, err := readGlobalInboundLedger(path)
	if err != nil {
		t.Fatalf("read normalized inbound ledger: %v", err)
	}
	canonicalKey := globalInboundKey("chat-normalized-key", "message-normalized-key")
	if item, ok := got.Items[canonicalKey]; !ok || item.Status != "done" {
		t.Fatalf("canonical legacy item = %#v present=%v, want done", item, ok)
	}
	if _, ok := got.Items["wrong-legacy-index"]; ok {
		t.Fatal("wrong legacy map key was exposed as a second inbound identity")
	}
	if _, claimed, err := claimGlobalInbound(ctx, path, "chat-normalized-key", "message-normalized-key", "owner-replay", now.Add(2*time.Minute)); err != nil || claimed {
		t.Fatalf("canonical done item was claimable after normalization: claimed=%v err=%v", claimed, err)
	}
}

func TestGlobalInboundLegacyInvalidRowsAreQuarantinedWithoutPoisoningClaims(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Date(2026, 9, 7, 18, 0, 0, 0, time.UTC)
	invalidKey := globalInboundKey("chat-legacy-invalid", "message-legacy-invalid")
	legacy := globalInboundLedger{
		Version: 1,
		Items: map[string]globalInboundItem{
			invalidKey: {
				ChatID:    "chat-legacy-invalid",
				MessageID: "message-legacy-invalid",
				Owner:     "legacy-owner",
				// A missing status is not proof that the message was handled.
				UpdatedAt: now,
			},
		},
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir invalid legacy ledger: %v", err)
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal invalid legacy ledger: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write invalid legacy ledger: %v", err)
	}

	if _, claimed, err := claimGlobalInbound(ctx, path, "chat-legacy-healthy", "message-healthy", "owner-new", now); err != nil || !claimed {
		t.Fatalf("healthy claim alongside invalid legacy row = claimed=%v err=%v", claimed, err)
	}
	if _, claimed, err := claimGlobalInbound(ctx, path, "chat-legacy-invalid", "message-legacy-invalid", "owner-new", now); claimed || !errors.Is(err, ErrInboundLedgerProjectionUntrusted) {
		t.Fatalf("invalid legacy row became claimable: claimed=%v err=%v", claimed, err)
	}
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		t.Fatalf("open invalid legacy sidecar: %v", err)
	}
	defer db.Close()
	var active int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger WHERE key = ?`, invalidKey).Scan(&active); err != nil {
		t.Fatalf("count invalid active row: %v", err)
	}
	if active != 0 {
		t.Fatalf("invalid legacy row was written into active ledger: %d", active)
	}
	var quarantined int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger_quarantine WHERE source_key = ?`, invalidKey).Scan(&quarantined); err != nil {
		t.Fatalf("count invalid quarantine row: %v", err)
	}
	if quarantined != 1 {
		t.Fatalf("invalid legacy row quarantine count = %d, want 1", quarantined)
	}
}

func TestGlobalInboundSQLiteTornProjectionFailsClosedBeforeClaim(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Now().UTC()
	claim, claimed, err := claimGlobalInbound(ctx, path, "chat-torn-projection", "message-torn-projection", "owner-a", now)
	if err != nil || !claimed {
		t.Fatalf("seed torn-projection claim: claimed=%v err=%v", claimed, err)
	}
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		t.Fatalf("open torn-projection ledger: %v", err)
	}
	// The scalar projection still says owner-a has a fresh claim, while the
	// canonical JSON no longer contains a valid status. A replacement owner must
	// fail closed rather than interpreting the torn row as unclaimed.
	_, err = db.ExecContext(ctx, `UPDATE inbound_ledger SET json = ? WHERE key = ?`, []byte(`{"chat_id":"chat-torn-projection","message_id":"message-torn-projection","owner":"owner-a"}`), claim.Key)
	if err != nil {
		db.Close()
		t.Fatalf("tear canonical inbound projection: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close torn-projection ledger: %v", err)
	}
	_, replacementClaimed, err := claimGlobalInbound(ctx, path, "chat-torn-projection", "message-torn-projection", "owner-b", now.Add(time.Second))
	if replacementClaimed || !errors.Is(err, ErrInboundLedgerProjectionUntrusted) {
		t.Fatalf("torn fresh claim was not fail-closed: claimed=%v err=%v", replacementClaimed, err)
	}
}

func TestGlobalInboundLedgerMigrationDoesNotRegressNewerSQLiteState(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	key := globalInboundKey("chat-1", "migration-race")
	staleAt := time.Now().UTC().Add(-2 * time.Hour)
	legacy := globalInboundLedger{
		Version: 1,
		Items: map[string]globalInboundItem{
			key: {
				ChatID:     "chat-1",
				MessageID:  "migration-race",
				Owner:      "legacy-owner",
				ClaimToken: "legacy-token",
				Status:     "claimed",
				ClaimedAt:  staleAt,
				UpdatedAt:  staleAt,
			},
		},
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir legacy inbound ledger: %v", err)
	}
	writeLegacy := func(owner string) {
		t.Helper()
		legacy.Items[key] = globalInboundItem{
			ChatID:     "chat-1",
			MessageID:  "migration-race",
			Owner:      owner,
			ClaimToken: "legacy-token",
			Status:     "claimed",
			ClaimedAt:  staleAt,
			UpdatedAt:  staleAt,
		}
		raw, err := json.MarshalIndent(legacy, "", "  ")
		if err != nil {
			t.Fatalf("marshal legacy inbound ledger: %v", err)
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatalf("write legacy inbound ledger: %v", err)
		}
	}
	writeLegacy("legacy-owner")

	claim, claimed, err := claimGlobalInbound(ctx, path, "chat-1", "migration-race", "new-owner", time.Now().UTC())
	if err != nil {
		t.Fatalf("seed SQLite claim from legacy ledger: %v", err)
	}
	if !claimed {
		t.Fatal("seed SQLite claim should win stale legacy claim")
	}
	if err := completeGlobalInbound(ctx, claim); err != nil {
		t.Fatalf("complete newer SQLite claim: %v", err)
	}

	// A legacy writer may still rewrite its old JSON snapshot after the sidecar
	// has reached done. Importing that snapshot must not reopen the message for
	// another owner merely because the JSON file mtime changed.
	writeLegacy("stale-legacy-writer")
	if _, claimed, err := claimGlobalInbound(ctx, path, "chat-1", "migration-race", "replay-owner", time.Now().UTC().Add(time.Second)); err != nil {
		t.Fatalf("claim after stale legacy rewrite: %v", err)
	} else if claimed {
		t.Fatal("stale legacy rewrite regressed newer SQLite done state into a claim")
	}

	got, err := readGlobalInboundLedger(path)
	if err != nil {
		t.Fatalf("read merged inbound ledger: %v", err)
	}
	if item := got.Items[key]; item.Status != "done" || item.Owner != "new-owner" {
		t.Fatalf("merged inbound item = %#v, want newer SQLite done state", item)
	}
}

func TestGlobalInboundLedgerMigrationDoesNotRegressDoneForLaterLegacyClaim(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Now().UTC()
	claim, claimed, err := claimGlobalInbound(ctx, path, "chat-terminal-import", "message-terminal-import", "sqlite-owner", now)
	if err != nil || !claimed {
		t.Fatalf("seed terminal-import claim = %#v claimed=%v err=%v", claim, claimed, err)
	}
	if err := completeGlobalInbound(ctx, claim); err != nil {
		t.Fatalf("complete terminal-import claim: %v", err)
	}

	// A legacy writer can flush a newer-looking snapshot after the SQLite
	// sidecar has completed the message. Status is a monotonic terminal fact;
	// timestamp ordering alone must not reopen the message for another handler.
	legacy := globalInboundLedger{
		Version: 1,
		Items: map[string]globalInboundItem{
			claim.Key: {
				ChatID:     claim.ChatID,
				MessageID:  claim.MessageID,
				Owner:      "stale-legacy-owner",
				ClaimToken: "stale-legacy-token",
				Status:     "claimed",
				ClaimedAt:  now.Add(2 * time.Hour),
				UpdatedAt:  now.Add(2 * time.Hour),
			},
		},
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal later legacy claim: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write later legacy claim: %v", err)
	}
	if _, claimed, err := claimGlobalInbound(ctx, path, claim.ChatID, claim.MessageID, "replay-owner", now.Add(3*time.Hour)); err != nil {
		t.Fatalf("claim after later legacy regression attempt: %v", err)
	} else if claimed {
		t.Fatal("later legacy claimed snapshot reopened a durable done message")
	}
	merged, err := readGlobalInboundLedger(path)
	if err != nil {
		t.Fatalf("read merged terminal-import ledger: %v", err)
	}
	if item := merged.Items[claim.Key]; item.Status != "done" || item.Owner != claim.Owner {
		t.Fatalf("merged terminal-import item = %#v, want original done owner", item)
	}
}

func TestGlobalInboundLedgerReadRecoversPartialSQLiteSidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	legacy := globalInboundLedger{
		Version: 1,
		Items: map[string]globalInboundItem{
			globalInboundKey("chat-1", "legacy-message"): {
				ChatID:    "chat-1",
				MessageID: "legacy-message",
				Owner:     "owner-a",
				Status:    "done",
				UpdatedAt: now,
			},
		},
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir legacy inbound ledger: %v", err)
	}
	raw, err := json.MarshalIndent(legacy, "", "  ")
	if err != nil {
		t.Fatalf("marshal legacy inbound ledger: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write legacy inbound ledger: %v", err)
	}
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		t.Fatalf("open partial inbound sqlite sidecar: %v", err)
	}
	if err := ensureGlobalInboundSQLite(context.Background(), db); err != nil {
		_ = db.Close()
		t.Fatalf("create partial inbound sqlite sidecar: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close partial inbound sqlite sidecar: %v", err)
	}

	got, err := readGlobalInboundLedger(path)
	if err != nil {
		t.Fatalf("read partial inbound sqlite sidecar: %v", err)
	}
	if item := got.Items[globalInboundKey("chat-1", "legacy-message")]; item.Status != "done" {
		t.Fatalf("partial inbound sqlite read did not recover legacy item: %#v", got.Items)
	}
}

func TestGlobalInboundLedgerStaleClaimCanBeRecovered(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)

	if _, claimed, err := claimGlobalInbound(ctx, path, "chat-1", "message-1", "owner-a", now); err != nil {
		t.Fatalf("first claim error: %v", err)
	} else if !claimed {
		t.Fatal("first claim should win")
	}
	if _, claimed, err := claimGlobalInbound(ctx, path, "chat-1", "message-1", "owner-b", now.Add(globalInboundClaimTTL-time.Second)); err != nil {
		t.Fatalf("fresh duplicate claim error: %v", err)
	} else if claimed {
		t.Fatal("fresh claimed entry should not be stolen")
	}
	if claim, claimed, err := claimGlobalInbound(ctx, path, "chat-1", "message-1", "owner-b", now.Add(globalInboundClaimTTL+time.Second)); err != nil {
		t.Fatalf("stale duplicate claim error: %v", err)
	} else if !claimed {
		t.Fatal("stale claimed entry should be recoverable")
	} else if claim.Owner != "owner-b" {
		t.Fatalf("recovered claim owner = %q, want owner-b", claim.Owner)
	}
}

func TestGlobalInboundLedgerPrunePreservesFreshClaims(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Now().UTC()
	err := updateGlobalInboundSQLite(ctx, path, func(tx *sql.Tx, _ time.Time) error {
		for i := 0; i < maxGlobalInboundLedgerIDs+1; i++ {
			item := globalInboundItem{
				ChatID:             "chat-prune",
				MessageID:          fmt.Sprintf("message-%04d", i),
				Owner:              "owner-live",
				ScopeID:            "scope-live",
				ProcessIncarnation: "process-live",
				LeaseGeneration:    4,
				ClaimToken:         fmt.Sprintf("claim-%04d", i),
				Status:             "claimed",
				ClaimedAt:          now,
				UpdatedAt:          now,
			}
			if err := upsertGlobalInboundSQLiteTx(ctx, tx, globalInboundKey(item.ChatID, item.MessageID), item); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed fresh claimed ledger: %v", err)
	}
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		t.Fatalf("open fresh claimed ledger: %v", err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger`).Scan(&count); err != nil {
		t.Fatalf("count fresh claimed ledger: %v", err)
	}
	if count != maxGlobalInboundLedgerIDs+1 {
		t.Fatalf("fresh claimed ledger count = %d, want %d; pruning evicted live ownership", count, maxGlobalInboundLedgerIDs+1)
	}
}

func TestGlobalInboundClaimCommitsBeforeBoundedMalformedPrune(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Now().UTC()
	old := now.Add(-globalInboundClaimTTL - time.Second)
	seedGlobalInboundSQLiteForPrune(t, path, func(i int) globalInboundItem {
		return globalInboundItem{
			ChatID:    "chat-bounded-prune",
			MessageID: fmt.Sprintf("message-%04d", i),
			Owner:     "owner-old",
			Status:    "done",
			ClaimedAt: old,
			UpdatedAt: old,
		}
	})
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		t.Fatalf("open bounded-prune ledger: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		db.Close()
		t.Fatalf("begin bounded-prune corruption: %v", err)
	}
	for i := 0; i < 600; i++ {
		key := globalInboundKey("chat-bounded-prune", fmt.Sprintf("message-%04d", i))
		if _, err := tx.ExecContext(ctx, `UPDATE inbound_ledger SET json = ? WHERE key = ?`, []byte("{"), key); err != nil {
			_ = tx.Rollback()
			db.Close()
			t.Fatalf("corrupt bounded-prune row %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		db.Close()
		t.Fatalf("commit bounded-prune corruption: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close bounded-prune ledger: %v", err)
	}

	claim, claimed, err := claimGlobalInbound(ctx, path, "chat-bounded-prune", "message-new", "owner-new", now)
	if err != nil || !claimed {
		t.Fatalf("new claim was rolled back by bounded malformed prune: claimed=%v err=%v", claimed, err)
	}
	db, err = openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		t.Fatalf("reopen bounded-prune ledger: %v", err)
	}
	defer db.Close()
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger WHERE key = ?`, claim.Key).Scan(&exists); err != nil {
		t.Fatalf("check committed bounded-prune claim: %v", err)
	}
	if exists != 1 {
		t.Fatalf("committed bounded-prune claim missing: exists=%d", exists)
	}
}

func TestGlobalInboundSQLitePruneUsesCanonicalClaimStatus(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Now().UTC()
	claimedAt := now.Add(-globalInboundClaimTTL - time.Second)
	seedGlobalInboundSQLiteForPrune(t, path, func(i int) globalInboundItem {
		return globalInboundItem{
			ChatID:             "chat-canonical-prune",
			MessageID:          fmt.Sprintf("message-%04d", i),
			Owner:              "owner-live",
			ProcessIncarnation: "process-live",
			LeaseGeneration:    8,
			ClaimToken:         fmt.Sprintf("claim-%04d", i),
			Status:             "claimed",
			ClaimedAt:          claimedAt,
			UpdatedAt:          claimedAt,
		}
	})
	mismatchKey := globalInboundKey("chat-canonical-prune", "message-0000")
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		t.Fatalf("open canonical-status prune ledger: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE inbound_ledger SET status = ?, updated_at = ? WHERE key = ?`, "done", now.UnixNano(), mismatchKey); err != nil {
		db.Close()
		t.Fatalf("create canonical/scalar status mismatch: %v", err)
	}
	var raw []byte
	if err := db.QueryRowContext(ctx, `SELECT json FROM inbound_ledger WHERE key = ?`, mismatchKey).Scan(&raw); err != nil {
		db.Close()
		t.Fatalf("read canonical claim before prune: %v", err)
	}
	var canonical globalInboundItem
	if err := json.Unmarshal(raw, &canonical); err != nil {
		db.Close()
		t.Fatalf("decode canonical claim before prune: %v", err)
	}
	if canonical.Status != "claimed" {
		db.Close()
		t.Fatalf("seed canonical status = %q, want claimed", canonical.Status)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		db.Close()
		t.Fatalf("begin canonical-status prune transaction: %v", err)
	}
	if err := pruneGlobalInboundSQLiteTx(ctx, tx, now); err != nil {
		tx.Rollback()
		db.Close()
		t.Fatalf("prune canonical-status mismatch: %v", err)
	}
	if err := tx.Commit(); err != nil {
		db.Close()
		t.Fatalf("commit canonical-status prune: %v", err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger`).Scan(&count); err != nil {
		t.Fatalf("count canonical-status prune ledger: %v", err)
	}
	if count != maxGlobalInboundLedgerIDs {
		t.Fatalf("safe rows were not pruned around canonical claimed row: count=%d want=%d", count, maxGlobalInboundLedgerIDs)
	}
	var scalarStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM inbound_ledger WHERE key = ?`, mismatchKey).Scan(&scalarStatus); err != nil {
		t.Fatalf("read canonical claimed row after prune: %v", err)
	}
	if scalarStatus != "done" {
		t.Fatalf("canonical claimed row scalar status changed unexpectedly: %q", scalarStatus)
	}
}

func TestGlobalInboundSQLitePruneRemovesExpiredCanonicalClaims(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Now().UTC()
	claimedAt := now.Add(-globalInboundClaimTTL - time.Second)
	seedGlobalInboundSQLiteForPrune(t, path, func(i int) globalInboundItem {
		return globalInboundItem{
			ChatID:             "chat-expired-canonical-prune",
			MessageID:          fmt.Sprintf("message-%04d", i),
			Owner:              "owner-expired",
			ProcessIncarnation: "process-expired",
			LeaseGeneration:    3,
			ClaimToken:         fmt.Sprintf("claim-%04d", i),
			Status:             "claimed",
			ClaimedAt:          claimedAt,
			UpdatedAt:          claimedAt,
		}
	})
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		t.Fatalf("open expired canonical claims: %v", err)
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin expired canonical prune: %v", err)
	}
	if err := pruneGlobalInboundSQLiteTx(ctx, tx, now); err != nil {
		_ = tx.Rollback()
		t.Fatalf("prune expired canonical claims: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit expired canonical prune: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger`).Scan(&count); err != nil {
		t.Fatalf("count expired canonical claims: %v", err)
	}
	if count != maxGlobalInboundLedgerIDs {
		t.Fatalf("expired canonical claim was never pruned: count=%d want=%d", count, maxGlobalInboundLedgerIDs)
	}
	key := globalInboundKey("chat-expired-canonical-prune", "message-0000")
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger WHERE key = ?`, key).Scan(&exists); err != nil {
		t.Fatalf("check expired canonical claim: %v", err)
	}
	if exists != 0 {
		t.Fatal("oldest expired canonical claim remained after pruning")
	}
}

func TestGlobalInboundSQLitePrunePreservesAmbiguousClaimProjection(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(context.Context, *sql.DB, string, time.Time) error
	}{
		{
			name: "malformed-canonical-json",
			mutate: func(ctx context.Context, db *sql.DB, key string, now time.Time) error {
				_, err := db.ExecContext(ctx, `UPDATE inbound_ledger SET status = ?, updated_at = ?, json = ? WHERE key = ?`, "done", now.Add(-globalInboundClaimTTL-time.Second).UnixNano(), []byte("{"), key)
				return err
			},
		},
		{
			name: "fresh-scalar-claim",
			mutate: func(ctx context.Context, db *sql.DB, key string, now time.Time) error {
				_, err := db.ExecContext(ctx, `UPDATE inbound_ledger SET status = ?, claimed_at = ?, updated_at = ? WHERE key = ?`, "claimed", now.UnixNano(), now.UnixNano(), key)
				return err
			},
		},
		{
			name: "scalar-chat-identity-mismatch",
			mutate: func(ctx context.Context, db *sql.DB, key string, now time.Time) error {
				_, err := db.ExecContext(ctx, `UPDATE inbound_ledger SET chat_id = ?, updated_at = ? WHERE key = ?`, "chat-other", now.Add(-globalInboundClaimTTL-time.Second).UnixNano(), key)
				return err
			},
		},
		{
			name: "scalar-message-identity-mismatch",
			mutate: func(ctx context.Context, db *sql.DB, key string, now time.Time) error {
				_, err := db.ExecContext(ctx, `UPDATE inbound_ledger SET message_id = ?, updated_at = ? WHERE key = ?`, "message-other", now.Add(-globalInboundClaimTTL-time.Second).UnixNano(), key)
				return err
			},
		},
		{
			name: "scalar-owner-identity-mismatch",
			mutate: func(ctx context.Context, db *sql.DB, key string, now time.Time) error {
				_, err := db.ExecContext(ctx, `UPDATE inbound_ledger SET owner = ?, updated_at = ? WHERE key = ?`, "owner-other", now.Add(-globalInboundClaimTTL-time.Second).UnixNano(), key)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
			now := time.Now().UTC()
			old := now.Add(-globalInboundClaimTTL - time.Second)
			seedGlobalInboundSQLiteForPrune(t, path, func(i int) globalInboundItem {
				return globalInboundItem{
					ChatID:             "chat-ambiguous-prune",
					MessageID:          fmt.Sprintf("message-%04d", i),
					Owner:              "owner-ambiguous",
					ProcessIncarnation: "process-ambiguous",
					LeaseGeneration:    5,
					ClaimToken:         fmt.Sprintf("claim-%04d", i),
					Status:             "done",
					ClaimedAt:          old,
					UpdatedAt:          old,
				}
			})
			db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
			if err != nil {
				t.Fatalf("open ambiguous prune ledger: %v", err)
			}
			key := globalInboundKey("chat-ambiguous-prune", "message-0000")
			if err := test.mutate(ctx, db, key, now); err != nil {
				db.Close()
				t.Fatalf("mutate ambiguous row: %v", err)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				db.Close()
				t.Fatalf("begin ambiguous prune: %v", err)
			}
			if err := pruneGlobalInboundSQLiteTx(ctx, tx, now); err != nil {
				tx.Rollback()
				db.Close()
				t.Fatalf("prune ambiguous row: %v", err)
			}
			if err := tx.Commit(); err != nil {
				db.Close()
				t.Fatalf("commit ambiguous prune: %v", err)
			}
			defer db.Close()
			var count int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger`).Scan(&count); err != nil {
				t.Fatalf("count ambiguous prune ledger: %v", err)
			}
			if count != maxGlobalInboundLedgerIDs {
				t.Fatalf("safe rows were not pruned around ambiguous claim projection: count=%d want=%d", count, maxGlobalInboundLedgerIDs)
			}
			var exists int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger WHERE key = ?`, key).Scan(&exists); err != nil {
				t.Fatalf("check ambiguous claim row: %v", err)
			}
			if exists != 1 {
				t.Fatalf("ambiguous claim row disappeared: exists=%d", exists)
			}
		})
	}
}

func TestGlobalInboundSQLitePruneScansPastMalformedPrefix(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Date(2026, 9, 7, 19, 0, 0, 0, time.UTC)
	old := now.Add(-globalInboundClaimTTL - time.Hour)
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		t.Fatalf("open prefix-prune ledger: %v", err)
	}
	if err := ensureGlobalInboundSQLite(ctx, db); err != nil {
		db.Close()
		t.Fatalf("ensure prefix-prune ledger: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		db.Close()
		t.Fatalf("begin prefix-prune seed: %v", err)
	}
	const malformed = 600
	for i := 0; i < malformed; i++ {
		item := globalInboundItem{
			ChatID: "chat-prefix-prune", MessageID: fmt.Sprintf("bad-%04d", i),
			Owner: "owner", Status: "done", ClaimedAt: old, UpdatedAt: old,
		}
		if err := upsertGlobalInboundSQLiteTx(ctx, tx, globalInboundKey(item.ChatID, item.MessageID), item); err != nil {
			tx.Rollback()
			db.Close()
			t.Fatalf("seed malformed-prefix row %d: %v", i, err)
		}
	}
	for i := 0; i < maxGlobalInboundLedgerIDs; i++ {
		item := globalInboundItem{
			ChatID: "chat-prefix-prune", MessageID: fmt.Sprintf("good-%04d", i),
			Owner: "owner", Status: "done", ClaimedAt: old.Add(time.Duration(i) * time.Nanosecond), UpdatedAt: old.Add(time.Duration(i) * time.Nanosecond),
		}
		if err := upsertGlobalInboundSQLiteTx(ctx, tx, globalInboundKey(item.ChatID, item.MessageID), item); err != nil {
			tx.Rollback()
			db.Close()
			t.Fatalf("seed safe-prefix row %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		db.Close()
		t.Fatalf("commit prefix-prune seed: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE inbound_ledger SET json = ? WHERE message_id LIKE 'bad-%'`, []byte("{")); err != nil {
		db.Close()
		t.Fatalf("corrupt prefix-prune rows: %v", err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		db.Close()
		t.Fatalf("begin prefix-prune maintenance: %v", err)
	}
	if err := pruneGlobalInboundSQLiteTx(ctx, tx, now); err != nil {
		tx.Rollback()
		db.Close()
		t.Fatalf("prune past malformed prefix: %v", err)
	}
	if err := tx.Commit(); err != nil {
		db.Close()
		t.Fatalf("commit prefix-prune maintenance: %v", err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger`).Scan(&count); err != nil {
		t.Fatalf("count prefix-prune rows: %v", err)
	}
	if count != maxGlobalInboundLedgerIDs {
		t.Fatalf("prefix-prune count = %d, want bounded count %d", count, maxGlobalInboundLedgerIDs)
	}
	var good int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_ledger WHERE message_id LIKE 'good-%'`).Scan(&good); err != nil {
		t.Fatalf("count retained safe rows: %v", err)
	}
	if good != maxGlobalInboundLedgerIDs-malformed {
		t.Fatalf("safe rows retained after malformed prefix = %d, want %d", good, maxGlobalInboundLedgerIDs-malformed)
	}
}

func seedGlobalInboundSQLiteForPrune(t *testing.T, path string, itemFor func(int) globalInboundItem) {
	t.Helper()
	ctx := context.Background()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir inbound prune ledger: %v", err)
	}
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		t.Fatalf("open inbound prune ledger: %v", err)
	}
	defer db.Close()
	if err := ensureGlobalInboundSQLite(ctx, db); err != nil {
		t.Fatalf("ensure inbound prune ledger: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin inbound prune seed: %v", err)
	}
	for i := 0; i < maxGlobalInboundLedgerIDs+1; i++ {
		item := itemFor(i)
		if err := upsertGlobalInboundSQLiteTx(ctx, tx, globalInboundKey(item.ChatID, item.MessageID), item); err != nil {
			tx.Rollback()
			t.Fatalf("seed inbound prune row %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit inbound prune seed: %v", err)
	}
}

func TestGlobalInboundLedgerGenerationReclaimKeepsClaimCAS(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	now := time.Now().UTC()
	first, claimed, err := claimGlobalInboundWithIdentity(ctx, path, "chat-generation", "message-1", globalInboundClaimIdentity{
		Owner: "machine-a", ScopeID: "scope-a", ProcessIncarnation: "process-a", LeaseGeneration: 1,
	}, now, nil)
	if err != nil || !claimed {
		t.Fatalf("first generation claim = %#v claimed=%v err=%v", first, claimed, err)
	}
	second, claimed, err := claimGlobalInboundWithIdentity(ctx, path, "chat-generation", "message-1", globalInboundClaimIdentity{
		Owner: "machine-b", ScopeID: "scope-a", ProcessIncarnation: "process-b", LeaseGeneration: 2, AllowUnexpiredReclaim: true,
	}, now.Add(time.Second), nil)
	if err != nil || !claimed {
		t.Fatalf("same-scope newer generation reclaim = %#v claimed=%v err=%v", second, claimed, err)
	}
	if completed, err := completeGlobalInboundClaim(ctx, first); err != nil || completed {
		t.Fatalf("stale generation completion = completed=%v err=%v, want fenced no-op", completed, err)
	}
	if completed, err := completeGlobalInboundClaim(ctx, second); err != nil || !completed {
		t.Fatalf("new generation completion = completed=%v err=%v", completed, err)
	}

	path = filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	if _, claimed, err := claimGlobalInboundWithIdentity(ctx, path, "chat-generation", "message-2", globalInboundClaimIdentity{
		Owner: "machine-a", ScopeID: "scope-a", LeaseGeneration: 1,
	}, now, nil); err != nil || !claimed {
		t.Fatalf("cross-scope seed claim = claimed=%v err=%v", claimed, err)
	}
	if _, claimed, err := claimGlobalInboundWithIdentity(ctx, path, "chat-generation", "message-2", globalInboundClaimIdentity{
		Owner: "machine-b", ScopeID: "scope-b", LeaseGeneration: 2, AllowUnexpiredReclaim: true,
	}, now.Add(time.Second), nil); err != nil {
		t.Fatalf("cross-scope reclaim error: %v", err)
	} else if claimed {
		t.Fatal("new generation from a different scope stole a fresh claim")
	}
}

func TestGlobalInboundLedgerReleaseReportsCanceledContext(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	claim, claimed, err := claimGlobalInbound(ctx, path, "chat-cancel", "message-1", "owner-a", time.Now().UTC())
	if err != nil || !claimed {
		t.Fatalf("seed canceled release claim = %#v claimed=%v err=%v", claim, claimed, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := releaseGlobalInbound(canceled, claim); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled release error = %v, want context canceled", err)
	}
	if _, claimed, err := claimGlobalInbound(ctx, path, "chat-cancel", "message-1", "owner-b", time.Now().UTC()); err != nil || claimed {
		t.Fatalf("claim after canceled release = claimed=%v err=%v, want original claim retained", claimed, err)
	}
	if err := releaseGlobalInbound(ctx, claim); err != nil {
		t.Fatalf("cleanup release after canceled context: %v", err)
	}
	if _, claimed, err := claimGlobalInbound(ctx, path, "chat-cancel", "message-1", "owner-b", time.Now().UTC()); err != nil || !claimed {
		t.Fatalf("claim after cleanup release = claimed=%v err=%v", claimed, err)
	}
}

func TestGlobalInboundSQLiteWriterSerializesConcurrentClaimLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	writer := &globalInboundSQLiteWriter{}
	const workers = 4
	const messagesPerWorker = 12
	errCh := make(chan error, workers*messagesPerWorker*2)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for message := 0; message < messagesPerWorker; message++ {
				messageID := fmt.Sprintf("concurrent-%d-%d", worker, message)
				claim, claimed, err := claimGlobalInboundWithWriter(ctx, path, "chat-concurrent", messageID, fmt.Sprintf("owner-%d", worker), time.Now().UTC(), writer)
				if err != nil {
					errCh <- fmt.Errorf("claim %s: %w", messageID, err)
					continue
				}
				if !claimed {
					errCh <- fmt.Errorf("claim %s was unexpectedly lost", messageID)
					continue
				}
				if err := completeGlobalInbound(ctx, claim); err != nil {
					errCh <- fmt.Errorf("complete %s: %w", messageID, err)
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if err := writer.close(); err != nil {
		t.Fatalf("close shared inbound writer: %v", err)
	}
	if err := writer.close(); err != nil {
		t.Fatalf("idempotent close of shared inbound writer: %v", err)
	}
	ledger, err := readGlobalInboundLedger(path)
	if err != nil {
		t.Fatalf("read concurrent inbound ledger: %v", err)
	}
	if got, want := len(ledger.Items), workers*messagesPerWorker; got != want {
		t.Fatalf("concurrent inbound ledger items = %d, want %d", got, want)
	}
	for key, item := range ledger.Items {
		if item.Status != "done" {
			t.Fatalf("concurrent inbound item %s = %#v, want done", key, item)
		}
	}
}

func BenchmarkGlobalInboundLedgerClaim(b *testing.B) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	b.Run("sqlite-full", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), "teams", "global-inbound-ledger.json")
		for i := 0; i < maxGlobalInboundLedgerIDs; i++ {
			claim, claimed, err := claimGlobalInbound(ctx, path, "chat-1", fmt.Sprintf("seed-%06d", i), "owner-a", now.Add(time.Duration(i)*time.Second))
			if err != nil {
				b.Fatalf("seed inbound claim: %v", err)
			}
			if !claimed {
				b.Fatal("seed inbound claim lost")
			}
			if err := completeGlobalInbound(ctx, claim); err != nil {
				b.Fatalf("seed inbound complete: %v", err)
			}
		}
		b.ReportAllocs()
		beforeIO, beforeIOOK := cxpPerfReadProcSelfIO()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, claimed, err := claimGlobalInbound(ctx, path, "chat-1", fmt.Sprintf("new-%06d", i), "owner-b", now.Add(time.Duration(i)*time.Second)); err != nil {
				b.Fatalf("claim full inbound ledger: %v", err)
			} else if !claimed {
				b.Fatal("new inbound claim lost")
			}
		}
		b.StopTimer()
		cxpPerfReportProcIODelta(b, beforeIO, beforeIOOK, b.N)
	})
}
