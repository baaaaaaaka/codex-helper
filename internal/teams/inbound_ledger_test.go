package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
