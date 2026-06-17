package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestGlobalOutboundLedgerPathForRegistryAndStore(t *testing.T) {
	tmp := t.TempDir()
	scopedRegistry := filepath.Join(tmp, "teams", "scopes", "scope-a", "registry.json")
	got, ok := globalOutboundLedgerPathForRegistry(scopedRegistry)
	if !ok {
		t.Fatal("scoped registry should enable global outbound ledger")
	}
	want := filepath.Join(tmp, "teams", "global-outbound-ledger.json")
	if got != want {
		t.Fatalf("scoped registry outbound ledger path = %q, want %q", got, want)
	}

	scopedStore := filepath.Join(tmp, "teams", "scopes", "scope-a", "state.json")
	got, ok = globalOutboundLedgerPathForStore(scopedStore)
	if !ok {
		t.Fatal("scoped store should enable global outbound ledger")
	}
	if got != want {
		t.Fatalf("scoped store outbound ledger path = %q, want %q", got, want)
	}

	plainRegistry := filepath.Join(tmp, "profile", "registry.json")
	got, ok = globalOutboundLedgerPathForRegistry(plainRegistry)
	if !ok {
		t.Fatal("plain registry should enable global outbound ledger")
	}
	want = filepath.Join(tmp, "profile", "teams-global-outbound-ledger.json")
	if got != want {
		t.Fatalf("plain outbound ledger path = %q, want %q", got, want)
	}

	if got, ok := globalOutboundLedgerPathForRegistry(""); ok || got != "" {
		t.Fatalf("empty registry should disable global outbound ledger, got path=%q ok=%v", got, ok)
	}
}

func TestGlobalOutboundLedgerRecordLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-outbound-ledger.json")
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)

	err := recordGlobalOutbound(ctx, path, globalOutboundItem{
		ChatID:     "control-chat",
		MessageID:  "message-1",
		ScopeID:    "scope-a",
		OutboxID:   "outbox-a",
		Kind:       "control",
		Origin:     teamstore.MessageOriginHelperOutbox,
		RecordedAt: now,
	}, now)
	if err != nil {
		t.Fatalf("record global outbound error: %v", err)
	}

	err = recordGlobalOutbound(ctx, path, globalOutboundItem{
		ChatID:    "control-chat",
		MessageID: "message-1",
		ScopeID:   "scope-a",
		MachineID: "machine-a",
	}, now.Add(time.Second))
	if err != nil {
		t.Fatalf("merge global outbound error: %v", err)
	}

	ledger, err := readGlobalOutboundLedger(path)
	if err != nil {
		t.Fatalf("read global outbound ledger error: %v", err)
	}
	item, ok := ledger.Items[globalOutboundKey("control-chat", "message-1")]
	if !ok {
		t.Fatalf("global outbound ledger missing recorded item: %#v", ledger.Items)
	}
	if item.OutboxID != "outbox-a" || item.MachineID != "machine-a" || item.Origin != teamstore.MessageOriginHelperOutbox {
		t.Fatalf("merged global outbound item = %#v", item)
	}
}

func TestGlobalOutboundLedgerMigratesLegacyJSONWithoutRewrite(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-outbound-ledger.json")
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	legacy := globalOutboundLedger{
		Version: 1,
		Items: map[string]globalOutboundItem{
			globalOutboundKey("control-chat", "legacy-message"): {
				ChatID:     "control-chat",
				MessageID:  "legacy-message",
				ScopeID:    "scope-a",
				OutboxID:   "legacy-outbox",
				Kind:       "final",
				Origin:     teamstore.MessageOriginHelperOutbox,
				RecordedAt: now.Add(-time.Hour),
				UpdatedAt:  now.Add(-time.Hour),
			},
		},
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir legacy outbound ledger: %v", err)
	}
	raw, err := json.MarshalIndent(legacy, "", "  ")
	if err != nil {
		t.Fatalf("marshal legacy outbound ledger: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write legacy outbound ledger: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read legacy outbound before: %v", err)
	}

	if err := recordGlobalOutbound(ctx, path, globalOutboundItem{
		ChatID:     "control-chat",
		MessageID:  "new-message",
		ScopeID:    "scope-a",
		OutboxID:   "new-outbox",
		Kind:       "progress",
		Origin:     teamstore.MessageOriginHelperOutbox,
		RecordedAt: now,
	}, now); err != nil {
		t.Fatalf("record migrated outbound ledger: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read legacy outbound after: %v", err)
	}
	if string(after) != string(before) {
		t.Fatal("recording outbound ledger rewrote legacy JSON")
	}
	if _, err := os.Stat(teamsLedgerSQLitePath(path)); err != nil {
		t.Fatalf("stat outbound sqlite sidecar: %v", err)
	}
	got, err := readGlobalOutboundLedger(path)
	if err != nil {
		t.Fatalf("read migrated outbound ledger: %v", err)
	}
	if _, ok := got.Items[globalOutboundKey("control-chat", "legacy-message")]; !ok {
		t.Fatalf("migrated ledger missing legacy item: %#v", got.Items)
	}
	if _, ok := got.Items[globalOutboundKey("control-chat", "new-message")]; !ok {
		t.Fatalf("migrated ledger missing new item: %#v", got.Items)
	}
}

func TestGlobalOutboundLedgerReadRecoversPartialSQLiteSidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "teams", "global-outbound-ledger.json")
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	legacy := globalOutboundLedger{
		Version: 1,
		Items: map[string]globalOutboundItem{
			globalOutboundKey("control-chat", "legacy-message"): {
				ChatID:     "control-chat",
				MessageID:  "legacy-message",
				ScopeID:    "scope-a",
				OutboxID:   "legacy-outbox",
				Kind:       "final",
				Origin:     teamstore.MessageOriginHelperOutbox,
				RecordedAt: now,
				UpdatedAt:  now,
			},
		},
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir legacy outbound ledger: %v", err)
	}
	raw, err := json.MarshalIndent(legacy, "", "  ")
	if err != nil {
		t.Fatalf("marshal legacy outbound ledger: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write legacy outbound ledger: %v", err)
	}
	db, err := openTeamsLedgerSQLite(teamsLedgerSQLitePath(path))
	if err != nil {
		t.Fatalf("open partial outbound sqlite sidecar: %v", err)
	}
	if err := ensureGlobalOutboundSQLite(context.Background(), db); err != nil {
		_ = db.Close()
		t.Fatalf("create partial outbound sqlite sidecar: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close partial outbound sqlite sidecar: %v", err)
	}

	got, err := readGlobalOutboundLedger(path)
	if err != nil {
		t.Fatalf("read partial outbound sqlite sidecar: %v", err)
	}
	if _, ok := got.Items[globalOutboundKey("control-chat", "legacy-message")]; !ok {
		t.Fatalf("partial outbound sqlite read did not recover legacy item: %#v", got.Items)
	}
}

func BenchmarkGlobalOutboundLedgerRecord(b *testing.B) {
	ctx := context.Background()
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	itemFor := func(i int) globalOutboundItem {
		return globalOutboundItem{
			ChatID:     "chat-1",
			MessageID:  fmt.Sprintf("message-%06d", i),
			ScopeID:    "scope-a",
			MachineID:  "machine-a",
			OutboxID:   fmt.Sprintf("outbox-%06d", i),
			SessionID:  "session-a",
			TurnID:     "turn-a",
			Kind:       "helper",
			Origin:     teamstore.MessageOriginHelperOutbox,
			RecordedAt: now,
		}
	}
	b.Run("sqlite-empty", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), "teams", "global-outbound-ledger.json")
		b.ReportAllocs()
		beforeIO, beforeIOOK := cxpPerfReadProcSelfIO()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := recordGlobalOutbound(ctx, path, itemFor(i), now); err != nil {
				b.Fatalf("record SQLite global outbound: %v", err)
			}
		}
		b.StopTimer()
		cxpPerfReportProcIODelta(b, beforeIO, beforeIOOK, b.N)
	})
	b.Run("sqlite-full", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), "teams", "global-outbound-ledger.json")
		var seed []globalOutboundItem
		for i := 0; i < maxGlobalOutboundLedgerIDs; i++ {
			seed = append(seed, itemFor(i))
		}
		if err := recordGlobalOutboundBatch(ctx, path, seed, now); err != nil {
			b.Fatalf("seed full global outbound ledger: %v", err)
		}
		b.ReportAllocs()
		beforeIO, beforeIOOK := cxpPerfReadProcSelfIO()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := recordGlobalOutbound(ctx, path, itemFor(maxGlobalOutboundLedgerIDs+i), now.Add(time.Duration(i)*time.Second)); err != nil {
				b.Fatalf("record full SQLite global outbound: %v", err)
			}
		}
		b.StopTimer()
		cxpPerfReportProcIODelta(b, beforeIO, beforeIOOK, b.N)
	})
}
