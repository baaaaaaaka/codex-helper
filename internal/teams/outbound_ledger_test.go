package teams

import (
	"context"
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
