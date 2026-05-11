package teams

import (
	"context"
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
