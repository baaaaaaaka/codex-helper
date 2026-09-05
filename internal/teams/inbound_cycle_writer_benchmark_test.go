package teams

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkGlobalInboundLedgerClaimCycleWriter(b *testing.B) {
	ctx := context.Background()
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(b.TempDir(), "teams", "global-inbound-ledger.json")
	writer := &globalInboundSQLiteWriter{}
	b.Cleanup(func() { _ = writer.close() })
	for i := 0; i < 32; i++ {
		claim, claimed, err := claimGlobalInboundWithWriter(ctx, path, "chat-1", fmt.Sprintf("seed-%02d", i), "owner-a", now.Add(time.Duration(i)*time.Second), writer)
		if err != nil || !claimed {
			b.Fatalf("seed inbound claim %d: claimed=%v err=%v", i, claimed, err)
		}
		if err := completeGlobalInbound(ctx, claim); err != nil {
			b.Fatalf("seed inbound complete %d: %v", i, err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		claim, claimed, err := claimGlobalInboundWithWriter(ctx, path, "chat-1", fmt.Sprintf("new-%06d", i), "owner-b", now.Add(time.Duration(i)*time.Second), writer)
		if err != nil {
			b.Fatalf("steady-state inbound claim: %v", err)
		}
		if !claimed {
			b.Fatal("steady-state inbound claim lost")
		}
		if err := completeGlobalInbound(ctx, claim); err != nil {
			b.Fatalf("steady-state inbound completion: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(1, "claim-cycles/op")
}
