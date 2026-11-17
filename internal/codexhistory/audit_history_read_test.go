package codexhistory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Audit probe for a cache hit on a completely unchanged history source.
func BenchmarkAuditUnchangedHistory(b *testing.B) {
	for _, count := range []int{1000, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			setBenchmarkUserCacheDir(b)
			root := b.TempDir()
			var body strings.Builder
			for i := 0; i < count; i++ {
				fmt.Fprintf(&body, "{\"session_id\":\"audit-%d\",\"ts\":1770777540,\"text\":\"task\"}\n", i)
			}
			if err := os.WriteFile(filepath.Join(root, "history.jsonl"), []byte(body.String()), 0600); err != nil {
				b.Fatal(err)
			}
			if _, err := loadHistoryIndexContext(context.Background(), root); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				idx, err := loadHistoryIndexContext(context.Background(), root)
				if err != nil || idx.len() != count {
					b.Fatalf("history len=%d err=%v", idx.len(), err)
				}
			}
		})
	}
}
