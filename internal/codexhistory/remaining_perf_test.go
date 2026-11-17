package codexhistory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkAuditHistorySmallAppend(b *testing.B) {
	for _, count := range []int{1000, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			setBenchmarkUserCacheDir(b)
			root := b.TempDir()
			path := filepath.Join(root, "history.jsonl")
			var body strings.Builder
			for i := 0; i < count; i++ {
				fmt.Fprintf(&body, "{\"session_id\":\"seed-%d\",\"ts\":10,\"text\":\"task\"}\n", i)
			}
			if err := os.WriteFile(path, []byte(body.String()), 0600); err != nil {
				b.Fatal(err)
			}
			if _, err := loadHistoryIndexContext(context.Background(), root); err != nil {
				b.Fatal(err)
			}
			// Warm both the durable and process-local headers before timing.
			if _, err := loadHistoryIndexContext(context.Background(), root); err != nil {
				b.Fatal(err)
			}
			f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := fmt.Sprintf("tail-%d", i)
				if _, err := fmt.Fprintf(f, "{\"session_id\":\"%s\",\"ts\":11,\"text\":\"new task\"}\n", id); err != nil {
					b.Fatal(err)
				}
				idx, err := loadHistoryIndexContext(context.Background(), root)
				if err != nil {
					b.Fatal(err)
				}
				if info, ok := idx.lookup(id); !ok || info.FirstPrompt != "new task" {
					b.Fatal("missing append")
				}
			}
		})
	}
}
