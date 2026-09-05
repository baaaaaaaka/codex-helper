//go:build linux

package teams

import (
	"fmt"
	"path/filepath"
	"testing"
)

// BenchmarkHistoryWatchTrustedCleanHotSet measures the steady-state event
// selection path. BenchmarkHistoryTieredStatHotSetUnchanged intentionally
// remains as the complete-scan fallback benchmark; this one is the production
// path after the watcher has been registered and no filesystem event has been
// observed. It must not stat or parse every transcript file.
func BenchmarkHistoryWatchTrustedCleanHotSet(b *testing.B) {
	dir := b.TempDir()
	const files = 5000
	paths := make([]string, 0, files)
	for i := 0; i < files; i++ {
		path := filepath.Join(dir, fmt.Sprintf("session-%05d.jsonl", i))
		writeSmallFile(b, path)
		paths = append(paths, path)
	}

	source := newHistoryWatchEventSource()
	b.Cleanup(func() { _ = source.Close() })
	if _, uncertain, err := source.Update(paths, true); err != nil {
		b.Fatalf("initial watcher registration: %v", err)
	} else if !uncertain {
		b.Fatal("initial watcher registration was trusted")
	}
	if dirty, uncertain, err := source.Update(nil, false); err != nil {
		b.Fatalf("settle watcher: %v", err)
	} else if uncertain || len(dirty) != 0 {
		b.Fatalf("settled watcher dirty=%d uncertain=%t", len(dirty), uncertain)
	}

	b.ReportMetric(float64(files), "paths")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dirty, uncertain, err := source.Update(nil, false)
		if err != nil {
			b.Fatalf("trusted watcher update: %v", err)
		}
		if uncertain || len(dirty) != 0 {
			b.Fatalf("trusted watcher dirty=%d uncertain=%t", len(dirty), uncertain)
		}
	}
}
