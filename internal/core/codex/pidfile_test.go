package codex

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Review M3: concurrent PID file writes never leave a corrupt file or a
// stray temp file (they used to share one ".tmp" file and could interleave).
func TestConcurrentPIDFileWritesStayValid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pids.json")
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recs := make([]pidRecord, i%7)
			for k := range recs {
				recs[k] = pidRecord{PID: 1000 + k, Key: strings.Repeat("k", i)}
			}
			data, _ := json.Marshal(recs)
			if err := writeFileAtomic(path, data); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var recs []pidRecord
	if err := json.Unmarshal(data, &recs); err != nil {
		t.Fatalf("PID file corrupt after concurrent writes: %v: %q", err, data)
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) != 1 {
		var names []string
		for _, e := range ents {
			names = append(names, e.Name())
		}
		t.Fatalf("files left: %v", names)
	}
	p := NewPool(PoolConfig{PIDFile: path})
	defer p.Close(0)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); p.writePIDs() }()
	}
	wg.Wait()
	if data, _ := os.ReadFile(path); string(data) != "[]" {
		t.Fatalf("PID file of an empty pool: %q", data)
	}
}
