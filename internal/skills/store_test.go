package skills

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestStoreConcurrentUpdateKeepsAllSources(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "config"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	const n = 16
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			source := Source{
				ID:         fmt.Sprintf("source-%02d", i),
				Name:       fmt.Sprintf("source-%02d", i),
				RemoteURL:  fmt.Sprintf("repo-%02d", i),
				TargetKind: TargetCodexHome,
			}
			if err := store.Update(func(cfg *Config, st *State) error {
				cfg.Sources = append(cfg.Sources, source)
				st.Sources = append(st.Sources, SourceState{ID: source.ID, Status: StatusReady})
				return nil
			}); err != nil {
				t.Errorf("update %d: %v", i, err)
			}
		}()
	}
	wg.Wait()
	cfg, err := store.LoadConfig()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Sources) != n {
		t.Fatalf("sources len = %d, want %d", len(cfg.Sources), n)
	}
	st, err := store.LoadState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(st.Sources) != n {
		t.Fatalf("states len = %d, want %d", len(st.Sources), n)
	}
}

func TestStoreRejectsCorruptJSONAndUnsupportedVersions(t *testing.T) {
	for _, tc := range []struct{ name, file, input string }{
		{"corrupt", ConfigFilename, "{not json"},
		{"future-config", ConfigFilename, `{"version":99,"sources":[]}`},
		{"future-state", StateFilename, `{"version":99,"sources":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, tc.file), []byte(tc.input), 0600); err != nil {
				t.Fatal(err)
			}
			if db, err := NewStore(dir); err == nil {
				db.Close()
				t.Fatal("accepted invalid legacy input")
			}
		})
	}
}
