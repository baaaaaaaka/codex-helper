package skills

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestStoreConcurrentUpdateKeepsAllSources(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "config"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
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
	store, err := NewStore(filepath.Join(t.TempDir(), "config"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := os.WriteFile(store.ConfigPath(), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write corrupt config: %v", err)
	}
	if _, err := store.LoadConfig(); err == nil || !strings.Contains(err.Error(), "parse skill-subscriptions.json") {
		t.Fatalf("LoadConfig corrupt error = %v", err)
	}

	store, err = NewStore(filepath.Join(t.TempDir(), "config"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := os.WriteFile(store.ConfigPath(), []byte(`{"version":99,"sources":[]}`), 0o600); err != nil {
		t.Fatalf("write unsupported config: %v", err)
	}
	if _, err := store.LoadConfig(); err == nil || !strings.Contains(err.Error(), "unsupported skill subscriptions version") {
		t.Fatalf("LoadConfig unsupported error = %v", err)
	}
	if err := os.WriteFile(store.StatePath(), []byte(`{"version":99,"sources":[]}`), 0o600); err != nil {
		t.Fatalf("write unsupported state: %v", err)
	}
	if _, err := store.LoadState(); err == nil || !strings.Contains(err.Error(), "unsupported skill subscription state version") {
		t.Fatalf("LoadState unsupported error = %v", err)
	}
}
