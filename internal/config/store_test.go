package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStore_LoadMissingReturnsDefault(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Version != CurrentVersion {
		t.Fatalf("Version=%d want %d", cfg.Version, CurrentVersion)
	}
}

func TestStore_SaveAndLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	in := Config{
		Version: CurrentVersion,
		Profiles: []Profile{
			{ID: "p1", Name: "n1", Host: "h", Port: 22, User: "u", CreatedAt: now},
		},
		DefaultModelProfile: "deepseek-work",
		ModelProfiles: map[string]ModelProfile{
			"deepseek-work": {
				Provider:  "deepseek",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				SSHProxy:  "n1",
				Revision:  2,
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
	}

	if err := store.Save(in); err != nil {
		t.Fatalf("Save: %v", err)
	}

	out, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if out.Version != CurrentVersion {
		t.Fatalf("Version=%d want %d", out.Version, CurrentVersion)
	}
	if len(out.Profiles) != 1 || out.Profiles[0].ID != "p1" {
		t.Fatalf("Profiles=%#v", out.Profiles)
	}
	if out.DefaultModelProfile != "deepseek-work" {
		t.Fatalf("DefaultModelProfile=%q", out.DefaultModelProfile)
	}
	if got := out.ModelProfiles["deepseek-work"]; got.Provider != "deepseek" || got.APIKeyRef != "env:DEEPSEEK_API_KEY" || got.Revision != 2 {
		t.Fatalf("ModelProfiles round trip failed: %#v", out.ModelProfiles)
	}
}

func TestStore_LoadMigratesVersionOneConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"profiles":[{"id":"p1","name":"n1","host":"h","port":22,"user":"u","createdAt":"2026-05-31T00:00:00Z"}]}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Version != CurrentVersion {
		t.Fatalf("Version=%d want %d", cfg.Version, CurrentVersion)
	}
	if len(cfg.Profiles) != 1 || cfg.Profiles[0].Name != "n1" {
		t.Fatalf("Profiles=%#v", cfg.Profiles)
	}
	if cfg.DefaultModelProfile != "" || len(cfg.ModelProfiles) != 0 {
		t.Fatalf("version 1 migration should not synthesize model profiles: default=%q profiles=%#v", cfg.DefaultModelProfile, cfg.ModelProfiles)
	}
	if got, ok := cfg.FindModelProfile(""); !ok || got.SSHProxy != "" {
		t.Fatalf("built-in default model profile should not inherit an ssh proxy: ok=%v profile=%#v", ok, got)
	}
}

func TestStore_LoadAcceptsNewerAdditiveConfig(t *testing.T) {
	// A future binary wrote a higher generation with only additive changes
	// (reader floor unchanged). An older binary must read it, not die — and the
	// unknown field must be ignored without error.
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	body := fmt.Sprintf(`{"version":%d,"minReader":%d,"profiles":[{"id":"p1","name":"n1"}],"someFutureField":{"x":1}}`,
		CurrentVersion+1, MinReaderVersion)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Profiles) != 1 || cfg.Profiles[0].ID != "p1" {
		t.Fatalf("Profiles=%#v", cfg.Profiles)
	}
	// The newer write-generation stamp is preserved (not downgraded on read).
	if cfg.Version != CurrentVersion+1 {
		t.Fatalf("Version=%d want %d", cfg.Version, CurrentVersion+1)
	}
}

func TestStore_SaveStampsReaderFloor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(Config{}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Version != CurrentVersion {
		t.Fatalf("Version=%d want %d", cfg.Version, CurrentVersion)
	}
	if cfg.MinReader != MinReaderVersion {
		t.Fatalf("MinReader=%d want %d", cfg.MinReader, MinReaderVersion)
	}
}

func TestStoreSaveCommitsApprovalBrokerGeneration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(Config{}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	var raw struct {
		Version   int `json:"version"`
		MinReader int `json:"minReader"`
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	if raw.Version != CurrentVersion {
		t.Fatalf("Version=%d want %d", raw.Version, CurrentVersion)
	}
	if raw.MinReader != MinReaderVersion {
		t.Fatalf("MinReader=%d want %d", raw.MinReader, MinReaderVersion)
	}
}

func TestStoreMigrationDropsLegacyExecutionModeField(t *testing.T) {
	for _, legacyField := range []string{`,"yoloEnabled":true`, `,"yoloEnabled":false`, ``} {
		name := "missing"
		if strings.Contains(legacyField, "true") {
			name = "true"
		} else if strings.Contains(legacyField, "false") {
			name = "false"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.json")
			document := `{"version":2,"minReader":1,"proxyEnabled":true,"profiles":[]` + legacyField + `}`
			if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := NewStore(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Save(cfg); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(raw, []byte("yoloEnabled")) {
				t.Fatalf("legacy field survived migration: %s", raw)
			}
			var header struct {
				Version   int `json:"version"`
				MinReader int `json:"minReader"`
			}
			if err := json.Unmarshal(raw, &header); err != nil {
				t.Fatal(err)
			}
			if header.Version != 3 || header.MinReader != 1 {
				t.Fatalf("migration header = %#v, want generation 3 readable by generation-1 readers", header)
			}
		})
	}
}

func TestDefaultPathForHome(t *testing.T) {
	home := filepath.Join(string(filepath.Separator), "tmp", "test-home")

	got, err := DefaultPathForHome(home)
	if err != nil {
		t.Fatalf("DefaultPathForHome: %v", err)
	}

	want := filepath.Join(home, ".config", "codex-proxy", "config.json")
	switch runtime.GOOS {
	case "windows":
		want = filepath.Join(home, "AppData", "Roaming", "codex-proxy", "config.json")
	case "darwin":
		want = filepath.Join(home, "Library", "Application Support", "codex-proxy", "config.json")
	}
	if got != want {
		t.Fatalf("DefaultPathForHome = %q, want %q", got, want)
	}
}

func TestStore_UpdateIsSerialized(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	const n = 25
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			errCh <- store.Update(func(cfg *Config) error {
				cfg.UpsertProfile(Profile{
					ID:        fmt.Sprintf("p%02d", i),
					Name:      "n",
					Host:      "h",
					Port:      22,
					User:      "u",
					CreatedAt: time.Now(),
				})
				return nil
			})
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
	}

	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Profiles) != n {
		t.Fatalf("Profiles len=%d want %d", len(cfg.Profiles), n)
	}
}

func TestStore_ErrorPaths(t *testing.T) {
	t.Run("Load rejects invalid JSON", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		store, err := NewStore(path)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		if _, err := store.Load(); err == nil {
			t.Fatalf("expected parse error")
		}
	})

	t.Run("Load rejects corrupt negative version", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		if err := os.WriteFile(path, []byte(`{"version":-1}`), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		store, err := NewStore(path)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		if _, err := store.Load(); err == nil {
			t.Fatalf("expected corrupt-config error for negative version")
		}
	})

	t.Run("Load rejects config requiring a newer reader", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		if err := os.WriteFile(path, []byte(fmt.Sprintf(`{"version":999,"minReader":%d}`, SupportedReaderVersion+1)), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		store, err := NewStore(path)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		_, err = store.Load()
		if !errors.Is(err, ErrStaleReader) {
			t.Fatalf("expected ErrStaleReader, got %v", err)
		}
	})

	t.Run("Save refuses to clobber a newer-generation file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		if err := os.WriteFile(path, []byte(fmt.Sprintf(`{"version":%d}`, CurrentVersion+1)), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		store, err := NewStore(path)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		if err := store.Save(Config{}); err == nil {
			t.Fatalf("expected refuse-to-overwrite error")
		}
	})

	t.Run("Update propagates callback error", func(t *testing.T) {
		dir := t.TempDir()
		store, err := NewStore(filepath.Join(dir, "config.json"))
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		if err := store.Update(func(cfg *Config) error {
			cfg.Version = CurrentVersion
			return fmt.Errorf("boom")
		}); err == nil {
			t.Fatalf("expected callback error")
		}
	})
}

func TestNewStoreDefaultPath(t *testing.T) {
	dir := t.TempDir()
	switch runtime.GOOS {
	case "windows":
		t.Setenv("APPDATA", dir)
	case "darwin":
		t.Setenv("HOME", dir)
	default:
		t.Setenv("XDG_CONFIG_HOME", dir)
	}
	base, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("UserConfigDir error: %v", err)
	}
	store, err := NewStore("")
	if err != nil {
		t.Fatalf("NewStore error: %v", err)
	}
	want := filepath.Join(base, "codex-proxy", "config.json")
	if store.Path() != want {
		t.Fatalf("expected path %q, got %q", want, store.Path())
	}
}
