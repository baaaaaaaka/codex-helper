package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestResolveAAAEnabledDefaultsFalse(t *testing.T) {
	if resolveAAAEnabled(config.Config{}) {
		t.Fatal("AAA must default to disabled when the preference is absent")
	}
}

func TestPersistAAAEnabledRoundTrip(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := persistAAAEnabled(store, true); err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !resolveAAAEnabled(cfg) {
		t.Fatal("AAA preference did not persist as enabled")
	}
	if err := persistAAAEnabled(store, false); err != nil {
		t.Fatal(err)
	}
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if resolveAAAEnabled(cfg) {
		t.Fatal("AAA preference did not persist as disabled")
	}
}

func TestLegacyYoloPreferenceDoesNotEnableAAA(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	legacy := []byte(`{"version":3,"minReader":1,"yoloEnabled":true,"profiles":[]}` + "\n")
	if err := os.WriteFile(path, legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if resolveAAAEnabled(cfg) {
		t.Fatal("legacy yolo preference must not opt a user into AAA")
	}
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) == "" || strings.Contains(strings.ToLower(string(raw)), "yoloenabled") || strings.Contains(strings.ToLower(string(raw)), "agentautoapproveenabled") {
		t.Fatalf("migrated default config retained an execution-mode preference: %s", raw)
	}
}
