package cli

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func runSelftest(t *testing.T, configPath string) error {
	t.Helper()
	cmd := newSelftestCmd(&rootOptions{configPath: configPath})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs(nil)
	return cmd.Execute()
}

func TestSelftestReadsValidConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.Save(config.Config{}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	if err := runSelftest(t, path); err != nil {
		t.Fatalf("selftest should pass on a readable config: %v", err)
	}
	after, err := os.ReadFile(store.Path())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("selftest changed native config: %v", err)
	}
}

func TestSelftestMissingConfigIsOK(t *testing.T) {
	// A fresh install with no config yet must not fail the readiness check.
	dir := t.TempDir()
	if err := runSelftest(t, filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("selftest should pass with no config: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("selftest created configuration state: %v, %v", entries, err)
	}
}

func TestSelftestFailsOnConfigNeedingNewerReader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	body := fmt.Sprintf(`{"version":99,"minReader":%d}`, config.SupportedReaderVersion+1)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := runSelftest(t, path); err == nil {
		t.Fatal("selftest should fail when config requires a newer reader")
	}
}
