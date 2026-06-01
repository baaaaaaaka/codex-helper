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
	if err := store.Save(config.Config{}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := runSelftest(t, path); err != nil {
		t.Fatalf("selftest should pass on a readable config: %v", err)
	}
}

func TestSelftestMissingConfigIsOK(t *testing.T) {
	// A fresh install with no config yet must not fail the readiness check.
	dir := t.TempDir()
	if err := runSelftest(t, filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("selftest should pass with no config: %v", err)
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
