package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestRuntimeMigrationReadyHookCommitsOnlyAfterCleanup(t *testing.T) {
	root := t.TempDir()
	previousTempDir := runtimeMigrationTempDir
	previousProbe := runtimeMigrationRemoteProbe
	runtimeMigrationTempDir = func() string { return filepath.Join(root, "tmp") }
	runtimeMigrationRemoteProbe = func(string) bool { return true }
	t.Cleanup(func() { runtimeMigrationTempDir = previousTempDir; runtimeMigrationRemoteProbe = previousProbe })
	store, err := config.NewStore(filepath.Join(root, "config", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{}); err != nil {
		t.Fatal(err)
	}
	hook := runtimeMigrationReadyHook(store, effectivePaths{CodexDir: filepath.Join(root, "codex")}, "", nil)
	hook()
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeGeneration != currentRuntimeGeneration {
		t.Fatalf("runtime generation = %d, want %d", cfg.RuntimeGeneration, currentRuntimeGeneration)
	}
	if cfg.RuntimeMigrationID == "" || cfg.RuntimeMigratedAt.IsZero() {
		t.Fatalf("migration marker is incomplete: %#v", cfg)
	}
}

func TestRuntimeMigrationReadyHookDoesNotCommitWithLiveArtifact(t *testing.T) {
	root := t.TempDir()
	previousTempDir := runtimeMigrationTempDir
	previousProbe := runtimeMigrationRemoteProbe
	runtimeMigrationTempDir = func() string { return filepath.Join(root, "tmp") }
	runtimeMigrationRemoteProbe = func(string) bool { return true }
	t.Cleanup(func() { runtimeMigrationTempDir = previousTempDir; runtimeMigrationRemoteProbe = previousProbe })
	configDir := filepath.Join(root, "config")
	store, err := config.NewStore(filepath.Join(configDir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{}); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(configDir, "codex-patched-live")
	if err := os.WriteFile(binary, []byte("copy"), 0o700); err != nil {
		t.Fatal(err)
	}
	lease, _ := json.Marshal(map[string]any{"version": 1, "pid": os.Getpid(), "heartbeat_unix": time.Now().Unix()})
	if err := os.WriteFile(binary+".lease", lease, 0o600); err != nil {
		t.Fatal(err)
	}
	hook := runtimeMigrationReadyHook(store, effectivePaths{CodexDir: filepath.Join(root, "codex")}, "", nil)
	hook()
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeGeneration != 0 {
		t.Fatalf("runtime generation = %d, want uncommitted", cfg.RuntimeGeneration)
	}
}

func TestRuntimeMigrationReadyHookDoesNotCommitWithoutRemoteTUI(t *testing.T) {
	root := t.TempDir()
	previousTempDir := runtimeMigrationTempDir
	previousProbe := runtimeMigrationRemoteProbe
	runtimeMigrationTempDir = func() string { return filepath.Join(root, "tmp") }
	runtimeMigrationRemoteProbe = func(string) bool { return false }
	t.Cleanup(func() { runtimeMigrationTempDir = previousTempDir; runtimeMigrationRemoteProbe = previousProbe })
	store, err := config.NewStore(filepath.Join(root, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{}); err != nil {
		t.Fatal(err)
	}
	runtimeMigrationReadyHook(store, effectivePaths{CodexDir: filepath.Join(root, "codex")}, "/old/codex", nil)()
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeGeneration != 0 || cfg.RuntimeMigrationID != "" {
		t.Fatalf("unsupported runtime was committed: %#v", cfg)
	}
}
