package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestRuntimeMigrationPreparesBeforeReadyHookCommits(t *testing.T) {
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
	if err := prepareRuntimeMigration(store, effectivePaths{CodexDir: filepath.Join(root, "codex")}, "/codex", nil); err != nil {
		t.Fatal(err)
	}
	hook := runtimeMigrationReadyHook(store, effectivePaths{CodexDir: filepath.Join(root, "codex")}, "/codex", nil)
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

func TestRuntimeMigrationDefersLegacyCleanupUntilReadyAndPreservesConfig(t *testing.T) {
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
	proxyEnabled := true
	if err := store.Save(config.Config{ProxyEnabled: &proxyEnabled}); err != nil {
		t.Fatal(err)
	}
	history := filepath.Join(configDir, "patch_history.json")
	binary := filepath.Join(configDir, "codex-patched-dead")
	if err := os.WriteFile(history, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binary, []byte("legacy copy"), 0o700); err != nil {
		t.Fatal(err)
	}
	lease, _ := json.Marshal(map[string]any{"version": 1, "pid": 99999999, "heartbeat_unix": time.Now().Unix()})
	if err := os.WriteFile(binary+".lease", lease, 0o600); err != nil {
		t.Fatal(err)
	}
	paths := effectivePaths{CodexDir: filepath.Join(root, "codex")}
	if err := prepareRuntimeMigration(store, paths, "/codex", nil); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{history, binary, binary + ".lease"} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("prepare removed %s before activation: %v", path, err)
		}
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeGeneration != 0 {
		t.Fatalf("prepare committed generation %d before ready", cfg.RuntimeGeneration)
	}
	runtimeMigrationReadyHook(store, paths, "/codex", nil)()
	for _, path := range []string{history, binary, binary + ".lease"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("ready hook did not remove %s: %v", path, err)
		}
	}
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeGeneration != currentRuntimeGeneration || cfg.ProxyEnabled == nil || !*cfg.ProxyEnabled {
		t.Fatalf("migration did not preserve config and commit generation: %#v", cfg)
	}
}

func TestRuntimeMigrationReadyCleanupBlockerCommitsAndRetriesWithoutFallback(t *testing.T) {
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
	paths := effectivePaths{CodexDir: filepath.Join(root, "codex")}
	if err := prepareRuntimeMigration(store, paths, "/codex", nil); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(configDir, "codex-patched-became-live")
	if err := os.WriteFile(binary, []byte("legacy copy"), 0o700); err != nil {
		t.Fatal(err)
	}
	lease, _ := json.Marshal(map[string]any{"version": 1, "pid": os.Getpid(), "heartbeat_unix": time.Now().Unix()})
	if err := os.WriteFile(binary+".lease", lease, 0o600); err != nil {
		t.Fatal(err)
	}
	runtimeMigrationReadyHook(store, paths, "/codex", nil)()
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeGeneration != currentRuntimeGeneration || cfg.RuntimeMigrationID == "" || !cfg.RuntimeCleanupPending {
		t.Fatalf("activated runtime did not retain retryable cleanup state: %#v", cfg)
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("live legacy binary was removed: %v", err)
	}
	if err := os.Remove(binary); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(binary + ".lease"); err != nil {
		t.Fatal(err)
	}
	runtimeMigrationReadyHook(store, paths, "/codex", nil)()
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeCleanupPending {
		t.Fatalf("retryable cleanup marker was not cleared: %#v", cfg)
	}
}

func TestRuntimeMigrationPrepareRejectsLiveArtifact(t *testing.T) {
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
	if err := prepareRuntimeMigration(store, effectivePaths{CodexDir: filepath.Join(root, "codex")}, "/codex", nil); err == nil {
		t.Fatal("prepareRuntimeMigration unexpectedly accepted live artifact")
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeGeneration != 0 {
		t.Fatalf("runtime generation = %d, want uncommitted", cfg.RuntimeGeneration)
	}
}

func TestRuntimeMigrationPrepareRejectsCodexWithoutRemoteTUI(t *testing.T) {
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
	if err := prepareRuntimeMigration(store, effectivePaths{CodexDir: filepath.Join(root, "codex")}, "/old/codex", nil); err == nil {
		t.Fatal("prepareRuntimeMigration unexpectedly accepted unsupported Codex")
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeGeneration != 0 || cfg.RuntimeMigrationID != "" {
		t.Fatalf("unsupported runtime was committed: %#v", cfg)
	}
}
