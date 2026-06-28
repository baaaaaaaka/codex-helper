package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/migration"
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
	if err := hook(); err != nil {
		t.Fatal(err)
	}
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

func TestRuntimeMigrationReadyHookFailsClosedWhenGenerationCommitFails(t *testing.T) {
	root := t.TempDir()
	store, err := config.NewStore(filepath.Join(root, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{}); err != nil {
		t.Fatal(err)
	}
	previousUpdate := runtimeMigrationStoreUpdate
	runtimeMigrationStoreUpdate = func(*config.Store, func(*config.Config) error) error {
		return errors.New("disk unavailable")
	}
	t.Cleanup(func() { runtimeMigrationStoreUpdate = previousUpdate })

	err = runtimeMigrationReadyHook(store, effectivePaths{CodexDir: filepath.Join(root, "codex")}, "/codex", nil)()
	if err == nil || !strings.Contains(err.Error(), "commit runtime migration") {
		t.Fatalf("ready hook error = %v, want durable commit failure", err)
	}
	cfg, loadErr := store.Load()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if cfg.RuntimeGeneration != 0 || cfg.RuntimeCleanupPending {
		t.Fatalf("failed commit changed activation state: %#v", cfg)
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
	if err := runtimeMigrationReadyHook(store, paths, "/codex", nil)(); err != nil {
		t.Fatal(err)
	}
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
	if err := runtimeMigrationReadyHook(store, paths, "/codex", nil)(); err != nil {
		t.Fatal(err)
	}
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
	if err := runtimeMigrationReadyHook(store, paths, "/codex", nil)(); err != nil {
		t.Fatal(err)
	}
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeCleanupPending {
		t.Fatalf("retryable cleanup marker was not cleared: %#v", cfg)
	}
}

func TestRuntimeMigrationPrepareAllowsLiveSessionPrivateBinary(t *testing.T) {
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
	var log bytes.Buffer
	paths := effectivePaths{CodexDir: filepath.Join(root, "codex")}
	if err := prepareRuntimeMigration(store, paths, "/codex", &log); err != nil {
		t.Fatalf("prepareRuntimeMigration rejected session-private binary: %v", err)
	}
	if !strings.Contains(log.String(), "1 live session-private binary file(s) deferred") {
		t.Fatalf("migration log did not explain deferred binary:\n%s", log.String())
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeGeneration != 0 {
		t.Fatalf("runtime generation = %d, want uncommitted", cfg.RuntimeGeneration)
	}
	if err := runtimeMigrationReadyHook(store, paths, "/codex", nil)(); err != nil {
		t.Fatal(err)
	}
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeGeneration != currentRuntimeGeneration || !cfg.RuntimeCleanupPending {
		t.Fatalf("live private binary should defer cleanup without blocking activation: %#v", cfg)
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("live private binary was removed: %v", err)
	}
}

func TestRuntimeMigrationPrepareReportsFreshSharedAuthLease(t *testing.T) {
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
	codexHome := filepath.Join(root, "codex")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	heartbeat := time.Now().UTC().Truncate(time.Second)
	leasePath := filepath.Join(codexHome, "auth.json.yolo-auth-lease-live")
	lease, _ := json.Marshal(map[string]any{"version": 1, "pid": os.Getpid(), "heartbeat_unix": heartbeat.Unix()})
	if err := os.WriteFile(leasePath, lease, 0o600); err != nil {
		t.Fatal(err)
	}
	err = prepareRuntimeMigration(store, effectivePaths{CodexDir: codexHome}, "/codex", nil)
	var blocked *migration.RuntimeBlockedError
	if !errors.As(err, &blocked) || len(blocked.Blockers) != 1 {
		t.Fatalf("migration error = %v, want one structured shared-auth blocker", err)
	}
	got := blocked.Blockers[0]
	if got.Kind != migration.RuntimeBlockerSharedAuthLease || got.PID != os.Getpid() || got.HeartbeatUnix != heartbeat.Unix() || got.Path != leasePath {
		t.Fatalf("blocker = %#v", got)
	}
}

func TestRuntimeMigrationPrepareReportsRecentUnverifiableSharedAuthLease(t *testing.T) {
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
	codexHome := filepath.Join(root, "codex")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	leasePath := filepath.Join(codexHome, "auth.json.yolo-auth-lease-malformed")
	if err := os.WriteFile(leasePath, []byte("incomplete legacy lease"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = prepareRuntimeMigration(store, effectivePaths{CodexDir: codexHome}, "/codex", nil)
	var blocked *migration.RuntimeBlockedError
	if !errors.As(err, &blocked) || len(blocked.Blockers) != 1 {
		t.Fatalf("migration error = %v, want one structured shared-auth blocker", err)
	}
	got := blocked.Blockers[0]
	if got.Kind != migration.RuntimeBlockerSharedAuthLease || got.PID != 0 || got.Path != leasePath || !strings.Contains(got.Reason, "no verifiable owner") {
		t.Fatalf("blocker = %#v", got)
	}
}

func TestRuntimeMigrationPrepareIgnoresStaleSharedAuthLeaseWithReusedPID(t *testing.T) {
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
	codexHome := filepath.Join(root, "codex")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-10 * time.Minute).UTC().Truncate(time.Second)
	leasePath := filepath.Join(codexHome, "auth.json.yolo-auth-lease-stale")
	lease, _ := json.Marshal(map[string]any{"version": 1, "pid": os.Getpid(), "heartbeat_unix": stale.Unix()})
	if err := os.WriteFile(leasePath, lease, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(leasePath, stale, stale); err != nil {
		t.Fatal(err)
	}
	if err := prepareRuntimeMigration(store, effectivePaths{CodexDir: codexHome}, "/codex", nil); err != nil {
		t.Fatalf("stale lease with reused live PID blocked migration: %v", err)
	}
	if _, err := os.Stat(leasePath); !os.IsNotExist(err) {
		t.Fatalf("stale lease was not retired: %v", err)
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

func TestCodexHelpHasOptionDoesNotConfuseRemoteAuthWithRemote(t *testing.T) {
	help := "Options: --remote-auth-token-env <ENV_VAR>"
	if codexHelpHasOption(help, "--remote") {
		t.Fatal("--remote-auth-token-env must not satisfy the independent --remote capability")
	}
	if !codexHelpHasOption(help, "--remote-auth-token-env") {
		t.Fatal("expected exact remote auth capability")
	}
}

func TestInstalledCodexRuntimeMigrationDefersLivePrivateArtifacts(t *testing.T) {
	if os.Getenv("CODEX_RUNTIME_CONTRACT_TEST") != "1" {
		t.Skip("set CODEX_RUNTIME_CONTRACT_TEST=1 to probe migration with an installed Codex package")
	}
	command := strings.TrimSpace(os.Getenv("CXP_CONTRACT_CODEX"))
	if command == "" {
		var err error
		command, err = exec.LookPath("codex")
		if err != nil {
			t.Fatalf("codex not found in PATH: %v", err)
		}
	}

	root := t.TempDir()
	tempDir := filepath.Join(root, "tmp")
	previousTempDir := runtimeMigrationTempDir
	runtimeMigrationTempDir = func() string { return tempDir }
	t.Cleanup(func() { runtimeMigrationTempDir = previousTempDir })

	store, err := config.NewStore(filepath.Join(root, "config", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{}); err != nil {
		t.Fatal(err)
	}
	codexHome := filepath.Join(root, "codex-home")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Dir(store.Path())
	legacyBinary := filepath.Join(configDir, "codex-patched-live-contract")
	if err := os.WriteFile(legacyBinary, []byte("session-private binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	lease, err := json.Marshal(map[string]any{
		"version":        1,
		"pid":            os.Getpid(),
		"heartbeat_unix": time.Now().UTC().Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacyBinary+".lease", lease, 0o600); err != nil {
		t.Fatal(err)
	}
	requirementsDir := filepath.Join(tempDir, "cx123abc-456d")
	if err := os.MkdirAll(requirementsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	requirementsPath := filepath.Join(requirementsDir, "reqs.toml")
	legacyRequirements := []byte(`allowed_approval_policies = ["never", "on-request", "on-failure", "untrusted"]
allowed_approval_policiez = ["never", "on-request", "on-failure", "untrusted"]
allowed_sandbox_modes = ["danger-full-access", "workspace-write", "read-only"]
allowed_sandbox_modez = ["danger-full-access", "workspace-write", "read-only"]
`)
	if err := os.WriteFile(requirementsPath, legacyRequirements, 0o600); err != nil {
		t.Fatal(err)
	}

	paths := effectivePaths{CodexDir: codexHome}
	var log bytes.Buffer
	if err := prepareRuntimeMigration(store, paths, command, &log); err != nil {
		t.Fatalf("prepare migration with installed Codex: %v", err)
	}
	if !strings.Contains(log.String(), "live session-private binary file(s) deferred") {
		t.Fatalf("prepare log did not describe deferred private runtime:\n%s", log.String())
	}
	if err := runtimeMigrationReadyHook(store, paths, command, &log)(); err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeGeneration != currentRuntimeGeneration || !cfg.RuntimeCleanupPending {
		t.Fatalf("activation state with live legacy session = %#v", cfg)
	}
	for _, path := range []string{legacyBinary, legacyBinary + ".lease", requirementsPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("live legacy artifact was removed during activation: %s: %v", path, err)
		}
	}

	if err := os.Remove(legacyBinary + ".lease"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(legacyBinary); err != nil {
		t.Fatal(err)
	}
	if err := runtimeMigrationReadyHook(store, paths, command, &log)(); err != nil {
		t.Fatal(err)
	}
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeCleanupPending {
		t.Fatalf("cleanup remained pending after legacy session exit: %#v", cfg)
	}
	if _, err := os.Stat(requirementsPath); !os.IsNotExist(err) {
		t.Fatalf("legacy requirements were not cleaned after session exit: %v", err)
	}
}
