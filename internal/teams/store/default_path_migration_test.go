package store

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/gofrs/flock"
)

func isolateDefaultPathMigrationDirs(t *testing.T) (string, string) {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("HOME", filepath.Join(tmp, "home"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(tmp, "state"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "cache"))
	configBase, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("UserConfigDir: %v", err)
	}
	statePath, err := appdirs.StatePath("teams", "state.json")
	if err != nil {
		t.Fatalf("StatePath: %v", err)
	}
	return filepath.Join(configBase, "codex-helper", "teams"), filepath.Dir(statePath)
}

func TestDefaultPathMigratesLegacyConfigStateAndSidecars(t *testing.T) {
	legacyDir, stateDir := isolateDefaultPathMigrationDirs(t)
	for name, body := range map[string]string{
		"state.json":                  `{"schema_version":5}`,
		storeSQLiteFileName:           "db",
		storeSQLiteFileName + "-wal":  "wal",
		storeSQLiteFileName + "-shm":  "shm",
		"helper-restart-pending.json": `{"pending":true}`,
		"workflow-notifications.json": `{"enabled":true}`,
		"workflow-webhook-url":        "https://workflow.example.test/hook",
	} {
		if err := os.MkdirAll(legacyDir, 0o700); err != nil {
			t.Fatalf("mkdir legacy: %v", err)
		}
		if err := os.WriteFile(filepath.Join(legacyDir, name), []byte(body), 0o600); err != nil {
			t.Fatalf("write legacy %s: %v", name, err)
		}
	}

	got, err := DefaultPath()
	if err != nil {
		t.Fatalf("DefaultPath: %v", err)
	}
	want := filepath.Join(stateDir, "state.json")
	if got != want {
		t.Fatalf("DefaultPath = %q, want %q", got, want)
	}
	for _, name := range []string{"state.json", storeSQLiteFileName, storeSQLiteFileName + "-wal", storeSQLiteFileName + "-shm", "helper-restart-pending.json", "workflow-notifications.json", "workflow-webhook-url"} {
		if _, err := os.Stat(filepath.Join(stateDir, name)); err != nil {
			t.Fatalf("migrated %s missing: %v", name, err)
		}
		if _, err := os.Stat(filepath.Join(legacyDir, name)); err != nil {
			t.Fatalf("legacy %s should remain: %v", name, err)
		}
	}
}

func TestDefaultPathDoesNotExposeStateWhenSQLiteMigrationFails(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink failure injection is POSIX-specific")
	}
	legacyDir, stateDir := isolateDefaultPathMigrationDirs(t)
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "state.json"), []byte(`{"schema_version":5}`), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	if err := os.Symlink(filepath.Join(legacyDir, "missing-db"), filepath.Join(legacyDir, storeSQLiteFileName)); err != nil {
		t.Fatalf("symlink legacy sqlite: %v", err)
	}

	got, err := DefaultPath()
	if err != nil {
		t.Fatalf("DefaultPath: %v", err)
	}
	wantLegacy := filepath.Join(legacyDir, "state.json")
	if got != wantLegacy {
		t.Fatalf("DefaultPath = %q, want legacy fallback %q", got, wantLegacy)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "state.json")); !os.IsNotExist(err) {
		t.Fatalf("new state should not be exposed after sqlite failure, stat err = %v", err)
	}
}

func TestDefaultPathFallsBackToLegacyWhenLegacyLockHeld(t *testing.T) {
	legacyDir, stateDir := isolateDefaultPathMigrationDirs(t)
	legacyPath := filepath.Join(legacyDir, "state.json")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"schema_version":5}`), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	lock := flock.New(legacyPath + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock legacy state: %v", err)
	}

	got, err := DefaultPath()
	if err != nil {
		t.Fatalf("DefaultPath with held lock: %v", err)
	}
	if got != legacyPath {
		t.Fatalf("DefaultPath with held lock = %q, want legacy %q", got, legacyPath)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "state.json")); !os.IsNotExist(err) {
		t.Fatalf("new state should not be exposed while lock is held, stat err = %v", err)
	}
	if err := lock.Unlock(); err != nil {
		t.Fatalf("unlock legacy state: %v", err)
	}

	got, err = DefaultPath()
	if err != nil {
		t.Fatalf("DefaultPath after unlock: %v", err)
	}
	want := filepath.Join(stateDir, "state.json")
	if got != want {
		t.Fatalf("DefaultPath after unlock = %q, want %q", got, want)
	}
	assertStoreFileContent(t, want, `{"schema_version":5}`)
}

func TestDefaultPathRefreshesPartialNewStateBeforePromotion(t *testing.T) {
	legacyDir, stateDir := isolateDefaultPathMigrationDirs(t)
	legacyPath := filepath.Join(legacyDir, "state.json")
	newPath := filepath.Join(stateDir, "state.json")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("mkdir state: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"schema_version":5,"sessions":{"fresh":{"id":"fresh"}}}`), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"schema_version":5,"sessions":{"stale":{"id":"stale"}}}`), 0o600); err != nil {
		t.Fatalf("write partial new state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "workflow-notifications.json"), []byte(`{"enabled":true}`), 0o600); err != nil {
		t.Fatalf("write legacy sidecar: %v", err)
	}

	got, err := DefaultPath()
	if err != nil {
		t.Fatalf("DefaultPath: %v", err)
	}
	if got != newPath {
		t.Fatalf("DefaultPath = %q, want %q", got, newPath)
	}
	assertStoreFileContent(t, newPath, `{"schema_version":5,"sessions":{"fresh":{"id":"fresh"}}}`)
	assertStoreFileContent(t, filepath.Join(stateDir, "workflow-notifications.json"), `{"enabled":true}`)
}

func TestDefaultPathRefreshesCorruptNewStateBeforePromotionCI(t *testing.T) {
	legacyDir, stateDir := isolateDefaultPathMigrationDirs(t)
	legacyPath := filepath.Join(legacyDir, "state.json")
	newPath := filepath.Join(stateDir, "state.json")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("mkdir state: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"schema_version":5,"sessions":{"fresh":{"id":"fresh"}}}`), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"schema_version":`), 0o600); err != nil {
		t.Fatalf("write corrupt new state: %v", err)
	}

	got, err := DefaultPath()
	if err != nil {
		t.Fatalf("DefaultPath: %v", err)
	}
	if got != newPath {
		t.Fatalf("DefaultPath = %q, want %q", got, newPath)
	}
	assertStoreFileContent(t, newPath, `{"schema_version":5,"sessions":{"fresh":{"id":"fresh"}}}`)
}

func TestDefaultPathRefreshesStaleLoadableNewStateBeforePromotionCI(t *testing.T) {
	legacyDir, stateDir := isolateDefaultPathMigrationDirs(t)
	legacyPath := filepath.Join(legacyDir, "state.json")
	newPath := filepath.Join(stateDir, "state.json")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("mkdir state: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"schema_version":5,"sessions":{"fresh":{"id":"fresh"}}}`), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"schema_version":5,"sessions":{"stale":{"id":"stale"}}}`), 0o600); err != nil {
		t.Fatalf("write stale new state: %v", err)
	}
	if err := os.Chtimes(newPath, time.Unix(100, 0), time.Unix(100, 0)); err != nil {
		t.Fatalf("chtimes new: %v", err)
	}
	if err := os.Chtimes(legacyPath, time.Unix(200, 0), time.Unix(200, 0)); err != nil {
		t.Fatalf("chtimes legacy: %v", err)
	}

	got, err := DefaultPath()
	if err != nil {
		t.Fatalf("DefaultPath: %v", err)
	}
	if got != newPath {
		t.Fatalf("DefaultPath = %q, want %q", got, newPath)
	}
	assertStoreFileContent(t, newPath, `{"schema_version":5,"sessions":{"fresh":{"id":"fresh"}}}`)
}

func TestDefaultPathRefreshesEqualMTimeDivergentStateBeforePromotionCI(t *testing.T) {
	legacyDir, stateDir := isolateDefaultPathMigrationDirs(t)
	legacyPath := filepath.Join(legacyDir, "state.json")
	newPath := filepath.Join(stateDir, "state.json")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("mkdir state: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"schema_version":5,"sessions":{"fresh":{"id":"fresh"}}}`), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"schema_version":5,"sessions":{"stale":{"id":"stale"}}}`), 0o600); err != nil {
		t.Fatalf("write stale new state: %v", err)
	}
	sameTime := time.Unix(200, 0)
	for _, path := range []string{legacyPath, newPath} {
		if err := os.Chtimes(path, sameTime, sameTime); err != nil {
			t.Fatalf("chtimes %s: %v", path, err)
		}
	}

	got, err := DefaultPath()
	if err != nil {
		t.Fatalf("DefaultPath: %v", err)
	}
	if got != newPath {
		t.Fatalf("DefaultPath = %q, want %q", got, newPath)
	}
	assertStoreFileContent(t, newPath, `{"schema_version":5,"sessions":{"fresh":{"id":"fresh"}}}`)
}

func assertStoreFileContent(t *testing.T, path string, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(got) != want {
		t.Fatalf("content for %s = %q, want %q", path, got, want)
	}
}
