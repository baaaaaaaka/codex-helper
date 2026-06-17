package beacon

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
)

func TestDefaultStorePathMigratesLegacyStateWhenNewDirExists(t *testing.T) {
	tmp := t.TempDir()
	legacyPath, newPath := isolateBeaconStoreDirsForTest(t, tmp)

	newDir := filepath.Dir(newPath)
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(newDir, 0o700); err != nil {
		t.Fatalf("mkdir new empty: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}

	got, err := DefaultStorePath()
	if err != nil {
		t.Fatalf("DefaultStorePath: %v", err)
	}
	if got != newPath {
		t.Fatalf("DefaultStorePath = %q, want %q", got, newPath)
	}
	if data, err := os.ReadFile(newPath); err != nil || string(data) != `{"version":1}` {
		t.Fatalf("migrated state = %q err=%v", data, err)
	}
}

func TestDefaultStorePathRefreshesStaleMigratedStateCI(t *testing.T) {
	tmp := t.TempDir()
	legacyPath, newPath := isolateBeaconStoreDirsForTest(t, tmp)
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"version":1,"profiles":{"fresh":{"name":"fresh"}}}`), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"version":1,"profiles":{"stale":{"name":"stale"}}}`), 0o600); err != nil {
		t.Fatalf("write stale new state: %v", err)
	}
	if err := os.Chtimes(newPath, time.Unix(100, 0), time.Unix(100, 0)); err != nil {
		t.Fatalf("chtimes new state: %v", err)
	}
	if err := os.Chtimes(legacyPath, time.Unix(200, 0), time.Unix(200, 0)); err != nil {
		t.Fatalf("chtimes legacy state: %v", err)
	}

	got, err := DefaultStorePath()
	if err != nil {
		t.Fatalf("DefaultStorePath: %v", err)
	}
	if got != newPath {
		t.Fatalf("DefaultStorePath = %q, want %q", got, newPath)
	}
	if data, err := os.ReadFile(newPath); err != nil || string(data) != `{"version":1,"profiles":{"fresh":{"name":"fresh"}}}` {
		t.Fatalf("refreshed state = %q err=%v", data, err)
	}
}

func TestDefaultStorePathRefreshesCorruptNewStateCI(t *testing.T) {
	tmp := t.TempDir()
	legacyPath, newPath := isolateBeaconStoreDirsForTest(t, tmp)
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"version":1,"profiles":{"fresh":{"name":"fresh"}}}`), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"version":`), 0o600); err != nil {
		t.Fatalf("write corrupt new state: %v", err)
	}

	got, err := DefaultStorePath()
	if err != nil {
		t.Fatalf("DefaultStorePath: %v", err)
	}
	if got != newPath {
		t.Fatalf("DefaultStorePath = %q, want %q", got, newPath)
	}
	if data, err := os.ReadFile(newPath); err != nil || string(data) != `{"version":1,"profiles":{"fresh":{"name":"fresh"}}}` {
		t.Fatalf("refreshed corrupt state = %q err=%v", data, err)
	}
}

func TestDefaultStorePathRefreshesEqualMTimeDivergentStateCI(t *testing.T) {
	tmp := t.TempDir()
	legacyPath, newPath := isolateBeaconStoreDirsForTest(t, tmp)
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"version":1,"profiles":{"fresh":{"name":"fresh"}}}`), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"version":1,"profiles":{"stale":{"name":"stale"}}}`), 0o600); err != nil {
		t.Fatalf("write stale new state: %v", err)
	}
	sameTime := time.Unix(200, 0)
	for _, path := range []string{legacyPath, newPath} {
		if err := os.Chtimes(path, sameTime, sameTime); err != nil {
			t.Fatalf("chtimes %s: %v", path, err)
		}
	}

	got, err := DefaultStorePath()
	if err != nil {
		t.Fatalf("DefaultStorePath: %v", err)
	}
	if got != newPath {
		t.Fatalf("DefaultStorePath = %q, want %q", got, newPath)
	}
	if data, err := os.ReadFile(newPath); err != nil || string(data) != `{"version":1,"profiles":{"fresh":{"name":"fresh"}}}` {
		t.Fatalf("equal-mtime refreshed state = %q err=%v", data, err)
	}
}

func TestDefaultStorePathSubprocessMigrationStressCI(t *testing.T) {
	if os.Getenv("CODEX_HELPER_BEACON_MIGRATION_WORKER") == "1" {
		t.Skip("parent stress only")
	}
	tmp := t.TempDir()
	legacyPath, newPath := isolateBeaconStoreDirsForTest(t, tmp)
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"version":1,"profiles":{"worker":{"name":"worker"}}}`), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}

	type proc struct {
		cmd *exec.Cmd
		out bytes.Buffer
	}
	procs := make([]proc, 6)
	for i := range procs {
		cmd := exec.Command(os.Args[0], "-test.run=TestDefaultStorePathSubprocessMigrationWorkerCI", "-test.v")
		cmd.Env = append(os.Environ(), "CODEX_HELPER_BEACON_MIGRATION_WORKER=1")
		cmd.Stdout = &procs[i].out
		cmd.Stderr = &procs[i].out
		procs[i].cmd = cmd
		if err := cmd.Start(); err != nil {
			t.Fatalf("start worker %d: %v", i, err)
		}
	}
	for i := range procs {
		if err := procs[i].cmd.Wait(); err != nil {
			t.Fatalf("worker %d failed: %v\n%s", i, err, procs[i].out.String())
		}
	}
	if data, err := os.ReadFile(newPath); err != nil || !strings.Contains(string(data), `"worker"`) {
		t.Fatalf("migrated beacon state = %q err=%v", data, err)
	}
}

func TestDefaultStorePathSubprocessMigrationWorkerCI(t *testing.T) {
	if os.Getenv("CODEX_HELPER_BEACON_MIGRATION_WORKER") != "1" {
		t.Skip("subprocess worker only")
	}
	got, err := DefaultStorePath()
	if err != nil {
		t.Fatalf("DefaultStorePath: %v", err)
	}
	if !strings.HasSuffix(filepath.ToSlash(got), "/state/codex-helper/beacon/state.json") {
		t.Fatalf("DefaultStorePath = %q, want state beacon path", got)
	}
	data, err := os.ReadFile(got)
	if err != nil {
		t.Fatalf("read migrated beacon state: %v", err)
	}
	if !strings.Contains(string(data), `"worker"`) {
		t.Fatalf("migrated beacon state = %q, want worker profile", data)
	}
}

func isolateBeaconStoreDirsForTest(t *testing.T, tmp string) (string, string) {
	t.Helper()
	home := filepath.Join(tmp, "home")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "cache"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(tmp, "state"))
	t.Setenv("LOCALAPPDATA", filepath.Join(tmp, "localappdata"))
	t.Setenv("APPDATA", filepath.Join(tmp, "appdata"))
	t.Setenv("CODEX_HELPER_BEACON_STORE", "")

	legacyPath, err := appdirs.LegacyCachePath("beacon", "state.json")
	if err != nil {
		t.Fatalf("legacy beacon path: %v", err)
	}
	newPath, err := appdirs.StatePath("beacon", "state.json")
	if err != nil {
		t.Fatalf("state beacon path: %v", err)
	}
	return legacyPath, newPath
}
