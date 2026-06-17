package appdirs

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestStateDirUsesExactOverride(t *testing.T) {
	tmp := t.TempDir()
	override := filepath.Join(tmp, "custom-state")
	t.Setenv(EnvStateDir, override)
	t.Setenv("XDG_STATE_HOME", filepath.Join(tmp, "xdg-state"))

	got, err := StateDir()
	if err != nil {
		t.Fatalf("StateDir error: %v", err)
	}
	if got != override {
		t.Fatalf("StateDir = %q, want exact override %q", got, override)
	}
}

func TestStateDirUsesXDGStateHome(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv(EnvStateDir, "")
	t.Setenv("XDG_STATE_HOME", filepath.Join(tmp, "state"))

	got, err := StateDir()
	if err != nil {
		t.Fatalf("StateDir error: %v", err)
	}
	want := filepath.Join(tmp, "state", AppName)
	if got != want {
		t.Fatalf("StateDir = %q, want %q", got, want)
	}
}

func TestStateDirExpandsHomeAndPreservesSpacesCI(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", filepath.Join(tmp, "home dir"))
	t.Setenv("USERPROFILE", filepath.Join(tmp, "home dir"))
	t.Setenv(EnvStateDir, "~/custom state")
	t.Setenv("XDG_STATE_HOME", filepath.Join(tmp, "ignored"))

	got, err := StateDir()
	if err != nil {
		t.Fatalf("StateDir error: %v", err)
	}
	want := filepath.Join(tmp, "home dir", "custom state")
	if got != want {
		t.Fatalf("StateDir = %q, want expanded override %q", got, want)
	}

	t.Setenv(EnvStateDir, "")
	t.Setenv("XDG_STATE_HOME", "~/xdg state")
	got, err = StateDir()
	if err != nil {
		t.Fatalf("StateDir with XDG error: %v", err)
	}
	want = filepath.Join(tmp, "home dir", "xdg state", AppName)
	if got != want {
		t.Fatalf("StateDir with XDG = %q, want %q", got, want)
	}
}

func TestStateDirUsesLocalStateDefaultOnUnix(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		t.Skip("platform default is intentionally platform-specific")
	}
	tmp := t.TempDir()
	t.Setenv(EnvStateDir, "")
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("HOME", filepath.Join(tmp, "home"))

	got, err := StateDir()
	if err != nil {
		t.Fatalf("StateDir error: %v", err)
	}
	want := filepath.Join(tmp, "home", ".local", "state", AppName)
	if got != want {
		t.Fatalf("StateDir = %q, want %q", got, want)
	}
}

func TestStateDirUsesConfigFallbackOnWindowsAndDarwinCI(t *testing.T) {
	if runtime.GOOS != "windows" && runtime.GOOS != "darwin" {
		t.Skip("config fallback is only the platform default on Windows and macOS")
	}
	tmp := t.TempDir()
	t.Setenv(EnvStateDir, "")
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("HOME", filepath.Join(tmp, "home"))
	t.Setenv("USERPROFILE", filepath.Join(tmp, "profile"))
	t.Setenv("APPDATA", filepath.Join(tmp, "AppData", "Roaming"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "config"))

	base, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("UserConfigDir: %v", err)
	}
	got, err := StateDir()
	if err != nil {
		t.Fatalf("StateDir error: %v", err)
	}
	want := filepath.Join(base, AppName, "state")
	if got != want {
		t.Fatalf("StateDir = %q, want config fallback %q", got, want)
	}
}

func TestResolveMigratedFileCopiesLegacyAndLeavesOld(t *testing.T) {
	tmp := t.TempDir()
	legacyPath := filepath.Join(tmp, "cache", "token.json")
	newPath := filepath.Join(tmp, "state", "token.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte("legacy-token"), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}

	got, err := ResolveMigratedFile(newPath, legacyPath)
	if err != nil {
		t.Fatalf("ResolveMigratedFile error: %v", err)
	}
	if got != newPath {
		t.Fatalf("ResolveMigratedFile = %q, want %q", got, newPath)
	}
	assertFileContent(t, legacyPath, "legacy-token")
	assertFileContent(t, newPath, "legacy-token")
}

func TestResolveMigratedFileFallsBackToLegacyWhenCopyFails(t *testing.T) {
	tmp := t.TempDir()
	legacyPath := filepath.Join(tmp, "cache", "token.json")
	blockedParent := filepath.Join(tmp, "state-file")
	newPath := filepath.Join(blockedParent, "token.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte("legacy-token"), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	if err := os.WriteFile(blockedParent, []byte("not a dir"), 0o600); err != nil {
		t.Fatalf("write blocked parent: %v", err)
	}

	got, err := ResolveMigratedFile(newPath, legacyPath)
	if err != nil {
		t.Fatalf("ResolveMigratedFile error: %v", err)
	}
	if got != legacyPath {
		t.Fatalf("ResolveMigratedFile = %q, want legacy fallback %q", got, legacyPath)
	}
	assertFileContent(t, legacyPath, "legacy-token")
}

func TestResolveMigratedFileRefreshesStaleExistingNewCI(t *testing.T) {
	tmp := t.TempDir()
	legacyPath := filepath.Join(tmp, "cache", "registry.json")
	newPath := filepath.Join(tmp, "state", "registry.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"version":2}`), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write new: %v", err)
	}
	if err := os.Chtimes(newPath, time.Unix(100, 0), time.Unix(100, 0)); err != nil {
		t.Fatalf("chtimes new: %v", err)
	}
	if err := os.Chtimes(legacyPath, time.Unix(200, 0), time.Unix(200, 0)); err != nil {
		t.Fatalf("chtimes legacy: %v", err)
	}

	got, err := ResolveMigratedFile(newPath, legacyPath)
	if err != nil {
		t.Fatalf("ResolveMigratedFile error: %v", err)
	}
	if got != newPath {
		t.Fatalf("ResolveMigratedFile = %q, want %q", got, newPath)
	}
	assertFileContent(t, newPath, `{"version":2}`)
}

func TestResolveMigratedFileRefreshesEqualMTimeDifferentExistingNewCI(t *testing.T) {
	tmp := t.TempDir()
	legacyPath := filepath.Join(tmp, "cache", "registry.json")
	newPath := filepath.Join(tmp, "state", "registry.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"version":2}`), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write new: %v", err)
	}
	sameTime := time.Unix(200, 0)
	for _, path := range []string{legacyPath, newPath} {
		if err := os.Chtimes(path, sameTime, sameTime); err != nil {
			t.Fatalf("chtimes %s: %v", path, err)
		}
	}

	got, err := ResolveMigratedFile(newPath, legacyPath)
	if err != nil {
		t.Fatalf("ResolveMigratedFile error: %v", err)
	}
	if got != newPath {
		t.Fatalf("ResolveMigratedFile = %q, want %q", got, newPath)
	}
	assertFileContent(t, newPath, `{"version":2}`)
}

func TestResolveMigratedDirCopiesUnitAndSkipsLocks(t *testing.T) {
	tmp := t.TempDir()
	legacyDir := filepath.Join(tmp, "cache", "scope")
	newDir := filepath.Join(tmp, "state", "scope")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	for name, body := range map[string]string{
		"state.json":       `{"version":1}`,
		"store.sqlite-wal": "wal",
		"state.json.lock":  "",
	} {
		if err := os.WriteFile(filepath.Join(legacyDir, name), []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	got, err := ResolveMigratedDir(newDir, legacyDir)
	if err != nil {
		t.Fatalf("ResolveMigratedDir error: %v", err)
	}
	if got != newDir {
		t.Fatalf("ResolveMigratedDir = %q, want %q", got, newDir)
	}
	assertFileContent(t, filepath.Join(newDir, "state.json"), `{"version":1}`)
	assertFileContent(t, filepath.Join(newDir, "store.sqlite-wal"), "wal")
	if _, err := os.Stat(filepath.Join(newDir, "state.json.lock")); !os.IsNotExist(err) {
		t.Fatalf("lock file should not be copied, stat err = %v", err)
	}
	assertFileContent(t, filepath.Join(legacyDir, "state.json"), `{"version":1}`)
}

func TestCopyDirContentsIfMissingMergesExistingDir(t *testing.T) {
	tmp := t.TempDir()
	legacyDir := filepath.Join(tmp, "cache", "scope")
	newDir := filepath.Join(tmp, "state", "scope")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(newDir, 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "state.json"), []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(newDir, "registry.json"), []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write new registry: %v", err)
	}

	if err := CopyDirContentsIfMissing(newDir, legacyDir); err != nil {
		t.Fatalf("CopyDirContentsIfMissing error: %v", err)
	}
	assertFileContent(t, filepath.Join(newDir, "state.json"), `{"version":1}`)
	assertFileContent(t, filepath.Join(newDir, "registry.json"), `{"version":1}`)
}

func TestResolveMigratedDirWithRequiredCompletesEmptyNewDir(t *testing.T) {
	tmp := t.TempDir()
	legacyDir := filepath.Join(tmp, "cache", "runtime")
	newDir := filepath.Join(tmp, "state", "runtime")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(newDir, 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "status.json"), []byte(`{"state":"running"}`), 0o600); err != nil {
		t.Fatalf("write legacy status: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "status.json.lock"), []byte(""), 0o600); err != nil {
		t.Fatalf("write legacy lock: %v", err)
	}

	got, err := ResolveMigratedDirWithRequired(newDir, legacyDir, "status.json")
	if err != nil {
		t.Fatalf("ResolveMigratedDirWithRequired error: %v", err)
	}
	if got != newDir {
		t.Fatalf("ResolveMigratedDirWithRequired = %q, want %q", got, newDir)
	}
	assertFileContent(t, filepath.Join(newDir, "status.json"), `{"state":"running"}`)
	if _, err := os.Stat(filepath.Join(newDir, "status.json.lock")); !os.IsNotExist(err) {
		t.Fatalf("lock file should not be copied, stat err = %v", err)
	}
}

func TestResolveMigratedRelatedFilesCopiesSQLiteSidecars(t *testing.T) {
	tmp := t.TempDir()
	legacyBase := filepath.Join(tmp, "cache", "adapter.sqlite")
	newBase := filepath.Join(tmp, "state", "adapter.sqlite")
	if err := os.MkdirAll(filepath.Dir(legacyBase), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	for suffix, body := range map[string]string{
		"":     "db",
		"-wal": "wal",
		"-shm": "shm",
	} {
		if err := os.WriteFile(legacyBase+suffix, []byte(body), 0o600); err != nil {
			t.Fatalf("write sqlite%s: %v", suffix, err)
		}
	}

	got, err := ResolveMigratedRelatedFiles(newBase, legacyBase, "-wal", "-shm")
	if err != nil {
		t.Fatalf("ResolveMigratedRelatedFiles error: %v", err)
	}
	if got != newBase {
		t.Fatalf("ResolveMigratedRelatedFiles = %q, want %q", got, newBase)
	}
	assertFileContent(t, newBase, "db")
	assertFileContent(t, newBase+"-wal", "wal")
	assertFileContent(t, newBase+"-shm", "shm")
}

func TestResolveMigratedRelatedFilesCompletesPartialNewFamily(t *testing.T) {
	tmp := t.TempDir()
	legacyBase := filepath.Join(tmp, "cache", "adapter.sqlite")
	newBase := filepath.Join(tmp, "state", "adapter.sqlite")
	if err := os.MkdirAll(filepath.Dir(legacyBase), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newBase), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	for suffix, body := range map[string]string{
		"":     "db",
		"-wal": "wal",
		"-shm": "shm",
	} {
		if err := os.WriteFile(legacyBase+suffix, []byte(body), 0o600); err != nil {
			t.Fatalf("write legacy sqlite%s: %v", suffix, err)
		}
	}
	if err := os.WriteFile(newBase, []byte("db"), 0o600); err != nil {
		t.Fatalf("write partial new base: %v", err)
	}

	got, err := ResolveMigratedRelatedFiles(newBase, legacyBase, "-wal", "-shm")
	if err != nil {
		t.Fatalf("ResolveMigratedRelatedFiles error: %v", err)
	}
	if got != newBase {
		t.Fatalf("ResolveMigratedRelatedFiles = %q, want %q", got, newBase)
	}
	assertFileContent(t, newBase, "db")
	assertFileContent(t, newBase+"-wal", "wal")
	assertFileContent(t, newBase+"-shm", "shm")
}

func TestResolveMigratedDirWithRequiredRefreshesStaleRequiredFileCI(t *testing.T) {
	tmp := t.TempDir()
	legacyDir := filepath.Join(tmp, "cache", "runtime")
	newDir := filepath.Join(tmp, "state", "runtime")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(newDir, 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	legacyStatus := filepath.Join(legacyDir, "status.json")
	newStatus := filepath.Join(newDir, "status.json")
	if err := os.WriteFile(legacyStatus, []byte(`{"state":"fresh"}`), 0o600); err != nil {
		t.Fatalf("write legacy status: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "details.json"), []byte(`{"copied":true}`), 0o600); err != nil {
		t.Fatalf("write legacy details: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "status.json.lock"), nil, 0o600); err != nil {
		t.Fatalf("write legacy lock: %v", err)
	}
	if err := os.WriteFile(newStatus, []byte(`{"state":"stale"}`), 0o600); err != nil {
		t.Fatalf("write new status: %v", err)
	}
	oldTime := time.Unix(100, 0)
	newTime := time.Unix(200, 0)
	if err := os.Chtimes(newStatus, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes new status: %v", err)
	}
	if err := os.Chtimes(legacyStatus, newTime, newTime); err != nil {
		t.Fatalf("chtimes legacy status: %v", err)
	}

	got, err := ResolveMigratedDirWithRequired(newDir, legacyDir, "status.json")
	if err != nil {
		t.Fatalf("ResolveMigratedDirWithRequired error: %v", err)
	}
	if got != newDir {
		t.Fatalf("ResolveMigratedDirWithRequired = %q, want %q", got, newDir)
	}
	assertFileContent(t, newStatus, `{"state":"fresh"}`)
	assertFileContent(t, filepath.Join(newDir, "details.json"), `{"copied":true}`)
	if _, err := os.Stat(filepath.Join(newDir, "status.json.lock")); !os.IsNotExist(err) {
		t.Fatalf("lock file should not be copied, stat err = %v", err)
	}
}

func TestResolveMigratedDirWithRequiredKeepsNewerRequiredFileCI(t *testing.T) {
	tmp := t.TempDir()
	legacyDir := filepath.Join(tmp, "cache", "runtime")
	newDir := filepath.Join(tmp, "state", "runtime")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(newDir, 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	legacyStatus := filepath.Join(legacyDir, "status.json")
	newStatus := filepath.Join(newDir, "status.json")
	if err := os.WriteFile(legacyStatus, []byte(`{"state":"legacy-old"}`), 0o600); err != nil {
		t.Fatalf("write legacy status: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "details.json"), []byte(`{"copied":true}`), 0o600); err != nil {
		t.Fatalf("write legacy details: %v", err)
	}
	if err := os.WriteFile(newStatus, []byte(`{"state":"newer"}`), 0o600); err != nil {
		t.Fatalf("write new status: %v", err)
	}
	oldTime := time.Unix(100, 0)
	newTime := time.Unix(200, 0)
	if err := os.Chtimes(legacyStatus, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes legacy status: %v", err)
	}
	if err := os.Chtimes(newStatus, newTime, newTime); err != nil {
		t.Fatalf("chtimes new status: %v", err)
	}

	got, err := ResolveMigratedDirWithRequired(newDir, legacyDir, "status.json")
	if err != nil {
		t.Fatalf("ResolveMigratedDirWithRequired error: %v", err)
	}
	if got != newDir {
		t.Fatalf("ResolveMigratedDirWithRequired = %q, want %q", got, newDir)
	}
	assertFileContent(t, newStatus, `{"state":"newer"}`)
	assertFileContent(t, filepath.Join(newDir, "details.json"), `{"copied":true}`)
}

func TestResolveMigratedDirWithRequiredRefreshesEqualMTimeDifferentRequiredFileCI(t *testing.T) {
	tmp := t.TempDir()
	legacyDir := filepath.Join(tmp, "cache", "runtime")
	newDir := filepath.Join(tmp, "state", "runtime")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(newDir, 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	legacyStatus := filepath.Join(legacyDir, "status.json")
	newStatus := filepath.Join(newDir, "status.json")
	if err := os.WriteFile(legacyStatus, []byte(`{"state":"legacy-fresh"}`), 0o600); err != nil {
		t.Fatalf("write legacy status: %v", err)
	}
	if err := os.WriteFile(newStatus, []byte(`{"state":"new-stale"}`), 0o600); err != nil {
		t.Fatalf("write new status: %v", err)
	}
	sameTime := time.Unix(200, 0)
	for _, path := range []string{legacyStatus, newStatus} {
		if err := os.Chtimes(path, sameTime, sameTime); err != nil {
			t.Fatalf("chtimes %s: %v", path, err)
		}
	}

	got, err := ResolveMigratedDirWithRequired(newDir, legacyDir, "status.json")
	if err != nil {
		t.Fatalf("ResolveMigratedDirWithRequired error: %v", err)
	}
	if got != newDir {
		t.Fatalf("ResolveMigratedDirWithRequired = %q, want %q", got, newDir)
	}
	assertFileContent(t, newStatus, `{"state":"legacy-fresh"}`)
}

func TestResolveMigratedDirWithRequiredSkipsUnsafeLegacySideFilesCI(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires privileges on some Windows CI images")
	}
	tmp := t.TempDir()
	legacyDir := filepath.Join(tmp, "cache", "runtime")
	newDir := filepath.Join(tmp, "state", "runtime")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(newDir, 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	legacyStatus := filepath.Join(legacyDir, "status.json")
	newStatus := filepath.Join(newDir, "status.json")
	if err := os.WriteFile(legacyStatus, []byte(`{"state":"fresh"}`), 0o600); err != nil {
		t.Fatalf("write legacy status: %v", err)
	}
	if err := os.WriteFile(newStatus, []byte(`{"state":"stale"}`), 0o600); err != nil {
		t.Fatalf("write new status: %v", err)
	}
	if err := os.Symlink(filepath.Join(tmp, "outside"), filepath.Join(legacyDir, "unsafe-link")); err != nil {
		t.Fatalf("create legacy symlink: %v", err)
	}
	if err := os.Chtimes(newStatus, time.Unix(100, 0), time.Unix(100, 0)); err != nil {
		t.Fatalf("chtimes new status: %v", err)
	}
	if err := os.Chtimes(legacyStatus, time.Unix(200, 0), time.Unix(200, 0)); err != nil {
		t.Fatalf("chtimes legacy status: %v", err)
	}

	got, err := ResolveMigratedDirWithRequired(newDir, legacyDir, "status.json")
	if err != nil {
		t.Fatalf("ResolveMigratedDirWithRequired error: %v", err)
	}
	if got != newDir {
		t.Fatalf("ResolveMigratedDirWithRequired = %q, want %q", got, newDir)
	}
	assertFileContent(t, newStatus, `{"state":"fresh"}`)
	if _, err := os.Lstat(filepath.Join(newDir, "unsafe-link")); !os.IsNotExist(err) {
		t.Fatalf("unsafe symlink should not be copied, stat err = %v", err)
	}
}

func TestResolveMigratedDirWithRequiredRejectsSymlinkRequiredFileCI(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires privileges on some Windows CI images")
	}
	tmp := t.TempDir()
	legacyDir := filepath.Join(tmp, "cache", "runtime")
	newDir := filepath.Join(tmp, "state", "runtime")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(newDir, 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "status.json"), []byte(`{"state":"legacy"}`), 0o600); err != nil {
		t.Fatalf("write legacy status: %v", err)
	}
	if err := os.Symlink(filepath.Join(tmp, "outside-status.json"), filepath.Join(newDir, "status.json")); err != nil {
		t.Fatalf("create new symlink status: %v", err)
	}

	got, err := ResolveMigratedDirWithRequired(newDir, legacyDir, "status.json")
	if err != nil {
		t.Fatalf("ResolveMigratedDirWithRequired error: %v", err)
	}
	if got != legacyDir {
		t.Fatalf("ResolveMigratedDirWithRequired = %q, want legacy fallback %q", got, legacyDir)
	}
}

func TestResolveMigratedRelatedFilesRequiresRegularSidecarsCI(t *testing.T) {
	tmp := t.TempDir()
	legacyBase := filepath.Join(tmp, "cache", "adapter.sqlite")
	newBase := filepath.Join(tmp, "state", "adapter.sqlite")
	if err := os.MkdirAll(filepath.Dir(legacyBase), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newBase), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.WriteFile(legacyBase, []byte("db"), 0o600); err != nil {
		t.Fatalf("write legacy db: %v", err)
	}
	if err := os.WriteFile(legacyBase+"-wal", []byte("wal"), 0o600); err != nil {
		t.Fatalf("write legacy wal: %v", err)
	}
	if err := os.WriteFile(newBase, []byte("db"), 0o600); err != nil {
		t.Fatalf("write new db: %v", err)
	}
	if err := os.MkdirAll(newBase+"-wal", 0o700); err != nil {
		t.Fatalf("mkdir bogus new wal directory: %v", err)
	}

	got, err := ResolveMigratedRelatedFiles(newBase, legacyBase, "-wal")
	if err != nil {
		t.Fatalf("ResolveMigratedRelatedFiles error: %v", err)
	}
	if got != legacyBase {
		t.Fatalf("ResolveMigratedRelatedFiles = %q, want legacy fallback %q", got, legacyBase)
	}
}

func TestResolveMigratedFileFaultInjectionKeepsLegacyRetryableCI(t *testing.T) {
	for _, stage := range []migrationHookStage{
		migrationHookFileBeforeCreateTemp,
		migrationHookFileAfterCopy,
		migrationHookFileBeforeRename,
	} {
		t.Run(string(stage), func(t *testing.T) {
			tmp := t.TempDir()
			legacyPath := filepath.Join(tmp, "cache", "registry.json")
			newPath := filepath.Join(tmp, "state", "registry.json")
			if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
				t.Fatalf("mkdir legacy: %v", err)
			}
			if err := os.WriteFile(legacyPath, []byte(`{"version":1}`), 0o600); err != nil {
				t.Fatalf("write legacy: %v", err)
			}
			withMigrationHook(t, func(gotStage migrationHookStage, path string) error {
				if gotStage == stage && path == newPath {
					return errors.New("simulated ENOSPC")
				}
				return nil
			})

			got, err := ResolveMigratedFile(newPath, legacyPath)
			if err != nil {
				t.Fatalf("ResolveMigratedFile error: %v", err)
			}
			if got != legacyPath {
				t.Fatalf("ResolveMigratedFile = %q, want legacy fallback %q", got, legacyPath)
			}
			if _, err := os.Stat(newPath); !os.IsNotExist(err) {
				t.Fatalf("new path should not be exposed after injected failure, stat err = %v", err)
			}

			migrationTestHook = nil
			got, err = ResolveMigratedFile(newPath, legacyPath)
			if err != nil {
				t.Fatalf("retry ResolveMigratedFile error: %v", err)
			}
			if got != newPath {
				t.Fatalf("retry ResolveMigratedFile = %q, want %q", got, newPath)
			}
			assertFileContent(t, newPath, `{"version":1}`)
		})
	}
}

func TestResolveMigratedFileAfterRenameFailureIsRetryableCI(t *testing.T) {
	tmp := t.TempDir()
	legacyPath := filepath.Join(tmp, "cache", "registry.json")
	newPath := filepath.Join(tmp, "state", "registry.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	withMigrationHook(t, func(stage migrationHookStage, path string) error {
		if stage == migrationHookFileAfterRename && path == newPath {
			return errors.New("simulated power loss after rename")
		}
		return nil
	})

	got, err := ResolveMigratedFile(newPath, legacyPath)
	if err != nil {
		t.Fatalf("ResolveMigratedFile error: %v", err)
	}
	if got != legacyPath {
		t.Fatalf("ResolveMigratedFile = %q, want legacy fallback after uncertain rename %q", got, legacyPath)
	}
	assertFileContent(t, newPath, `{"version":1}`)

	migrationTestHook = nil
	got, err = ResolveMigratedFile(newPath, legacyPath)
	if err != nil {
		t.Fatalf("retry ResolveMigratedFile error: %v", err)
	}
	if got != newPath {
		t.Fatalf("retry ResolveMigratedFile = %q, want %q", got, newPath)
	}
}

func TestResolveMigratedFileReplacementFaultInjectionKeepsLegacyRetryableCI(t *testing.T) {
	for _, stage := range []migrationHookStage{
		migrationHookFileBeforeCreateTemp,
		migrationHookFileAfterCopy,
		migrationHookFileBeforeRename,
	} {
		t.Run(string(stage), func(t *testing.T) {
			tmp := t.TempDir()
			legacyPath := filepath.Join(tmp, "cache", "registry.json")
			newPath := filepath.Join(tmp, "state", "registry.json")
			if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
				t.Fatalf("mkdir legacy: %v", err)
			}
			if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
				t.Fatalf("mkdir new: %v", err)
			}
			if err := os.WriteFile(legacyPath, []byte(`{"version":2}`), 0o600); err != nil {
				t.Fatalf("write legacy: %v", err)
			}
			if err := os.WriteFile(newPath, []byte(`{"version":1}`), 0o600); err != nil {
				t.Fatalf("write new: %v", err)
			}
			if err := os.Chtimes(newPath, time.Unix(100, 0), time.Unix(100, 0)); err != nil {
				t.Fatalf("chtimes new: %v", err)
			}
			if err := os.Chtimes(legacyPath, time.Unix(200, 0), time.Unix(200, 0)); err != nil {
				t.Fatalf("chtimes legacy: %v", err)
			}
			withMigrationHook(t, func(gotStage migrationHookStage, path string) error {
				if gotStage == stage && path == newPath {
					return errors.New("simulated replacement failure")
				}
				return nil
			})

			got, err := ResolveMigratedFile(newPath, legacyPath)
			if err != nil {
				t.Fatalf("ResolveMigratedFile error: %v", err)
			}
			if got != legacyPath {
				t.Fatalf("ResolveMigratedFile = %q, want legacy fallback %q", got, legacyPath)
			}
			assertFileContent(t, newPath, `{"version":1}`)

			migrationTestHook = nil
			got, err = ResolveMigratedFile(newPath, legacyPath)
			if err != nil {
				t.Fatalf("retry ResolveMigratedFile error: %v", err)
			}
			if got != newPath {
				t.Fatalf("retry ResolveMigratedFile = %q, want %q", got, newPath)
			}
			assertFileContent(t, newPath, `{"version":2}`)
		})
	}
}

func TestResolveMigratedFileReplacementAfterRenameFailureExposesFreshNewCI(t *testing.T) {
	tmp := t.TempDir()
	legacyPath := filepath.Join(tmp, "cache", "registry.json")
	newPath := filepath.Join(tmp, "state", "registry.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"version":2}`), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write new: %v", err)
	}
	if err := os.Chtimes(newPath, time.Unix(100, 0), time.Unix(100, 0)); err != nil {
		t.Fatalf("chtimes new: %v", err)
	}
	if err := os.Chtimes(legacyPath, time.Unix(200, 0), time.Unix(200, 0)); err != nil {
		t.Fatalf("chtimes legacy: %v", err)
	}
	withMigrationHook(t, func(stage migrationHookStage, path string) error {
		if stage == migrationHookFileAfterRename && path == newPath {
			return errors.New("simulated power loss after replacement rename")
		}
		return nil
	})

	got, err := ResolveMigratedFile(newPath, legacyPath)
	if err != nil {
		t.Fatalf("ResolveMigratedFile error: %v", err)
	}
	if got != newPath {
		t.Fatalf("ResolveMigratedFile = %q, want fresh new path %q", got, newPath)
	}
	assertFileContent(t, newPath, `{"version":2}`)
}

func TestResolveMigratedDirWithRequiredReplacementFaultInjectionKeepsLegacyRetryableCI(t *testing.T) {
	for _, stage := range []migrationHookStage{
		migrationHookFileBeforeCreateTemp,
		migrationHookFileAfterCopy,
		migrationHookFileBeforeRename,
	} {
		t.Run(string(stage), func(t *testing.T) {
			tmp := t.TempDir()
			legacyDir := filepath.Join(tmp, "cache", "runtime")
			newDir := filepath.Join(tmp, "state", "runtime")
			if err := os.MkdirAll(legacyDir, 0o700); err != nil {
				t.Fatalf("mkdir legacy: %v", err)
			}
			if err := os.MkdirAll(newDir, 0o700); err != nil {
				t.Fatalf("mkdir new: %v", err)
			}
			legacyStatus := filepath.Join(legacyDir, "status.json")
			newStatus := filepath.Join(newDir, "status.json")
			if err := os.WriteFile(legacyStatus, []byte(`{"state":"fresh"}`), 0o600); err != nil {
				t.Fatalf("write legacy status: %v", err)
			}
			if err := os.WriteFile(newStatus, []byte(`{"state":"stale"}`), 0o600); err != nil {
				t.Fatalf("write new status: %v", err)
			}
			if err := os.Chtimes(newStatus, time.Unix(100, 0), time.Unix(100, 0)); err != nil {
				t.Fatalf("chtimes new status: %v", err)
			}
			if err := os.Chtimes(legacyStatus, time.Unix(200, 0), time.Unix(200, 0)); err != nil {
				t.Fatalf("chtimes legacy status: %v", err)
			}
			withMigrationHook(t, func(gotStage migrationHookStage, path string) error {
				if gotStage == stage && path == newStatus {
					return errors.New("simulated replacement ENOSPC")
				}
				return nil
			})

			got, err := ResolveMigratedDirWithRequired(newDir, legacyDir, "status.json")
			if err != nil {
				t.Fatalf("ResolveMigratedDirWithRequired error: %v", err)
			}
			if got != legacyDir {
				t.Fatalf("ResolveMigratedDirWithRequired = %q, want legacy fallback %q", got, legacyDir)
			}
			assertFileContent(t, newStatus, `{"state":"stale"}`)

			migrationTestHook = nil
			got, err = ResolveMigratedDirWithRequired(newDir, legacyDir, "status.json")
			if err != nil {
				t.Fatalf("retry ResolveMigratedDirWithRequired error: %v", err)
			}
			if got != newDir {
				t.Fatalf("retry ResolveMigratedDirWithRequired = %q, want %q", got, newDir)
			}
			assertFileContent(t, newStatus, `{"state":"fresh"}`)
		})
	}
}

func TestResolveMigratedDirWithRequiredReplacementAfterRenameFailureExposesFreshNewCI(t *testing.T) {
	tmp := t.TempDir()
	legacyDir := filepath.Join(tmp, "cache", "runtime")
	newDir := filepath.Join(tmp, "state", "runtime")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(newDir, 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	legacyStatus := filepath.Join(legacyDir, "status.json")
	newStatus := filepath.Join(newDir, "status.json")
	if err := os.WriteFile(legacyStatus, []byte(`{"state":"fresh"}`), 0o600); err != nil {
		t.Fatalf("write legacy status: %v", err)
	}
	if err := os.WriteFile(newStatus, []byte(`{"state":"stale"}`), 0o600); err != nil {
		t.Fatalf("write new status: %v", err)
	}
	if err := os.Chtimes(newStatus, time.Unix(100, 0), time.Unix(100, 0)); err != nil {
		t.Fatalf("chtimes new status: %v", err)
	}
	if err := os.Chtimes(legacyStatus, time.Unix(200, 0), time.Unix(200, 0)); err != nil {
		t.Fatalf("chtimes legacy status: %v", err)
	}
	withMigrationHook(t, func(stage migrationHookStage, path string) error {
		if stage == migrationHookFileAfterRename && path == newStatus {
			return errors.New("simulated power loss after replacement rename")
		}
		return nil
	})

	got, err := ResolveMigratedDirWithRequired(newDir, legacyDir, "status.json")
	if err != nil {
		t.Fatalf("ResolveMigratedDirWithRequired error: %v", err)
	}
	if got != newDir {
		t.Fatalf("ResolveMigratedDirWithRequired = %q, want fresh new dir %q", got, newDir)
	}
	assertFileContent(t, newStatus, `{"state":"fresh"}`)
	assertFileContent(t, legacyStatus, `{"state":"fresh"}`)
}

func TestCopyFileReplacingActiveTempDoesNotInheritOldSourceMTimeCI(t *testing.T) {
	tmp := t.TempDir()
	legacyPath := filepath.Join(tmp, "cache", "registry.json")
	newPath := filepath.Join(tmp, "state", "registry.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"version":2}`), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write new: %v", err)
	}
	now := time.Unix(10_000, 0)
	oldSourceTime := now.Add(-48 * time.Hour)
	if err := os.Chtimes(legacyPath, oldSourceTime, oldSourceTime); err != nil {
		t.Fatalf("chtimes legacy: %v", err)
	}
	withMigrationCleanupForTest(t, now, 24*time.Hour, nil)
	withMigrationHook(t, func(stage migrationHookStage, path string) error {
		if stage == migrationHookFileAfterCopy && path == newPath {
			cleanupStaleMigrationTemps(newPath)
		}
		return nil
	})

	if err := CopyFileReplacing(newPath, legacyPath); err != nil {
		t.Fatalf("CopyFileReplacing error: %v", err)
	}
	assertFileContent(t, newPath, `{"version":2}`)
}

func TestResolveMigratedDirFaultInjectionKeepsLegacyRetryableCI(t *testing.T) {
	for _, stage := range []migrationHookStage{
		migrationHookDirAfterCopy,
		migrationHookDirBeforeRename,
	} {
		t.Run(string(stage), func(t *testing.T) {
			tmp := t.TempDir()
			legacyDir := filepath.Join(tmp, "cache", "outbound")
			newDir := filepath.Join(tmp, "state", "outbound")
			if err := os.MkdirAll(legacyDir, 0o700); err != nil {
				t.Fatalf("mkdir legacy: %v", err)
			}
			if err := os.WriteFile(filepath.Join(legacyDir, "artifact.txt"), []byte("artifact"), 0o600); err != nil {
				t.Fatalf("write legacy artifact: %v", err)
			}
			withMigrationHook(t, func(gotStage migrationHookStage, path string) error {
				if gotStage == stage && path == newDir {
					return errors.New("simulated directory migration failure")
				}
				return nil
			})

			got, err := ResolveMigratedDir(newDir, legacyDir)
			if err != nil {
				t.Fatalf("ResolveMigratedDir error: %v", err)
			}
			if got != legacyDir {
				t.Fatalf("ResolveMigratedDir = %q, want legacy fallback %q", got, legacyDir)
			}
			if _, err := os.Stat(newDir); !os.IsNotExist(err) {
				t.Fatalf("new dir should not be exposed after injected failure, stat err = %v", err)
			}

			migrationTestHook = nil
			got, err = ResolveMigratedDir(newDir, legacyDir)
			if err != nil {
				t.Fatalf("retry ResolveMigratedDir error: %v", err)
			}
			if got != newDir {
				t.Fatalf("retry ResolveMigratedDir = %q, want %q", got, newDir)
			}
			assertFileContent(t, filepath.Join(newDir, "artifact.txt"), "artifact")
		})
	}
}

func TestResolveMigratedDirAfterRenameFailureIsRetryableCI(t *testing.T) {
	tmp := t.TempDir()
	legacyDir := filepath.Join(tmp, "cache", "outbound")
	newDir := filepath.Join(tmp, "state", "outbound")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "artifact.txt"), []byte("artifact"), 0o600); err != nil {
		t.Fatalf("write legacy artifact: %v", err)
	}
	withMigrationHook(t, func(stage migrationHookStage, path string) error {
		if stage == migrationHookDirAfterRename && path == newDir {
			return errors.New("simulated power loss after directory rename")
		}
		return nil
	})

	got, err := ResolveMigratedDir(newDir, legacyDir)
	if err != nil {
		t.Fatalf("ResolveMigratedDir error: %v", err)
	}
	if got != legacyDir {
		t.Fatalf("ResolveMigratedDir = %q, want legacy fallback after uncertain rename %q", got, legacyDir)
	}
	assertFileContent(t, filepath.Join(newDir, "artifact.txt"), "artifact")

	migrationTestHook = nil
	got, err = ResolveMigratedDir(newDir, legacyDir)
	if err != nil {
		t.Fatalf("retry ResolveMigratedDir error: %v", err)
	}
	if got != newDir {
		t.Fatalf("retry ResolveMigratedDir = %q, want %q", got, newDir)
	}
}

func TestCleanupStaleMigrationTempsRemovesOnlyOldMatchingFileTempsCI(t *testing.T) {
	tmp := t.TempDir()
	legacyPath := filepath.Join(tmp, "cache", "registry.json")
	newPath := filepath.Join(tmp, "state", "registry.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new parent: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	oldMatching := filepath.Join(filepath.Dir(newPath), ".registry.json.migrating-old")
	freshMatching := filepath.Join(filepath.Dir(newPath), ".registry.json.migrating-fresh")
	oldUnrelated := filepath.Join(filepath.Dir(newPath), ".other.json.migrating-old")
	noLeadingDot := filepath.Join(filepath.Dir(newPath), "registry.json.migrating-old")
	for _, path := range []string{oldMatching, freshMatching, oldUnrelated, noLeadingDot} {
		if err := os.WriteFile(path, []byte("temp"), 0o600); err != nil {
			t.Fatalf("write temp %s: %v", path, err)
		}
	}
	now := time.Unix(10_000, 0)
	oldTime := now.Add(-48 * time.Hour)
	withMigrationCleanupForTest(t, now, 24*time.Hour, nil)
	for _, path := range []string{oldMatching, oldUnrelated, noLeadingDot} {
		if err := os.Chtimes(path, oldTime, oldTime); err != nil {
			t.Fatalf("chtimes old temp %s: %v", path, err)
		}
	}
	if err := os.Chtimes(freshMatching, now, now); err != nil {
		t.Fatalf("chtimes fresh temp: %v", err)
	}

	got, err := ResolveMigratedFile(newPath, legacyPath)
	if err != nil {
		t.Fatalf("ResolveMigratedFile error: %v", err)
	}
	if got != newPath {
		t.Fatalf("ResolveMigratedFile = %q, want %q", got, newPath)
	}
	assertPathMissing(t, oldMatching)
	assertFileContent(t, freshMatching, "temp")
	assertFileContent(t, oldUnrelated, "temp")
	assertFileContent(t, noLeadingDot, "temp")
	assertFileContent(t, newPath, `{"version":1}`)
}

func TestCleanupStaleMigrationTempsRemovesOnlyOldMatchingDirTempsCI(t *testing.T) {
	tmp := t.TempDir()
	legacyDir := filepath.Join(tmp, "cache", "outbound")
	newDir := filepath.Join(tmp, "state", "outbound")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "artifact.txt"), []byte("artifact"), 0o600); err != nil {
		t.Fatalf("write legacy artifact: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newDir), 0o700); err != nil {
		t.Fatalf("mkdir new parent: %v", err)
	}
	oldMatching := filepath.Join(filepath.Dir(newDir), ".outbound.migrating-old")
	freshMatching := filepath.Join(filepath.Dir(newDir), ".outbound.migrating-fresh")
	oldUnrelated := filepath.Join(filepath.Dir(newDir), ".other.migrating-old")
	for _, dir := range []string{oldMatching, freshMatching, oldUnrelated} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("mkdir temp dir %s: %v", dir, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "temp.txt"), []byte("temp"), 0o600); err != nil {
			t.Fatalf("write temp dir file %s: %v", dir, err)
		}
	}
	now := time.Unix(10_000, 0)
	oldTime := now.Add(-48 * time.Hour)
	withMigrationCleanupForTest(t, now, 24*time.Hour, nil)
	for _, dir := range []string{oldMatching, oldUnrelated} {
		if err := os.Chtimes(dir, oldTime, oldTime); err != nil {
			t.Fatalf("chtimes old temp dir %s: %v", dir, err)
		}
	}
	if err := os.Chtimes(freshMatching, now, now); err != nil {
		t.Fatalf("chtimes fresh temp dir: %v", err)
	}

	got, err := ResolveMigratedDir(newDir, legacyDir)
	if err != nil {
		t.Fatalf("ResolveMigratedDir error: %v", err)
	}
	if got != newDir {
		t.Fatalf("ResolveMigratedDir = %q, want %q", got, newDir)
	}
	assertPathMissing(t, oldMatching)
	assertFileContent(t, filepath.Join(freshMatching, "temp.txt"), "temp")
	assertFileContent(t, filepath.Join(oldUnrelated, "temp.txt"), "temp")
	assertFileContent(t, filepath.Join(newDir, "artifact.txt"), "artifact")
}

func TestCleanupStaleMigrationTempsDoesNotBlockMigrationOnRemoveFailureCI(t *testing.T) {
	tmp := t.TempDir()
	legacyPath := filepath.Join(tmp, "cache", "registry.json")
	newPath := filepath.Join(tmp, "state", "registry.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatalf("mkdir new parent: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	oldMatching := filepath.Join(filepath.Dir(newPath), ".registry.json.migrating-old")
	if err := os.WriteFile(oldMatching, []byte("temp"), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	now := time.Unix(10_000, 0)
	oldTime := now.Add(-48 * time.Hour)
	if err := os.Chtimes(oldMatching, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes old temp: %v", err)
	}
	withMigrationCleanupForTest(t, now, 24*time.Hour, func(path string) error {
		if path == oldMatching {
			return errors.New("simulated cleanup failure")
		}
		return os.RemoveAll(path)
	})

	got, err := ResolveMigratedFile(newPath, legacyPath)
	if err != nil {
		t.Fatalf("ResolveMigratedFile error: %v", err)
	}
	if got != newPath {
		t.Fatalf("ResolveMigratedFile = %q, want %q", got, newPath)
	}
	assertFileContent(t, oldMatching, "temp")
	assertFileContent(t, newPath, `{"version":1}`)
}

func withMigrationHook(t *testing.T, hook func(stage migrationHookStage, path string) error) {
	t.Helper()
	prev := migrationTestHook
	migrationTestHook = hook
	t.Cleanup(func() {
		migrationTestHook = prev
	})
}

func withMigrationCleanupForTest(t *testing.T, now time.Time, ttl time.Duration, removeAll func(string) error) {
	t.Helper()
	prevTTL := migrationCleanupTTL
	prevNow := migrationCleanupNow
	prevRemoveAll := migrationCleanupRemoveAll
	migrationCleanupTTL = ttl
	migrationCleanupNow = func() time.Time { return now }
	if removeAll == nil {
		migrationCleanupRemoveAll = os.RemoveAll
	} else {
		migrationCleanupRemoveAll = removeAll
	}
	t.Cleanup(func() {
		migrationCleanupTTL = prevTTL
		migrationCleanupNow = prevNow
		migrationCleanupRemoveAll = prevRemoveAll
	})
}

func assertFileContent(t *testing.T, path string, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(got) != want {
		t.Fatalf("content for %s = %q, want %q", path, got, want)
	}
}

func assertPathMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("%s should be missing, stat err = %v", path, err)
	}
}
