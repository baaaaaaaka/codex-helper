package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "codex-helper-cli-tests-")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create CLI test temp dir: %v\n", err)
		os.Exit(2)
	}
	_ = os.Setenv("HOME", filepath.Join(tmp, "home"))
	_ = os.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "cache"))
	_ = os.Setenv("LOCALAPPDATA", filepath.Join(tmp, "localappdata"))

	code := m.Run()
	_ = os.RemoveAll(tmp)
	os.Exit(code)
}

func newTempStore(t *testing.T) *config.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return store
}

func writeProbeScript(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func setTestCodexHomeEnv(t *testing.T, codexDir string) {
	t.Helper()
	t.Setenv(codexhistory.EnvCodexDir, codexDir)
	t.Setenv(envCodexHome, codexDir)
}

func writeStub(t *testing.T, dir, name, unix, win string) {
	t.Helper()
	path := filepath.Join(dir, name)
	content := unix
	if runtime.GOOS == "windows" {
		path += ".cmd"
		content = win
	}
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatalf("write stub: %v", err)
	}
}

func requireStringSlice(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func canonicalPath(t *testing.T, path string) string {
	t.Helper()
	if path == "" {
		return path
	}
	abs, err := filepath.Abs(path)
	if err == nil {
		path = abs
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err == nil {
		return resolved
	}
	return filepath.Clean(path)
}
