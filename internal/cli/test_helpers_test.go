package cli

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

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

func withCodexYoloRuntimeGOOS(t *testing.T, goos string) {
	t.Helper()
	previous := codexYoloRuntimeGOOS
	codexYoloRuntimeGOOS = goos
	t.Cleanup(func() {
		codexYoloRuntimeGOOS = previous
	})
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
