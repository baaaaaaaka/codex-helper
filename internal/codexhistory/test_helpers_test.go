package codexhistory

import "testing"

// tempCodexRoot returns an isolated Codex data root whose persistent cache
// handles are closed before testing.TempDir removes it. Unix permits unlinking
// an open SQLite database, but Windows correctly rejects that cleanup, so tests
// that exercise catalog persistence must use the same explicit lifecycle as
// production callers.
func tempCodexRoot(t *testing.T) string {
	t.Helper()
	if err := CloseCaches(); err != nil {
		t.Fatalf("close caches before temporary Codex root: %v", err)
	}
	root := t.TempDir()
	t.Cleanup(func() {
		if err := CloseCaches(); err != nil {
			t.Errorf("close caches for temporary Codex root: %v", err)
		}
	})
	return root
}
