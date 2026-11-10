package helperruntime

import (
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

// Keep the former pointer-reader contract across native snapshot replacement,
// including Windows delete-sharing and SQLite's connection/page-cache lifetime.
func TestVersionPointerReplacementWhileReaderOpen(t *testing.T) {
	root := t.TempDir()
	if err := Activate(root, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	old, err := sqlitestate.OpenSnapshot(filepath.Join(root, BootFile), maxBootRecordSize)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	var good string
	if err := old.QueryRow(`SELECT good FROM boot WHERE id=1`).Scan(&good); err != nil || good != "v1.0.0" {
		t.Fatal(good, err)
	}
	if err := Activate(root, "v2.0.0"); err != nil {
		t.Fatal(err)
	}
	if err := old.QueryRow(`SELECT good FROM boot WHERE id=1`).Scan(&good); err != nil || good != "v1.0.0" {
		t.Fatal("open reader changed", good, err)
	}
	s, err := ReadBoot(root)
	if err != nil || s.Good != "v2.0.0" || s.Previous != "v1.0.0" {
		t.Fatal("fresh reader", s, err)
	}
}
