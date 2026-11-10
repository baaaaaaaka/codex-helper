package delegation

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestWorkerSQLiteSecureCreationAndQuietReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	s, err := OpenWorkerDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Mode().Perm() != 0600 {
		t.Fatal(before.Mode())
	}
	s, err = OpenWorkerDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Sys().(*syscall.Stat_t).Ctim != after.Sys().(*syscall.Stat_t).Ctim {
		t.Fatal("unchanged reopen dirtied database metadata")
	}
}
