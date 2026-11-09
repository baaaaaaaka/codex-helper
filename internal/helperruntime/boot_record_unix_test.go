//go:build !windows

package helperruntime

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestSafeBootRejectsFIFORecordsWithoutOpeningThem(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{BootFile, "active", "previous"} {
		if err := syscall.Mkfifo(filepath.Join(root, name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ReadBoot(root); err == nil {
		t.Fatal("FIFO boot record accepted")
	}
	for _, name := range []string{"active", "previous"} {
		if _, err := readVersionPointer(root, name); err == nil {
			t.Fatal("FIFO pointer accepted", name)
		}
	}
	if err := os.Remove(filepath.Join(root, BootFile)); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "active"), filepath.Join(root, BootFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBoot(root); err == nil {
		t.Fatal("symlink to FIFO boot record accepted")
	}
}
