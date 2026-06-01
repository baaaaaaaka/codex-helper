//go:build !windows

package update

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReplaceBinaryErrors(t *testing.T) {
	t.Run("source missing", func(t *testing.T) {
		dir := t.TempDir()
		_, err := replaceBinary(filepath.Join(dir, "missing"), filepath.Join(dir, "dest"), replaceOptions{})
		if err == nil {
			t.Fatalf("expected error for missing source")
		}
	})

	t.Run("dest directory missing", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src")
		if err := os.WriteFile(src, []byte("data"), 0o600); err != nil {
			t.Fatalf("write src: %v", err)
		}
		dest := filepath.Join(dir, "missing", "dest")
		if _, err := replaceBinary(src, dest, replaceOptions{}); err == nil {
			t.Fatalf("expected error for missing dest dir")
		}
	})
}

func TestReplaceBinaryBacksUpPrevious(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "codex-proxy")
	if err := os.WriteFile(dest, []byte("OLD BINARY"), 0o755); err != nil {
		t.Fatalf("write dest: %v", err)
	}
	src := filepath.Join(dir, "new")
	if err := os.WriteFile(src, []byte("NEW BINARY"), 0o755); err != nil {
		t.Fatalf("write src: %v", err)
	}

	if _, err := replaceBinary(src, dest, replaceOptions{}); err != nil {
		t.Fatalf("replaceBinary: %v", err)
	}

	if got, _ := os.ReadFile(dest); string(got) != "NEW BINARY" {
		t.Fatalf("dest = %q, want NEW BINARY", got)
	}
	prev := PreviousBinaryPath(dest)
	got, err := os.ReadFile(prev)
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	if string(got) != "OLD BINARY" {
		t.Fatalf("backup = %q, want OLD BINARY", got)
	}
	if info, err := os.Stat(prev); err != nil || info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("backup should be executable, mode=%v err=%v", info.Mode(), err)
	}
}

func TestReplaceBinaryFirstInstallHasNoBackup(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "codex-proxy")
	src := filepath.Join(dir, "new")
	if err := os.WriteFile(src, []byte("NEW BINARY"), 0o755); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if _, err := replaceBinary(src, dest, replaceOptions{}); err != nil {
		t.Fatalf("replaceBinary: %v", err)
	}
	if _, err := os.Stat(PreviousBinaryPath(dest)); !os.IsNotExist(err) {
		t.Fatalf("expected no backup on first install, err=%v", err)
	}
}
