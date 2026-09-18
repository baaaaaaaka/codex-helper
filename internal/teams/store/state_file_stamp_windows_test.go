//go:build windows

package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSourceFileIdentityWindows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transcript.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	identity, err := SourceFileIdentity(path)
	if err != nil {
		t.Fatalf("source identity: %v", err)
	}
	if identity == "" {
		t.Fatal("source identity is empty")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat source file: %v", err)
	}
	fromInfo, err := SourceFileIdentityFromFileInfo(path, info)
	if err != nil {
		t.Fatalf("source identity from file info: %v", err)
	}
	if fromInfo != identity {
		t.Fatalf("source identity from file info = %q, want %q", fromInfo, identity)
	}
	if changeTime := SourceFileChangeTime(path, info); changeTime == 0 {
		t.Skip("filesystem does not expose a usable per-file USN revision")
	}
}

func TestSourceFileChangeTimeWindowsDetectsTimestampRestoredRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same-size-rewrite.jsonl")
	content := []byte("0123456789abcdef\n")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat source file: %v", err)
	}
	before := SourceFileChangeTime(path, info)
	if before == 0 {
		t.Skip("filesystem does not expose a usable per-file USN revision")
	}
	content[0] = 'x'
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("rewrite source file: %v", err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("restore source mtime: %v", err)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat rewritten source file: %v", err)
	}
	after := SourceFileChangeTime(path, afterInfo)
	if after == 0 {
		t.Fatalf("source revision disappeared after rewrite")
	}
	if after == before {
		t.Fatalf("source revision did not change after same-size rewrite with restored mtime: %d", after)
	}
}
