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
}
