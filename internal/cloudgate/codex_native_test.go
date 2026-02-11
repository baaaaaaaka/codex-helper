package cloudgate

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestTargetTriple(t *testing.T) {
	triple := targetTriple()

	switch runtime.GOOS {
	case "linux":
		switch runtime.GOARCH {
		case "amd64":
			if triple != "x86_64-unknown-linux-musl" {
				t.Errorf("expected x86_64-unknown-linux-musl, got %q", triple)
			}
		case "arm64":
			if triple != "aarch64-unknown-linux-musl" {
				t.Errorf("expected aarch64-unknown-linux-musl, got %q", triple)
			}
		default:
			if triple != "" {
				t.Errorf("expected empty triple for unsupported arch %s, got %q", runtime.GOARCH, triple)
			}
		}
	case "darwin":
		switch runtime.GOARCH {
		case "amd64":
			if triple != "x86_64-apple-darwin" {
				t.Errorf("expected x86_64-apple-darwin, got %q", triple)
			}
		case "arm64":
			if triple != "aarch64-apple-darwin" {
				t.Errorf("expected aarch64-apple-darwin, got %q", triple)
			}
		default:
			if triple != "" {
				t.Errorf("expected empty triple for unsupported arch %s, got %q", runtime.GOARCH, triple)
			}
		}
	case "windows":
		if triple != "" {
			t.Errorf("expected empty triple on Windows, got %q", triple)
		}
	}
}

func TestFindNativeBinaryNotFound(t *testing.T) {
	dir := t.TempDir()
	wrapper := filepath.Join(dir, "bin", "codex.js")
	if err := os.MkdirAll(filepath.Dir(wrapper), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(wrapper, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, _, err := FindNativeBinary(wrapper)
	if err == nil {
		t.Fatal("expected error for missing native binary")
	}
}

func TestFindNativeBinaryUnsupportedPlatform(t *testing.T) {
	if targetTriple() != "" {
		t.Skip("only test on unsupported platforms")
	}

	_, _, err := FindNativeBinary("/usr/bin/node")
	if err == nil {
		t.Fatal("expected error for unsupported platform")
	}
}

func TestFindNativeBinaryWithMockStructure(t *testing.T) {
	triple := targetTriple()
	if triple == "" {
		t.Skip("unsupported platform for this test")
	}

	// Create a mock package structure:
	// <dir>/bin/codex.js
	// <dir>/vendor/<triple>/codex/codex
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	nativeDir := filepath.Join(dir, "vendor", triple, "codex")
	pathDir := filepath.Join(dir, "vendor", triple, "path")

	for _, d := range []string{binDir, nativeDir, pathDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	wrapperPath := filepath.Join(binDir, "codex.js")
	if err := os.WriteFile(wrapperPath, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}

	nativePath := filepath.Join(nativeDir, "codex")
	if err := os.WriteFile(nativePath, []byte("native binary"), 0o755); err != nil {
		t.Fatalf("write native: %v", err)
	}

	gotBin, gotPath, err := FindNativeBinary(wrapperPath)
	if err != nil {
		t.Fatalf("FindNativeBinary: %v", err)
	}
	if gotBin != nativePath {
		t.Errorf("expected native binary %q, got %q", nativePath, gotBin)
	}
	if gotPath != pathDir {
		t.Errorf("expected path dir %q, got %q", pathDir, gotPath)
	}
}

func TestFindNativeBinaryNoPathDir(t *testing.T) {
	triple := targetTriple()
	if triple == "" {
		t.Skip("unsupported platform for this test")
	}

	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	nativeDir := filepath.Join(dir, "vendor", triple, "codex")

	for _, d := range []string{binDir, nativeDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	wrapperPath := filepath.Join(binDir, "codex.js")
	if err := os.WriteFile(wrapperPath, []byte("wrapper"), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}
	nativePath := filepath.Join(nativeDir, "codex")
	if err := os.WriteFile(nativePath, []byte("native"), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, gotPath, err := FindNativeBinary(wrapperPath)
	if err != nil {
		t.Fatalf("FindNativeBinary: %v", err)
	}
	if gotPath != "" {
		t.Errorf("expected empty path dir when vendor/path doesn't exist, got %q", gotPath)
	}
}

func TestPrepareYoloBinaryMissingWrapper(t *testing.T) {
	_, _, err := PrepareYoloBinary("/nonexistent/codex", t.TempDir())
	if err == nil {
		t.Fatal("expected error for missing wrapper")
	}
}
