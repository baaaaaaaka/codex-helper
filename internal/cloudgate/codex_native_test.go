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
		switch runtime.GOARCH {
		case "amd64":
			if triple != "x86_64-pc-windows-msvc" {
				t.Errorf("expected x86_64-pc-windows-msvc, got %q", triple)
			}
		case "arm64":
			if triple != "aarch64-pc-windows-msvc" {
				t.Errorf("expected aarch64-pc-windows-msvc, got %q", triple)
			}
		default:
			if triple != "" {
				t.Errorf("expected empty triple for unsupported arch %s, got %q", runtime.GOARCH, triple)
			}
		}
	}
}

func TestPlatformPackageName(t *testing.T) {
	name := platformPackageName()
	if targetTriple() == "" {
		if name != "" {
			t.Errorf("expected empty platform package for unsupported triple, got %q", name)
		}
		return
	}
	if name == "" {
		t.Error("expected non-empty platform package name")
	}
}

func TestNativeBinaryName(t *testing.T) {
	name := nativeBinaryName()
	if runtime.GOOS == "windows" {
		if name != "codex.exe" {
			t.Errorf("expected codex.exe on windows, got %q", name)
		}
	} else {
		if name != "codex" {
			t.Errorf("expected codex on non-windows, got %q", name)
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

// TestFindNativeBinaryWithMockStructure tests Strategy 1: vendor/ in pkg root.
func TestFindNativeBinaryWithMockStructure(t *testing.T) {
	triple := targetTriple()
	if triple == "" {
		t.Skip("unsupported platform for this test")
	}

	dir := t.TempDir()
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}
	binDir := filepath.Join(dir, "bin")
	nativeDir := filepath.Join(dir, "vendor", triple, "codex")
	pathDirExpected := filepath.Join(dir, "vendor", triple, "path")

	for _, d := range []string{binDir, nativeDir, pathDirExpected} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	wrapperPath := filepath.Join(binDir, "codex.js")
	if err := os.WriteFile(wrapperPath, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}

	nativePath := filepath.Join(nativeDir, nativeBinaryName())
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
	if gotPath != pathDirExpected {
		t.Errorf("expected path dir %q, got %q", pathDirExpected, gotPath)
	}
}

func TestFindNativeBinaryNoPathDir(t *testing.T) {
	triple := targetTriple()
	if triple == "" {
		t.Skip("unsupported platform for this test")
	}

	dir := t.TempDir()
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}
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
	nativePath := filepath.Join(nativeDir, nativeBinaryName())
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

// TestFindNativeBinaryPlatformSubPackage tests Strategy 2: platform-specific
// npm sub-package (e.g. node_modules/@openai/codex-win32-x64/vendor/...).
func TestFindNativeBinaryPlatformSubPackage(t *testing.T) {
	triple := targetTriple()
	if triple == "" {
		t.Skip("unsupported platform for this test")
	}
	platPkg := platformPackageName()
	if platPkg == "" {
		t.Skip("no platform package for this platform")
	}

	dir := t.TempDir()
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}

	// <dir>/bin/codex.js  (wrapper)
	// <dir>/node_modules/<platPkg>/vendor/<triple>/codex/codex[.exe]
	binDir := filepath.Join(dir, "bin")
	nativeDir := filepath.Join(dir, "node_modules", platPkg, "vendor", triple, "codex")
	pathDirExpected := filepath.Join(dir, "node_modules", platPkg, "vendor", triple, "path")

	for _, d := range []string{binDir, nativeDir, pathDirExpected} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	wrapperPath := filepath.Join(binDir, "codex.js")
	if err := os.WriteFile(wrapperPath, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}

	nativePath := filepath.Join(nativeDir, nativeBinaryName())
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
	if gotPath != pathDirExpected {
		t.Errorf("expected path dir %q, got %q", pathDirExpected, gotPath)
	}
}

// TestParseNpmCmdShim verifies extraction of the .js path from an npm .cmd shim.
func TestParseNpmCmdShim(t *testing.T) {
	dir := t.TempDir()

	// Create a mock codex.js at the resolved path.
	jsDir := filepath.Join(dir, "node_modules", "@openai", "codex", "bin")
	if err := os.MkdirAll(jsDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	jsPath := filepath.Join(jsDir, "codex.js")
	if err := os.WriteFile(jsPath, []byte("// entry"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Create a .cmd shim that references the .js via %dp0%.
	cmdContent := `@ECHO off
GOTO start
:find_dp0
SET dp0=%~dp0
EXIT /b
:start
SETLOCAL
CALL :find_dp0
endLocal & goto #_undefined_# 2>NUL || title %COMSPEC% & "%_prog%"  "%dp0%\node_modules\@openai\codex\bin\codex.js" %*
`
	cmdPath := filepath.Join(dir, "codex.cmd")
	if err := os.WriteFile(cmdPath, []byte(cmdContent), 0o644); err != nil {
		t.Fatalf("write cmd: %v", err)
	}

	got, err := parseNpmCmdShim(cmdPath)
	if err != nil {
		t.Fatalf("parseNpmCmdShim: %v", err)
	}
	if got != jsPath {
		t.Errorf("expected %q, got %q", jsPath, got)
	}
}

// TestParseNpmCmdShimNotFound returns error if .js file does not exist.
func TestParseNpmCmdShimNotFound(t *testing.T) {
	dir := t.TempDir()
	cmdContent := `endLocal & "%_prog%"  "%dp0%\node_modules\codex\bin\codex.js" %*`
	cmdPath := filepath.Join(dir, "codex.cmd")
	if err := os.WriteFile(cmdPath, []byte(cmdContent), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := parseNpmCmdShim(cmdPath)
	if err == nil {
		t.Fatal("expected error for missing .js file")
	}
}

// TestParseNpmCmdShimNoMatch returns error for .cmd without a .js reference.
func TestParseNpmCmdShimNoMatch(t *testing.T) {
	dir := t.TempDir()
	cmdPath := filepath.Join(dir, "other.cmd")
	if err := os.WriteFile(cmdPath, []byte("@echo hello"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := parseNpmCmdShim(cmdPath)
	if err == nil {
		t.Fatal("expected error for .cmd without .js reference")
	}
}

// TestResolveWrapperJS verifies that a .js wrapper is returned as-is (after symlink resolution).
func TestResolveWrapperJS(t *testing.T) {
	dir := t.TempDir()
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}
	jsPath := filepath.Join(dir, "codex.js")
	if err := os.WriteFile(jsPath, []byte("// entry"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := resolveWrapper(jsPath)
	if err != nil {
		t.Fatalf("resolveWrapper: %v", err)
	}
	if got != jsPath {
		t.Errorf("expected %q, got %q", jsPath, got)
	}
}

// TestFindVendorBinaryMissing returns empty strings for missing vendor dir.
func TestFindVendorBinaryMissing(t *testing.T) {
	bin, pd := findVendorBinary("/nonexistent/vendor")
	if bin != "" || pd != "" {
		t.Errorf("expected empty results, got bin=%q path=%q", bin, pd)
	}
}

func TestPrepareYoloBinaryMissingWrapper(t *testing.T) {
	_, _, err := PrepareYoloBinary("/nonexistent/codex", t.TempDir())
	if err == nil {
		t.Fatal("expected error for missing wrapper")
	}
}
