package codexbinary

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
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

func TestTargetTripleFor(t *testing.T) {
	tests := []struct {
		goos   string
		goarch string
		want   string
	}{
		{goos: "linux", goarch: "amd64", want: "x86_64-unknown-linux-musl"},
		{goos: "linux", goarch: "arm64", want: "aarch64-unknown-linux-musl"},
		{goos: "darwin", goarch: "amd64", want: "x86_64-apple-darwin"},
		{goos: "darwin", goarch: "arm64", want: "aarch64-apple-darwin"},
		{goos: "windows", goarch: "amd64", want: "x86_64-pc-windows-msvc"},
		{goos: "windows", goarch: "arm64", want: "aarch64-pc-windows-msvc"},
		{goos: "linux", goarch: "386", want: ""},
		{goos: "freebsd", goarch: "amd64", want: ""},
	}

	for _, tt := range tests {
		if got := targetTripleFor(tt.goos, tt.goarch); got != tt.want {
			t.Fatalf("targetTripleFor(%q, %q) = %q, want %q", tt.goos, tt.goarch, got, tt.want)
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

func TestPlatformPackageNameForTriple(t *testing.T) {
	tests := []struct {
		triple string
		want   string
	}{
		{triple: "x86_64-unknown-linux-musl", want: filepath.Join("@openai", "codex-linux-x64")},
		{triple: "aarch64-unknown-linux-musl", want: filepath.Join("@openai", "codex-linux-arm64")},
		{triple: "x86_64-apple-darwin", want: filepath.Join("@openai", "codex-darwin-x64")},
		{triple: "aarch64-apple-darwin", want: filepath.Join("@openai", "codex-darwin-arm64")},
		{triple: "x86_64-pc-windows-msvc", want: filepath.Join("@openai", "codex-win32-x64")},
		{triple: "aarch64-pc-windows-msvc", want: filepath.Join("@openai", "codex-win32-arm64")},
		{triple: "unknown", want: ""},
	}

	for _, tt := range tests {
		if got := platformPackageNameForTriple(tt.triple); got != tt.want {
			t.Fatalf("platformPackageNameForTriple(%q) = %q, want %q", tt.triple, got, tt.want)
		}
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

func TestNativeBinaryNameForOS(t *testing.T) {
	if got := nativeBinaryNameForOS("windows"); got != "codex.exe" {
		t.Fatalf("nativeBinaryNameForOS(windows) = %q, want codex.exe", got)
	}
	if got := nativeBinaryNameForOS("linux"); got != "codex" {
		t.Fatalf("nativeBinaryNameForOS(linux) = %q, want codex", got)
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
	if !errors.Is(err, ErrNativeBinaryNotFound) {
		t.Fatalf("missing-binary error should be retryable (ErrNativeBinaryNotFound), got %v", err)
	}
}

// TestFindNativeBinaryWithRetrySucceedsWhenBinaryAppears simulates the npm
// reinstall race: the vendor binary is briefly absent, then appears. The retry
// wrapper must ride out the gap instead of failing the launch.
func TestFindNativeBinaryWithRetrySucceedsWhenBinaryAppears(t *testing.T) {
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
	if err := os.WriteFile(wrapperPath, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}
	nativePath := filepath.Join(nativeDir, nativeBinaryName())

	// Tighten the retry budget for the test and restore afterward.
	prevAttempts, prevDelay := nativeBinaryResolveAttempts, nativeBinaryResolveDelay
	t.Cleanup(func() {
		nativeBinaryResolveAttempts, nativeBinaryResolveDelay = prevAttempts, prevDelay
	})
	nativeBinaryResolveAttempts = 10
	nativeBinaryResolveDelay = 10 * time.Millisecond

	// The binary appears after a couple of retry intervals.
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(25 * time.Millisecond)
		_ = os.WriteFile(nativePath, []byte("native binary"), 0o755)
	}()
	t.Cleanup(func() { <-done })

	gotBin, _, err := FindNativeBinaryWithRetry(wrapperPath)
	if err != nil {
		t.Fatalf("FindNativeBinaryWithRetry: %v", err)
	}
	if gotBin != nativePath {
		t.Errorf("expected native binary %q, got %q", nativePath, gotBin)
	}
}

// TestFindNativeBinaryWithRetryExhausts verifies the wrapper gives up (rather
// than hanging) when the binary never appears.
func TestFindNativeBinaryWithRetryExhausts(t *testing.T) {
	triple := targetTriple()
	if triple == "" {
		t.Skip("unsupported platform for this test")
	}
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	wrapperPath := filepath.Join(binDir, "codex.js")
	if err := os.WriteFile(wrapperPath, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}

	prevAttempts, prevDelay := nativeBinaryResolveAttempts, nativeBinaryResolveDelay
	t.Cleanup(func() {
		nativeBinaryResolveAttempts, nativeBinaryResolveDelay = prevAttempts, prevDelay
	})
	nativeBinaryResolveAttempts = 3
	nativeBinaryResolveDelay = 1 * time.Millisecond

	_, _, err := FindNativeBinaryWithRetry(wrapperPath)
	if !errors.Is(err, ErrNativeBinaryNotFound) {
		t.Fatalf("expected ErrNativeBinaryNotFound after exhausting retries, got %v", err)
	}
}

// TestFindNativeBinaryWithRetryDoesNotRetryHardError verifies a non-not-found
// error (e.g. an unresolvable wrapper path) returns immediately without burning
// the retry budget.
func TestFindNativeBinaryWithRetryDoesNotRetryHardError(t *testing.T) {
	if targetTriple() == "" {
		t.Skip("unsupported platform for this test")
	}
	prevAttempts, prevDelay := nativeBinaryResolveAttempts, nativeBinaryResolveDelay
	t.Cleanup(func() {
		nativeBinaryResolveAttempts, nativeBinaryResolveDelay = prevAttempts, prevDelay
	})
	nativeBinaryResolveAttempts = 5
	nativeBinaryResolveDelay = 10 * time.Second // would make retries take ~40s

	start := time.Now()
	_, _, err := FindNativeBinaryWithRetry(filepath.Join(t.TempDir(), "does-not-exist", "codex"))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error for an unresolvable wrapper")
	}
	if errors.Is(err, ErrNativeBinaryNotFound) {
		t.Fatalf("unresolvable wrapper should be a hard error, not retryable not-found: %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("hard error should short-circuit, took %s", elapsed)
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
	platformPkgDir := filepath.Join(dir, "node_modules", platPkg)
	nativeDir := filepath.Join(dir, "node_modules", platPkg, "vendor", triple, "codex")
	pathDirExpected := filepath.Join(dir, "node_modules", platPkg, "vendor", triple, "path")

	for _, d := range []string{binDir, platformPkgDir, nativeDir, pathDirExpected} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	wrapperPath := filepath.Join(binDir, "codex.js")
	if err := os.WriteFile(wrapperPath, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}
	if err := os.WriteFile(filepath.Join(platformPkgDir, "package.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write platform package.json: %v", err)
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

func TestFindNativeBinaryPlatformSubPackageNewLayout(t *testing.T) {
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

	binDir := filepath.Join(dir, "bin")
	platformPkgDir := filepath.Join(dir, "node_modules", platPkg)
	nativeDir := filepath.Join(platformPkgDir, "vendor", triple, "bin")
	pathDirExpected := filepath.Join(platformPkgDir, "vendor", triple, "codex-path")

	for _, d := range []string{binDir, platformPkgDir, nativeDir, pathDirExpected} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	wrapperPath := filepath.Join(binDir, "codex.js")
	if err := os.WriteFile(wrapperPath, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}
	if err := os.WriteFile(filepath.Join(platformPkgDir, "package.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write platform package.json: %v", err)
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

func TestFindVendorBinaryPrefersNewLayout(t *testing.T) {
	triple := targetTriple()
	if triple == "" {
		t.Skip("unsupported platform for this test")
	}

	dir := t.TempDir()
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}
	vendorRoot := filepath.Join(dir, "vendor")
	newNativeDir := filepath.Join(vendorRoot, triple, "bin")
	newPathDir := filepath.Join(vendorRoot, triple, "codex-path")
	legacyNativeDir := filepath.Join(vendorRoot, triple, "codex")
	legacyPathDir := filepath.Join(vendorRoot, triple, "path")
	for _, d := range []string{newNativeDir, newPathDir, legacyNativeDir, legacyPathDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	newNativePath := filepath.Join(newNativeDir, nativeBinaryName())
	if err := os.WriteFile(newNativePath, []byte("new native binary"), 0o755); err != nil {
		t.Fatalf("write new native: %v", err)
	}
	legacyNativePath := filepath.Join(legacyNativeDir, nativeBinaryName())
	if err := os.WriteFile(legacyNativePath, []byte("legacy native binary"), 0o755); err != nil {
		t.Fatalf("write legacy native: %v", err)
	}

	gotBin, gotPath := findVendorBinary(vendorRoot)
	if gotBin != newNativePath {
		t.Fatalf("expected new native binary %q, got %q", newNativePath, gotBin)
	}
	if gotPath != newPathDir {
		t.Fatalf("expected new path dir %q, got %q", newPathDir, gotPath)
	}
}

// TestFindNativeBinaryPlatformSiblingPackage covers npm layouts where the
// scoped platform package is installed next to @openai/codex under the same
// node_modules root.
func TestFindNativeBinaryPlatformSiblingPackage(t *testing.T) {
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

	nodeModulesRoot := filepath.Join(dir, "node_modules")
	binDir := filepath.Join(nodeModulesRoot, "@openai", "codex", "bin")
	platformPkgDir := filepath.Join(nodeModulesRoot, platPkg)
	nativeDir := filepath.Join(nodeModulesRoot, platPkg, "vendor", triple, "codex")
	pathDirExpected := filepath.Join(nodeModulesRoot, platPkg, "vendor", triple, "path")

	for _, d := range []string{binDir, platformPkgDir, nativeDir, pathDirExpected} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	wrapperPath := filepath.Join(binDir, "codex.js")
	if err := os.WriteFile(wrapperPath, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}
	if err := os.WriteFile(filepath.Join(platformPkgDir, "package.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write platform package.json: %v", err)
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

func TestFindNativeBinaryPrefersNestedPlatformSubPackage(t *testing.T) {
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

	nodeModulesRoot := filepath.Join(dir, "node_modules")
	pkgDir := filepath.Join(nodeModulesRoot, "@openai", "codex")
	binDir := filepath.Join(pkgDir, "bin")
	nestedPkgDir := filepath.Join(pkgDir, "node_modules", platPkg)
	nestedNativeDir := filepath.Join(pkgDir, "node_modules", platPkg, "vendor", triple, "codex")
	nestedPathDir := filepath.Join(pkgDir, "node_modules", platPkg, "vendor", triple, "path")
	siblingPkgDir := filepath.Join(nodeModulesRoot, platPkg)
	siblingNativeDir := filepath.Join(nodeModulesRoot, platPkg, "vendor", triple, "codex")
	siblingPathDir := filepath.Join(nodeModulesRoot, platPkg, "vendor", triple, "path")

	for _, d := range []string{binDir, nestedPkgDir, nestedNativeDir, nestedPathDir, siblingPkgDir, siblingNativeDir, siblingPathDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	wrapperPath := filepath.Join(binDir, "codex.js")
	if err := os.WriteFile(wrapperPath, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nestedPkgDir, "package.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write nested package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(siblingPkgDir, "package.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write sibling package.json: %v", err)
	}

	nestedNativePath := filepath.Join(nestedNativeDir, nativeBinaryName())
	if err := os.WriteFile(nestedNativePath, []byte("nested native binary"), 0o755); err != nil {
		t.Fatalf("write nested native: %v", err)
	}
	siblingNativePath := filepath.Join(siblingNativeDir, nativeBinaryName())
	if err := os.WriteFile(siblingNativePath, []byte("sibling native binary"), 0o755); err != nil {
		t.Fatalf("write sibling native: %v", err)
	}

	gotBin, gotPath, err := FindNativeBinary(wrapperPath)
	if err != nil {
		t.Fatalf("FindNativeBinary: %v", err)
	}
	if gotBin != nestedNativePath {
		t.Errorf("expected nested native binary %q, got %q", nestedNativePath, gotBin)
	}
	if gotPath != nestedPathDir {
		t.Errorf("expected nested path dir %q, got %q", nestedPathDir, gotPath)
	}
}

func TestFindNativeBinaryDoesNotFallBackAfterBrokenNestedPlatformPackage(t *testing.T) {
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

	nodeModulesRoot := filepath.Join(dir, "node_modules")
	pkgDir := filepath.Join(nodeModulesRoot, "@openai", "codex")
	binDir := filepath.Join(pkgDir, "bin")
	nestedPkgDir := filepath.Join(pkgDir, "node_modules", platPkg)
	siblingNativeDir := filepath.Join(nodeModulesRoot, platPkg, "vendor", triple, "codex")
	siblingPathDir := filepath.Join(nodeModulesRoot, platPkg, "vendor", triple, "path")

	for _, d := range []string{binDir, nestedPkgDir, siblingNativeDir, siblingPathDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	wrapperPath := filepath.Join(binDir, "codex.js")
	if err := os.WriteFile(wrapperPath, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}

	nestedPackageJSON := filepath.Join(nestedPkgDir, "package.json")
	if err := os.WriteFile(nestedPackageJSON, []byte("{}"), 0o644); err != nil {
		t.Fatalf("write nested package.json: %v", err)
	}

	siblingNativePath := filepath.Join(siblingNativeDir, nativeBinaryName())
	if err := os.WriteFile(siblingNativePath, []byte("sibling native binary"), 0o755); err != nil {
		t.Fatalf("write sibling native: %v", err)
	}

	_, _, err := FindNativeBinary(wrapperPath)
	if err == nil {
		t.Fatal("expected error for missing nested platform vendor binary")
	}
	if !strings.Contains(err.Error(), filepath.Join(nestedPkgDir, "vendor")) {
		t.Fatalf("expected error to mention nested vendor root, got %v", err)
	}
	if strings.Contains(err.Error(), siblingNativePath) {
		t.Fatalf("expected error to stop before sibling native binary, got %v", err)
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

func TestParseNpmCmdShimManagedNodeForm(t *testing.T) {
	dir := t.TempDir()

	jsDir := filepath.Join(dir, "node_modules", "@openai", "codex", "bin")
	if err := os.MkdirAll(jsDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	jsPath := filepath.Join(jsDir, "codex.js")
	if err := os.WriteFile(jsPath, []byte("// entry"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	cmdContent := `@echo off
set "_script=%~dp0node_modules\@openai\codex\bin\codex.js"
"%_prog%" "%_script%" %*
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
