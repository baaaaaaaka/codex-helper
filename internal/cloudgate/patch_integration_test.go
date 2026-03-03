package cloudgate

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func pathWithinTempRoot(path string) bool {
	path = filepath.Clean(path)
	tmpRoot := filepath.Clean(os.TempDir())
	if path == "" || tmpRoot == "" || path == "." || tmpRoot == "." {
		return false
	}

	a := path
	b := tmpRoot
	if runtime.GOOS == "windows" {
		a = strings.ToLower(a)
		b = strings.ToLower(b)
	}

	if a == b {
		return true
	}
	if !strings.HasSuffix(b, string(filepath.Separator)) {
		b += string(filepath.Separator)
	}
	return strings.HasPrefix(a, b)
}

func cleanupRequirementsDir(path string) {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) {
		return
	}
	dir := filepath.Clean(filepath.Dir(path))
	if dir == "." || dir == string(filepath.Separator) {
		return
	}
	if !pathWithinTempRoot(dir) || dir == filepath.Clean(os.TempDir()) {
		return
	}
	_ = os.RemoveAll(dir)
}

func TestCleanupRequirementsDirEmptySafe(t *testing.T) {
	cleanupRequirementsDir("")
	cleanupRequirementsDir("   ")
}

func TestCleanupRequirementsDirRelativeSafe(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "marker")
	if err := os.WriteFile(marker, []byte("ok"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	cleanupRequirementsDir("relative/reqs.toml")
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("relative path should be ignored, marker missing: %v", err)
	}
}

func TestCleanupRequirementsDirRemovesTmpDir(t *testing.T) {
	dir := t.TempDir()
	reqPath := filepath.Join(dir, "reqs.toml")
	if err := os.WriteFile(reqPath, []byte("ok"), 0o644); err != nil {
		t.Fatalf("write req: %v", err)
	}
	cleanupRequirementsDir(reqPath)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("tmp requirements dir should be removed, stat err=%v", err)
	}
}

// TestCodexPatchIntegration is an integration test that requires a real Codex
// installation. It is skipped unless CODEX_PATCH_TEST=1 is set.
// CI installs Codex via npm before running this test.
func TestCodexPatchIntegration(t *testing.T) {
	if os.Getenv("CODEX_PATCH_TEST") != "1" {
		t.Skip("CODEX_PATCH_TEST not set")
	}

	// 1. Find the codex wrapper via PATH.
	wrapper, err := exec.LookPath("codex")
	if err != nil {
		t.Fatalf("codex not found in PATH: %v", err)
	}
	t.Logf("codex wrapper: %s", wrapper)

	// 2. Locate the native binary.
	nativeBin, _, err := FindNativeBinary(wrapper)
	if err != nil {
		t.Skipf("FindNativeBinary: %v (native binary not bundled for this platform)", err)
	}
	t.Logf("native binary: %s", nativeBin)

	// 3. Patch the binary into a temp directory.
	cacheDir := filepath.Join(t.TempDir(), "patch-cache")
	result, err := PatchCodexBinary(nativeBin, cacheDir)
	if err != nil {
		t.Fatalf("PatchCodexBinary: %v", err)
	}
	defer result.Cleanup()
	defer cleanupRequirementsDir(result.RequirementsPath)

	if result.PatchedBinary == "" {
		t.Fatal("expected a patched binary (binary may already be patched)")
	}
	t.Logf("patched binary: %s", result.PatchedBinary)

	// 4. Verify the patched binary no longer contains the original requirements path.
	patched, err := os.ReadFile(result.PatchedBinary)
	if err != nil {
		t.Fatalf("read patched binary: %v", err)
	}
	if bytes.Contains(patched, []byte(origReqPath)) {
		t.Error("patched binary still contains original requirements path")
	}

	// 5. Verify the permissive requirements file was written correctly.
	reqData, err := os.ReadFile(result.RequirementsPath)
	if err != nil {
		t.Fatalf("read requirements file: %v", err)
	}
	reqStr := string(reqData)
	if !strings.Contains(reqStr, "allowed_approval_policies") {
		t.Error("requirements file missing allowed_approval_policies")
	}
	if !strings.Contains(reqStr, "allowed_sandbox_modes") {
		t.Error("requirements file missing allowed_sandbox_modes")
	}
	t.Logf("requirements written to: %s", result.RequirementsPath)

	// 6. Verify patched binary is executable (especially important on macOS).
	cmd := exec.Command(result.PatchedBinary, "--version")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("patched binary is not executable: %v\noutput: %s", err, string(out))
	}
	if !strings.Contains(strings.ToLower(string(out)), "codex") {
		t.Fatalf("unexpected --version output from patched binary: %s", string(out))
	}
}
