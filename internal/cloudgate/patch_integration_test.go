package cloudgate

import (
	"bytes"
	"fmt"
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

func validatePatchedMarkerState(original, patched []byte, orig string, replacement string) error {
	origCount := bytes.Count(original, []byte(orig))
	patchedOrigCount := bytes.Count(patched, []byte(orig))
	if patchedOrigCount != 0 {
		return fmt.Errorf("patched binary still contains %d occurrences of %q", patchedOrigCount, orig)
	}
	if origCount == 0 {
		return nil
	}
	patchedReplacementCount := bytes.Count(patched, []byte(replacement))
	if patchedReplacementCount < origCount {
		return fmt.Errorf(
			"patched binary contains %d occurrences of %q, want at least %d",
			patchedReplacementCount,
			replacement,
			origCount,
		)
	}
	return nil
}

func TestValidatePatchedMarkerState(t *testing.T) {
	t.Run("requires replacement when original marker existed", func(t *testing.T) {
		err := validatePatchedMarkerState(
			[]byte("before /api/codex/config/requirements after"),
			[]byte("before /api/codex/config/requirementz after"),
			"/api/codex/config/requirements",
			"/api/codex/config/requirementz",
		)
		if err != nil {
			t.Fatalf("validatePatchedMarkerState returned error: %v", err)
		}
	})

	t.Run("allows binaries that never embedded the original marker", func(t *testing.T) {
		err := validatePatchedMarkerState(
			[]byte("no requirements path here"),
			[]byte("still no requirements path here"),
			origReqPath,
			"/tmp/cx123456-7890/reqs.toml",
		)
		if err != nil {
			t.Fatalf("validatePatchedMarkerState returned error: %v", err)
		}
	})

	t.Run("fails when original marker remains", func(t *testing.T) {
		err := validatePatchedMarkerState(
			[]byte("before chatgpt_plan_type after"),
			[]byte("before chatgpt_plan_type after"),
			"chatgpt_plan_type",
			"chatgpt_plan_typf",
		)
		if err == nil {
			t.Fatal("validatePatchedMarkerState should have failed")
		}
	})
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
	original, err := os.ReadFile(nativeBin)
	if err != nil {
		t.Fatalf("read original binary: %v", err)
	}

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

	checks := []struct {
		orig    string
		patched string
	}{
		{orig: origReqPath, patched: result.RequirementsPath},
		{orig: "/api/codex/config/requirements", patched: "/api/codex/config/requirementz"},
		{orig: "/wham/config/requirements", patched: "/wham/config/requirementz"},
		{orig: "chatgpt_plan_type", patched: "chatgpt_plan_typf"},
		{orig: "allowed_approval_policies", patched: "allowed_approval_policiez"},
		{orig: "allowed_sandbox_modes", patched: "allowed_sandbox_modez"},
	}
	for _, check := range checks {
		if err := validatePatchedMarkerState(original, patched, check.orig, check.patched); err != nil {
			t.Fatal(err)
		}
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
