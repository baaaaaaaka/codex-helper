//go:build !windows

package cloudgate

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// buildSyntheticBinary creates a fake binary containing the expected marker strings.
func buildSyntheticBinary(t *testing.T, markers ...string) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString("HEADER_PADDING_000")
	for _, m := range markers {
		buf.WriteString(m)
		buf.WriteString("\x00PADDING\x00")
	}
	buf.WriteString("TRAILER_PADDING")
	return buf.Bytes()
}

func TestPatchCodexBinaryBasic(t *testing.T) {
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	origPath := filepath.Join(dir, "codex")

	data := buildSyntheticBinary(t,
		origReqPath,
		"/api/codex/config/requirements",
		"/wham/config/requirements",
	)
	if err := os.WriteFile(origPath, data, 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	result, err := PatchCodexBinary(origPath, cacheDir)
	if err != nil {
		t.Fatalf("PatchCodexBinary: %v", err)
	}
	defer result.Cleanup()
	defer os.RemoveAll(filepath.Dir(patchedReqPath))

	if result.PatchedBinary == "" {
		t.Fatal("expected patched binary path")
	}

	patched, err := os.ReadFile(result.PatchedBinary)
	if err != nil {
		t.Fatalf("read patched: %v", err)
	}

	// Original paths should be gone.
	if bytes.Contains(patched, []byte(origReqPath)) {
		t.Error("patched binary still contains original requirements path")
	}
	if bytes.Contains(patched, []byte("/api/codex/config/requirements")) {
		t.Error("patched binary still contains original API path")
	}
	if bytes.Contains(patched, []byte("/wham/config/requirements")) {
		t.Error("patched binary still contains original WHAM path")
	}

	// Replacement paths should be present.
	if !bytes.Contains(patched, []byte(patchedReqPath)) {
		t.Error("patched binary missing new requirements path")
	}
	if !bytes.Contains(patched, []byte("/api/codex/config/requirementz")) {
		t.Error("patched binary missing sabotaged API path")
	}
	if !bytes.Contains(patched, []byte("/wham/config/requirementz")) {
		t.Error("patched binary missing sabotaged WHAM path")
	}

	// Requirements file should exist.
	reqData, err := os.ReadFile(patchedReqPath)
	if err != nil {
		t.Fatalf("read requirements: %v", err)
	}
	if !strings.Contains(string(reqData), `"never"`) {
		t.Error("requirements missing never policy")
	}
	if !strings.Contains(string(reqData), `"danger-full-access"`) {
		t.Error("requirements missing danger-full-access sandbox mode")
	}
}

func TestPatchCodexBinaryIdempotent(t *testing.T) {
	dir := t.TempDir()

	// Binary that already has the patched paths (simulating a re-patch).
	data := buildSyntheticBinary(t,
		patchedReqPath,
		"/api/codex/config/requirementz",
		"/wham/config/requirementz",
	)
	origPath := filepath.Join(dir, "codex")
	if err := os.WriteFile(origPath, data, 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	cacheDir := filepath.Join(dir, "cache")
	result, err := PatchCodexBinary(origPath, cacheDir)
	if err != nil {
		t.Fatalf("PatchCodexBinary: %v", err)
	}

	// No patches needed — should return empty result.
	if result.PatchedBinary != "" {
		t.Errorf("expected empty PatchedBinary for already-patched binary, got %q", result.PatchedBinary)
	}
}

func TestPatchCodexBinaryMultipleOccurrences(t *testing.T) {
	dir := t.TempDir()

	// Binary with the requirements path appearing twice.
	data := buildSyntheticBinary(t,
		origReqPath,
		"MIDDLE",
		origReqPath,
	)
	origPath := filepath.Join(dir, "codex")
	if err := os.WriteFile(origPath, data, 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	cacheDir := filepath.Join(dir, "cache")
	_, err := PatchCodexBinary(origPath, cacheDir)
	if err == nil {
		t.Fatal("expected error for multiple occurrences")
	}
	if !strings.Contains(err.Error(), "expected 1 occurrence") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPatchCodexBinaryNoTarget(t *testing.T) {
	dir := t.TempDir()

	// Binary with none of the target strings.
	data := buildSyntheticBinary(t, "unrelated content only")
	origPath := filepath.Join(dir, "codex")
	if err := os.WriteFile(origPath, data, 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	cacheDir := filepath.Join(dir, "cache")
	result, err := PatchCodexBinary(origPath, cacheDir)
	if err != nil {
		t.Fatalf("PatchCodexBinary: %v", err)
	}

	if result.PatchedBinary != "" {
		t.Errorf("expected empty result for binary without targets, got %q", result.PatchedBinary)
	}
}

func TestPatchCodexBinaryMissingFile(t *testing.T) {
	_, err := PatchCodexBinary("/nonexistent/binary", t.TempDir())
	if err == nil {
		t.Fatal("expected error for missing binary")
	}
}

func TestPatchCodexBinaryPartialPatches(t *testing.T) {
	dir := t.TempDir()

	// Binary with only the API path (no system req path, no WHAM path).
	data := buildSyntheticBinary(t,
		"/api/codex/config/requirements",
	)
	origPath := filepath.Join(dir, "codex")
	if err := os.WriteFile(origPath, data, 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	cacheDir := filepath.Join(dir, "cache")
	result, err := PatchCodexBinary(origPath, cacheDir)
	if err != nil {
		t.Fatalf("PatchCodexBinary: %v", err)
	}
	defer result.Cleanup()
	defer os.RemoveAll(filepath.Dir(patchedReqPath))

	if result.PatchedBinary == "" {
		t.Fatal("expected patched binary for partial match")
	}

	patched, err := os.ReadFile(result.PatchedBinary)
	if err != nil {
		t.Fatalf("read patched: %v", err)
	}
	if bytes.Contains(patched, []byte("/api/codex/config/requirements")) {
		t.Error("API path should be patched")
	}
	if !bytes.Contains(patched, []byte("/api/codex/config/requirementz")) {
		t.Error("sabotaged API path missing")
	}
}

func TestPatchLengthConsistency(t *testing.T) {
	// Verify all patch pairs have equal byte lengths.
	if len(origReqPath) != len(patchedReqPath) {
		t.Errorf("requirements path length mismatch: %d vs %d", len(origReqPath), len(patchedReqPath))
	}
	for _, p := range cloudRequirementsPatches {
		if len(p.old) != len(p.new) {
			t.Errorf("%s: length mismatch: %d vs %d", p.name, len(p.old), len(p.new))
		}
	}
}

func TestPermissiveRequirementsFormat(t *testing.T) {
	// Validate the TOML content matches expected format.
	if !strings.Contains(permissiveRequirements, "allowed_approval_policies") {
		t.Error("missing allowed_approval_policies key")
	}
	if !strings.Contains(permissiveRequirements, "allowed_sandbox_modes") {
		t.Error("missing allowed_sandbox_modes key")
	}

	// Check all expected values are present.
	expectedPolicies := []string{"never", "on-request", "on-failure", "untrusted"}
	for _, p := range expectedPolicies {
		if !strings.Contains(permissiveRequirements, `"`+p+`"`) {
			t.Errorf("missing approval policy %q", p)
		}
	}

	expectedSandboxModes := []string{"danger-full-access", "workspace-write", "read-only"}
	for _, m := range expectedSandboxModes {
		if !strings.Contains(permissiveRequirements, `"`+m+`"`) {
			t.Errorf("missing sandbox mode %q", m)
		}
	}
}

func TestCleanupRemovesPatchedBinary(t *testing.T) {
	dir := t.TempDir()
	patchedPath := filepath.Join(dir, "codex-patched")
	if err := os.WriteFile(patchedPath, []byte("test"), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	result := &PatchResult{PatchedBinary: patchedPath}
	result.Cleanup()

	if _, err := os.Stat(patchedPath); err == nil {
		t.Error("patched binary should be removed after Cleanup")
	}
}

func TestPatchResultCleanupNilSafe(t *testing.T) {
	var result *PatchResult
	result.Cleanup() // should not panic

	result2 := &PatchResult{}
	result2.Cleanup() // empty path, should not panic
}
