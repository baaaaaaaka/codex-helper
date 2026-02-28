package cloudgate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func buildMachOSyntheticBinary(t *testing.T, markers ...string) []byte {
	t.Helper()
	data := buildSyntheticBinary(t, markers...)
	copy(data[:4], []byte{0xFE, 0xED, 0xFA, 0xCF})
	return data
}

func TestLooksLikeMachO(t *testing.T) {
	for i, magic := range machOMagics {
		data := append([]byte{}, magic...)
		data = append(data, 0x00, 0x00)
		if !looksLikeMachO(data) {
			t.Fatalf("expected magic[%d] to be recognized as Mach-O", i)
		}
	}

	if looksLikeMachO([]byte("not-a-mach-o")) {
		t.Fatal("unexpected Mach-O detection for plain text")
	}
	if looksLikeMachO([]byte{0xCA, 0xFE, 0xBA}) {
		t.Fatal("unexpected Mach-O detection for short buffer")
	}
}

func TestPatchCodexBinaryWithRuntime_DarwinMachOSigns(t *testing.T) {
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	origPath := filepath.Join(dir, "codex")

	data := buildMachOSyntheticBinary(t, origReqPath, "/api/codex/config/requirements")
	if err := os.WriteFile(origPath, data, 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	called := false
	result, err := patchCodexBinaryWithRuntime(origPath, cacheDir, "darwin", func(path string) error {
		called = true
		if _, statErr := os.Stat(path); statErr != nil {
			t.Fatalf("sign target should exist: %v", statErr)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("patchCodexBinaryWithRuntime: %v", err)
	}
	defer result.Cleanup()
	defer os.RemoveAll(filepath.Dir(patchedReqPath))

	if !called {
		t.Fatal("expected darwin Mach-O patch to invoke codesign")
	}
	if result.PatchedBinary == "" {
		t.Fatal("expected patched binary")
	}
}

func TestPatchCodexBinaryWithRuntime_DarwinSignFailureCleansPatchedBinary(t *testing.T) {
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	origPath := filepath.Join(dir, "codex")

	data := buildMachOSyntheticBinary(t, origReqPath, "/api/codex/config/requirements")
	if err := os.WriteFile(origPath, data, 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := patchCodexBinaryWithRuntime(origPath, cacheDir, "darwin", func(string) error {
		return fmt.Errorf("codesign failed")
	})
	if err == nil {
		t.Fatal("expected error when codesign fails")
	}
	if !strings.Contains(err.Error(), "ad-hoc sign patched binary") {
		t.Fatalf("unexpected error: %v", err)
	}

	matches, globErr := filepath.Glob(filepath.Join(cacheDir, "codex-patched-*"))
	if globErr != nil {
		t.Fatalf("glob patched binaries: %v", globErr)
	}
	if len(matches) != 0 {
		t.Fatalf("expected patched binary cleanup on sign failure, found %v", matches)
	}
}

func TestPatchCodexBinaryWithRuntime_NonDarwinSkipsCodesign(t *testing.T) {
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	origPath := filepath.Join(dir, "codex")

	data := buildMachOSyntheticBinary(t, origReqPath, "/api/codex/config/requirements")
	if err := os.WriteFile(origPath, data, 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	called := false
	result, err := patchCodexBinaryWithRuntime(origPath, cacheDir, "linux", func(string) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("patchCodexBinaryWithRuntime: %v", err)
	}
	defer result.Cleanup()
	defer os.RemoveAll(filepath.Dir(patchedReqPath))

	if called {
		t.Fatal("did not expect codesign on non-darwin runtime")
	}
}

func TestPatchCodexBinaryWithRuntime_DarwinNonMachOSkipsCodesign(t *testing.T) {
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	origPath := filepath.Join(dir, "codex")

	data := buildSyntheticBinary(t, origReqPath, "/api/codex/config/requirements")
	if err := os.WriteFile(origPath, data, 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	called := false
	result, err := patchCodexBinaryWithRuntime(origPath, cacheDir, "darwin", func(string) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("patchCodexBinaryWithRuntime: %v", err)
	}
	defer result.Cleanup()
	defer os.RemoveAll(filepath.Dir(patchedReqPath))

	if called {
		t.Fatal("did not expect codesign for non-Mach-O binary")
	}
}

func TestPatchCodexBinaryBasic(t *testing.T) {
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	origPath := filepath.Join(dir, "codex")

	data := buildSyntheticBinary(t,
		origReqPath,
		"/api/codex/config/requirements",
		"/wham/config/requirements",
		"allowed_approval_policies",
		"allowed_sandbox_modes",
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

	// Original TOML keys should be gone.
	if bytes.Contains(patched, []byte("allowed_approval_policies")) {
		t.Error("patched binary still contains original approval policies key")
	}
	if bytes.Contains(patched, []byte("allowed_sandbox_modes")) {
		t.Error("patched binary still contains original sandbox modes key")
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

	// Patched TOML keys should be present.
	if !bytes.Contains(patched, []byte("allowed_approval_policiez")) {
		t.Error("patched binary missing renamed approval policies key")
	}
	if !bytes.Contains(patched, []byte("allowed_sandbox_modez")) {
		t.Error("patched binary missing renamed sandbox modes key")
	}

	// Requirements file should contain both original and patched key names.
	reqData, err := os.ReadFile(patchedReqPath)
	if err != nil {
		t.Fatalf("read requirements: %v", err)
	}
	reqStr := string(reqData)
	if !strings.Contains(reqStr, `"never"`) {
		t.Error("requirements missing never policy")
	}
	if !strings.Contains(reqStr, `"danger-full-access"`) {
		t.Error("requirements missing danger-full-access sandbox mode")
	}
	for _, key := range []string{
		"allowed_approval_policies",
		"allowed_approval_policiez",
		"allowed_sandbox_modes",
		"allowed_sandbox_modez",
	} {
		if !strings.Contains(reqStr, key) {
			t.Errorf("requirements missing key %q", key)
		}
	}
}

func TestPatchCodexBinaryIdempotent(t *testing.T) {
	dir := t.TempDir()

	// Binary that already has the patched paths (simulating a re-patch).
	data := buildSyntheticBinary(t,
		patchedReqPath,
		"/api/codex/config/requirementz",
		"/wham/config/requirementz",
		"allowed_approval_policiez",
		"allowed_sandbox_modez",
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
	for _, p := range tomlKeyPatches {
		if len(p.old) != len(p.new) {
			t.Errorf("%s: length mismatch: %d vs %d", p.name, len(p.old), len(p.new))
		}
	}
}

func TestPermissiveRequirementsFormat(t *testing.T) {
	// Both original and patched key names must be present so the file
	// works regardless of whether the TOML-key binary patches applied.
	for _, key := range []string{
		"allowed_approval_policies",
		"allowed_approval_policiez",
		"allowed_sandbox_modes",
		"allowed_sandbox_modez",
	} {
		if !strings.Contains(permissiveRequirements, key) {
			t.Errorf("missing key %q", key)
		}
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
	leasePath := patchLeasePath(patchedPath)
	if err := os.WriteFile(leasePath, []byte("123\n"), 0o600); err != nil {
		t.Fatalf("write lease: %v", err)
	}

	result := &PatchResult{PatchedBinary: patchedPath, leasePath: leasePath}
	result.Cleanup()

	if _, err := os.Stat(patchedPath); err == nil {
		t.Error("patched binary should be removed after Cleanup")
	}
	if _, err := os.Stat(leasePath); err == nil {
		t.Error("patch lease should be removed after Cleanup")
	}
}

func writeManagedLeaseForTest(t *testing.T, leasePath string, pid int, heartbeatAt time.Time, modTime time.Time) {
	t.Helper()
	if err := writePatchLease(leasePath, pid, heartbeatAt); err != nil {
		t.Fatalf("write managed lease: %v", err)
	}
	if !modTime.IsZero() {
		if err := os.Chtimes(leasePath, modTime, modTime); err != nil {
			t.Fatalf("set lease modtime: %v", err)
		}
	}
}

func TestCleanupStalePatchedBinariesRemovesOnlyStaleManagedLeases(t *testing.T) {
	cacheDir := t.TempDir()
	staleTime := time.Now().Add(-patchLeaseStaleAfter - time.Minute)

	stalePath := filepath.Join(cacheDir, "codex-patched-stale")
	if err := os.WriteFile(stalePath, []byte("stale"), 0o755); err != nil {
		t.Fatalf("write stale patch: %v", err)
	}
	writeManagedLeaseForTest(t, patchLeasePath(stalePath), 2147483647, staleTime, staleTime)

	livePath := filepath.Join(cacheDir, "codex-patched-live")
	if err := os.WriteFile(livePath, []byte("live"), 0o755); err != nil {
		t.Fatalf("write live patch: %v", err)
	}
	writeManagedLeaseForTest(t, patchLeasePath(livePath), 2147483647, time.Now(), time.Now())

	staleLivePIDPath := filepath.Join(cacheDir, "codex-patched-stale-live-pid")
	if err := os.WriteFile(staleLivePIDPath, []byte("stale-live-pid"), 0o755); err != nil {
		t.Fatalf("write stale-live-pid patch: %v", err)
	}
	writeManagedLeaseForTest(t, patchLeasePath(staleLivePIDPath), os.Getpid(), staleTime, staleTime)

	unleasedPath := filepath.Join(cacheDir, "codex-patched-unleased")
	if err := os.WriteFile(unleasedPath, []byte("unleased"), 0o755); err != nil {
		t.Fatalf("write unleased patch: %v", err)
	}

	cleanupStalePatchedBinaries(cacheDir)

	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale patch should be removed, stat err=%v", err)
	}
	if _, err := os.Stat(patchLeasePath(stalePath)); !os.IsNotExist(err) {
		t.Fatalf("stale lease should be removed, stat err=%v", err)
	}

	if _, err := os.Stat(livePath); err != nil {
		t.Fatalf("live patch should be preserved: %v", err)
	}
	if _, err := os.Stat(patchLeasePath(livePath)); err != nil {
		t.Fatalf("live lease should be preserved: %v", err)
	}

	if _, err := os.Stat(staleLivePIDPath); err != nil {
		t.Fatalf("stale lease with live local pid should be preserved: %v", err)
	}
	if _, err := os.Stat(patchLeasePath(staleLivePIDPath)); err != nil {
		t.Fatalf("stale lease with live local pid should be preserved: %v", err)
	}

	if _, err := os.Stat(unleasedPath); err != nil {
		t.Fatalf("unleased patch should be preserved: %v", err)
	}
}

func TestCleanupStalePatchedBinariesPreservesInvalidLease(t *testing.T) {
	cacheDir := t.TempDir()
	path := filepath.Join(cacheDir, "codex-patched-invalid-lease")
	if err := os.WriteFile(path, []byte("bin"), 0o755); err != nil {
		t.Fatalf("write patch: %v", err)
	}
	lease := patchLeasePath(path)
	if err := os.WriteFile(lease, []byte("not-a-pid\n"), 0o600); err != nil {
		t.Fatalf("write invalid lease: %v", err)
	}

	cleanupStalePatchedBinaries(cacheDir)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("patch with invalid lease should be preserved: %v", err)
	}
	if _, err := os.Stat(lease); err != nil {
		t.Fatalf("invalid lease should be preserved: %v", err)
	}
}

func TestCleanupStalePatchedBinariesPreservesLegacyPIDLease(t *testing.T) {
	cacheDir := t.TempDir()
	path := filepath.Join(cacheDir, "codex-patched-legacy-pid")
	if err := os.WriteFile(path, []byte("bin"), 0o755); err != nil {
		t.Fatalf("write patch: %v", err)
	}
	lease := patchLeasePath(path)
	if err := os.WriteFile(lease, []byte("2147483647\n"), 0o600); err != nil {
		t.Fatalf("write legacy lease: %v", err)
	}

	cleanupStalePatchedBinaries(cacheDir)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("patch with legacy pid lease should be preserved: %v", err)
	}
	if _, err := os.Stat(lease); err != nil {
		t.Fatalf("legacy pid lease should be preserved: %v", err)
	}
}

func TestPatchResultCleanupNilSafe(t *testing.T) {
	var result *PatchResult
	result.Cleanup() // should not panic

	result2 := &PatchResult{}
	result2.Cleanup() // empty path, should not panic
}

func TestPatchCodexBinaryOrigSHA256_Patched(t *testing.T) {
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	origPath := filepath.Join(dir, "codex")

	data := buildSyntheticBinary(t,
		origReqPath,
		"/api/codex/config/requirements",
		"/wham/config/requirements",
		"allowed_approval_policies",
		"allowed_sandbox_modes",
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

	// OrigSHA256 should match the SHA-256 of the original binary data.
	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	if result.OrigSHA256 != want {
		t.Errorf("OrigSHA256 = %q, want %q", result.OrigSHA256, want)
	}
}

func TestPatchCodexBinaryOrigSHA256_NoPatchNeeded(t *testing.T) {
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	origPath := filepath.Join(dir, "codex")

	// Binary that already has the patched paths — no patches needed.
	data := buildSyntheticBinary(t,
		patchedReqPath,
		"/api/codex/config/requirementz",
		"/wham/config/requirementz",
		"allowed_approval_policiez",
		"allowed_sandbox_modez",
	)
	if err := os.WriteFile(origPath, data, 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	result, err := PatchCodexBinary(origPath, cacheDir)
	if err != nil {
		t.Fatalf("PatchCodexBinary: %v", err)
	}

	// Even when no patches applied, OrigSHA256 should be set.
	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	if result.OrigSHA256 != want {
		t.Errorf("OrigSHA256 = %q, want %q (even when no patches applied)", result.OrigSHA256, want)
	}
	if result.PatchedBinary != "" {
		t.Errorf("expected empty PatchedBinary, got %q", result.PatchedBinary)
	}
}

func TestPatchCodexBinaryOrigSHA256_NoTargetStrings(t *testing.T) {
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	origPath := filepath.Join(dir, "codex")

	data := buildSyntheticBinary(t, "unrelated content only")
	if err := os.WriteFile(origPath, data, 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	result, err := PatchCodexBinary(origPath, cacheDir)
	if err != nil {
		t.Fatalf("PatchCodexBinary: %v", err)
	}

	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	if result.OrigSHA256 != want {
		t.Errorf("OrigSHA256 = %q, want %q", result.OrigSHA256, want)
	}
}

func TestPatchCodexBinaryOrigSHA256_DifferentFromPatched(t *testing.T) {
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	origPath := filepath.Join(dir, "codex")

	data := buildSyntheticBinary(t,
		origReqPath,
		"/api/codex/config/requirements",
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
		t.Fatal("expected patched binary")
	}

	// Read patched binary and compute its hash.
	patchedData, err := os.ReadFile(result.PatchedBinary)
	if err != nil {
		t.Fatalf("read patched: %v", err)
	}
	patchedSum := sha256.Sum256(patchedData)
	patchedHash := hex.EncodeToString(patchedSum[:])

	// OrigSHA256 should differ from the patched binary's hash.
	if result.OrigSHA256 == patchedHash {
		t.Error("OrigSHA256 should differ from the patched binary's hash")
	}
}
