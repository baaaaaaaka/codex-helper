package cloudgate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

const (
	// Original path embedded in the Codex binary (28 bytes).
	origReqPath = "/etc/codex/requirements.toml"

	// Replacement path — same length (28 bytes), user-writable.
	patchedReqPath = "/tmp/cxreq/requirements.toml"

	// Permissive requirements TOML that allows all policies.
	// Values must be lowercase kebab-case to match Codex's serde deserialization.
	permissiveRequirements = `allowed_approval_policies = ["never", "on-request", "on-failure", "untrusted"]
allowed_sandbox_modes = ["danger-full-access", "workspace-write", "read-only"]
`
)

// binaryPatch defines a single byte-level patch: find old, replace with new (same length).
type binaryPatch struct {
	old  []byte
	new  []byte
	name string // for error messages
}

// cloudRequirementsPatches sabotages the cloud requirements URL paths so the
// server returns 404. The binary's fail-open behavior then treats this as
// "no cloud requirements", allowing all approval policies.
var cloudRequirementsPatches = []binaryPatch{
	{
		// "/api/codex/config/requirements" (30 bytes) → change last char 's' → 'z'
		old:  []byte("/api/codex/config/requirements"),
		new:  []byte("/api/codex/config/requirementz"),
		name: "cloud requirements API path",
	},
	{
		// "/wham/config/requirements" (25 bytes) → change last char 's' → 'z'
		old:  []byte("/wham/config/requirements"),
		new:  []byte("/wham/config/requirementz"),
		name: "cloud requirements WHAM path",
	},
}

// PatchResult holds the results of patching a Codex binary.
type PatchResult struct {
	// PatchedBinary is the path to the patched binary, or empty if patching
	// was skipped.
	PatchedBinary string
	// RequirementsPath is the path to the permissive requirements file.
	RequirementsPath string
	// OrigSHA256 is the SHA-256 hex digest of the original binary before patching.
	OrigSHA256 string
}

// RemoveCloudRequirementsCache deletes the cloud requirements cache file
// from the given Codex data directory. Codex caches cloud requirements at
// <codexDir>/cloud-requirements-cache.json; if this cache exists, Codex
// skips the URL fetch entirely, bypassing our binary URL-sabotage patches.
func RemoveCloudRequirementsCache(codexDir string) error {
	if codexDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		codexDir = filepath.Join(home, ".codex")
	}
	p := filepath.Join(codexDir, "cloud-requirements-cache.json")
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Cleanup removes the patched binary and requirements file.
func (r *PatchResult) Cleanup() {
	if r == nil {
		return
	}
	if r.PatchedBinary != "" {
		_ = os.Remove(r.PatchedBinary)
	}
}

// PatchCodexBinary patches a Codex binary:
//  1. Redirects the system requirements path to a user-writable permissive file.
//  2. Sabotages the cloud requirements URL paths so the fetch 404s (fail-open).
//
// The original binary is not modified; a copy is placed in cacheDir.
func PatchCodexBinary(origBinary string, cacheDir string) (*PatchResult, error) {
	data, err := os.ReadFile(origBinary)
	if err != nil {
		return nil, fmt.Errorf("read binary: %w", err)
	}

	sum := sha256.Sum256(data)
	origHash := hex.EncodeToString(sum[:])

	patched := false

	// Patch 1: Redirect system requirements path.
	old := []byte(origReqPath)
	new := []byte(patchedReqPath)
	if len(old) != len(new) {
		return nil, fmt.Errorf("path length mismatch: %d vs %d", len(old), len(new))
	}
	if count := bytes.Count(data, old); count == 1 {
		data = bytes.Replace(data, old, new, 1)
		patched = true
	} else if count > 1 {
		return nil, fmt.Errorf("expected 1 occurrence of %q, found %d", origReqPath, count)
	}
	// count == 0: already patched or not present, continue with other patches.

	// Patch 2: Sabotage cloud requirements URL paths.
	for _, p := range cloudRequirementsPatches {
		if len(p.old) != len(p.new) {
			return nil, fmt.Errorf("%s: length mismatch %d vs %d", p.name, len(p.old), len(p.new))
		}
		if count := bytes.Count(data, p.old); count == 1 {
			data = bytes.Replace(data, p.old, p.new, 1)
			patched = true
		}
		// count == 0: already patched; count > 1: ambiguous, skip.
		// count == 0: already patched, skip.
	}

	if !patched {
		return &PatchResult{OrigSHA256: origHash}, nil
	}

	// Write patched binary.
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	patchedName := "codex-patched"
	if runtime.GOOS == "windows" {
		patchedName = "codex-patched.exe"
	}
	patchedPath := filepath.Join(cacheDir, patchedName)
	if err := os.WriteFile(patchedPath, data, 0o755); err != nil {
		return nil, fmt.Errorf("write patched binary: %w", err)
	}

	// Write permissive requirements.
	reqDir := filepath.Dir(patchedReqPath)
	if err := os.MkdirAll(reqDir, 0o755); err != nil {
		return nil, fmt.Errorf("create requirements dir: %w", err)
	}
	if err := os.WriteFile(patchedReqPath, []byte(permissiveRequirements), 0o644); err != nil {
		return nil, fmt.Errorf("write requirements: %w", err)
	}

	return &PatchResult{
		PatchedBinary:    patchedPath,
		RequirementsPath: patchedReqPath,
		OrigSHA256:       origHash,
	}, nil
}
