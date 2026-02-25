package cloudgate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	// Original path embedded in the Codex binary (28 bytes).
	origReqPath = "/etc/codex/requirements.toml"

	// Replacement path — same length (28 bytes), user-writable.
	patchedReqPath = "/tmp/cxreq/requirements.toml"

	// Permissive requirements TOML that allows all policies.
	// Values must be lowercase kebab-case to match Codex's serde deserialization.
	//
	// Both original and patched key names are included so the file works
	// regardless of whether the TOML-key binary patches were applied.
	// Codex's ConfigRequirementsToml does NOT use serde(deny_unknown_fields),
	// so the unrecognized duplicate keys are silently ignored.
	permissiveRequirements = `allowed_approval_policies = ["never", "on-request", "on-failure", "untrusted"]
allowed_approval_policiez = ["never", "on-request", "on-failure", "untrusted"]
allowed_sandbox_modes = ["danger-full-access", "workspace-write", "read-only"]
allowed_sandbox_modez = ["danger-full-access", "workspace-write", "read-only"]
`

	adhocCodesignTimeout = 10 * time.Second
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

// tomlKeyPatches renames the serde field names that Codex uses to
// deserialize cloud requirements TOML. When these keys are patched,
// the cloud requirements response (which uses the original key names)
// can no longer be parsed, causing Codex to treat it as "no cloud
// requirements" (fail-open). The local permissive requirements file
// uses the patched key names so it is still parsed correctly.
//
// This is necessary because ChatGPT-authenticated sessions fetch cloud
// requirements via the multiplexed backend-api connection rather than
// dedicated REST endpoints, so URL sabotage alone is insufficient.
var tomlKeyPatches = []binaryPatch{
	{
		// "allowed_approval_policies" (25 bytes) → change last char 's' → 'z'
		old:  []byte("allowed_approval_policies"),
		new:  []byte("allowed_approval_policiez"),
		name: "approval policies TOML key",
	},
	{
		// "allowed_sandbox_modes" (21 bytes) → change last char 's' → 'z'
		old:  []byte("allowed_sandbox_modes"),
		new:  []byte("allowed_sandbox_modez"),
		name: "sandbox modes TOML key",
	},
}

// Mach-O and FAT magic numbers in byte order as they appear on disk.
var machOMagics = [][]byte{
	{0xFE, 0xED, 0xFA, 0xCE}, // MH_MAGIC
	{0xCE, 0xFA, 0xED, 0xFE}, // MH_CIGAM
	{0xFE, 0xED, 0xFA, 0xCF}, // MH_MAGIC_64
	{0xCF, 0xFA, 0xED, 0xFE}, // MH_CIGAM_64
	{0xCA, 0xFE, 0xBA, 0xBE}, // FAT_MAGIC
	{0xBE, 0xBA, 0xFE, 0xCA}, // FAT_CIGAM
	{0xCA, 0xFE, 0xBA, 0xBF}, // FAT_MAGIC_64
	{0xBF, 0xBA, 0xFE, 0xCA}, // FAT_CIGAM_64
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

func looksLikeMachO(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	header := data[:4]
	for _, magic := range machOMagics {
		if bytes.Equal(header, magic) {
			return true
		}
	}
	return false
}

func adHocCodesign(path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), adhocCodesignTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "codesign", "-s", "-", "--force", path)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("codesign timed out after %s", adhocCodesignTimeout)
	}
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg != "" {
			return fmt.Errorf("%w: %s", err, msg)
		}
		return err
	}
	return nil
}

func patchCodexBinaryWithRuntime(
	origBinary string,
	cacheDir string,
	goos string,
	codesignFn func(string) error,
) (*PatchResult, error) {
	if codesignFn == nil {
		codesignFn = adHocCodesign
	}

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
	}

	// Patch 3: Rename TOML field names used for cloud requirements parsing.
	for _, p := range tomlKeyPatches {
		if len(p.old) != len(p.new) {
			return nil, fmt.Errorf("%s: length mismatch %d vs %d", p.name, len(p.old), len(p.new))
		}
		if count := bytes.Count(data, p.old); count == 1 {
			data = bytes.Replace(data, p.old, p.new, 1)
			patched = true
		}
		// count == 0: already patched; count > 1: ambiguous, skip.
	}

	if !patched {
		return &PatchResult{OrigSHA256: origHash}, nil
	}

	// Write patched binary.
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	patchedName := "codex-patched"
	if goos == "windows" {
		patchedName = "codex-patched.exe"
	}
	patchedPath := filepath.Join(cacheDir, patchedName)
	if err := os.WriteFile(patchedPath, data, 0o755); err != nil {
		return nil, fmt.Errorf("write patched binary: %w", err)
	}
	if goos == "darwin" && looksLikeMachO(data) {
		if err := codesignFn(patchedPath); err != nil {
			_ = os.Remove(patchedPath)
			return nil, fmt.Errorf("ad-hoc sign patched binary: %w", err)
		}
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

// PatchCodexBinary patches a Codex binary:
//  1. Redirects the system requirements path to a user-writable permissive file.
//  2. Sabotages the cloud requirements URL paths so the fetch 404s (fail-open).
//  3. Renames TOML field names so cloud requirements responses can't be parsed
//     (fail-open). This covers ChatGPT-authenticated sessions where cloud
//     requirements are fetched via the multiplexed backend-api connection
//     rather than dedicated REST endpoints.
//
// The original binary is not modified; a copy is placed in cacheDir.
func PatchCodexBinary(origBinary string, cacheDir string) (*PatchResult, error) {
	return patchCodexBinaryWithRuntime(origBinary, cacheDir, runtime.GOOS, nil)
}
