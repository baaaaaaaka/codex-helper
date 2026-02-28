package cloudgate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/proc"
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
	patchLeaseSuffix     = ".lease"
	// Keep heartbeats infrequent to reduce churn on shared filesystems.
	patchLeaseHeartbeatInterval = 15 * time.Second
	// A lease must miss multiple heartbeats before cleanup can reclaim it.
	patchLeaseStaleAfter = 2 * time.Minute
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

	leasePath          string
	stopLeaseHeartbeat func()
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
	if r.stopLeaseHeartbeat != nil {
		r.stopLeaseHeartbeat()
		r.stopLeaseHeartbeat = nil
	}
	if r.leasePath != "" {
		_ = os.Remove(r.leasePath)
	}
	if r.PatchedBinary != "" {
		_ = os.Remove(r.PatchedBinary)
	}
}

func patchLeasePath(binaryPath string) string {
	return binaryPath + patchLeaseSuffix
}

type patchLease struct {
	Version       int   `json:"version"`
	PID           int   `json:"pid"`
	HeartbeatUnix int64 `json:"heartbeat_unix"`
}

func parsePatchLease(data []byte) (patchLease, bool) {
	raw := strings.TrimSpace(string(data))
	if raw == "" {
		return patchLease{}, false
	}
	var lease patchLease
	if err := json.Unmarshal([]byte(raw), &lease); err != nil {
		return patchLease{}, false
	}
	if lease.Version != 1 || lease.PID <= 0 || lease.HeartbeatUnix <= 0 {
		return patchLease{}, false
	}
	return lease, true
}

func writePatchLease(path string, pid int, at time.Time) error {
	payload, err := json.Marshal(patchLease{
		Version:       1,
		PID:           pid,
		HeartbeatUnix: at.Unix(),
	})
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	return os.WriteFile(path, payload, 0o600)
}

func createPatchLease(binaryPath string) (string, func(), error) {
	lease := patchLeasePath(binaryPath)
	pid := os.Getpid()
	if pid <= 0 {
		return "", nil, fmt.Errorf("invalid pid for lease: %d", pid)
	}
	if err := writePatchLease(lease, pid, time.Now()); err != nil {
		return "", nil, err
	}

	done := make(chan struct{})
	stopped := make(chan struct{})
	var once sync.Once

	go func() {
		defer close(stopped)
		ticker := time.NewTicker(patchLeaseHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = writePatchLease(lease, pid, time.Now())
			}
		}
	}()

	stop := func() {
		once.Do(func() {
			close(done)
			<-stopped
		})
	}

	return lease, stop, nil
}

func patchLeaseLastSeenAt(lease patchLease, leaseModTime time.Time) time.Time {
	heartbeatAt := time.Unix(lease.HeartbeatUnix, 0)
	if leaseModTime.After(heartbeatAt) {
		return leaseModTime
	}
	return heartbeatAt
}

// cleanupStalePatchedBinaries removes patched binaries with stale managed
// leases. Files without a valid lease are preserved conservatively.
func cleanupStalePatchedBinaries(cacheDir string) {
	if strings.TrimSpace(cacheDir) == "" {
		return
	}
	now := time.Now()
	patterns := []string{
		filepath.Join(cacheDir, "codex-patched-*"),
		filepath.Join(cacheDir, "codex-patched-*.exe"),
	}
	seen := map[string]struct{}{}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			continue
		}
		for _, candidate := range matches {
			if strings.HasSuffix(candidate, patchLeaseSuffix) {
				continue
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			info, err := os.Stat(candidate)
			if err != nil || info.IsDir() {
				continue
			}
			lease := patchLeasePath(candidate)
			leaseData, err := os.ReadFile(lease)
			if err != nil {
				continue
			}
			leaseInfo, err := os.Stat(lease)
			if err != nil || leaseInfo.IsDir() {
				continue
			}
			leaseMeta, ok := parsePatchLease(leaseData)
			if !ok {
				continue
			}
			lastSeenAt := patchLeaseLastSeenAt(leaseMeta, leaseInfo.ModTime())
			age := now.Sub(lastSeenAt)
			if age < 0 || age <= patchLeaseStaleAfter {
				continue
			}
			if proc.IsAlive(leaseMeta.PID) {
				continue
			}
			_ = os.Remove(candidate)
			_ = os.Remove(lease)
		}
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
	cleanupStalePatchedBinaries(cacheDir)

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
	pattern := "codex-patched-*"
	if goos == "windows" {
		pattern = "codex-patched-*.exe"
	}
	tmpPatched, err := os.CreateTemp(cacheDir, pattern)
	if err != nil {
		return nil, fmt.Errorf("create patched binary path: %w", err)
	}
	patchedPath := tmpPatched.Name()
	_ = tmpPatched.Close()
	if err := os.WriteFile(patchedPath, data, 0o755); err != nil {
		return nil, fmt.Errorf("write patched binary: %w", err)
	}
	if goos != "windows" {
		if err := os.Chmod(patchedPath, 0o755); err != nil {
			_ = os.Remove(patchedPath)
			return nil, fmt.Errorf("set patched binary executable: %w", err)
		}
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

	leasePath, stopLeaseHeartbeat, err := createPatchLease(patchedPath)
	if err != nil {
		_ = os.Remove(patchedPath)
		return nil, fmt.Errorf("create patch lease: %w", err)
	}

	return &PatchResult{
		PatchedBinary:      patchedPath,
		RequirementsPath:   patchedReqPath,
		OrigSHA256:         origHash,
		leasePath:          leasePath,
		stopLeaseHeartbeat: stopLeaseHeartbeat,
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
