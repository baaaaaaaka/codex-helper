package migration

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	legacyHistoryFile = "patch_history.json"
	legacyCacheFile   = "cloud-requirements-cache.json"
	legacyAuthBackup  = ".yolo-auth-backup"
	legacyAuthLease   = ".yolo-auth-lease-"
	legacyBinaryLease = ".lease"
	legacyCacheKey    = "codex-cloud-requirements-cache-v3-064f8542-75b4-494c-a294-97d3ce597271"

	// Legacy auth leases were refreshed every 15 seconds. A fresh lease protects
	// shared auth.json state; a stale lease must not pin an upgrade merely because
	// its numeric PID has since been reused by an unrelated process.
	legacyAuthLeaseStaleAfter = 2 * time.Minute
)

var (
	legacyReqDirPattern   = regexp.MustCompile(`^cx[0-9a-f]{6}-[0-9a-f]{4}$`)
	legacyCacheKeyPattern = regexp.MustCompile(`codex-cloud-requirements-cache-v[0-9]+-[0-9a-fA-F-]{36}`)
)

var legacyRequirements = []byte(`allowed_approval_policies = ["never", "on-request", "on-failure", "untrusted"]
allowed_approval_policiez = ["never", "on-request", "on-failure", "untrusted"]
allowed_sandbox_modes = ["danger-full-access", "workspace-write", "read-only"]
allowed_sandbox_modez = ["danger-full-access", "workspace-write", "read-only"]
`)

type CleanupOptions struct {
	ConfigDir    string
	CodexHome    string
	TempDir      string
	BinaryPath   string
	ProcessAlive func(int) bool
	Now          func() time.Time
}

type CleanupReport struct {
	Removed         []string
	Restored        []string
	Preserved       []string
	Deferred        []string
	Blockers        []string
	RuntimeBlockers []RuntimeBlocker
}

type RuntimeBlocker struct {
	Kind          string
	Path          string
	PID           int
	HeartbeatUnix int64
	Reason        string
}

const RuntimeBlockerSharedAuthLease = "shared_auth_lease"

// RuntimeBlockedError keeps migration failures machine-readable across the
// codexrunner launch-error wrapper so CLI and Teams surfaces can explain the
// exact shared-state conflict instead of reporting a generic setup problem.
type RuntimeBlockedError struct {
	Blockers []RuntimeBlocker
}

func (e *RuntimeBlockedError) Error() string {
	count := 0
	if e != nil {
		count = len(e.Blockers)
	}
	message := fmt.Sprintf("standard runtime migration is waiting for %d shared legacy authentication lease(s); existing patched binaries may continue running", count)
	if count == 0 {
		return message
	}
	details := make([]string, 0, min(count, 4))
	for index, blocker := range e.Blockers {
		if index >= 4 {
			break
		}
		owner := "unverified owner"
		if blocker.PID > 0 {
			owner = fmt.Sprintf("pid=%d", blocker.PID)
		}
		if blocker.HeartbeatUnix > 0 {
			owner += ", heartbeat=" + time.Unix(blocker.HeartbeatUnix, 0).UTC().Format(time.RFC3339)
		}
		if name := filepath.Base(blocker.Path); name != "" && name != "." {
			owner += ", artifact=" + name
		}
		details = append(details, owner)
	}
	if count > len(details) {
		details = append(details, fmt.Sprintf("and %d more", count-len(details)))
	}
	return message + ": " + strings.Join(details, "; ")
}

// InspectLegacyRuntimeAssets performs the blocker and provenance checks used
// before a candidate runtime starts, without deleting or rewriting files.
// Ambiguous assets are reported as preserved; only a live or unsafe lease is a
// blocker.
func InspectLegacyRuntimeAssets(options CleanupOptions) (CleanupReport, error) {
	if options.ProcessAlive == nil {
		return CleanupReport{}, fmt.Errorf("process liveness callback is required")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	var report CleanupReport
	cacheDirs := []string{}
	if strings.TrimSpace(options.ConfigDir) != "" {
		cacheDirs = append(cacheDirs, options.ConfigDir)
	}
	if strings.TrimSpace(options.TempDir) != "" {
		matches, _ := filepath.Glob(filepath.Join(options.TempDir, "codex-proxy-yolo-uid-*"))
		cacheDirs = append(cacheDirs, matches...)
	}
	seen := make(map[string]bool)
	for _, dir := range cacheDirs {
		dir = filepath.Clean(strings.TrimSpace(dir))
		if dir == "." || seen[dir] {
			continue
		}
		seen[dir] = true
		inspectLegacyBinaries(&report, dir, options.ProcessAlive)
	}
	if strings.TrimSpace(options.CodexHome) != "" {
		inspectLegacyAuth(&report, options.CodexHome, options.ProcessAlive, options.Now())
	}
	sort.Strings(report.Preserved)
	sort.Strings(report.Deferred)
	sort.Strings(report.Blockers)
	sort.Slice(report.RuntimeBlockers, func(i, j int) bool { return report.RuntimeBlockers[i].Path < report.RuntimeBlockers[j].Path })
	return report, nil
}

// PrepareLegacyAuthentication restores the original authentication document
// before the candidate Codex process starts. Other legacy assets remain in
// place until the app-server initialize handshake succeeds.
func PrepareLegacyAuthentication(options CleanupOptions) (CleanupReport, error) {
	if options.ProcessAlive == nil {
		return CleanupReport{}, fmt.Errorf("process liveness callback is required")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	var report CleanupReport
	if strings.TrimSpace(options.CodexHome) != "" {
		cleanupLegacyAuth(&report, options.CodexHome, options.ProcessAlive, options.Now())
	}
	sort.Strings(report.Removed)
	sort.Strings(report.Restored)
	sort.Strings(report.Preserved)
	sort.Strings(report.Deferred)
	sort.Strings(report.Blockers)
	sort.Slice(report.RuntimeBlockers, func(i, j int) bool { return report.RuntimeBlockers[i].Path < report.RuntimeBlockers[j].Path })
	return report, nil
}

func (r CleanupReport) Complete() bool {
	// Ambiguous files are intentionally preserved as reported orphans. They do
	// not keep the old runtime reachable and must not strand an otherwise healthy
	// upgrade. Only a live/unsafe-to-touch asset blocks the commit.
	return len(r.Blockers) == 0
}

// CleanupLegacyRuntimeAssets removes only artifacts whose provenance can be
// proven from the old helper's exact names, formats, signatures, and leases.
// Ambiguous files are preserved and reported; a live lease is always a blocker.
func CleanupLegacyRuntimeAssets(options CleanupOptions) (CleanupReport, error) {
	if options.ProcessAlive == nil {
		return CleanupReport{}, fmt.Errorf("process liveness callback is required")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	var report CleanupReport
	if strings.TrimSpace(options.ConfigDir) != "" {
		removeKnownFile(&report, filepath.Join(options.ConfigDir, legacyHistoryFile))
		removeKnownFile(&report, filepath.Join(options.ConfigDir, legacyHistoryFile+".lock"))
	}

	cacheDirs := []string{}
	if strings.TrimSpace(options.ConfigDir) != "" {
		cacheDirs = append(cacheDirs, options.ConfigDir)
	}
	if strings.TrimSpace(options.TempDir) != "" {
		matches, _ := filepath.Glob(filepath.Join(options.TempDir, "codex-proxy-yolo-uid-*"))
		cacheDirs = append(cacheDirs, matches...)
	}
	seen := make(map[string]bool)
	hasLiveLegacyBinary := false
	for _, dir := range cacheDirs {
		dir = filepath.Clean(strings.TrimSpace(dir))
		if dir == "." || seen[dir] {
			continue
		}
		seen[dir] = true
		if cleanupLegacyBinaries(&report, dir, options.ProcessAlive) {
			hasLiveLegacyBinary = true
		}
	}
	// A running patched Codex may read its redirected requirements file again
	// on a later turn. Keep all proven legacy requirements until every live
	// private binary has exited; RuntimeCleanupPending will retry this cleanup on
	// a subsequent standard-runtime launch.
	if !hasLiveLegacyBinary {
		cleanupLegacyRequirements(&report, options.TempDir)
	}
	if strings.TrimSpace(options.CodexHome) != "" {
		cleanupLegacyAuth(&report, options.CodexHome, options.ProcessAlive, options.Now())
		cleanupLegacyCloudCache(&report, options.CodexHome, options.BinaryPath)
	}

	sort.Strings(report.Removed)
	sort.Strings(report.Restored)
	sort.Strings(report.Preserved)
	sort.Strings(report.Deferred)
	sort.Strings(report.Blockers)
	sort.Slice(report.RuntimeBlockers, func(i, j int) bool { return report.RuntimeBlockers[i].Path < report.RuntimeBlockers[j].Path })
	return report, nil
}

func removeKnownFile(report *CleanupReport, path string) {
	if strings.TrimSpace(path) == "" || path == "." {
		return
	}
	if err := os.Remove(path); err == nil {
		report.Removed = append(report.Removed, path)
	} else if !os.IsNotExist(err) {
		report.Blockers = append(report.Blockers, path)
	}
}

type legacyLease struct {
	Version       int   `json:"version"`
	PID           int   `json:"pid"`
	HeartbeatUnix int64 `json:"heartbeat_unix"`
}

func parseLegacyLease(data []byte) (legacyLease, bool) {
	var lease legacyLease
	if json.Unmarshal(data, &lease) != nil || lease.Version != 1 || lease.PID <= 0 || lease.HeartbeatUnix <= 0 {
		return legacyLease{}, false
	}
	return lease, true
}

func legacyLeaseLastSeenAt(path string, lease legacyLease) time.Time {
	lastSeen := time.Unix(lease.HeartbeatUnix, 0)
	if info, err := os.Stat(path); err == nil && info.ModTime().After(lastSeen) {
		lastSeen = info.ModTime()
	}
	return lastSeen
}

func legacyAuthLeaseIsActive(path string, lease legacyLease, alive func(int) bool, now time.Time) bool {
	if !alive(lease.PID) {
		return false
	}
	age := now.Sub(legacyLeaseLastSeenAt(path, lease))
	return age < 0 || age <= legacyAuthLeaseStaleAfter
}

func appendSharedAuthBlocker(report *CleanupReport, path string, lease legacyLease, reason string) {
	report.Blockers = append(report.Blockers, path)
	report.RuntimeBlockers = append(report.RuntimeBlockers, RuntimeBlocker{
		Kind:          RuntimeBlockerSharedAuthLease,
		Path:          path,
		PID:           lease.PID,
		HeartbeatUnix: lease.HeartbeatUnix,
		Reason:        reason,
	})
}

func cleanupLegacyBinaries(report *CleanupReport, dir string, alive func(int) bool) bool {
	hasLive := false
	matches, _ := filepath.Glob(filepath.Join(dir, "codex-patched-*"))
	for _, binary := range matches {
		if strings.HasSuffix(binary, legacyBinaryLease) {
			continue
		}
		info, err := os.Stat(binary)
		if err != nil || info.IsDir() {
			continue
		}
		leasePath := binary + legacyBinaryLease
		data, err := os.ReadFile(leasePath)
		lease, valid := parseLegacyLease(data)
		if err != nil || !valid {
			report.Preserved = append(report.Preserved, binary)
			continue
		}
		if alive(lease.PID) {
			report.Blockers = append(report.Blockers, binary)
			report.Deferred = append(report.Deferred, binary)
			hasLive = true
			continue
		}
		if os.Remove(binary) == nil {
			report.Removed = append(report.Removed, binary)
		} else {
			report.Blockers = append(report.Blockers, binary)
			continue
		}
		if removeErr := os.Remove(leasePath); removeErr == nil {
			report.Removed = append(report.Removed, leasePath)
		} else if !os.IsNotExist(removeErr) {
			report.Blockers = append(report.Blockers, leasePath)
		}
	}
	return hasLive
}

func inspectLegacyBinaries(report *CleanupReport, dir string, alive func(int) bool) {
	matches, _ := filepath.Glob(filepath.Join(dir, "codex-patched-*"))
	for _, binary := range matches {
		if strings.HasSuffix(binary, legacyBinaryLease) {
			continue
		}
		info, err := os.Stat(binary)
		if err != nil || info.IsDir() {
			continue
		}
		data, err := os.ReadFile(binary + legacyBinaryLease)
		lease, valid := parseLegacyLease(data)
		if err != nil || !valid {
			report.Preserved = append(report.Preserved, binary)
			continue
		}
		if alive(lease.PID) {
			// The old executable and requirements are session-private. Keep them
			// until their owner exits, but do not prevent the original Codex binary
			// and the standard broker from starting alongside them.
			report.Deferred = append(report.Deferred, binary)
		}
	}
}

func cleanupLegacyRequirements(report *CleanupReport, tempDir string) {
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() || !legacyReqDirPattern.MatchString(entry.Name()) {
			continue
		}
		dir := filepath.Join(tempDir, entry.Name())
		path := filepath.Join(dir, "reqs.toml")
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if !bytes.Equal(data, legacyRequirements) {
			report.Preserved = append(report.Preserved, path)
			continue
		}
		if removeErr := os.Remove(path); removeErr == nil {
			report.Removed = append(report.Removed, path)
			_ = os.Remove(dir)
		} else {
			report.Blockers = append(report.Blockers, path)
		}
	}
}

func cleanupLegacyAuth(report *CleanupReport, codexHome string, alive func(int) bool, now time.Time) {
	authPath := filepath.Join(codexHome, "auth.json")
	backupPath := authPath + legacyAuthBackup
	leases, _ := filepath.Glob(authPath + legacyAuthLease + "*")
	for _, path := range leases {
		data, readErr := os.ReadFile(path)
		lease, valid := parseLegacyLease(data)
		if readErr == nil && valid {
			if legacyAuthLeaseIsActive(path, lease, alive, now) {
				appendSharedAuthBlocker(report, path, lease, "fresh heartbeat from a live process still owns shared auth.json state")
				continue
			}
			if removeErr := os.Remove(path); removeErr == nil {
				report.Removed = append(report.Removed, path)
			} else {
				report.Blockers = append(report.Blockers, path)
			}
			continue
		}
		info, statErr := os.Stat(path)
		if statErr == nil && now.Sub(info.ModTime()) > 24*time.Hour {
			if removeErr := os.Remove(path); removeErr == nil {
				report.Removed = append(report.Removed, path)
			} else {
				report.Blockers = append(report.Blockers, path)
			}
			continue
		}
		appendSharedAuthBlocker(report, path, legacyLease{}, "recent legacy authentication lease has no verifiable owner metadata")
	}
	if len(report.Blockers) > 0 {
		for _, blocker := range report.Blockers {
			if strings.HasPrefix(blocker, authPath+legacyAuthLease) {
				return
			}
		}
	}
	backup, err := os.ReadFile(backupPath)
	if err != nil {
		return
	}
	sanitized, changed, err := sanitizeLegacyAuth(backup)
	if err != nil || !changed {
		report.Preserved = append(report.Preserved, backupPath)
		return
	}
	current, err := os.ReadFile(authPath)
	if os.IsNotExist(err) {
		mode := os.FileMode(0o600)
		if info, statErr := os.Stat(backupPath); statErr == nil {
			mode = info.Mode().Perm()
		}
		if atomicWrite(authPath, backup, mode) != nil {
			report.Preserved = append(report.Preserved, backupPath)
			return
		}
		report.Restored = append(report.Restored, authPath)
		if removeErr := os.Remove(backupPath); removeErr == nil {
			report.Removed = append(report.Removed, backupPath)
		} else {
			report.Blockers = append(report.Blockers, backupPath)
		}
		return
	}
	if err != nil {
		report.Preserved = append(report.Preserved, backupPath)
		return
	}
	if bytes.Equal(current, backup) {
		if removeErr := os.Remove(backupPath); removeErr == nil {
			report.Removed = append(report.Removed, backupPath)
		} else {
			report.Blockers = append(report.Blockers, backupPath)
		}
		return
	}
	if !bytes.Equal(current, sanitized) {
		report.Preserved = append(report.Preserved, backupPath)
		return
	}
	mode := os.FileMode(0o600)
	if info, statErr := os.Stat(authPath); statErr == nil {
		mode = info.Mode().Perm()
	}
	if atomicWrite(authPath, backup, mode) != nil {
		report.Preserved = append(report.Preserved, backupPath)
		return
	}
	report.Restored = append(report.Restored, authPath)
	if removeErr := os.Remove(backupPath); removeErr == nil {
		report.Removed = append(report.Removed, backupPath)
	} else {
		report.Blockers = append(report.Blockers, backupPath)
	}
}

func inspectLegacyAuth(report *CleanupReport, codexHome string, alive func(int) bool, now time.Time) {
	authPath := filepath.Join(codexHome, "auth.json")
	leases, _ := filepath.Glob(authPath + legacyAuthLease + "*")
	for _, path := range leases {
		data, readErr := os.ReadFile(path)
		lease, valid := parseLegacyLease(data)
		if readErr == nil && valid {
			if legacyAuthLeaseIsActive(path, lease, alive, now) {
				appendSharedAuthBlocker(report, path, lease, "fresh heartbeat from a live process still owns shared auth.json state")
			}
			continue
		}
		info, statErr := os.Stat(path)
		if statErr == nil && now.Sub(info.ModTime()) <= 24*time.Hour {
			appendSharedAuthBlocker(report, path, legacyLease{}, "recent legacy authentication lease has no verifiable owner metadata")
		}
	}
}

func sanitizeLegacyAuth(data []byte) ([]byte, bool, error) {
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, false, err
	}
	tokens, ok := doc["tokens"].(map[string]any)
	if !ok {
		return nil, false, nil
	}
	token, ok := tokens["id_token"].(string)
	if !ok {
		return nil, false, nil
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, false, fmt.Errorf("invalid token")
	}
	payloadData, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, false, err
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadData, &payload); err != nil {
		return nil, false, err
	}
	claims, ok := payload["https://api.openai.com/auth"].(map[string]any)
	if !ok {
		return nil, false, nil
	}
	if _, ok := claims["chatgpt_plan_type"]; !ok {
		return nil, false, nil
	}
	delete(claims, "chatgpt_plan_type")
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, false, err
	}
	parts[1] = base64.RawURLEncoding.EncodeToString(encoded)
	tokens["id_token"] = strings.Join(parts, ".")
	out, err := json.MarshalIndent(doc, "", "  ")
	return append(out, '\n'), true, err
}

func atomicWrite(destination string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(destination), ".runtime-migrate-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err = tmp.Write(data); err == nil {
		err = tmp.Chmod(mode)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmpPath, destination)
}

type legacyCachePayload struct {
	CachedAt      time.Time       `json:"cached_at"`
	ExpiresAt     time.Time       `json:"expires_at"`
	ChatGPTUserID string          `json:"chatgpt_user_id"`
	AccountID     string          `json:"account_id"`
	Contents      json.RawMessage `json:"contents"`
}

type legacyCacheEnvelope struct {
	Signature     string             `json:"signature"`
	SignedPayload legacyCachePayload `json:"signed_payload"`
}

func cleanupLegacyCloudCache(report *CleanupReport, codexHome string, binaryPath string) {
	path := filepath.Join(codexHome, legacyCacheFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var envelope legacyCacheEnvelope
	if json.Unmarshal(data, &envelope) != nil || !bytes.Equal(bytes.TrimSpace(envelope.SignedPayload.Contents), []byte("null")) {
		return
	}
	payload, marshalErr := json.Marshal(envelope.SignedPayload)
	if marshalErr != nil {
		report.Preserved = append(report.Preserved, path)
		return
	}
	keys := [][]byte{[]byte(legacyCacheKey)}
	if binary, readErr := os.ReadFile(binaryPath); readErr == nil {
		if found := legacyCacheKeyPattern.Find(binary); found != nil && !bytes.Equal(found, keys[0]) {
			keys = append(keys, append([]byte(nil), found...))
		}
	}
	signature, decodeErr := base64.StdEncoding.DecodeString(envelope.Signature)
	verified := false
	for _, key := range keys {
		mac := hmac.New(sha256.New, key)
		_, _ = mac.Write(payload)
		verified = verified || (decodeErr == nil && hmac.Equal(signature, mac.Sum(nil)))
	}
	if !verified {
		report.Preserved = append(report.Preserved, path)
		return
	}
	if removeErr := os.Remove(path); removeErr == nil {
		report.Removed = append(report.Removed, path)
	} else {
		report.Blockers = append(report.Blockers, path)
	}
}
