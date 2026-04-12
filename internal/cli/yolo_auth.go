package cli

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/proc"
)

const (
	envCodexHome                = "CODEX_HOME"
	yoloAuthLeaseHeartbeatAfter = 15 * time.Second
	yoloAuthLeaseStaleAfter     = 2 * time.Minute
	yoloAuthLegacyLeaseMaxAge   = 24 * time.Hour
)

type yoloAuthOverride struct {
	path       string
	sanitized  []byte
	backupPath string
	leasePath  string
	identity   *execIdentity
	stopLease  func()
}

func logYoloAuthStatus(log io.Writer, override *yoloAuthOverride, err error) {
	if log == nil {
		return
	}

	switch {
	case err != nil:
		_, _ = fmt.Fprintf(log, "yolo auth override failed: %v; using existing auth state.\n", err)
	case override != nil:
		_, _ = fmt.Fprintf(log, "yolo auth override active; temporarily masking workspace plan in: %s\n", override.path)
	}
}

func resolveCodexHome(override string, workingDir string) (string, error) {
	paths, err := resolveEffectivePaths("", override, workingDir)
	if err != nil {
		return "", err
	}
	return paths.CodexDir, nil
}

func resolveCodexHomePath(raw string, workingDir string) (string, error) {
	path := filepath.Clean(os.ExpandEnv(strings.TrimSpace(raw)))
	if filepath.IsAbs(path) {
		return path, nil
	}
	if strings.TrimSpace(workingDir) != "" {
		return filepath.Clean(filepath.Join(workingDir, path)), nil
	}
	return filepath.Abs(path)
}

func codexHomeEnv(codexHome string) []string {
	if strings.TrimSpace(codexHome) == "" {
		return nil
	}
	return []string{
		codexhistory.EnvCodexDir + "=" + codexHome,
		envCodexHome + "=" + codexHome,
	}
}

func yoloCodexHomeEnv(codexHome string) []string {
	return codexHomeEnv(codexHome)
}

func prepareYoloAuthOverride(codexHome string, identity *execIdentity) (*yoloAuthOverride, error) {
	codexHome, err := resolveCodexHomePath(codexHome, "")
	if err != nil {
		return nil, err
	}

	authPath := filepath.Join(codexHome, "auth.json")
	original, err := os.ReadFile(authPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	sanitized, changed, err := sanitizeAuthJSONPlanClaim(original)
	if err != nil {
		return nil, err
	}
	backupPath := yoloAuthBackupPath(authPath)
	hasLeases, err := hasOtherYoloAuthLeases(authPath, "")
	if err != nil {
		return nil, err
	}
	if !changed && !hasLeases {
		backup, readErr := os.ReadFile(backupPath)
		switch {
		case os.IsNotExist(readErr):
			return nil, nil
		case readErr != nil:
			return nil, readErr
		}

		backupSanitized, backupChanged, sanitizeErr := sanitizeAuthJSONPlanClaim(backup)
		if sanitizeErr != nil || !backupChanged || !bytes.Equal(backupSanitized, original) {
			// The backup is stale or unrelated to the current masked auth file.
			_ = os.Remove(backupPath)
			return nil, nil
		}
	}

	mode := os.FileMode(0o600)
	if info, statErr := os.Stat(authPath); statErr == nil {
		mode = info.Mode().Perm()
	}
	if changed {
		if err := os.WriteFile(backupPath, original, 0o600); err != nil {
			return nil, err
		}
		if err := ensurePathOwnedByIdentity(backupPath, identity); err != nil {
			_ = os.Remove(backupPath)
			return nil, err
		}
		if err := os.WriteFile(authPath, sanitized, mode); err != nil {
			_ = os.Remove(backupPath)
			return nil, err
		}
		if err := ensurePathOwnedByIdentity(authPath, identity); err != nil {
			_ = os.WriteFile(authPath, original, mode)
			_ = os.Remove(backupPath)
			return nil, err
		}
	} else {
		sanitized = original
	}

	leasePath, stopLease, err := createYoloAuthLease(authPath)
	if err != nil {
		if changed {
			_ = os.WriteFile(authPath, original, mode)
			_ = os.Remove(backupPath)
		}
		return nil, err
	}
	if err := ensurePathOwnedByIdentity(leasePath, identity); err != nil {
		_ = os.Remove(leasePath)
		if changed {
			_ = os.WriteFile(authPath, original, mode)
			_ = os.Remove(backupPath)
		}
		return nil, err
	}

	return &yoloAuthOverride{
		path:       authPath,
		sanitized:  sanitized,
		backupPath: backupPath,
		leasePath:  leasePath,
		identity:   identity,
		stopLease:  stopLease,
	}, nil
}

func (o *yoloAuthOverride) Cleanup() {
	if o == nil || o.path == "" {
		return
	}

	if o.stopLease != nil {
		o.stopLease()
		o.stopLease = nil
	}
	if o.leasePath != "" {
		_ = os.Remove(o.leasePath)
	}

	hasOtherLeases, err := hasOtherYoloAuthLeases(o.path, o.leasePath)
	if err != nil || hasOtherLeases {
		return
	}

	current, err := os.ReadFile(o.path)
	if err != nil {
		return
	}

	backup, err := os.ReadFile(o.backupPath)
	if os.IsNotExist(err) {
		return
	}
	if err != nil || !bytes.Equal(current, o.sanitized) {
		_ = os.Remove(o.backupPath)
		return
	}

	mode := os.FileMode(0o600)
	if info, statErr := os.Stat(o.path); statErr == nil {
		mode = info.Mode().Perm()
	}
	if err := os.WriteFile(o.path, backup, mode); err == nil {
		_ = ensurePathOwnedByIdentity(o.path, o.identity)
		_ = os.Remove(o.backupPath)
	}
}

func yoloAuthLeasePrefix(authPath string) string {
	return filepath.Base(authPath) + ".yolo-auth-lease-"
}

func yoloAuthBackupPath(authPath string) string {
	return authPath + ".yolo-auth-backup"
}

type yoloAuthLease struct {
	Version       int   `json:"version"`
	PID           int   `json:"pid"`
	HeartbeatUnix int64 `json:"heartbeat_unix"`
}

func parseYoloAuthLease(data []byte) (yoloAuthLease, bool) {
	raw := strings.TrimSpace(string(data))
	if raw == "" {
		return yoloAuthLease{}, false
	}
	var lease yoloAuthLease
	if err := json.Unmarshal([]byte(raw), &lease); err != nil {
		return yoloAuthLease{}, false
	}
	if lease.Version != 1 || lease.PID <= 0 || lease.HeartbeatUnix <= 0 {
		return yoloAuthLease{}, false
	}
	return lease, true
}

func writeYoloAuthLease(path string, pid int, at time.Time) error {
	payload, err := json.Marshal(yoloAuthLease{
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

func createYoloAuthLease(authPath string) (string, func(), error) {
	file, err := os.CreateTemp(filepath.Dir(authPath), yoloAuthLeasePrefix(authPath)+"*")
	if err != nil {
		return "", nil, err
	}
	path := file.Name()
	if closeErr := file.Close(); closeErr != nil {
		_ = os.Remove(path)
		return "", nil, closeErr
	}

	pid := os.Getpid()
	if pid <= 0 {
		_ = os.Remove(path)
		return "", nil, fmt.Errorf("invalid pid for auth lease: %d", pid)
	}
	if err := writeYoloAuthLease(path, pid, time.Now()); err != nil {
		_ = os.Remove(path)
		return "", nil, err
	}

	done := make(chan struct{})
	stopped := make(chan struct{})
	var once sync.Once

	go func() {
		defer close(stopped)
		ticker := time.NewTicker(yoloAuthLeaseHeartbeatAfter)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = writeYoloAuthLease(path, pid, time.Now())
			}
		}
	}()

	stop := func() {
		once.Do(func() {
			close(done)
			<-stopped
		})
	}

	return path, stop, nil
}

func yoloAuthLeaseLastSeenAt(lease yoloAuthLease, leaseModTime time.Time) time.Time {
	heartbeatAt := time.Unix(lease.HeartbeatUnix, 0)
	if leaseModTime.After(heartbeatAt) {
		return leaseModTime
	}
	return heartbeatAt
}

func hasOtherYoloAuthLeases(authPath string, ownLeasePath string) (bool, error) {
	dir := filepath.Dir(authPath)
	prefix := yoloAuthLeasePrefix(authPath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	ownLeasePath = filepath.Clean(ownLeasePath)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		candidate := filepath.Join(dir, entry.Name())
		if filepath.Clean(candidate) == ownLeasePath {
			continue
		}

		info, statErr := entry.Info()
		if statErr != nil {
			return true, nil
		}

		data, readErr := os.ReadFile(candidate)
		if readErr != nil {
			return true, nil
		}

		if lease, ok := parseYoloAuthLease(data); ok {
			lastSeenAt := yoloAuthLeaseLastSeenAt(lease, info.ModTime())
			age := time.Since(lastSeenAt)
			if age > yoloAuthLeaseStaleAfter && !proc.IsAlive(lease.PID) {
				_ = os.Remove(candidate)
				continue
			}
			return true, nil
		}

		// Legacy zero-byte leases from older builds never carried process
		// metadata. Clean up obviously stale ones so they don't pin auth.json
		// and the backup file forever after an unclean exit.
		if time.Since(info.ModTime()) > yoloAuthLegacyLeaseMaxAge {
			_ = os.Remove(candidate)
			continue
		}
		return true, nil
	}
	return false, nil
}

func sanitizeAuthJSONPlanClaim(data []byte) ([]byte, bool, error) {
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, false, err
	}

	tokens, ok := doc["tokens"].(map[string]any)
	if !ok {
		return nil, false, nil
	}

	rawIDToken, ok := tokens["id_token"].(string)
	if !ok || strings.TrimSpace(rawIDToken) == "" {
		return nil, false, nil
	}

	sanitizedJWT, changed, err := stripJWTPlanClaim(rawIDToken)
	if err != nil {
		return nil, false, err
	}
	if !changed {
		return nil, false, nil
	}

	tokens["id_token"] = sanitizedJWT
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, false, err
	}
	out = append(out, '\n')
	return out, true, nil
}

func stripJWTPlanClaim(jwt string) (string, bool, error) {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return "", false, fmt.Errorf("invalid auth id_token format")
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false, fmt.Errorf("decode auth id_token payload: %w", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return "", false, fmt.Errorf("parse auth id_token payload: %w", err)
	}

	authClaims, ok := payload["https://api.openai.com/auth"].(map[string]any)
	if !ok {
		return jwt, false, nil
	}
	if _, ok := authClaims["chatgpt_plan_type"]; !ok {
		return jwt, false, nil
	}

	delete(authClaims, "chatgpt_plan_type")

	updatedPayload, err := json.Marshal(payload)
	if err != nil {
		return "", false, fmt.Errorf("encode auth id_token payload: %w", err)
	}
	parts[1] = base64.RawURLEncoding.EncodeToString(updatedPayload)
	return strings.Join(parts, "."), true, nil
}
