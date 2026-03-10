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

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
)

const envCodexHome = "CODEX_HOME"

type yoloAuthOverride struct {
	path       string
	sanitized  []byte
	backupPath string
	leasePath  string
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
	if v := strings.TrimSpace(override); v != "" {
		return resolveCodexHomePath(v, workingDir)
	}
	if v := strings.TrimSpace(os.Getenv(envCodexHome)); v != "" {
		return resolveCodexHomePath(v, workingDir)
	}
	if v := strings.TrimSpace(os.Getenv(codexhistory.EnvCodexDir)); v != "" {
		return resolveCodexHomePath(v, workingDir)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".codex"), nil
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

func yoloCodexHomeEnv(codexHome string) []string {
	if strings.TrimSpace(codexHome) == "" {
		return nil
	}
	return []string{
		codexhistory.EnvCodexDir + "=" + codexHome,
		envCodexHome + "=" + codexHome,
	}
}

func prepareYoloAuthOverride(codexHome string) (*yoloAuthOverride, error) {
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
		if _, statErr := os.Stat(backupPath); os.IsNotExist(statErr) {
			return nil, nil
		} else if statErr != nil {
			return nil, statErr
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
		if err := os.WriteFile(authPath, sanitized, mode); err != nil {
			_ = os.Remove(backupPath)
			return nil, err
		}
	} else {
		sanitized = original
	}

	leasePath, err := createYoloAuthLease(authPath)
	if err != nil {
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
	}, nil
}

func (o *yoloAuthOverride) Cleanup() {
	if o == nil || o.path == "" {
		return
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
		_ = os.Remove(o.backupPath)
	}
}

func yoloAuthLeasePrefix(authPath string) string {
	return filepath.Base(authPath) + ".yolo-auth-lease-"
}

func yoloAuthBackupPath(authPath string) string {
	return authPath + ".yolo-auth-backup"
}

func createYoloAuthLease(authPath string) (string, error) {
	file, err := os.CreateTemp(filepath.Dir(authPath), yoloAuthLeasePrefix(authPath)+"*")
	if err != nil {
		return "", err
	}
	path := file.Name()
	if closeErr := file.Close(); closeErr != nil {
		_ = os.Remove(path)
		return "", closeErr
	}
	return path, nil
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
