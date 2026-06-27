package migration

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCleanupLegacyRuntimeAssetsRemovesOnlyProvenDeadArtifacts(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "config")
	codexHome := filepath.Join(root, "codex")
	tempDir := filepath.Join(root, "tmp")
	for _, dir := range []string{configDir, codexHome, tempDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writeTestFile(t, filepath.Join(configDir, legacyHistoryFile), []byte(`{"version":1}`))
	writeTestFile(t, filepath.Join(configDir, legacyHistoryFile+".lock"), nil)
	binary := filepath.Join(configDir, "codex-patched-123")
	writeTestFile(t, binary, []byte("copy"))
	lease, _ := json.Marshal(legacyLease{Version: 1, PID: 321, HeartbeatUnix: time.Now().Unix()})
	writeTestFile(t, binary+legacyBinaryLease, lease)
	reqDir := filepath.Join(tempDir, "cx123abc-456d")
	if err := os.MkdirAll(reqDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(reqDir, "reqs.toml"), legacyRequirements)

	originalAuth, maskedAuth := testLegacyAuthPair(t)
	authPath := filepath.Join(codexHome, "auth.json")
	writeTestFile(t, authPath, maskedAuth)
	writeTestFile(t, authPath+legacyAuthBackup, originalAuth)
	authLease := authPath + legacyAuthLease + "dead"
	writeTestFile(t, authLease, lease)
	writeLegacyCloudCache(t, filepath.Join(codexHome, legacyCacheFile))

	report, err := CleanupLegacyRuntimeAssets(CleanupOptions{
		ConfigDir:    configDir,
		CodexHome:    codexHome,
		TempDir:      tempDir,
		ProcessAlive: func(pid int) bool { return false },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Complete() {
		t.Fatalf("cleanup incomplete: %#v", report)
	}
	for _, path := range []string{
		filepath.Join(configDir, legacyHistoryFile), binary, binary + legacyBinaryLease,
		filepath.Join(reqDir, "reqs.toml"), authPath + legacyAuthBackup, authLease,
		filepath.Join(codexHome, legacyCacheFile),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("artifact still exists: %s (err=%v)", path, err)
		}
	}
	gotAuth, err := os.ReadFile(authPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotAuth) != string(originalAuth) {
		t.Fatalf("auth was not restored\ngot:  %s\nwant: %s", gotAuth, originalAuth)
	}
}

func TestCleanupLegacyRuntimeAssetsPreservesLiveAndAmbiguousArtifacts(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "config")
	tempDir := filepath.Join(root, "tmp")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(tempDir, 0o700); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(configDir, "codex-patched-live")
	writeTestFile(t, live, []byte("copy"))
	lease, _ := json.Marshal(legacyLease{Version: 1, PID: 999, HeartbeatUnix: time.Now().Unix()})
	writeTestFile(t, live+legacyBinaryLease, lease)
	ambiguous := filepath.Join(configDir, "codex-patched-unknown")
	writeTestFile(t, ambiguous, []byte("unproven"))

	report, err := CleanupLegacyRuntimeAssets(CleanupOptions{
		ConfigDir:    configDir,
		CodexHome:    filepath.Join(root, "codex"),
		TempDir:      tempDir,
		ProcessAlive: func(pid int) bool { return pid == 999 },
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Complete() || len(report.Blockers) != 1 || len(report.Preserved) != 1 {
		t.Fatalf("report = %#v", report)
	}
	for _, path := range []string{live, live + legacyBinaryLease, ambiguous} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("artifact should be preserved: %s: %v", path, err)
		}
	}
}

func testLegacyAuthPair(t *testing.T) ([]byte, []byte) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_plan_type": "team",
			"chatgpt_user_id":   "user",
		},
	})
	token := "header." + base64.RawURLEncoding.EncodeToString(payload) + ".signature"
	doc := map[string]any{"tokens": map[string]any{"id_token": token}}
	original, _ := json.MarshalIndent(doc, "", "  ")
	original = append(original, '\n')
	masked, changed, err := sanitizeLegacyAuth(original)
	if err != nil || !changed {
		t.Fatalf("sanitizeLegacyAuth: changed=%v err=%v", changed, err)
	}
	return original, masked
}

func writeLegacyCloudCache(t *testing.T, path string) {
	t.Helper()
	payload := legacyCachePayload{
		CachedAt:      time.Date(2026, 6, 27, 0, 0, 0, 0, time.UTC),
		ExpiresAt:     time.Date(2026, 6, 27, 0, 30, 0, 0, time.UTC),
		ChatGPTUserID: "user",
		AccountID:     "account",
		Contents:      json.RawMessage("null"),
	}
	payloadBytes, _ := json.Marshal(payload)
	mac := hmac.New(sha256.New, []byte(legacyCacheKey))
	_, _ = mac.Write(payloadBytes)
	doc := legacyCacheEnvelope{Signature: base64.StdEncoding.EncodeToString(mac.Sum(nil)), SignedPayload: payload}
	data, _ := json.MarshalIndent(doc, "", "  ")
	writeTestFile(t, path, data)
}

func writeTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
