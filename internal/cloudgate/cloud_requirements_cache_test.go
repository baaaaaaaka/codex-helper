package cloudgate

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

func encodeCloudReqTestJWT(t *testing.T, payload map[string]any) string {
	t.Helper()
	headerBytes, err := json.Marshal(map[string]string{"alg": "none", "typ": "JWT"})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(headerBytes) + "." +
		base64.RawURLEncoding.EncodeToString(payloadBytes) + "." +
		base64.RawURLEncoding.EncodeToString([]byte("sig"))
}

func writeCloudReqTestBinary(t *testing.T, dir string, key string) string {
	t.Helper()
	path := filepath.Join(dir, "codex")
	if err := os.WriteFile(path, []byte("prefix "+key+" suffix"), 0o755); err != nil {
		t.Fatalf("write test binary: %v", err)
	}
	return path
}

func readCloudReqTestCache(t *testing.T, codexDir string) cloudRequirementsCacheFile {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(codexDir, cloudRequirementsCacheFilename))
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	var cache cloudRequirementsCacheFile
	if err := json.Unmarshal(data, &cache); err != nil {
		t.Fatalf("parse cache: %v", err)
	}
	return cache
}

func verifyCloudReqTestCacheSignature(t *testing.T, cache cloudRequirementsCacheFile, key string) {
	t.Helper()
	payloadBytes, err := json.Marshal(cache.SignedPayload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write(payloadBytes)
	want := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if cache.Signature != want {
		t.Fatalf("cache signature = %q, want %q", cache.Signature, want)
	}
}

func TestInstallYoloCloudRequirementsBypassWritesSignedEmptyCacheForChatGPTAuth(t *testing.T) {
	codexDir := t.TempDir()
	key := "codex-cloud-requirements-cache-v9-11111111-2222-3333-4444-555555555555"
	binary := writeCloudReqTestBinary(t, t.TempDir(), key)
	idToken := encodeCloudReqTestJWT(t, map[string]any{
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_user_id":    "user-1",
			"chatgpt_account_id": "fallback-account",
			"chatgpt_plan_type":  "business",
		},
	})
	auth := map[string]any{
		"auth_mode": "chatgpt",
		"tokens": map[string]any{
			"id_token":      idToken,
			"access_token":  "access",
			"refresh_token": "refresh",
			"account_id":    "account-1",
		},
	}
	authBytes, _ := json.Marshal(auth)
	if err := os.WriteFile(filepath.Join(codexDir, "auth.json"), authBytes, 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}

	status, err := InstallYoloCloudRequirementsBypass(codexDir, binary)
	if err != nil {
		t.Fatalf("InstallYoloCloudRequirementsBypass: %v", err)
	}
	if !status.Installed {
		t.Fatalf("cache should be installed: %#v", status)
	}
	cache := readCloudReqTestCache(t, codexDir)
	verifyCloudReqTestCacheSignature(t, cache, key)
	if cache.SignedPayload.ChatGPTUserID != "user-1" || cache.SignedPayload.AccountID != "account-1" {
		t.Fatalf("cache identity = %#v", cache.SignedPayload)
	}
	if cache.SignedPayload.Contents != nil {
		t.Fatalf("cache contents = %#v, want nil", *cache.SignedPayload.Contents)
	}
	if cache.SignedPayload.ExpiresAt.Sub(cache.SignedPayload.CachedAt) != 30*time.Minute {
		t.Fatalf("cache ttl = %s, want 30m", cache.SignedPayload.ExpiresAt.Sub(cache.SignedPayload.CachedAt))
	}
}

func TestInstallYoloCloudRequirementsBypassUsesAgentIdentityWithoutMutatingAuth(t *testing.T) {
	codexDir := t.TempDir()
	binary := writeCloudReqTestBinary(t, t.TempDir(), defaultCloudRequirementsCacheHMACKey)
	agentIdentity := encodeCloudReqTestJWT(t, map[string]any{
		"agent_runtime_id":           "runtime-1",
		"agent_private_key":          "private",
		"account_id":                 "agent-account",
		"chatgpt_user_id":            "agent-user",
		"email":                      "user@example.com",
		"plan_type":                  "enterprise",
		"chatgpt_account_is_fedramp": false,
	})
	original := []byte(`{"auth_mode":"agentIdentity","agent_identity":"` + agentIdentity + `"}`)
	authPath := filepath.Join(codexDir, "auth.json")
	if err := os.WriteFile(authPath, original, 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}

	status, err := InstallYoloCloudRequirementsBypass(codexDir, binary)
	if err != nil {
		t.Fatalf("InstallYoloCloudRequirementsBypass: %v", err)
	}
	if !status.Installed {
		t.Fatalf("cache should be installed: %#v", status)
	}
	cache := readCloudReqTestCache(t, codexDir)
	if cache.SignedPayload.ChatGPTUserID != "agent-user" || cache.SignedPayload.AccountID != "agent-account" {
		t.Fatalf("cache identity = %#v", cache.SignedPayload)
	}
	current, err := os.ReadFile(authPath)
	if err != nil {
		t.Fatalf("read auth: %v", err)
	}
	if string(current) != string(original) {
		t.Fatalf("auth.json was mutated: %s", current)
	}
}

func TestInstallYoloCloudRequirementsBypassPrefersAccessTokenEnv(t *testing.T) {
	codexDir := t.TempDir()
	binary := writeCloudReqTestBinary(t, t.TempDir(), defaultCloudRequirementsCacheHMACKey)
	agentIdentity := encodeCloudReqTestJWT(t, map[string]any{
		"account_id":      "env-account",
		"chatgpt_user_id": "env-user",
		"plan_type":       "enterprise",
	})
	t.Setenv("CODEX_ACCESS_TOKEN", agentIdentity)
	idToken := encodeCloudReqTestJWT(t, map[string]any{
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_user_id": "file-user",
		},
	})
	authBytes, _ := json.Marshal(map[string]any{
		"tokens": map[string]any{
			"id_token":   idToken,
			"account_id": "file-account",
		},
	})
	if err := os.WriteFile(filepath.Join(codexDir, "auth.json"), authBytes, 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}

	status, err := InstallYoloCloudRequirementsBypass(codexDir, binary)
	if err != nil {
		t.Fatalf("InstallYoloCloudRequirementsBypass: %v", err)
	}
	if !status.Installed {
		t.Fatalf("cache should be installed: %#v", status)
	}
	cache := readCloudReqTestCache(t, codexDir)
	if cache.SignedPayload.ChatGPTUserID != "env-user" || cache.SignedPayload.AccountID != "env-account" {
		t.Fatalf("cache identity = %#v", cache.SignedPayload)
	}
}

func TestInstallYoloCloudRequirementsBypassRemovesStaleCacheWhenIdentityMissing(t *testing.T) {
	codexDir := t.TempDir()
	cachePath := filepath.Join(codexDir, cloudRequirementsCacheFilename)
	if err := os.WriteFile(cachePath, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write stale cache: %v", err)
	}
	status, err := InstallYoloCloudRequirementsBypass(codexDir, filepath.Join(t.TempDir(), "missing"))
	if err != nil {
		t.Fatalf("InstallYoloCloudRequirementsBypass: %v", err)
	}
	if status.Installed || status.Reason == "" {
		t.Fatalf("status = %#v, want no install with reason", status)
	}
	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Fatalf("stale cache should be removed, stat err = %v", err)
	}
}

func TestIdentityFromAuthJSONFallsBackToAuthClaimAccountID(t *testing.T) {
	idToken := encodeCloudReqTestJWT(t, map[string]any{
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_user_id":    "user-2",
			"chatgpt_account_id": "account-from-claim",
		},
	})
	authBytes, _ := json.Marshal(map[string]any{
		"tokens": map[string]any{"id_token": idToken},
	})
	identity, ok, err := identityFromAuthJSON(authBytes)
	if err != nil {
		t.Fatalf("identityFromAuthJSON: %v", err)
	}
	if !ok || identity.ChatGPTUserID != "user-2" || identity.AccountID != "account-from-claim" {
		t.Fatalf("identity = %#v ok=%v", identity, ok)
	}
}

func TestIdentityFromAuthJSONSupportsObjectIDTokenAccountID(t *testing.T) {
	authBytes, _ := json.Marshal(map[string]any{
		"tokens": map[string]any{
			"id_token": map[string]any{
				"chatgpt_user_id":    "object-user",
				"chatgpt_account_id": "object-account",
			},
		},
	})
	identity, ok, err := identityFromAuthJSON(authBytes)
	if err != nil {
		t.Fatalf("identityFromAuthJSON: %v", err)
	}
	if !ok || identity.ChatGPTUserID != "object-user" || identity.AccountID != "object-account" {
		t.Fatalf("identity = %#v ok=%v", identity, ok)
	}
}

func TestWriteEmptyCloudRequirementsCacheUsesCodexCanonicalPayload(t *testing.T) {
	cachePath := filepath.Join(t.TempDir(), cloudRequirementsCacheFilename)
	key := []byte(defaultCloudRequirementsCacheHMACKey)
	now := time.Date(2026, 5, 10, 1, 2, 3, 987654321, time.UTC)
	identity := cloudRequirementsIdentity{ChatGPTUserID: "user-canonical", AccountID: "account-canonical"}

	if err := writeEmptyCloudRequirementsCache(cachePath, key, identity, now); err != nil {
		t.Fatalf("writeEmptyCloudRequirementsCache: %v", err)
	}
	cache := readCloudReqTestCache(t, filepath.Dir(cachePath))
	payloadBytes, err := json.Marshal(cache.SignedPayload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	wantPayload := `{"cached_at":"2026-05-10T01:02:03Z","expires_at":"2026-05-10T01:32:03Z","chatgpt_user_id":"user-canonical","account_id":"account-canonical","contents":null}`
	if string(payloadBytes) != wantPayload {
		t.Fatalf("payload bytes = %s, want %s", payloadBytes, wantPayload)
	}
	verifyCloudReqTestCacheSignature(t, cache, string(key))
}
