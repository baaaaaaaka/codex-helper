package cloudgate

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
	"strings"
	"time"
)

const (
	cloudRequirementsCacheFilename       = "cloud-requirements-cache.json"
	defaultCloudRequirementsCacheHMACKey = "codex-cloud-requirements-cache-v3-064f8542-75b4-494c-a294-97d3ce597271"
)

var cloudRequirementsCacheHMACKeyPattern = regexp.MustCompile(`codex-cloud-requirements-cache-v[0-9]+-[0-9a-fA-F-]{36}`)

type YoloCloudRequirementsBypassStatus struct {
	CachePath string
	Installed bool
	Reason    string
}

type cloudRequirementsIdentity struct {
	ChatGPTUserID string
	AccountID     string
}

type cloudRequirementsCachePayload struct {
	CachedAt      time.Time `json:"cached_at"`
	ExpiresAt     time.Time `json:"expires_at"`
	ChatGPTUserID string    `json:"chatgpt_user_id"`
	AccountID     string    `json:"account_id"`
	Contents      *string   `json:"contents"`
}

type cloudRequirementsCacheFile struct {
	Signature     string                        `json:"signature"`
	SignedPayload cloudRequirementsCachePayload `json:"signed_payload"`
}

// InstallYoloCloudRequirementsBypass writes a signed empty cloud requirements
// cache for the current Codex auth identity. Newer Codex builds can derive the
// account plan from AgentIdentity auth or CODEX_ACCESS_TOKEN, which cannot be
// safely edited without invalidating the JWT signature. A valid "no
// requirements" cache avoids the startup network fetch while preserving the
// original auth material.
func InstallYoloCloudRequirementsBypass(codexDir string, codexBinaryPath string) (YoloCloudRequirementsBypassStatus, error) {
	cachePath := filepath.Join(codexDir, cloudRequirementsCacheFilename)
	status := YoloCloudRequirementsBypassStatus{CachePath: cachePath}

	var removeErr error
	if err := os.Remove(cachePath); err != nil && !os.IsNotExist(err) {
		removeErr = err
		status.Reason = "could not remove stale cloud requirements cache"
	}

	identity, ok, err := currentCloudRequirementsIdentity(codexDir)
	if err != nil {
		return status, err
	}
	if !ok {
		if status.Reason == "" {
			status.Reason = "no Codex auth identity found"
		}
		return status, nil
	}
	if removeErr != nil {
		return status, fmt.Errorf("remove stale cloud requirements cache: %w", removeErr)
	}

	key, err := discoverCloudRequirementsCacheHMACKey(codexBinaryPath)
	if err != nil {
		return status, err
	}
	if err := writeEmptyCloudRequirementsCache(cachePath, key, identity, time.Now().UTC()); err != nil {
		return status, err
	}
	status.Installed = true
	return status, nil
}

func currentCloudRequirementsIdentity(codexDir string) (cloudRequirementsIdentity, bool, error) {
	if raw := strings.TrimSpace(os.Getenv("CODEX_ACCESS_TOKEN")); raw != "" {
		if identity, ok, err := identityFromAgentIdentityJWT(raw); err == nil && ok {
			return identity, true, nil
		}
	}

	data, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
	if err != nil {
		if os.IsNotExist(err) || strings.Contains(strings.ToLower(err.Error()), "not a directory") {
			return cloudRequirementsIdentity{}, false, nil
		}
		return cloudRequirementsIdentity{}, false, fmt.Errorf("read Codex auth.json: %w", err)
	}
	return identityFromAuthJSON(data)
}

func identityFromAuthJSON(data []byte) (cloudRequirementsIdentity, bool, error) {
	var doc struct {
		AuthMode      string `json:"auth_mode"`
		AgentIdentity string `json:"agent_identity"`
		Tokens        *struct {
			IDToken   json.RawMessage `json:"id_token"`
			AccountID string          `json:"account_id"`
		} `json:"tokens"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return cloudRequirementsIdentity{}, false, fmt.Errorf("parse Codex auth.json: %w", err)
	}

	if strings.EqualFold(doc.AuthMode, "agentIdentity") && strings.TrimSpace(doc.AgentIdentity) != "" {
		return identityFromAgentIdentityJWT(doc.AgentIdentity)
	}
	if doc.Tokens != nil {
		if identity, ok, err := identityFromTokenData(doc.Tokens.IDToken, doc.Tokens.AccountID); err != nil || ok {
			return identity, ok, err
		}
	}
	if strings.TrimSpace(doc.AgentIdentity) != "" {
		return identityFromAgentIdentityJWT(doc.AgentIdentity)
	}
	return cloudRequirementsIdentity{}, false, nil
}

func identityFromTokenData(rawIDToken json.RawMessage, accountID string) (cloudRequirementsIdentity, bool, error) {
	accountID = strings.TrimSpace(accountID)
	if len(rawIDToken) == 0 || bytes.Equal(rawIDToken, []byte("null")) {
		return cloudRequirementsIdentity{}, false, nil
	}

	var token string
	if err := json.Unmarshal(rawIDToken, &token); err == nil {
		payload, err := decodeJWTPayloadObject(token)
		if err != nil {
			return cloudRequirementsIdentity{}, false, err
		}
		authClaims, _ := payload["https://api.openai.com/auth"].(map[string]any)
		userID, _ := authClaims["chatgpt_user_id"].(string)
		if strings.TrimSpace(userID) == "" {
			userID, _ = authClaims["user_id"].(string)
		}
		if accountID == "" {
			accountID, _ = authClaims["chatgpt_account_id"].(string)
		}
		return completeCloudRequirementsIdentity(userID, accountID)
	}

	var info struct {
		ChatGPTUserID string `json:"chatgpt_user_id"`
		AccountID     string `json:"account_id"`
		ChatGPTAcctID string `json:"chatgpt_account_id"`
	}
	if err := json.Unmarshal(rawIDToken, &info); err != nil {
		return cloudRequirementsIdentity{}, false, nil
	}
	if accountID == "" {
		accountID = info.AccountID
	}
	if accountID == "" {
		accountID = info.ChatGPTAcctID
	}
	return completeCloudRequirementsIdentity(info.ChatGPTUserID, accountID)
}

func identityFromAgentIdentityJWT(jwt string) (cloudRequirementsIdentity, bool, error) {
	payload, err := decodeJWTPayloadObject(jwt)
	if err != nil {
		return cloudRequirementsIdentity{}, false, err
	}
	userID, _ := payload["chatgpt_user_id"].(string)
	accountID, _ := payload["account_id"].(string)
	return completeCloudRequirementsIdentity(userID, accountID)
}

func completeCloudRequirementsIdentity(userID string, accountID string) (cloudRequirementsIdentity, bool, error) {
	userID = strings.TrimSpace(userID)
	accountID = strings.TrimSpace(accountID)
	if userID == "" || accountID == "" {
		return cloudRequirementsIdentity{}, false, nil
	}
	return cloudRequirementsIdentity{ChatGPTUserID: userID, AccountID: accountID}, true, nil
}

func decodeJWTPayloadObject(jwt string) (map[string]any, error) {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 || parts[1] == "" {
		return nil, fmt.Errorf("invalid JWT format")
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode JWT payload: %w", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("parse JWT payload: %w", err)
	}
	return payload, nil
}

func discoverCloudRequirementsCacheHMACKey(codexBinaryPath string) ([]byte, error) {
	if strings.TrimSpace(codexBinaryPath) != "" {
		if key, err := discoverCloudRequirementsCacheHMACKeyInFile(codexBinaryPath); err == nil {
			return key, nil
		}
		if nativePath, _, err := FindNativeBinary(codexBinaryPath); err == nil {
			if key, err := discoverCloudRequirementsCacheHMACKeyInFile(nativePath); err == nil {
				return key, nil
			}
		}
	}
	return []byte(defaultCloudRequirementsCacheHMACKey), nil
}

func discoverCloudRequirementsCacheHMACKeyInFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	match := cloudRequirementsCacheHMACKeyPattern.Find(data)
	if match == nil {
		return nil, fmt.Errorf("cloud requirements cache HMAC key not found in %s", filepath.Clean(path))
	}
	return append([]byte(nil), match...), nil
}

func writeEmptyCloudRequirementsCache(cachePath string, hmacKey []byte, identity cloudRequirementsIdentity, now time.Time) error {
	now = now.UTC().Truncate(time.Second)
	payload := cloudRequirementsCachePayload{
		CachedAt:      now,
		ExpiresAt:     now.Add(30 * time.Minute),
		ChatGPTUserID: identity.ChatGPTUserID,
		AccountID:     identity.AccountID,
		Contents:      nil,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal cloud requirements cache payload: %w", err)
	}
	mac := hmac.New(sha256.New, hmacKey)
	_, _ = mac.Write(payloadBytes)
	cacheFile := cloudRequirementsCacheFile{
		Signature:     base64.StdEncoding.EncodeToString(mac.Sum(nil)),
		SignedPayload: payload,
	}
	cacheBytes, err := json.MarshalIndent(cacheFile, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal cloud requirements cache: %w", err)
	}
	cacheBytes = append(cacheBytes, '\n')
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o755); err != nil {
		return fmt.Errorf("create cloud requirements cache dir: %w", err)
	}
	if err := os.WriteFile(cachePath, cacheBytes, 0o600); err != nil {
		return fmt.Errorf("write cloud requirements cache: %w", err)
	}
	return nil
}
