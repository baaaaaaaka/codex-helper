package responsesadapter

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strings"
)

const defaultScopeSalt = "codex-helper-responses-adapter-scope-v1"

func KeyFingerprint(apiKey string, salt string) string {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return "no-key"
	}
	if strings.TrimSpace(salt) == "" {
		salt = defaultScopeSalt
	}
	mac := hmac.New(sha256.New, []byte(salt))
	_, _ = mac.Write([]byte(apiKey))
	sum := mac.Sum(nil)
	return "key:" + hex.EncodeToString(sum[:])[:24]
}

func BaseURLHash(base string) string {
	base = normalizeBaseURL(base)
	if base == "" {
		return "default-url"
	}
	sum := sha256.Sum256([]byte(base))
	return "url:" + hex.EncodeToString(sum[:])[:24]
}

func normalizeBaseURL(base string) string {
	base = strings.TrimSpace(base)
	if base == "" {
		return ""
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return strings.TrimRight(base, "/")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}
