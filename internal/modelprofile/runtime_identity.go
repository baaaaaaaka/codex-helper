package modelprofile

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

const defaultRuntimeSalt = "codex-helper-model-profile-runtime-v1"

func RuntimeKeySalt(name string, revision int) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = config.DefaultModelProfileName
	}
	if revision <= 0 {
		revision = 1
	}
	return fmt.Sprintf("%s:%s:v%d", defaultRuntimeSalt, name, revision)
}

func KeyFingerprint(apiKey string, salt string) string {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return ""
	}
	if strings.TrimSpace(salt) == "" {
		salt = defaultRuntimeSalt
	}
	mac := hmac.New(sha256.New, []byte(salt))
	_, _ = mac.Write([]byte(apiKey))
	sum := mac.Sum(nil)
	return "key:" + hex.EncodeToString(sum[:])[:24]
}

func BaseURLHash(base string) string {
	base = normalizeBaseURL(base)
	if base == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(base))
	return "url:" + hex.EncodeToString(sum[:])[:24]
}

func CatalogFingerprint(provider ProviderSpec) string {
	raw, err := CodexModelCatalogJSON(provider)
	if err != nil || len(raw) == 0 {
		return ""
	}
	sum := sha256.Sum256(raw)
	return "catalog:" + hex.EncodeToString(sum[:])[:24]
}

func ModelFingerprint(provider ProviderSpec, modelRef string) string {
	model, ok := provider.ResolveModel(modelRef)
	if !ok {
		return ""
	}
	material := strings.Join([]string{
		strings.TrimSpace(provider.ID),
		strings.TrimSpace(model.PublicID()),
		strings.TrimSpace(model.UpstreamModel()),
		fmt.Sprint(model.ContextWindow),
		fmt.Sprint(model.MaxContextWindow),
		fmt.Sprint(model.SupportsTools),
		fmt.Sprint(model.SupportsVision),
		fmt.Sprint(model.SupportsReason),
		fmt.Sprint(model.SupportsSearch),
	}, "\n")
	sum := sha256.Sum256([]byte(material))
	return "model:" + hex.EncodeToString(sum[:])[:24]
}

func SSHProxyFingerprint(profile *config.Profile) string {
	if profile == nil {
		return ""
	}
	material := strings.Join([]string{
		strings.TrimSpace(profile.ID),
		strings.TrimSpace(profile.Name),
		strings.TrimSpace(profile.Host),
		fmt.Sprint(profile.Port),
		strings.TrimSpace(profile.User),
		strings.Join(profile.SSHArgs, "\x00"),
	}, "\n")
	sum := sha256.Sum256([]byte(material))
	return "ssh:" + hex.EncodeToString(sum[:])[:24]
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
