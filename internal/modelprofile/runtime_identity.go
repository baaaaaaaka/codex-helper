package modelprofile

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	catalog, err := CodexModelCatalogJSON(provider)
	if err != nil || len(catalog) == 0 {
		return ""
	}
	// The public Codex catalog does not contain transport details. Include the
	// route/interface contract as well so changing an Anthropic/Beta endpoint
	// or operation mapping invalidates a captured runtime snapshot and a
	// long-lived adapter instance.
	raw, err := json.Marshal(struct {
		Catalog          json.RawMessage                  `json:"catalog"`
		DefaultInterface string                           `json:"defaultInterface"`
		Interfaces       map[string]config.ModelInterface `json:"interfaces"`
		Routes           map[string]string                `json:"routes"`
		Adapter          string                           `json:"adapter"`
		Conversion       string                           `json:"conversion"`
		StrictConversion bool                             `json:"strictConversion"`
	}{json.RawMessage(catalog), provider.DefaultInterface, provider.Interfaces, provider.RouteInterfaces, provider.AdapterProfile, provider.ConversionProfile, provider.StrictConversion})
	if err != nil {
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
	policy, _ := json.Marshal(struct {
		Reasoning          any                              `json:"reasoning"`
		Tools              any                              `json:"tools"`
		Messages           any                              `json:"messages"`
		Sampling           any                              `json:"sampling"`
		Responses          any                              `json:"responses"`
		Stream             any                              `json:"stream"`
		HTTP               any                              `json:"http"`
		Cache              any                              `json:"cache"`
		Features           any                              `json:"features"`
		NativeTools        any                              `json:"nativeTools"`
		SourcePolicy       any                              `json:"sourcePolicy"`
		Interface          string                           `json:"interface"`
		Conversion         string                           `json:"conversion"`
		StrictConversion   bool                             `json:"strictConversion"`
		Operation          string                           `json:"operation"`
		ProviderInterfaces map[string]config.ModelInterface `json:"providerInterfaces"`
		ProviderRoutes     map[string]string                `json:"providerRoutes"`
	}{model.ReasoningPolicy, model.ToolPolicy, model.MessagePolicy, model.SamplingPolicy, model.ResponsesPolicy, model.StreamPolicy, model.HTTPPolicy, model.CachePolicy, model.Features, model.NativeTools, model.SourcePolicy, model.DefaultInterface, model.ConversionProfile, model.StrictConversion, model.Operation, provider.Interfaces, provider.RouteInterfaces})
	material := strings.Join([]string{
		strings.TrimSpace(provider.ID),
		strings.TrimSpace(model.PublicID()),
		strings.TrimSpace(model.UpstreamModel()),
		fmt.Sprint(model.ContextWindow), fmt.Sprint(model.MaxContextWindow), fmt.Sprint(model.MaxOutputTokens),
		fmt.Sprint(model.SupportsTools), fmt.Sprint(model.SupportsVision), fmt.Sprint(model.SupportsReason), fmt.Sprint(model.SupportsSearch),
		strings.TrimSpace(model.ConversionProfile), fmt.Sprint(model.StrictConversion), strings.TrimSpace(model.Operation),
		string(policy),
	}, "\n")
	sum := sha256.Sum256([]byte(material))
	return "model:" + hex.EncodeToString(sum[:])[:24]
}

func legacyModelFingerprintV1(provider ProviderSpec, modelRef string) string {
	model, ok := provider.ResolveModel(modelRef)
	if !ok {
		return ""
	}
	return legacyModelFingerprintV1ForModel(provider.ID, model)
}

func legacyModelFingerprintV1ForModel(providerID string, model ModelSpec) string {
	material := strings.Join([]string{
		strings.TrimSpace(providerID),
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
