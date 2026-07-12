package modelprofile

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

type Snapshot struct {
	Name                          string    `json:"name,omitempty"`
	Provider                      string    `json:"provider,omitempty"`
	Model                         string    `json:"model,omitempty"`
	BaseURL                       string    `json:"baseUrl,omitempty"`
	APIKeyRef                     string    `json:"apiKeyRef,omitempty"`
	SSHProxy                      string    `json:"sshProxy,omitempty"`
	DefaultReasoningEffort        string    `json:"defaultReasoningEffort,omitempty"`
	SupportedReasoningEffortsJSON string    `json:"supportedReasoningEffortsJson,omitempty"`
	ReasoningEffortMapJSON        string    `json:"reasoningEffortMapJson,omitempty"`
	Revision                      int       `json:"revision,omitempty"`
	KeyFingerprint                string    `json:"keyFingerprint,omitempty"`
	BaseURLHash                   string    `json:"baseUrlHash,omitempty"`
	AdapterProfile                string    `json:"adapterProfile,omitempty"`
	DefaultModel                  string    `json:"defaultModel,omitempty"`
	ModelFingerprint              string    `json:"modelFingerprint,omitempty"`
	CatalogFingerprint            string    `json:"catalogFingerprint,omitempty"`
	SSHProxyFingerprint           string    `json:"sshProxyFingerprint,omitempty"`
	CapturedAt                    time.Time `json:"capturedAt,omitempty"`
}

type Resolved struct {
	Name       string
	Profile    config.ModelProfile
	Provider   ProviderSpec
	Model      ModelSpec
	SSHProfile *config.Profile
}

func (r Resolved) IsDefault() bool {
	return strings.EqualFold(r.Provider.ID, DefaultProvider)
}

func (r Resolved) Revision() int {
	if r.Profile.Revision <= 0 {
		return 1
	}
	return r.Profile.Revision
}

func (r Resolved) SelectedPublicModel() string {
	if model := r.Model.PublicID(); strings.TrimSpace(model) != "" {
		return model
	}
	if model := strings.TrimSpace(r.Profile.Model); model != "" {
		return model
	}
	return r.Provider.DefaultPublicModel()
}

func (r Resolved) Snapshot(now time.Time) Snapshot {
	if now.IsZero() {
		now = time.Now()
	}
	name := strings.TrimSpace(r.Name)
	if name == "" {
		name = config.DefaultModelProfileName
	}
	provider := strings.TrimSpace(r.Profile.Provider)
	if provider == "" {
		provider = r.Provider.ID
	}
	if provider == "" {
		provider = DefaultProvider
	}
	model := strings.TrimSpace(r.SelectedPublicModel())
	return Snapshot{
		Name:                          name,
		Provider:                      provider,
		Model:                         model,
		BaseURL:                       strings.TrimSpace(r.Provider.BaseURL),
		APIKeyRef:                     strings.TrimSpace(r.Profile.APIKeyRef),
		SSHProxy:                      strings.TrimSpace(r.Profile.SSHProxy),
		DefaultReasoningEffort:        strings.TrimSpace(r.Profile.DefaultReasoningEffort),
		SupportedReasoningEffortsJSON: encodeStringList(r.Profile.SupportedReasoningEfforts),
		ReasoningEffortMapJSON:        encodeStringMap(r.Profile.ReasoningEffortMap),
		Revision:                      r.Revision(),
		BaseURLHash:                   BaseURLHash(r.Provider.BaseURL),
		AdapterProfile:                strings.TrimSpace(r.Provider.AdapterProfile),
		DefaultModel:                  model,
		ModelFingerprint:              ModelFingerprint(r.Provider, model),
		CatalogFingerprint:            CatalogFingerprint(r.Provider),
		SSHProxyFingerprint:           SSHProxyFingerprint(r.SSHProfile),
		CapturedAt:                    now.UTC(),
	}
}

func (r Resolved) RuntimeSnapshot(now time.Time, secrets *SecretStore, env func(string) string) (Snapshot, error) {
	snapshot := r.Snapshot(now)
	if r.Provider.UsesAdapter {
		apiKey, err := ResolveAPIKey(r.Profile.APIKeyRef, secrets, env)
		if err != nil {
			return Snapshot{}, err
		}
		snapshot.KeyFingerprint = KeyFingerprint(apiKey, RuntimeKeySalt(r.Name, r.Revision()))
	}
	return snapshot, nil
}

func ValidateSnapshotRuntime(snapshot Snapshot, resolved Resolved, apiKey string) error {
	if snapshot.IsZero() {
		return nil
	}
	expected := resolved.Snapshot(snapshot.CapturedAt)
	if strings.TrimSpace(snapshot.BaseURLHash) != "" && snapshot.BaseURLHash != expected.BaseURLHash {
		return fmt.Errorf("model profile %q base URL changed since the chat was pinned", snapshot.Name)
	}
	if strings.TrimSpace(snapshot.AdapterProfile) != "" && snapshot.AdapterProfile != expected.AdapterProfile {
		return fmt.Errorf("model profile %q adapter profile changed since the chat was pinned", snapshot.Name)
	}
	if strings.TrimSpace(snapshot.Model) != "" && snapshot.Model != expected.Model {
		return fmt.Errorf("model profile %q model changed since the chat was pinned", snapshot.Name)
	}
	if strings.TrimSpace(snapshot.DefaultModel) != "" && snapshot.DefaultModel != expected.DefaultModel {
		return fmt.Errorf("model profile %q default model changed since the chat was pinned", snapshot.Name)
	}
	if strings.TrimSpace(snapshot.ModelFingerprint) != "" && snapshot.ModelFingerprint != expected.ModelFingerprint && !snapshotModelFingerprintStillCompatible(snapshot, resolved) {
		return fmt.Errorf("model profile %q selected model mapping changed since the chat was pinned", snapshot.Name)
	}
	if strings.TrimSpace(snapshot.CatalogFingerprint) != "" && snapshot.CatalogFingerprint != expected.CatalogFingerprint && !snapshotSelectedModelStillCompatible(snapshot, resolved) {
		return fmt.Errorf("model profile %q model catalog changed since the chat was pinned", snapshot.Name)
	}
	if strings.TrimSpace(snapshot.SSHProxyFingerprint) != "" && snapshot.SSHProxyFingerprint != expected.SSHProxyFingerprint {
		return fmt.Errorf("model profile %q ssh proxy changed since the chat was pinned", snapshot.Name)
	}
	if strings.TrimSpace(snapshot.KeyFingerprint) != "" {
		got := KeyFingerprint(apiKey, RuntimeKeySalt(resolved.Name, resolved.Revision()))
		if got != snapshot.KeyFingerprint {
			return fmt.Errorf("model profile %q api key changed since the chat was pinned", snapshot.Name)
		}
	}
	return nil
}

func (s Snapshot) IsZero() bool {
	return strings.TrimSpace(s.Name) == "" &&
		strings.TrimSpace(s.Provider) == "" &&
		strings.TrimSpace(s.Model) == "" &&
		strings.TrimSpace(s.BaseURL) == "" &&
		strings.TrimSpace(s.APIKeyRef) == "" &&
		strings.TrimSpace(s.SSHProxy) == "" &&
		strings.TrimSpace(s.DefaultReasoningEffort) == "" &&
		strings.TrimSpace(s.SupportedReasoningEffortsJSON) == "" &&
		strings.TrimSpace(s.ReasoningEffortMapJSON) == "" &&
		s.Revision <= 0 &&
		strings.TrimSpace(s.KeyFingerprint) == "" &&
		strings.TrimSpace(s.BaseURLHash) == "" &&
		strings.TrimSpace(s.AdapterProfile) == "" &&
		strings.TrimSpace(s.DefaultModel) == "" &&
		strings.TrimSpace(s.ModelFingerprint) == "" &&
		strings.TrimSpace(s.CatalogFingerprint) == "" &&
		strings.TrimSpace(s.SSHProxyFingerprint) == "" &&
		s.CapturedAt.IsZero()
}

func (s Snapshot) IsDefault() bool {
	provider := strings.TrimSpace(s.Provider)
	name := strings.TrimSpace(s.Name)
	return strings.EqualFold(provider, DefaultProvider) ||
		(provider == "" && (name == "" || strings.EqualFold(name, config.DefaultModelProfileName)))
}

func Resolve(cfg config.Config, ref string) (Resolved, error) {
	name := strings.TrimSpace(ref)
	if name == "" {
		name = cfg.EffectiveDefaultModelProfile()
	}
	if name == "" {
		name = config.DefaultModelProfileName
	}

	var profile config.ModelProfile
	if strings.EqualFold(name, config.DefaultModelProfileName) {
		profile = config.ModelProfile{Provider: DefaultProvider, Revision: 1}
		name = config.DefaultModelProfileName
	} else {
		var ok bool
		name, profile, ok = findNamedModelProfile(cfg, name)
		if !ok {
			return Resolved{}, fmt.Errorf("model profile %q not found", strings.TrimSpace(ref))
		}
	}

	providerID := profile.Provider
	if canonicalModel, definition, ok := config.FindModelDefinition(cfg, profile.Model); ok && (strings.TrimSpace(providerID) == "" || strings.EqualFold(providerID, definition.Provider)) {
		provider, providerOK := findStructuredProvider(cfg.ModelProviders, definition.Provider)
		if !providerOK {
			return Resolved{}, fmt.Errorf("model profile %q references missing provider %q", name, definition.Provider)
		}
		credentialRef := strings.TrimSpace(profile.Credential)
		if credentialRef == "" {
			credentialRef = strings.TrimSpace(provider.Credential)
		}
		credential := config.ModelCredential{}
		if credentialRef != "" {
			var credentialOK bool
			credential, credentialOK = findStructuredCredential(cfg.ModelCredentials, credentialRef)
			if !credentialOK {
				return Resolved{}, fmt.Errorf("model profile %q references missing credential %q", name, credentialRef)
			}
			profile.APIKeyRef = credential.APIKeyRef
		}
		if strings.TrimSpace(profile.SSHProxy) == "" {
			profile.SSHProxy = strings.TrimSpace(provider.SSHProxy)
		}
		profile.Model = canonicalModel
		profile.Provider = definition.Provider
		resolvedProvider, selected := structuredProviderSpec(definition.Provider, provider, credential, canonicalModel, definition)
		return finishResolvedModel(cfg, name, profile, resolvedProvider, selected)
	}
	if strings.TrimSpace(providerID) == "" {
		providerID = DefaultProvider
	}
	spec, err := MustLookupProvider(providerID)
	if err != nil {
		return Resolved{}, err
	}
	if baseURL := strings.TrimSpace(profile.BaseURL); baseURL != "" {
		spec.BaseURL = baseURL
	}
	spec.DefaultReasoningEffort = strings.TrimSpace(profile.DefaultReasoningEffort)
	spec.SupportedReasoningEfforts = normalizeStringList(profile.SupportedReasoningEfforts)
	spec.ReasoningEffortMap = cloneStringMap(profile.ReasoningEffortMap)
	if spec.UsesAdapter && strings.TrimSpace(spec.BaseURL) == "" {
		return Resolved{}, fmt.Errorf("model profile %q requires a base URL for provider %q", name, spec.ID)
	}
	if spec.UsesAdapter && strings.TrimSpace(profile.APIKeyRef) == "" {
		return Resolved{}, fmt.Errorf("model profile %q requires an api key for provider %q", name, spec.ID)
	}
	selectedModel, ok := spec.ResolveModel(profile.Model)
	if spec.UsesAdapter && !ok {
		return Resolved{}, unknownModelForProfileError(name, spec, profile.Model)
	}
	if ok {
		profile.Model = selectedModel.PublicID()
		spec = spec.WithSelectedModel(selectedModel)
	}

	var sshProfile *config.Profile
	if strings.TrimSpace(profile.SSHProxy) != "" {
		p, ok := cfg.FindProfile(profile.SSHProxy)
		if !ok {
			return Resolved{}, fmt.Errorf("model profile %q references missing ssh proxy %q", name, profile.SSHProxy)
		}
		sshProfile = &p
	}

	return Resolved{
		Name:       name,
		Profile:    profile,
		Provider:   spec,
		Model:      selectedModel,
		SSHProfile: sshProfile,
	}, nil
}

func structuredProviderSpec(name string, provider config.ModelProvider, credential config.ModelCredential, publicID string, model config.ModelDefinition) (ProviderSpec, ModelSpec) {
	protocol := strings.ToLower(strings.TrimSpace(provider.Protocol))
	direct := protocol == "responses"
	adapterProfile := "openai-chat"
	if direct {
		adapterProfile = "openai-responses"
	}
	tools := boolValue(model.Capabilities.Tools, true)
	vision := boolValue(model.Capabilities.Vision, false)
	reasoning := boolValue(model.Capabilities.Reasoning, false)
	search := boolValue(model.Capabilities.NativeWebSearch, false)
	contextWindow := model.Limits.ContextWindow
	if contextWindow <= 0 {
		contextWindow = 128000
	}
	maxContextWindow := model.Limits.MaxContextWindow
	if maxContextWindow <= 0 {
		maxContextWindow = contextWindow
	}
	selected := ModelSpec{
		ID: publicID, UpstreamID: strings.TrimSpace(model.UpstreamModel), DisplayName: firstNonEmpty(model.DisplayName, publicID),
		Description: model.Description, Aliases: append([]string(nil), model.Aliases...), Priority: model.Priority,
		ContextWindow: contextWindow, MaxContextWindow: maxContextWindow, MaxOutputTokens: model.Limits.MaxOutputTokens,
		EffectiveContextPercent: model.Limits.EffectiveContextPercent, SupportsTools: tools, SupportsVision: vision,
		SupportsReason: reasoning, SupportsSearch: search, ReasoningPolicy: model.Reasoning, ToolPolicy: model.Tools,
		MessagePolicy: model.Messages, SamplingPolicy: model.Sampling, StreamPolicy: mergeStreamPolicy(provider.Stream, model.Stream),
		HTTPPolicy: mergeHTTPPolicy(provider.HTTP, model.HTTP), CachePolicy: model.Cache,
	}
	spec := ProviderSpec{
		ID: name, DisplayName: name, DefaultModel: publicID, Models: []ModelSpec{selected}, BaseURL: strings.TrimRight(provider.BaseURL, "/"),
		AdapterProfile: adapterProfile, UsesAdapter: true, DirectResponses: direct, AllowsAnyModel: false,
		DisableHostedWebSearch: !search, SupportsTools: tools, SupportsVision: vision, SupportsReason: reasoning,
		DefaultReasoningEffort: model.Reasoning.DefaultEffort, SupportedReasoningEfforts: normalizeStringList(model.Reasoning.SupportedEfforts),
		ReasoningEffortMap: cloneStringMap(model.Reasoning.EffortMap), HTTP: mergeHTTPPolicy(provider.HTTP, model.HTTP),
		Stream: mergeStreamPolicy(provider.Stream, model.Stream), Headers: mergeStringMaps(provider.Headers, credential.Headers),
		AuthType: strings.ToLower(strings.TrimSpace(credential.AuthType)), AuthHeader: strings.TrimSpace(credential.Header),
	}
	return spec, selected
}

func mergeStringMaps(base, override map[string]string) map[string]string {
	out := cloneStringMap(base)
	if out == nil && len(override) > 0 {
		out = map[string]string{}
	}
	for key, value := range override {
		out[key] = value
	}
	return out
}

func finishResolvedModel(cfg config.Config, name string, profile config.ModelProfile, spec ProviderSpec, selected ModelSpec) (Resolved, error) {
	if strings.TrimSpace(spec.BaseURL) == "" {
		return Resolved{}, fmt.Errorf("model profile %q requires a base URL for provider %q", name, spec.ID)
	}
	if strings.TrimSpace(profile.APIKeyRef) == "" {
		return Resolved{}, fmt.Errorf("model profile %q requires an api key for provider %q", name, spec.ID)
	}
	var sshProfile *config.Profile
	if strings.TrimSpace(profile.SSHProxy) != "" {
		p, ok := cfg.FindProfile(profile.SSHProxy)
		if !ok {
			return Resolved{}, fmt.Errorf("model profile %q references missing ssh proxy %q", name, profile.SSHProxy)
		}
		sshProfile = &p
	}
	return Resolved{Name: name, Profile: profile, Provider: spec, Model: selected, SSHProfile: sshProfile}, nil
}

func boolValue(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func mergeHTTPPolicy(base, override config.ModelHTTPPolicy) config.ModelHTTPPolicy {
	out := base
	if override.TimeoutSeconds != 0 {
		out.TimeoutSeconds = override.TimeoutSeconds
	}
	if override.ResponseHeaderTimeoutSeconds != 0 {
		out.ResponseHeaderTimeoutSeconds = override.ResponseHeaderTimeoutSeconds
	}
	if override.MaxRetries != nil {
		out.MaxRetries = override.MaxRetries
	}
	if len(override.RetryStatuses) > 0 {
		out.RetryStatuses = append([]int(nil), override.RetryStatuses...)
	}
	if override.HonorRetryAfter != nil {
		out.HonorRetryAfter = override.HonorRetryAfter
	}
	if override.RetryTransportErrors != nil {
		out.RetryTransportErrors = override.RetryTransportErrors
	}
	if override.MaxConcurrentRequests != 0 {
		out.MaxConcurrentRequests = override.MaxConcurrentRequests
	}
	return out
}

func mergeStreamPolicy(base, override config.ModelStreamPolicy) config.ModelStreamPolicy {
	out := base
	if override.IdleTimeoutSeconds != 0 {
		out.IdleTimeoutSeconds = override.IdleTimeoutSeconds
	}
	if override.UpstreamMode != "" {
		out.UpstreamMode = override.UpstreamMode
	}
	if override.Format != "" {
		out.Format = override.Format
	}
	if override.SynthesizeResponsesSSE != nil {
		out.SynthesizeResponsesSSE = override.SynthesizeResponsesSSE
	}
	if override.ReasoningDeltaPath != "" {
		out.ReasoningDeltaPath = override.ReasoningDeltaPath
	}
	if override.CachedTokensPath != "" {
		out.CachedTokensPath = override.CachedTokensPath
	}
	return out
}

func findStructuredProvider(values map[string]config.ModelProvider, ref string) (config.ModelProvider, bool) {
	for name, value := range values {
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(ref)) {
			return value, true
		}
	}
	return config.ModelProvider{}, false
}

func findStructuredCredential(values map[string]config.ModelCredential, ref string) (config.ModelCredential, bool) {
	for name, value := range values {
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(ref)) {
			return value, true
		}
	}
	return config.ModelCredential{}, false
}

func ResolveSnapshot(cfg config.Config, snapshot Snapshot) (Resolved, error) {
	if snapshot.IsZero() {
		return Resolve(cfg, "")
	}
	name := strings.TrimSpace(snapshot.Name)
	if name == "" {
		name = config.DefaultModelProfileName
	}
	providerID := strings.TrimSpace(snapshot.Provider)
	if providerID == "" {
		providerID = DefaultProvider
	}
	profile := config.ModelProfile{
		Provider:                  providerID,
		Model:                     strings.TrimSpace(firstNonEmpty(snapshot.Model, snapshot.DefaultModel)),
		BaseURL:                   strings.TrimSpace(snapshot.BaseURL),
		APIKeyRef:                 strings.TrimSpace(snapshot.APIKeyRef),
		SSHProxy:                  strings.TrimSpace(snapshot.SSHProxy),
		DefaultReasoningEffort:    strings.TrimSpace(snapshot.DefaultReasoningEffort),
		SupportedReasoningEfforts: decodeStringList(snapshot.SupportedReasoningEffortsJSON),
		ReasoningEffortMap:        decodeStringMap(snapshot.ReasoningEffortMapJSON),
		Revision:                  snapshot.Revision,
	}
	if profile.Revision <= 0 {
		profile.Revision = 1
	}
	spec, err := MustLookupProvider(providerID)
	if err != nil {
		return Resolved{}, err
	}
	if baseURL := strings.TrimSpace(profile.BaseURL); baseURL != "" {
		spec.BaseURL = baseURL
	}
	spec.DefaultReasoningEffort = strings.TrimSpace(profile.DefaultReasoningEffort)
	spec.SupportedReasoningEfforts = normalizeStringList(profile.SupportedReasoningEfforts)
	spec.ReasoningEffortMap = cloneStringMap(profile.ReasoningEffortMap)
	if spec.UsesAdapter && strings.TrimSpace(spec.BaseURL) == "" {
		return Resolved{}, fmt.Errorf("model profile %q requires a base URL for provider %q", name, spec.ID)
	}
	if spec.UsesAdapter && strings.TrimSpace(profile.APIKeyRef) == "" {
		return Resolved{}, fmt.Errorf("model profile %q requires an api key for provider %q", name, spec.ID)
	}
	selectedModel, ok := spec.ResolveModel(profile.Model)
	if spec.UsesAdapter && !ok {
		return Resolved{}, unknownModelForProfileError(name, spec, profile.Model)
	}
	if ok {
		profile.Model = selectedModel.PublicID()
		spec = spec.WithSelectedModel(selectedModel)
	}
	var sshProfile *config.Profile
	if strings.TrimSpace(profile.SSHProxy) != "" {
		p, ok := cfg.FindProfile(profile.SSHProxy)
		if !ok {
			return Resolved{}, fmt.Errorf("model profile %q references missing ssh proxy %q", name, profile.SSHProxy)
		}
		sshProfile = &p
	}
	return Resolved{Name: name, Profile: profile, Provider: spec, Model: selectedModel, SSHProfile: sshProfile}, nil
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.ToLower(strings.TrimSpace(value))
		if key != "" && value != "" {
			out[key] = value
		}
	}
	return out
}

func normalizeStringList(in []string) []string {
	out := make([]string, 0, len(in))
	seen := map[string]bool{}
	for _, value := range in {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func encodeStringList(in []string) string {
	normalized := normalizeStringList(in)
	if len(normalized) == 0 {
		return ""
	}
	raw, err := json.Marshal(normalized)
	if err != nil {
		return ""
	}
	return string(raw)
}

func decodeStringList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var out []string
	if json.Unmarshal([]byte(raw), &out) != nil {
		return nil
	}
	return normalizeStringList(out)
}

func encodeStringMap(in map[string]string) string {
	normalized := cloneStringMap(in)
	if len(normalized) == 0 {
		return ""
	}
	raw, err := json.Marshal(normalized)
	if err != nil {
		return ""
	}
	return string(raw)
}

func decodeStringMap(raw string) map[string]string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var out map[string]string
	if json.Unmarshal([]byte(raw), &out) != nil {
		return nil
	}
	return cloneStringMap(out)
}

func findNamedModelProfile(cfg config.Config, ref string) (string, config.ModelProfile, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", config.ModelProfile{}, false
	}
	for name, profile := range cfg.ModelProfiles {
		if strings.EqualFold(strings.TrimSpace(name), ref) {
			return name, profile, true
		}
	}
	return "", config.ModelProfile{}, false
}

func snapshotSelectedModelStillCompatible(snapshot Snapshot, resolved Resolved) bool {
	modelRef := strings.TrimSpace(firstNonEmpty(snapshot.Model, snapshot.DefaultModel))
	if modelRef == "" {
		return false
	}
	model, ok := resolved.Provider.ResolveModel(modelRef)
	return ok && strings.EqualFold(model.PublicID(), resolved.SelectedPublicModel())
}

func snapshotModelFingerprintStillCompatible(snapshot Snapshot, resolved Resolved) bool {
	fingerprint := strings.TrimSpace(snapshot.ModelFingerprint)
	if fingerprint == "" || !snapshotSelectedModelStillCompatible(snapshot, resolved) {
		return false
	}
	modelRef := strings.TrimSpace(firstNonEmpty(snapshot.Model, snapshot.DefaultModel))
	model, ok := resolved.Provider.ResolveModel(modelRef)
	if !ok {
		return false
	}
	if fingerprint == legacyModelFingerprintV1ForModel(resolved.Provider.ID, model) {
		return true
	}
	for _, legacy := range legacyContextWindowModelVariants(model) {
		if fingerprint == legacyModelFingerprintV1ForModel(resolved.Provider.ID, legacy) {
			return true
		}
	}
	return false
}

func legacyContextWindowModelVariants(model ModelSpec) []ModelSpec {
	if !knownLegacyContextWindowCorrection(model) {
		return nil
	}
	legacy := model
	legacy.ContextWindow = 128000
	legacy.MaxContextWindow = 128000
	return []ModelSpec{legacy}
}

func knownLegacyContextWindowCorrection(model ModelSpec) bool {
	if model.ContextWindow != millionTokenContextWindow || model.MaxContextWindow != millionTokenContextWindow {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(model.PublicID())) {
	case "deepseek/deepseek-v4-flash",
		"deepseek/deepseek-v4-pro",
		"mimo/mimo-v2.5",
		"mimo/mimo-v2.5-pro":
		return true
	default:
		return false
	}
}

func unknownModelForProfileError(name string, spec ProviderSpec, ref string) error {
	_, err := spec.MustResolveModel(ref)
	if err != nil {
		return fmt.Errorf("model profile %q references %w", name, err)
	}
	return fmt.Errorf("model profile %q references unknown model %q for provider %q", name, strings.TrimSpace(ref), spec.ID)
}
