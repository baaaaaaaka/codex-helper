package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
)

// validateRawModelConfig applies strict decoding only to the independently
// versioned model configuration. The outer CXP config intentionally remains
// forward-compatible with additive fields written by newer helpers.
func validateRawModelConfig(raw []byte) error {
	var envelope struct {
		ModelConfigVersion int                        `json:"modelConfigVersion"`
		ModelCredentials   map[string]json.RawMessage `json:"modelCredentials"`
		ModelProviders     map[string]json.RawMessage `json:"modelProviders"`
		Models             map[string]json.RawMessage `json:"models"`
		ModelProfiles      map[string]json.RawMessage `json:"modelProfiles"`
		ModelSources       map[string]json.RawMessage `json:"modelSources"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return err
	}
	if envelope.ModelConfigVersion == 0 && len(envelope.ModelCredentials) == 0 && len(envelope.ModelProviders) == 0 && len(envelope.Models) == 0 {
		return nil
	}
	for name, value := range envelope.ModelCredentials {
		var out ModelCredential
		if err := strictModelJSON(value, &out); err != nil {
			return fmt.Errorf("model credential %q: %w", name, err)
		}
	}
	for name, value := range envelope.ModelProviders {
		var out ModelProvider
		if err := strictModelJSON(value, &out); err != nil {
			return fmt.Errorf("model provider %q: %w", name, err)
		}
	}
	for name, value := range envelope.Models {
		var out ModelDefinition
		if err := strictModelJSON(value, &out); err != nil {
			return fmt.Errorf("model %q: %w", name, err)
		}
	}
	for name, value := range envelope.ModelProfiles {
		var out ModelProfile
		if err := strictModelJSON(value, &out); err != nil {
			return fmt.Errorf("model profile %q: %w", name, err)
		}
	}
	for name, value := range envelope.ModelSources {
		var out ModelSource
		if err := strictModelJSON(value, &out); err != nil {
			return fmt.Errorf("model source %q: %w", name, err)
		}
	}
	return nil
}

func strictModelJSON(raw json.RawMessage, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON")
		}
		return err
	}
	return nil
}

func ValidateModelConfig(cfg Config) error {
	if cfg.ModelConfigVersion < 0 || cfg.ModelConfigVersion > CurrentModelConfigVersion {
		return fmt.Errorf("unsupported model config version %d (supported: %d)", cfg.ModelConfigVersion, CurrentModelConfigVersion)
	}
	if (len(cfg.ModelCredentials) > 0 || len(cfg.ModelProviders) > 0 || len(cfg.Models) > 0) && cfg.ModelConfigVersion == 0 {
		return fmt.Errorf("modelConfigVersion is required when modelCredentials, modelProviders, or models are configured")
	}
	for name, credential := range cfg.ModelCredentials {
		if strings.TrimSpace(name) == "" || (strings.TrimSpace(credential.APIKeyRef) == "" && !credential.Pending) {
			return fmt.Errorf("model credential %q requires apiKeyRef", name)
		}
		switch value := strings.ToLower(strings.TrimSpace(credential.AuthType)); value {
		case "", "bearer", "header":
		default:
			return fmt.Errorf("model credential %q has invalid authType %q", name, credential.AuthType)
		}
	}
	for name, provider := range cfg.ModelProviders {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("model provider name is empty")
		}
		switch strings.ToLower(strings.TrimSpace(provider.Protocol)) {
		case "responses", "chat-completions":
		default:
			return fmt.Errorf("model provider %q has invalid protocol %q", name, provider.Protocol)
		}
		parsed, err := url.Parse(strings.TrimSpace(provider.BaseURL))
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("model provider %q requires an absolute http(s) baseUrl", name)
		}
		if ref := strings.TrimSpace(provider.Credential); ref != "" {
			if _, ok := findModelCredential(cfg.ModelCredentials, ref); !ok {
				return fmt.Errorf("model provider %q references missing credential %q", name, ref)
			}
		}
		if err := validateHTTPPolicy("model provider "+name, provider.HTTP); err != nil {
			return err
		}
		if err := validateStreamPolicy("model provider "+name, provider.Stream); err != nil {
			return err
		}
	}
	seenAliases := map[string]string{}
	for name, model := range cfg.Models {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(model.UpstreamModel) == "" {
			return fmt.Errorf("model %q requires upstreamModel", name)
		}
		if _, ok := findModelProvider(cfg.ModelProviders, model.Provider); !ok {
			return fmt.Errorf("model %q references missing provider %q", name, model.Provider)
		}
		scope := strings.ToLower(strings.TrimSpace(model.Provider))
		for _, alias := range append([]string{name}, model.Aliases...) {
			key := strings.ToLower(strings.TrimSpace(alias))
			if key == "" {
				continue
			}
			scopedKey := scope + "\x00" + key
			if previous, ok := seenAliases[scopedKey]; ok && !strings.EqualFold(previous, name) {
				return fmt.Errorf("model alias %q conflicts within provider %q between %q and %q", alias, model.Provider, previous, name)
			}
			seenAliases[scopedKey] = name
		}
		if err := validateModelDefinition(name, model); err != nil {
			return err
		}
	}
	for name, profile := range cfg.ModelProfiles {
		if strings.TrimSpace(profile.Model) != "" && len(cfg.Models) > 0 {
			if _, _, ok := FindModelDefinition(cfg, profile.Model); !ok && strings.TrimSpace(profile.Provider) == "" {
				return fmt.Errorf("model profile %q references missing model %q", name, profile.Model)
			}
		}
		if ref := strings.TrimSpace(profile.Credential); ref != "" {
			if _, ok := findModelCredential(cfg.ModelCredentials, ref); !ok {
				return fmt.Errorf("model profile %q references missing credential %q", name, ref)
			}
		}
	}
	if err := validateGlobalDefaults(cfg); err != nil {
		return err
	}
	for name, source := range cfg.ModelSources {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("model source name is empty")
		}
		switch strings.ToLower(strings.TrimSpace(source.Kind)) {
		case "", "git":
			if strings.TrimSpace(source.URL) == "" {
				return fmt.Errorf("model source %q requires url", name)
			}
		case "file", "directory":
			if strings.TrimSpace(source.Path) == "" {
				return fmt.Errorf("model source %q requires path", name)
			}
		default:
			return fmt.Errorf("model source %q has invalid kind %q", name, source.Kind)
		}
		for _, credential := range source.Credentials {
			if _, ok := cfg.ModelCredentials[credential]; !ok {
				return fmt.Errorf("model source %q owns missing credential %q", name, credential)
			}
		}
		for _, provider := range source.Providers {
			if _, ok := cfg.ModelProviders[provider]; !ok {
				return fmt.Errorf("model source %q owns missing provider %q", name, provider)
			}
		}
		for _, model := range source.Models {
			if _, ok := cfg.Models[model]; !ok {
				return fmt.Errorf("model source %q owns missing model %q", name, model)
			}
		}
		for _, profile := range source.Profiles {
			value, ok := cfg.ModelProfiles[profile]
			if !ok || !strings.EqualFold(strings.TrimSpace(value.Source), name) {
				return fmt.Errorf("model source %q owns missing or mismatched profile %q", name, profile)
			}
		}
	}
	return nil
}

func validateGlobalDefaults(cfg Config) error {
	if cfg.Defaults == nil {
		return nil
	}
	rawSelector := cfg.Defaults.Model
	selector := strings.TrimSpace(rawSelector)
	if rawSelector != selector {
		return fmt.Errorf("defaults.model must not contain surrounding whitespace")
	}
	if selector != "" && !strings.EqualFold(selector, DefaultModelProfileName) {
		prefix, value, qualified := strings.Cut(selector, ":")
		if !qualified || strings.TrimSpace(value) == "" {
			return fmt.Errorf("defaults.model %q must be `default`, `official:<slug>`, `model:<provider>/<model>`, or `profile:<name>`", selector)
		}
		switch strings.ToLower(strings.TrimSpace(prefix)) {
		case "official":
			// Availability is account/runtime-specific and is validated by the
			// default command before persistence. The config layer only enforces
			// the canonical typed selector shape.
		case "profile":
			if _, ok := cfg.FindModelProfile(strings.TrimSpace(value)); !ok {
				return fmt.Errorf("defaults.model references missing profile %q", strings.TrimSpace(value))
			}
		case "model":
			if _, _, ok := FindModelDefinition(cfg, strings.TrimSpace(value)); !ok {
				return fmt.Errorf("defaults.model references missing model %q", strings.TrimSpace(value))
			}
		default:
			return fmt.Errorf("defaults.model %q must be `default`, `official:<slug>`, `model:<provider>/<model>`, or `profile:<name>`", selector)
		}
	}
	rawEffort := cfg.Defaults.ReasoningEffort
	effort := strings.TrimSpace(rawEffort)
	if rawEffort != effort || len(effort) > 64 || strings.ContainsAny(effort, " \t\r\n") {
		return fmt.Errorf("defaults.reasoningEffort %q must be a single model-advertised value", effort)
	}
	return nil
}

// ParseModelConfigFragment parses a repository-owned model configuration using
// the same strict schema as the local config. Repository credentials may be
// declared as pending slots, but raw secret values are never accepted.
func ParseModelConfigFragment(raw []byte) (Config, error) {
	if err := validateRawModelConfig(raw); err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Config{}, err
	}
	for name, credential := range cfg.ModelCredentials {
		if strings.TrimSpace(credential.APIKeyRef) != "" {
			return Config{}, fmt.Errorf("repository model credential %q must not contain apiKeyRef", name)
		}
		credential.Pending = true
		cfg.ModelCredentials[name] = credential
	}
	for name, provider := range cfg.ModelProviders {
		for header := range provider.Headers {
			if strings.EqualFold(strings.TrimSpace(header), "Authorization") {
				return Config{}, fmt.Errorf("repository model provider %q must not contain an Authorization header", name)
			}
		}
	}
	if err := ValidateModelConfig(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateModelDefinition(name string, model ModelDefinition) error {
	if model.Limits.ContextWindow < 0 || model.Limits.MaxContextWindow < 0 || model.Limits.MaxOutputTokens < 0 || model.Limits.EffectiveContextPercent < 0 || model.Limits.EffectiveContextPercent > 100 {
		return fmt.Errorf("model %q has invalid limits", name)
	}
	if model.Capabilities.Tools != nil && !*model.Capabilities.Tools && model.Capabilities.ParallelTools != nil && *model.Capabilities.ParallelTools {
		return fmt.Errorf("model %q cannot enable parallelTools while tools=false", name)
	}
	if model.Capabilities.Reasoning != nil && !*model.Capabilities.Reasoning && (len(model.Reasoning.SupportedEfforts) > 0 || strings.TrimSpace(model.Reasoning.DefaultEffort) != "") {
		return fmt.Errorf("model %q cannot configure reasoning efforts while reasoning=false", name)
	}
	if model.Search.Native != nil && model.Capabilities.NativeWebSearch != nil && *model.Search.Native != *model.Capabilities.NativeWebSearch {
		return fmt.Errorf("model %q has conflicting native web-search declarations", name)
	}
	for _, declaration := range []struct {
		field string
		value string
	}{
		{field: "capabilityModes.tools", value: model.CapabilityModes.Tools},
		{field: "capabilityModes.parallelTools", value: model.CapabilityModes.ParallelTools},
		{field: "capabilityModes.vision", value: model.CapabilityModes.Vision},
		{field: "capabilityModes.reasoning", value: model.CapabilityModes.Reasoning},
		{field: "capabilityModes.reasoningSummary", value: model.CapabilityModes.ReasoningSummary},
		{field: "capabilityModes.webSearch", value: model.CapabilityModes.WebSearch},
	} {
		validator := validateModelCapabilityMode
		if declaration.field == "capabilityModes.webSearch" {
			validator = validateModelWebSearchMode
		}
		if err := validator(name, declaration.field, declaration.value); err != nil {
			return err
		}
	}
	if value := model.Search.Fallback.Model; strings.TrimSpace(value) != value {
		return fmt.Errorf("model %q search fallback model must not contain surrounding whitespace", name)
	}
	if value := model.Search.Fallback.Effort; value != strings.TrimSpace(value) || len(value) > 64 || strings.ContainsAny(value, " \t\r\n") {
		return fmt.Errorf("model %q search fallback effort must be a single model value", name)
	}
	for field, value := range map[string]string{
		"thinkingMode":          model.Reasoning.ThinkingMode,
		"historyPolicy":         model.Reasoning.HistoryPolicy,
		"toolChoice":            model.Tools.ToolChoice,
		"parallel":              model.Tools.Parallel,
		"parallelEnforcement":   model.Tools.ParallelEnforcement,
		"strictSchema":          model.Tools.StrictSchema,
		"emptyAssistantContent": model.Tools.EmptyAssistantContent,
		"plainTextToolCall":     model.Tools.PlainTextToolCall,
		"customToolMode":        model.Tools.CustomToolMode,
		"temperature":           model.Sampling.Temperature,
		"topP":                  model.Sampling.TopP,
	} {
		if err := validateModelEnum(name, field, value); err != nil {
			return err
		}
	}
	if err := validateHTTPPolicy("model "+name, model.HTTP); err != nil {
		return err
	}
	if err := validateStreamPolicy("model "+name, model.Stream); err != nil {
		return err
	}
	for operation, route := range model.Routes {
		operation = strings.TrimSpace(operation)
		if operation == "" || strings.TrimSpace(route.Interface) == "" || strings.TrimSpace(route.Adapter) == "" || strings.TrimSpace(route.Protocol) == "" {
			return fmt.Errorf("model %q has incomplete route %q", name, operation)
		}
		switch strings.ToLower(strings.TrimSpace(route.Protocol)) {
		case "responses", "chat-completions", "anthropic", "beta", "fim":
		default:
			return fmt.Errorf("model %q route %q has invalid protocol %q", name, operation, route.Protocol)
		}
	}
	for field, value := range map[string]string{
		"responses.structuredOutput.jsonObject": model.Responses.StructuredOutput.JSONObject,
		"responses.structuredOutput.jsonSchema": model.Responses.StructuredOutput.JSONSchema,
	} {
		if err := validateCapabilityMode(name, field, value); err != nil {
			return err
		}
	}
	return nil
}

func validateModelEnum(model, field, value string) error {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return nil
	}
	allowed := map[string][]string{
		"thinkingMode":          {"disabled", "auto", "always", "effort-dependent", "provider-default"},
		"historyPolicy":         {"never", "tool-calls-only", "always", "provider-default"},
		"toolChoice":            {"full", "auto-only", "omit"},
		"parallel":              {"auto", "enabled", "disabled"},
		"parallelEnforcement":   {"advisory", "strict"},
		"strictSchema":          {"preserve", "strip"},
		"emptyAssistantContent": {"preserve", "empty-string", "omit"},
		"plainTextToolCall":     {"allow", "reject"},
		"customToolMode":        {"function", "shell-fallback", "omit"},
		"temperature":           {"forward", "strip", "strip-when-reasoning"},
		"topP":                  {"forward", "strip", "strip-when-reasoning"},
	}
	for _, candidate := range allowed[field] {
		if value == candidate {
			return nil
		}
	}
	return fmt.Errorf("model %q has invalid %s %q (allowed: %s)", model, field, value, strings.Join(allowed[field], ", "))
}

func validateCapabilityMode(model, field, value string) error {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return nil
	}
	for _, allowed := range []string{"native", "advisory", "unsupported"} {
		if value == allowed {
			return nil
		}
	}
	return fmt.Errorf("model %q has invalid %s %q (allowed: native, advisory, unsupported)", model, field, value)
}

func validateModelCapabilityMode(model, field, value string) error {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return nil
	}
	for _, allowed := range []string{"native", "translated", "plugin", "advisory", "unsupported", "unknown"} {
		if value == allowed {
			return nil
		}
	}
	return fmt.Errorf("model %q has invalid %s %q (allowed: native, translated, plugin, advisory, unsupported, unknown)", model, field, value)
}

func validateModelWebSearchMode(model, field, value string) error {
	if strings.EqualFold(strings.TrimSpace(value), "fallback") {
		return nil
	}
	return validateModelCapabilityMode(model, field, value)
}

func validateHTTPPolicy(owner string, policy ModelHTTPPolicy) error {
	if policy.TimeoutSeconds < 0 || policy.ResponseHeaderTimeoutSeconds < 0 || policy.MaxConcurrentRequests < 0 || (policy.MaxRetries != nil && *policy.MaxRetries < 0) {
		return fmt.Errorf("%s has invalid http timeout or retries", owner)
	}
	for _, status := range policy.RetryStatuses {
		if status < 100 || status > 599 {
			return fmt.Errorf("%s has invalid retry status %d", owner, status)
		}
	}
	return nil
}

func validateStreamPolicy(owner string, policy ModelStreamPolicy) error {
	if policy.IdleTimeoutSeconds < 0 || policy.FirstEventTimeoutSeconds < 0 || policy.SemanticProgressTimeoutSeconds < 0 || policy.MaxDurationSeconds < 0 {
		return fmt.Errorf("%s has invalid stream timeout", owner)
	}
	switch strings.ToLower(strings.TrimSpace(policy.UpstreamMode)) {
	case "", "stream", "nonstream-buffered", "auto":
		break
	default:
		return fmt.Errorf("%s has invalid stream upstreamMode %q", owner, policy.UpstreamMode)
	}
	switch strings.ToLower(strings.TrimSpace(policy.HeartbeatMode)) {
	case "", "ignore", "transport-only", "semantic":
		return nil
	default:
		return fmt.Errorf("%s has invalid stream heartbeatMode %q", owner, policy.HeartbeatMode)
	}
}

func findModelCredential(values map[string]ModelCredential, ref string) (ModelCredential, bool) {
	for name, value := range values {
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(ref)) {
			return value, true
		}
	}
	return ModelCredential{}, false
}

func findModelProvider(values map[string]ModelProvider, ref string) (ModelProvider, bool) {
	for name, value := range values {
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(ref)) {
			return value, true
		}
	}
	return ModelProvider{}, false
}

func FindModelDefinition(cfg Config, ref string) (string, ModelDefinition, bool) {
	rawRef := strings.TrimSpace(ref)
	ref = strings.ToLower(rawRef)
	qualifiedProvider, qualifiedModel, qualified := SplitQualifiedModelID(rawRef)
	qualifiedProvider = strings.ToLower(qualifiedProvider)
	qualifiedModel = strings.ToLower(qualifiedModel)
	matches := make([]modelMatch, 0, 1)
	names := make([]string, 0, len(cfg.Models))
	for name := range cfg.Models {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		model := cfg.Models[name]
		if qualified {
			if !strings.EqualFold(strings.TrimSpace(model.Provider), qualifiedProvider) {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(name), QualifiedModelID(qualifiedProvider, qualifiedModel)) || strings.EqualFold(strings.TrimSpace(name), qualifiedModel) {
				return name, model, true
			}
		}
		for _, alias := range append([]string{name}, model.Aliases...) {
			candidate := strings.ToLower(strings.TrimSpace(alias))
			if candidate != ref && (!qualified || candidate != qualifiedModel) {
				continue
			}
			if qualified && !strings.EqualFold(strings.TrimSpace(model.Provider), qualifiedProvider) {
				continue
			}
			matches = append(matches, modelMatch{name: name, model: model})
		}
	}
	if len(matches) == 1 {
		return matches[0].name, matches[0].model, true
	}
	return "", ModelDefinition{}, false
}

type modelMatch struct {
	name  string
	model ModelDefinition
}
