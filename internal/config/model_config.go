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
		for _, alias := range append([]string{name}, model.Aliases...) {
			key := strings.ToLower(strings.TrimSpace(alias))
			if key == "" {
				continue
			}
			if previous, ok := seenAliases[key]; ok && !strings.EqualFold(previous, name) {
				return fmt.Errorf("model alias %q conflicts between %q and %q", alias, previous, name)
			}
			seenAliases[key] = name
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
	for name, source := range cfg.ModelSources {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(source.URL) == "" {
			return fmt.Errorf("model source %q requires url", name)
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
	for field, value := range map[string]string{
		"thinkingMode":          model.Reasoning.ThinkingMode,
		"historyPolicy":         model.Reasoning.HistoryPolicy,
		"toolChoice":            model.Tools.ToolChoice,
		"parallel":              model.Tools.Parallel,
		"strictSchema":          model.Tools.StrictSchema,
		"emptyAssistantContent": model.Tools.EmptyAssistantContent,
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
	return validateStreamPolicy("model "+name, model.Stream)
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
		"strictSchema":          {"preserve", "strip"},
		"emptyAssistantContent": {"preserve", "empty-string", "omit"},
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
	if policy.IdleTimeoutSeconds < 0 {
		return fmt.Errorf("%s has invalid stream idle timeout", owner)
	}
	switch strings.ToLower(strings.TrimSpace(policy.UpstreamMode)) {
	case "", "stream", "nonstream-buffered", "auto":
		return nil
	default:
		return fmt.Errorf("%s has invalid stream upstreamMode %q", owner, policy.UpstreamMode)
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
	ref = strings.ToLower(strings.TrimSpace(ref))
	names := make([]string, 0, len(cfg.Models))
	for name := range cfg.Models {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		model := cfg.Models[name]
		for _, alias := range append([]string{name}, model.Aliases...) {
			if strings.ToLower(strings.TrimSpace(alias)) == ref {
				return name, model, true
			}
		}
	}
	return "", ModelDefinition{}, false
}
