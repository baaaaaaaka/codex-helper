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
		ModelCatalogs      map[string]json.RawMessage `json:"modelCatalogs"`
		ProviderBindings   map[string]json.RawMessage `json:"modelProviderBindings"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return err
	}
	if envelope.ModelConfigVersion == 0 && len(envelope.ModelCredentials) == 0 && len(envelope.ModelProviders) == 0 && len(envelope.Models) == 0 && len(envelope.ModelProfiles) == 0 && len(envelope.ModelSources) == 0 && len(envelope.ModelCatalogs) == 0 && len(envelope.ProviderBindings) == 0 {
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
	for name, value := range envelope.ModelCatalogs {
		var out ModelCatalog
		if err := strictModelJSON(value, &out); err != nil {
			return fmt.Errorf("model catalog %q: %w", name, err)
		}
	}
	for name, value := range envelope.ProviderBindings {
		var out ModelProviderBinding
		if err := strictModelJSON(value, &out); err != nil {
			return fmt.Errorf("model provider binding %q: %w", name, err)
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
	if (len(cfg.ModelCredentials) > 0 || len(cfg.ModelProviders) > 0 || len(cfg.Models) > 0 || len(cfg.ModelCatalogs) > 0 || len(cfg.ModelProviderBindings) > 0) && cfg.ModelConfigVersion == 0 {
		return fmt.Errorf("modelConfigVersion is required when model credentials, providers, models, catalogs, or provider bindings are configured")
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
	seenProviders := map[string]string{}
	for name, provider := range cfg.ModelProviders {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("model provider name is empty")
		}
		providerKey := strings.ToLower(strings.TrimSpace(name))
		if previous, ok := seenProviders[providerKey]; ok && !strings.EqualFold(previous, name) {
			return fmt.Errorf("model provider names %q and %q differ only by case", previous, name)
		}
		seenProviders[providerKey] = name
		switch strings.ToLower(strings.TrimSpace(provider.Protocol)) {
		case "responses", "chat-completions":
		default:
			return fmt.Errorf("model provider %q has invalid protocol %q", name, provider.Protocol)
		}
		if err := validateProviderConversion(name, provider); err != nil {
			return err
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
		for interfaceName, ref := range provider.InterfaceCredentials {
			if _, ok := findModelInterface(provider.Interfaces, interfaceName); !ok {
				return fmt.Errorf("model provider %q interfaceCredentials references missing interface %q", name, interfaceName)
			}
			if _, ok := findModelCredential(cfg.ModelCredentials, ref); !ok {
				return fmt.Errorf("model provider %q interface %q references missing credential %q", name, interfaceName, ref)
			}
		}
		if err := validateHTTPPolicy("model provider "+name, provider.HTTP); err != nil {
			return err
		}
		if err := validateStreamPolicy("model provider "+name, provider.Stream); err != nil {
			return err
		}
		if len(provider.Interfaces) > 0 {
			if provider.DefaultInterface != "" {
				if _, ok := findModelInterface(provider.Interfaces, provider.DefaultInterface); !ok {
					return fmt.Errorf("model provider %q references missing defaultInterface %q", name, provider.DefaultInterface)
				}
			}
			for interfaceName, iface := range provider.Interfaces {
				if err := ValidateModelCatalogID(interfaceName); err != nil {
					return fmt.Errorf("model provider %q interface %q: %w", name, interfaceName, err)
				}
				if strings.TrimSpace(iface.Adapter) == "" {
					return fmt.Errorf("model provider %q interface %q requires adapter", name, interfaceName)
				}
				protocol := strings.ToLower(strings.TrimSpace(iface.Protocol))
				if protocol == "" {
					protocol = modelProtocolForAdapter(iface.Adapter)
				}
				if !modelProtocolSupported(protocol) {
					return fmt.Errorf("model provider %q interface %q has invalid protocol %q", name, interfaceName, iface.Protocol)
				}
				if expected := modelProtocolForAdapter(iface.Adapter); expected != "" && !modelProtocolsCompatible(expected, protocol) {
					return fmt.Errorf("model provider %q interface %q protocol %q is incompatible with adapter %q", name, interfaceName, protocol, iface.Adapter)
				}
				parsed, err := url.Parse(strings.TrimSpace(iface.BaseURL))
				if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
					return fmt.Errorf("model provider %q interface %q requires an absolute http(s) baseUrl", name, interfaceName)
				}
				if parsed.User != nil {
					return fmt.Errorf("model provider %q interface %q baseUrl must not contain credentials", name, interfaceName)
				}
				if err := validateModelConversion("model provider "+name+" interface "+interfaceName, iface.Conversion); err != nil {
					return err
				}
			}
		}
	}
	// Catalog routes are provider-scoped: two providers may legitimately
	// publish the same short model name. Keep aliases unique within a provider
	// and require callers to use provider/model when an alias is ambiguous.
	seenAliases := map[string]string{}
	for name, model := range cfg.Models {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(model.UpstreamModel) == "" {
			return fmt.Errorf("model %q requires upstreamModel", name)
		}
		if _, ok := findModelProvider(cfg.ModelProviders, model.Provider); !ok {
			return fmt.Errorf("model %q references missing provider %q", name, model.Provider)
		}
		if model.DefaultInterface != "" {
			provider, _ := findModelProvider(cfg.ModelProviders, model.Provider)
			if _, ok := findModelInterface(provider.Interfaces, model.DefaultInterface); !ok {
				return fmt.Errorf("model %q references missing defaultInterface %q", name, model.DefaultInterface)
			}
		}
		for _, alias := range append([]string{name}, model.Aliases...) {
			key := strings.ToLower(strings.TrimSpace(alias))
			if key == "" {
				continue
			}
			scope := strings.ToLower(strings.TrimSpace(model.Provider)) + "\x00" + key
			if previous, ok := seenAliases[scope]; ok && !strings.EqualFold(previous, name) {
				return fmt.Errorf("model alias %q conflicts between %q and %q", alias, previous, name)
			}
			seenAliases[scope] = name
		}
		if err := ValidateModelDefinition(name, model); err != nil {
			return err
		}
		for featureName, feature := range model.Features {
			switch feature.Support {
			case "native", "translated", "plugin", "unsupported":
			default:
				return fmt.Errorf("model %q feature %q has invalid support %q", name, featureName, feature.Support)
			}
			featureName = strings.TrimSpace(featureName)
			if featureName == "" {
				return fmt.Errorf("model %q has an empty feature name", name)
			}
			if feature.RequireSources || feature.Sources != nil || feature.NativeTool != nil || feature.Fallback != nil {
				if featureName != "webSearch" {
					return fmt.Errorf("model %q feature %q has web-search-only fields", name, featureName)
				}
			}
			if feature.Interface != "" && !featureRouteCapable(featureName, feature.Operation) {
				return fmt.Errorf("model %q feature %q interface is not bound to an operation", name, featureName)
			}
			if feature.Support == "unsupported" && (feature.Interface != "" || feature.Operation != "" || feature.Fallback != nil || feature.NativeTool != nil) {
				return fmt.Errorf("model %q unsupported feature %q must not declare a route or fallback", name, featureName)
			}
			if feature.Interface != "" {
				provider, ok := findModelProvider(cfg.ModelProviders, model.Provider)
				if !ok || len(provider.Interfaces) == 0 {
					return fmt.Errorf("model %q feature %q references interface without provider interfaces", name, featureName)
				}
				if _, ok := findModelInterface(provider.Interfaces, feature.Interface); !ok {
					return fmt.Errorf("model %q feature %q references missing interface %q", name, featureName, feature.Interface)
				}
			}
			if feature.Support == "plugin" && featureName == "webSearch" && (feature.Fallback == nil || strings.TrimSpace(feature.Fallback.Selector) == "") {
				return fmt.Errorf("model %q plugin feature %q requires fallback.selector", name, featureName)
			}
			if (feature.Support == "native" || feature.Support == "translated") && featureRouteCapable(featureName, feature.Operation) && feature.Interface == "" {
				return fmt.Errorf("model %q %s feature %q requires interface", name, feature.Support, featureName)
			}
			if feature.Support == "plugin" && (feature.Interface != "" || feature.Operation != "") {
				return fmt.Errorf("model %q plugin feature %q must not declare an upstream route", name, featureName)
			}
			if feature.NativeTool != nil {
				if feature.Support != "native" {
					return fmt.Errorf("model %q feature %q nativeTool requires support=native", name, featureName)
				}
				if len(feature.NativeTool.InputTypes) == 0 || strings.TrimSpace(feature.NativeTool.UpstreamType) == "" {
					return fmt.Errorf("model %q feature %q nativeTool requires inputTypes and upstreamType", name, featureName)
				}
				seenNativeTypes := map[string]bool{}
				for _, value := range feature.NativeTool.InputTypes {
					value = strings.TrimSpace(value)
					if value == "" || strings.ContainsAny(value, " \t\r\n") || seenNativeTypes[strings.ToLower(value)] {
						return fmt.Errorf("model %q feature %q nativeTool has invalid inputType %q", name, featureName, value)
					}
					seenNativeTypes[strings.ToLower(value)] = true
				}
			}
			if feature.Sources != nil {
				mode := strings.ToLower(strings.TrimSpace(feature.Sources.Mode))
				switch mode {
				case "", "annotations", "text", "unsupported":
				default:
					return fmt.Errorf("model %q feature %q has invalid sources.mode %q", name, featureName, feature.Sources.Mode)
				}
				if mode == "unsupported" && (feature.Sources.RequireURL || feature.Sources.RequireSources || feature.RequireSources) {
					return fmt.Errorf("model %q feature %q cannot require sources when sources.mode is unsupported", name, featureName)
				}
				if mode == "text" && (feature.Sources.RequireURL || feature.Sources.RequireSources || feature.RequireSources) {
					return fmt.Errorf("model %q feature %q cannot require URL/structured sources when sources.mode is text", name, featureName)
				}
			}
			if err := validateModelOperation(name, featureName, feature.Operation); err != nil {
				return err
			}
		}
		if err := ValidateModelFeatureRoutes(name, model.Features); err != nil {
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
		switch kind := strings.ToLower(strings.TrimSpace(source.Kind)); kind {
		case "", "git":
			if strings.TrimSpace(source.URL) == "" {
				return fmt.Errorf("model source %q requires url", name)
			}
		case "file", "directory":
			if strings.TrimSpace(source.Path) == "" {
				return fmt.Errorf("model source %q requires path", name)
			}
			if strings.TrimSpace(source.URL) != "" {
				return fmt.Errorf("model source %q local kind %q must not include url", name, kind)
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
	for name, catalog := range cfg.ModelCatalogs {
		if err := catalog.Validate(name); err != nil {
			return err
		}
	}
	for provider, binding := range cfg.ModelProviderBindings {
		if err := binding.Validate(provider, cfg.ModelCatalogs); err != nil {
			return err
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
			return fmt.Errorf("defaults.model %q must be `default`, `official:<slug>`, or `profile:<name>`", selector)
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
		default:
			return fmt.Errorf("defaults.model %q must be `default`, `official:<slug>`, or `profile:<name>`", selector)
		}
	}
	rawEffort := cfg.Defaults.ReasoningEffort
	effort := strings.TrimSpace(rawEffort)
	if rawEffort != effort || len(effort) > 64 || strings.ContainsAny(effort, " \t\r\n") {
		return fmt.Errorf("defaults.reasoningEffort %q must be a single model-advertised value", effort)
	}
	return nil
}

func findModelInterface(interfaces map[string]ModelInterface, ref string) (ModelInterface, bool) {
	for name, iface := range interfaces {
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(ref)) {
			return iface, true
		}
	}
	return ModelInterface{}, false
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

// ValidateModelDefinition validates the policy-bearing portion of a model
// definition. External catalog readers use the same validator so a catalog
// cannot pass its own parser and then fail later when it is materialized into
// the local config schema.
func ValidateModelDefinition(name string, model ModelDefinition) error {
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
		"images":                model.Messages.Images,
		"audio":                 model.Messages.Audio,
		"video":                 model.Messages.Video,
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
	for field, value := range map[string]string{
		"responses.previousResponseId": model.Responses.PreviousResponseID,
		"responses.background":         model.Responses.Background,
		"responses.contextManagement":  model.Responses.ContextManagement,
	} {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "", "native", "unsupported", "delegated":
		default:
			return fmt.Errorf("model %q has invalid %s policy %q", name, field, value)
		}
	}
	return nil
}

// validateModelDefinition is kept as a private compatibility shim for the
// split-catalog loader. The canonical validator is exported so the newer
// model-catalog package can share exactly the same policy checks.
func validateModelDefinition(name string, model ModelDefinition) error {
	return ValidateModelDefinition(name, model)
}

// ValidateModelFeatureRoutes rejects ambiguous operation selectors before
// they can be projected into the runtime route map. Without this check, two
// JSON features could publish the same operation with different interfaces;
// map iteration order would then silently decide which credential and wire
// protocol was used.
func ValidateModelFeatureRoutes(name string, features map[string]ModelFeature) error {
	routes := map[string]string{}
	for featureName, feature := range features {
		if feature.Support == "unsupported" || feature.Support == "plugin" || strings.TrimSpace(feature.Interface) == "" {
			continue
		}
		key := modelFeatureRouteKey(featureName, feature.Operation)
		if key == "" {
			continue
		}
		interfaceName := strings.TrimSpace(feature.Interface)
		if previous, ok := routes[key]; ok && !strings.EqualFold(previous, interfaceName) {
			return fmt.Errorf("model %q operation route %q maps to conflicting interfaces %q and %q", name, key, previous, interfaceName)
		}
		routes[key] = interfaceName
	}
	return nil
}

func modelFeatureRouteKey(featureName, operation string) string {
	featureName = strings.ToLower(strings.TrimSpace(featureName))
	if featureName == "websearch" {
		return "websearch"
	}
	if operation = strings.ToLower(strings.TrimSpace(operation)); operation != "" {
		return operation
	}
	switch featureName {
	case "chat", "responses", "prefix", "fim":
		return featureName
	default:
		return ""
	}
}

func validateModelConversion(owner string, conversion ModelConversion) error {
	profile := strings.TrimSpace(conversion.Profile)
	if conversion.Enabled && profile == "" {
		return fmt.Errorf("%s conversion.enabled requires conversion.profile", owner)
	}
	if !conversion.Enabled && profile != "" {
		return fmt.Errorf("%s conversion.profile requires conversion.enabled", owner)
	}
	if conversion.Strict != nil && !conversion.Enabled {
		return fmt.Errorf("%s conversion.strict requires conversion.enabled", owner)
	}
	if len(profile) > 128 || strings.ContainsAny(profile, " \t\r\n/\\") {
		return fmt.Errorf("%s conversion.profile %q is invalid", owner, profile)
	}
	return nil
}

func validateProviderConversion(name string, provider ModelProvider) error {
	profile := strings.TrimSpace(provider.ConversionProfile)
	if profile != "" && (len(profile) > 128 || strings.ContainsAny(profile, " \t\r\n/\\")) {
		return fmt.Errorf("model provider %q conversionProfile %q is invalid", name, profile)
	}
	operation := strings.ToLower(strings.TrimSpace(provider.Operation))
	if operation != "" && operation != "chat" && operation != "responses" && operation != "prefix" && operation != "fim" {
		return fmt.Errorf("model provider %q has invalid operation %q", name, provider.Operation)
	}
	return nil
}

func validateModelOperation(model, feature, operation string) error {
	operation = strings.ToLower(strings.TrimSpace(operation))
	feature = strings.ToLower(strings.TrimSpace(feature))
	if operation == "" {
		if feature == "fim" || feature == "prefix" {
			return fmt.Errorf("model %q feature %q requires operation %q", model, feature, feature)
		}
		return nil
	}
	switch operation {
	case "chat", "responses", "prefix", "fim":
	default:
		return fmt.Errorf("model %q feature %q has invalid operation %q (allowed: chat, responses, prefix, fim)", model, feature, operation)
	}
	if (feature == "fim" || feature == "prefix") && operation != feature {
		return fmt.Errorf("model %q feature %q must use operation %q", model, feature, feature)
	}
	return nil
}

func featureRouteCapable(feature, operation string) bool {
	feature = strings.ToLower(strings.TrimSpace(feature))
	operation = strings.ToLower(strings.TrimSpace(operation))
	if operation != "" {
		return true
	}
	switch feature {
	case "chat", "responses", "websearch", "prefix", "fim":
		return true
	default:
		return false
	}
}

func modelProtocolForAdapter(adapter string) string {
	switch strings.ToLower(strings.TrimSpace(adapter)) {
	case "openai-responses", "mimo-responses":
		return "responses"
	case "deepseek-anthropic":
		return "messages"
	case "deepseek-beta":
		return "beta"
	default:
		return "chat-completions"
	}
}

func modelProtocolSupported(protocol string) bool {
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case "chat-completions", "responses", "messages", "anthropic", "beta", "fim":
		return true
	default:
		return false
	}
}

func modelProtocolsCompatible(expected, actual string) bool {
	expected = strings.ToLower(strings.TrimSpace(expected))
	actual = strings.ToLower(strings.TrimSpace(actual))
	if expected == actual {
		return true
	}
	return (expected == "messages" && actual == "anthropic") || (expected == "anthropic" && actual == "messages") || (expected == "beta" && actual == "fim") || (expected == "fim" && actual == "beta")
}

func validateModelEnum(model, field, value string) error {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return nil
	}
	allowed := map[string][]string{
		"thinkingMode":          {"disabled", "auto", "always", "effort-dependent", "provider-default"},
		"historyPolicy":         {"never", "tool-calls-only", "always", "omit", "drop", "text-only", "preserve", "keep", "provider-default"},
		"toolChoice":            {"full", "auto-only", "omit"},
		"parallel":              {"auto", "enabled", "disabled"},
		"parallelEnforcement":   {"advisory", "strict"},
		"strictSchema":          {"preserve", "strip"},
		"emptyAssistantContent": {"preserve", "empty-string", "omit"},
		"plainTextToolCall":     {"allow", "reject"},
		"customToolMode":        {"function", "shell-fallback", "omit"},
		"temperature":           {"forward", "strip", "strip-when-reasoning"},
		"topP":                  {"forward", "strip", "strip-when-reasoning"},
		"images":                {"allow", "enabled", "forward", "multimodal", "omit", "drop", "provider-default"},
		"audio":                 {"allow", "enabled", "forward", "multimodal", "omit", "drop", "provider-default"},
		"video":                 {"allow", "enabled", "forward", "multimodal", "omit", "drop", "provider-default"},
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
	switch strings.ToLower(strings.TrimSpace(policy.HeartbeatMode)) {
	case "", "ignore", "transport", "transport-only", "semantic":
	default:
		return fmt.Errorf("%s has invalid stream heartbeatMode %q", owner, policy.HeartbeatMode)
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
	// Exact provider/model selectors are stable even when their short aliases
	// are also published by another provider.
	for _, name := range names {
		if strings.ToLower(strings.TrimSpace(name)) == ref {
			return name, cfg.Models[name], true
		}
	}
	matchedName := ""
	var matched ModelDefinition
	for _, name := range names {
		model := cfg.Models[name]
		for _, alias := range append([]string{name}, model.Aliases...) {
			if strings.ToLower(strings.TrimSpace(alias)) == ref {
				if matchedName != "" && !strings.EqualFold(matchedName, name) {
					return "", ModelDefinition{}, false
				}
				matchedName, matched = name, model
			}
		}
	}
	return matchedName, matched, matchedName != ""
}
