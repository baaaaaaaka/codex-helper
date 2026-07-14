package responsesadapter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type ProviderRegistryOptions struct {
	DefaultProvider string
	Providers       []ProviderConfig
	ProxyKeys       map[string]string
	KeySalt         string
}

type ProviderConfig struct {
	ID                    string
	ProfileID             string
	BaseURL               string
	APIKey                string
	DefaultModel          string
	Models                []ModelInfo
	Adapter               ProviderAdapter
	CustomToolMode        string
	UnsupportedToolPolicy string
	ConversionProfile     string
	StrictConversion      bool
	Operation             string
	NativeTools           []NativeToolSpec
	SourcePolicy          SourcePolicy
	ResponsesPolicy       ResponsesPolicy
	// Routes contains operation-specific adapters. The default ProviderConfig
	// fields remain the fallback route; a request with an explicit operation or
	// a declared native web-search tool selects the matching route before any
	// upstream request is made.
	Routes []ProviderRouteConfig
}

type ProviderRouteConfig struct {
	Key       string
	Operation string
	Adapter   ProviderAdapter
	ProfileID string
	// BaseURL and APIKey are in-memory route metadata used only to derive
	// runtime identity. APIKey is never serialized or emitted; keeping it here
	// makes cache/stats scopes distinguish interfaces that use different
	// credentials while the adapter itself retains the actual secret.
	BaseURL               string
	APIKey                string
	CustomToolMode        string
	UnsupportedToolPolicy string
	ConversionProfile     string
	StrictConversion      bool
	NativeTools           []NativeToolSpec
	SourcePolicy          SourcePolicy
	ResponsesPolicy       ResponsesPolicy
}

type ProviderRegistry struct {
	defaultProvider string
	providers       []registeredProvider
	byID            map[string]int
	byModel         map[string][]int
	proxyKeys       map[string]string
	models          []ModelInfo
}

type registeredProvider struct {
	id            string
	models        map[string]bool
	routes        map[string]string
	listModels    []ModelInfo
	openModel     bool
	runtime       ProviderRuntime
	routesRuntime map[string]ProviderRuntime
}

func NewProviderRegistry(opts ProviderRegistryOptions) (*ProviderRegistry, error) {
	if len(opts.Providers) == 0 {
		return nil, fmt.Errorf("at least one provider is required")
	}
	registry := &ProviderRegistry{
		defaultProvider: normalizeProviderID(opts.DefaultProvider),
		byID:            map[string]int{},
		byModel:         map[string][]int{},
		proxyKeys:       map[string]string{},
	}
	for key, provider := range opts.ProxyKeys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		normalizedProvider := normalizeProviderID(provider)
		if normalizedProvider == "" {
			normalizedProvider = "*"
		}
		registry.proxyKeys[key] = normalizedProvider
	}
	for _, cfg := range opts.Providers {
		if err := ValidateConversionProfile(cfg.ConversionProfile); err != nil {
			return nil, fmt.Errorf("provider %q: %w", cfg.ID, err)
		}
		if err := validateSourcePolicy(cfg.SourcePolicy); err != nil {
			return nil, fmt.Errorf("provider %q: %w", cfg.ID, err)
		}
		for _, route := range cfg.Routes {
			key := strings.ToLower(strings.TrimSpace(route.Key))
			if key == "" {
				return nil, fmt.Errorf("provider %q has a route without a key", cfg.ID)
			}
			if route.Adapter == nil {
				return nil, fmt.Errorf("provider %q route %q has no adapter", cfg.ID, route.Key)
			}
			if err := validateSourcePolicy(route.SourcePolicy); err != nil {
				return nil, fmt.Errorf("provider %q route %q: %w", cfg.ID, route.Key, err)
			}
			switch operation := strings.ToLower(strings.TrimSpace(route.Operation)); operation {
			case "", "chat", "responses", "prefix", "fim":
			default:
				return nil, fmt.Errorf("provider %q route %q has invalid operation %q", cfg.ID, route.Key, route.Operation)
			}
		}
		registered, err := buildRegisteredProvider(cfg, opts.KeySalt)
		if err != nil {
			return nil, err
		}
		if _, exists := registry.byID[registered.id]; exists {
			return nil, fmt.Errorf("duplicate provider id %q", registered.id)
		}
		idx := len(registry.providers)
		registry.byID[registered.id] = idx
		registry.providers = append(registry.providers, registered)
		for model := range registered.models {
			registry.byModel[model] = append(registry.byModel[model], idx)
		}
		for _, model := range providerModelsForList(registered) {
			registry.models = append(registry.models, model)
		}
	}
	if registry.defaultProvider == "" {
		registry.defaultProvider = registry.providers[0].id
	}
	if _, ok := registry.byID[registry.defaultProvider]; !ok {
		return nil, fmt.Errorf("default provider %q is not configured", registry.defaultProvider)
	}
	for _, provider := range registry.proxyKeys {
		if provider == "*" {
			continue
		}
		if _, ok := registry.byID[provider]; !ok {
			return nil, fmt.Errorf("proxy key locks to unknown provider %q", provider)
		}
	}
	return registry, nil
}

func (r *ProviderRegistry) Models() []ModelInfo {
	out := make([]ModelInfo, len(r.models))
	copy(out, r.models)
	return out
}

func (r *ProviderRegistry) Resolve(req *http.Request, body ResponsesRequest) (ProviderRuntime, error) {
	lock, err := r.resolveProxyLock(req)
	if err != nil {
		return ProviderRuntime{}, err
	}
	explicit := normalizeProviderID(firstNonEmpty(
		req.Header.Get("x-codex-provider"),
		req.Header.Get("x-adapter-provider"),
	))
	if lock != "" && lock != "*" {
		if explicit != "" && explicit != lock {
			return ProviderRuntime{}, routeErrorf(http.StatusUnauthorized, "proxy key is locked to provider %q", lock)
		}
		if strings.TrimSpace(body.Model) != "" && !r.modelCanRouteToProvider(body.Model, lock) {
			return ProviderRuntime{}, routeErrorf(http.StatusUnauthorized, "proxy key is locked to provider %q, but model %q routes elsewhere", lock, body.Model)
		}
		explicit = lock
	}
	if explicit != "" {
		return r.resolveWithProvider(explicit, body.Model, body)
	}
	model := strings.TrimSpace(body.Model)
	if model == "" {
		return r.resolveWithProvider(r.defaultProvider, "", body)
	}
	matches := r.byModel[normalizeModelID(model)]
	switch len(matches) {
	case 0:
		defaultProvider := r.providers[r.byID[r.defaultProvider]]
		if defaultProvider.openModel {
			return checkedRuntimeForRequest(defaultProvider, model, body)
		}
		return ProviderRuntime{}, routeErrorf(http.StatusBadRequest, "model %q is not configured for any provider", model)
	case 1:
		return checkedRuntimeForRequest(r.providers[matches[0]], model, body)
	default:
		return ProviderRuntime{}, routeErrorf(http.StatusConflict, "model %q is configured by multiple providers; set x-codex-provider or use a provider-locked proxy key", model)
	}
}

func (r *ProviderRegistry) modelCanRouteToProvider(model string, providerID string) bool {
	idx, ok := r.byID[normalizeProviderID(providerID)]
	if !ok {
		return false
	}
	provider := r.providers[idx]
	if provider.openModel {
		return true
	}
	return provider.models[normalizeModelID(model)]
}

func (r *ProviderRegistry) resolveProxyLock(req *http.Request) (string, error) {
	if len(r.proxyKeys) == 0 {
		return "", nil
	}
	header := strings.TrimSpace(req.Header.Get("Authorization"))
	presented := ""
	if strings.HasPrefix(strings.ToLower(header), "bearer ") {
		presented = strings.TrimSpace(header[len("Bearer "):])
	}
	if presented == "" {
		return "", routeErrorf(http.StatusUnauthorized, "missing proxy authorization key")
	}
	lock, ok := r.proxyKeys[presented]
	if !ok {
		return "", routeErrorf(http.StatusUnauthorized, "invalid proxy authorization key")
	}
	return lock, nil
}

func (r *ProviderRegistry) resolveWithProvider(providerID string, model string, body ResponsesRequest) (ProviderRuntime, error) {
	idx, ok := r.byID[normalizeProviderID(providerID)]
	if !ok {
		return ProviderRuntime{}, routeErrorf(http.StatusBadRequest, "provider %q is not configured", providerID)
	}
	provider := r.providers[idx]
	model = firstNonEmpty(model, provider.runtime.Model)
	if strings.TrimSpace(model) == "" {
		return ProviderRuntime{}, routeErrorf(http.StatusBadRequest, "model is required for provider %q", provider.id)
	}
	if !provider.openModel && !provider.models[normalizeModelID(model)] {
		return ProviderRuntime{}, routeErrorf(http.StatusBadRequest, "model %q is not configured for provider %q", model, provider.id)
	}
	return checkedRuntimeForRequest(provider, model, body)
}

func checkedRuntimeForRequest(provider registeredProvider, model string, body ResponsesRequest) (ProviderRuntime, error) {
	runtime := runtimeForRequest(provider, model, body)
	if requestRouteKey(body) != "" && runtime.Adapter == nil {
		key := requestRouteKey(body)
		if key == "websearch" {
			return ProviderRuntime{}, routeErrorf(http.StatusBadRequest, "provider %q has no configured web-search route", provider.id)
		}
		return ProviderRuntime{}, routeErrorf(http.StatusBadRequest, "provider %q has no configured %q route", provider.id, key)
	}
	return runtime, nil
}

func buildRegisteredProvider(cfg ProviderConfig, keySalt string) (registeredProvider, error) {
	id := normalizeProviderID(cfg.ID)
	if id == "" {
		return registeredProvider{}, fmt.Errorf("provider id is required")
	}
	profileID := firstNonEmpty(cfg.ProfileID, id)
	baseURL := inferProviderBaseURL(profileID, cfg.BaseURL, cfg.APIKey)
	if strings.TrimSpace(baseURL) == "" && cfg.Adapter == nil {
		return registeredProvider{}, fmt.Errorf("provider %q base URL is required", id)
	}
	models := map[string]bool{}
	routes := map[string]string{}
	var listModels []ModelInfo
	for _, model := range cfg.Models {
		id := strings.TrimSpace(model.ID)
		if id != "" {
			norm := normalizeModelID(id)
			upstream := strings.TrimSpace(firstNonEmpty(model.UpstreamID, id))
			if !models[norm] {
				listModels = append(listModels, ModelInfo{ID: id, OwnedBy: firstNonEmpty(model.OwnedBy, cfg.ID)})
			}
			models[norm] = true
			routes[norm] = upstream
		}
	}
	if strings.TrimSpace(cfg.DefaultModel) != "" {
		norm := normalizeModelID(cfg.DefaultModel)
		if !models[norm] {
			listModels = append(listModels, ModelInfo{ID: strings.TrimSpace(cfg.DefaultModel), OwnedBy: cfg.ID})
		}
		models[norm] = true
		if strings.TrimSpace(routes[norm]) == "" {
			routes[norm] = strings.TrimSpace(cfg.DefaultModel)
		}
	}
	adapter := cfg.Adapter
	if adapter == nil {
		var err error
		adapter, err = NewConfiguredAdapter(AdapterOptions{
			AdapterID: profileID, ConversionProfile: cfg.ConversionProfile, StrictConversion: cfg.StrictConversion,
			BaseURL: baseURL, APIKey: cfg.APIKey, Profile: ProfileForProvider(profileID),
		})
		if err != nil {
			return registeredProvider{}, fmt.Errorf("provider %q adapter: %w", id, err)
		}
	}
	profileVersion := firstNonEmpty(profileID, id) + ":v1"
	if conversion := strings.TrimSpace(cfg.ConversionProfile); conversion != "" {
		profileVersion += ":" + conversion
	}
	return registeredProvider{
		id:         id,
		models:     models,
		routes:     routes,
		listModels: listModels,
		openModel:  len(models) == 0,
		runtime: ProviderRuntime{
			Adapter:               adapter,
			ProviderID:            id,
			PublicModel:           strings.TrimSpace(cfg.DefaultModel),
			Model:                 strings.TrimSpace(cfg.DefaultModel),
			KeyFingerprint:        KeyFingerprint(cfg.APIKey, keySalt),
			BaseURLHash:           BaseURLHash(baseURL),
			ProfileVersion:        profileVersion,
			CustomToolMode:        strings.TrimSpace(cfg.CustomToolMode),
			UnsupportedToolPolicy: strings.TrimSpace(cfg.UnsupportedToolPolicy),
			ConversionProfile:     strings.TrimSpace(cfg.ConversionProfile),
			StrictConversion:      cfg.StrictConversion,
			Operation:             strings.TrimSpace(cfg.Operation),
			NativeTools:           append([]NativeToolSpec(nil), cfg.NativeTools...),
			SourcePolicy:          cfg.SourcePolicy,
			ResponsesPolicy:       cfg.ResponsesPolicy,
		},
		routesRuntime: buildRouteRuntimes(cfg, id, baseURL, profileVersion, cfg.APIKey, keySalt),
	}, nil
}

func providerModelsForList(provider registeredProvider) []ModelInfo {
	if len(provider.models) == 0 {
		if provider.runtime.Model == "" {
			return nil
		}
		return []ModelInfo{{ID: provider.runtime.Model, OwnedBy: provider.id}}
	}
	out := make([]ModelInfo, len(provider.listModels))
	copy(out, provider.listModels)
	return out
}

func runtimeForModel(provider registeredProvider, model string) ProviderRuntime {
	runtime := provider.runtime
	publicModel := strings.TrimSpace(model)
	runtime.PublicModel = publicModel
	upstreamModel := strings.TrimSpace(provider.routes[normalizeModelID(publicModel)])
	if upstreamModel == "" {
		upstreamModel = publicModel
	}
	runtime.Model = upstreamModel
	return runtime
}

func runtimeForRequest(provider registeredProvider, model string, body ResponsesRequest) ProviderRuntime {
	runtime := runtimeForModel(provider, model)
	key := requestRouteKey(body)
	if key == "" {
		// A model may use a capability-specific default interface (for example
		// DeepSeek's Anthropic web-search route) while ordinary chat remains on
		// a different interface. Codex does not send an operation for ordinary
		// turns, so prefer the explicitly declared chat route before falling
		// back to the provider-level adapter.
		if _, ok := provider.routesRuntime["chat"]; ok {
			key = "chat"
		} else if _, ok := provider.routesRuntime["responses"]; ok {
			key = "responses"
		}
	}
	if key == "" {
		return runtime
	}
	if route, ok := provider.routesRuntime[key]; ok {
		route.PublicModel = strings.TrimSpace(model)
		route.Model = strings.TrimSpace(provider.routes[normalizeModelID(model)])
		if route.Model == "" {
			route.Model = strings.TrimSpace(model)
		}
		return route
	}
	if strings.TrimSpace(body.Operation) != "" {
		// Never let an explicit operation fall through to a chat adapter. This
		// was the source of silent DeepSeek Beta/FIM misrouting.
		return ProviderRuntime{ProviderID: provider.id, PublicModel: model, Model: model, Operation: strings.ToLower(strings.TrimSpace(body.Operation)), UnsupportedToolPolicy: "error"}
	}
	return runtime
}

func requestRouteKey(body ResponsesRequest) string {
	if operation := strings.ToLower(strings.TrimSpace(body.Operation)); operation != "" {
		return operation
	}
	if hasNativeWebSearchTool(body.Tools) {
		return "websearch"
	}
	return ""
}

func hasNativeWebSearchTool(raw []byte) bool {
	if len(raw) == 0 {
		return false
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	return containsNativeWebSearchTool(value)
}

func containsNativeWebSearchTool(value any) bool {
	switch item := value.(type) {
	case []any:
		for _, child := range item {
			if containsNativeWebSearchTool(child) {
				return true
			}
		}
	case map[string]any:
		// Only a tool definition's type identifies a native search request.
		// Looking at name/upstream_type (or recursively walking arbitrary
		// parameter schemas) misclassifies an ordinary function named
		// "web_search" and can route it to an unconfigured search endpoint.
		if text, ok := item["type"].(string); ok && (strings.EqualFold(text, "web_search") || strings.EqualFold(text, "web_search_preview")) {
			return true
		}
		// Namespaced tool declarations are the one supported nested shape. Do
		// not recurse into function parameters or arbitrary metadata.
		if nested, ok := item["tools"]; ok && containsNativeWebSearchTool(nested) {
			return true
		}
	}
	return false
}

func buildRouteRuntimes(cfg ProviderConfig, id, baseURL, profileVersion, apiKey, keySalt string) map[string]ProviderRuntime {
	if len(cfg.Routes) == 0 {
		return nil
	}
	out := make(map[string]ProviderRuntime, len(cfg.Routes))
	for _, route := range cfg.Routes {
		key := strings.ToLower(strings.TrimSpace(route.Key))
		if key == "" || route.Adapter == nil {
			continue
		}
		version := profileVersion + ":" + firstNonEmpty(route.ProfileID, key)
		if conversion := strings.TrimSpace(route.ConversionProfile); conversion != "" {
			version += ":" + conversion
		}
		routeBaseURL := firstNonEmpty(route.BaseURL, baseURL)
		routeAPIKey := firstNonEmpty(route.APIKey, apiKey)
		out[key] = ProviderRuntime{
			Adapter: route.Adapter, ProviderID: id, KeyFingerprint: KeyFingerprint(routeAPIKey, keySalt), BaseURLHash: BaseURLHash(routeBaseURL), ProfileVersion: version,
			CustomToolMode: strings.TrimSpace(route.CustomToolMode), UnsupportedToolPolicy: strings.TrimSpace(route.UnsupportedToolPolicy), ConversionProfile: strings.TrimSpace(route.ConversionProfile), StrictConversion: route.StrictConversion, Operation: strings.ToLower(strings.TrimSpace(route.Operation)), NativeTools: append([]NativeToolSpec(nil), route.NativeTools...), SourcePolicy: route.SourcePolicy, ResponsesPolicy: route.ResponsesPolicy,
		}
	}
	return out
}

func inferProviderBaseURL(profileID string, baseURL string, apiKey string) string {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL != "" {
		return baseURL
	}
	profile := strings.ToLower(strings.TrimSpace(profileID))
	switch {
	case strings.Contains(profile, "openai"):
		return "https://api.openai.com/v1"
	default:
		return ""
	}
}

func normalizeProviderID(id string) string {
	return strings.ToLower(strings.TrimSpace(id))
}

func normalizeModelID(model string) string {
	return strings.ToLower(strings.TrimSpace(model))
}
