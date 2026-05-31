package responsesadapter

import (
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
	ID           string
	ProfileID    string
	BaseURL      string
	APIKey       string
	DefaultModel string
	Models       []ModelInfo
	Adapter      ProviderAdapter
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
	id         string
	models     map[string]bool
	routes     map[string]string
	listModels []ModelInfo
	openModel  bool
	runtime    ProviderRuntime
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
		return r.resolveWithProvider(explicit, body.Model)
	}
	model := strings.TrimSpace(body.Model)
	if model == "" {
		return r.resolveWithProvider(r.defaultProvider, "")
	}
	matches := r.byModel[normalizeModelID(model)]
	switch len(matches) {
	case 0:
		defaultProvider := r.providers[r.byID[r.defaultProvider]]
		if defaultProvider.openModel {
			return runtimeForModel(defaultProvider, model), nil
		}
		return ProviderRuntime{}, routeErrorf(http.StatusBadRequest, "model %q is not configured for any provider", model)
	case 1:
		return runtimeForModel(r.providers[matches[0]], model), nil
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

func (r *ProviderRegistry) resolveWithProvider(providerID string, model string) (ProviderRuntime, error) {
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
	return runtimeForModel(provider, model), nil
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
		adapter = OpenAIChatAdapter{
			BaseURL: baseURL,
			APIKey:  cfg.APIKey,
			Profile: ProfileForProvider(profileID),
		}
	}
	return registeredProvider{
		id:         id,
		models:     models,
		routes:     routes,
		listModels: listModels,
		openModel:  len(models) == 0,
		runtime: ProviderRuntime{
			Adapter:        adapter,
			ProviderID:     id,
			PublicModel:    strings.TrimSpace(cfg.DefaultModel),
			Model:          strings.TrimSpace(cfg.DefaultModel),
			KeyFingerprint: KeyFingerprint(cfg.APIKey, keySalt),
			BaseURLHash:    BaseURLHash(baseURL),
			ProfileVersion: firstNonEmpty(profileID, id) + ":v1",
		},
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

func inferProviderBaseURL(profileID string, baseURL string, apiKey string) string {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL != "" {
		return baseURL
	}
	profile := strings.ToLower(strings.TrimSpace(profileID))
	switch {
	case strings.Contains(profile, "mimo"):
		if strings.HasPrefix(strings.TrimSpace(apiKey), "tp-") {
			return "https://token-plan-cn.xiaomimimo.com/v1"
		}
		return "https://api.xiaomimimo.com/v1"
	case strings.Contains(profile, "deepseek"):
		return "https://api.deepseek.com/v1"
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
