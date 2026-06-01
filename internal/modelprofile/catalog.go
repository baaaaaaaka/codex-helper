package modelprofile

import (
	"fmt"
	"sort"
	"strings"
)

const DefaultProvider = "default"

type ProviderSpec struct {
	ID             string
	DisplayName    string
	DefaultModel   string
	Models         []ModelSpec
	BaseURL        string
	AdapterProfile string
	RecommendedEnv string
	UsesAdapter    bool
	SupportsTools  bool
	SupportsVision bool
	SupportsReason bool
}

type ModelSpec struct {
	ID               string
	UpstreamID       string
	DisplayName      string
	Description      string
	Aliases          []string
	ContextWindow    int
	MaxContextWindow int
	SupportsTools    bool
	SupportsVision   bool
	SupportsReason   bool
	SupportsSearch   bool
	Priority         int
}

func (m ModelSpec) PublicID() string {
	return strings.TrimSpace(firstNonEmpty(m.ID, m.UpstreamID))
}

func (m ModelSpec) UpstreamModel() string {
	return strings.TrimSpace(firstNonEmpty(m.UpstreamID, m.ID))
}

func (m ModelSpec) Label() string {
	return strings.TrimSpace(firstNonEmpty(m.DisplayName, m.PublicID()))
}

func (p ProviderSpec) ModelCatalog() []ModelSpec {
	if len(p.Models) == 0 {
		model := strings.TrimSpace(p.DefaultModel)
		if model == "" {
			return nil
		}
		return []ModelSpec{{
			ID:               model,
			UpstreamID:       model,
			DisplayName:      model,
			Description:      p.DisplayName + " model",
			ContextWindow:    128000,
			MaxContextWindow: 128000,
			SupportsTools:    p.SupportsTools,
			SupportsVision:   p.SupportsVision,
			SupportsReason:   p.SupportsReason,
		}}
	}
	out := make([]ModelSpec, 0, len(p.Models))
	seen := map[string]bool{}
	for _, model := range p.Models {
		publicID := model.PublicID()
		upstreamID := model.UpstreamModel()
		if publicID == "" || upstreamID == "" {
			continue
		}
		if seen[strings.ToLower(publicID)] {
			continue
		}
		seen[strings.ToLower(publicID)] = true
		if model.ContextWindow <= 0 {
			model.ContextWindow = 128000
		}
		if model.MaxContextWindow <= 0 {
			model.MaxContextWindow = model.ContextWindow
		}
		if model.DisplayName == "" {
			model.DisplayName = publicID
		}
		if model.Description == "" {
			model.Description = p.DisplayName + " model"
		}
		out = append(out, model)
	}
	return out
}

func (p ProviderSpec) DefaultPublicModel() string {
	if model, ok := p.ResolveModel(p.DefaultModel); ok {
		return model.PublicID()
	}
	for _, model := range p.ModelCatalog() {
		if model.PublicID() != "" {
			return model.PublicID()
		}
	}
	return strings.TrimSpace(p.DefaultModel)
}

func (p ProviderSpec) ResolveModel(ref string) (ModelSpec, bool) {
	models := p.ModelCatalog()
	if len(models) == 0 {
		return ModelSpec{}, false
	}
	ref = strings.ToLower(strings.TrimSpace(ref))
	if ref == "" {
		ref = strings.ToLower(strings.TrimSpace(p.DefaultModel))
	}
	if ref == "" {
		return models[0], true
	}
	var matched *ModelSpec
	for i := range models {
		model := models[i]
		for _, alias := range modelAliases(p.ID, model) {
			if strings.ToLower(strings.TrimSpace(alias)) != ref {
				continue
			}
			if matched != nil && !strings.EqualFold(matched.PublicID(), model.PublicID()) {
				return ModelSpec{}, false
			}
			copy := model
			matched = &copy
		}
	}
	if matched != nil {
		return *matched, true
	}
	return ModelSpec{}, false
}

func (p ProviderSpec) MustResolveModel(ref string) (ModelSpec, error) {
	model, ok := p.ResolveModel(ref)
	if ok {
		return model, nil
	}
	choices := make([]string, 0, len(p.ModelCatalog()))
	for _, model := range p.ModelCatalog() {
		if id := model.PublicID(); id != "" {
			choices = append(choices, id)
		}
	}
	return ModelSpec{}, fmt.Errorf("unknown model %q for provider %q; available models: %s", strings.TrimSpace(ref), p.ID, strings.Join(choices, ", "))
}

func modelAliases(providerID string, model ModelSpec) []string {
	raw := []string{
		model.PublicID(),
		model.UpstreamModel(),
		model.DisplayName,
	}
	raw = append(raw, model.Aliases...)
	providerID = strings.TrimSpace(providerID)
	if providerID != "" {
		prefix := providerID + "/"
		raw = append(raw,
			strings.TrimPrefix(model.PublicID(), prefix),
			strings.TrimPrefix(model.UpstreamModel(), prefix),
		)
	}
	for _, value := range []string{model.PublicID(), model.UpstreamModel()} {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if idx := strings.LastIndex(value, "/"); idx >= 0 && idx+1 < len(value) {
			raw = append(raw, value[idx+1:])
			value = value[idx+1:]
		}
		if idx := strings.LastIndex(value, "-"); idx >= 0 && idx+1 < len(value) {
			raw = append(raw, value[idx+1:])
		}
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(raw))
	for _, alias := range raw {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		key := strings.ToLower(alias)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, alias)
	}
	return out
}

var providerCatalog = map[string]ProviderSpec{
	DefaultProvider: {
		ID:          DefaultProvider,
		DisplayName: "Codex official API",
	},
	"deepseek": {
		ID:             "deepseek",
		DisplayName:    "DeepSeek",
		DefaultModel:   "deepseek-v4-flash",
		BaseURL:        "https://api.deepseek.com/v1",
		AdapterProfile: "deepseek",
		RecommendedEnv: "DEEPSEEK_API_KEY",
		UsesAdapter:    true,
		SupportsTools:  true,
		SupportsReason: true,
		Models: []ModelSpec{{
			ID:               "deepseek/deepseek-v4-flash",
			UpstreamID:       "deepseek-v4-flash",
			DisplayName:      "DeepSeek V4 Flash",
			Description:      "Fast DeepSeek coding model routed by CXP.",
			Aliases:          []string{"flash", "v4-flash", "default", "fast"},
			ContextWindow:    128000,
			MaxContextWindow: 128000,
			SupportsTools:    true,
			SupportsReason:   true,
			Priority:         0,
		}, {
			ID:               "deepseek/deepseek-v4-pro",
			UpstreamID:       "deepseek-v4-pro",
			DisplayName:      "DeepSeek V4 Pro",
			Description:      "Higher-quality DeepSeek coding model routed by CXP.",
			Aliases:          []string{"pro", "v4-pro"},
			ContextWindow:    128000,
			MaxContextWindow: 128000,
			SupportsTools:    true,
			SupportsReason:   true,
			Priority:         1,
		}},
	},
	"mimo": {
		ID:             "mimo",
		DisplayName:    "MiMo",
		DefaultModel:   "mimo-v2.5",
		BaseURL:        "https://api.xiaomimimo.com/v1",
		AdapterProfile: "mimo",
		RecommendedEnv: "MIMO_API_KEY",
		UsesAdapter:    true,
		SupportsTools:  true,
		SupportsVision: true,
		Models: []ModelSpec{{
			ID:               "mimo/mimo-v2.5",
			UpstreamID:       "mimo-v2.5",
			DisplayName:      "MiMo 2.5",
			Description:      "MiMo 2.5 family model routed by CXP.",
			Aliases:          []string{"base", "standard", "normal", "default", "mimo25"},
			ContextWindow:    128000,
			MaxContextWindow: 128000,
			SupportsTools:    true,
			SupportsVision:   true,
			Priority:         0,
		}, {
			ID:               "mimo/mimo-v2.5-pro",
			UpstreamID:       "mimo-v2.5-pro",
			DisplayName:      "MiMo 2.5 Pro",
			Description:      "MiMo 2.5 Pro family model routed by CXP.",
			Aliases:          []string{"pro", "mimo25-pro"},
			ContextWindow:    128000,
			MaxContextWindow: 128000,
			SupportsTools:    true,
			SupportsVision:   true,
			Priority:         1,
		}},
	},
	"kimi": {
		ID:             "kimi",
		DisplayName:    "Kimi",
		DefaultModel:   "kimi-k2",
		BaseURL:        "https://api.moonshot.cn/v1",
		AdapterProfile: "openai-chat",
		RecommendedEnv: "KIMI_API_KEY",
		UsesAdapter:    true,
		SupportsTools:  true,
	},
	"glm": {
		ID:             "glm",
		DisplayName:    "GLM",
		DefaultModel:   "glm-4.5",
		BaseURL:        "https://open.bigmodel.cn/api/paas/v4",
		AdapterProfile: "openai-chat",
		RecommendedEnv: "GLM_API_KEY",
		UsesAdapter:    true,
		SupportsTools:  true,
	},
	"minimax": {
		ID:             "minimax",
		DisplayName:    "MiniMax",
		DefaultModel:   "abab6.5s-chat",
		BaseURL:        "https://api.minimax.chat/v1",
		AdapterProfile: "openai-chat",
		RecommendedEnv: "MINIMAX_API_KEY",
		UsesAdapter:    true,
		SupportsTools:  true,
	},
	"qwen": {
		ID:             "qwen",
		DisplayName:    "Qwen",
		DefaultModel:   "qwen-plus",
		BaseURL:        "https://dashscope.aliyuncs.com/compatible-mode/v1",
		AdapterProfile: "openai-chat",
		RecommendedEnv: "QWEN_API_KEY",
		UsesAdapter:    true,
		SupportsTools:  true,
		SupportsVision: true,
	},
}

func NormalizeProvider(id string) string {
	return strings.ToLower(strings.TrimSpace(id))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func LookupProvider(id string) (ProviderSpec, bool) {
	id = NormalizeProvider(id)
	if id == "" {
		id = DefaultProvider
	}
	spec, ok := providerCatalog[id]
	return spec, ok
}

func MustLookupProvider(id string) (ProviderSpec, error) {
	spec, ok := LookupProvider(id)
	if !ok {
		return ProviderSpec{}, fmt.Errorf("unknown model provider %q", strings.TrimSpace(id))
	}
	return spec, nil
}

func ProviderIDs() []string {
	ids := make([]string, 0, len(providerCatalog))
	for id := range providerCatalog {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
