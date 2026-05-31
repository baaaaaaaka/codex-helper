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
	for _, model := range p.ModelCatalog() {
		if model.PublicID() != "" {
			return model.PublicID()
		}
	}
	return strings.TrimSpace(p.DefaultModel)
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
			ContextWindow:    128000,
			MaxContextWindow: 128000,
			SupportsTools:    true,
			SupportsReason:   true,
			Priority:         0,
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
