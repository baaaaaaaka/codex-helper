package modelprofile

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

const externalMillionTokenContextWindow = 1000000

func testExternalFamilyProvider(id string) ProviderSpec {
	if id == "deepseek" {
		return ProviderSpec{
			ID: "deepseek", DisplayName: "DeepSeek (external catalog)", DefaultModel: "deepseek/deepseek-v4-flash",
			BaseURL: "https://catalog.example/deepseek/v1", AdapterProfile: "openai-chat", UsesAdapter: true,
			SupportsTools: true, SupportsReason: true,
			Models: []ModelSpec{{ID: "deepseek/deepseek-v4-flash", UpstreamID: "deepseek-v4-flash", DisplayName: "DeepSeek V4 Flash", Aliases: []string{"flash", "default"}, ContextWindow: externalMillionTokenContextWindow, MaxContextWindow: externalMillionTokenContextWindow, SupportsTools: true, SupportsReason: true, Priority: 0}, {ID: "deepseek/deepseek-v4-pro", UpstreamID: "deepseek-v4-pro", DisplayName: "DeepSeek V4 Pro", Aliases: []string{"pro"}, ContextWindow: externalMillionTokenContextWindow, MaxContextWindow: externalMillionTokenContextWindow, SupportsTools: true, SupportsReason: true, Priority: 1}},
		}
	}
	return ProviderSpec{
		ID: "mimo", DisplayName: "MiMo (external catalog)", DefaultModel: "mimo/mimo-v2.5",
		BaseURL: "https://catalog.example/mimo/v1", AdapterProfile: "openai-chat", UsesAdapter: true,
		SupportsTools: true, SupportsVision: true, SupportsReason: true,
		Models: []ModelSpec{{ID: "mimo/mimo-v2.5", UpstreamID: "mimo-v2.5", DisplayName: "MiMo 2.5", Aliases: []string{"base", "standard", "normal", "default", "mimo25"}, ContextWindow: externalMillionTokenContextWindow, MaxContextWindow: externalMillionTokenContextWindow, SupportsTools: true, SupportsVision: true, SupportsReason: true, Priority: 0}, {ID: "mimo/mimo-v2.5-pro", UpstreamID: "mimo-v2.5-pro", DisplayName: "MiMo 2.5 Pro", Aliases: []string{"pro", "mimo25-pro"}, ContextWindow: externalMillionTokenContextWindow, MaxContextWindow: externalMillionTokenContextWindow, SupportsTools: true, SupportsVision: true, SupportsReason: true, Priority: 1}},
	}
}

func TestCodexModelCatalogJSONUsesPublicModelIDs(t *testing.T) {
	spec := testExternalFamilyProvider("mimo")
	raw, err := CodexModelCatalogJSON(spec)
	if err != nil {
		t.Fatalf("CodexModelCatalogJSON: %v", err)
	}
	var decoded struct {
		Models []struct {
			Slug             string   `json:"slug"`
			DisplayName      string   `json:"display_name"`
			Priority         int      `json:"priority"`
			InputModalities  []string `json:"input_modalities"`
			ContextWindow    int      `json:"context_window"`
			MaxContextWindow int      `json:"max_context_window"`
			TruncationPolicy struct {
				Mode  string `json:"mode"`
				Limit int    `json:"limit"`
			} `json:"truncation_policy"`
			DefaultReasoningLevel    string                 `json:"default_reasoning_level"`
			SupportedReasoningLevels []codexReasoningPreset `json:"supported_reasoning_levels"`
			MultiAgentVersion        *string                `json:"multi_agent_version"`
			ToolMode                 string                 `json:"tool_mode"`
		} `json:"models"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal catalog: %v\n%s", err, raw)
	}
	if len(decoded.Models) != 2 {
		t.Fatalf("models len = %d, want 2; catalog=%s", len(decoded.Models), raw)
	}
	if decoded.Models[0].Slug != "mimo/mimo-v2.5" || decoded.Models[1].Slug != "mimo/mimo-v2.5-pro" {
		t.Fatalf("slugs = %#v", decoded.Models)
	}
	if decoded.Models[0].Priority >= decoded.Models[1].Priority {
		t.Fatalf("default model should sort before alternates in Codex model/list: priorities=%d,%d", decoded.Models[0].Priority, decoded.Models[1].Priority)
	}
	if decoded.Models[0].DisplayName == "" || decoded.Models[0].ContextWindow <= 0 {
		t.Fatalf("first model metadata incomplete: %#v", decoded.Models[0])
	}
	for _, model := range decoded.Models {
		if model.ContextWindow != externalMillionTokenContextWindow || model.MaxContextWindow != externalMillionTokenContextWindow {
			t.Fatalf("%s context window = %d/%d, want %d/%d", model.Slug, model.ContextWindow, model.MaxContextWindow, externalMillionTokenContextWindow, externalMillionTokenContextWindow)
		}
	}
	if decoded.Models[0].TruncationPolicy.Mode != "tokens" || decoded.Models[0].TruncationPolicy.Limit <= 0 {
		t.Fatalf("truncation policy = %#v", decoded.Models[0].TruncationPolicy)
	}
	if decoded.Models[0].DefaultReasoningLevel != "medium" {
		t.Fatalf("default reasoning level = %q, want medium", decoded.Models[0].DefaultReasoningLevel)
	}
	var efforts []string
	for _, option := range decoded.Models[0].SupportedReasoningLevels {
		efforts = append(efforts, option.Effort)
	}
	if got := strings.Join(efforts, ","); got != "low,medium,high,xhigh" {
		t.Fatalf("supported reasoning levels = %q", got)
	}
}

func TestCodexModelCatalogJSONOnlyAdvertisesMultiAgentForEnabledFallback(t *testing.T) {
	disabled := false
	provider := ProviderSpec{
		ID:                     "fallback",
		DisplayName:            "Fallback",
		DefaultModel:           "fallback/model",
		DisableHostedWebSearch: true,
		Models: []ModelSpec{{
			ID:            "fallback/model",
			UpstreamID:    "fallback/model",
			SupportsTools: true,
			SearchPolicy:  config.ModelSearchPolicy{Fallback: config.ModelSearchFallback{Enabled: &disabled}},
		}, {
			ID:            "fallback/enabled",
			UpstreamID:    "fallback/enabled",
			SupportsTools: true,
			SearchPolicy:  config.ModelSearchPolicy{Fallback: config.ModelSearchFallback{Model: "official/gpt-5.6-luna"}},
		}},
	}
	raw, err := CodexModelCatalogJSON(provider)
	if err != nil {
		t.Fatal(err)
	}
	var catalog codexCatalog
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	if got := catalog.Models[0].MultiAgentVersion; got != nil {
		t.Fatalf("disabled fallback advertised multi-agent version %#v", got)
	}
	if got := catalog.Models[1].MultiAgentVersion; got == nil || *got != "v1" {
		t.Fatalf("enabled fallback multi-agent version = %#v, want v1 for chat adapters", got)
	}
	if got := catalog.Models[1].ToolMode; got != "code_mode_only" {
		t.Fatalf("enabled fallback tool mode = %q, want code_mode_only", got)
	}
}

func TestCodexModelCatalogJSONUsesConfiguredReasoningEfforts(t *testing.T) {
	provider := ProviderSpec{
		ID:                        "configured",
		DisplayName:               "Configured",
		DefaultModel:              "configured/model",
		SupportsReason:            true,
		DefaultReasoningEffort:    "max",
		SupportedReasoningEfforts: []string{"low", "high", "max"},
		Models: []ModelSpec{{
			ID:             "configured/model",
			SupportsReason: true,
		}},
	}
	raw, err := CodexModelCatalogJSON(provider)
	if err != nil {
		t.Fatal(err)
	}
	var catalog codexCatalog
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	if len(catalog.Models) != 1 {
		t.Fatalf("models = %#v", catalog.Models)
	}
	model := catalog.Models[0]
	if model.DefaultReasoningLevel != "max" {
		t.Fatalf("default effort = %q, want max", model.DefaultReasoningLevel)
	}
	efforts := make([]string, 0, len(model.SupportedReasoningLevels))
	for _, effort := range model.SupportedReasoningLevels {
		efforts = append(efforts, effort.Effort)
	}
	if want := []string{"low", "high", "max"}; !reflect.DeepEqual(efforts, want) {
		t.Fatalf("efforts = %#v, want %#v", efforts, want)
	}
}

func TestThirdPartyMillionTokenProviderCatalogWindows(t *testing.T) {
	for _, tc := range []struct {
		provider string
		models   []string
	}{
		{
			provider: "deepseek",
			models: []string{
				"deepseek/deepseek-v4-flash",
				"deepseek/deepseek-v4-pro",
			},
		},
		{
			provider: "mimo",
			models: []string{
				"mimo/mimo-v2.5",
				"mimo/mimo-v2.5-pro",
			},
		},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			spec := testExternalFamilyProvider(tc.provider)
			for _, modelID := range tc.models {
				model, ok := spec.ResolveModel(modelID)
				if !ok {
					t.Fatalf("ResolveModel(%q) failed", modelID)
				}
				if model.ContextWindow != externalMillionTokenContextWindow || model.MaxContextWindow != externalMillionTokenContextWindow {
					t.Fatalf("%s context window = %d/%d, want %d/%d", modelID, model.ContextWindow, model.MaxContextWindow, externalMillionTokenContextWindow, externalMillionTokenContextWindow)
				}
			}
		})
	}
}

func TestProviderSpecDefaultPublicModel(t *testing.T) {
	spec := testExternalFamilyProvider("deepseek")
	if got := spec.DefaultPublicModel(); got != "deepseek/deepseek-v4-flash" {
		t.Fatalf("DefaultPublicModel = %q", got)
	}
	models := spec.ModelCatalog()
	if len(models) != 2 || models[0].UpstreamModel() != "deepseek-v4-flash" || models[1].UpstreamModel() != "deepseek-v4-pro" {
		t.Fatalf("models = %#v", models)
	}
	if got, ok := spec.ResolveModel("pro"); !ok || got.PublicID() != "deepseek/deepseek-v4-pro" {
		t.Fatalf("ResolveModel(pro) = %#v ok=%v", got, ok)
	}
	if got, ok := spec.ResolveModel("flash"); !ok || got.PublicID() != "deepseek/deepseek-v4-flash" {
		t.Fatalf("ResolveModel(flash) = %#v ok=%v", got, ok)
	}
}

func TestResponsesCompatibleProviderAcceptsConfiguredModel(t *testing.T) {
	spec, err := MustLookupProvider("responses-compatible")
	if err != nil {
		t.Fatalf("lookup responses-compatible: %v", err)
	}
	if !spec.DirectResponses || !spec.AllowsAnyModel || !spec.DisableHostedWebSearch || spec.BaseURL != "" || spec.RecommendedEnv != "RESPONSES_API_KEY" {
		t.Fatalf("unexpected Responses-compatible provider: %#v", spec)
	}
	const modelID = "Example/Reasoning-Model"
	model, ok := spec.ResolveModel(modelID)
	if !ok || model.PublicID() != modelID || model.UpstreamModel() != modelID {
		t.Fatalf("ResolveModel(%q) = %#v, %v", modelID, model, ok)
	}
	raw, err := CodexModelCatalogJSON(spec.WithSelectedModel(model))
	if err != nil {
		t.Fatalf("CodexModelCatalogJSON: %v", err)
	}
	if !strings.Contains(string(raw), `"slug": "`+modelID+`"`) {
		t.Fatalf("dynamic model missing from catalog:\n%s", raw)
	}
}

func TestMiMoProviderSpecResolvesTierAliases(t *testing.T) {
	spec := testExternalFamilyProvider("mimo")
	for _, tc := range []struct {
		ref  string
		want string
	}{
		{ref: "", want: "mimo/mimo-v2.5"},
		{ref: "base", want: "mimo/mimo-v2.5"},
		{ref: "standard", want: "mimo/mimo-v2.5"},
		{ref: "mimo25", want: "mimo/mimo-v2.5"},
		{ref: "mimo-v2.5", want: "mimo/mimo-v2.5"},
		{ref: "mimo/mimo-v2.5", want: "mimo/mimo-v2.5"},
		{ref: "pro", want: "mimo/mimo-v2.5-pro"},
		{ref: "mimo25-pro", want: "mimo/mimo-v2.5-pro"},
		{ref: "mimo-v2.5-pro", want: "mimo/mimo-v2.5-pro"},
		{ref: "mimo/mimo-v2.5-pro", want: "mimo/mimo-v2.5-pro"},
	} {
		got, ok := spec.ResolveModel(tc.ref)
		if !ok || got.PublicID() != tc.want {
			t.Fatalf("ResolveModel(%q) = %#v ok=%v, want %q", tc.ref, got, ok, tc.want)
		}
	}
}

func TestModelChoicesListUserFacingModelsAndCredentialScopes(t *testing.T) {
	choices := ModelChoices()
	byID := map[string]ModelChoice{}
	for _, choice := range choices {
		byID[choice.ID] = choice
	}
	for _, id := range []string{"default"} {
		if _, ok := byID[id]; !ok {
			t.Fatalf("ModelChoices missing %q: %#v", id, choices)
		}
	}
	if len(choices) != 1 {
		t.Fatalf("ModelChoices len=%d choices=%#v, want official only without external catalogs", len(choices), choices)
	}
	if _, ok := MustLookupProvider("deepseek"); ok == nil {
		t.Fatal("deepseek must not be a built-in provider")
	}
	if _, ok := MustLookupProvider("mimo"); ok == nil {
		t.Fatal("mimo must not be a built-in provider")
	}
}

func TestLookupModelChoiceRejectsAmbiguousTierAlias(t *testing.T) {
	if got, ok := LookupModelChoice("mimo25-pro"); ok || got.ID != "" {
		t.Fatalf("LookupModelChoice(mimo25-pro) = %#v ok=%v, want external catalog-only miss", got, ok)
	}
	if got, ok := LookupModelChoice("pro"); ok || got.ID != "" {
		t.Fatalf("LookupModelChoice(pro) = %#v ok=%v, want ambiguous miss", got, ok)
	}
}
