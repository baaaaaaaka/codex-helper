package modelprofile

import (
	"encoding/json"
	"testing"
)

func TestCodexModelCatalogJSONUsesPublicModelIDs(t *testing.T) {
	spec, err := MustLookupProvider("mimo")
	if err != nil {
		t.Fatalf("lookup mimo: %v", err)
	}
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
		if model.ContextWindow != millionTokenContextWindow || model.MaxContextWindow != millionTokenContextWindow {
			t.Fatalf("%s context window = %d/%d, want %d/%d", model.Slug, model.ContextWindow, model.MaxContextWindow, millionTokenContextWindow, millionTokenContextWindow)
		}
	}
	if decoded.Models[0].TruncationPolicy.Mode != "tokens" || decoded.Models[0].TruncationPolicy.Limit <= 0 {
		t.Fatalf("truncation policy = %#v", decoded.Models[0].TruncationPolicy)
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
			spec, err := MustLookupProvider(tc.provider)
			if err != nil {
				t.Fatalf("lookup provider: %v", err)
			}
			for _, modelID := range tc.models {
				model, ok := spec.ResolveModel(modelID)
				if !ok {
					t.Fatalf("ResolveModel(%q) failed", modelID)
				}
				if model.ContextWindow != millionTokenContextWindow || model.MaxContextWindow != millionTokenContextWindow {
					t.Fatalf("%s context window = %d/%d, want %d/%d", modelID, model.ContextWindow, model.MaxContextWindow, millionTokenContextWindow, millionTokenContextWindow)
				}
			}
		})
	}
}

func TestProviderSpecDefaultPublicModel(t *testing.T) {
	spec, err := MustLookupProvider("deepseek")
	if err != nil {
		t.Fatalf("lookup deepseek: %v", err)
	}
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

func TestMiMoProviderSpecResolvesTierAliases(t *testing.T) {
	spec, err := MustLookupProvider("mimo")
	if err != nil {
		t.Fatalf("lookup mimo: %v", err)
	}
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
	for _, id := range []string{"default", "deepseek-v4-flash", "deepseek-v4-pro", "mimo-v2.5", "mimo-v2.5-pro"} {
		if _, ok := byID[id]; !ok {
			t.Fatalf("ModelChoices missing %q: %#v", id, choices)
		}
	}
	if len(choices) != 5 {
		t.Fatalf("ModelChoices len=%d choices=%#v, want only fully covered simple models", len(choices), choices)
	}
	if got := byID["deepseek-v4-flash"].CredentialScope; got != "deepseek" {
		t.Fatalf("deepseek credential scope = %q", got)
	}
	if got := byID["deepseek-v4-pro"].CredentialScope; got != "deepseek" {
		t.Fatalf("deepseek pro credential scope = %q", got)
	}
	if got := byID["mimo-v2.5"].CredentialScope; got != "mimo25" {
		t.Fatalf("mimo base credential scope = %q", got)
	}
	if got := byID["mimo-v2.5-pro"].CredentialScope; got != "mimo25" {
		t.Fatalf("mimo pro credential scope = %q", got)
	}
	if byID["mimo-v2.5"].RecommendedProfile != "mimo25" || byID["mimo-v2.5-pro"].RecommendedProfile != "mimo25-pro" {
		t.Fatalf("mimo recommended profiles: base=%q pro=%q", byID["mimo-v2.5"].RecommendedProfile, byID["mimo-v2.5-pro"].RecommendedProfile)
	}
}

func TestLookupModelChoiceRejectsAmbiguousTierAlias(t *testing.T) {
	if got, ok := LookupModelChoice("mimo25-pro"); !ok || got.ID != "mimo-v2.5-pro" {
		t.Fatalf("LookupModelChoice(mimo25-pro) = %#v ok=%v", got, ok)
	}
	if got, ok := LookupModelChoice("pro"); ok || got.ID != "" {
		t.Fatalf("LookupModelChoice(pro) = %#v ok=%v, want ambiguous miss", got, ok)
	}
}
