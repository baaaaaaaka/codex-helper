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
	if decoded.Models[0].TruncationPolicy.Mode != "tokens" || decoded.Models[0].TruncationPolicy.Limit <= 0 {
		t.Fatalf("truncation policy = %#v", decoded.Models[0].TruncationPolicy)
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
	if len(models) != 1 || models[0].UpstreamModel() != "deepseek-v4-flash" {
		t.Fatalf("models = %#v", models)
	}
}
