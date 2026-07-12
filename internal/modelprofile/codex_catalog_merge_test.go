package modelprofile

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestMergeCodexModelCatalogJSONPreservesOfficialModels(t *testing.T) {
	official := []byte(`{"models":[{"slug":"gpt-current","display_name":"GPT Current","priority":7,"supported_reasoning_levels":[{"effort":"low"},{"effort":"max"}],"unknown_future_field":{"kept":true}}]}`)
	provider := ProviderSpec{
		ID:             "third",
		DisplayName:    "Third",
		DefaultModel:   "third/model",
		UsesAdapter:    true,
		SupportsTools:  true,
		SupportsReason: true,
	}
	merged, err := MergeCodexModelCatalogJSON(official, []ProviderSpec{provider})
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Models []map[string]any `json:"models"`
	}
	var want struct {
		Models []map[string]any `json:"models"`
	}
	if err := json.Unmarshal(merged, &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(official, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Models[0], want.Models[0]) {
		t.Fatalf("official model changed\n got: %#v\nwant: %#v", got.Models[0], want.Models[0])
	}
	if len(got.Models) != 2 || got.Models[1]["slug"] != "third/model" {
		t.Fatalf("merged models = %#v", got.Models)
	}
	if priority := intValue(got.Models[1]["priority"]); priority != 8 {
		t.Fatalf("third-party priority = %d, want 8", priority)
	}
}

func TestMergeCodexModelCatalogJSONRejectsSlugCollision(t *testing.T) {
	official := []byte(`{"models":[{"slug":"third/model","priority":1}]}`)
	provider := ProviderSpec{ID: "third", DisplayName: "Third", DefaultModel: "third/model", UsesAdapter: true}
	_, err := MergeCodexModelCatalogJSON(official, []ProviderSpec{provider})
	if err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("MergeCodexModelCatalogJSON() error = %v, want conflict", err)
	}
}

func TestThirdPartyCodexModelCatalogJSONDoesNotRequireOfficialModels(t *testing.T) {
	provider := ProviderSpec{
		ID:           "third",
		DisplayName:  "Third",
		DefaultModel: "third/model",
		UsesAdapter:  true,
	}
	raw, err := ThirdPartyCodexModelCatalogJSON([]ProviderSpec{provider})
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Models []map[string]any `json:"models"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Models) != 1 || got.Models[0]["slug"] != "third/model" {
		t.Fatalf("models = %#v", got.Models)
	}
}
