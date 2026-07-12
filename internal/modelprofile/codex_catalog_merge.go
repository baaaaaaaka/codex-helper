package modelprofile

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// MergeCodexModelCatalogJSON appends third-party models to an official Codex
// catalog without changing any official model fields. Third-party priorities
// are moved behind every official model so merely configuring a provider does
// not replace Codex's official default model.
func MergeCodexModelCatalogJSON(official []byte, providers []ProviderSpec) ([]byte, error) {
	return mergeCodexModelCatalogJSON(official, providers, true)
}

// ThirdPartyCodexModelCatalogJSON builds a usable catalog from verified
// third-party providers only. It is the non-blocking fallback when the
// official Codex catalog is unavailable.
func ThirdPartyCodexModelCatalogJSON(providers []ProviderSpec) ([]byte, error) {
	return mergeCodexModelCatalogJSON([]byte(`{"models":[]}`), providers, false)
}

func mergeCodexModelCatalogJSON(official []byte, providers []ProviderSpec, requireOfficial bool) ([]byte, error) {
	var catalog struct {
		Models []map[string]any `json:"models"`
	}
	if err := json.Unmarshal(official, &catalog); err != nil {
		return nil, fmt.Errorf("decode official Codex model catalog: %w", err)
	}
	if requireOfficial && len(catalog.Models) == 0 {
		return nil, fmt.Errorf("official Codex model catalog contains no models")
	}
	seen := make(map[string]string, len(catalog.Models))
	maxPriority := 0
	for _, model := range catalog.Models {
		slug := strings.TrimSpace(stringValue(model["slug"]))
		if slug == "" {
			return nil, fmt.Errorf("official Codex model catalog contains a model without a slug")
		}
		key := strings.ToLower(slug)
		if previous, ok := seen[key]; ok {
			return nil, fmt.Errorf("official Codex model catalog repeats model slug %q (first %q)", slug, previous)
		}
		seen[key] = slug
		if priority := intValue(model["priority"]); priority > maxPriority {
			maxPriority = priority
		}
	}

	nextPriority := maxPriority + 1
	for _, provider := range providers {
		raw, err := CodexModelCatalogJSON(provider)
		if err != nil {
			return nil, err
		}
		var third struct {
			Models []map[string]any `json:"models"`
		}
		if err := json.Unmarshal(raw, &third); err != nil {
			return nil, fmt.Errorf("decode third-party Codex model catalog for %q: %w", provider.ID, err)
		}
		for _, model := range third.Models {
			slug := strings.TrimSpace(stringValue(model["slug"]))
			if slug == "" {
				return nil, fmt.Errorf("third-party provider %q contains a model without a slug", provider.ID)
			}
			key := strings.ToLower(slug)
			if previous, ok := seen[key]; ok {
				return nil, fmt.Errorf("third-party model slug %q conflicts with catalog model %q", slug, previous)
			}
			model["priority"] = nextPriority
			nextPriority++
			seen[key] = slug
			catalog.Models = append(catalog.Models, model)
		}
	}
	if len(catalog.Models) == 0 {
		return nil, fmt.Errorf("Codex model catalog contains no models")
	}
	return json.MarshalIndent(catalog, "", "  ")
}

func stringValue(value any) string {
	valueString, _ := value.(string)
	return valueString
}

func intValue(value any) int {
	switch number := value.(type) {
	case float64:
		if number > math.MaxInt || number < math.MinInt {
			return 0
		}
		return int(number)
	case int:
		return number
	default:
		return 0
	}
}
