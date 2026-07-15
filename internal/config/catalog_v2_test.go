package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseCatalogV2ScopesIdentityByProvider(t *testing.T) {
	root := t.TempDir()
	writeCatalogJSON(t, filepath.Join(root, "manifest.json"), CatalogManifest{
		SchemaVersion: CurrentCatalogSchemaVersion,
		Kind:          "catalog",
		Providers: []CatalogProviderRef{
			{ID: "official", File: "providers/official.json"},
			{ID: "hub", File: "providers/hub.json"},
		},
		Models: []CatalogModelRef{
			{Provider: "official", ID: "deepseek-v4-pro", File: "models/official-deepseek.json"},
			{Provider: "hub", ID: "deepseek-v4-pro", File: "models/hub-deepseek.json"},
		},
	})
	for _, provider := range []string{"official", "hub"} {
		writeCatalogJSON(t, filepath.Join(root, "providers", provider+".json"), CatalogProviderDocument{
			SchemaVersion: CurrentCatalogSchemaVersion,
			Kind:          "provider",
			ID:            provider,
			Interfaces: map[string]CatalogInterface{
				"chat": {Adapter: "openai-chat", Protocol: "chat-completions", BaseURL: "https://" + provider + ".example.invalid/v1", CredentialRef: provider + "-key", Auth: CatalogAuth{Type: "bearer"}},
			},
		})
	}
	for _, model := range []CatalogModelDocument{
		{SchemaVersion: 2, Kind: "model", Provider: "official", ID: "deepseek-v4-pro", UpstreamModel: "deepseek-v4-pro", Aliases: []string{"pro"}, Capabilities: CatalogCapabilities{Tools: "native", ParallelTools: "unsupported", Reasoning: "native", ReasoningSummary: "unknown", Vision: "unsupported", WebSearch: "unsupported"}, Reasoning: CatalogReasoningPolicy{Efforts: []string{"high"}, Default: "high"}, Routes: map[string]ModelRoute{"responses": {Interface: "chat", Adapter: "openai-chat", Protocol: "chat-completions"}}},
		{SchemaVersion: 2, Kind: "model", Provider: "hub", ID: "deepseek-v4-pro", UpstreamModel: "hub/deepseek-v4-pro", Aliases: []string{"pro"}, Capabilities: CatalogCapabilities{Tools: "native", ParallelTools: "unsupported", Reasoning: "native", ReasoningSummary: "unknown", Vision: "unsupported", WebSearch: "unsupported"}, Reasoning: CatalogReasoningPolicy{Efforts: []string{"xhigh"}, Default: "xhigh"}, Routes: map[string]ModelRoute{"responses": {Interface: "chat", Adapter: "openai-chat", Protocol: "chat-completions"}}},
	} {
		name := model.Provider + "-deepseek.json"
		writeCatalogJSON(t, filepath.Join(root, "models", name), model)
	}

	snapshot, err := ParseCatalogV2(root)
	if err != nil {
		t.Fatalf("ParseCatalogV2: %v", err)
	}
	if len(snapshot.Config.Models) != 2 || len(snapshot.Config.ModelProfiles) != 2 || len(snapshot.Config.ModelCredentials) != 2 {
		t.Fatalf("snapshot sizes: models=%d profiles=%d credentials=%d", len(snapshot.Config.Models), len(snapshot.Config.ModelProfiles), len(snapshot.Config.ModelCredentials))
	}
	if _, _, ok := FindModelDefinition(snapshot.Config, "deepseek-v4-pro"); ok {
		t.Fatal("ambiguous bare model selector resolved")
	}
	if _, _, ok := FindModelDefinition(snapshot.Config, "official/deepseek-v4-pro"); !ok {
		t.Fatal("qualified official selector did not resolve")
	}
	if _, _, ok := FindModelDefinition(snapshot.Config, "hub/deepseek-v4-pro"); !ok {
		t.Fatal("qualified hub selector did not resolve")
	}
	if got := snapshot.Config.Models["official/deepseek-v4-pro"].CapabilityModes.ParallelTools; got != "unsupported" {
		t.Fatalf("capability mode was not preserved: %q", got)
	}
}

func TestParseCatalogV2RejectsSecretsAndUnknownFields(t *testing.T) {
	root := t.TempDir()
	writeCatalogJSON(t, filepath.Join(root, "manifest.json"), CatalogManifest{SchemaVersion: 2, Kind: "catalog", Providers: []CatalogProviderRef{{ID: "p", File: "provider.json"}}, Models: []CatalogModelRef{{Provider: "p", ID: "m", File: "model.json"}}})
	writeCatalogJSON(t, filepath.Join(root, "provider.json"), CatalogProviderDocument{SchemaVersion: 2, Kind: "provider", ID: "p", Interfaces: map[string]CatalogInterface{"chat": {Adapter: "openai-chat", Protocol: "chat-completions", BaseURL: "https://example.invalid/v1", CredentialRef: "p", Headers: map[string]string{"Authorization": "Bearer should-not-be-here"}}}})
	writeCatalogJSON(t, filepath.Join(root, "model.json"), CatalogModelDocument{SchemaVersion: 2, Kind: "model", Provider: "p", ID: "m", UpstreamModel: "m"})
	if _, err := ParseCatalogV2(root); err == nil || !strings.Contains(err.Error(), "credential header") {
		t.Fatalf("secret header error = %v", err)
	}

	// Replace the provider with a structurally invalid document. Strict decoding
	// must reject fields that a subscription loader would otherwise ignore.
	writeRawCatalogJSON(t, filepath.Join(root, "provider.json"), []byte(`{"schemaVersion":2,"kind":"provider","id":"p","unexpected":true,"interfaces":{"chat":{"adapter":"openai-chat","protocol":"chat-completions","baseUrl":"https://example.invalid/v1"}}}`))
	if _, err := ParseCatalogV2(root); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown provider field error = %v", err)
	}
}

func TestParseCatalogV2RejectsUnsafePathsAndDuplicateFiles(t *testing.T) {
	root := t.TempDir()
	writeCatalogJSON(t, filepath.Join(root, "manifest.json"), CatalogManifest{SchemaVersion: 2, Kind: "catalog", Providers: []CatalogProviderRef{{ID: "p", File: "../provider.json"}}, Models: []CatalogModelRef{{Provider: "p", ID: "m", File: "model.json"}}})
	if _, err := ParseCatalogV2(root); err == nil || !strings.Contains(err.Error(), "repository-relative") {
		t.Fatalf("unsafe path error = %v", err)
	}
}

func TestParseCatalogV2RequiresExplicitCapabilitiesAndSupportedRoute(t *testing.T) {
	root := t.TempDir()
	writeCatalogJSON(t, filepath.Join(root, "manifest.json"), CatalogManifest{SchemaVersion: 2, Kind: "catalog", Providers: []CatalogProviderRef{{ID: "p", File: "provider.json"}}, Models: []CatalogModelRef{{Provider: "p", ID: "m", File: "model.json"}}})
	writeCatalogJSON(t, filepath.Join(root, "provider.json"), CatalogProviderDocument{SchemaVersion: 2, Kind: "provider", ID: "p", Interfaces: map[string]CatalogInterface{"chat": {Adapter: "openai-chat", Protocol: "chat-completions", BaseURL: "https://example.invalid/v1", CredentialRef: "p", Auth: CatalogAuth{Type: "bearer"}}}})
	writeCatalogJSON(t, filepath.Join(root, "model.json"), CatalogModelDocument{SchemaVersion: 2, Kind: "model", Provider: "p", ID: "m", UpstreamModel: "m", Capabilities: CatalogCapabilities{Tools: "native", Reasoning: "native", Vision: "unsupported", WebSearch: "unsupported"}, Routes: map[string]ModelRoute{"responses": {Interface: "chat", Adapter: "openai-chat", Protocol: "chat-completions"}}})
	if _, err := ParseCatalogV2(root); err == nil || !strings.Contains(err.Error(), "parallelTools") {
		t.Fatalf("missing capability error = %v", err)
	}

	writeCatalogJSON(t, filepath.Join(root, "model.json"), CatalogModelDocument{SchemaVersion: 2, Kind: "model", Provider: "p", ID: "m", UpstreamModel: "m", Capabilities: CatalogCapabilities{Tools: "native", ParallelTools: "unknown", Reasoning: "native", ReasoningSummary: "unknown", Vision: "unsupported", WebSearch: "unsupported"}, Routes: map[string]ModelRoute{"anthropic": {Interface: "chat", Adapter: "deepseek-anthropic-v1", Protocol: "anthropic"}}})
	if _, err := ParseCatalogV2(root); err == nil || !strings.Contains(err.Error(), "operation-aware dispatcher") {
		t.Fatalf("unsupported operation error = %v", err)
	}
}

func TestParseCanonicalCatalogFixtures(t *testing.T) {
	roots := strings.TrimSpace(os.Getenv("CXP_CANONICAL_CATALOG_ROOTS"))
	if roots == "" {
		t.Skip("set CXP_CANONICAL_CATALOG_ROOTS to validate external subscription fixtures")
	}
	for _, root := range strings.Split(roots, string(os.PathListSeparator)) {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		if snapshot, err := ParseCatalogV2(root); err != nil {
			t.Errorf("ParseCatalogV2(%q): %v", root, err)
		} else if len(snapshot.Config.Models) == 0 || snapshot.Digest == "" {
			t.Errorf("ParseCatalogV2(%q) returned empty snapshot: %#v", root, snapshot)
		}
	}
}

func writeCatalogJSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	writeRawCatalogJSON(t, path, raw)
}

func writeRawCatalogJSON(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}
