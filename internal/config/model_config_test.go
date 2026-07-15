package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStoreStrictlyRejectsUnknownStructuredModelField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	raw := `{"version":5,"profiles":[],"modelConfigVersion":1,"modelCredentials":{"ih":{"apiKeyRef":"env:KEY"}},"modelProviders":{"ih":{"protocol":"chat-completions","baseUrl":"https://example.invalid/v1","credential":"ih"}},"models":{"m":{"provider":"ih","upstreamModel":"m","capabilities":{"tolos":true}}}}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Load()
	if err == nil || !strings.Contains(err.Error(), `unknown field "tolos"`) {
		t.Fatalf("Load error = %v", err)
	}
}

func TestValidateStructuredModelReferencesAndContradictions(t *testing.T) {
	no, yes := false, true
	cfg := Config{ModelConfigVersion: 1,
		ModelCredentials: map[string]ModelCredential{"ih": {APIKeyRef: "env:KEY"}},
		ModelProviders:   map[string]ModelProvider{"ih": {Protocol: "chat-completions", BaseURL: "https://example.invalid/v1", Credential: "ih"}},
		Models:           map[string]ModelDefinition{"m": {Provider: "ih", UpstreamModel: "upstream", Capabilities: ModelCapabilities{Tools: &no, ParallelTools: &yes}}},
	}
	if err := ValidateModelConfig(cfg); err == nil || !strings.Contains(err.Error(), "parallelTools") {
		t.Fatalf("ValidateModelConfig error = %v", err)
	}
	cfg.Models["m"] = ModelDefinition{Provider: "missing", UpstreamModel: "upstream"}
	if err := ValidateModelConfig(cfg); err == nil || !strings.Contains(err.Error(), "missing provider") {
		t.Fatalf("ValidateModelConfig error = %v", err)
	}
}

func TestParseModelConfigFragmentRejectsCredentialRefsFromRepository(t *testing.T) {
	raw := []byte(`{"modelConfigVersion":1,"modelCredentials":{"hub":{"apiKeyRef":"env:SHOULD_NOT_BE_IN_REPO"}},"modelProviders":{"hub":{"protocol":"chat-completions","baseUrl":"https://example.invalid/v1","credential":"hub"}},"models":{"m":{"provider":"hub","upstreamModel":"m"}}}`)
	if _, err := ParseModelConfigFragment(raw); err == nil || !strings.Contains(err.Error(), "must not contain apiKeyRef") {
		t.Fatalf("ParseModelConfigFragment error = %v", err)
	}
}

func TestValidateModelHTTPPhaseTimeouts(t *testing.T) {
	policy := ModelHTTPPolicy{ResponseHeaderTimeoutSeconds: -1}
	if err := validateHTTPPolicy("model m", policy); err == nil {
		t.Fatal("negative response header timeout accepted")
	}
	if err := validateStreamPolicy("model m", ModelStreamPolicy{IdleTimeoutSeconds: -1}); err == nil {
		t.Fatal("negative stream idle timeout accepted")
	}
}

func TestValidateModelHTTPConcurrency(t *testing.T) {
	if err := validateHTTPPolicy("model m", ModelHTTPPolicy{MaxConcurrentRequests: -1}); err == nil {
		t.Fatal("negative maxConcurrentRequests accepted")
	}
}

func TestValidateModelProgressAndCapabilityPolicies(t *testing.T) {
	falseValue := false
	trueValue := true
	if err := validateStreamPolicy("model m", ModelStreamPolicy{SemanticProgressTimeoutSeconds: -1}); err == nil {
		t.Fatal("negative semantic timeout accepted")
	}
	if err := validateStreamPolicy("model m", ModelStreamPolicy{HeartbeatMode: "unknown"}); err == nil {
		t.Fatal("unknown heartbeat mode accepted")
	}
	if err := validateCapabilityMode("m", "responses.structuredOutput.jsonSchema", "unsupported"); err != nil {
		t.Fatal(err)
	}
	if err := validateCapabilityMode("m", "responses.structuredOutput.jsonSchema", "broken"); err == nil {
		t.Fatal("unknown structured output capability accepted")
	}
	if err := validateModelDefinition("m", ModelDefinition{Provider: "p", UpstreamModel: "m", Capabilities: ModelCapabilities{NativeWebSearch: &falseValue}, Search: ModelSearchPolicy{Native: &trueValue}}); err == nil {
		t.Fatal("conflicting native web-search declarations accepted")
	}
	if err := validateModelDefinition("m", ModelDefinition{Provider: "p", UpstreamModel: "m", Tools: ModelToolPolicy{ParallelEnforcement: "strict"}, Responses: ModelResponsesPolicy{StructuredOutput: ModelStructuredOutputPolicy{JSONSchema: "native"}}}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateGlobalDefaultsRejectsMalformedOrDanglingValues(t *testing.T) {
	cfg := Config{Defaults: &GlobalDefaults{Model: "work"}}
	if err := ValidateModelConfig(cfg); err == nil || !strings.Contains(err.Error(), "defaults.model") {
		t.Fatalf("unqualified defaults.model error = %v", err)
	}
	cfg.Defaults.Model = "profile:missing"
	if err := ValidateModelConfig(cfg); err == nil || !strings.Contains(err.Error(), "missing profile") {
		t.Fatalf("dangling defaults.model error = %v", err)
	}
	cfg.Defaults.Model = "official:gpt-test"
	cfg.Defaults.ReasoningEffort = "high\ninvalid"
	if err := ValidateModelConfig(cfg); err == nil || !strings.Contains(err.Error(), "reasoningEffort") {
		t.Fatalf("invalid defaults.reasoningEffort error = %v", err)
	}
	cfg.Defaults.Model = " official:gpt-test"
	cfg.Defaults.ReasoningEffort = ""
	if err := ValidateModelConfig(cfg); err == nil || !strings.Contains(err.Error(), "surrounding whitespace") {
		t.Fatalf("whitespace defaults.model error = %v", err)
	}
	cfg.Defaults.Model = "official:gpt-test"
	cfg.Defaults.ReasoningEffort = " high"
	if err := ValidateModelConfig(cfg); err == nil || !strings.Contains(err.Error(), "reasoningEffort") {
		t.Fatalf("whitespace defaults.reasoningEffort error = %v", err)
	}
	cfg.Defaults = &GlobalDefaults{Model: "profile:work", ReasoningEffort: "high"}
	cfg.ModelProfiles = map[string]ModelProfile{"work": {Provider: "synthetic", Revision: 1}}
	if err := ValidateModelConfig(cfg); err != nil {
		t.Fatalf("valid global defaults: %v", err)
	}
}

func TestValidateModelCatalogAndProviderBinding(t *testing.T) {
	cfg := Config{
		ModelConfigVersion: 1,
		ModelCatalogs: map[string]ModelCatalog{
			"nvidia": {Type: ModelCatalogTypeGit, URL: "https://github.com/example/catalog.git", File: "models.json"},
		},
		ModelProviderBindings: map[string]ModelProviderBinding{
			"nvidia": {Catalog: "NVIDIA", SecretRef: "secret:model-credential/catalog-provider/nvidia/default/api-key", InterfaceSecrets: map[string]string{"anthropic": "secret:deepseek/anthropic"}},
		},
	}
	if err := ValidateModelConfig(cfg); err != nil {
		t.Fatalf("valid catalog config rejected: %v", err)
	}
	cfg.ModelProviderBindings["nvidia"] = ModelProviderBinding{Catalog: "missing"}
	if err := ValidateModelConfig(cfg); err == nil || !strings.Contains(err.Error(), "missing catalog") {
		t.Fatalf("missing catalog binding accepted: %v", err)
	}
}

func TestStoreStrictlyRejectsUnknownCatalogField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	raw := `{"version":6,"profiles":[],"modelConfigVersion":1,"modelCatalogs":{"test":{"type":"managed-json","file":"models.json","managedFile":"model-catalogs/test.json","unexpected":true}}}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); err == nil || !strings.Contains(err.Error(), `unknown field "unexpected"`) {
		t.Fatalf("Load error = %v", err)
	}
}

func TestValidateModelCatalogRejectsPathTraversalOnlyAtSegmentBoundary(t *testing.T) {
	valid := ModelCatalog{Type: ModelCatalogTypeGit, URL: "https://example.invalid/catalog.git", File: "foo..bar/models.json"}
	if err := valid.Validate("catalog"); err != nil {
		t.Fatalf("filename containing dots was rejected: %v", err)
	}
	traversal := valid
	traversal.File = "../models.json"
	if err := traversal.Validate("catalog"); err == nil {
		t.Fatal("path traversal was accepted")
	}
	scp := valid
	scp.URL = "git@github.example:team/catalog.git"
	if err := scp.Validate("catalog"); err != nil {
		t.Fatalf("scp-style Git URL rejected: %v", err)
	}
}

func TestCatalogModelAliasesAreProviderScoped(t *testing.T) {
	cfg := Config{
		ModelConfigVersion: 1,
		ModelProviders: map[string]ModelProvider{
			"nvidia": {Protocol: "chat-completions", BaseURL: "https://nvidia.example/v1"},
			"hub":    {Protocol: "chat-completions", BaseURL: "https://hub.example/v1"},
		},
		Models: map[string]ModelDefinition{
			"nvidia/deepseek-v4": {Provider: "nvidia", UpstreamModel: "deepseek-ai/deepseek-v4", Aliases: []string{"deepseek-v4"}},
			"hub/deepseek-v4":    {Provider: "hub", UpstreamModel: "deepseek-ai/deepseek-v4", Aliases: []string{"deepseek-v4"}},
		},
	}
	if err := ValidateModelConfig(cfg); err != nil {
		t.Fatalf("same alias across providers was rejected: %v", err)
	}
	if name, _, ok := FindModelDefinition(cfg, "nvidia/deepseek-v4"); !ok || name != "nvidia/deepseek-v4" {
		t.Fatalf("exact provider/model selector did not resolve: %q %v", name, ok)
	}
	if _, _, ok := FindModelDefinition(cfg, "deepseek-v4"); ok {
		t.Fatal("ambiguous short alias resolved without a provider")
	}
}

func TestValidateModelConversionAndOperation(t *testing.T) {
	strict := true
	cfg := Config{
		ModelConfigVersion: 1,
		ModelCredentials:   map[string]ModelCredential{"key": {APIKeyRef: "env:KEY"}},
		ModelProviders: map[string]ModelProvider{"deepseek": {
			Protocol: "chat-completions", BaseURL: "https://example.invalid",
			Interfaces: map[string]ModelInterface{"anthropic": {
				Adapter: "deepseek-anthropic", BaseURL: "https://example.invalid/anthropic",
				Conversion: ModelConversion{Enabled: true, Profile: "deepseek-anthropic-v1", Strict: &strict},
			}},
		}},
		Models: map[string]ModelDefinition{"m": {Provider: "deepseek", UpstreamModel: "m", DefaultInterface: "anthropic", Features: map[string]ModelFeature{
			"fim": {Support: "native", Interface: "anthropic", Operation: "fim"},
		}}},
	}
	if err := ValidateModelConfig(cfg); err != nil {
		t.Fatalf("valid conversion config rejected: %v", err)
	}
	cfg.ModelProviders["deepseek"].Interfaces["anthropic"] = ModelInterface{Adapter: "deepseek-anthropic", BaseURL: "https://example.invalid/anthropic", Conversion: ModelConversion{Enabled: true}}
	if err := ValidateModelConfig(cfg); err == nil || !strings.Contains(err.Error(), "conversion.profile") {
		t.Fatalf("missing conversion profile accepted: %v", err)
	}
}

func TestValidateModelFeatureRoutesRejectsConflictingOperationInterfaces(t *testing.T) {
	cfg := Config{
		ModelConfigVersion: 1,
		ModelProviders: map[string]ModelProvider{
			"provider": {
				Protocol:         "chat-completions",
				BaseURL:          "https://example.invalid/v1",
				DefaultInterface: "chat",
				Interfaces: map[string]ModelInterface{
					"chat": {Adapter: "openai-chat", Protocol: "chat-completions", BaseURL: "https://example.invalid/v1"},
					"beta": {Adapter: "deepseek-beta", Protocol: "beta", BaseURL: "https://example.invalid/beta", Conversion: ModelConversion{Enabled: true, Profile: "deepseek-beta-v1"}},
				},
			},
		},
		Models: map[string]ModelDefinition{
			"model": {
				Provider:      "provider",
				UpstreamModel: "vendor/model",
				Features: map[string]ModelFeature{
					"chat":      {Support: "native", Interface: "chat", Operation: "chat"},
					"responses": {Support: "translated", Interface: "beta", Operation: "chat"},
				},
			},
		},
	}
	if err := ValidateModelConfig(cfg); err == nil || !strings.Contains(err.Error(), "conflicting interfaces") {
		t.Fatalf("conflicting operation routes accepted: %v", err)
	}
}
