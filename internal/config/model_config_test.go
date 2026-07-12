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
