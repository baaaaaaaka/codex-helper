package cli

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
)

func TestResponsesRegistryFromFileLoadsProvidersAndProxyKeys(t *testing.T) {
	t.Setenv("MIMO_TEST_KEY", "tp-mimo")
	t.Setenv("DEEPSEEK_TEST_KEY", "sk-ds")
	path := filepath.Join(t.TempDir(), "providers.json")
	if err := os.WriteFile(path, []byte(`{
		"default_provider":"mimo",
		"providers":[
			{"id":"mimo","profile":"mimo","api_key_env":"MIMO_TEST_KEY","default_model":"mimo-v2.5","models":["mimo-v2.5","mimo-v2.5-pro"]},
			{"id":"deepseek","profile":"deepseek","api_key_env":"DEEPSEEK_TEST_KEY","default_model":"deepseek-v4-flash","models":["deepseek-v4-flash"]}
		],
		"proxy_keys":{"mi-key":"mimo"}
	}`), 0o600); err != nil {
		t.Fatalf("write providers json: %v", err)
	}

	registry, err := responsesRegistryFromFile(path, "ds-key:deepseek,all-key:*", "salt")
	if err != nil {
		t.Fatalf("responsesRegistryFromFile: %v", err)
	}

	mimoReq := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	mimoReq.Header.Set("Authorization", "Bearer mi-key")
	runtime, err := registry.Resolve(mimoReq, responsesadapter.ResponsesRequest{Model: "mimo-v2.5-pro"})
	if err != nil {
		t.Fatalf("resolve mimo: %v", err)
	}
	if runtime.ProviderID != "mimo" || runtime.BaseURLHash != responsesadapter.BaseURLHash("https://token-plan-cn.xiaomimimo.com/v1") {
		t.Fatalf("mimo runtime = %#v", runtime)
	}
	if runtime.KeyFingerprint != responsesadapter.KeyFingerprint("tp-mimo", "salt") {
		t.Fatalf("mimo key fingerprint = %q", runtime.KeyFingerprint)
	}

	dsReq := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	dsReq.Header.Set("Authorization", "Bearer ds-key")
	runtime, err = registry.Resolve(dsReq, responsesadapter.ResponsesRequest{})
	if err != nil {
		t.Fatalf("resolve deepseek default: %v", err)
	}
	if runtime.ProviderID != "deepseek" || runtime.Model != "deepseek-v4-flash" {
		t.Fatalf("deepseek runtime = %#v", runtime)
	}
}

func TestParseResponsesProxyKeysRejectsMalformedEntries(t *testing.T) {
	if got, err := parseResponsesProxyKeys("a:mimo,b:*"); err != nil || got["a"] != "mimo" || got["b"] != "*" {
		t.Fatalf("parse valid = %#v err = %v", got, err)
	}
	if _, err := parseResponsesProxyKeys("missing-provider"); err == nil {
		t.Fatal("malformed proxy key entry should fail")
	}
}

func TestResponsesFacadeFromOptionsSetsScopeIdentity(t *testing.T) {
	store := responsesadapter.NewMemoryStore()
	facade, err := responsesFacadeFromOptions(responsesServeOptions{
		baseURL:   "https://api.deepseek.com/v1",
		provider:  "deepseek",
		model:     "deepseek-v4-flash",
		scopeSalt: "salt",
	}, "sk-test", store)
	if err != nil {
		t.Fatalf("responsesFacadeFromOptions: %v", err)
	}
	if facade.Store != store {
		t.Fatal("facade did not use provided store")
	}
	if facade.KeyFingerprint != responsesadapter.KeyFingerprint("sk-test", "salt") {
		t.Fatalf("key fingerprint = %q", facade.KeyFingerprint)
	}
	if facade.BaseURLHash != responsesadapter.BaseURLHash("https://api.deepseek.com/v1") {
		t.Fatalf("base URL hash = %q", facade.BaseURLHash)
	}
}
