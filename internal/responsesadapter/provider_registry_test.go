package responsesadapter

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProviderRegistryRoutesByModelAndProviderLock(t *testing.T) {
	registry := mustProviderRegistry(t, ProviderRegistryOptions{
		DefaultProvider: "mimo",
		ProxyKeys: map[string]string{
			"ds-key":  "deepseek",
			"mi-key":  "mimo",
			"all-key": "*",
		},
		Providers: []ProviderConfig{
			{ID: "mimo", ProfileID: "mimo", APIKey: "sk-mimo", DefaultModel: "mimo-v2.5", Models: []ModelInfo{{ID: "mimo-v2.5"}, {ID: "mimo-v2.5-pro"}}, Adapter: fakeAdapter{}},
			{ID: "deepseek", ProfileID: "deepseek", APIKey: "sk-ds", DefaultModel: "deepseek-v4-flash", Models: []ModelInfo{{ID: "deepseek-v4-flash"}}, Adapter: fakeAdapter{}},
		},
	})

	runtime, err := registry.Resolve(authorizedRequest("mi-key"), ResponsesRequest{Model: "mimo-v2.5-pro"})
	if err != nil {
		t.Fatalf("resolve mimo: %v", err)
	}
	if runtime.ProviderID != "mimo" || runtime.Model != "mimo-v2.5-pro" {
		t.Fatalf("runtime = %#v", runtime)
	}

	runtime, err = registry.Resolve(authorizedRequest("ds-key"), ResponsesRequest{})
	if err != nil {
		t.Fatalf("resolve locked default: %v", err)
	}
	if runtime.ProviderID != "deepseek" || runtime.Model != "deepseek-v4-flash" {
		t.Fatalf("locked default runtime = %#v", runtime)
	}

	_, err = registry.Resolve(authorizedRequest("ds-key"), ResponsesRequest{Model: "mimo-v2.5"})
	var routeErr RouteError
	if !errors.As(err, &routeErr) || routeErr.Status != http.StatusUnauthorized {
		t.Fatalf("locked provider mismatch err = %#v, want unauthorized route error", err)
	}
	if !strings.Contains(err.Error(), "locked to provider") {
		t.Fatalf("locked provider mismatch message = %v", err)
	}

	_, err = registry.Resolve(httptest.NewRequest(http.MethodPost, "/v1/responses", nil), ResponsesRequest{Model: "mimo-v2.5"})
	if !errors.As(err, &routeErr) || routeErr.Status != http.StatusUnauthorized {
		t.Fatalf("missing auth err = %#v, want unauthorized", err)
	}
}

func TestProviderRegistryRoutesPublicModelIDToUpstreamModel(t *testing.T) {
	registry := mustProviderRegistry(t, ProviderRegistryOptions{
		DefaultProvider: "mimo",
		ProxyKeys:       map[string]string{"mi-key": "mimo"},
		Providers: []ProviderConfig{{
			ID:           "mimo",
			ProfileID:    "mimo",
			APIKey:       "sk-mimo",
			DefaultModel: "mimo/mimo-v2.5",
			Models: []ModelInfo{{
				ID:         "mimo/mimo-v2.5",
				OwnedBy:    "mimo",
				UpstreamID: "mimo-v2.5",
			}, {
				ID:         "mimo/mimo-v2.5-pro",
				OwnedBy:    "mimo",
				UpstreamID: "mimo-v2.5-pro",
			}},
			Adapter: fakeAdapter{},
		}},
	})

	runtime, err := registry.Resolve(authorizedRequest("mi-key"), ResponsesRequest{Model: "mimo/mimo-v2.5-pro"})
	if err != nil {
		t.Fatalf("resolve namespaced model: %v", err)
	}
	if runtime.PublicModel != "mimo/mimo-v2.5-pro" || runtime.Model != "mimo-v2.5-pro" {
		t.Fatalf("runtime model mapping = public %q upstream %q", runtime.PublicModel, runtime.Model)
	}

	models := registry.Models()
	if len(models) != 2 || models[0].ID != "mimo/mimo-v2.5" || models[1].ID != "mimo/mimo-v2.5-pro" {
		t.Fatalf("listed models = %#v", models)
	}
}

func TestProviderRegistryRequiresExplicitProviderForAmbiguousModel(t *testing.T) {
	registry := mustProviderRegistry(t, ProviderRegistryOptions{
		DefaultProvider: "a",
		Providers: []ProviderConfig{
			{ID: "a", ProfileID: "generic", BaseURL: "https://a.example/v1", DefaultModel: "same-model", Models: []ModelInfo{{ID: "same-model"}}, Adapter: fakeAdapter{}},
			{ID: "b", ProfileID: "generic", BaseURL: "https://b.example/v1", DefaultModel: "same-model", Models: []ModelInfo{{ID: "same-model"}}, Adapter: fakeAdapter{}},
		},
	})

	_, err := registry.Resolve(httptest.NewRequest(http.MethodPost, "/v1/responses", nil), ResponsesRequest{Model: "same-model"})
	var routeErr RouteError
	if !errors.As(err, &routeErr) || routeErr.Status != http.StatusConflict {
		t.Fatalf("ambiguous model err = %#v, want conflict", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	req.Header.Set("x-codex-provider", "b")
	runtime, err := registry.Resolve(req, ResponsesRequest{Model: "same-model"})
	if err != nil {
		t.Fatalf("explicit provider resolve: %v", err)
	}
	if runtime.ProviderID != "b" {
		t.Fatalf("runtime provider = %q, want b", runtime.ProviderID)
	}
}

func TestProviderRegistryInfersMimoBaseURLAndFingerprints(t *testing.T) {
	registry := mustProviderRegistry(t, ProviderRegistryOptions{
		KeySalt: "test-salt",
		Providers: []ProviderConfig{
			{ID: "mimo-payg", ProfileID: "mimo", APIKey: "sk-payg", DefaultModel: "mimo-v2.5-pro"},
			{ID: "mimo-token", ProfileID: "mimo", APIKey: "tp-token", DefaultModel: "mimo-v2.5"},
		},
	})

	paygReq := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	paygReq.Header.Set("x-codex-provider", "mimo-payg")
	payg, err := registry.Resolve(paygReq, ResponsesRequest{})
	if err != nil {
		t.Fatalf("payg resolve: %v", err)
	}
	if payg.BaseURLHash != BaseURLHash("https://api.xiaomimimo.com/v1") {
		t.Fatalf("payg base hash = %q", payg.BaseURLHash)
	}
	if payg.KeyFingerprint != KeyFingerprint("sk-payg", "test-salt") {
		t.Fatalf("payg key fingerprint = %q", payg.KeyFingerprint)
	}

	tokenReq := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	tokenReq.Header.Set("x-codex-provider", "mimo-token")
	token, err := registry.Resolve(tokenReq, ResponsesRequest{})
	if err != nil {
		t.Fatalf("token resolve: %v", err)
	}
	if token.BaseURLHash != BaseURLHash("https://token-plan-cn.xiaomimimo.com/v1") {
		t.Fatalf("token base hash = %q", token.BaseURLHash)
	}
}

func TestProviderRegistryModelsIncludeOwners(t *testing.T) {
	registry := mustProviderRegistry(t, ProviderRegistryOptions{
		Providers: []ProviderConfig{
			{ID: "mimo", ProfileID: "mimo", APIKey: "sk", DefaultModel: "mimo-v2.5", Models: []ModelInfo{{ID: "mimo-v2.5-pro"}}, Adapter: fakeAdapter{}},
			{ID: "deepseek", ProfileID: "deepseek", APIKey: "sk", DefaultModel: "deepseek-v4-flash", Adapter: fakeAdapter{}},
		},
	})
	models := registry.Models()
	seen := map[string]string{}
	for _, model := range models {
		seen[model.ID] = model.OwnedBy
	}
	if seen["mimo-v2.5-pro"] != "mimo" || seen["mimo-v2.5"] != "mimo" || seen["deepseek-v4-flash"] != "deepseek" {
		t.Fatalf("models = %#v", models)
	}
}

func mustProviderRegistry(t *testing.T, opts ProviderRegistryOptions) *ProviderRegistry {
	t.Helper()
	registry, err := NewProviderRegistry(opts)
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	return registry
}

func authorizedRequest(key string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	return req
}
