package responsesadapter

import (
	"encoding/json"
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

func TestProviderRegistryRequiresExplicitExternalBaseURLAndFingerprints(t *testing.T) {
	registry := mustProviderRegistry(t, ProviderRegistryOptions{
		KeySalt: "test-salt",
		Providers: []ProviderConfig{
			{ID: "catalog-payg", ProfileID: "catalog-payg", BaseURL: "https://payg.example/v1", APIKey: "sk-payg", DefaultModel: "vendor/model-pro"},
			{ID: "catalog-token", ProfileID: "catalog-token", BaseURL: "https://token.example/v1", APIKey: "tp-token", DefaultModel: "vendor/model"},
		},
	})

	paygReq := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	paygReq.Header.Set("x-codex-provider", "catalog-payg")
	payg, err := registry.Resolve(paygReq, ResponsesRequest{})
	if err != nil {
		t.Fatalf("payg resolve: %v", err)
	}
	if payg.BaseURLHash != BaseURLHash("https://payg.example/v1") {
		t.Fatalf("payg base hash = %q", payg.BaseURLHash)
	}
	if payg.KeyFingerprint != KeyFingerprint("sk-payg", "test-salt") {
		t.Fatalf("payg key fingerprint = %q", payg.KeyFingerprint)
	}

	tokenReq := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	tokenReq.Header.Set("x-codex-provider", "catalog-token")
	token, err := registry.Resolve(tokenReq, ResponsesRequest{})
	if err != nil {
		t.Fatalf("token resolve: %v", err)
	}
	if token.BaseURLHash != BaseURLHash("https://token.example/v1") {
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

func TestProviderRegistryPinsConversionProfileInRuntime(t *testing.T) {
	registry := mustProviderRegistry(t, ProviderRegistryOptions{Providers: []ProviderConfig{{
		ID: "deepseek", ProfileID: "deepseek-anthropic", BaseURL: "https://example.invalid", APIKey: "test-key", DefaultModel: "deepseek-v4",
		ConversionProfile: "deepseek-anthropic-v1", StrictConversion: true,
	}}})
	runtime, err := registry.Resolve(httptest.NewRequest(http.MethodPost, "/v1/responses", nil), ResponsesRequest{Model: "deepseek-v4"})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.ConversionProfile != "deepseek-anthropic-v1" || !runtime.StrictConversion || !strings.Contains(runtime.ProfileVersion, "deepseek-anthropic-v1") {
		t.Fatalf("runtime conversion metadata = %#v", runtime)
	}
	if _, ok := runtime.Adapter.(AnthropicAdapter); !ok {
		t.Fatalf("adapter type = %T, want AnthropicAdapter", runtime.Adapter)
	}
}

func TestProviderRegistrySelectsOperationAndNativeSearchRoutes(t *testing.T) {
	chat := &recordingAdapter{}
	chatRoute := &recordingAdapter{}
	fim := &recordingAdapter{}
	search := &recordingAdapter{}
	registry := mustProviderRegistry(t, ProviderRegistryOptions{Providers: []ProviderConfig{{
		ID: "deepseek", ProfileID: "deepseek", BaseURL: "https://example.invalid", APIKey: "key", DefaultModel: "deepseek-v4", Adapter: chat,
		Routes: []ProviderRouteConfig{
			{Key: "chat", Adapter: chatRoute, ProfileID: "deepseek-chat"},
			{Key: "fim", Operation: "fim", Adapter: fim, ProfileID: "deepseek-beta", BaseURL: "https://beta.example/v1", APIKey: "beta-key"},
			{Key: "websearch", Adapter: search, ProfileID: "deepseek-anthropic", NativeTools: []NativeToolSpec{{InputTypes: []string{"web_search_preview"}, UpstreamType: "web_search"}}, SourcePolicy: SourcePolicy{Mode: "annotations"}},
		},
	}}})

	runtime, err := registry.Resolve(httptest.NewRequest(http.MethodPost, "/v1/responses", nil), ResponsesRequest{Model: "deepseek-v4", Operation: "fim"})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Adapter != fim || runtime.Operation != "fim" {
		t.Fatalf("FIM route = %#v, want beta adapter and fim operation", runtime)
	}
	if runtime.BaseURLHash != BaseURLHash("https://beta.example/v1") || runtime.KeyFingerprint != KeyFingerprint("beta-key", "") {
		t.Fatalf("FIM route identity = %#v, want route-specific URL/key scope", runtime)
	}
	runtime, err = registry.Resolve(httptest.NewRequest(http.MethodPost, "/v1/responses", nil), ResponsesRequest{Model: "deepseek-v4", Tools: json.RawMessage(`[{"type":"web_search_preview"}]`)})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Adapter != search || runtime.SourcePolicy.Mode != "annotations" {
		t.Fatalf("web-search route = %#v, want search adapter", runtime)
	}
	runtime, err = registry.Resolve(httptest.NewRequest(http.MethodPost, "/v1/responses", nil), ResponsesRequest{Model: "deepseek-v4"})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Adapter != chatRoute {
		t.Fatalf("default route = %#v, want explicit chat route adapter", runtime)
	}
}

func TestProviderRegistryDoesNotTreatFunctionNameOrSchemaAsNativeSearch(t *testing.T) {
	chat := &recordingAdapter{}
	search := &recordingAdapter{}
	registry := mustProviderRegistry(t, ProviderRegistryOptions{Providers: []ProviderConfig{{
		ID: "deepseek", ProfileID: "deepseek", BaseURL: "https://example.invalid", APIKey: "key", DefaultModel: "deepseek-v4", Adapter: chat,
		Routes: []ProviderRouteConfig{{Key: "websearch", Adapter: search, ProfileID: "deepseek-anthropic", NativeTools: []NativeToolSpec{{InputTypes: []string{"web_search_preview"}, UpstreamType: "web_search"}}}},
	}}})
	for _, raw := range []string{
		`[{"type":"function","name":"web_search","parameters":{"type":"object","properties":{"query":{"type":"web_search"}}}}]`,
		`[{"type":"function","function":{"name":"web_search"}}]`,
	} {
		runtime, err := registry.Resolve(httptest.NewRequest(http.MethodPost, "/v1/responses", nil), ResponsesRequest{Model: "deepseek-v4", Tools: json.RawMessage(raw)})
		if err != nil {
			t.Fatalf("Resolve(%s): %v", raw, err)
		}
		if runtime.Adapter != chat {
			t.Fatalf("function tool %s selected native search route: %#v", raw, runtime)
		}
	}
	runtime, err := registry.Resolve(httptest.NewRequest(http.MethodPost, "/v1/responses", nil), ResponsesRequest{Model: "deepseek-v4", Tools: json.RawMessage(`[{"type":"namespace","tools":[{"type":"web_search_preview"}]}]`)})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Adapter != search {
		t.Fatalf("nested native search tool selected wrong route: %#v", runtime)
	}
}

func TestProviderRegistryRejectsImpossibleSourcePolicy(t *testing.T) {
	for _, mode := range []string{"text", "unsupported"} {
		_, err := NewProviderRegistry(ProviderRegistryOptions{Providers: []ProviderConfig{{
			ID: "mimo", ProfileID: "mimo", BaseURL: "https://example.invalid", APIKey: "synthetic", DefaultModel: "mimo-v2.5",
			SourcePolicy: SourcePolicy{Mode: mode, RequireSources: true}, Adapter: fakeAdapter{},
		}}})
		if err == nil || !strings.Contains(err.Error(), "cannot require") {
			t.Fatalf("source mode %q accepted impossible policy: %v", mode, err)
		}
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
