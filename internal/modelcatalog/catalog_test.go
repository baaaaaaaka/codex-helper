package modelcatalog

import (
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func validDocument() Document {
	return Document{
		CatalogVersion: CurrentVersion,
		Providers: map[string]Provider{
			"nvidia": {
				Interfaces: map[string]Interface{
					"chat": {Adapter: "openai-chat", BaseURL: "https://integrate.api.nvidia.com/v1", Auth: Auth{Type: "bearer"}},
				},
				Models: map[string]Model{
					"deepseek-v4": {UpstreamModel: "deepseek-ai/deepseek-v4", DisplayName: "DeepSeek V4", DefaultInterface: "chat"},
					"mimo-v2.5":   {UpstreamModel: "xiaomi/mimo-v2.5", DisplayName: "MiMo V2.5", DefaultInterface: "chat"},
				},
			},
		},
	}
}

func TestParseRejectsUnknownFieldsAndTrailingJSON(t *testing.T) {
	doc := validDocument()
	raw, err := Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(append(raw, []byte("{}")...)); err == nil {
		t.Fatal("expected trailing JSON to be rejected")
	}
	unknown := strings.Replace(string(raw), `"providers"`, `"unexpected": {}, "providers"`, 1)
	if _, err := Parse([]byte(unknown)); err == nil {
		t.Fatal("expected unknown field to be rejected")
	}
}

func TestParseRejectsCatalogV1WithoutCompatibilityFallback(t *testing.T) {
	raw := []byte(`{"catalogVersion":1,"providers":{"nvidia":{"interfaces":{"chat":{"adapter":"openai-chat","baseUrl":"https://example.invalid/v1"}},"models":{"m":{"upstreamModel":"m","defaultInterface":"chat"}}}}}`)
	if _, err := Parse(raw); err == nil || !strings.Contains(err.Error(), "unsupported catalogVersion") {
		t.Fatalf("expected catalog v1 rejection, got %v", err)
	}
}

func TestRoutesFlattenProviderAndModelWithStableSelector(t *testing.T) {
	doc := validDocument()
	routes := doc.Routes()
	if len(routes) != 2 {
		t.Fatalf("got %d routes, want 2", len(routes))
	}
	if routes[0].Selector != "nvidia/deepseek-v4" || routes[1].Selector != "nvidia/mimo-v2.5" {
		t.Fatalf("unexpected selectors: %#v", routes)
	}
	if routes[0].Provider.BaseURL != doc.Providers["nvidia"].Interfaces["chat"].BaseURL || routes[0].Model.UpstreamModel != "deepseek-ai/deepseek-v4" {
		t.Fatalf("provider/model mapping lost: %#v", routes[0])
	}
	if routes[0].Credential != "catalog-provider/nvidia/chat" || routes[0].InterfaceID != "chat" {
		t.Fatalf("unexpected credential scope %q", routes[0].Credential)
	}
}

func TestValidateRejectsCredentialBearingProviderAndAliasCollision(t *testing.T) {
	doc := validDocument()
	doc.Providers["nvidia"] = Provider{Interfaces: map[string]Interface{"chat": {Adapter: "openai-chat", BaseURL: "https://user:secret@example.test/v1"}}, Models: map[string]Model{"a": {UpstreamModel: "a", DefaultInterface: "chat"}}}
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "credentials") {
		t.Fatalf("expected URL credential error, got %v", err)
	}
	doc = validDocument()
	doc.Providers["nvidia"].Models["mimo-v2.5"] = Model{UpstreamModel: "mimo", Aliases: []string{"deepseek-v4"}}
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("expected alias collision, got %v", err)
	}
}

func TestValidateRejectsAuthorizationHeaderAuth(t *testing.T) {
	doc := validDocument()
	provider := doc.Providers["nvidia"]
	iface := provider.Interfaces["chat"]
	iface.Auth = Auth{Type: "header", Header: "Authorization"}
	provider.Interfaces["chat"] = iface
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "Authorization") {
		t.Fatalf("expected authorization header rejection, got %v", err)
	}
	provider = doc.Providers["nvidia"]
	iface = provider.Interfaces["chat"]
	iface.Auth = Auth{}
	iface.Headers = map[string]string{"Authorization": "Bearer leaked"}
	provider.Interfaces["chat"] = iface
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "headers") {
		t.Fatalf("expected authorization header map rejection, got %v", err)
	}
}

func TestValidateRejectsHeaderInjectionInCatalogInterface(t *testing.T) {
	doc := validDocument()
	provider := doc.Providers["nvidia"]
	iface := provider.Interfaces["chat"]
	iface.Headers = map[string]string{"X-Test\r\nInjected": "value"}
	provider.Interfaces["chat"] = iface
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "invalid header name") {
		t.Fatalf("header name injection was accepted: %v", err)
	}
	iface.Headers = map[string]string{"X-Test": "value\nInjected: true"}
	provider.Interfaces["chat"] = iface
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "contains a newline") {
		t.Fatalf("header value injection was accepted: %v", err)
	}
}

func TestValidateRejectsConflictingOperationInterfaces(t *testing.T) {
	doc := validDocument()
	provider := doc.Providers["nvidia"]
	provider.Interfaces["beta"] = Interface{Adapter: "deepseek-beta", Protocol: "beta", BaseURL: "https://example.invalid/beta", Conversion: config.ModelConversion{Enabled: true, Profile: "deepseek-beta-v1"}}
	model := provider.Models["deepseek-v4"]
	model.Features = map[string]Feature{
		"chat":      {Support: "native", Interface: "chat", Operation: "chat"},
		"responses": {Support: "translated", Interface: "beta", Operation: "chat"},
	}
	provider.Models["deepseek-v4"] = model
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "conflicting interfaces") {
		t.Fatalf("conflicting operation routes accepted: %v", err)
	}
}

func TestValidateRejectsCaseFoldedProviderCollision(t *testing.T) {
	doc := validDocument()
	doc.Providers["NVIDIA"] = doc.Providers["nvidia"]
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "differ only by case") {
		t.Fatalf("case-folded provider collision was accepted: %v", err)
	}
}

func TestCatalogV2RoutesPerFeatureInterfaceAndFallback(t *testing.T) {
	doc := Document{
		CatalogVersion: CurrentVersion,
		Providers: map[string]Provider{
			"deepseek": {
				DefaultInterface: "openai",
				Interfaces: map[string]Interface{
					"openai":    {Adapter: "deepseek-openai", BaseURL: "https://api.deepseek.com"},
					"anthropic": {Adapter: "deepseek-anthropic", BaseURL: "https://api.deepseek.com/anthropic", Auth: Auth{Type: "header", Header: "x-api-key"}, Conversion: config.ModelConversion{Enabled: true, Profile: "deepseek-anthropic-v1"}},
					"beta":      {Adapter: "deepseek-beta", BaseURL: "https://api.deepseek.com/beta", Conversion: config.ModelConversion{Enabled: true, Profile: "deepseek-beta-v1"}},
				},
				Models: map[string]Model{
					"deepseek-v4-pro": {
						UpstreamModel: "deepseek-v4-pro", DefaultInterface: "openai",
						Features: map[string]Feature{
							"webSearch": {Support: "native", Interface: "anthropic", RequireSources: true, NativeTool: &config.ModelNativeTool{InputTypes: []string{"web_search_preview"}, UpstreamType: "web_search"}, Sources: &config.ModelSourcePolicy{Mode: "annotations", RequireURL: true}},
							"prefix":    {Support: "native", Interface: "beta", Operation: "prefix"},
							"fim":       {Support: "native", Interface: "beta", Operation: "fim"},
							"vision":    {Support: "unsupported"},
						},
					},
				},
			},
		},
	}
	if err := doc.Validate(); err != nil {
		t.Fatal(err)
	}
	routes := doc.Routes()
	if len(routes) != 1 {
		t.Fatalf("routes = %#v", routes)
	}
	route := routes[0]
	if route.Provider.DefaultInterface != "openai" || route.Provider.AdapterProfile != "deepseek-openai" {
		t.Fatalf("default route = %#v", route.Provider)
	}
	if route.InterfaceID != "openai" || route.Model.DefaultInterface != "openai" || route.AuthType != "" || route.AuthHeader != "" {
		t.Fatalf("ordinary route was replaced by the feature interface: %#v", route)
	}
	search := route.Model.Features["webSearch"]
	if search.Support != "native" || search.Interface != "anthropic" || !search.RequireSources {
		t.Fatalf("search feature = %#v", search)
	}
	if search.NativeTool == nil || search.NativeTool.UpstreamType != "web_search" || search.Sources == nil || search.Sources.Mode != "annotations" {
		t.Fatalf("search wire mapping = %#v", search)
	}
	if route.Model.Capabilities.NativeWebSearch == nil || !*route.Model.Capabilities.NativeWebSearch {
		t.Fatalf("native search capability was not projected: %#v", route.Model.Capabilities)
	}
	if got := route.Provider.Interfaces["anthropic"].Conversion.Profile; got != "deepseek-anthropic-v1" {
		t.Fatalf("anthropic conversion profile = %q", got)
	}
	if got := route.Model.Features["fim"].Operation; got != "fim" {
		t.Fatalf("fim operation = %q", got)
	}
}

func TestCatalogV2RoutesNativeOperationInterfaceWhenNoSearchTool(t *testing.T) {
	doc := validDocument()
	provider := doc.Providers["nvidia"]
	provider.Interfaces["beta"] = Interface{Adapter: "deepseek-beta", BaseURL: "https://example.invalid/beta", Conversion: config.ModelConversion{Enabled: true, Profile: "deepseek-beta-v1"}}
	model := provider.Models["deepseek-v4"]
	model.DefaultInterface = "chat"
	model.Features = map[string]Feature{"fim": {Support: "native", Interface: "beta", Operation: "fim"}}
	provider.Models["deepseek-v4"] = model
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err != nil {
		t.Fatal(err)
	}
	routes := doc.Routes()
	var deepseek Route
	for _, route := range routes {
		if route.Selector == "nvidia/deepseek-v4" {
			deepseek = route
			break
		}
	}
	if deepseek.InterfaceID != "chat" || deepseek.Model.DefaultInterface != "chat" {
		t.Fatalf("ordinary route was replaced by the native operation interface: %#v", routes)
	}
}

func TestCatalogV2RejectsMalformedConversionAndOperation(t *testing.T) {
	doc := validDocument()
	provider := doc.Providers["nvidia"]
	iface := provider.Interfaces["chat"]
	iface.Conversion = config.ModelConversion{Enabled: true}
	provider.Interfaces["chat"] = iface
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "conversion.profile") {
		t.Fatalf("missing conversion profile accepted: %v", err)
	}
	doc = validDocument()
	provider = doc.Providers["nvidia"]
	model := provider.Models["deepseek-v4"]
	model.Features = map[string]Feature{"fim": {Support: "native", Interface: "chat", Operation: "chat"}}
	provider.Models["deepseek-v4"] = model
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "must use operation") {
		t.Fatalf("mismatched operation accepted: %v", err)
	}
}

func TestCatalogV2RejectsImpossibleSourceRequirements(t *testing.T) {
	for _, mode := range []string{"text", "unsupported"} {
		doc := validDocument()
		provider := doc.Providers["nvidia"]
		model := provider.Models["deepseek-v4"]
		model.Features = map[string]Feature{
			"webSearch": {
				Support:        "native",
				Interface:      "chat",
				RequireSources: true,
				Sources:        &config.ModelSourcePolicy{Mode: mode},
			},
		}
		provider.Models["deepseek-v4"] = model
		doc.Providers["nvidia"] = provider
		if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "cannot require") {
			t.Fatalf("mode %q accepted impossible source requirement: %v", mode, err)
		}
	}
}

func TestCatalogV2RejectsUnregisteredAdapterAndDelegatedWithoutFallback(t *testing.T) {
	doc := validDocument()
	iface := doc.Providers["nvidia"].Interfaces["chat"]
	iface.Adapter = "arbitrary-json"
	provider := doc.Providers["nvidia"]
	provider.Interfaces["chat"] = iface
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "unsupported adapter") {
		t.Fatalf("expected adapter validation error, got %v", err)
	}
	doc = validDocument()
	provider = doc.Providers["nvidia"]
	model := provider.Models["deepseek-v4"]
	model.Features = map[string]Feature{"webSearch": {Support: "plugin"}}
	provider.Models["deepseek-v4"] = model
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "fallback.selector") {
		t.Fatalf("expected plugin fallback validation error, got %v", err)
	}
}

func TestCatalogV2RejectsProtocolAdapterMismatch(t *testing.T) {
	doc := validDocument()
	provider := doc.Providers["nvidia"]
	iface := provider.Interfaces["chat"]
	iface.Protocol = "messages"
	provider.Interfaces["chat"] = iface
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("protocol/adapter mismatch was accepted: %v", err)
	}
}

func TestCatalogV2RejectsIgnoredFeatureFields(t *testing.T) {
	doc := validDocument()
	provider := doc.Providers["nvidia"]
	model := provider.Models["deepseek-v4"]
	model.Features = map[string]Feature{
		"vision": {Support: "native", Interface: "chat"},
	}
	provider.Models["deepseek-v4"] = model
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "not bound to an operation") {
		t.Fatalf("ignored feature interface was accepted: %v", err)
	}
}

func TestCatalogV2SeparatesNativeAndPluginSearchCapabilities(t *testing.T) {
	doc := validDocument()
	provider := doc.Providers["nvidia"]
	model := provider.Models["deepseek-v4"]
	model.Features = map[string]Feature{
		"webSearch": {Support: "plugin", Fallback: &Fallback{Selector: "default/gpt-5.6-luna"}},
	}
	provider.Models["deepseek-v4"] = model
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err != nil {
		t.Fatal(err)
	}
	route := doc.Routes()[0]
	if route.Model.Capabilities.NativeWebSearch == nil || *route.Model.Capabilities.NativeWebSearch {
		t.Fatalf("plugin search was projected as native: %#v", route.Model.Capabilities)
	}
}

func TestCatalogV2DoesNotProjectPluginToolsAsRuntimeSupport(t *testing.T) {
	doc := validDocument()
	provider := doc.Providers["nvidia"]
	model := provider.Models["deepseek-v4"]
	model.Features = map[string]Feature{"tools": {Support: "plugin"}}
	provider.Models["deepseek-v4"] = model
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err != nil {
		t.Fatal(err)
	}
	route := doc.Routes()[0]
	if route.Model.Capabilities.Tools == nil || *route.Model.Capabilities.Tools {
		t.Fatalf("plugin tools were projected as runtime support: %#v", route.Model.Capabilities)
	}
}

func TestCatalogV2RequiresRouteForTranslatedOperationFeature(t *testing.T) {
	doc := validDocument()
	provider := doc.Providers["nvidia"]
	model := provider.Models["deepseek-v4"]
	model.Features = map[string]Feature{"fim": {Support: "translated", Operation: "fim"}}
	provider.Models["deepseek-v4"] = model
	doc.Providers["nvidia"] = provider
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "translated feature") {
		t.Fatalf("translated operation without interface was accepted: %v", err)
	}
}
