package modelcatalog

import (
	"reflect"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func contractHTTPPolicy() config.ModelHTTPPolicy {
	maxRetries := 2
	honorRetryAfter := true
	transportRetry := true
	return config.ModelHTTPPolicy{
		TimeoutSeconds:               30,
		ResponseHeaderTimeoutSeconds: 7,
		MaxRetries:                   &maxRetries,
		RetryStatuses:                []int{408, 429, 500},
		HonorRetryAfter:              &honorRetryAfter,
		RetryTransportErrors:         &transportRetry,
		MaxConcurrentRequests:        3,
	}
}

func contractStreamPolicy() config.ModelStreamPolicy {
	synthesize := true
	return config.ModelStreamPolicy{
		UpstreamMode:           "stream",
		Format:                 "event-stream",
		IdleTimeoutSeconds:     11,
		SynthesizeResponsesSSE: &synthesize,
		ReasoningDeltaPath:     "choices.0.delta.reasoning_content",
		CachedTokensPath:       "usage.prompt_tokens_details.cached_tokens",
	}
}

func contractFullDocument() Document {
	strict := true
	providerHTTP := contractHTTPPolicy()
	providerStream := contractStreamPolicy()
	return Document{
		CatalogVersion: CurrentVersion,
		Providers: map[string]Provider{
			"deepseek": {
				DefaultInterface: "chat",
				Interfaces: map[string]Interface{
					"chat": {
						Adapter: "deepseek-openai", Protocol: "chat-completions", BaseURL: "https://example.invalid/v1",
						Auth: Auth{Type: "bearer"}, Headers: map[string]string{"X-Provider": "shared"},
						Endpoints: map[string]string{"chat": "/chat/completions"}, HTTP: providerHTTP, Stream: providerStream,
					},
					"anthropic": {
						Adapter: "deepseek-anthropic", Protocol: "messages", BaseURL: "https://example.invalid/anthropic",
						Conversion: config.ModelConversion{Enabled: true, Profile: "deepseek-anthropic-v1", Strict: &strict},
						Auth:       Auth{Type: "header", Header: "x-api-key"}, Headers: map[string]string{"X-Route": "anthropic"},
						Endpoints: map[string]string{"messages": "/messages"}, HTTP: providerHTTP, Stream: providerStream,
					},
					"beta": {
						Adapter: "deepseek-beta", Protocol: "beta", BaseURL: "https://example.invalid/beta",
						Conversion: config.ModelConversion{Enabled: true, Profile: "deepseek-beta-v1", Strict: &strict},
						Auth:       Auth{Type: "bearer"}, HTTP: providerHTTP, Stream: providerStream,
					},
				},
				Models: map[string]Model{
					"deepseek-v4": {
						UpstreamModel: "deepseek-ai/deepseek-v4", DisplayName: "DeepSeek V4", Aliases: []string{"default", "v4"},
						Description: "contract fixture", Priority: 4, DefaultInterface: "chat",
						Limits: config.ModelLimits{ContextWindow: 1000, MaxContextWindow: 2000, MaxOutputTokens: 500, EffectiveContextPercent: 80, Source: "provider", Enforcement: "truncate"},
						Reasoning: config.ModelReasoningPolicy{
							SupportedEfforts: []string{"low", "medium", "high"}, DefaultEffort: "medium", EffortMap: map[string]string{"high": "xhigh"},
							ThinkingMode: "always", EnabledRequest: map[string]any{"thinking": "enabled"}, DisabledRequest: map[string]any{"thinking": "disabled"},
							StripSamplingWhenEnabled: func() *bool { v := true; return &v }(), HistoryPolicy: "preserve", ResponseField: "reasoning_content",
						},
						Tools:     config.ModelToolPolicy{ToolChoice: "full", Parallel: "enabled", StrictSchema: "preserve", EmptyAssistantContent: "omit", InvalidArguments: "error", PlainTextToolCall: "reject", ToolCallIDMaxLength: 64, ToolNameMaxLength: 64, ValidateArguments: func() *bool { v := true; return &v }(), CustomToolMode: "function"},
						Messages:  config.ModelMessagePolicy{SystemRole: "system", DeveloperRole: "developer", MergeSystemMessages: func() *bool { v := true; return &v }(), Images: "allow", Audio: "allow", Video: "drop"},
						Sampling:  config.ModelSamplingPolicy{Temperature: "forward", TopP: "strip-when-reasoning"},
						Responses: config.ModelResponsesPolicy{PreviousResponseID: "delegated", Background: "unsupported", ContextManagement: "unsupported"},
						Stream:    providerStream, HTTP: providerHTTP,
						Cache: config.ModelCachePolicy{PromptCacheKey: "native", PreviousResponseID: "delegated", CacheControl: "native", UsageField: "usage.prompt_tokens_details.cached_tokens"},
						Features: map[string]Feature{
							"chat":      {Support: "native", Interface: "chat", Operation: "chat"},
							"streaming": {Support: "native"},
							// Reasoning is a policy on the ordinary chat route. A
							// different interface for the same operation would be
							// ambiguous and is rejected by the catalog contract.
							"reasoning":        {Support: "translated", Interface: "chat", Operation: "chat"},
							"tools":            {Support: "native"},
							"parallelTools":    {Support: "native"},
							"strictTools":      {Support: "native"},
							"structuredOutput": {Support: "native"},
							"logprobs":         {Support: "native"},
							"cacheUsage":       {Support: "native"},
							"vision":           {Support: "unsupported"},
							"audioInput":       {Support: "unsupported"},
							"videoInput":       {Support: "unsupported"},
							"webSearch": {
								Support: "native", Interface: "anthropic", Operation: "chat", RequireSources: true,
								NativeTool: &config.ModelNativeTool{InputTypes: []string{"web_search_preview"}, UpstreamType: "web_search", Name: "web_search", AllowedFields: []string{"query", "domains"}},
								Sources:    &config.ModelSourcePolicy{Mode: "annotations", RequireURL: true, RequireSources: true},
							},
							"prefix": {Support: "native", Interface: "beta", Operation: "prefix"},
							"fim":    {Support: "native", Interface: "beta", Operation: "fim"},
						},
					},
				},
			},
		},
	}
}

func TestCatalogContractRoundTripsEveryJSONSurface(t *testing.T) {
	original := contractFullDocument()
	raw, err := Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, parsed) {
		t.Fatalf("catalog round trip changed the contract:\noriginal=%#v\nparsed=%#v", original, parsed)
	}
	routes := parsed.Routes()
	if len(routes) != 1 {
		t.Fatalf("Routes() = %#v, want one model route", routes)
	}
	route := routes[0]
	if route.InterfaceID != "chat" || route.Model.DefaultInterface != "chat" || route.Provider.Interfaces["beta"].Conversion.Profile != "deepseek-beta-v1" {
		t.Fatalf("route conversion/interface projection = %#v", route)
	}
	if route.Model.Limits.MaxOutputTokens != 500 || route.Model.Reasoning.DefaultEffort != "medium" || route.Model.Cache.UsageField == "" {
		t.Fatalf("model policies were not projected: %#v", route.Model)
	}
	search := route.Model.Features["webSearch"]
	if search.NativeTool == nil || search.Sources == nil || search.Sources.RequireURL != true || search.Operation != "chat" {
		t.Fatalf("web search contract was not projected: %#v", search)
	}
}

func TestCatalogContractRejectsPoliciesOutsideLocalConfigVocabulary(t *testing.T) {
	doc := contractFullDocument()
	model := doc.Providers["deepseek"].Models["deepseek-v4"]
	model.Tools.Parallel = "native"
	doc.Providers["deepseek"].Models["deepseek-v4"] = model
	if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), "invalid parallel") {
		t.Fatalf("catalog accepted a policy that runtime config cannot consume: %v", err)
	}
}

func TestCatalogContractEveryRegisteredAdapterHasAValidFixture(t *testing.T) {
	for adapter := range supportedAdapters {
		t.Run(adapter, func(t *testing.T) {
			protocol := protocolForAdapter(adapter)
			conversion := config.ModelConversion{}
			if adapter == "deepseek-anthropic" {
				conversion = config.ModelConversion{Enabled: true, Profile: "deepseek-anthropic-v1"}
			}
			if adapter == "deepseek-beta" {
				conversion = config.ModelConversion{Enabled: true, Profile: "deepseek-beta-v1"}
			}
			doc := Document{CatalogVersion: CurrentVersion, Providers: map[string]Provider{
				"provider": {
					DefaultInterface: "route",
					Interfaces:       map[string]Interface{"route": {Adapter: adapter, Protocol: protocol, BaseURL: "https://example.invalid/v1", Conversion: conversion}},
					Models:           map[string]Model{"model": {UpstreamModel: "upstream-model", DefaultInterface: "route"}},
				},
			}}
			if err := doc.Validate(); err != nil {
				t.Fatalf("adapter fixture rejected: %v", err)
			}
			routes := doc.Routes()
			if len(routes) != 1 || routes[0].Provider.AdapterProfile != adapter || routes[0].Provider.Interfaces["route"].Adapter != adapter {
				t.Fatalf("adapter fixture route = %#v", routes)
			}
		})
	}
}

func TestCatalogContractFeatureModesProjectOnlyExecutableCapabilities(t *testing.T) {
	for _, support := range []string{"native", "translated", "plugin", "unsupported"} {
		t.Run(support, func(t *testing.T) {
			doc := validDocument()
			provider := doc.Providers["nvidia"]
			model := provider.Models["deepseek-v4"]
			feature := Feature{Support: support}
			model.Features = map[string]Feature{"tools": feature}
			provider.Models["deepseek-v4"] = model
			doc.Providers["nvidia"] = provider
			if err := doc.Validate(); err != nil {
				t.Fatal(err)
			}
			capability := doc.Routes()[0].Model.Capabilities.Tools
			if capability == nil {
				t.Fatal("tools capability was omitted")
			}
			want := support == "native" || support == "translated"
			if *capability != want {
				t.Fatalf("tools capability = %v, want %v for support=%s", *capability, want, support)
			}
		})
	}
}

func TestCatalogContractRejectsNativeToolShapeMatrix(t *testing.T) {
	tests := []struct {
		name string
		tool config.ModelNativeTool
		want string
	}{
		{"missing input types", config.ModelNativeTool{UpstreamType: "web_search"}, "inputTypes"},
		{"missing upstream type", config.ModelNativeTool{InputTypes: []string{"web_search"}}, "upstreamType"},
		{"whitespace input type", config.ModelNativeTool{InputTypes: []string{"web search"}, UpstreamType: "web_search"}, "inputType"},
		{"duplicate input type", config.ModelNativeTool{InputTypes: []string{"web_search", "WEB_SEARCH"}, UpstreamType: "web_search"}, "inputType"},
		{"whitespace allowed field", config.ModelNativeTool{InputTypes: []string{"web_search"}, UpstreamType: "web_search", AllowedFields: []string{"query value"}}, "allowed field"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := validDocument()
			provider := doc.Providers["nvidia"]
			model := provider.Models["deepseek-v4"]
			model.Features = map[string]Feature{"webSearch": {Support: "native", Interface: "chat", NativeTool: &tt.tool}}
			provider.Models["deepseek-v4"] = model
			doc.Providers["nvidia"] = provider
			if err := doc.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tt.want)
			}
		})
	}
}
