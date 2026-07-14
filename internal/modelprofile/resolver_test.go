package modelprofile

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestResolveDefaultModelProfile(t *testing.T) {
	got, err := Resolve(config.Config{Version: config.CurrentVersion}, "")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Name != config.DefaultModelProfileName || !got.IsDefault() || got.Provider.UsesAdapter {
		t.Fatalf("default resolved profile = %#v", got)
	}
}

func TestResolveTypedGlobalOfficialModelSelector(t *testing.T) {
	cfg := config.Config{
		Version:  config.CurrentVersion,
		Defaults: &config.GlobalDefaults{Model: "official:gpt-test-model"},
	}
	got, err := Resolve(cfg, "")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Name != "official:gpt-test-model" || !got.IsDefault() || got.SelectedPublicModel() != "gpt-test-model" {
		t.Fatalf("official default resolved profile = %#v", got)
	}
}

func TestResolveModelSelectorPrefixesDisambiguateProfileAndOfficial(t *testing.T) {
	cfg := config.Config{Version: config.CurrentVersion, ModelProfiles: map[string]config.ModelProfile{
		"gpt-collision": {Provider: "responses-compatible", Model: "vendor/model", BaseURL: "https://example.invalid/v1", APIKeyRef: "env:KEY", Revision: 1},
	}}
	official, err := Resolve(cfg, "gpt-collision")
	if err != nil {
		t.Fatal(err)
	}
	if !official.IsDefault() || official.SelectedPublicModel() != "gpt-collision" {
		t.Fatalf("unqualified gpt selector = %#v", official)
	}
	profile, err := Resolve(cfg, "profile:gpt-collision")
	if err != nil {
		t.Fatal(err)
	}
	if profile.IsDefault() || profile.Name != "gpt-collision" || profile.SelectedPublicModel() != "vendor/model" {
		t.Fatalf("qualified profile selector = %#v", profile)
	}
}

func TestResolveStructuredModelInheritsProviderCredentialAndPolicies(t *testing.T) {
	yes, no := true, false
	retries := 4
	cfg := config.Config{ModelConfigVersion: 1,
		ModelCredentials: map[string]config.ModelCredential{"ih": {APIKeyRef: "env:IH_KEY"}},
		ModelProviders:   map[string]config.ModelProvider{"ih": {Protocol: "chat-completions", BaseURL: "https://ih.example/v1", Credential: "ih", HTTP: config.ModelHTTPPolicy{RetryStatuses: []int{429, 529}}}},
		Models:           map[string]config.ModelDefinition{"glm": {Provider: "ih", UpstreamModel: "nvidia/glm", DisplayName: "GLM", Capabilities: config.ModelCapabilities{Tools: &yes, ParallelTools: &no, Reasoning: &yes}, Limits: config.ModelLimits{ContextWindow: 200000, MaxOutputTokens: 16384}, Reasoning: config.ModelReasoningPolicy{SupportedEfforts: []string{"high"}, DefaultEffort: "high"}, Tools: config.ModelToolPolicy{Parallel: "disabled"}, HTTP: config.ModelHTTPPolicy{MaxRetries: &retries}}},
		ModelProfiles:    map[string]config.ModelProfile{"work": {Model: "glm", Revision: 1}},
	}
	got, err := Resolve(cfg, "work")
	if err != nil {
		t.Fatal(err)
	}
	if got.Profile.APIKeyRef != "env:IH_KEY" || got.Provider.BaseURL != "https://ih.example/v1" || got.Model.UpstreamModel() != "nvidia/glm" {
		t.Fatalf("resolved = %#v", got)
	}
	if got.Model.ContextWindow != 200000 || got.Model.MaxOutputTokens != 16384 || got.Model.ToolPolicy.Parallel != "disabled" {
		t.Fatalf("model = %#v", got.Model)
	}
	if got.Model.HTTPPolicy.MaxRetries == nil || *got.Model.HTTPPolicy.MaxRetries != 4 || len(got.Model.HTTPPolicy.RetryStatuses) != 2 {
		t.Fatalf("http = %#v", got.Model.HTTPPolicy)
	}
	if fp := ModelFingerprint(got.Provider, got.Model.PublicID()); fp == "" {
		t.Fatal("empty model fingerprint")
	}
	snapshot := got.Snapshot(time.Now())
	if snapshot.DefaultReasoningEffort != "high" || snapshot.SupportedReasoningEffortsJSON != `["high"]` {
		t.Fatalf("structured reasoning snapshot = %#v", snapshot)
	}
}

func TestResolveCatalogV2UsesModelDefaultInterfaceAndFeatureRoute(t *testing.T) {
	yes := true
	cfg := config.Config{
		ModelConfigVersion: 1,
		ModelCredentials:   map[string]config.ModelCredential{"ds": {APIKeyRef: "env:DS_KEY"}},
		ModelProviders: map[string]config.ModelProvider{
			"deepseek": {
				Protocol: "chat-completions", BaseURL: "https://api.deepseek.com", Credential: "ds",
				DefaultInterface: "openai", AdapterProfile: "deepseek-openai",
				Interfaces: map[string]config.ModelInterface{
					"openai":    {Adapter: "deepseek-openai", Protocol: "chat-completions", BaseURL: "https://api.deepseek.com"},
					"anthropic": {Adapter: "deepseek-anthropic", Protocol: "messages", BaseURL: "https://api.deepseek.com/anthropic", Auth: config.ModelInterfaceAuth{Type: "header", Header: "x-api-key"}, Conversion: config.ModelConversion{Enabled: true, Profile: "deepseek-anthropic-v1"}},
				},
			},
		},
		Models: map[string]config.ModelDefinition{
			"deepseek/deepseek-v4-pro": {
				Provider: "deepseek", UpstreamModel: "deepseek-v4-pro", DefaultInterface: "openai",
				Capabilities: config.ModelCapabilities{Reasoning: &yes},
				Features: map[string]config.ModelFeature{
					"webSearch": {Support: "native", Interface: "anthropic", Fallback: &config.ModelFeatureFallback{Selector: "openai/gpt-5.6-luna", Effort: "high", Tier: "flex"}},
				},
			},
		},
		ModelProfiles: map[string]config.ModelProfile{"work": {Provider: "deepseek", Model: "deepseek/deepseek-v4-pro", Revision: 1}},
	}
	got, err := Resolve(cfg, "work")
	if err != nil {
		t.Fatal(err)
	}
	if got.Provider.BaseURL != "https://api.deepseek.com" || got.Provider.AdapterProfile != "deepseek-openai" || got.Model.DefaultInterface != "openai" {
		t.Fatalf("model default interface was not retained: %#v", got)
	}
	if !got.Provider.DisableHostedWebSearch || got.Provider.SearchFallback == nil || got.Provider.SearchFallback.Tier != "flex" {
		t.Fatalf("search fallback = %#v", got.Provider)
	}
	if feature := got.Model.Features["webSearch"]; feature.Interface != "anthropic" || feature.Support != "native" {
		t.Fatalf("feature route = %#v", feature)
	}
	if got.Provider.RouteInterfaces["chat"] != "openai" || got.Provider.RouteInterfaces["websearch"] != "anthropic" {
		t.Fatalf("feature routes = %#v", got.Provider.RouteInterfaces)
	}
	if got.Provider.ConversionProfile != "" || got.Model.ConversionProfile != "" || got.Provider.StrictConversion {
		t.Fatalf("ordinary route unexpectedly inherited feature conversion: provider=%#v model=%#v", got.Provider, got.Model)
	}
}

func TestResolveCatalogV2RetainsSharedProviderTransportFields(t *testing.T) {
	cfg := config.Config{
		ModelConfigVersion: 1,
		ModelCredentials:   map[string]config.ModelCredential{"key": {APIKeyRef: "env:MODEL_KEY"}},
		ModelProviders: map[string]config.ModelProvider{
			"vendor": {
				Protocol: "chat-completions", BaseURL: "https://vendor.example/v1", Credential: "key",
				Headers:          map[string]string{"X-Provider-Shared": "provider"},
				Endpoints:        map[string]string{"messages": "/provider-messages"},
				DefaultInterface: "chat",
				Interfaces: map[string]config.ModelInterface{
					"chat":   {Adapter: "openai-chat", Protocol: "chat-completions", BaseURL: "https://vendor.example/v1", Headers: map[string]string{"X-Interface": "chat"}},
					"search": {Adapter: "openai-chat", Protocol: "chat-completions", BaseURL: "https://vendor.example/search", Headers: map[string]string{"X-Interface": "search"}},
				},
			},
		},
		Models: map[string]config.ModelDefinition{
			"vendor/model": {Provider: "vendor", UpstreamModel: "vendor-model", DefaultInterface: "search", Features: map[string]config.ModelFeature{"webSearch": {Support: "native", Interface: "search"}}},
		},
		ModelProfiles: map[string]config.ModelProfile{"work": {Provider: "vendor", Model: "vendor/model", Revision: 1}},
	}
	resolved, err := Resolve(cfg, "work")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Provider.Headers["x-provider-shared"] != "provider" || resolved.Provider.Headers["x-interface"] != "search" {
		t.Fatalf("effective headers lost provider/interface fields: %#v", resolved.Provider.Headers)
	}
	if resolved.Provider.ProviderHeaders["X-Provider-Shared"] != "provider" || resolved.Provider.ProviderHeaders["X-Interface"] != "" {
		t.Fatalf("shared provider headers polluted by selected interface: %#v", resolved.Provider.ProviderHeaders)
	}
	if resolved.Provider.Endpoints["messages"] != "/provider-messages" {
		t.Fatalf("effective endpoints lost provider field: %#v", resolved.Provider.Endpoints)
	}
}

func TestResolveCatalogV2TurnsResponsesRouteIntoChatWhenNativeToolNeedsIt(t *testing.T) {
	yes := true
	cfg := config.Config{
		ModelConfigVersion: 1,
		ModelCredentials:   map[string]config.ModelCredential{"mimo": {APIKeyRef: "env:MIMO_KEY"}},
		ModelProviders: map[string]config.ModelProvider{"mimo": {
			Protocol: "responses", BaseURL: "https://api.example.invalid/v1", Credential: "mimo", DefaultInterface: "responses",
			Interfaces: map[string]config.ModelInterface{
				"responses": {Adapter: "mimo-responses", Protocol: "responses", BaseURL: "https://api.example.invalid/v1"},
			},
		}},
		Models: map[string]config.ModelDefinition{"mimo/mimo-v2.5": {
			Provider: "mimo", UpstreamModel: "mimo-v2.5", DefaultInterface: "responses", Capabilities: config.ModelCapabilities{NativeWebSearch: &yes},
			Features: map[string]config.ModelFeature{"webSearch": {Support: "native", Interface: "responses", NativeTool: &config.ModelNativeTool{InputTypes: []string{"web_search_preview"}, UpstreamType: "web_search"}}},
		}},
		ModelProfiles: map[string]config.ModelProfile{"work": {Provider: "mimo", Model: "mimo/mimo-v2.5", Revision: 1}},
	}
	got, err := Resolve(cfg, "work")
	if err != nil {
		t.Fatal(err)
	}
	if got.Provider.DirectResponses || got.Provider.AdapterProfile != "openai-chat" || len(got.Model.NativeTools) != 1 {
		t.Fatalf("native tool did not select chat adapter: provider=%#v model=%#v", got.Provider, got.Model)
	}
}

func TestResolveCatalogV2NativeSearchSelectsFeatureInterfaceCredential(t *testing.T) {
	yes := true
	cfg := config.Config{
		ModelConfigVersion: 1,
		ModelCredentials: map[string]config.ModelCredential{
			"openai":    {APIKeyRef: "env:DEEPSEEK_OPENAI"},
			"anthropic": {APIKeyRef: "env:DEEPSEEK_ANTHROPIC"},
		},
		ModelProviders: map[string]config.ModelProvider{
			"deepseek": {
				Protocol: "chat-completions", BaseURL: "https://api.deepseek.com", DefaultInterface: "openai",
				InterfaceCredentials: map[string]string{"openai": "openai", "anthropic": "anthropic"},
				Interfaces: map[string]config.ModelInterface{
					"openai":    {Adapter: "deepseek-openai", Protocol: "chat-completions", BaseURL: "https://api.deepseek.com"},
					"anthropic": {Adapter: "deepseek-anthropic", Protocol: "messages", BaseURL: "https://api.deepseek.com/anthropic", Auth: config.ModelInterfaceAuth{Type: "header", Header: "x-api-key"}, Conversion: config.ModelConversion{Enabled: true, Profile: "deepseek-anthropic-v1"}},
				},
			},
		},
		Models: map[string]config.ModelDefinition{
			"deepseek/deepseek-v4": {
				Provider: "deepseek", UpstreamModel: "deepseek-v4", DefaultInterface: "openai", Capabilities: config.ModelCapabilities{NativeWebSearch: &yes},
				Features: map[string]config.ModelFeature{"webSearch": {Support: "native", Interface: "anthropic", NativeTool: &config.ModelNativeTool{InputTypes: []string{"web_search_preview"}, UpstreamType: "web_search"}, Sources: &config.ModelSourcePolicy{Mode: "annotations", RequireURL: true, RequireSources: true}}},
			},
		},
		ModelProfiles: map[string]config.ModelProfile{"work": {Provider: "deepseek", Model: "deepseek/deepseek-v4", Revision: 1}},
	}
	resolved, err := Resolve(cfg, "work")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Provider.DefaultInterface != "openai" || resolved.Provider.AdapterProfile != "deepseek-openai" || resolved.Provider.BaseURL != "https://api.deepseek.com" {
		t.Fatalf("ordinary interface was not retained: %#v", resolved.Provider)
	}
	if resolved.Profile.APIKeyRef != "env:DEEPSEEK_OPENAI" || resolved.Provider.AuthType != "" || resolved.Provider.AuthHeader != "" {
		t.Fatalf("default interface credential/auth was not selected: profile=%#v provider=%#v", resolved.Profile, resolved.Provider)
	}
	if !resolved.Model.SourcePolicy.RequireSources {
		t.Fatalf("sources.requireSources was lost during route resolution: %#v", resolved.Model.SourcePolicy)
	}
	if resolved.Provider.RouteInterfaces["chat"] != "openai" || resolved.Provider.RouteInterfaces["websearch"] != "anthropic" {
		t.Fatalf("native search route map = %#v", resolved.Provider.RouteInterfaces)
	}
}

func TestResolveCatalogV2BindsFIMOperationToSelectedInterface(t *testing.T) {
	cfg := config.Config{
		ModelConfigVersion: 1,
		ModelCredentials:   map[string]config.ModelCredential{"beta": {APIKeyRef: "env:DEEPSEEK_BETA"}},
		ModelProviders: map[string]config.ModelProvider{
			"deepseek": {
				Protocol: "chat-completions", BaseURL: "https://api.deepseek.com", Credential: "beta", DefaultInterface: "beta",
				Interfaces: map[string]config.ModelInterface{
					"beta": {Adapter: "deepseek-beta", Protocol: "beta", BaseURL: "https://api.deepseek.com/beta", Conversion: config.ModelConversion{Enabled: true, Profile: "deepseek-beta-v1"}},
				},
			},
		},
		Models: map[string]config.ModelDefinition{
			"deepseek/deepseek-v4": {
				Provider: "deepseek", UpstreamModel: "deepseek-v4", DefaultInterface: "beta",
				Features: map[string]config.ModelFeature{"fim": {Support: "native", Interface: "beta", Operation: "fim"}},
			},
		},
		ModelProfiles: map[string]config.ModelProfile{"work": {Provider: "deepseek", Model: "deepseek/deepseek-v4", Revision: 1}},
	}
	resolved, err := Resolve(cfg, "work")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Model.Operation != "fim" || resolved.Provider.Operation != "fim" {
		t.Fatalf("FIM operation was not bound: model=%#v provider=%#v", resolved.Model, resolved.Provider)
	}
}

func TestResolveSnapshotUsesSelectedInterfaceCredential(t *testing.T) {
	cfg := config.Config{
		ModelConfigVersion: 1,
		ModelCredentials: map[string]config.ModelCredential{
			"deepseek-default":   {APIKeyRef: "env:DEEPSEEK_DEFAULT"},
			"deepseek-anthropic": {APIKeyRef: "env:DEEPSEEK_ANTHROPIC", AuthType: "header", Header: "x-api-key"},
		},
		ModelProviders: map[string]config.ModelProvider{
			"deepseek": {
				Protocol: "chat-completions", BaseURL: "https://api.deepseek.com", Credential: "deepseek-default",
				DefaultInterface: "openai", InterfaceCredentials: map[string]string{"openai": "deepseek-default", "anthropic": "deepseek-anthropic"},
				Interfaces: map[string]config.ModelInterface{
					"openai":    {Adapter: "deepseek-openai", Protocol: "chat-completions", BaseURL: "https://api.deepseek.com"},
					"anthropic": {Adapter: "deepseek-anthropic", Protocol: "messages", BaseURL: "https://api.deepseek.com/anthropic", Auth: config.ModelInterfaceAuth{Type: "header", Header: "x-api-key"}},
				},
			},
		},
		Models: map[string]config.ModelDefinition{
			"deepseek/deepseek-v4-pro": {Provider: "deepseek", UpstreamModel: "deepseek-v4-pro", DefaultInterface: "anthropic"},
		},
	}
	got, err := ResolveSnapshot(cfg, Snapshot{Name: "deepseek-work", Provider: "deepseek", Model: "deepseek/deepseek-v4-pro", APIKeyRef: "env:DEEPSEEK_ANTHROPIC", Revision: 1})
	if err != nil {
		t.Fatalf("ResolveSnapshot: %v", err)
	}
	if got.Provider.BaseURL != "https://api.deepseek.com/anthropic" || got.Provider.AuthHeader != "x-api-key" || got.Provider.AuthType != "header" {
		t.Fatalf("snapshot selected wrong interface auth: %#v", got.Provider)
	}
}

func TestResolveThirdPartyModelProfileWithSSHProxy(t *testing.T) {
	now := time.Now()
	cfg := structuredExternalFamilyConfig("deepseek", "deepseek/deepseek-v4-pro", "deepseek-work", "env:DEEPSEEK_API_KEY")
	cfg.Profiles = []config.Profile{{
		ID:        "ssh-1",
		Name:      "work",
		Host:      "host",
		Port:      22,
		User:      "user",
		CreatedAt: now,
	}}
	cfg.ModelProfiles["deepseek-work"] = config.ModelProfile{Provider: "deepseek", Model: "deepseek/deepseek-v4-pro", APIKeyRef: "env:DEEPSEEK_API_KEY", SSHProxy: "work", Revision: 3}

	got, err := Resolve(cfg, "DeepSeek-Work")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Name != "deepseek-work" || got.Provider.ID != "deepseek" || got.SelectedPublicModel() != "deepseek/deepseek-v4-pro" || !got.Provider.UsesAdapter || got.Revision() != 3 {
		t.Fatalf("resolved profile = %#v", got)
	}
	if got.SSHProfile == nil || got.SSHProfile.ID != "ssh-1" {
		t.Fatalf("SSHProfile=%#v", got.SSHProfile)
	}
}

func TestResolveResponsesCompatibleProfileUsesLocalConfiguration(t *testing.T) {
	cfg := config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"custom": {
				Provider:  "responses-compatible",
				Model:     "example/reasoning-model",
				BaseURL:   "https://responses.example.invalid/v1",
				APIKeyRef: "env:RESPONSES_API_KEY",
				Revision:  2,
			},
		},
	}
	resolved, err := Resolve(cfg, "custom")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.Provider.BaseURL != "https://responses.example.invalid/v1" || resolved.SelectedPublicModel() != "example/reasoning-model" || !resolved.Provider.DirectResponses || !resolved.Provider.DisableHostedWebSearch {
		t.Fatalf("resolved=%#v", resolved)
	}
	snapshot := resolved.Snapshot(time.Now())
	if snapshot.BaseURL != resolved.Provider.BaseURL || snapshot.BaseURLHash == "" {
		t.Fatalf("snapshot=%#v", snapshot)
	}
	resumed, err := ResolveSnapshot(cfg, snapshot)
	if err != nil {
		t.Fatalf("ResolveSnapshot: %v", err)
	}
	if resumed.Provider.BaseURL != resolved.Provider.BaseURL {
		t.Fatalf("resumed=%#v", resumed)
	}
}

func TestResolveChatCompatibleProfileAppliesExternalReasoningOverrides(t *testing.T) {
	cfg := config.Config{Version: config.CurrentVersion, ModelProfiles: map[string]config.ModelProfile{
		"external": {
			Provider: "chat-compatible", Model: "vendor/future-model",
			BaseURL: "https://chat.example.invalid/v1", APIKeyRef: "env:CHAT_API_KEY",
			DefaultReasoningEffort: "high", ReasoningEffortMap: map[string]string{"xhigh": "max"}, Revision: 1,
		},
	}}
	resolved, err := Resolve(cfg, "external")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.Provider.DirectResponses || resolved.SelectedPublicModel() != "vendor/future-model" || resolved.Provider.DefaultReasoningEffort != "high" || resolved.Provider.ReasoningEffortMap["xhigh"] != "max" {
		t.Fatalf("resolved=%#v", resolved)
	}
}

func TestResolveSnapshotPinsProfileFields(t *testing.T) {
	now := time.Now()
	cfg := structuredExternalFamilyConfig("mimo", "mimo/mimo-v2.5-pro", "mimo25", "")
	cfg.Profiles = []config.Profile{{
		ID:        "ssh-1",
		Name:      "jump",
		Host:      "host",
		Port:      22,
		User:      "user",
		CreatedAt: now,
	}}
	snapshot := Snapshot{
		Name:      "mimo25",
		Provider:  "mimo",
		APIKeyRef: "env:OLD_MIMO_KEY",
		Model:     "mimo/mimo-v2.5-pro",
		SSHProxy:  "jump",
		Revision:  3,
	}
	got, err := ResolveSnapshot(cfg, snapshot)
	if err != nil {
		t.Fatalf("ResolveSnapshot: %v", err)
	}
	if got.Name != "mimo25" || got.Profile.APIKeyRef != "env:OLD_MIMO_KEY" || got.SelectedPublicModel() != "mimo/mimo-v2.5-pro" || got.Revision() != 3 || got.Provider.ID != "mimo" {
		t.Fatalf("resolved snapshot = %#v", got)
	}
	if got.SSHProfile == nil || got.SSHProfile.Name != "jump" {
		t.Fatalf("SSHProfile = %#v", got.SSHProfile)
	}
}

func TestRuntimeSnapshotCapturesAndValidatesRuntimeIdentity(t *testing.T) {
	cfg := structuredExternalFamilyConfig("mimo", "mimo/mimo-v2.5-pro", "mimo25", "")
	cfg.Profiles = []config.Profile{{
		ID:   "ssh-1",
		Name: "jump",
		Host: "host",
		Port: 22,
		User: "user",
	}}
	cfg.ModelProfiles["mimo25"] = config.ModelProfile{Provider: "mimo", Model: "mimo/mimo-v2.5-pro", APIKeyRef: "env:MIMO_API_KEY", SSHProxy: "jump", Revision: 2}
	resolved, err := Resolve(cfg, "mimo25")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	snapshot, err := resolved.RuntimeSnapshot(time.Now(), nil, func(name string) string {
		if name != "MIMO_API_KEY" {
			t.Fatalf("env lookup name=%q", name)
		}
		return "sk-mimo-one"
	})
	if err != nil {
		t.Fatalf("RuntimeSnapshot: %v", err)
	}
	if snapshot.Model != "mimo/mimo-v2.5-pro" || snapshot.DefaultModel != "mimo/mimo-v2.5-pro" || snapshot.KeyFingerprint == "" || snapshot.BaseURLHash == "" || snapshot.ModelFingerprint == "" || snapshot.CatalogFingerprint == "" || snapshot.SSHProxyFingerprint == "" {
		t.Fatalf("runtime snapshot missing identity fields: %#v", snapshot)
	}
	if err := ValidateSnapshotRuntime(snapshot, resolved, "sk-mimo-one"); err != nil {
		t.Fatalf("ValidateSnapshotRuntime same key: %v", err)
	}
	if err := ValidateSnapshotRuntime(snapshot, resolved, "sk-mimo-two"); err == nil || !strings.Contains(err.Error(), "api key changed") {
		t.Fatalf("ValidateSnapshotRuntime changed key err=%v, want api key changed", err)
	}
	changedModel := snapshot
	changedModel.Model = "mimo/mimo-v2.5"
	if err := ValidateSnapshotRuntime(changedModel, resolved, "sk-mimo-one"); err == nil || !strings.Contains(err.Error(), "model changed") {
		t.Fatalf("ValidateSnapshotRuntime changed model err=%v, want model changed", err)
	}
	changedModelMapping := snapshot
	changedModelMapping.ModelFingerprint = "model:old"
	if err := ValidateSnapshotRuntime(changedModelMapping, resolved, "sk-mimo-one"); err == nil || !strings.Contains(err.Error(), "selected model mapping changed") {
		t.Fatalf("ValidateSnapshotRuntime changed model mapping err=%v, want selected model mapping changed", err)
	}
	additiveCatalogChange := snapshot
	additiveCatalogChange.ModelFingerprint = ""
	additiveCatalogChange.CatalogFingerprint = "catalog:old"
	if err := ValidateSnapshotRuntime(additiveCatalogChange, resolved, "sk-mimo-one"); err != nil {
		t.Fatalf("ValidateSnapshotRuntime old additive catalog fingerprint: %v", err)
	}
}

func TestRuntimeSnapshotRejectsLegacyContextWindowFingerprint(t *testing.T) {
	cfg := structuredExternalFamilyConfig("mimo", "mimo/mimo-v2.5-pro", "mimo25", "")
	cfg.ModelProfiles["mimo25"] = config.ModelProfile{Provider: "mimo", Model: "mimo/mimo-v2.5-pro", APIKeyRef: "env:MIMO_API_KEY", Revision: 1}
	resolved, err := Resolve(cfg, "mimo25")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	oldProvider := providerWithLegacy128KContext(resolved.Provider)
	snapshot := resolved.Snapshot(time.Now())
	snapshot.ModelFingerprint = legacyModelFingerprintV1(oldProvider, snapshot.Model)
	snapshot.CatalogFingerprint = CatalogFingerprint(oldProvider)
	if err := ValidateSnapshotRuntime(snapshot, resolved, "sk-test"); err == nil || !strings.Contains(err.Error(), "selected model mapping changed") {
		t.Fatalf("ValidateSnapshotRuntime legacy context fingerprint err=%v, want strict rejection", err)
	}
}

func TestRuntimeSnapshotRejectsRouteInterfaceChange(t *testing.T) {
	cfg := structuredExternalFamilyConfig("deepseek", "deepseek/deepseek-v4-pro", "deepseek-pro", "")
	cfg.ModelProfiles["deepseek-pro"] = config.ModelProfile{Provider: "deepseek", Model: "deepseek/deepseek-v4-pro", APIKeyRef: "env:DEEPSEEK_API_KEY", Revision: 1}
	resolved, err := Resolve(cfg, "deepseek-pro")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	resolved.Provider.RouteInterfaces = map[string]string{"chat": "openai", "fim": "beta"}
	snapshot := resolved.Snapshot(time.Now())
	changed := resolved
	changed.Provider = resolved.Provider
	changed.Provider.RouteInterfaces = map[string]string{"chat": "openai", "fim": "anthropic"}
	if err := ValidateSnapshotRuntime(snapshot, changed, "sk-test"); err == nil || !strings.Contains(err.Error(), "selected model mapping changed") {
		t.Fatalf("ValidateSnapshotRuntime route change err=%v, want selected model mapping changed", err)
	}
}

func TestRuntimeSnapshotRejectsLegacyFingerprintWhenUpstreamMappingChanges(t *testing.T) {
	cfg := structuredExternalFamilyConfig("deepseek", "deepseek/deepseek-v4-pro", "deepseek-pro", "")
	cfg.ModelProfiles["deepseek-pro"] = config.ModelProfile{Provider: "deepseek", Model: "deepseek/deepseek-v4-pro", APIKeyRef: "env:DEEPSEEK_API_KEY", Revision: 1}
	resolved, err := Resolve(cfg, "deepseek-pro")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	oldProvider := providerWithLegacy128KContext(resolved.Provider)
	snapshot := resolved.Snapshot(time.Now())
	snapshot.ModelFingerprint = legacyModelFingerprintV1(oldProvider, snapshot.Model)
	snapshot.CatalogFingerprint = CatalogFingerprint(oldProvider)

	changed := resolved
	changed.Provider.Models = append([]ModelSpec(nil), resolved.Provider.Models...)
	for i := range changed.Provider.Models {
		if changed.Provider.Models[i].PublicID() == snapshot.Model {
			changed.Provider.Models[i].UpstreamID = "deepseek-v4-pro-next"
		}
	}

	err = ValidateSnapshotRuntime(snapshot, changed, "sk-test")
	if err == nil || !strings.Contains(err.Error(), "selected model mapping changed") {
		t.Fatalf("ValidateSnapshotRuntime upstream change err=%v, want selected model mapping changed", err)
	}
}

func providerWithLegacy128KContext(provider ProviderSpec) ProviderSpec {
	provider.Models = append([]ModelSpec(nil), provider.Models...)
	for i := range provider.Models {
		provider.Models[i].ContextWindow = 128000
		provider.Models[i].MaxContextWindow = 128000
	}
	return provider
}

func TestResolveModelProfileErrors(t *testing.T) {
	cfg := config.Config{Version: config.CurrentVersion, ModelProfiles: map[string]config.ModelProfile{
		"missing-key": {Provider: "kimi", Model: "kimi-k2"},
		"missing-ssh": {Provider: "kimi", Model: "kimi-k2", APIKeyRef: "env:KIMI_API_KEY", SSHProxy: "missing"},
		"bad-model":   {Provider: "kimi", Model: "nope", APIKeyRef: "env:KIMI_API_KEY"},
		"unknown":     {Provider: "unknown", APIKeyRef: "env:KEY"},
	}}
	for _, tc := range []struct {
		name string
		want string
	}{
		{"missing", "not found"},
		{"missing-key", "requires an api key"},
		{"missing-ssh", "missing ssh proxy"},
		{"bad-model", "available models"},
		{"unknown", "unknown model provider"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Resolve(cfg, tc.name)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Resolve error=%v want containing %q", err, tc.want)
			}
		})
	}
}

func structuredExternalFamilyConfig(provider, model, profileName, apiKeyRef string) config.Config {
	upstream := strings.TrimPrefix(model, provider+"/")
	return config.Config{
		Version:            config.CurrentVersion,
		ModelConfigVersion: 1,
		ModelProviders:     map[string]config.ModelProvider{provider: {Protocol: "chat-completions", BaseURL: "https://catalog.example/" + provider + "/v1"}},
		Models:             map[string]config.ModelDefinition{model: {Provider: provider, UpstreamModel: upstream, DisplayName: model, Capabilities: config.ModelCapabilities{Tools: boolPtr(true), Reasoning: boolPtr(true)}, Limits: config.ModelLimits{ContextWindow: 1000000, MaxContextWindow: 1000000}}},
		ModelProfiles:      map[string]config.ModelProfile{profileName: {Provider: provider, Model: model, APIKeyRef: apiKeyRef, Revision: 1}},
	}
}

func boolPtr(value bool) *bool { return &value }

func TestSecretStoreAndAPIKeyRefs(t *testing.T) {
	store := NewSecretStore(filepath.Join(t.TempDir(), "secrets.json"))
	ref := SecretRefForProfile("deepseek-work")
	if err := store.Put(ref, "sk-test"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	value, err := ResolveAPIKey(ref, store, nil)
	if err != nil {
		t.Fatalf("ResolveAPIKey secret: %v", err)
	}
	if value != "sk-test" {
		t.Fatalf("secret value=%q", value)
	}
	value, err = ResolveAPIKey("env:MODEL_KEY", nil, func(name string) string {
		if name != "MODEL_KEY" {
			t.Fatalf("env lookup name=%q", name)
		}
		return "env-key"
	})
	if err != nil || value != "env-key" {
		t.Fatalf("ResolveAPIKey env value=%q err=%v", value, err)
	}
	if strings.Contains(MaskRef(ref), "deepseek-work") {
		t.Fatalf("MaskRef leaked secret key path: %q", MaskRef(ref))
	}
	if Fingerprint("sk-test") == "" || Fingerprint("sk-test") == Fingerprint("different") {
		t.Fatalf("Fingerprint not stable enough")
	}
}

func TestMergeHTTPPolicyIncludesPhaseTimeoutsAndExplicitZeroRetries(t *testing.T) {
	zero := 0
	base := config.ModelHTTPPolicy{TimeoutSeconds: 90, ResponseHeaderTimeoutSeconds: 120}
	override := config.ModelHTTPPolicy{ResponseHeaderTimeoutSeconds: 45, MaxRetries: &zero}
	got := mergeHTTPPolicy(base, override)
	if got.TimeoutSeconds != 90 || got.ResponseHeaderTimeoutSeconds != 45 {
		t.Fatalf("merged timeouts = %#v", got)
	}
	if got.MaxRetries == nil || *got.MaxRetries != 0 {
		t.Fatalf("MaxRetries = %#v, want explicit zero", got.MaxRetries)
	}
}

func TestMergeHTTPPolicyOverridesModelConcurrency(t *testing.T) {
	got := mergeHTTPPolicy(config.ModelHTTPPolicy{MaxConcurrentRequests: 4}, config.ModelHTTPPolicy{MaxConcurrentRequests: 1})
	if got.MaxConcurrentRequests != 1 {
		t.Fatalf("maxConcurrentRequests = %d", got.MaxConcurrentRequests)
	}
}

func TestMergeStreamPolicyIncludesIdleTimeout(t *testing.T) {
	got := mergeStreamPolicy(config.ModelStreamPolicy{IdleTimeoutSeconds: 300}, config.ModelStreamPolicy{IdleTimeoutSeconds: 60})
	if got.IdleTimeoutSeconds != 60 {
		t.Fatalf("IdleTimeoutSeconds = %d", got.IdleTimeoutSeconds)
	}
}

func TestResolveStructuredModelAfterConfigStoreLoad(t *testing.T) {
	zero := 0
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		Version:            config.CurrentVersion,
		ModelConfigVersion: config.CurrentModelConfigVersion,
		ModelCredentials:   map[string]config.ModelCredential{"ih": {APIKeyRef: "env:KEY"}},
		ModelProviders: map[string]config.ModelProvider{"ih": {
			Protocol: "chat-completions", BaseURL: "https://example.invalid/v1", Credential: "ih",
			HTTP:   config.ModelHTTPPolicy{ResponseHeaderTimeoutSeconds: 45, MaxRetries: &zero},
			Stream: config.ModelStreamPolicy{IdleTimeoutSeconds: 90},
		}},
		Models:        map[string]config.ModelDefinition{"glm": {Provider: "ih", UpstreamModel: "vendor/glm"}},
		ModelProfiles: map[string]config.ModelProfile{"work-glm": {Provider: "ih", Model: "glm", Credential: "ih", Revision: 1}},
	}
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(loaded, "work-glm")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Model.HTTPPolicy.MaxRetries == nil || *resolved.Model.HTTPPolicy.MaxRetries != 0 || resolved.Model.HTTPPolicy.ResponseHeaderTimeoutSeconds != 45 || resolved.Model.StreamPolicy.IdleTimeoutSeconds != 90 {
		t.Fatalf("resolved policies = HTTP:%#v stream:%#v", resolved.Model.HTTPPolicy, resolved.Model.StreamPolicy)
	}
}
