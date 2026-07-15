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

func TestResolveStructuredModelAppliesProfileReasoningOverride(t *testing.T) {
	yes := true
	cfg := config.Config{ModelConfigVersion: 1,
		ModelCredentials: map[string]config.ModelCredential{"p-key": {APIKeyRef: "env:P_KEY"}},
		ModelProviders:   map[string]config.ModelProvider{"p": {Protocol: "chat-completions", BaseURL: "https://p.example.invalid/v1", Credential: "p-key"}},
		Models:           map[string]config.ModelDefinition{"p/model": {Provider: "p", UpstreamModel: "vendor/model", Capabilities: config.ModelCapabilities{Reasoning: &yes}, Reasoning: config.ModelReasoningPolicy{SupportedEfforts: []string{"low", "high"}, DefaultEffort: "low", EffortMap: map[string]string{"high": "medium"}}}},
		ModelProfiles:    map[string]config.ModelProfile{"p/model": {Provider: "p", Model: "p/model", Credential: "p-key", DefaultReasoningEffort: "high", SupportedReasoningEfforts: []string{"high"}, ReasoningEffortMap: map[string]string{"high": "max"}, Revision: 1}},
	}
	got, err := Resolve(cfg, "p/model")
	if err != nil {
		t.Fatal(err)
	}
	if got.Provider.DefaultReasoningEffort != "high" || got.Model.ReasoningPolicy.DefaultEffort != "high" || got.Model.ReasoningPolicy.EffortMap["high"] != "max" || len(got.Model.ReasoningPolicy.SupportedEfforts) != 1 {
		t.Fatalf("profile reasoning override was ignored: provider=%#v model=%#v", got.Provider, got.Model)
	}
}

func TestResolveThirdPartyModelProfileWithSSHProxy(t *testing.T) {
	now := time.Now()
	cfg := config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:        "ssh-1",
			Name:      "work",
			Host:      "host",
			Port:      22,
			User:      "user",
			CreatedAt: now,
		}},
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-work": {
				Provider:  "deepseek",
				Model:     "deepseek/deepseek-v4-pro",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				SSHProxy:  "work",
				Revision:  3,
			},
		},
	}

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
	cfg := config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:        "ssh-1",
			Name:      "jump",
			Host:      "host",
			Port:      22,
			User:      "user",
			CreatedAt: now,
		}},
		ModelProfiles: map[string]config.ModelProfile{
			"mimo25": {
				Provider:  "mimo",
				APIKeyRef: "env:NEW_MIMO_KEY",
				SSHProxy:  "jump",
				Revision:  9,
			},
		},
	}
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
	cfg := config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:   "ssh-1",
			Name: "jump",
			Host: "host",
			Port: 22,
			User: "user",
		}},
		ModelProfiles: map[string]config.ModelProfile{
			"mimo25": {
				Provider:  "mimo",
				Model:     "mimo/mimo-v2.5-pro",
				APIKeyRef: "env:MIMO_API_KEY",
				SSHProxy:  "jump",
				Revision:  2,
			},
		},
	}
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

func TestRuntimeSnapshotAllowsLegacyContextWindowFingerprint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		profile  string
		provider string
		model    string
		keyRef   string
	}{
		{
			name:     "deepseek-flash",
			profile:  "deepseek-flash",
			provider: "deepseek",
			model:    "deepseek/deepseek-v4-flash",
			keyRef:   "env:DEEPSEEK_API_KEY",
		},
		{
			name:     "deepseek-pro",
			profile:  "deepseek-pro",
			provider: "deepseek",
			model:    "deepseek/deepseek-v4-pro",
			keyRef:   "env:DEEPSEEK_API_KEY",
		},
		{
			name:     "mimo25",
			profile:  "mimo25",
			provider: "mimo",
			model:    "mimo/mimo-v2.5",
			keyRef:   "env:MIMO_API_KEY",
		},
		{
			name:     "mimo25-pro",
			profile:  "mimo25-pro",
			provider: "mimo",
			model:    "mimo/mimo-v2.5-pro",
			keyRef:   "env:MIMO_API_KEY",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Config{
				Version: config.CurrentVersion,
				ModelProfiles: map[string]config.ModelProfile{
					tc.profile: {
						Provider:  tc.provider,
						Model:     tc.model,
						APIKeyRef: tc.keyRef,
						Revision:  1,
					},
				},
			}
			resolved, err := Resolve(cfg, tc.profile)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			oldProvider := providerWithLegacy128KContext(resolved.Provider)
			snapshot := resolved.Snapshot(time.Now())
			snapshot.ModelFingerprint = legacyModelFingerprintV1(oldProvider, snapshot.Model)
			snapshot.CatalogFingerprint = CatalogFingerprint(oldProvider)

			if err := ValidateSnapshotRuntime(snapshot, resolved, "sk-test"); err != nil {
				t.Fatalf("ValidateSnapshotRuntime legacy context fingerprint: %v", err)
			}
		})
	}
}

func TestRuntimeSnapshotRejectsLegacyFingerprintWhenUpstreamMappingChanges(t *testing.T) {
	cfg := config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-pro": {
				Provider:  "deepseek",
				Model:     "deepseek/deepseek-v4-pro",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				Revision:  1,
			},
		},
	}
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
	cfg := config.Config{
		Version: config.CurrentVersion,
		ModelProfiles: map[string]config.ModelProfile{
			"missing-key": {Provider: "deepseek"},
			"missing-ssh": {Provider: "deepseek", APIKeyRef: "env:DEEPSEEK_API_KEY", SSHProxy: "none"},
			"bad-model":   {Provider: "deepseek", Model: "nope", APIKeyRef: "env:DEEPSEEK_API_KEY"},
			"unknown":     {Provider: "unknown", APIKeyRef: "env:KEY"},
		},
	}
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
