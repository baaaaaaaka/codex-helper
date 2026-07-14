package cli

import "github.com/baaaaaaaka/codex-helper/internal/modelprofile"

// testExternalProviderSpec models the provider metadata that an external
// catalog would materialize. It deliberately lives in tests: production must
// not carry a DeepSeek or MiMo registry entry.
func testExternalProviderSpec(provider string) modelprofile.ProviderSpec {
	if provider == "deepseek" {
		return modelprofile.ProviderSpec{
			ID: "deepseek", DisplayName: "DeepSeek (external)", DefaultModel: "deepseek/deepseek-v4-flash", BaseURL: "https://catalog.example/deepseek/v1", AdapterProfile: "openai-chat", UsesAdapter: true, SupportsTools: true, SupportsReason: true,
			Models: []modelprofile.ModelSpec{{ID: "deepseek/deepseek-v4-flash", UpstreamID: "deepseek-v4-flash", DisplayName: "DeepSeek V4 Flash", ContextWindow: 1000000, MaxContextWindow: 1000000, SupportsTools: true, SupportsReason: true}, {ID: "deepseek/deepseek-v4-pro", UpstreamID: "deepseek-v4-pro", DisplayName: "DeepSeek V4 Pro", ContextWindow: 1000000, MaxContextWindow: 1000000, SupportsTools: true, SupportsReason: true}},
		}
	}
	return modelprofile.ProviderSpec{
		ID: "mimo", DisplayName: "MiMo (external)", DefaultModel: "mimo/mimo-v2.5", BaseURL: "https://catalog.example/mimo/v1", AdapterProfile: "openai-chat", UsesAdapter: true, SupportsTools: true, SupportsVision: true, SupportsReason: true,
		Models: []modelprofile.ModelSpec{{ID: "mimo/mimo-v2.5", UpstreamID: "mimo-v2.5", DisplayName: "MiMo 2.5", ContextWindow: 1000000, MaxContextWindow: 1000000, SupportsTools: true, SupportsVision: true, SupportsReason: true}, {ID: "mimo/mimo-v2.5-pro", UpstreamID: "mimo-v2.5-pro", DisplayName: "MiMo 2.5 Pro", ContextWindow: 1000000, MaxContextWindow: 1000000, SupportsTools: true, SupportsVision: true, SupportsReason: true}},
	}
}
