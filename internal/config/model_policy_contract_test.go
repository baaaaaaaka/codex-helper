package config

import "testing"

func TestParseModelConfigFragmentAcceptsProgressAndCapabilityContract(t *testing.T) {
	raw := []byte(`{
  "modelConfigVersion": 1,
  "modelProviders": {"p": {"protocol":"chat-completions", "baseUrl":"https://example.invalid/v1", "stream": {"firstEventTimeoutSeconds": 30, "semanticProgressTimeoutSeconds": 60, "maxDurationSeconds": 120, "heartbeatMode":"transport-only", "reasoningTokensPath":"usage.completion_tokens_details.reasoning_tokens"}}},
  "models": {"m": {"provider":"p", "upstreamModel":"upstream/m", "tools":{"parallel":"disabled", "parallelEnforcement":"strict", "plainTextToolCall":"reject"}, "responses":{"structuredOutput":{"jsonObject":"unsupported", "jsonSchema":"native"}}, "capabilities":{"nativeWebSearch":false}, "search":{"native":false,"fallback":{"enabled":true,"model":"gpt-5.6-luna","effort":"high"}}}}
}`)
	cfg, err := ParseModelConfigFragment(raw)
	if err != nil {
		t.Fatalf("ParseModelConfigFragment: %v", err)
	}
	if err := ValidateModelConfig(cfg); err != nil {
		t.Fatalf("ValidateModelConfig: %v", err)
	}
	if cfg.ModelProviders["p"].Stream.MaxDurationSeconds != 120 || cfg.Models["m"].Tools.ParallelEnforcement != "strict" || cfg.Models["m"].Search.Fallback.Model != "gpt-5.6-luna" {
		t.Fatalf("policy was not decoded: %#v %#v", cfg.Models["m"], cfg.ModelProviders["p"])
	}
}
