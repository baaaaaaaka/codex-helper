package responsesadapter

import (
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func testCatalogDeepSeekProfile() ProviderProfile {
	return ProviderProfile{ID: "catalog-deepseek", IncludeUsageStreamOptions: true, MergeSystemMessages: true, OmitEmptyAssistantContentWithToolCalls: true, DefaultReasoningEffort: "high", ReasoningEffortMap: map[string]string{"xhigh": "max"}, EnableThinking: true, StripSamplingWhenThinking: true}
}

func testCatalogMiMoProfile(imagePolicy, reasoningPolicy string) ProviderProfile {
	parallel := true
	return ProviderProfile{ID: "catalog-mimo", IncludeUsageStreamOptions: true, MergeSystemMessages: true, OmitEmptyAssistantContentWithToolCalls: true, DefaultReasoningEffort: "high", EnableThinking: true, ForceParallelToolCalls: &parallel, StripSamplingWhenThinking: true, DropNonAutoToolChoice: true, ImagePolicy: imagePolicy, ReasoningContentPolicy: reasoningPolicy}
}

func TestProviderProfileReasoningEffortMapsXHighByProvider(t *testing.T) {
	tests := []struct {
		name     string
		profile  ProviderProfile
		request  string
		expected string
	}{
		{name: "catalog mapping uses max", profile: ProviderProfile{DefaultReasoningEffort: "high", ReasoningEffortMap: map[string]string{"xhigh": "max"}}, request: "xhigh", expected: "max"},
		{name: "catalog default stays high", profile: ProviderProfile{DefaultReasoningEffort: "high"}, request: "xhigh", expected: "high"},
		{name: "generic xhigh stays high", profile: ProfileForProvider("openai"), request: "xhigh", expected: "high"},
		{name: "glm xhigh uses max", profile: ProfileForProvider("glm"), request: "xhigh", expected: "max"},
		{name: "external override replaces mapping", profile: ProfileForProvider("openai-chat").WithReasoningOverrides("high", map[string]string{"xhigh": "max"}), request: "xhigh", expected: "max"},
		{name: "catalog default effort", profile: ProviderProfile{DefaultReasoningEffort: "high"}, request: "", expected: "high"},
		{name: "none is preserved for model policy", profile: ProviderProfile{DefaultReasoningEffort: "high"}, request: "none", expected: "none"},
		{name: "explicit catalog map can downgrade none", profile: ProviderProfile{DefaultReasoningEffort: "high"}.WithReasoningOverrides("high", map[string]string{"none": "low"}), request: "none", expected: "low"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.profile.reasoningEffort(tt.request); got != tt.expected {
				t.Fatalf("reasoningEffort(%q) = %q, want %q", tt.request, got, tt.expected)
			}
		})
	}
}

func TestWithModelPoliciesMapsExternalHistoryAndImagePolicies(t *testing.T) {
	profile := ProfileForProvider("external-catalog").WithModelPolicies(
		config.ModelReasoningPolicy{HistoryPolicy: "never"},
		config.ModelToolPolicy{},
		config.ModelMessagePolicy{Images: "multimodal"},
		config.ModelSamplingPolicy{},
	)
	if profile.ReasoningContentPolicy != "drop" {
		t.Fatalf("reasoning content policy=%q, want drop", profile.ReasoningContentPolicy)
	}
	if profile.ImagePolicy != "multimodal" {
		t.Fatalf("image policy=%q, want multimodal", profile.ImagePolicy)
	}
	if profile.shouldSendReasoningContent("any-model") {
		t.Fatal("never history policy should drop reasoning content")
	}
}
