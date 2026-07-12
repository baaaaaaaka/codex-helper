package responsesadapter

import "testing"

func TestProviderProfileReasoningEffortMapsXHighByProvider(t *testing.T) {
	tests := []struct {
		name     string
		profile  ProviderProfile
		request  string
		expected string
	}{
		{name: "deepseek xhigh uses max", profile: ProfileForProvider("deepseek"), request: "xhigh", expected: "max"},
		{name: "mimo xhigh stays high", profile: ProfileForProvider("mimo"), request: "xhigh", expected: "high"},
		{name: "generic xhigh stays high", profile: ProfileForProvider("openai"), request: "xhigh", expected: "high"},
		{name: "glm xhigh uses max", profile: ProfileForProvider("glm"), request: "xhigh", expected: "max"},
		{name: "external override replaces mapping", profile: ProfileForProvider("openai-chat").WithReasoningOverrides("high", map[string]string{"xhigh": "max"}), request: "xhigh", expected: "max"},
		{name: "deepseek default stays high", profile: ProfileForProvider("deepseek"), request: "", expected: "high"},
		{name: "none is preserved for model policy", profile: ProfileForProvider("deepseek"), request: "none", expected: "none"},
		{name: "explicit compatibility map can downgrade none", profile: ProfileForProvider("deepseek").WithReasoningOverrides("high", map[string]string{"none": "low"}), request: "none", expected: "low"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.profile.reasoningEffort(tt.request); got != tt.expected {
				t.Fatalf("reasoningEffort(%q) = %q, want %q", tt.request, got, tt.expected)
			}
		})
	}
}
