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
		{name: "deepseek default stays high", profile: ProfileForProvider("deepseek"), request: "", expected: "high"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.profile.reasoningEffort(tt.request); got != tt.expected {
				t.Fatalf("reasoningEffort(%q) = %q, want %q", tt.request, got, tt.expected)
			}
		})
	}
}
