package cloudgate

import "testing"

func TestShouldIntercept(t *testing.T) {
	cfg := DefaultConfig()

	tests := []struct {
		host string
		want bool
	}{
		{"chatgpt.com", true},
		{"chatgpt.com:443", true},
		{"chat.openai.com", true},
		{"chat.openai.com:443", true},
		{"api.openai.com", false},
		{"api.openai.com:443", false},
		{"example.com", false},
		{"", false},
	}

	for _, tt := range tests {
		if got := cfg.ShouldIntercept(tt.host); got != tt.want {
			t.Errorf("ShouldIntercept(%q) = %v, want %v", tt.host, got, tt.want)
		}
	}
}

func TestShouldInterceptNilConfig(t *testing.T) {
	var cfg *Config
	if cfg.ShouldIntercept("chatgpt.com") {
		t.Error("nil config should not intercept")
	}
}

func TestDefaultConfigHosts(t *testing.T) {
	cfg := DefaultConfig()
	if !cfg.Hosts["chatgpt.com"] {
		t.Error("missing chatgpt.com")
	}
	if !cfg.Hosts["chat.openai.com"] {
		t.Error("missing chat.openai.com")
	}
}

func TestCleanupNilSafe(t *testing.T) {
	var cfg *Config
	cfg.Cleanup() // should not panic

	cfg2 := DefaultConfig()
	cfg2.Cleanup() // no MITM, should not panic
}
