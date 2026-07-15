package responsesadapter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestConfiguredAdapterContractCoversEveryTranslatedAdapter(t *testing.T) {
	tests := []struct {
		name    string
		adapter string
		profile string
		want    string
	}{
		{name: "openai chat", adapter: "openai-chat", want: "OpenAIChatAdapter"},
		{name: "deepseek openai", adapter: "deepseek-openai", want: "OpenAIChatAdapter"},
		{name: "mimo chat", adapter: "mimo-chat", want: "OpenAIChatAdapter"},
		{name: "deepseek anthropic", adapter: "deepseek-anthropic", profile: "deepseek-anthropic-v1", want: "AnthropicAdapter"},
		{name: "deepseek beta", adapter: "deepseek-beta", profile: "deepseek-beta-v1", want: "DeepSeekBetaAdapter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, err := NewConfiguredAdapter(AdapterOptions{AdapterID: tt.adapter, ConversionProfile: tt.profile, BaseURL: "https://example.invalid/v1", APIKey: "test-key"})
			if err != nil {
				t.Fatalf("NewConfiguredAdapter() = %v", err)
			}
			var got string
			switch adapter.(type) {
			case OpenAIChatAdapter:
				got = "OpenAIChatAdapter"
			case AnthropicAdapter:
				got = "AnthropicAdapter"
			case DeepSeekBetaAdapter:
				got = "DeepSeekBetaAdapter"
			default:
				got = "unknown"
			}
			if got != tt.want {
				t.Fatalf("adapter type = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestChatAdapterContractMatrixUsesCatalogTransportAndReturnsUsage(t *testing.T) {
	tests := []struct {
		name    string
		adapter string
		profile string
	}{
		{name: "openai", adapter: "openai-chat"},
		{name: "deepseek", adapter: "deepseek-openai"},
		{name: "mimo", adapter: "mimo-chat"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/chat/completions" {
					t.Errorf("path = %q, want /v1/chat/completions", r.URL.Path)
				}
				if got := r.Header.Get("Authorization"); got != "Bearer catalog-key" {
					t.Errorf("authorization = %q", got)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode request: %v", err)
				}
				if body["model"] != "catalog-model" || body["stream"] != false {
					t.Errorf("request identity = %#v", body)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`)
			}))
			defer server.Close()
			adapter, err := NewConfiguredAdapter(AdapterOptions{
				AdapterID: tt.adapter, ConversionProfile: tt.profile, BaseURL: server.URL + "/v1", APIKey: "catalog-key",
				Profile: ProfileForProvider(tt.adapter), Stream: config.ModelStreamPolicy{UpstreamMode: "nonstream-buffered"}, HTTP: config.ModelHTTPPolicy{TimeoutSeconds: 5},
			})
			if err != nil {
				t.Fatal(err)
			}
			stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "catalog-model", InputText: "hello"})
			if err != nil {
				t.Fatal(err)
			}
			events := collectProviderEvents(t, stream)
			var text string
			var usage *Usage
			for _, event := range events {
				if event.Kind == ProviderEventTextDelta {
					text += event.Delta
				}
				if event.Kind == ProviderEventUsage {
					usage = event.Usage
				}
			}
			if text != "ok" || usage == nil || usage.InputTokens != 4 || usage.OutputTokens != 2 || usage.TotalTokens != 6 {
				t.Fatalf("events = %#v", events)
			}
		})
	}
}
