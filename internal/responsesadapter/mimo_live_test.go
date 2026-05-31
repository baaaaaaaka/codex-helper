package responsesadapter

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLiveMimo25MultimodalResponsesFlow(t *testing.T) {
	if !liveEnvEnabled("CODEX_HELPER_LIVE_MIMO", "CODEX_HELPER_LIVE_MIMO_MULTIMODAL") {
		t.Skip("set CODEX_HELPER_LIVE_MIMO=1 or CODEX_HELPER_LIVE_MIMO_MULTIMODAL=1 to run live MiMo multimodal test")
	}
	apiKey := strings.TrimSpace(os.Getenv("MIMO_API_KEY"))
	if apiKey == "" {
		t.Skip("MIMO_API_KEY is not set")
	}

	facade := liveFacade("mimo", "mimo-v2.5", "https://api.xiaomimimo.com/v1", apiKey)
	body := map[string]any{
		"model":             "mimo-v2.5",
		"max_output_tokens": 256,
		"input": []any{
			map[string]any{
				"type": "message",
				"role": "user",
				"content": []any{
					map[string]any{"type": "input_text", "text": "What is the dominant color in this image? Answer with one English word."},
					map[string]any{"type": "input_image", "image_url": redPNGDataURL()},
				},
			},
		},
	}
	response := postLiveResponses(t, facade, body)
	text := strings.ToLower(response.OutputText)
	if !strings.Contains(text, "red") {
		t.Fatalf("output_text = %q output=%#v, want answer to identify red image", response.OutputText, response.Output)
	}
}

func TestLiveMimo25ToolCallResponsesFlow(t *testing.T) {
	if !liveEnvEnabled("CODEX_HELPER_LIVE_MIMO", "CODEX_HELPER_LIVE_MIMO_TOOLS") {
		t.Skip("set CODEX_HELPER_LIVE_MIMO=1 or CODEX_HELPER_LIVE_MIMO_TOOLS=1 to run live MiMo tool-call test")
	}
	apiKey := strings.TrimSpace(os.Getenv("MIMO_API_KEY"))
	if apiKey == "" {
		t.Skip("MIMO_API_KEY is not set")
	}

	response := postLiveResponses(t, liveFacade("mimo", "mimo-v2.5", "https://api.xiaomimimo.com/v1", apiKey), liveToolCallRequest("mimo-v2.5", 256))
	assertLiveWeatherToolCall(t, response)
}

func TestLiveDeepSeekV4FlashTextResponsesFlow(t *testing.T) {
	if !liveEnvEnabled("CODEX_HELPER_LIVE_DEEPSEEK") {
		t.Skip("set CODEX_HELPER_LIVE_DEEPSEEK=1 to run live DeepSeek test")
	}
	apiKey := strings.TrimSpace(os.Getenv("DEEPSEEK_API_KEY"))
	if apiKey == "" {
		t.Skip("DEEPSEEK_API_KEY is not set")
	}

	response := postLiveResponses(t, liveFacade("deepseek", "deepseek-v4-flash", "https://api.deepseek.com/v1", apiKey), map[string]any{
		"model":             "deepseek-v4-flash",
		"max_output_tokens": 192,
		"input":             "Reply with exactly: cxp-live-ok",
	})
	if !strings.Contains(strings.ToLower(response.OutputText), "cxp-live-ok") {
		t.Fatalf("output_text = %q output=%#v, want cxp-live-ok", response.OutputText, response.Output)
	}
	if response.Usage == nil || response.Usage.TotalTokens == 0 {
		t.Fatalf("usage = %#v, want populated usage", response.Usage)
	}
}

func TestLiveDeepSeekV4FlashToolCallResponsesFlow(t *testing.T) {
	if !liveEnvEnabled("CODEX_HELPER_LIVE_DEEPSEEK", "CODEX_HELPER_LIVE_DEEPSEEK_TOOLS") {
		t.Skip("set CODEX_HELPER_LIVE_DEEPSEEK=1 or CODEX_HELPER_LIVE_DEEPSEEK_TOOLS=1 to run live DeepSeek tool-call test")
	}
	apiKey := strings.TrimSpace(os.Getenv("DEEPSEEK_API_KEY"))
	if apiKey == "" {
		t.Skip("DEEPSEEK_API_KEY is not set")
	}

	response := postLiveResponses(t, liveFacade("deepseek", "deepseek-v4-flash", "https://api.deepseek.com/v1", apiKey), liveToolCallRequest("deepseek-v4-flash", 320))
	assertLiveWeatherToolCall(t, response)
}

func liveEnvEnabled(names ...string) bool {
	for _, name := range names {
		if os.Getenv(name) == "1" {
			return true
		}
	}
	return false
}

func liveFacade(provider string, model string, baseURL string, apiKey string) *Facade {
	return &Facade{
		Adapter: OpenAIChatAdapter{
			BaseURL:    baseURL,
			APIKey:     apiKey,
			Profile:    ProfileForProvider(provider),
			HTTPClient: &http.Client{Timeout: 90 * time.Second},
		},
		Store:        NewMemoryStore(),
		ProviderID:   provider,
		DefaultModel: model,
		NewID:        func(prefix string) (string, error) { return prefix + "_live_" + provider, nil },
	}
}

func postLiveResponses(t *testing.T, facade *Facade, body map[string]any) responseObject {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(raw))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response responseObject
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rec.Body.String())
	}
	return response
}

func liveToolCallRequest(model string, maxTokens int) map[string]any {
	return map[string]any{
		"model":             model,
		"max_output_tokens": maxTokens,
		"input":             "Call the tool named get_weather with city set to Paris. Do not answer directly.",
		"tool_choice":       "auto",
		"tools": []any{
			map[string]any{
				"type":        "function",
				"name":        "get_weather",
				"description": "Get weather by city",
				"parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"city": map[string]any{"type": "string"},
					},
					"required": []string{"city"},
				},
			},
		},
	}
}

func assertLiveWeatherToolCall(t *testing.T, response responseObject) {
	t.Helper()
	for _, item := range response.Output {
		if item.Type != "function_call" {
			continue
		}
		if item.Name != "get_weather" {
			t.Fatalf("tool name = %q output=%#v", item.Name, response.Output)
		}
		if !strings.Contains(strings.ToLower(item.Arguments), "paris") {
			t.Fatalf("tool arguments = %q output=%#v", item.Arguments, response.Output)
		}
		return
	}
	t.Fatalf("missing function_call output: output_text=%q output=%#v", response.OutputText, response.Output)
}

func redPNGDataURL() string {
	img := image.NewRGBA(image.Rect(0, 0, 32, 32))
	for y := 0; y < 32; y++ {
		for x := 0; x < 32; x++ {
			img.Set(x, y, color.RGBA{R: 255, A: 255})
		}
	}
	var buf bytes.Buffer
	_ = png.Encode(&buf, img)
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(buf.Bytes())
}
