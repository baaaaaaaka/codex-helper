package responsesadapter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAIChatAdapterStreamsTextAndUsage(t *testing.T) {
	var gotBody chatCompletionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"content":"he"}}]}`,
			"",
			`data: {"choices":[{"delta":{"content":"llo"}}]}`,
			"",
			`data: {"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5,"prompt_tokens_details":{"cached_tokens":1},"completion_tokens_details":{"reasoning_tokens":0}}}`,
			"",
			`data: [DONE]`,
			"",
		}, "\n")))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", APIKey: "test-key", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{
		Model:     "model-a",
		InputText: "hello",
		Tools: []ChatTool{{
			Type:     "function",
			Function: ChatFunction{Name: "read_file", Parameters: json.RawMessage(`{"type":"object"}`)},
		}},
	})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	if got := eventText(events); got != "hello" {
		t.Fatalf("text = %q", got)
	}
	if len(events) == 0 || events[len(events)-1].Kind != ProviderEventDone {
		t.Fatalf("events = %#v", events)
	}
	var usage *Usage
	for _, event := range events {
		if event.Kind == ProviderEventUsage {
			usage = event.Usage
		}
	}
	if usage == nil || usage.InputTokens != 3 || usage.CachedTokens != 1 || usage.TotalTokens != 5 {
		t.Fatalf("usage = %#v", usage)
	}
	if !gotBody.Stream || gotBody.Model != "model-a" || len(gotBody.Messages) != 1 || gotBody.Messages[0].Content != "hello" {
		t.Fatalf("request body = %#v", gotBody)
	}
	if gotBody.StreamOptions == nil || !gotBody.StreamOptions.IncludeUsage {
		t.Fatalf("stream_options = %#v", gotBody.StreamOptions)
	}
	if len(gotBody.Tools) != 1 || gotBody.Tools[0].Function.Name != "read_file" {
		t.Fatalf("tools = %#v", gotBody.Tools)
	}
}

func TestOpenAIChatAdapterKeepsTailUsageAfterFinishChunk(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}`,
			"",
			`data: {"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10,"prompt_tokens_details":{"cached_tokens":4},"completion_tokens_details":{"reasoning_tokens":2}}}`,
			"",
			`data: [DONE]`,
			"",
		}, "\n")))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	if got := eventText(events); got != "done" {
		t.Fatalf("text = %q", got)
	}
	var usage *Usage
	for _, event := range events {
		if event.Kind == ProviderEventUsage {
			usage = event.Usage
		}
	}
	if usage == nil || usage.InputTokens != 7 || usage.OutputTokens != 3 || usage.TotalTokens != 10 || usage.CachedTokens != 4 || usage.ReasoningTokens != 2 {
		t.Fatalf("usage = %#v, events = %#v", usage, events)
	}
	if len(events) == 0 || events[len(events)-1].Kind != ProviderEventDone {
		t.Fatalf("events = %#v", events)
	}
}

func TestOpenAIChatAdapterHandlesNetworkChunkBoundariesInsideSSELine(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		_, _ = w.Write([]byte(`data: {"choices":[{"delta":`))
		flusher.Flush()
		_, _ = w.Write([]byte(`{"content":"hel"}}]}` + "\n\n"))
		flusher.Flush()
		_, _ = w.Write([]byte(`data: {"choices":[{"delta":{"content":"lo"}}]}` + "\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	if got := eventText(events); got != "hello" {
		t.Fatalf("text = %q, events = %#v", got, events)
	}
}

func TestOpenAIChatAdapterIncludesHistoryMessages(t *testing.T) {
	var gotBody chatCompletionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{
		Model:     "model-a",
		InputText: "continue",
		History: []ResponseRecord{
			{InputText: "first user", OutputText: "first assistant"},
		},
	})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	_ = collectEvents(stream)

	want := []chatMessage{
		{Role: "user", Content: "first user"},
		{Role: "assistant", Content: "first assistant"},
		{Role: "user", Content: "continue"},
	}
	if len(gotBody.Messages) != len(want) {
		t.Fatalf("messages = %#v", gotBody.Messages)
	}
	for i := range want {
		if gotBody.Messages[i].Role != want[i].Role || gotBody.Messages[i].Content != want[i].Content || gotBody.Messages[i].ToolCallID != want[i].ToolCallID || len(gotBody.Messages[i].ToolCalls) != 0 {
			t.Fatalf("message %d = %#v, want %#v", i, gotBody.Messages[i], want[i])
		}
	}
}

func TestOpenAIChatAdapterSendsToolCallTranscript(t *testing.T) {
	var gotBody chatCompletionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{
		Model: "model-a",
		Messages: []ProviderMessage{
			{Role: "user", Content: "read main"},
			{Role: "assistant", ReasoningContent: "need file", ToolCalls: []ToolCallRecord{{ID: "call_read", Name: "read_file", Arguments: `{"path":"main.go"}`}}},
			{Role: "tool", ToolCallID: "call_read", Content: "file contents"},
		},
	})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	_ = collectEvents(stream)

	if len(gotBody.Messages) != 3 {
		t.Fatalf("messages = %#v", gotBody.Messages)
	}
	assistant := gotBody.Messages[1]
	if assistant.Role != "assistant" || len(assistant.ToolCalls) != 1 || assistant.ToolCalls[0].ID != "call_read" {
		t.Fatalf("assistant message = %#v", assistant)
	}
	if assistant.ReasoningContent != "need file" {
		t.Fatalf("assistant reasoning_content = %q", assistant.ReasoningContent)
	}
	if assistant.ToolCalls[0].Function.Name != "read_file" || assistant.ToolCalls[0].Function.Arguments != `{"path":"main.go"}` {
		t.Fatalf("tool call = %#v", assistant.ToolCalls[0])
	}
	tool := gotBody.Messages[2]
	if tool.Role != "tool" || tool.ToolCallID != "call_read" || tool.Content != "file contents" {
		t.Fatalf("tool message = %#v", tool)
	}
}

func TestOpenAIChatAdapterSendsCodexRequestControls(t *testing.T) {
	var gotBody chatCompletionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	parallel := false
	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{
		Model:             "model-a",
		Instructions:      "base instructions",
		InputText:         "hello",
		ToolChoice:        json.RawMessage(`"none"`),
		ParallelToolCalls: &parallel,
	})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	_ = collectEvents(stream)

	if len(gotBody.Messages) != 2 {
		t.Fatalf("messages = %#v", gotBody.Messages)
	}
	if gotBody.Messages[0].Role != "system" || gotBody.Messages[0].Content != "base instructions" {
		t.Fatalf("instruction message = %#v", gotBody.Messages[0])
	}
	if gotBody.Messages[1].Role != "user" || gotBody.Messages[1].Content != "hello" {
		t.Fatalf("user message = %#v", gotBody.Messages[1])
	}
	if string(gotBody.ToolChoice) != `"none"` {
		t.Fatalf("tool_choice = %s", gotBody.ToolChoice)
	}
	if gotBody.ParallelToolCalls == nil || *gotBody.ParallelToolCalls {
		t.Fatalf("parallel_tool_calls = %#v", gotBody.ParallelToolCalls)
	}
}

func TestOpenAIChatAdapterAppliesDeepSeekProviderProfile(t *testing.T) {
	var gotBody chatCompletionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	maxTokens := 1024
	temperature := 0.5
	topP := 0.9
	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", Profile: ProfileForProvider("deepseek"), HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{
		Model:           "deepseek-v4-pro",
		InputText:       "hello",
		MaxOutputTokens: &maxTokens,
		ReasoningEffort: "xhigh",
		Temperature:     &temperature,
		TopP:            &topP,
	})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	_ = collectEvents(stream)

	if gotBody.MaxCompletionTokens == nil || *gotBody.MaxCompletionTokens != 1024 {
		t.Fatalf("max_completion_tokens = %#v", gotBody.MaxCompletionTokens)
	}
	if gotBody.Thinking == nil || gotBody.Thinking.Type != "enabled" {
		t.Fatalf("thinking = %#v", gotBody.Thinking)
	}
	if gotBody.ReasoningEffort != "high" {
		t.Fatalf("reasoning_effort = %q", gotBody.ReasoningEffort)
	}
	if gotBody.Temperature != nil || gotBody.TopP != nil {
		t.Fatalf("sampling should be stripped in thinking mode: temperature=%#v top_p=%#v", gotBody.Temperature, gotBody.TopP)
	}
}

func TestOpenAIChatAdapterDeepSeekReasoningContentModelMatrix(t *testing.T) {
	for _, tc := range []struct {
		name          string
		model         string
		wantReasoning string
		wantThinking  bool
	}{
		{name: "v4 preserves reasoning content", model: "deepseek-v4-pro", wantReasoning: "prior thought", wantThinking: true},
		{name: "legacy reasoner strips reasoning content", model: "deepseek-reasoner", wantReasoning: "", wantThinking: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotBody chatCompletionRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
					t.Fatalf("decode body: %v", err)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: [DONE]\n\n"))
			}))
			defer server.Close()

			adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", Profile: ProfileForProvider("deepseek"), HTTPClient: server.Client()}
			stream, err := adapter.Stream(context.Background(), ProviderRequest{
				Model: tc.model,
				Messages: []ProviderMessage{
					{Role: "user", Content: "first"},
					{Role: "assistant", Content: "answer", ReasoningContent: "prior thought"},
					{Role: "user", Content: "follow-up"},
				},
			})
			if err != nil {
				t.Fatalf("Stream error: %v", err)
			}
			_ = collectEvents(stream)
			if len(gotBody.Messages) != 3 {
				t.Fatalf("messages = %#v", gotBody.Messages)
			}
			if gotBody.Messages[1].ReasoningContent != tc.wantReasoning {
				t.Fatalf("reasoning_content = %q, want %q", gotBody.Messages[1].ReasoningContent, tc.wantReasoning)
			}
			if (gotBody.Thinking != nil) != tc.wantThinking {
				t.Fatalf("thinking = %#v, want present=%v", gotBody.Thinking, tc.wantThinking)
			}
		})
	}
}

func TestOpenAIChatAdapterAppliesMimoProviderProfile(t *testing.T) {
	var gotBody chatCompletionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	parallel := false
	temperature := 0.5
	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", Profile: ProfileForProvider("mimo"), HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{
		Model:             "mimo-v2.5-pro",
		InputText:         "hello",
		ParallelToolCalls: &parallel,
		ToolChoice:        json.RawMessage(`"required"`),
		ReasoningEffort:   "",
		Temperature:       &temperature,
	})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	_ = collectEvents(stream)

	if gotBody.ParallelToolCalls == nil || !*gotBody.ParallelToolCalls {
		t.Fatalf("parallel_tool_calls = %#v", gotBody.ParallelToolCalls)
	}
	if len(gotBody.ToolChoice) != 0 {
		t.Fatalf("tool_choice should be stripped for MiMo non-auto values: %s", gotBody.ToolChoice)
	}
	if gotBody.Thinking == nil || gotBody.Thinking.Type != "enabled" {
		t.Fatalf("thinking = %#v", gotBody.Thinking)
	}
	if gotBody.ReasoningEffort != "high" {
		t.Fatalf("reasoning_effort = %q", gotBody.ReasoningEffort)
	}
	if gotBody.Temperature != nil {
		t.Fatalf("temperature should be stripped for MiMo v2.5 thinking mode: %#v", gotBody.Temperature)
	}
}

func TestOpenAIChatAdapterMimoPreservesAutoToolChoice(t *testing.T) {
	var gotBody chatCompletionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", Profile: ProfileForProvider("mimo"), HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{
		Model:      "mimo-v2.5-pro",
		InputText:  "hello",
		ToolChoice: json.RawMessage(`"auto"`),
	})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	_ = collectEvents(stream)
	if string(gotBody.ToolChoice) != `"auto"` {
		t.Fatalf("tool_choice = %s", gotBody.ToolChoice)
	}
}

func TestOpenAIChatAdapterMimoFlashKeepsSamplingWithoutThinking(t *testing.T) {
	var gotBody chatCompletionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	temperature := 0.5
	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", Profile: ProfileForProvider("mimo"), HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{
		Model:       "mimo-v2-flash",
		InputText:   "hello",
		Temperature: &temperature,
	})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	_ = collectEvents(stream)

	if gotBody.Thinking != nil {
		t.Fatalf("thinking should not be enabled for mimo-v2-flash: %#v", gotBody.Thinking)
	}
	if gotBody.Temperature == nil || *gotBody.Temperature != temperature {
		t.Fatalf("temperature = %#v", gotBody.Temperature)
	}
}

func TestFacadeMimo25ForwardsInputImageAsMultimodalChatContent(t *testing.T) {
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	facade := &Facade{
		Adapter:      OpenAIChatAdapter{BaseURL: server.URL + "/v1", Profile: ProfileForProvider("mimo"), HTTPClient: server.Client(), MaxRetries: -1},
		Store:        NewMemoryStore(),
		ProviderID:   "mimo",
		DefaultModel: "mimo-v2.5",
		NewID:        func(prefix string) (string, error) { return prefix + "_001", nil },
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model":"mimo-v2.5",
		"input":[{"type":"message","role":"user","content":[
			{"type":"input_text","text":"what color is this?"},
			{"type":"input_image","image_url":"data:image/png;base64,abc","detail":"auto"}
		]}]
	}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	content := firstChatMessageContent(t, gotBody)
	parts, ok := content.([]any)
	if !ok {
		t.Fatalf("content = %#v, want multimodal array", content)
	}
	if len(parts) != 2 {
		t.Fatalf("parts = %#v", parts)
	}
	text := parts[0].(map[string]any)
	if text["type"] != "text" || text["text"] != "what color is this?" {
		t.Fatalf("text part = %#v", text)
	}
	image := parts[1].(map[string]any)
	imageURL := image["image_url"].(map[string]any)
	if image["type"] != "image_url" || imageURL["url"] != "data:image/png;base64,abc" || imageURL["detail"] != "auto" {
		t.Fatalf("image part = %#v", image)
	}
}

func TestFacadeMimo25AddsFallbackTextForImageOnlyMessages(t *testing.T) {
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	facade := &Facade{
		Adapter:      OpenAIChatAdapter{BaseURL: server.URL + "/v1", Profile: ProfileForProvider("mimo"), HTTPClient: server.Client(), MaxRetries: -1},
		Store:        NewMemoryStore(),
		ProviderID:   "mimo",
		DefaultModel: "mimo-v2.5",
		NewID:        func(prefix string) (string, error) { return prefix + "_001", nil },
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model":"mimo-v2.5",
		"input":[{"type":"message","role":"user","content":[
			{"type":"input_image","image_url":"https://example.test/image.png"}
		]}]
	}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	parts, ok := firstChatMessageContent(t, gotBody).([]any)
	if !ok || len(parts) != 2 {
		t.Fatalf("content = %#v", firstChatMessageContent(t, gotBody))
	}
	if parts[0].(map[string]any)["type"] != "text" || strings.TrimSpace(parts[0].(map[string]any)["text"].(string)) == "" {
		t.Fatalf("fallback text part = %#v", parts[0])
	}
	if parts[1].(map[string]any)["type"] != "image_url" {
		t.Fatalf("image part = %#v", parts[1])
	}
}

func TestFacadeMimo25ProDropsImagePartsWithPlaceholder(t *testing.T) {
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	facade := &Facade{
		Adapter:      OpenAIChatAdapter{BaseURL: server.URL + "/v1", Profile: ProfileForProvider("mimo"), HTTPClient: server.Client(), MaxRetries: -1},
		Store:        NewMemoryStore(),
		ProviderID:   "mimo",
		DefaultModel: "mimo-v2.5-pro",
		NewID:        func(prefix string) (string, error) { return prefix + "_001", nil },
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model":"mimo-v2.5-pro",
		"input":[{"type":"message","role":"user","content":[
			{"type":"input_text","text":"what color is this?"},
			{"type":"input_image","image_url":"data:image/png;base64,abc"}
		]}]
	}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	content, ok := firstChatMessageContent(t, gotBody).(string)
	if !ok {
		t.Fatalf("content = %#v, want text-only fallback", firstChatMessageContent(t, gotBody))
	}
	if !strings.Contains(content, "what color is this?") || !strings.Contains(content, "image attachment omitted") {
		t.Fatalf("content = %q", content)
	}
	encoded, _ := json.Marshal(gotBody)
	if strings.Contains(string(encoded), "image_url") {
		t.Fatalf("mimo-v2.5-pro should not receive image_url parts: %s", encoded)
	}
}

func firstChatMessageContent(t *testing.T, body map[string]any) any {
	t.Helper()
	messages, ok := body["messages"].([]any)
	if !ok || len(messages) == 0 {
		t.Fatalf("messages = %#v", body["messages"])
	}
	message, ok := messages[0].(map[string]any)
	if !ok {
		t.Fatalf("first message = %#v", messages[0])
	}
	return message["content"]
}

func TestOpenAIChatAdapterMapsDeveloperMessagesToSystem(t *testing.T) {
	var gotBody chatCompletionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{
		Model:    "model-a",
		Messages: []ProviderMessage{{Role: "developer", Content: "be precise"}, {Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	_ = collectEvents(stream)

	if len(gotBody.Messages) != 2 || gotBody.Messages[0].Role != "system" || gotBody.Messages[0].Content != "be precise" {
		t.Fatalf("messages = %#v", gotBody.Messages)
	}
}

func TestChatMessagesMergeSystemMessagesAndOmitEmptyAssistantToolContent(t *testing.T) {
	messages := chatMessagesFromProviderRequestWithProfile(ProviderRequest{
		Messages: []ProviderMessage{
			{Role: "system", Content: "root"},
			{Role: "user", Content: "hi"},
			{Role: "developer", Content: "dev"},
			{Role: "assistant", ToolCalls: []ToolCallRecord{{ID: "call_1", Name: "tool", Arguments: "{}"}}},
			{Role: "tool", ToolCallID: "call_1", Content: "ok"},
		},
	}, ProfileForProvider("minimax"))

	if len(messages) != 4 {
		t.Fatalf("messages = %#v", messages)
	}
	if messages[0].Role != "system" || messages[0].Content != "root\n\ndev" {
		t.Fatalf("system message = %#v", messages[0])
	}
	if !messages[2].OmitContent {
		t.Fatalf("assistant tool message should omit empty content: %#v", messages[2])
	}
	raw, err := json.Marshal(messages[2])
	if err != nil {
		t.Fatalf("marshal assistant: %v", err)
	}
	if strings.Contains(string(raw), `"content"`) {
		t.Fatalf("assistant content should be omitted: %s", raw)
	}
}

func TestOpenAIChatAdapterStreamsToolCallDeltas(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"read_file","arguments":"{\"path\":\""}}]}}]}`,
			"",
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"main.go\"}"}}]}}]}`,
			"",
			`data: [DONE]`,
			"",
		}, "\n")))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "read"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	var toolEvents []ProviderEvent
	for _, event := range events {
		if event.Kind == ProviderEventToolCallDelta {
			toolEvents = append(toolEvents, event)
		}
	}
	if len(toolEvents) != 2 {
		t.Fatalf("tool events = %#v", events)
	}
	if got := toolEvents[0].ToolCall; got == nil || got.Index != 0 || got.ID != "call_1" || got.Name != "read_file" || got.ArgumentsDelta != `{"path":"` {
		t.Fatalf("first tool delta = %#v", got)
	}
	if got := toolEvents[1].ToolCall; got == nil || got.Index != 0 || got.ArgumentsDelta != `main.go"}` {
		t.Fatalf("second tool delta = %#v", got)
	}
	if len(events) == 0 || events[len(events)-1].Kind != ProviderEventDone {
		t.Fatalf("events = %#v", events)
	}
}

func TestOpenAIChatAdapterStreamsMixedContentAndToolCallsFromSameChunk(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			": keepalive",
			"",
			`event: completion.chunk`,
			`data: {"choices":[{"delta":{"role":"assistant","content":"I will ","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"read_file","arguments":"{\"path\":\""}}]}}]}`,
			"",
			`data: {"choices":[{"delta":{"role":"assistant","content":"check.","tool_calls":[{"index":0,"function":{"arguments":"main.go\"}"}}]}}]}`,
			"",
			`data: [DONE]`,
			"",
		}, "\n")))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "read"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	if got := eventText(events); got != "I will check." {
		t.Fatalf("text = %q, events = %#v", got, events)
	}
	var args strings.Builder
	var gotName string
	for _, event := range events {
		if event.Kind != ProviderEventToolCallDelta {
			continue
		}
		if event.ToolCall.Name != "" {
			gotName = event.ToolCall.Name
		}
		args.WriteString(event.ToolCall.ArgumentsDelta)
	}
	if gotName != "read_file" || args.String() != `{"path":"main.go"}` {
		t.Fatalf("tool call name=%q args=%q events=%#v", gotName, args.String(), events)
	}
}

func TestOpenAIChatAdapterStreamsInterleavedParallelToolCalls(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_a","function":{"name":"tool_a","arguments":"{\"a\":"}},{"index":1,"id":"call_b","function":{"name":"tool_b","arguments":"{\"b\":"}}]}}]}`,
			"",
			`data: {"choices":[{"delta":{"tool_calls":[{"index":1,"function":{"arguments":"2}"}},{"index":0,"function":{"arguments":"1}"}}]}}]}`,
			"",
			`data: [DONE]`,
			"",
		}, "\n")))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "parallel"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	got := map[int]string{}
	names := map[int]string{}
	for _, event := range events {
		if event.Kind != ProviderEventToolCallDelta {
			continue
		}
		got[event.ToolCall.Index] += event.ToolCall.ArgumentsDelta
		if event.ToolCall.Name != "" {
			names[event.ToolCall.Index] = event.ToolCall.Name
		}
	}
	if names[0] != "tool_a" || got[0] != `{"a":1}` || names[1] != "tool_b" || got[1] != `{"b":2}` {
		t.Fatalf("tool deltas names=%#v args=%#v events=%#v", names, got, events)
	}
}

func TestOpenAIChatAdapterStreamsReasoningDeltas(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"reasoning_content":"think "}}]}`,
			"",
			`data: {"choices":[{"delta":{"content":"answer"}}]}`,
			"",
			`data: [DONE]`,
			"",
		}, "\n")))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	if len(events) < 3 || events[0].Kind != ProviderEventReasoningDelta || events[0].Delta != "think " {
		t.Fatalf("events = %#v", events)
	}
	if got := eventText(events); got != "answer" {
		t.Fatalf("text = %q", got)
	}
}

func TestOpenAIChatAdapterExtractsInlineThinkTags(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"content":"<thi"}}]}`,
			"",
			`data: {"choices":[{"delta":{"content":"nk>hidden</think>vis"}}]}`,
			"",
			`data: {"choices":[{"delta":{"content":"ible"}}]}`,
			"",
			`data: [DONE]`,
			"",
		}, "\n")))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	var reasoning strings.Builder
	for _, event := range events {
		if event.Kind == ProviderEventReasoningDelta {
			reasoning.WriteString(event.Delta)
		}
	}
	if reasoning.String() != "hidden" {
		t.Fatalf("reasoning = %q, events = %#v", reasoning.String(), events)
	}
	if got := eventText(events); got != "visible" {
		t.Fatalf("text = %q, events = %#v", got, events)
	}
}

func TestOpenAIChatAdapterExtractsInlineThinkTagsAcrossClosingBoundary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"content":"<think>thinking"}}]}`,
			"",
			`data: {"choices":[{"delta":{"content":" more</thi"}}]}`,
			"",
			`data: {"choices":[{"delta":{"content":"nk>visible"}}]}`,
			"",
			`data: [DONE]`,
			"",
		}, "\n")))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	var reasoning strings.Builder
	for _, event := range events {
		if event.Kind == ProviderEventReasoningDelta {
			reasoning.WriteString(event.Delta)
		}
	}
	if reasoning.String() != "thinking more" {
		t.Fatalf("reasoning = %q, events = %#v", reasoning.String(), events)
	}
	if got := eventText(events); got != "visible" {
		t.Fatalf("text = %q, events = %#v", got, events)
	}
}

func TestOpenAIChatAdapterExtractsMultipleInlineThinkBlocks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"content":"<think>a</think>mid"}}]}`,
			"",
			`data: {"choices":[{"delta":{"content":"<think>b</think>end"}}]}`,
			"",
			`data: [DONE]`,
			"",
		}, "\n")))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	var reasoning strings.Builder
	for _, event := range events {
		if event.Kind == ProviderEventReasoningDelta {
			reasoning.WriteString(event.Delta)
		}
	}
	if reasoning.String() != "ab" {
		t.Fatalf("reasoning = %q, events = %#v", reasoning.String(), events)
	}
	if got := eventText(events); got != "midend" {
		t.Fatalf("text = %q, events = %#v", got, events)
	}
}

func TestOpenAIChatAdapterReportsUpstreamErrorsAndTruncatedStreams(t *testing.T) {
	t.Run("http error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "bad model", http.StatusBadRequest)
		}))
		defer server.Close()

		adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
		if _, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"}); err == nil || !strings.Contains(err.Error(), "bad model") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("missing done", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte(`data: {"choices":[{"delta":{"content":"partial"}}]}` + "\n\n"))
		}))
		defer server.Close()

		adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
		stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
		if err != nil {
			t.Fatalf("Stream error: %v", err)
		}
		events := collectEvents(stream)
		if len(events) == 0 || events[len(events)-1].Kind != ProviderEventError {
			t.Fatalf("events = %#v", events)
		}
	})

	t.Run("invalid json chunk", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: completion\n"))
			_, _ = w.Write([]byte("data: {\"choices\":\n\n"))
		}))
		defer server.Close()

		adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
		stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
		if err != nil {
			t.Fatalf("Stream error: %v", err)
		}
		events := collectEvents(stream)
		if len(events) == 0 || events[len(events)-1].Kind != ProviderEventError || !strings.Contains(events[len(events)-1].Err.Error(), "invalid chat completion SSE chunk") {
			t.Fatalf("events = %#v", events)
		}
	})
}

func collectEvents(stream <-chan ProviderEvent) []ProviderEvent {
	var events []ProviderEvent
	for event := range stream {
		events = append(events, event)
	}
	return events
}

func eventText(events []ProviderEvent) string {
	var out strings.Builder
	for _, event := range events {
		if event.Kind == ProviderEventTextDelta {
			out.WriteString(event.Delta)
		}
	}
	return out.String()
}
