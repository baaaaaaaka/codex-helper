package responsesadapter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func collectProviderEvents(t *testing.T, stream <-chan ProviderEvent) []ProviderEvent {
	t.Helper()
	var events []ProviderEvent
	for event := range stream {
		events = append(events, event)
		if event.Kind == ProviderEventError {
			if event.Err != nil {
				t.Fatalf("provider error: %v", event.Err)
			}
		}
	}
	return events
}

func TestAnthropicConverterBuildsMessagesRequestAndParsesBufferedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/messages" {
			t.Errorf("path = %s, want /messages", r.URL.Path)
		}
		if got := r.Header.Get("x-api-key"); got != "test-key" {
			t.Errorf("x-api-key = %q", got)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if body["model"] != "deepseek-v4" || body["system"] != "be precise" {
			t.Errorf("request identity = %#v", body)
		}
		messages, _ := body["messages"].([]any)
		if len(messages) != 1 {
			t.Errorf("messages = %#v", messages)
		}
		tools, _ := body["tools"].([]any)
		if len(tools) != 1 {
			t.Errorf("tools = %#v", tools)
		}
		if _, ok := body["thinking"]; !ok {
			t.Errorf("reasoning effort was not converted: %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"msg_1","content":[{"type":"thinking","thinking":"internal"},{"type":"text","text":"answer"}],"stop_reason":"end_turn","usage":{"input_tokens":12,"output_tokens":4}}`)
	}))
	defer server.Close()
	adapter := AnthropicAdapter{BaseURL: server.URL, APIKey: "test-key", AuthType: "header", AuthHeader: "x-api-key", StreamMode: "nonstream-buffered", HTTPClient: server.Client()}
	effort := "high"
	stream, err := adapter.Stream(context.Background(), ProviderRequest{
		Model: "deepseek-v4", Instructions: "be precise", InputText: "hello", ReasoningEffort: effort,
		Tools: []ChatTool{{Type: "function", Function: ChatFunction{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	events := collectProviderEvents(t, stream)
	var text, reasoning string
	var usage *Usage
	var done bool
	for _, event := range events {
		switch event.Kind {
		case ProviderEventTextDelta:
			text += event.Delta
		case ProviderEventReasoningDelta:
			reasoning += event.Delta
		case ProviderEventUsage:
			usage = event.Usage
		case ProviderEventDone:
			done = true
		}
	}
	if text != "answer" || reasoning != "internal" || !done || usage == nil || usage.InputTokens != 12 || usage.OutputTokens != 4 {
		t.Fatalf("events = %#v", events)
	}
}

func TestAnthropicConverterRejectsUnsupportedResponseFormat(t *testing.T) {
	adapter := AnthropicAdapter{BaseURL: "https://example.invalid", APIKey: "key", AuthType: "header", AuthHeader: "x-api-key"}
	if _, err := adapter.Stream(context.Background(), ProviderRequest{Model: "deepseek-v4", InputText: "json", ResponseFormat: json.RawMessage(`{"type":"json_object"}`)}); err == nil || !strings.Contains(err.Error(), "does not support response_format") {
		t.Fatalf("Anthropic response_format was silently accepted: %v", err)
	}
}

func TestAnthropicConverterParsesSSEWithoutDuplicateDone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_start\ndata: {\"message\":{\"usage\":{\"input_tokens\":3}}}\n\n")
		_, _ = io.WriteString(w, "event: content_block_start\ndata: {\"index\":1,\"content_block\":{\"type\":\"server_tool_use\",\"id\":\"server-1\",\"name\":\"web_search\"}}\n\n")
		_, _ = io.WriteString(w, "event: content_block_delta\ndata: {\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"q\\\":\\\"ignored\\\"}\"}}\n\n")
		_, _ = io.WriteString(w, "event: content_block_start\ndata: {\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call-1\",\"name\":\"lookup\"}}\n\n")
		_, _ = io.WriteString(w, "event: content_block_delta\ndata: {\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"q\\\":\\\"x\\\"}\"}}\n\n")
		_, _ = io.WriteString(w, "event: message_delta\ndata: {\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":2}}\n\n")
		_, _ = io.WriteString(w, "event: message_stop\ndata: {}\n\n")
	}))
	defer server.Close()
	adapter := AnthropicAdapter{BaseURL: server.URL, APIKey: "key", AuthType: "header", AuthHeader: "x-api-key", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "m", InputText: "x"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectProviderEvents(t, stream)
	doneCount := 0
	var call *ProviderToolCallDelta
	callCount := 0
	for _, event := range events {
		if event.Kind == ProviderEventDone {
			doneCount++
		}
		if event.Kind == ProviderEventToolCallDelta && event.ToolCall != nil {
			callCount++
			if call == nil {
				call = &ProviderToolCallDelta{}
			}
			call.Index = event.ToolCall.Index
			call.ID += event.ToolCall.ID
			call.Name += event.ToolCall.Name
			call.ArgumentsDelta += event.ToolCall.ArgumentsDelta
		}
	}
	if doneCount != 1 || callCount != 2 || call == nil || call.Index != 2 || call.ID != "call-1" || call.Name != "lookup" || !strings.Contains(call.ArgumentsDelta, `"q"`) || strings.Contains(call.ArgumentsDelta, "ignored") {
		t.Fatalf("events = %#v", events)
	}
}

func TestNativeToolMappingForwardsMiMoWebSearchAndPreservesSources(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		tools, _ := body["tools"].([]any)
		if len(tools) != 1 {
			t.Errorf("tools = %#v", tools)
		} else if tool, ok := tools[0].(map[string]any); !ok || tool["type"] != "web_search" {
			t.Errorf("native tool = %#v", tools[0])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"answer","annotations":[{"type":"url_citation","url":"https://example.test/source","title":"Source"}]},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)
	}))
	defer server.Close()
	adapter := OpenAIChatAdapter{BaseURL: server.URL, APIKey: "key", HTTPClient: server.Client(), StreamMode: "nonstream-buffered", Profile: ProfileForProvider("mimo")}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{
		Model: "mimo-v2.5", InputText: "search", NativeTools: []ProviderNativeTool{{InputType: "web_search_preview", UpstreamType: "web_search"}},
		SourcePolicy: SourcePolicy{Mode: "annotations", RequireURL: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	events := collectProviderEvents(t, stream)
	found := false
	for _, event := range events {
		if event.Kind == ProviderEventSource && event.Source != nil && event.Source.URL == "https://example.test/source" {
			found = true
		}
	}
	if !found {
		t.Fatalf("events = %#v", events)
	}
}

func TestAnthropicNativeSearchSourcesBecomeTypedEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"content":[{"type":"server_tool_use","name":"web_search"},{"type":"web_search_tool_result","content":[{"type":"web_search_result","title":"Docs","url":"https://example.test/docs"}]},{"type":"text","text":"answer"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer server.Close()
	adapter := AnthropicAdapter{BaseURL: server.URL, APIKey: "key", AuthType: "header", AuthHeader: "x-api-key", HTTPClient: server.Client(), StreamMode: "nonstream-buffered"}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "deepseek-v4", InputText: "search", SourcePolicy: SourcePolicy{Mode: "annotations", RequireURL: true}})
	if err != nil {
		t.Fatal(err)
	}
	events := collectProviderEvents(t, stream)
	for _, event := range events {
		if event.Kind == ProviderEventSource && event.Source != nil && event.Source.URL == "https://example.test/docs" && event.Source.Title == "Docs" {
			return
		}
	}
	t.Fatalf("events = %#v", events)
}

func TestSourcePolicyRejectsUnsupportedOrUnattributedSearchResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"answer","annotations":[{"type":"url_citation"}]},"finish_reason":"stop"}]}`)
	}))
	defer server.Close()
	adapter := OpenAIChatAdapter{BaseURL: server.URL, APIKey: "key", HTTPClient: server.Client(), StreamMode: "nonstream-buffered"}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "mimo-v2.5", InputText: "search", SourcePolicy: SourcePolicy{Mode: "annotations", RequireURL: true}})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for event := range stream {
		if event.Kind == ProviderEventError && event.Err != nil && strings.Contains(event.Err.Error(), "without a URL") {
			found = true
		}
	}
	if !found {
		t.Fatal("missing source attribution error")
	}

	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"answer","annotations":[{"type":"url_citation","url":"https://example.test/source"}]},"finish_reason":"stop"}]}`)
	})
	stream, err = adapter.Stream(context.Background(), ProviderRequest{Model: "mimo-v2.5", InputText: "search", SourcePolicy: SourcePolicy{Mode: "unsupported"}})
	if err != nil {
		t.Fatal(err)
	}
	found = false
	for event := range stream {
		if event.Kind == ProviderEventError && event.Err != nil && strings.Contains(event.Err.Error(), "marks sources unsupported") {
			found = true
		}
	}
	if !found {
		t.Fatal("missing unsupported source policy error")
	}
}

func TestChatSourceTextPolicyPreservesSearchResultText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"answer","annotations":[{"type":"web_search_result","title":"Docs","snippet":"source excerpt"}]},"finish_reason":"stop"}]}`)
	}))
	defer server.Close()
	adapter := OpenAIChatAdapter{BaseURL: server.URL, APIKey: "key", HTTPClient: server.Client(), StreamMode: "nonstream-buffered"}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "mimo-v2.5", InputText: "search", SourcePolicy: SourcePolicy{Mode: "text"}})
	if err != nil {
		t.Fatal(err)
	}
	var text string
	for event := range stream {
		if event.Kind == ProviderEventTextDelta {
			text += event.Delta
		}
		if event.Kind == ProviderEventError {
			t.Fatalf("provider error: %v", event.Err)
		}
	}
	if !strings.Contains(text, "answer") || !strings.Contains(text, "source excerpt") {
		t.Fatalf("text mode lost source text: %q", text)
	}
}

func TestDeepSeekBetaConverterUsesFIMWireShape(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/completions" {
			t.Errorf("path = %s, want /completions", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if body["prompt"] != "prefix" || body["suffix"] != "suffix" || body["model"] != "deepseek-v4-pro" {
			t.Errorf("FIM request = %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"text":"middle","finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)
	}))
	defer server.Close()
	adapter, err := NewConfiguredAdapter(AdapterOptions{AdapterID: "deepseek-beta", ConversionProfile: "deepseek-beta-v1", BaseURL: server.URL, APIKey: "key", HTTP: config.ModelHTTPPolicy{TimeoutSeconds: 5}, Stream: config.ModelStreamPolicy{UpstreamMode: "nonstream-buffered"}})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "deepseek-v4-pro", Operation: "fim", Prefix: "prefix", Suffix: "suffix"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectProviderEvents(t, stream)
	if len(events) < 2 || events[0].Kind != ProviderEventTextDelta || events[0].Delta != "middle" {
		t.Fatalf("events = %#v", events)
	}
}

func TestDeepSeekBetaFIMRejectsUnsupportedResponseFormat(t *testing.T) {
	adapter, err := NewConfiguredAdapter(AdapterOptions{AdapterID: "deepseek-beta", ConversionProfile: "deepseek-beta-v1", BaseURL: "https://example.invalid", APIKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Stream(context.Background(), ProviderRequest{Model: "deepseek-v4", Operation: "fim", Prefix: "prefix", ResponseFormat: json.RawMessage(`{"type":"json_object"}`)}); err == nil || !strings.Contains(err.Error(), "does not support response_format") {
		t.Fatalf("DeepSeek Beta FIM response_format was silently accepted: %v", err)
	}
}

func TestConfiguredAdapterRejectsUnknownConversionProfile(t *testing.T) {
	if _, err := NewConfiguredAdapter(AdapterOptions{AdapterID: "custom", ConversionProfile: "user-template"}); err == nil || !strings.Contains(err.Error(), "unsupported wire conversion profile") {
		t.Fatalf("unknown conversion profile accepted: %v", err)
	}
}

func TestConfiguredAdapterRejectsMismatchedConversionProfile(t *testing.T) {
	if _, err := NewConfiguredAdapter(AdapterOptions{AdapterID: "openai-chat", ConversionProfile: "deepseek-anthropic-v1"}); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("mismatched conversion profile accepted: %v", err)
	}
}

func TestDeepSeekBetaFIMSSERequiresDoneMarker(t *testing.T) {
	out := make(chan ProviderEvent, 4)
	parseDeepSeekFIMSSE(context.Background(), strings.NewReader("data: {\"choices\":[{\"delta\":{\"text\":\"partial\"}}]}\n\n"), out)
	close(out)
	var events []ProviderEvent
	for event := range out {
		events = append(events, event)
	}
	if len(events) != 2 || events[0].Kind != ProviderEventTextDelta || events[1].Kind != ProviderEventError || !strings.Contains(events[1].Err.Error(), "before [DONE]") {
		t.Fatalf("events = %#v", events)
	}
}
