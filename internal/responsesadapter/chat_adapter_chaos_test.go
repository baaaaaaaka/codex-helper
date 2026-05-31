package responsesadapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAIChatAdapterStopsAfterInvalidChunk(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {bad json}\n\n"))
		_, _ = w.Write([]byte(`data: {"choices":[{"delta":{"content":"after"}}]}` + "\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	if len(events) != 1 || events[0].Kind != ProviderEventError {
		t.Fatalf("events = %#v", events)
	}
	if got := eventText(events); got != "" {
		t.Fatalf("text after invalid chunk should not be emitted, got %q", got)
	}
}

func TestOpenAIChatAdapterHandlesDoneWithWhitespace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
		_, _ = w.Write([]byte("data:   [DONE]   \n\n"))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	if got := eventText(events); got != "ok" {
		t.Fatalf("text = %q, events = %#v", got, events)
	}
	if len(events) == 0 || events[len(events)-1].Kind != ProviderEventDone {
		t.Fatalf("events = %#v", events)
	}
}

func TestOpenAIChatAdapterParsesToolArgumentsBeforeIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"path\":\""}}]}}]}`,
			"",
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_late","function":{"name":"read_file","arguments":"main.go\"}"}}]}}]}`,
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
	var args strings.Builder
	var id, name string
	for _, event := range events {
		if event.Kind != ProviderEventToolCallDelta {
			continue
		}
		if event.ToolCall.ID != "" {
			id = event.ToolCall.ID
		}
		if event.ToolCall.Name != "" {
			name = event.ToolCall.Name
		}
		args.WriteString(event.ToolCall.ArgumentsDelta)
	}
	if id != "call_late" || name != "read_file" || args.String() != `{"path":"main.go"}` {
		t.Fatalf("tool call id=%q name=%q args=%q events=%#v", id, name, args.String(), events)
	}
}

func TestOpenAIChatAdapterHandlesLargeSSETextChunk(t *testing.T) {
	large := strings.Repeat("x", 256*1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"choices":[{"delta":{"content":"` + large + `"}}]}` + "\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	if got := eventText(events); len(got) != len(large) || got != large {
		t.Fatalf("large text len = %d, want %d", len(got), len(large))
	}
}
