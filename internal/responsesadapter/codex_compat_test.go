package responsesadapter

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCodexCompatTextSSEMaintainsActiveMessageItem(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: "hel"},
			{Kind: ProviderEventTextDelta, Delta: "lo"},
			{Kind: ProviderEventDone},
		},
	})

	events := streamEventsForCodexCompat(t, facade, `{"model":"model-a","stream":true,"input":"say hello"}`)

	assertCodexCompatibleSSE(t, events)
}

func TestCodexCompatReasoningSSEMaintainsActiveReasoningItem(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventReasoningDelta, Delta: "think"},
			{Kind: ProviderEventTextDelta, Delta: "answer"},
			{Kind: ProviderEventDone},
		},
	})

	events := streamEventsForCodexCompat(t, facade, `{"model":"model-a","stream":true,"input":"reason"}`)

	assertCodexCompatibleSSE(t, events)
}

func TestCodexCompatToolCallSSEMaintainsFunctionCallDeltas(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_read", Name: "read_file"}},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ArgumentsDelta: `{"path":"`}},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ArgumentsDelta: `main.go"}`}},
			{Kind: ProviderEventDone},
		},
	})

	events := streamEventsForCodexCompat(t, facade, `{"model":"model-a","stream":true,"input":"read"}`)

	assertCodexCompatibleSSE(t, events)
}

func TestCodexCompatMixedTextAndToolCallSSE(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: "I will "},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_read", Name: "read_file", ArgumentsDelta: `{"path":"main.go"}`}},
			{Kind: ProviderEventTextDelta, Delta: "check."},
			{Kind: ProviderEventDone},
		},
	})

	events := streamEventsForCodexCompat(t, facade, `{"model":"model-a","stream":true,"input":"read"}`)

	assertCodexCompatibleSSE(t, events)
}

func streamEventsForCodexCompat(t *testing.T, handler http.Handler, body string) []parsedSSEEvent {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	return parseSSEEvents(t, rec.Body.String())
}

func assertCodexCompatibleSSE(t *testing.T, events []parsedSSEEvent) {
	t.Helper()
	activeMessage := false
	activeReasoning := false
	functionCalls := map[string]bool{}
	for _, event := range events {
		switch event.name {
		case "response.output_item.added":
			item := eventItem(t, event)
			itemType := stringField(t, item, "type")
			switch itemType {
			case "message":
				if stringField(t, item, "role") != "assistant" {
					t.Fatalf("message added with wrong role: %#v", item)
				}
				requireArrayField(t, item, "content")
				activeMessage = true
				activeReasoning = false
			case "reasoning":
				requireArrayField(t, item, "summary")
				activeReasoning = true
				activeMessage = false
			case "function_call":
				callID := stringField(t, item, "call_id")
				if callID == "" || stringField(t, item, "name") == "" {
					t.Fatalf("function_call added missing identity: %#v", item)
				}
				if _, ok := item["arguments"].(string); !ok {
					t.Fatalf("function_call added missing string arguments field: %#v", item)
				}
				functionCalls[callID] = true
			default:
				t.Fatalf("unexpected output_item.added type %q: %#v", itemType, item)
			}
		case "response.output_text.delta":
			if !activeMessage {
				t.Fatalf("output_text delta without active message item: %#v", event.data)
			}
		case "response.reasoning_text.delta":
			if !activeReasoning {
				t.Fatalf("%s without active reasoning item: %#v", event.name, event.data)
			}
		case "response.reasoning_summary_part.added", "response.reasoning_summary_text.delta":
			t.Fatalf("raw provider reasoning should not be emitted as visible summary event: %#v", event.data)
		case "response.function_call_arguments.delta":
			itemID := stringField(t, event.data, "item_id")
			if itemID == "" {
				t.Fatalf("function_call_arguments.delta missing item_id: %#v", event.data)
			}
		case "response.output_item.done":
			item := eventItem(t, event)
			switch stringField(t, item, "type") {
			case "message":
				requireArrayField(t, item, "content")
				activeMessage = false
			case "reasoning":
				requireArrayField(t, item, "summary")
				activeReasoning = false
			case "function_call":
				callID := stringField(t, item, "call_id")
				if !functionCalls[callID] {
					t.Fatalf("function_call done without added call %q: %#v", callID, item)
				}
			}
		}
	}
}

func eventItem(t *testing.T, event parsedSSEEvent) map[string]any {
	t.Helper()
	item, ok := event.data["item"].(map[string]any)
	if !ok {
		t.Fatalf("%s missing item: %#v", event.name, event.data)
	}
	if _, err := json.Marshal(item); err != nil {
		t.Fatalf("%s item is not JSON serializable: %v", event.name, err)
	}
	return item
}

func requireArrayField(t *testing.T, item map[string]any, field string) []any {
	t.Helper()
	values, ok := item[field].([]any)
	if !ok {
		t.Fatalf("item missing array field %q: %#v", field, item)
	}
	return values
}

func stringField(t *testing.T, value map[string]any, field string) string {
	t.Helper()
	got, ok := value[field].(string)
	if !ok {
		return ""
	}
	return got
}
