package responsesadapter

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFacadeLongToolLoopHistoryRoundTrip(t *testing.T) {
	adapter := &recordingAdapter{events: []ProviderEvent{
		{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_a", Name: "read_file", ArgumentsDelta: `{"path":"a.go"}`}},
		{Kind: ProviderEventDone},
	}}
	facade := newTestFacade(NewMemoryStore(), adapter)

	doReq := func(body string) {
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
		req.Header.Set("x-codex-thread-id", "thread-long-tool")
		rec := httptest.NewRecorder()
		facade.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %s status = %d, body = %s", body, rec.Code, rec.Body.String())
		}
	}

	doReq(`{"model":"model-a","input":"read a"}`)

	adapter.events = []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "saw a"}, {Kind: ProviderEventDone}}
	doReq(`{"model":"model-a","previous_response_id":"resp_001","input":[{"type":"function_call_output","call_id":"call_a","output":"contents a"}]}`)

	adapter.events = []ProviderEvent{
		{Kind: ProviderEventTextDelta, Delta: "checking b"},
		{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_b", Name: "read_file", ArgumentsDelta: `{"path":"b.go"}`}},
		{Kind: ProviderEventDone},
	}
	doReq(`{"model":"model-a","previous_response_id":"resp_002","input":"read b"}`)

	adapter.events = []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "final"}, {Kind: ProviderEventDone}}
	doReq(`{"model":"model-a","previous_response_id":"resp_003","input":[{"type":"function_call_output","call_id":"call_b","output":"contents b"}]}`)

	if len(adapter.requests) != 4 {
		t.Fatalf("requests = %d", len(adapter.requests))
	}
	messages := adapter.requests[3].Messages
	got := summarizeMessages(messages)
	want := []string{
		"user:read a",
		"assistant:call_a:read_file:{\"path\":\"a.go\"}",
		"tool:call_a:contents a",
		"assistant:saw a",
		"user:read b",
		"assistant:checking b:call_b:read_file:{\"path\":\"b.go\"}",
		"tool:call_b:contents b",
	}
	if len(got) != len(want) {
		t.Fatalf("messages = %#v\nsummary = %#v", messages, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("message %d = %q, want %q\nall: %#v", i, got[i], want[i], got)
		}
	}
}

func TestFacadeToolArgumentsBeforeNameArePreserved(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ArgumentsDelta: `{"path":"`}},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_late", Name: "read_file"}},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ArgumentsDelta: `main.go"}`}},
			{Kind: ProviderEventDone},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"read"}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"call_id":"call_late"`) || !strings.Contains(rec.Body.String(), `"arguments":"{\"path\":\"main.go\"}"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func summarizeMessages(messages []ProviderMessage) []string {
	out := make([]string, 0, len(messages))
	for _, message := range messages {
		switch message.Role {
		case "assistant":
			parts := []string{"assistant"}
			if strings.TrimSpace(message.Content) != "" {
				parts = append(parts, message.Content)
			}
			for _, call := range message.ToolCalls {
				parts = append(parts, fmt.Sprintf("%s:%s:%s", call.ID, call.Name, call.Arguments))
			}
			out = append(out, strings.Join(parts, ":"))
		case "tool":
			out = append(out, fmt.Sprintf("tool:%s:%s", message.ToolCallID, message.Content))
		default:
			out = append(out, fmt.Sprintf("%s:%s", message.Role, message.Content))
		}
	}
	return out
}
