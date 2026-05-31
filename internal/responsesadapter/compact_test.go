package responsesadapter

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFacadeCompactEndpointReturnsCompactedHistory(t *testing.T) {
	adapter := &recordingAdapter{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "summary"}, {Kind: ProviderEventDone}}}
	facade := newTestFacade(NewMemoryStore(), adapter)

	req := httptest.NewRequest(http.MethodPost, "/v1/responses/compact", strings.NewReader(`{
		"model":"model-a",
		"instructions":"compact this",
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hi"}]}
		]
	}`))
	req.Header.Set("x-codex-thread-id", "thread-compact")
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body compactResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Output) != 1 || body.Output[0].Type != "message" || body.Output[0].Role != "user" {
		t.Fatalf("body = %#v", body)
	}
	if len(body.Output[0].Content) != 1 || body.Output[0].Content[0].Type != "input_text" || body.Output[0].Content[0].Text != "summary" {
		t.Fatalf("content = %#v", body.Output[0].Content)
	}
	if len(adapter.requests) != 1 {
		t.Fatalf("requests = %d", len(adapter.requests))
	}
	got := adapter.requests[0]
	if got.Scope.Thread != "thread-compact" || got.Model != "model-a" {
		t.Fatalf("provider request = %#v", got)
	}
	if len(got.Messages) != 2 || got.Messages[0].Role != "system" || got.Messages[0].Content != "compact this" {
		t.Fatalf("provider messages = %#v", got.Messages)
	}
	if !strings.Contains(got.Messages[1].Content, "user: hello") || !strings.Contains(got.Messages[1].Content, "assistant: hi") {
		t.Fatalf("compact transcript = %#v", got.Messages[1])
	}
}

func TestFacadeCompactEndpointIncludesReasoningToolCallsAndToolOutputs(t *testing.T) {
	adapter := &recordingAdapter{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "summary"}, {Kind: ProviderEventDone}}}
	facade := newTestFacade(NewMemoryStore(), adapter)

	req := httptest.NewRequest(http.MethodPost, "/v1/responses/compact", strings.NewReader(`{
		"model":"model-a",
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"inspect"}]},
			{"type":"reasoning","summary":[{"type":"summary_text","text":"need the file"}]},
			{"type":"function_call","call_id":"call_read","name":"read_file","arguments":"{\"path\":\"main.go\"}"},
			{"type":"function_call_output","call_id":"call_read","output":"file contents"}
		]
	}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(adapter.requests) != 1 {
		t.Fatalf("requests = %d", len(adapter.requests))
	}
	transcript := adapter.requests[0].Messages[1].Content
	for _, want := range []string{
		"user: inspect",
		"assistant reasoning: need the file",
		`assistant tool_call call_read read_file: {"path":"main.go"}`,
		"tool call_read: file contents",
	} {
		if !strings.Contains(transcript, want) {
			t.Fatalf("compact transcript missing %q:\n%s", want, transcript)
		}
	}
}

func TestFacadeCompactSummaryCanBeUsedAsReplacementInput(t *testing.T) {
	adapter := &recordingAdapter{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "compact summary"}, {Kind: ProviderEventDone}}}
	facade := newTestFacade(NewMemoryStore(), adapter)

	compactReq := httptest.NewRequest(http.MethodPost, "/v1/responses/compact", strings.NewReader(`{
		"model":"model-a",
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"secret old detail"}]},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"old answer"}]}
		]
	}`))
	compactRec := httptest.NewRecorder()
	facade.ServeHTTP(compactRec, compactReq)
	if compactRec.Code != http.StatusOK {
		t.Fatalf("compact status = %d, body = %s", compactRec.Code, compactRec.Body.String())
	}
	var compacted compactResponse
	if err := json.Unmarshal(compactRec.Body.Bytes(), &compacted); err != nil {
		t.Fatalf("decode compact response: %v", err)
	}
	if len(compacted.Output) != 1 {
		t.Fatalf("compacted output = %#v", compacted.Output)
	}

	replacementInput, err := json.Marshal([]any{
		compacted.Output[0],
		map[string]any{"type": "message", "role": "user", "content": []map[string]string{{"type": "input_text", "text": "new question"}}},
	})
	if err != nil {
		t.Fatalf("marshal replacement input: %v", err)
	}
	adapter.events = []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "answer"}, {Kind: ProviderEventDone}}
	responseReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":`+string(replacementInput)+`}`))
	responseRec := httptest.NewRecorder()
	facade.ServeHTTP(responseRec, responseReq)
	if responseRec.Code != http.StatusOK {
		t.Fatalf("response status = %d, body = %s", responseRec.Code, responseRec.Body.String())
	}
	if len(adapter.requests) != 2 {
		t.Fatalf("requests = %d", len(adapter.requests))
	}
	messages := adapter.requests[1].Messages
	if len(messages) != 2 || messages[0].Content != "compact summary" || messages[1].Content != "new question" {
		t.Fatalf("replacement messages = %#v", messages)
	}
	for _, message := range messages {
		if strings.Contains(message.Content, "secret old detail") || strings.Contains(message.Content, "old answer") {
			t.Fatalf("old history leaked into replacement messages: %#v", messages)
		}
	}
}

func TestFacadeCompactEndpointRejectsBadInput(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{})

	req := httptest.NewRequest(http.MethodPost, "/responses/compact", strings.NewReader(`{"model":"model-a","input":{}}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}
