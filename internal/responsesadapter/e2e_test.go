package responsesadapter

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestFacadeOpenAIChatAdapterCodexLikeToolRoundTrip(t *testing.T) {
	var mu sync.Mutex
	var requests []chatCompletionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		var body chatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		mu.Lock()
		requests = append(requests, body)
		call := len(requests)
		mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		switch call {
		case 1:
			_, _ = w.Write([]byte(strings.Join([]string{
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_read","type":"function","function":{"name":"read_file","arguments":"{\"path\":\""}}]}}]}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"main.go\"}"}}]}}]}`,
				"",
				`data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":4,"total_tokens":9}}`,
				"",
				`data: [DONE]`,
				"",
			}, "\n")))
		case 2:
			_, _ = w.Write([]byte(strings.Join([]string{
				`data: {"choices":[{"delta":{"content":"final answer"}}]}`,
				"",
				`data: [DONE]`,
				"",
			}, "\n")))
		default:
			t.Fatalf("unexpected upstream call %d", call)
		}
	}))
	defer server.Close()

	facade := newTestFacade(NewMemoryStore(), OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()})
	first := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model":"model-a",
		"instructions":"base instructions",
		"input":"read main",
		"tools":[{"type":"function","name":"read_file","parameters":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}}],
		"tool_choice":"auto",
		"parallel_tool_calls":true,
		"reasoning":{"effort":"medium"},
		"store":false,
		"include":["reasoning.encrypted_content"],
		"service_tier":"auto",
		"prompt_cache_key":"cache-key",
		"text":{"verbosity":"low"}
	}`))
	first.Header.Set("x-codex-thread-id", "thread-e2e")
	firstRec := httptest.NewRecorder()
	facade.ServeHTTP(firstRec, first)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}
	var firstBody responseObject
	if err := json.Unmarshal(firstRec.Body.Bytes(), &firstBody); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	if len(firstBody.Output) != 1 || firstBody.Output[0].Type != "function_call" || firstBody.Output[0].CallID != "call_read" || firstBody.Output[0].Arguments != `{"path":"main.go"}` {
		t.Fatalf("first output = %#v", firstBody.Output)
	}
	if firstBody.Usage == nil || firstBody.Usage.TotalTokens != 9 {
		t.Fatalf("first usage = %#v", firstBody.Usage)
	}

	second := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model":"model-a",
		"instructions":"base instructions",
		"previous_response_id":"resp_001",
		"input":[{"type":"function_call_output","call_id":"call_read","output":"file contents"}]
	}`))
	second.Header.Set("x-codex-thread-id", "thread-e2e")
	secondRec := httptest.NewRecorder()
	facade.ServeHTTP(secondRec, second)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second status = %d, body = %s", secondRec.Code, secondRec.Body.String())
	}
	var secondBody responseObject
	if err := json.Unmarshal(secondRec.Body.Bytes(), &secondBody); err != nil {
		t.Fatalf("decode second response: %v", err)
	}
	if secondBody.OutputText != "final answer" || secondBody.PreviousResponseID != "resp_001" {
		t.Fatalf("second response = %#v", secondBody)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("upstream requests = %d", len(requests))
	}
	firstUpstream := requests[0]
	if len(firstUpstream.Messages) != 2 || firstUpstream.Messages[0].Role != "system" || firstUpstream.Messages[0].Content != "base instructions" || firstUpstream.Messages[1].Content != "read main" {
		t.Fatalf("first upstream messages = %#v", firstUpstream.Messages)
	}
	if len(firstUpstream.Tools) != 1 || firstUpstream.Tools[0].Function.Name != "read_file" {
		t.Fatalf("first upstream tools = %#v", firstUpstream.Tools)
	}
	if string(firstUpstream.ToolChoice) != `"auto"` || firstUpstream.ParallelToolCalls == nil || !*firstUpstream.ParallelToolCalls {
		t.Fatalf("first upstream controls: tool_choice=%s parallel=%#v", firstUpstream.ToolChoice, firstUpstream.ParallelToolCalls)
	}

	secondUpstream := requests[1]
	if len(secondUpstream.Messages) != 4 {
		t.Fatalf("second upstream messages = %#v", secondUpstream.Messages)
	}
	if secondUpstream.Messages[0].Role != "system" || secondUpstream.Messages[1].Role != "user" || secondUpstream.Messages[2].Role != "assistant" || secondUpstream.Messages[3].Role != "tool" {
		t.Fatalf("second upstream roles = %#v", secondUpstream.Messages)
	}
	if len(secondUpstream.Messages[2].ToolCalls) != 1 || secondUpstream.Messages[2].ToolCalls[0].ID != "call_read" || secondUpstream.Messages[3].ToolCallID != "call_read" || secondUpstream.Messages[3].Content != "file contents" {
		t.Fatalf("second upstream tool transcript = %#v", secondUpstream.Messages)
	}
}
