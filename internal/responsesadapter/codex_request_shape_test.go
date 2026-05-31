package responsesadapter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFacadeAcceptsCodexFullResponsesRequestShape(t *testing.T) {
	parallel := true
	toolChoice := json.RawMessage(`"auto"`)
	adapter := &recordingAdapter{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "ok"}, {Kind: ProviderEventDone}}}
	facade := newTestFacade(NewMemoryStore(), adapter)

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model":"model-a",
		"instructions":"base instructions",
		"input":[
			{"type":"message","role":"developer","content":[{"type":"input_text","text":"developer note"}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}
		],
		"tools":[{"type":"function","name":"read_file","description":"Read a file","parameters":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]},"strict":true}],
		"tool_choice":"auto",
		"parallel_tool_calls":true,
		"max_output_tokens":2048,
		"reasoning":{"effort":"medium","summary":"auto"},
		"store":false,
		"stream":false,
		"include":["reasoning.encrypted_content"],
		"prompt_cache_key":"cache-key",
		"service_tier":"auto",
		"text":{"verbosity":"low","format":{"type":"json_schema","name":"codex_output_schema","schema":{"type":"object"},"strict":false}},
		"client_metadata":{"traceparent":"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"}
	}`))
	req.Header.Set("x-codex-thread-id", "thread-shape")
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(adapter.requests) != 1 {
		t.Fatalf("adapter requests = %d", len(adapter.requests))
	}
	got := adapter.requests[0]
	if got.Instructions != "base instructions" {
		t.Fatalf("instructions = %q", got.Instructions)
	}
	if string(got.ToolChoice) != string(toolChoice) {
		t.Fatalf("tool_choice = %s", got.ToolChoice)
	}
	if got.ParallelToolCalls == nil || *got.ParallelToolCalls != parallel {
		t.Fatalf("parallel_tool_calls = %#v", got.ParallelToolCalls)
	}
	if got.MaxOutputTokens == nil || *got.MaxOutputTokens != 2048 {
		t.Fatalf("max_output_tokens = %#v", got.MaxOutputTokens)
	}
	if got.ReasoningEffort != "medium" {
		t.Fatalf("reasoning effort = %q", got.ReasoningEffort)
	}
	if len(got.Messages) != 2 || got.Messages[0].Role != "developer" || got.Messages[1].Role != "user" {
		t.Fatalf("messages = %#v", got.Messages)
	}
	if len(got.Tools) != 1 || got.Tools[0].Function.Name != "read_file" || got.Tools[0].Function.Strict == nil || !*got.Tools[0].Function.Strict {
		t.Fatalf("tools = %#v", got.Tools)
	}
}

func TestFacadeAcceptsCodexBridgeInputShapeMatrix(t *testing.T) {
	for _, tc := range []struct {
		name      string
		body      string
		wantText  string
		wantRoles []string
	}{
		{name: "model only probe", body: `{"model":"model-a"}`, wantText: ""},
		{name: "empty string probe", body: `{"model":"model-a","input":""}`, wantText: ""},
		{name: "empty array probe", body: `{"model":"model-a","input":[]}`, wantText: ""},
		{name: "instructions only", body: `{"model":"model-a","instructions":"system only"}`, wantText: ""},
		{name: "string input", body: `{"model":"model-a","input":"reply pong"}`, wantText: "reply pong", wantRoles: []string{"user"}},
		{name: "role content array", body: `{"model":"model-a","input":[{"role":"user","content":"reply pong"}]}`, wantText: "reply pong", wantRoles: []string{"user"}},
		{name: "message input_text array", body: `{"model":"model-a","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"reply pong"}]}]}`, wantText: "reply pong", wantRoles: []string{"user"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adapter := &recordingAdapter{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "ok"}, {Kind: ProviderEventDone}}}
			facade := newTestFacade(NewMemoryStore(), adapter)
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			facade.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}
			if len(adapter.requests) != 1 {
				t.Fatalf("requests = %d", len(adapter.requests))
			}
			got := adapter.requests[0]
			if got.InputText != tc.wantText {
				t.Fatalf("input text = %q, want %q", got.InputText, tc.wantText)
			}
			if len(got.Messages) != len(tc.wantRoles) {
				t.Fatalf("messages = %#v", got.Messages)
			}
			for i, role := range tc.wantRoles {
				if got.Messages[i].Role != role {
					t.Fatalf("message %d = %#v", i, got.Messages[i])
				}
			}
		})
	}
}

func TestOpenAIChatAdapterOmitsUnsetCodexControls(t *testing.T) {
	var raw map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "hello"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	_ = collectEvents(stream)

	for _, key := range []string{"tool_choice", "parallel_tool_calls"} {
		if _, ok := raw[key]; ok {
			t.Fatalf("%s should be omitted from request body: %#v", key, raw)
		}
	}
}
