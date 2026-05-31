package responsesadapter

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestFacadeReplaysCachedReasoningForCodexFullReplay(t *testing.T) {
	store := NewMemoryStore()
	adapter := &recordingAdapter{events: []ProviderEvent{
		{Kind: ProviderEventReasoningDelta, Delta: "inspect first"},
		{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_ls", Name: "exec_command", ArgumentsDelta: `{"cmd":"ls"}`}},
		{Kind: ProviderEventDone},
	}}
	facade := newTestFacade(store, adapter)

	first := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"run ls"}`))
	first.Header.Set("x-codex-thread-id", "thread-replay")
	firstRec := httptest.NewRecorder()
	facade.ServeHTTP(firstRec, first)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first status = %d body = %s", firstRec.Code, firstRec.Body.String())
	}

	adapter.events = []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "done"}, {Kind: ProviderEventDone}}
	second := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model":"model-a",
		"input":[
			{"type":"function_call","call_id":"call_ls","name":"exec_command","arguments":"{\"cmd\":\"ls\"}"},
			{"type":"function_call_output","call_id":"call_ls","output":"main.go"},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"continue"}]}
		]
	}`))
	second.Header.Set("x-codex-thread-id", "thread-replay")
	secondRec := httptest.NewRecorder()
	facade.ServeHTTP(secondRec, second)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second status = %d body = %s", secondRec.Code, secondRec.Body.String())
	}
	if len(adapter.requests) != 2 {
		t.Fatalf("requests = %d", len(adapter.requests))
	}
	got := adapter.requests[1].Messages[0]
	if got.Role != "assistant" || got.ReasoningContent != "inspect first" || len(got.ToolCalls) != 1 || got.ToolCalls[0].ID != "call_ls" {
		t.Fatalf("replayed assistant message = %#v", got)
	}
}

func TestSQLiteReasoningCachePersistsAndStaysScoped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "responses.sqlite")
	scope := Scope{Tenant: "tenant", User: "user", Provider: "deepseek", Model: "deepseek-v4-flash", Thread: "thread", Branch: "main", KeyFingerprint: "key:a"}
	store, err := NewSQLiteStore(path, SQLiteStoreOptions{})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	if err := store.Store(ResponseRecord{
		ID:            "resp_reason",
		Scope:         scope,
		OutputText:    "I inspected it.",
		ReasoningText: "need workspace",
		ToolCalls:     []ToolCallRecord{{ID: "call_read", Name: "read_file", Arguments: `{}`}},
		Status:        ResponseStatusCompleted,
	}); err != nil {
		t.Fatalf("store reasoning record: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := NewSQLiteStore(path, SQLiteStoreOptions{})
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer reopened.Close()

	reasoning, err := reopened.LookupReasoning(scope, ProviderMessage{Role: "assistant", ToolCalls: []ToolCallRecord{{ID: "call_read"}}})
	if err != nil {
		t.Fatalf("lookup call reasoning: %v", err)
	}
	if reasoning != "need workspace" {
		t.Fatalf("call reasoning = %q", reasoning)
	}
	reasoning, err = reopened.LookupReasoning(scope, ProviderMessage{Role: "assistant", Content: "I inspected it."})
	if err != nil {
		t.Fatalf("lookup content reasoning: %v", err)
	}
	if reasoning != "need workspace" {
		t.Fatalf("content reasoning = %q", reasoning)
	}
	scope.KeyFingerprint = "key:b"
	reasoning, err = reopened.LookupReasoning(scope, ProviderMessage{Role: "assistant", ToolCalls: []ToolCallRecord{{ID: "call_read"}}})
	if err != nil {
		t.Fatalf("lookup other scope reasoning: %v", err)
	}
	if reasoning != "" {
		t.Fatalf("reasoning leaked across key fingerprints: %q", reasoning)
	}
}
