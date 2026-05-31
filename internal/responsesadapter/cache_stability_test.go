package responsesadapter

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAIChatAdapterOutboundBodyStableForSameHistory(t *testing.T) {
	req := ProviderRequest{
		Model: "model-a",
		Scope: Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model-a", Thread: "thread", Branch: "main", KeyFingerprint: "key:a"},
		History: []ResponseRecord{
			{
				ID:            "resp_1",
				Scope:         Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model-a", Thread: "thread", Branch: "main", KeyFingerprint: "key:a"},
				InputMessages: []ProviderMessage{{Role: "user", Content: "inspect repo"}},
				OutputText:    "I will inspect it.",
				ReasoningText: "Need to list files.",
				ToolCalls:     []ToolCallRecord{{ID: "call_ls", Name: "exec_command", Arguments: `{"cmd":"ls"}`, Status: "completed"}},
				Usage:         &Usage{InputTokens: 100, OutputTokens: 20, TotalTokens: 120, CachedTokens: 80},
				Status:        ResponseStatusCompleted,
			},
			{
				ID:                 "resp_2",
				PreviousResponseID: "resp_1",
				Scope:              Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model-a", Thread: "thread", Branch: "main", KeyFingerprint: "key:a"},
				InputMessages:      []ProviderMessage{{Role: "tool", ToolCallID: "call_ls", Content: "main.go\nREADME.md"}},
				OutputText:         "Files found.",
				Status:             ResponseStatusCompleted,
			},
		},
		InputMessages: []ProviderMessage{{Role: "user", Content: "continue"}},
		Messages: []ProviderMessage{
			{Role: "user", Content: "inspect repo"},
			{Role: "assistant", Content: "I will inspect it.", ReasoningContent: "Need to list files.", ToolCalls: []ToolCallRecord{{ID: "call_ls", Name: "exec_command", Arguments: `{"cmd":"ls"}`, Status: "completed"}}},
			{Role: "tool", ToolCallID: "call_ls", Content: "main.go\nREADME.md"},
			{Role: "assistant", Content: "Files found."},
			{Role: "user", Content: "continue"},
		},
	}

	first, err := canonicalChatCompletionBody(req, ProfileForProvider("deepseek"))
	if err != nil {
		t.Fatalf("first body: %v", err)
	}
	second, err := canonicalChatCompletionBody(req, ProfileForProvider("deepseek"))
	if err != nil {
		t.Fatalf("second body: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("outbound body not stable\nfirst:  %s\nsecond: %s", first, second)
	}
	if string(first) == "" || !json.Valid(first) {
		t.Fatalf("invalid body: %s", first)
	}
}

func canonicalChatCompletionBody(req ProviderRequest, profile ProviderProfile) ([]byte, error) {
	body := chatCompletionRequest{
		Model:               req.Model,
		Messages:            chatMessagesFromProviderRequestWithProfile(req, profile),
		Tools:               req.Tools,
		ToolChoice:          chatToolChoice(req.ToolChoice, profile),
		ParallelToolCalls:   req.ParallelToolCalls,
		MaxCompletionTokens: req.MaxOutputTokens,
		ReasoningEffort:     profile.reasoningEffort(req.ReasoningEffort),
		Temperature:         req.Temperature,
		TopP:                req.TopP,
		Stream:              true,
	}
	if profile.ForceParallelToolCalls != nil {
		body.ParallelToolCalls = profile.ForceParallelToolCalls
	}
	if profile.shouldEnableThinking(req.Model) {
		body.Thinking = &thinkingConfig{Type: "enabled"}
	}
	if profile.shouldStripSampling(req.Model) {
		body.Temperature = nil
		body.TopP = nil
	}
	if profile.IncludeUsageStreamOptions {
		body.StreamOptions = &chatStreamOptions{IncludeUsage: true}
	}
	return json.Marshal(body)
}

func TestFacadeRejectsSameChatKeySwitchWithSQLiteStore(t *testing.T) {
	store, err := NewSQLiteStore(t.TempDir()+"/responses.sqlite", SQLiteStoreOptions{})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()
	adapter := &recordingAdapter{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "first"}, {Kind: ProviderEventDone}}}
	facade := newTestFacade(store, adapter)

	firstReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"one"}`))
	firstReq.Header.Set("x-codex-thread-id", "thread-a")
	firstReq.Header.Set("x-adapter-key-fingerprint", "key:a")
	firstRec := httptest.NewRecorder()
	facade.ServeHTTP(firstRec, firstReq)
	if firstRec.Code != 200 {
		t.Fatalf("first status = %d body = %s", firstRec.Code, firstRec.Body.String())
	}

	adapter.events = []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "second"}, {Kind: ProviderEventDone}}
	secondReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","previous_response_id":"resp_001","input":"two"}`))
	secondReq.Header.Set("x-codex-thread-id", "thread-a")
	secondReq.Header.Set("x-adapter-key-fingerprint", "key:b")
	secondRec := httptest.NewRecorder()
	facade.ServeHTTP(secondRec, secondReq)
	if secondRec.Code != 409 {
		t.Fatalf("second status = %d body = %s", secondRec.Code, secondRec.Body.String())
	}
}
