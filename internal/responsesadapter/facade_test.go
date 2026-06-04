package responsesadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFacadeStreamsTextLifecycleAndStoresCompletedResponse(t *testing.T) {
	store := NewMemoryStore()
	facade := newTestFacade(store, fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: "hel"},
			{Kind: ProviderEventTextDelta, Delta: "lo"},
			{Kind: ProviderEventUsage, Usage: &Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5, CachedTokens: 1, ReasoningTokens: 4}},
			{Kind: ProviderEventDone},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","stream":true,"input":"say hello"}`))
	req.Header.Set("x-codex-thread-id", "thread-a")
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{
		"event: response.created",
		"event: response.output_item.added",
		"event: response.output_text.delta",
		"event: response.output_item.done",
		"event: response.completed",
		`"type":"response.completed"`,
		`"output_text":"hello"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("stream missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, `"phase":`) {
		t.Fatalf("responses adapter should not synthesize message phase:\n%s", body)
	}
	events := parseSSEEvents(t, body)
	added := firstSSEEventNamed(t, events, "response.output_item.added")
	item, ok := added.data["item"].(map[string]any)
	if !ok {
		t.Fatalf("added item = %#v", added.data["item"])
	}
	content, ok := item["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("added message content = %#v", item["content"])
	}
	contentPart, ok := content[0].(map[string]any)
	if !ok || contentPart["type"] != "output_text" {
		t.Fatalf("added message content part = %#v", content[0])
	}
	completed := events[len(events)-1]
	response, ok := completed.data["response"].(map[string]any)
	if !ok {
		t.Fatalf("completed response = %#v", completed.data)
	}
	usage, ok := response["usage"].(map[string]any)
	if !ok || usage["total_tokens"] != float64(5) {
		t.Fatalf("completed usage = %#v", response["usage"])
	}
	inputDetails, ok := usage["input_tokens_details"].(map[string]any)
	if !ok || inputDetails["cached_tokens"] != float64(1) {
		t.Fatalf("completed input token details = %#v", usage)
	}
	outputDetails, ok := usage["output_tokens_details"].(map[string]any)
	if !ok || outputDetails["reasoning_tokens"] != float64(4) {
		t.Fatalf("completed output token details = %#v", usage)
	}
	rawResponse, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("marshal completed response: %v", err)
	}
	var completedResponse responseObject
	if err := json.Unmarshal(rawResponse, &completedResponse); err != nil {
		t.Fatalf("decode completed response: %v", err)
	}
	if len(completedResponse.Output) != 1 || completedResponse.Output[0].Type != "message" {
		t.Fatalf("completed output = %#v", completedResponse.Output)
	}

	record, err := store.Get("resp_001", Scope{Tenant: "local", User: "local", Provider: "test-provider", Model: "model-a", Thread: "thread-a", Branch: "main"})
	if err != nil {
		t.Fatalf("stored response not found: %v", err)
	}
	if record.InputText != "say hello" || record.OutputText != "hello" || record.Status != ResponseStatusCompleted {
		t.Fatalf("stored record = %#v", record)
	}
	if record.Usage == nil || record.Usage.TotalTokens != 5 {
		t.Fatalf("stored usage = %#v", record.Usage)
	}
}

func TestFacadeCompletedUsageIncludesZeroCachedTokens(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: "ok"},
			{Kind: ProviderEventUsage, Usage: &Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5}},
			{Kind: ProviderEventDone},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","stream":true,"input":"say ok"}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	events := parseSSEEvents(t, rec.Body.String())
	completed := firstSSEEventNamed(t, events, "response.completed")
	response, ok := completed.data["response"].(map[string]any)
	if !ok {
		t.Fatalf("completed response = %#v", completed.data["response"])
	}
	usage, ok := response["usage"].(map[string]any)
	if !ok {
		t.Fatalf("completed usage = %#v", response["usage"])
	}
	inputDetails, ok := usage["input_tokens_details"].(map[string]any)
	if !ok {
		t.Fatalf("completed usage missing input_tokens_details: %#v", usage)
	}
	if inputDetails["cached_tokens"] != float64(0) {
		t.Fatalf("cached_tokens = %#v, want 0 in %#v", inputDetails["cached_tokens"], usage)
	}
	outputDetails, ok := usage["output_tokens_details"].(map[string]any)
	if !ok {
		t.Fatalf("completed usage missing output_tokens_details: %#v", usage)
	}
	if outputDetails["reasoning_tokens"] != float64(0) {
		t.Fatalf("reasoning_tokens = %#v, want 0 in %#v", outputDetails["reasoning_tokens"], usage)
	}
}

func TestFacadeReturnsNonStreamResponse(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: "done"},
			{Kind: ProviderEventDone},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":[{"role":"user","content":[{"type":"input_text","text":"run"}]}]}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["id"] != "resp_001" || body["output_text"] != "done" || body["status"] != string(ResponseStatusCompleted) {
		t.Fatalf("response body = %#v", body)
	}
}

func TestFacadeKeepsPublicModelWhenProviderUsesUpstreamModel(t *testing.T) {
	store := NewMemoryStore()
	adapter := &recordingAdapter{events: []ProviderEvent{
		{Kind: ProviderEventTextDelta, Delta: "mapped"},
		{Kind: ProviderEventDone},
	}}
	registry := mustProviderRegistry(t, ProviderRegistryOptions{
		DefaultProvider: "mimo",
		ProxyKeys:       map[string]string{"mi-key": "mimo"},
		Providers: []ProviderConfig{{
			ID:           "mimo",
			ProfileID:    "mimo",
			APIKey:       "sk-mimo",
			DefaultModel: "mimo/mimo-v2.5",
			Models: []ModelInfo{{
				ID:         "mimo/mimo-v2.5",
				OwnedBy:    "mimo",
				UpstreamID: "mimo-v2.5",
			}, {
				ID:         "mimo/mimo-v2.5-pro",
				OwnedBy:    "mimo",
				UpstreamID: "mimo-v2.5-pro",
			}},
			Adapter: adapter,
		}},
	})
	facade := &Facade{
		Router: registry,
		Store:  store,
		NewID: func(prefix string) (string, error) {
			return prefix + "_001", nil
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"mimo/mimo-v2.5-pro","input":"map model"}`))
	req.Header.Set("Authorization", "Bearer mi-key")
	req.Header.Set("x-codex-thread-id", "thread-public")
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["model"] != "mimo/mimo-v2.5-pro" {
		t.Fatalf("response model = %q, want public model", body["model"])
	}
	if len(adapter.requests) != 1 || adapter.requests[0].Model != "mimo-v2.5-pro" {
		t.Fatalf("upstream request model = %#v, want upstream model", adapter.requests)
	}
	if adapter.requests[0].Scope.Model != "mimo/mimo-v2.5-pro" {
		t.Fatalf("request scope model = %q, want public model", adapter.requests[0].Scope.Model)
	}
	record, err := store.Get("resp_001", adapter.requests[0].Scope)
	if err != nil {
		t.Fatalf("stored response not found under public model scope: %v", err)
	}
	if record.Model != "mimo/mimo-v2.5-pro" {
		t.Fatalf("stored response model = %q, want public model", record.Model)
	}
}

func TestFacadeExposesReusableInstanceHealth(t *testing.T) {
	facade := &Facade{InstanceID: "adapter-inst"}
	req := httptest.NewRequest(http.MethodGet, "/_codex_proxy/health", nil)
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if body["ok"] != true || body["status"] != "ok" || body["instanceId"] != "adapter-inst" {
		t.Fatalf("health body = %#v", body)
	}
}

func TestFacadeStreamsToolCallLifecycleAndStoresCompletedResponse(t *testing.T) {
	store := NewMemoryStore()
	facade := newTestFacade(store, fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_read", Name: "read_file"}},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ArgumentsDelta: `{"path":"`}},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ArgumentsDelta: `main.go"}`}},
			{Kind: ProviderEventDone},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","stream":true,"input":"read main"}`))
	req.Header.Set("x-codex-thread-id", "thread-tools")
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{
		"event: response.output_item.added",
		"event: response.function_call_arguments.delta",
		"event: response.output_item.done",
		"event: response.completed",
		`"type":"function_call"`,
		`"call_id":"call_read"`,
		`"name":"read_file"`,
		`"arguments":"{\"path\":\"main.go\"}"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("stream missing %q:\n%s", want, body)
		}
	}

	record, err := store.Get("resp_001", Scope{Tenant: "local", User: "local", Provider: "test-provider", Model: "model-a", Thread: "thread-tools", Branch: "main"})
	if err != nil {
		t.Fatalf("stored response not found: %v", err)
	}
	if record.OutputText != "" || len(record.ToolCalls) != 1 {
		t.Fatalf("stored record = %#v", record)
	}
	call := record.ToolCalls[0]
	if call.ID != "call_read" || call.Name != "read_file" || call.Arguments != `{"path":"main.go"}` || call.Status != "completed" {
		t.Fatalf("stored call = %#v", call)
	}
}

func TestFacadeReturnsNonStreamParallelToolCalls(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_a", Name: "tool_a", ArgumentsDelta: `{"a":`}},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 1, ID: "call_b", Name: "tool_b", ArgumentsDelta: `{"b":`}},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ArgumentsDelta: `1}`}},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 1, ArgumentsDelta: `2}`}},
			{Kind: ProviderEventDone},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"parallel"}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Output []outputItem `json:"output"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Output) != 2 {
		t.Fatalf("output = %#v", body.Output)
	}
	if body.Output[0].CallID != "call_a" || body.Output[0].Name != "tool_a" || body.Output[0].Arguments != `{"a":1}` {
		t.Fatalf("first call = %#v", body.Output[0])
	}
	if body.Output[1].CallID != "call_b" || body.Output[1].Name != "tool_b" || body.Output[1].Arguments != `{"b":2}` {
		t.Fatalf("second call = %#v", body.Output[1])
	}
}

func TestFacadeStreamsMixedTextAndToolCallLifecycle(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: "I will "},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_read", Name: "read_file", ArgumentsDelta: `{"path":"main.go"}`}},
			{Kind: ProviderEventTextDelta, Delta: "check."},
			{Kind: ProviderEventDone},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","stream":true,"input":"read"}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	events := parseSSEEvents(t, rec.Body.String())
	var completed responseObject
	for _, event := range events {
		if event.name != "response.completed" {
			continue
		}
		raw, err := json.Marshal(event.data["response"])
		if err != nil {
			t.Fatalf("marshal completed response: %v", err)
		}
		if err := json.Unmarshal(raw, &completed); err != nil {
			t.Fatalf("decode completed response: %v", err)
		}
	}
	if completed.OutputText != "I will check." {
		t.Fatalf("output_text = %q", completed.OutputText)
	}
	if len(completed.Output) != 2 || completed.Output[0].Type != "message" || completed.Output[1].Type != "function_call" {
		t.Fatalf("output = %#v", completed.Output)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"event: response.output_text.delta",
		"event: response.function_call_arguments.delta",
		`"call_id":"call_read"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("stream missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, `"phase":`) {
		t.Fatalf("responses adapter should not synthesize message phase for tool-call responses:\n%s", body)
	}
}

func TestFacadeSalvagesMalformedToolArguments(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_bad", Name: "tool", ArgumentsDelta: `{"unterminated"`}},
			{Kind: ProviderEventDone},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"bad args"}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Output []outputItem `json:"output"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Output) != 1 || body.Output[0].Arguments != "{}" {
		t.Fatalf("output = %#v", body.Output)
	}
}

func TestFacadeStreamsReasoningLifecycleAndStoresCompletedResponse(t *testing.T) {
	store := NewMemoryStore()
	facade := newTestFacade(store, fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventReasoningDelta, Delta: "think"},
			{Kind: ProviderEventTextDelta, Delta: "answer"},
			{Kind: ProviderEventDone},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","stream":true,"input":"x"}`))
	req.Header.Set("x-codex-thread-id", "thread-reasoning")
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{
		"event: response.reasoning_text.delta",
		`"type":"reasoning"`,
		`"encrypted_content":"think"`,
		`"content":[{"type":"reasoning_text","text":"think"}]`,
		`"output_text":"answer"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("stream missing %q:\n%s", want, body)
		}
	}
	for _, forbidden := range []string{
		"event: response.reasoning_summary_part.added",
		"event: response.reasoning_summary_text.delta",
		`"summary":[{"text":"think"`,
		`"summary":[{"type":"summary_text","text":"think"`,
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("stream leaked raw reasoning as visible summary %q:\n%s", forbidden, body)
		}
	}
	events := parseSSEEvents(t, body)
	added := firstSSEEventNamed(t, events, "response.output_item.added")
	item, ok := added.data["item"].(map[string]any)
	if !ok {
		t.Fatalf("added item = %#v", added.data["item"])
	}
	summary, ok := item["summary"].([]any)
	if !ok || len(summary) != 0 {
		t.Fatalf("added reasoning summary = %#v", item["summary"])
	}
	record, err := store.Get("resp_001", Scope{Tenant: "local", User: "local", Provider: "test-provider", Model: "model-a", Thread: "thread-reasoning", Branch: "main"})
	if err != nil {
		t.Fatalf("stored response not found: %v", err)
	}
	if record.ReasoningText != "think" || record.OutputText != "answer" {
		t.Fatalf("stored record = %#v", record)
	}
}

func TestFacadeFailsToolCallWithoutFunctionName(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_missing", ArgumentsDelta: `{}`}},
			{Kind: ProviderEventDone},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","stream":true,"input":"bad tool"}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: response.failed") || !strings.Contains(body, "missing a function name") {
		t.Fatalf("unexpected stream:\n%s", body)
	}
}

func TestFacadeFailsWhenToolCallIdentityChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []ProviderEvent
		want   string
	}{
		{
			name: "id change",
			events: []ProviderEvent{
				{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_a", Name: "tool"}},
				{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_b"}},
				{Kind: ProviderEventDone},
			},
			want: "changed id",
		},
		{
			name: "name change",
			events: []ProviderEvent{
				{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_a", Name: "tool_a"}},
				{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, Name: "tool_b"}},
				{Kind: ProviderEventDone},
			},
			want: "changed name",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			facade := newTestFacade(NewMemoryStore(), fakeAdapter{events: tc.events})
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","stream":true,"input":"bad tool"}`))
			rec := httptest.NewRecorder()
			facade.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}
			body := rec.Body.String()
			if !strings.Contains(body, "event: response.failed") || !strings.Contains(body, tc.want) {
				t.Fatalf("unexpected stream:\n%s", body)
			}
		})
	}
}

func TestFacadeStreamEventsHaveMonotonicSequenceAndMatchingType(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: "checking"},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_1", Name: "read_file", ArgumentsDelta: `{}`}},
			{Kind: ProviderEventDone},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","stream":true,"input":"x"}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	events := parseSSEEvents(t, rec.Body.String())
	if len(events) == 0 {
		t.Fatalf("no SSE events:\n%s", rec.Body.String())
	}
	for i, event := range events {
		if gotType, _ := event.data["type"].(string); gotType != event.name {
			t.Fatalf("event %d type = %q, want %q: %#v", i, gotType, event.name, event.data)
		}
		gotSeq, ok := event.data["sequence_number"].(float64)
		if !ok {
			t.Fatalf("event %d missing sequence_number: %#v", i, event.data)
		}
		if wantSeq := float64(i + 1); gotSeq != wantSeq {
			t.Fatalf("event %d sequence_number = %v, want %v", i, gotSeq, wantSeq)
		}
	}
}

func TestFacadePassesNormalizedRequestAndHistoryToAdapter(t *testing.T) {
	store := NewMemoryStore()
	adapter := &recordingAdapter{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "first"}, {Kind: ProviderEventDone}}}
	facade := newTestFacade(store, adapter)

	first := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"start","tools":[{"type":"local_shell"}]}`))
	first.Header.Set("x-codex-thread-id", "thread-a")
	firstRec := httptest.NewRecorder()
	facade.ServeHTTP(firstRec, first)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}

	adapter.events = []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "second"}, {Kind: ProviderEventDone}}
	second := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","previous_response_id":"resp_001","input":[{"role":"user","content":[{"type":"input_text","text":"continue"}]}]}`))
	second.Header.Set("x-codex-thread-id", "thread-a")
	secondRec := httptest.NewRecorder()
	facade.ServeHTTP(secondRec, second)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second status = %d, body = %s", secondRec.Code, secondRec.Body.String())
	}

	requests := adapter.requests
	if len(requests) != 2 {
		t.Fatalf("adapter requests = %d", len(requests))
	}
	if requests[0].InputText != "start" || requests[0].Scope.Thread != "thread-a" {
		t.Fatalf("first provider request = %#v", requests[0])
	}
	if len(requests[0].Tools) != 1 || requests[0].Tools[0].Function.Name != "shell" {
		t.Fatalf("first tools = %#v", requests[0].Tools)
	}
	if requests[1].InputText != "continue" || requests[1].PreviousResponseID != "resp_001" {
		t.Fatalf("second provider request = %#v", requests[1])
	}
	if len(requests[1].History) != 1 || requests[1].History[0].OutputText != "first" {
		t.Fatalf("second history = %#v", requests[1].History)
	}
}

func TestFacadeMapsFunctionCallOutputToChatTranscript(t *testing.T) {
	store := NewMemoryStore()
	adapter := &recordingAdapter{events: []ProviderEvent{
		{Kind: ProviderEventReasoningDelta, Delta: "need to read the file"},
		{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_read", Name: "read_file", ArgumentsDelta: `{"path":"main.go"}`}},
		{Kind: ProviderEventDone},
	}}
	facade := newTestFacade(store, adapter)

	first := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"read main"}`))
	first.Header.Set("x-codex-thread-id", "thread-tool-output")
	firstRec := httptest.NewRecorder()
	facade.ServeHTTP(firstRec, first)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}

	adapter.events = []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "final"}, {Kind: ProviderEventDone}}
	second := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","previous_response_id":"resp_001","input":[{"type":"function_call_output","call_id":"call_read","output":"file contents"}]}`))
	second.Header.Set("x-codex-thread-id", "thread-tool-output")
	secondRec := httptest.NewRecorder()
	facade.ServeHTTP(secondRec, second)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second status = %d, body = %s", secondRec.Code, secondRec.Body.String())
	}

	if len(adapter.requests) != 2 {
		t.Fatalf("requests = %d", len(adapter.requests))
	}
	messages := adapter.requests[1].Messages
	if len(messages) != 3 {
		t.Fatalf("messages = %#v", messages)
	}
	if messages[0].Role != "user" || messages[0].Content != "read main" {
		t.Fatalf("first message = %#v", messages[0])
	}
	if messages[1].Role != "assistant" || len(messages[1].ToolCalls) != 1 || messages[1].ToolCalls[0].ID != "call_read" || messages[1].ToolCalls[0].Name != "read_file" {
		t.Fatalf("assistant tool call message = %#v", messages[1])
	}
	if messages[1].ReasoningContent != "need to read the file" {
		t.Fatalf("assistant reasoning_content = %q", messages[1].ReasoningContent)
	}
	if messages[2].Role != "tool" || messages[2].ToolCallID != "call_read" || messages[2].Content != "file contents" {
		t.Fatalf("tool output message = %#v", messages[2])
	}

	adapter.events = []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "next"}, {Kind: ProviderEventDone}}
	third := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","previous_response_id":"resp_002","input":"next question"}`))
	third.Header.Set("x-codex-thread-id", "thread-tool-output")
	thirdRec := httptest.NewRecorder()
	facade.ServeHTTP(thirdRec, third)
	if thirdRec.Code != http.StatusOK {
		t.Fatalf("third status = %d, body = %s", thirdRec.Code, thirdRec.Body.String())
	}
	messages = adapter.requests[2].Messages
	if len(messages) != 5 {
		t.Fatalf("third messages = %#v", messages)
	}
	if messages[2].Role != "tool" || messages[2].ToolCallID != "call_read" || messages[3].Role != "assistant" || messages[3].Content != "final" || messages[4].Content != "next question" {
		t.Fatalf("third transcript = %#v", messages)
	}
}

func TestFacadeRejectsOrphanFunctionCallOutput(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":[{"type":"function_call_output","call_id":"missing","output":"orphan"}]}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "without matching function_call") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestFacadeRejectsPreviousResponseScopeMismatch(t *testing.T) {
	store := NewMemoryStore()
	facade := newTestFacade(store, fakeAdapter{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "a"}, {Kind: ProviderEventDone}}})

	first := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"one"}`))
	first.Header.Set("x-codex-thread-id", "thread-a")
	firstRec := httptest.NewRecorder()
	facade.ServeHTTP(firstRec, first)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}

	second := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","previous_response_id":"resp_001","input":"two"}`))
	second.Header.Set("x-codex-thread-id", "thread-b")
	secondRec := httptest.NewRecorder()
	facade.ServeHTTP(secondRec, second)
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("second status = %d, want conflict; body = %s", secondRec.Code, secondRec.Body.String())
	}
	if !strings.Contains(secondRec.Body.String(), ErrScopeMismatch.Error()) {
		t.Fatalf("scope mismatch body = %s", secondRec.Body.String())
	}
}

func TestFacadeRejectsPreviousResponseScopeMismatchAcrossDimensions(t *testing.T) {
	for _, tc := range []struct {
		name  string
		model string
		set   func(*http.Request)
	}{
		{name: "tenant", set: func(r *http.Request) { r.Header.Set("x-adapter-tenant", "tenant-b") }},
		{name: "user", set: func(r *http.Request) { r.Header.Set("x-adapter-user", "user-b") }},
		{name: "thread", set: func(r *http.Request) { r.Header.Set("x-codex-thread-id", "thread-b") }},
		{name: "branch", set: func(r *http.Request) { r.Header.Set("x-adapter-branch", "branch-b") }},
		{name: "model", model: "model-b", set: func(r *http.Request) { r.Header.Set("x-codex-thread-id", "thread-a") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemoryStore()
			facade := newTestFacade(store, fakeAdapter{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "a"}, {Kind: ProviderEventDone}}})

			first := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"one"}`))
			first.Header.Set("x-codex-thread-id", "thread-a")
			firstRec := httptest.NewRecorder()
			facade.ServeHTTP(firstRec, first)
			if firstRec.Code != http.StatusOK {
				t.Fatalf("first status = %d, body = %s", firstRec.Code, firstRec.Body.String())
			}

			model := firstNonEmpty(tc.model, "model-a")
			second := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"`+model+`","previous_response_id":"resp_001","input":"two"}`))
			second.Header.Set("x-codex-thread-id", "thread-a")
			tc.set(second)
			secondRec := httptest.NewRecorder()
			facade.ServeHTTP(secondRec, second)
			if secondRec.Code != http.StatusConflict {
				t.Fatalf("second status = %d, want conflict; body = %s", secondRec.Code, secondRec.Body.String())
			}
			if !strings.Contains(secondRec.Body.String(), ErrScopeMismatch.Error()) {
				t.Fatalf("scope mismatch body = %s", secondRec.Body.String())
			}
		})
	}
}

func TestFacadeHealthAndModels(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{})

	health := httptest.NewRecorder()
	facade.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"status":"ok"`) {
		t.Fatalf("health = %d %s", health.Code, health.Body.String())
	}

	models := httptest.NewRecorder()
	facade.ServeHTTP(models, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if models.Code != http.StatusOK || !strings.Contains(models.Body.String(), `"id":"model-a"`) {
		t.Fatalf("models = %d %s", models.Code, models.Body.String())
	}
}

func TestFacadeErrorBoundaries(t *testing.T) {
	noAdapter := newTestFacade(NewMemoryStore(), nil)
	noAdapterRec := httptest.NewRecorder()
	noAdapter.ServeHTTP(noAdapterRec, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"x"}`)))
	if noAdapterRec.Code != http.StatusInternalServerError {
		t.Fatalf("no adapter status = %d", noAdapterRec.Code)
	}

	badJSON := newTestFacade(NewMemoryStore(), fakeAdapter{})
	badJSONRec := httptest.NewRecorder()
	badJSON.ServeHTTP(badJSONRec, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{`)))
	if badJSONRec.Code != http.StatusBadRequest {
		t.Fatalf("bad JSON status = %d", badJSONRec.Code)
	}

	missingModel := &Facade{Adapter: fakeAdapter{}, Store: NewMemoryStore()}
	missingModelRec := httptest.NewRecorder()
	missingModel.ServeHTTP(missingModelRec, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"input":"x"}`)))
	if missingModelRec.Code != http.StatusBadRequest {
		t.Fatalf("missing model status = %d", missingModelRec.Code)
	}

	providerErr := newTestFacade(NewMemoryStore(), fakeAdapter{err: errors.New("upstream unavailable")})
	providerErrRec := httptest.NewRecorder()
	providerErr.ServeHTTP(providerErrRec, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"x"}`)))
	if providerErrRec.Code != http.StatusBadGateway {
		t.Fatalf("provider error status = %d, body = %s", providerErrRec.Code, providerErrRec.Body.String())
	}
}

func TestFacadeEmitsFailedWhenProviderStreamEndsBeforeDone(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "partial"}},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","stream":true,"input":"x"}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: response.failed") || strings.Contains(body, "event: response.completed") {
		t.Fatalf("unexpected stream:\n%s", body)
	}
	if _, err := facade.Store.Get("resp_001", Scope{Tenant: "local", User: "local", Provider: "test-provider", Model: "model-a", Thread: "default", Branch: "main"}); err != ErrResponseNotFound {
		t.Fatalf("failed response should not be stored, err = %v", err)
	}
}

func TestFacadeEmitsFailedWhenProviderErrorsAfterPartialOutput(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: "partial"},
			{Kind: ProviderEventError, Err: errors.New("upstream exploded")},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","stream":true,"input":"x"}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{
		"event: response.failed",
		"upstream exploded",
		`"output_text":"partial"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("stream missing %q:\n%s", want, body)
		}
	}
	if _, err := facade.Store.Get("resp_001", Scope{Tenant: "local", User: "local", Provider: "test-provider", Model: "model-a", Thread: "default", Branch: "main"}); err != ErrResponseNotFound {
		t.Fatalf("failed response should not be stored, err = %v", err)
	}
}

func TestFacadeNonStreamProviderErrorDoesNotStorePartialOutput(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: "partial"},
			{Kind: ProviderEventError, Err: errors.New("upstream exploded")},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"x"}`))
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if _, err := facade.Store.Get("resp_001", Scope{Tenant: "local", User: "local", Provider: "test-provider", Model: "model-a", Thread: "default", Branch: "main"}); err != ErrResponseNotFound {
		t.Fatalf("failed response should not be stored, err = %v", err)
	}
}

func TestFacadeCompletesWithoutWaitingForProviderChannelClose(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-stream", true: "stream"}[stream], func(t *testing.T) {
			facade := newTestFacade(NewMemoryStore(), openDoneAdapter{})
			body := `{"model":"model-a","input":"x"}`
			if stream {
				body = `{"model":"model-a","stream":true,"input":"x"}`
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
			rec := httptest.NewRecorder()
			facade.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), `"output_text":"done"`) {
				t.Fatalf("body = %s", rec.Body.String())
			}
		})
	}
}

func TestFacadeWaitsForActiveTurnInSameScope(t *testing.T) {
	oldTimeout := activeTurnWaitTimeout
	oldPoll := activeTurnPollDelay
	activeTurnWaitTimeout = 2 * time.Second
	activeTurnPollDelay = time.Millisecond
	t.Cleanup(func() {
		activeTurnWaitTimeout = oldTimeout
		activeTurnPollDelay = oldPoll
	})

	adapter := &blockingAdapter{started: make(chan struct{}), release: make(chan struct{})}
	facade := newTestFacade(NewMemoryStore(), adapter)

	var firstStatus int
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"first"}`))
		req.Header.Set("x-codex-thread-id", "thread-a")
		rec := httptest.NewRecorder()
		facade.ServeHTTP(rec, req)
		firstStatus = rec.Code
	}()

	<-adapter.started
	var secondStatus int
	wg.Add(1)
	go func() {
		defer wg.Done()
		second := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"second"}`))
		second.Header.Set("x-codex-thread-id", "thread-a")
		secondRec := httptest.NewRecorder()
		facade.ServeHTTP(secondRec, second)
		secondStatus = secondRec.Code
	}()
	time.Sleep(20 * time.Millisecond)
	close(adapter.release)
	wg.Wait()
	if firstStatus != http.StatusOK {
		t.Fatalf("first status = %d", firstStatus)
	}
	if secondStatus != http.StatusOK {
		t.Fatalf("second status = %d", secondStatus)
	}
}

func TestFacadeActiveTurnWaitDoesNotBlockOtherScopes(t *testing.T) {
	oldTimeout := activeTurnWaitTimeout
	oldPoll := activeTurnPollDelay
	activeTurnWaitTimeout = 2 * time.Second
	activeTurnPollDelay = time.Millisecond
	t.Cleanup(func() {
		activeTurnWaitTimeout = oldTimeout
		activeTurnPollDelay = oldPoll
	})

	adapter := &selectiveBlockingAdapter{
		blockThread: "thread-a",
		blockInput:  "first",
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	facade := newTestFacade(NewMemoryStore(), adapter)

	var firstWG sync.WaitGroup
	firstWG.Add(1)
	go func() {
		defer firstWG.Done()
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"first"}`))
		req.Header.Set("x-codex-thread-id", "thread-a")
		rec := httptest.NewRecorder()
		facade.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("first status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}()
	<-adapter.started

	other := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"other"}`))
	other.Header.Set("x-codex-thread-id", "thread-b")
	otherRec := httptest.NewRecorder()
	facade.ServeHTTP(otherRec, other)
	if otherRec.Code != http.StatusOK {
		t.Fatalf("other scope status = %d, body = %s", otherRec.Code, otherRec.Body.String())
	}
	if !strings.Contains(otherRec.Body.String(), `"output_text":"reply:thread-b:other"`) {
		t.Fatalf("other scope body = %s", otherRec.Body.String())
	}

	sameDone := make(chan int, 1)
	go func() {
		same := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"queued"}`))
		same.Header.Set("x-codex-thread-id", "thread-a")
		sameRec := httptest.NewRecorder()
		facade.ServeHTTP(sameRec, same)
		sameDone <- sameRec.Code
	}()
	select {
	case status := <-sameDone:
		t.Fatalf("same scope completed before active turn release with status %d", status)
	case <-time.After(20 * time.Millisecond):
	}

	close(adapter.release)
	firstWG.Wait()
	select {
	case status := <-sameDone:
		if status != http.StatusOK {
			t.Fatalf("same scope status after release = %d", status)
		}
	case <-time.After(time.Second):
		t.Fatal("same scope request did not complete after active turn release")
	}
}

func TestFacadeRejectsActiveTurnAfterWaitTimeout(t *testing.T) {
	oldTimeout := activeTurnWaitTimeout
	oldPoll := activeTurnPollDelay
	activeTurnWaitTimeout = 20 * time.Millisecond
	activeTurnPollDelay = time.Millisecond
	t.Cleanup(func() {
		activeTurnWaitTimeout = oldTimeout
		activeTurnPollDelay = oldPoll
	})

	adapter := &blockingAdapter{started: make(chan struct{}), release: make(chan struct{})}
	facade := newTestFacade(NewMemoryStore(), adapter)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"first"}`))
		req.Header.Set("x-codex-thread-id", "thread-a")
		rec := httptest.NewRecorder()
		facade.ServeHTTP(rec, req)
	}()

	<-adapter.started
	second := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"second"}`))
	second.Header.Set("x-codex-thread-id", "thread-a")
	secondRec := httptest.NewRecorder()
	facade.ServeHTTP(secondRec, second)
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("second status = %d, body = %s", secondRec.Code, secondRec.Body.String())
	}
	close(adapter.release)
	wg.Wait()
}

func TestFacadeActiveTurnWaitCancellationDoesNotClaimScope(t *testing.T) {
	oldTimeout := activeTurnWaitTimeout
	oldPoll := activeTurnPollDelay
	activeTurnWaitTimeout = 2 * time.Second
	activeTurnPollDelay = time.Millisecond
	t.Cleanup(func() {
		activeTurnWaitTimeout = oldTimeout
		activeTurnPollDelay = oldPoll
	})

	adapter := &blockingAdapter{started: make(chan struct{}), release: make(chan struct{})}
	facade := newTestFacade(NewMemoryStore(), adapter)

	var firstWG sync.WaitGroup
	firstWG.Add(1)
	go func() {
		defer firstWG.Done()
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"first"}`))
		req.Header.Set("x-codex-thread-id", "thread-a")
		rec := httptest.NewRecorder()
		facade.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("first status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}()
	<-adapter.started

	ctx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan int, 1)
	go func() {
		waiter := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"waiter"}`)).WithContext(ctx)
		waiter.Header.Set("x-codex-thread-id", "thread-a")
		waiterRec := httptest.NewRecorder()
		facade.ServeHTTP(waiterRec, waiter)
		waiterDone <- waiterRec.Code
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case status := <-waiterDone:
		if status != http.StatusInternalServerError {
			t.Fatalf("canceled waiter status = %d", status)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}

	close(adapter.release)
	firstWG.Wait()

	third := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"third"}`))
	third.Header.Set("x-codex-thread-id", "thread-a")
	thirdRec := httptest.NewRecorder()
	facade.ServeHTTP(thirdRec, third)
	if thirdRec.Code != http.StatusOK {
		t.Fatalf("third status = %d, body = %s", thirdRec.Code, thirdRec.Body.String())
	}
}

func newTestFacade(store ResponseStore, adapter ProviderAdapter) *Facade {
	var next int
	return &Facade{
		Adapter:      adapter,
		Store:        store,
		ProviderID:   "test-provider",
		DefaultModel: "model-a",
		Models:       []ModelInfo{{ID: "model-a", OwnedBy: "test-provider"}},
		NewID: func(prefix string) (string, error) {
			next++
			return prefix + "_00" + string(rune('0'+next)), nil
		},
	}
}

type fakeAdapter struct {
	events []ProviderEvent
	err    error
}

func (a fakeAdapter) Stream(context.Context, ProviderRequest) (<-chan ProviderEvent, error) {
	if a.err != nil {
		return nil, a.err
	}
	ch := make(chan ProviderEvent, len(a.events))
	for _, event := range a.events {
		ch <- event
	}
	close(ch)
	return ch, nil
}

type recordingAdapter struct {
	mu       sync.Mutex
	events   []ProviderEvent
	requests []ProviderRequest
}

func (a *recordingAdapter) Stream(_ context.Context, req ProviderRequest) (<-chan ProviderEvent, error) {
	a.mu.Lock()
	a.requests = append(a.requests, req)
	events := append([]ProviderEvent(nil), a.events...)
	a.mu.Unlock()
	ch := make(chan ProviderEvent, len(events))
	for _, event := range events {
		ch <- event
	}
	close(ch)
	return ch, nil
}

type blockingAdapter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (a *blockingAdapter) Stream(ctx context.Context, _ ProviderRequest) (<-chan ProviderEvent, error) {
	ch := make(chan ProviderEvent)
	a.once.Do(func() { close(a.started) })
	go func() {
		defer close(ch)
		select {
		case <-ctx.Done():
			return
		case <-a.release:
			ch <- ProviderEvent{Kind: ProviderEventTextDelta, Delta: "first"}
			ch <- ProviderEvent{Kind: ProviderEventDone}
		}
	}()
	return ch, nil
}

type selectiveBlockingAdapter struct {
	blockThread string
	blockInput  string
	started     chan struct{}
	release     chan struct{}
	once        sync.Once
}

func (a *selectiveBlockingAdapter) Stream(ctx context.Context, req ProviderRequest) (<-chan ProviderEvent, error) {
	ch := make(chan ProviderEvent)
	go func() {
		defer close(ch)
		if req.Scope.Thread == a.blockThread && req.InputText == a.blockInput {
			a.once.Do(func() { close(a.started) })
			select {
			case <-ctx.Done():
				return
			case <-a.release:
			}
		}
		ch <- ProviderEvent{Kind: ProviderEventTextDelta, Delta: "reply:" + req.Scope.Thread + ":" + req.InputText}
		ch <- ProviderEvent{Kind: ProviderEventDone}
	}()
	return ch, nil
}

type openDoneAdapter struct{}

func (openDoneAdapter) Stream(context.Context, ProviderRequest) (<-chan ProviderEvent, error) {
	ch := make(chan ProviderEvent, 2)
	ch <- ProviderEvent{Kind: ProviderEventTextDelta, Delta: "done"}
	ch <- ProviderEvent{Kind: ProviderEventDone}
	return ch, nil
}

type parsedSSEEvent struct {
	name string
	data map[string]any
}

func parseSSEEvents(t *testing.T, body string) []parsedSSEEvent {
	t.Helper()
	var events []parsedSSEEvent
	for _, block := range strings.Split(body, "\n\n") {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		var name string
		var dataLine string
		for _, line := range strings.Split(block, "\n") {
			if strings.HasPrefix(line, "event: ") {
				name = strings.TrimPrefix(line, "event: ")
			}
			if strings.HasPrefix(line, "data: ") {
				dataLine = strings.TrimPrefix(line, "data: ")
			}
		}
		if name == "" || dataLine == "" {
			t.Fatalf("invalid SSE block %q", block)
		}
		var data map[string]any
		if err := json.Unmarshal([]byte(dataLine), &data); err != nil {
			t.Fatalf("decode SSE data for %s: %v\n%s", name, err, dataLine)
		}
		events = append(events, parsedSSEEvent{name: name, data: data})
	}
	if len(events) == 0 {
		t.Fatalf("no SSE events parsed from %q", body)
	}
	for i, event := range events {
		if event.name == "" {
			t.Fatalf("event %d has empty name", i)
		}
		if _, ok := event.data["type"]; !ok {
			t.Fatalf("event %d missing type: %#v", i, event.data)
		}
	}
	return events
}

func firstSSEEventNamed(t *testing.T, events []parsedSSEEvent, name string) parsedSSEEvent {
	t.Helper()
	for _, event := range events {
		if event.name == name {
			return event
		}
	}
	t.Fatalf("missing SSE event %q in %#v", name, events)
	return parsedSSEEvent{}
}
