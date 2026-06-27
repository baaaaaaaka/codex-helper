package responsesadapter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
)

func TestFacadeShellPolicyPreparesLocalOutputAndKeepsStoredOriginal(t *testing.T) {
	store := NewMemoryStore()
	policy := responsespolicy.NewShellEscalationPolicy(16)
	facade := &Facade{
		Adapter:      &singleToolCallAdapter{},
		Store:        store,
		ShellPolicy:  policy,
		DefaultModel: "model-a",
		ProviderID:   "provider-a",
		NewID:        func(string) (string, error) { return "resp-policy", nil },
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","stream":true,"prompt_cache_key":"thread-a","input":"use gpu"}`))
	recorder := httptest.NewRecorder()
	facade.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `require_escalated`) {
		t.Fatalf("local Responses output did not request escalation:\n%s", recorder.Body.String())
	}
	record, err := store.Get("resp-policy", (Scope{Provider: "provider-a", Model: "model-a", Thread: "thread-a"}).withDefaults())
	if err != nil {
		t.Fatalf("stored response: %v", err)
	}
	if len(record.ToolCalls) != 1 || record.ToolCalls[0].Arguments != `{"command":"nvidia-smi"}` {
		t.Fatalf("stored tool calls = %#v, want original arguments", record.ToolCalls)
	}
}

func TestFacadeShellPolicyRestoresClientReplayBeforeProvider(t *testing.T) {
	policy := responsespolicy.NewShellEscalationPolicy(16)
	prepared := policy.Prepare("call-gpu", responsespolicy.ShellCommandTool, `{"command":"nvidia-smi"}`)
	adapter := &capturingDoneAdapter{}
	facade := &Facade{
		Adapter:      adapter,
		Store:        NewMemoryStore(),
		ShellPolicy:  policy,
		DefaultModel: "model-a",
	}
	input, _ := json.Marshal([]map[string]any{
		{"type": "function_call", "call_id": "call-gpu", "name": "shell_command", "arguments": prepared},
		{"type": "function_call_output", "call_id": "call-gpu", "output": "ok"},
	})
	body, _ := json.Marshal(map[string]any{"model": "model-a", "input": json.RawMessage(input)})
	recorder := httptest.NewRecorder()
	facade.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(body))))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if len(adapter.request.InputMessages) == 0 || len(adapter.request.InputMessages[0].ToolCalls) != 1 {
		t.Fatalf("provider request = %#v", adapter.request)
	}
	if got := adapter.request.InputMessages[0].ToolCalls[0].Arguments; got != `{"command":"nvidia-smi"}` {
		t.Fatalf("provider arguments = %s", got)
	}
}

type singleToolCallAdapter struct{}

func (*singleToolCallAdapter) Stream(ctx context.Context, _ ProviderRequest) (<-chan ProviderEvent, error) {
	ch := make(chan ProviderEvent, 3)
	ch <- ProviderEvent{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call-gpu", Name: "shell_command", ArgumentsDelta: `{"command":"nvidia-smi"}`}}
	ch <- ProviderEvent{Kind: ProviderEventDone}
	close(ch)
	return ch, nil
}

type capturingDoneAdapter struct{ request ProviderRequest }

func (a *capturingDoneAdapter) Stream(_ context.Context, request ProviderRequest) (<-chan ProviderEvent, error) {
	a.request = request
	ch := make(chan ProviderEvent, 1)
	ch <- ProviderEvent{Kind: ProviderEventDone}
	close(ch)
	return ch, nil
}
