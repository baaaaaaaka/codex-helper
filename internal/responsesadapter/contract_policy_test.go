package responsesadapter

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestFacadeRejectsUnsupportedStructuredOutputBeforeProviderCall(t *testing.T) {
	adapter := &recordingAdapter{events: []ProviderEvent{{Kind: ProviderEventDone}}}
	registry, err := NewProviderRegistry(ProviderRegistryOptions{
		DefaultProvider: "m",
		Providers: []ProviderConfig{{
			ID: "m", DefaultModel: "m", Adapter: adapter,
			ResponsesPolicy: config.ModelResponsesPolicy{StructuredOutput: config.ModelStructuredOutputPolicy{JSONObject: "unsupported", JSONSchema: "native"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	facade := &Facade{Router: registry, Store: NewMemoryStore()}
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"m","response_format":{"type":"json_object"},"input":"x"}`)))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "unsupported") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if len(adapter.requests) != 0 {
		t.Fatalf("provider was called %d times", len(adapter.requests))
	}
}

func TestFacadeFailsClosedForNativeStructuredOutput(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{name: "reasoning only", body: `{"model":"m","response_format":{"type":"json_schema","json_schema":{"schema":{"required":["answer"]}}},"input":"x"}`, want: "no message"},
		{name: "invalid json", body: `{"model":"m","response_format":{"type":"json_object"},"input":"x"}`, want: "invalid json_object"},
		{name: "multiple json values", body: `{"model":"m","response_format":{"type":"json_object"},"input":"x"}`, want: "multiple JSON values"},
		{name: "missing required", body: `{"model":"m","response_format":{"type":"json_schema","json_schema":{"schema":{"required":["answer"]}}},"input":"x"}`, want: "missing required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var events []ProviderEvent
			switch tc.name {
			case "reasoning only":
				events = []ProviderEvent{{Kind: ProviderEventReasoningDelta, Delta: "thinking"}, {Kind: ProviderEventDone}}
			case "invalid json":
				events = []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "not-json"}, {Kind: ProviderEventDone}}
			case "multiple json values":
				events = []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: `{"answer":true} {"extra":true}`}, {Kind: ProviderEventDone}}
			case "missing required":
				events = []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: `{"other":true}`}, {Kind: ProviderEventDone}}
			}
			registry, err := NewProviderRegistry(ProviderRegistryOptions{DefaultProvider: "m", Providers: []ProviderConfig{{
				ID: "m", DefaultModel: "m", Adapter: fakeAdapter{events: events},
				ResponsesPolicy: config.ModelResponsesPolicy{StructuredOutput: config.ModelStructuredOutputPolicy{JSONObject: "native", JSONSchema: "native"}},
			}}})
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			(&Facade{Router: registry, Store: NewMemoryStore()}).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(tc.body)))
			if rec.Code != http.StatusBadGateway || !strings.Contains(rec.Body.String(), tc.want) {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestFacadeStrictParallelToolPolicyFailsClosed(t *testing.T) {
	args := `{"x":1}`
	registry, err := NewProviderRegistry(ProviderRegistryOptions{DefaultProvider: "m", Providers: []ProviderConfig{{
		ID: "m", DefaultModel: "m", Adapter: fakeAdapter{events: []ProviderEvent{
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "c1", Name: "a", ArgumentsDelta: args}},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 1, ID: "c2", Name: "b", ArgumentsDelta: args}},
			{Kind: ProviderEventDone},
		}}, ParallelToolEnforcement: "strict",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	(&Facade{Router: registry, Store: NewMemoryStore()}).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"m","input":"x"}`)))
	if rec.Code != http.StatusBadGateway || !strings.Contains(rec.Body.String(), "serial tool calls") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
