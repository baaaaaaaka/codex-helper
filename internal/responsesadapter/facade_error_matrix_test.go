package responsesadapter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOpenAIChatAdapterHTTPErrorBodies(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{name: "bad request json", status: http.StatusBadRequest, body: `{"error":{"message":"bad model"}}`, want: "bad model"},
		{name: "unauthorized text", status: http.StatusUnauthorized, body: "missing key", want: "missing key"},
		{name: "rate limited", status: http.StatusTooManyRequests, body: `{"error":{"message":"try later"}}`, want: "429 Too Many Requests"},
		{name: "server error html", status: http.StatusInternalServerError, body: "<html>boom</html>", want: "<html>boom</html>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, tc.body, tc.status)
			}))
			defer server.Close()

			adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client()}
			_, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
			if err == nil {
				t.Fatalf("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestOpenAIChatAdapterRetriesTransientHTTPFailures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		retryAfter string
		failures   int
		wantCalls  int
	}{
		{name: "429", status: http.StatusTooManyRequests, failures: 2, wantCalls: 3},
		{name: "503", status: http.StatusServiceUnavailable, failures: 1, wantCalls: 2},
		{name: "retry after zero", status: http.StatusTooManyRequests, retryAfter: "0", failures: 1, wantCalls: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if calls <= tc.failures {
					if tc.retryAfter != "" {
						w.Header().Set("Retry-After", tc.retryAfter)
					}
					http.Error(w, "try later", tc.status)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
				_, _ = w.Write([]byte("data: [DONE]\n\n"))
			}))
			defer server.Close()

			adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client(), MaxRetries: 3, RetryBase: time.Millisecond}
			stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
			if err != nil {
				t.Fatalf("Stream error: %v", err)
			}
			if text := eventText(collectEvents(stream)); text != "ok" {
				t.Fatalf("text = %q", text)
			}
			if calls != tc.wantCalls {
				t.Fatalf("calls = %d, want %d", calls, tc.wantCalls)
			}
		})
	}
}

func TestOpenAIChatAdapterRetriesNetworkFailure(t *testing.T) {
	var calls int
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("connection reset")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")),
			Request:    req,
		}, nil
	})}

	adapter := OpenAIChatAdapter{BaseURL: "http://upstream.test/v1", HTTPClient: client, MaxRetries: 2, RetryBase: time.Millisecond}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	if text := eventText(collectEvents(stream)); text != "ok" {
		t.Fatalf("text = %q", text)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestOpenAIChatAdapterAbortDuringRetryBackoff(t *testing.T) {
	var calls int
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		cancel()
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client(), MaxRetries: 3, RetryBase: time.Second}
	_, err := adapter.Stream(ctx, ProviderRequest{Model: "model-a", InputText: "x"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestOpenAIChatAdapterDoesNotRetryNonRetryable400(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, `{"error":{"message":"bad request"}}`, http.StatusBadRequest)
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client(), MaxRetries: 3, RetryBase: time.Millisecond}
	_, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err == nil {
		t.Fatalf("expected error")
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestUpstreamHTTPErrorEnhancesContextOverflowAndMalformedJSON(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		want    string
		notWant string
	}{
		{
			name:   "context overflow",
			status: http.StatusBadRequest,
			body:   `{"error":{"code":"context_length_exceeded","message":"maximum context length exceeded"}}`,
			want:   "/compact",
		},
		{
			name:   "malformed tool arguments",
			status: http.StatusBadRequest,
			body:   `data:{"error":{"message":"unexpected end of data: line 1 column 46"}}`,
			want:   "tool_call.arguments",
		},
		{
			name:    "unrelated bad request",
			status:  http.StatusBadRequest,
			body:    `{"error":{"message":"invalid api key"}}`,
			want:    "upstream chat completions returned",
			notWant: "/compact",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := upstreamHTTPError(tc.status, http.StatusText(tc.status), tc.body, "model-a")
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if tc.notWant != "" && strings.Contains(err.Error(), tc.notWant) {
				t.Fatalf("error = %v, should not contain %q", err, tc.notWant)
			}
		})
	}
}

func TestDetectContextOverflowMatchesProviderWordings(t *testing.T) {
	for _, snippet := range []string{
		`{"error":{"code":"context_length_exceeded","message":"too long"}}`,
		"This model's maximum context length is 8192 tokens.",
		"prompt is too long: 12345 tokens > 8192 maximum",
		"input length and `max_tokens` exceed context limit",
		"请求失败：上下文长度超出模型限制",
		"Requested 200000 tokens exceeds the maximum of 128000 supported by this model.",
	} {
		if got := detectContextOverflow(http.StatusBadRequest, snippet, "model-a"); !strings.Contains(got, "/compact") {
			t.Fatalf("snippet %q produced %q", snippet, got)
		}
	}
	for _, snippet := range []string{
		`{"error":{"message":"webSearchEnabled is false"}}`,
		"invalid request: model not found",
		"context_length_exceeded",
	} {
		status := http.StatusBadRequest
		if snippet == "context_length_exceeded" {
			status = http.StatusInternalServerError
		}
		if got := detectContextOverflow(status, snippet, "model-a"); got != "" {
			t.Fatalf("snippet %q should not match, got %q", snippet, got)
		}
	}
}

func TestDetectMalformedJSONFieldMatchesProviderWordings(t *testing.T) {
	for _, snippet := range []string{
		`data:{"error":{"message":"unexpected end of data: line 1 column 46"}}`,
		"unexpected end of data: line 1 column 12",
		"unterminated string in JSON field",
		"malformed JSON in request body",
	} {
		if got := detectMalformedJSONField(http.StatusBadRequest, snippet); !strings.Contains(got, "tool_call.arguments") {
			t.Fatalf("snippet %q produced %q", snippet, got)
		}
	}
	if got := detectMalformedJSONField(http.StatusInternalServerError, "unexpected end of data"); got != "" {
		t.Fatalf("non-400 should not match, got %q", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestFacadeReleasesActiveTurnAfterProviderFailure(t *testing.T) {
	for _, tc := range []struct {
		name        string
		firstEvents []ProviderEvent
		firstErr    error
	}{
		{
			name:        "adapter stream error",
			firstErr:    errors.New("connect failed"),
			firstEvents: nil,
		},
		{
			name:        "provider event error",
			firstEvents: []ProviderEvent{{Kind: ProviderEventError, Err: errors.New("provider failed")}},
		},
		{
			name:        "truncated stream",
			firstEvents: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "partial"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adapter := &scriptedAdapter{
				scripts: []adapterScript{
					{events: tc.firstEvents, err: tc.firstErr},
					{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "recovered"}, {Kind: ProviderEventDone}}},
				},
			}
			facade := newTestFacade(NewMemoryStore(), adapter)

			first := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"first"}`))
			first.Header.Set("x-codex-thread-id", "thread-retry")
			firstRec := httptest.NewRecorder()
			facade.ServeHTTP(firstRec, first)
			if firstRec.Code != http.StatusBadGateway {
				t.Fatalf("first status = %d, body = %s", firstRec.Code, firstRec.Body.String())
			}

			second := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"second"}`))
			second.Header.Set("x-codex-thread-id", "thread-retry")
			secondRec := httptest.NewRecorder()
			facade.ServeHTTP(secondRec, second)
			if secondRec.Code != http.StatusOK {
				t.Fatalf("second status = %d, body = %s", secondRec.Code, secondRec.Body.String())
			}
			if !strings.Contains(secondRec.Body.String(), `"output_text":"recovered"`) {
				t.Fatalf("second body = %s", secondRec.Body.String())
			}
		})
	}
}

type adapterScript struct {
	events []ProviderEvent
	err    error
}

type scriptedAdapter struct {
	mu      sync.Mutex
	scripts []adapterScript
	next    int
}

func (a *scriptedAdapter) Stream(context.Context, ProviderRequest) (<-chan ProviderEvent, error) {
	a.mu.Lock()
	if a.next >= len(a.scripts) {
		a.mu.Unlock()
		return nil, fmt.Errorf("unexpected adapter call")
	}
	script := a.scripts[a.next]
	a.next++
	a.mu.Unlock()
	if script.err != nil {
		return nil, script.err
	}
	ch := make(chan ProviderEvent, len(script.events))
	for _, event := range script.events {
		ch <- event
	}
	close(ch)
	return ch, nil
}
