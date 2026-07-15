package responsesadapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestDefaultUpstreamHTTPClientTimesOutWaitingForResponseHeaders(t *testing.T) {
	oldTimeout := upstreamHTTPResponseHeaderTimeout
	upstreamHTTPResponseHeaderTimeout = 25 * time.Millisecond
	defer func() {
		upstreamHTTPResponseHeaderTimeout = oldTimeout
	}()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{
		BaseURL:    server.URL + "/v1",
		MaxRetries: -1,
	}
	started := time.Now()
	_, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("expected response header timeout")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("error = %v, want timeout", err)
	}
	if elapsed >= 150*time.Millisecond {
		t.Fatalf("Stream waited %s, want response header timeout before delayed server write", elapsed)
	}
}

func TestOpenAIChatAdapterEmitsErrorWhenSSEBodyStaysIdle(t *testing.T) {
	oldTimeout := upstreamHTTPStreamIdleTimeout
	upstreamHTTPStreamIdleTimeout = 25 * time.Millisecond
	defer func() {
		upstreamHTTPStreamIdleTimeout = oldTimeout
	}()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(200 * time.Millisecond)
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{
		BaseURL:    server.URL + "/v1",
		HTTPClient: server.Client(),
		MaxRetries: -1,
	}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	events := collectEvents(stream)
	if len(events) != 1 || events[0].Kind != ProviderEventError {
		t.Fatalf("events = %#v, want one error event", events)
	}
	if events[0].Err == nil || !strings.Contains(events[0].Err.Error(), "idle timeout") {
		t.Fatalf("error = %v, want idle timeout", events[0].Err)
	}
}

func TestOpenAIChatAdapterUsesConfiguredStreamIdleTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(200 * time.Millisecond)
	}))
	defer server.Close()

	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client(), MaxRetries: -1, StreamIdleTimeout: 25 * time.Millisecond}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectEvents(stream)
	if len(events) != 1 || events[0].Err == nil || !strings.Contains(events[0].Err.Error(), "25ms") {
		t.Fatalf("events = %#v, want configured idle timeout", events)
	}
}

func TestOpenAIChatAdapterDistinguishesSemanticProgressFromTransportHeartbeats(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"start\"}}]}\n\n"))
		flusher.Flush()
		for i := 0; i < 10; i++ {
			_, _ = w.Write([]byte(": heartbeat\n\n"))
			flusher.Flush()
			time.Sleep(8 * time.Millisecond)
		}
	}))
	defer server.Close()
	adapter := OpenAIChatAdapter{
		BaseURL: server.URL + "/v1", HTTPClient: server.Client(), MaxRetries: -1,
		StreamIdleTimeout: 200 * time.Millisecond, SemanticProgressTimeout: 25 * time.Millisecond,
	}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectEvents(stream)
	if len(events) == 0 || events[len(events)-1].Kind != ProviderEventError {
		t.Fatalf("events = %#v", events)
	}
	timeout, ok := events[len(events)-1].Err.(ProviderTimeoutError)
	if !ok || timeout.Kind != ProviderTimeoutSemanticProgress {
		t.Fatalf("error = %#v, want semantic progress timeout", events[len(events)-1].Err)
	}

	t.Run("semantic heartbeat counts as progress", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher, _ := w.(http.Flusher)
			for i := 0; i < 20; i++ {
				_, _ = w.Write([]byte(": heartbeat\n\n"))
				flusher.Flush()
				time.Sleep(3 * time.Millisecond)
			}
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			flusher.Flush()
		}))
		defer server.Close()
		adapter := OpenAIChatAdapter{
			BaseURL: server.URL + "/v1", HTTPClient: server.Client(), MaxRetries: -1,
			StreamIdleTimeout: 200 * time.Millisecond, SemanticProgressTimeout: 20 * time.Millisecond,
			HeartbeatMode: "semantic",
		}
		stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
		if err != nil {
			t.Fatal(err)
		}
		events := collectEvents(stream)
		if len(events) == 0 || events[len(events)-1].Kind != ProviderEventDone {
			t.Fatalf("events = %#v, semantic heartbeats should keep the request alive", events)
		}
	})
}

func TestOpenAIChatAdapterFirstEventAndHardDeadline(t *testing.T) {
	t.Run("first event", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			time.Sleep(200 * time.Millisecond)
		}))
		defer server.Close()
		adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client(), MaxRetries: -1, StreamIdleTimeout: time.Second, FirstEventTimeout: 25 * time.Millisecond}
		stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a"})
		if err != nil {
			t.Fatal(err)
		}
		events := collectEvents(stream)
		if len(events) == 0 {
			t.Fatal("no events")
		}
		timeout, ok := events[len(events)-1].Err.(ProviderTimeoutError)
		if !ok || timeout.Kind != ProviderTimeoutFirstEvent {
			t.Fatalf("error = %#v", events[len(events)-1].Err)
		}
	})
	t.Run("hard deadline", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher, _ := w.(http.Flusher)
			for i := 0; i < 20; i++ {
				_, _ = w.Write([]byte(": heartbeat\n\n"))
				flusher.Flush()
				time.Sleep(8 * time.Millisecond)
			}
		}))
		defer server.Close()
		adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client(), MaxRetries: -1, StreamIdleTimeout: time.Second, MaxDuration: 30 * time.Millisecond}
		stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a"})
		if err != nil {
			t.Fatal(err)
		}
		events := collectEvents(stream)
		if len(events) == 0 {
			t.Fatal("no events")
		}
		timeout, ok := events[len(events)-1].Err.(ProviderTimeoutError)
		if !ok || timeout.Kind != ProviderTimeoutDeadline {
			t.Fatalf("error = %#v", events[len(events)-1].Err)
		}
	})
}

func TestOpenAIChatAdapterRejectsConfiguredPlainTextToolCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"<tool_call>read_file\"}}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	profile := ProviderProfile{ID: "generic", PlainTextToolCall: "reject"}
	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client(), MaxRetries: -1, Profile: profile}
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectEvents(stream)
	if len(events) == 0 || events[len(events)-1].Kind != ProviderEventError || !strings.Contains(events[len(events)-1].Err.Error(), "plain-text tool call") {
		t.Fatalf("events = %#v", events)
	}
}

func TestOpenAIChatAdapterExplicitZeroRetries(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(529)
	}))
	defer server.Close()
	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client(), MaxRetriesSet: true}
	if _, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"}); err == nil {
		t.Fatal("expected HTTP 529 error")
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want one attempt", requests)
	}
}

func TestOpenAIChatAdapterCanIgnoreRetryAfter(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(529)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()
	ignore := false
	adapter := OpenAIChatAdapter{BaseURL: server.URL + "/v1", HTTPClient: server.Client(), MaxRetries: 1, MaxRetriesSet: true, RetryBase: time.Millisecond, HonorRetryAfter: &ignore}
	started := time.Now()
	stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: "model-a", InputText: "x"})
	if err != nil {
		t.Fatal(err)
	}
	_ = collectEvents(stream)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("retry honored Retry-After despite policy false: %s", elapsed)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want two attempts", requests)
	}
}

func TestNewUpstreamHTTPClientConfiguresBoundedProxyTransport(t *testing.T) {
	proxyURL := &url.URL{Scheme: "http", Host: "127.0.0.1:9"}
	client := NewUpstreamHTTPClient(http.ProxyURL(proxyURL))

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", client.Transport)
	}
	if transport.Proxy == nil {
		t.Fatal("proxy function is nil")
	}
	if transport.DialContext == nil {
		t.Fatal("DialContext is nil")
	}
	if transport.TLSHandshakeTimeout != upstreamHTTPTLSHandshakeTimeout {
		t.Fatalf("TLSHandshakeTimeout = %s, want %s", transport.TLSHandshakeTimeout, upstreamHTTPTLSHandshakeTimeout)
	}
	if transport.ResponseHeaderTimeout != upstreamHTTPResponseHeaderTimeout {
		t.Fatalf("ResponseHeaderTimeout = %s, want %s", transport.ResponseHeaderTimeout, upstreamHTTPResponseHeaderTimeout)
	}
	if transport.ExpectContinueTimeout != upstreamHTTPExpectContinueTimeout {
		t.Fatalf("ExpectContinueTimeout = %s, want %s", transport.ExpectContinueTimeout, upstreamHTTPExpectContinueTimeout)
	}
	if transport.IdleConnTimeout != upstreamHTTPIdleConnTimeout {
		t.Fatalf("IdleConnTimeout = %s, want %s", transport.IdleConnTimeout, upstreamHTTPIdleConnTimeout)
	}
}

func TestNewUpstreamHTTPClientUsesConfiguredResponseHeaderTimeout(t *testing.T) {
	client := NewUpstreamHTTPClientWithResponseHeaderTimeout(nil, 17*time.Second)
	transport := client.Transport.(*http.Transport)
	if transport.ResponseHeaderTimeout != 17*time.Second {
		t.Fatalf("ResponseHeaderTimeout = %s", transport.ResponseHeaderTimeout)
	}
}
