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
