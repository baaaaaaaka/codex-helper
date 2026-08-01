package appgateway

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestProbeAcceptsPendingHealthWithIdentity(t *testing.T) {
	server, port := startProbeTestServer(t, http.StatusServiceUnavailable, `{"ok":false,"instanceId":"gateway-1","recovery":"pending"}`)
	defer server.Close()
	health, err := Probe(context.Background(), port, time.Second)
	if err != nil || health.OK || health.InstanceID != "gateway-1" || health.Recovery != StatePending {
		t.Fatalf("pending health = %#v/%v", health, err)
	}
}

func TestProbeRejectsMissingIdentityAndUnexpectedStatus(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "missing identity", status: http.StatusOK, body: `{"ok":true}`},
		{name: "redirect", status: http.StatusFound, body: `{"ok":true,"instanceId":"gateway-1"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, port := startProbeTestServer(t, tc.status, tc.body)
			defer server.Close()
			if _, err := Probe(context.Background(), port, time.Second); err == nil {
				t.Fatal("invalid health response was accepted")
			}
		})
	}
}

func TestProbeHonorsCancellation(t *testing.T) {
	server, port := startProbeTestServer(t, http.StatusOK, `{"ok":true,"instanceId":"gateway-1"}`)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Probe(ctx, port, time.Second); err == nil {
		t.Fatal("cancelled health probe succeeded")
	}
}

func startProbeTestServer(t *testing.T, status int, body string) (*http.Server, int) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_codex_proxy/health" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, body)
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return server, listener.Addr().(*net.TCPAddr).Port
}
