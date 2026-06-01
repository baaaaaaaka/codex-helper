package manager

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestHealthClient_CheckHTTPProxy(t *testing.T) {
	port, closeFn := startHealthOnlyServer(t, "inst-1")
	defer closeFn()

	hc := HealthClient{Timeout: 1 * time.Second}
	if err := hc.CheckHTTPProxy(port, "inst-1"); err != nil {
		t.Fatalf("CheckHTTPProxy: %v", err)
	}
	if err := hc.CheckHTTPProxy(port, "wrong"); err == nil {
		t.Fatalf("expected instance id mismatch error")
	}
}

func TestHealthClient_CheckHTTPProxyContextCancellation(t *testing.T) {
	port, closeFn := startBlockingHealthServer(t)
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := (HealthClient{Timeout: 3 * time.Second}).CheckHTTPProxyContext(ctx, port, "inst-1")
	if err == nil {
		t.Fatal("CheckHTTPProxyContext error = nil, want cancellation")
	}
	if ctx.Err() == nil {
		t.Fatalf("context was not canceled: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 1500*time.Millisecond {
		t.Fatalf("health check ignored context cancellation; elapsed=%s err=%v", elapsed, err)
	}
}

func TestHealthClient_CheckHTTPProxyUsesOneShotConnection(t *testing.T) {
	closeCh := make(chan struct{}, 1)
	requestCloseCh := make(chan bool, 1)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/_codex_proxy/health", func(w http.ResponseWriter, r *http.Request) {
		requestCloseCh <- r.Close
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":         true,
			"instanceId": "inst-one-shot",
		})
	})
	srv := &http.Server{
		Handler: mux,
		ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateClosed {
				select {
				case closeCh <- struct{}{}:
				default:
				}
			}
		},
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		_ = srv.Shutdown(context.Background())
		_ = ln.Close()
	}()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	tcp, err := net.ResolveTCPAddr("tcp", "127.0.0.1:"+portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}

	hc := HealthClient{Timeout: 1 * time.Second}
	if err := hc.CheckHTTPProxy(tcp.Port, "inst-one-shot"); err != nil {
		t.Fatalf("CheckHTTPProxy: %v", err)
	}

	select {
	case got := <-requestCloseCh:
		if !got {
			t.Fatalf("health request did not ask the proxy server to close the connection")
		}
	case <-time.After(time.Second):
		t.Fatalf("health request was not observed")
	}

	select {
	case <-closeCh:
	case <-time.After(time.Second):
		t.Fatalf("health check connection remained open after the one-shot probe")
	}
}

func TestHealthClient_CheckHTTPProxyCONNECT(t *testing.T) {
	port, closeFn := startConnectProxyServer(t, http.StatusOK)
	defer closeFn()

	hc := HealthClient{Timeout: time.Second}
	if err := hc.CheckHTTPProxyCONNECT(port, "graph.example.test:443"); err != nil {
		t.Fatalf("CheckHTTPProxyCONNECT: %v", err)
	}
}

func TestHealthClient_CheckHTTPProxyCONNECTErrors(t *testing.T) {
	hc := HealthClient{Timeout: 200 * time.Millisecond}

	t.Run("invalid target", func(t *testing.T) {
		if err := hc.CheckHTTPProxyCONNECT(1, "graph.example.test"); err == nil {
			t.Fatalf("expected invalid target error")
		}
	})

	t.Run("non-200 connect", func(t *testing.T) {
		port, closeFn := startConnectProxyServer(t, http.StatusBadGateway)
		defer closeFn()
		if err := hc.CheckHTTPProxyCONNECT(port, "graph.example.test:443"); err == nil {
			t.Fatalf("expected CONNECT status error")
		}
	})
}

func startConnectProxyServer(t *testing.T, connectStatus int) (port int, closeFn func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			t.Fatalf("unexpected method %s", r.Method)
		}
		w.WriteHeader(connectStatus)
	})

	srv := &http.Server{Handler: handler}
	go func() { _ = srv.Serve(ln) }()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	tcp, err := net.ResolveTCPAddr("tcp", "127.0.0.1:"+portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}

	return tcp.Port, func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
		}
		_ = ln.Close()
	}
}

func startHealthOnlyServer(t *testing.T, instanceID string) (port int, closeFn func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/_codex_proxy/health", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":         true,
			"instanceId": instanceID,
		})
	})

	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	tcp, err := net.ResolveTCPAddr("tcp", "127.0.0.1:"+portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}

	return tcp.Port, func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
		}
		_ = ln.Close()
	}
}

func startBlockingHealthServer(t *testing.T) (port int, closeFn func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/_codex_proxy/health", func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	tcp, err := net.ResolveTCPAddr("tcp", "127.0.0.1:"+portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}

	return tcp.Port, func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
		}
		_ = ln.Close()
	}
}

func TestHealthClient_CheckHTTPProxyErrors(t *testing.T) {
	hc := HealthClient{Timeout: 200 * time.Millisecond}

	t.Run("invalid port", func(t *testing.T) {
		if err := hc.CheckHTTPProxy(0, ""); err == nil {
			t.Fatalf("expected invalid port error")
		}
	})

	t.Run("non-200 status", func(t *testing.T) {
		port, closeFn := startCustomHealthServer(t, http.StatusInternalServerError, `{"ok":true,"instanceId":"x"}`)
		defer closeFn()
		if err := hc.CheckHTTPProxy(port, ""); err == nil {
			t.Fatalf("expected status error")
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		port, closeFn := startCustomHealthServer(t, http.StatusOK, "not-json")
		defer closeFn()
		if err := hc.CheckHTTPProxy(port, ""); err == nil {
			t.Fatalf("expected json decode error")
		}
	})

	t.Run("ok false", func(t *testing.T) {
		port, closeFn := startCustomHealthServer(t, http.StatusOK, `{"ok":false,"instanceId":"x"}`)
		defer closeFn()
		if err := hc.CheckHTTPProxy(port, ""); err == nil {
			t.Fatalf("expected health not ok error")
		}
	})
}

func startCustomHealthServer(t *testing.T, status int, body string) (port int, closeFn func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/_codex_proxy/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})

	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	tcp, err := net.ResolveTCPAddr("tcp", "127.0.0.1:"+portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}

	return tcp.Port, func() {
		_ = srv.Shutdown(context.Background())
		_ = ln.Close()
	}
}
