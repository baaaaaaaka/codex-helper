package localproxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPProxy_HealthEndpoint(t *testing.T) {
	p := NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
		return nil, io.EOF
	}), Options{InstanceID: "health-id"})

	httpAddr, err := p.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Close(context.Background()) }()

	resp, err := http.Get("http://" + httpAddr + "/_codex_proxy/health")
	if err != nil {
		t.Fatalf("GET health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%s", resp.Status)
	}

	var body struct {
		OK         bool   `json:"ok"`
		InstanceID string `json:"instanceId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !body.OK || body.InstanceID != "health-id" {
		t.Fatalf("body=%#v", body)
	}
}

func TestHTTPProxyDefaultTimeoutsBoundIdleConnections(t *testing.T) {
	p := NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
		return nil, io.EOF
	}), Options{})
	if p.idle != defaultHTTPProxyIdleTimeout {
		t.Fatalf("idle timeout = %s, want %s", p.idle, defaultHTTPProxyIdleTimeout)
	}
	if p.tunnelIdle != defaultHTTPProxyTunnelIdleTimeout {
		t.Fatalf("tunnel idle timeout = %s, want %s", p.tunnelIdle, defaultHTTPProxyTunnelIdleTimeout)
	}
	if _, err := p.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Close(context.Background()) }()
	if p.server == nil || p.server.IdleTimeout != defaultHTTPProxyIdleTimeout {
		t.Fatalf("server idle timeout = %v, want %v", p.server, defaultHTTPProxyIdleTimeout)
	}
}

func TestHTTPProxyConnectIdleTunnelClosesBothSides(t *testing.T) {
	upstreamCh := make(chan net.Conn, 1)
	p := NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
		clientSide, upstreamSide := net.Pipe()
		upstreamCh <- upstreamSide
		return clientSide, nil
	}), Options{TunnelIdleTimeout: 20 * time.Millisecond})
	httpAddr, err := p.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Close(context.Background()) }()

	conn, err := net.DialTimeout("tcp", httpAddr, time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	_ = openConnectTunnel(t, conn)
	upstream := <-upstreamCh
	defer upstream.Close()

	assertConnClosesSoon(t, conn, "client tunnel")
	assertConnClosesSoon(t, upstream, "upstream tunnel")
}

func TestHTTPProxyConnectTunnelStaysOpenWithOneWayActivity(t *testing.T) {
	upstreamCh := make(chan net.Conn, 1)
	p := NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
		clientSide, upstreamSide := net.Pipe()
		upstreamCh <- upstreamSide
		return clientSide, nil
	}), Options{TunnelIdleTimeout: 750 * time.Millisecond})
	httpAddr, err := p.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Close(context.Background()) }()

	conn, err := net.DialTimeout("tcp", httpAddr, time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	br := openConnectTunnel(t, conn)
	upstream := <-upstreamCh
	defer upstream.Close()

	payload := []byte("abcdef")
	writeErr := make(chan error, 1)
	go func() {
		for _, b := range payload {
			time.Sleep(150 * time.Millisecond)
			if _, err := upstream.Write([]byte{b}); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()

	got := make([]byte, len(payload))
	for i := range got {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		b, err := br.ReadByte()
		if err != nil {
			t.Fatalf("read one-way tunnel byte %d: %v; got %q", i, err, string(got[:i]))
		}
		got[i] = b
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write one-way tunnel payload: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("one-way tunnel payload = %q, want %q", string(got), string(payload))
	}
}

func TestHTTPProxy_ForwardsPlainHTTP(t *testing.T) {
	originAddr, closeOrigin := startHTTPOrigin(t)
	defer closeOrigin()

	rec := &recordingDialer{}

	p := NewHTTPProxy(rec, Options{InstanceID: "plain-http"})
	httpAddr, err := p.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Close(context.Background()) }()

	proxyURL, _ := url.Parse("http://" + httpAddr)
	client := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		},
	}

	resp, err := client.Get("http://" + originAddr + "/hello")
	if err != nil {
		t.Fatalf("GET via proxy: %v", err)
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	if got := strings.TrimSpace(string(b)); got != "hello" {
		t.Fatalf("body=%q", got)
	}

	if !rec.SawAddr(originAddr) {
		t.Fatalf("expected dialer to see origin addr %q, got %#v", originAddr, rec.Addrs())
	}
}

func TestHTTPProxyPlainHTTPUsesOneShotOriginConnection(t *testing.T) {
	originAddr, requestCloseCh, originClosedCh, closeOrigin := startTrackingHTTPOrigin(t)
	defer closeOrigin()

	p := NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, 2*time.Second)
	}), Options{InstanceID: "plain-http-one-shot"})
	httpAddr, err := p.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Close(context.Background()) }()

	proxyURL, _ := url.Parse("http://" + httpAddr)
	client := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		},
	}

	resp, err := client.Get("http://" + originAddr + "/hello")
	if err != nil {
		t.Fatalf("GET via proxy: %v", err)
	}
	_ = resp.Body.Close()

	select {
	case got := <-requestCloseCh:
		if !got {
			t.Fatalf("proxy origin request did not ask the origin server to close the connection")
		}
	case <-time.After(time.Second):
		t.Fatalf("origin request was not observed")
	}

	select {
	case <-originClosedCh:
	case <-time.After(time.Second):
		t.Fatalf("proxied plain HTTP origin connection remained open after response")
	}
}

func TestHTTPProxyRejectsSelfLoopTargets(t *testing.T) {
	var called atomic.Bool
	p := NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
		called.Store(true)
		return nil, errors.New("dialer should not be called for self-loop target")
	}), Options{InstanceID: "self-loop"})

	httpAddr, err := p.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Close(context.Background()) }()

	_, port, err := net.SplitHostPort(httpAddr)
	if err != nil {
		t.Fatalf("split http addr: %v", err)
	}
	targets := []string{
		httpAddr,
		net.JoinHostPort("localhost", port),
	}

	for _, target := range targets {
		t.Run("http_"+target, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "http://"+target+"/loop", nil)
			p.handleHTTP(rec, req)
			if rec.Code != http.StatusLoopDetected {
				t.Fatalf("expected 508 for self-loop target %s, got %d", target, rec.Code)
			}
		})

		t.Run("connect_"+target, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodConnect, "http://"+target, nil)
			req.Host = target
			p.handleConnect(rec, req)
			if rec.Code != http.StatusLoopDetected {
				t.Fatalf("expected 508 for self-loop CONNECT target %s, got %d", target, rec.Code)
			}
		})
	}

	if called.Load() {
		t.Fatal("dialer was called for a self-loop target")
	}
}

func TestHTTPProxyServeHTTPRejectsSelfLoopTargets(t *testing.T) {
	var called atomic.Bool
	p := NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
		called.Store(true)
		return nil, errors.New("dialer should not be called for self-loop target")
	}), Options{InstanceID: "self-loop-server"})

	httpAddr, err := p.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Close(context.Background()) }()

	_, port, err := net.SplitHostPort(httpAddr)
	if err != nil {
		t.Fatalf("split http addr: %v", err)
	}
	targets := []string{
		httpAddr,
		net.JoinHostPort("localhost", port),
	}

	proxyURL, _ := url.Parse("http://" + httpAddr)
	client := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		},
	}

	for _, target := range targets {
		resp, err := client.Get("http://" + target + "/loop")
		if err != nil {
			t.Fatalf("GET self-loop target %s: %v", target, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusLoopDetected {
			t.Fatalf("expected 508 for self-loop target %s, got %d", target, resp.StatusCode)
		}
	}

	if called.Load() {
		t.Fatal("dialer was called for a self-loop target")
	}
}

func TestHTTPProxySelfTargetDetection(t *testing.T) {
	p := &HTTPProxy{addr: "127.0.0.1:4751"}

	tests := []struct {
		name string
		addr string
		want bool
	}{
		{name: "ipv4 loopback", addr: "127.0.0.1:4751", want: true},
		{name: "localhost", addr: "localhost:4751", want: true},
		{name: "localhost trailing dot", addr: "localhost.:4751", want: true},
		{name: "ipv6 loopback", addr: "[::1]:4751", want: true},
		{name: "different loopback port", addr: "127.0.0.1:4752", want: false},
		{name: "non loopback same port", addr: "192.0.2.10:4751", want: false},
		{name: "hostname same port", addr: "example.com:4751", want: false},
		{name: "missing port", addr: "127.0.0.1", want: false},
		{name: "malformed", addr: "not a host:port", want: false},
		{name: "empty", addr: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := p.isSelfTarget(tt.addr); got != tt.want {
				t.Fatalf("isSelfTarget(%q) = %v, want %v", tt.addr, got, tt.want)
			}
		})
	}
}

func TestHTTPProxyEdgeCases(t *testing.T) {
	t.Run("Start rejects double start", func(t *testing.T) {
		p := NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
			return nil, io.EOF
		}), Options{})
		addr, err := p.Start("127.0.0.1:0")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = p.Close(context.Background()) }()
		if _, err := p.Start(addr); err == nil {
			t.Fatalf("expected Start to fail when already started")
		}
	})

	t.Run("Start fails when port is occupied", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		defer ln.Close()
		addr := ln.Addr().String()
		p := NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
			return nil, io.EOF
		}), Options{})
		if _, err := p.Start(addr); err == nil {
			t.Fatalf("expected Start to fail for occupied port")
		}
	})

	t.Run("Close releases listener", func(t *testing.T) {
		p := NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
			return nil, io.EOF
		}), Options{})
		addr, err := p.Start("127.0.0.1:0")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := p.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			t.Fatalf("expected listener at %s to be closed", addr)
		}
		restartedAddr, err := p.Start("127.0.0.1:0")
		if err != nil {
			t.Fatalf("Start after Close: %v", err)
		}
		defer func() { _ = p.Close(context.Background()) }()
		if restartedAddr == "" {
			t.Fatal("expected restarted proxy address")
		}
	})

	t.Run("handleConnect validates host and hijack", func(t *testing.T) {
		p := NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
			return nil, errors.New("dial failed")
		}), Options{})
		rec := httptest.NewRecorder()
		req := &http.Request{Method: http.MethodConnect}
		p.handleConnect(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for missing host, got %d", rec.Code)
		}

		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodConnect, "http://example.com", nil)
		req.Host = "example.com:443"
		p.handleConnect(rec, req)
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("expected 502 for dial failure, got %d", rec.Code)
		}

		upstream, downstream := net.Pipe()
		defer upstream.Close()
		defer downstream.Close()
		p = NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
			return upstream, nil
		}), Options{})
		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodConnect, "http://example.com", nil)
		req.Host = "example.com:443"
		p.handleConnect(rec, req)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500 for missing hijacker, got %d", rec.Code)
		}
	})

	t.Run("handleHTTP forwards non-200 status", func(t *testing.T) {
		originAddr, closeOrigin := startHTTPOrigin(t)
		defer closeOrigin()
		originURL := "http://" + originAddr + "/fail"

		p := NewHTTPProxy(dialerFunc(func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, 2*time.Second)
		}), Options{})
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, originURL, nil)
		p.handleHTTP(rec, req)
		if rec.Code == http.StatusOK {
			t.Fatalf("expected non-200 status")
		}
	})

	t.Run("NewSOCKS5Dialer defaults timeout", func(t *testing.T) {
		d, err := NewSOCKS5Dialer("127.0.0.1:9999", 0)
		if err != nil {
			t.Fatalf("NewSOCKS5Dialer error: %v", err)
		}
		if d == nil {
			t.Fatalf("expected dialer to be created")
		}
	})
}

func TestIsClosedNetworkErrorAcceptsWindowsCloseMessage(t *testing.T) {
	err := &net.OpError{Op: "close", Net: "tcp", Err: errors.New("use of closed network connection")}
	if !isClosedNetworkError(err) {
		t.Fatalf("isClosedNetworkError(%v) = false, want true", err)
	}
}

func startHTTPOrigin(t *testing.T) (addr string, closeFn func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen origin: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("hello"))
	})

	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()

	return ln.Addr().String(), func() {
		_ = srv.Shutdown(context.Background())
		_ = ln.Close()
	}
}

func startTrackingHTTPOrigin(t *testing.T) (addr string, requestCloseCh <-chan bool, closedCh <-chan struct{}, closeFn func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen origin: %v", err)
	}

	requestClose := make(chan bool, 1)
	closed := make(chan struct{}, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		requestClose <- r.Close
		_, _ = w.Write([]byte("hello"))
	})

	srv := &http.Server{
		Handler: mux,
		ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateClosed {
				select {
				case closed <- struct{}{}:
				default:
				}
			}
		},
	}
	go func() { _ = srv.Serve(ln) }()

	return ln.Addr().String(), requestClose, closed, func() {
		_ = srv.Shutdown(context.Background())
		_ = ln.Close()
	}
}

func openConnectTunnel(t *testing.T, conn net.Conn) *bufio.Reader {
	t.Helper()
	if _, err := io.WriteString(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read CONNECT status: %v", err)
	}
	if !strings.Contains(status, "200") {
		t.Fatalf("CONNECT status = %q, want 200", status)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read CONNECT headers: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}
	return br
}

func assertConnClosesSoon(t *testing.T, conn net.Conn, label string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	buf := make([]byte, 1)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		n, err := conn.Read(buf)
		if n > 0 {
			t.Fatalf("%s unexpectedly produced data while waiting for idle close", label)
		}
		if err == nil {
			continue
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			continue
		}
		return
	}
	t.Fatalf("%s did not close after tunnel idle timeout", label)
}

type recordingDialer struct {
	mu    sync.Mutex
	addrs []string
}

func (d *recordingDialer) Dial(network, addr string) (net.Conn, error) {
	d.mu.Lock()
	d.addrs = append(d.addrs, addr)
	d.mu.Unlock()
	return net.DialTimeout(network, addr, 2*time.Second)
}

func (d *recordingDialer) Addrs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.addrs))
	copy(out, d.addrs)
	return out
}

func (d *recordingDialer) SawAddr(addr string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, a := range d.addrs {
		if a == addr {
			return true
		}
	}
	return false
}
