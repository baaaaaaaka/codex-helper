package cloudgate

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func setupMITMTest(t *testing.T) (*CA, *CertCache) {
	t.Helper()
	ca, err := EnsureCA(t.TempDir())
	if err != nil {
		t.Fatalf("EnsureCA: %v", err)
	}
	return ca, NewCertCache(ca)
}

// startUpstreamTLS starts a local TLS server to act as the "real" upstream.
func startUpstreamTLS(t *testing.T, handler http.Handler) (addr string, cleanup func()) {
	t.Helper()

	// Use a separate CA for the upstream to keep things clean.
	upCA, err := EnsureCA(t.TempDir())
	if err != nil {
		t.Fatalf("upstream EnsureCA: %v", err)
	}
	upCC := NewCertCache(upCA)
	upCert, err := upCC.GetCert("localhost")
	if err != nil {
		t.Fatalf("upstream GetCert: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	tlsLn := tls.NewListener(ln, &tls.Config{
		Certificates: []tls.Certificate{*upCert},
	})

	srv := &http.Server{Handler: handler}
	go func() { _ = srv.Serve(tlsLn) }()

	return ln.Addr().String(), func() { _ = srv.Close() }
}

// fixedDialer always dials the given address regardless of the requested target.
type fixedDialer struct {
	addr string
}

func (d *fixedDialer) Dial(_, _ string) (net.Conn, error) {
	return net.DialTimeout("tcp", d.addr, 5*time.Second)
}

func TestMITMRequirementsInjection(t *testing.T) {
	ca, cc := setupMITMTest(t)

	// Track whether upstream was called.
	var upstreamHit atomic.Int32
	upstreamAddr, upstreamCleanup := startUpstreamTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHit.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"contents":{"approval_policy":["OnRequest"]}}`)
	}))
	defer upstreamCleanup()

	mitmCfg := &MITMConfig{
		CA:        ca,
		CertCache: cc,
		// Skip upstream TLS verification since upstream is self-signed.
		UpstreamTLS: &tls.Config{InsecureSkipVerify: true},
	}

	// TCP listener to simulate the proxy accepting a CONNECT-hijacked connection.
	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	defer proxyLn.Close()

	dialer := &fixedDialer{addr: upstreamAddr}

	done := make(chan error, 1)
	go func() {
		conn, err := proxyLn.Accept()
		if err != nil {
			done <- err
			return
		}
		// Simulate what the proxy does: read Client Hello, then hand to MITM.
		raw, _, readErr := ReadClientHello(conn)
		if readErr != nil {
			done <- readErr
			return
		}
		done <- handleMITM(conn, raw, "chatgpt.com:443", dialer, mitmCfg)
	}()

	// Client connects to proxy and does TLS handshake.
	rawConn, err := net.DialTimeout("tcp", proxyLn.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	tlsConn := tls.Client(rawConn, &tls.Config{
		ServerName: "chatgpt.com",
		RootCAs:    pool,
		NextProtos: []string{"http/1.1"},
	})
	defer tlsConn.Close()

	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("client TLS handshake: %v", err)
	}

	// Send HTTP request for requirements path.
	req, _ := http.NewRequest("GET", "https://chatgpt.com/api/codex/config/requirements", nil)
	if err := req.Write(tlsConn); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(tlsConn), req)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"contents":null}` {
		t.Errorf("expected injected response, got %q", body)
	}

	if upstreamHit.Load() != 0 {
		t.Error("upstream was called — requirements should be intercepted")
	}
}

func TestMITMForwardsOtherRequests(t *testing.T) {
	ca, cc := setupMITMTest(t)

	var upstreamHit atomic.Int32
	upstreamAddr, upstreamCleanup := startUpstreamTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHit.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "upstream-ok")
	}))
	defer upstreamCleanup()

	mitmCfg := &MITMConfig{
		CA:          ca,
		CertCache:   cc,
		UpstreamTLS: &tls.Config{InsecureSkipVerify: true},
	}

	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	defer proxyLn.Close()

	dialer := &fixedDialer{addr: upstreamAddr}

	done := make(chan error, 1)
	go func() {
		conn, err := proxyLn.Accept()
		if err != nil {
			done <- err
			return
		}
		raw, _, readErr := ReadClientHello(conn)
		if readErr != nil {
			done <- readErr
			return
		}
		done <- handleMITM(conn, raw, "chatgpt.com:443", dialer, mitmCfg)
	}()

	rawConn, err := net.DialTimeout("tcp", proxyLn.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	tlsConn := tls.Client(rawConn, &tls.Config{
		ServerName: "chatgpt.com",
		RootCAs:    pool,
		NextProtos: []string{"http/1.1"},
	})
	defer tlsConn.Close()

	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("client TLS handshake: %v", err)
	}

	// Request a non-requirements path — should be forwarded to upstream.
	req, _ := http.NewRequest("GET", "https://chatgpt.com/api/other/endpoint", nil)
	if err := req.Write(tlsConn); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(tlsConn), req)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "upstream-ok" {
		t.Errorf("expected upstream response, got %q", body)
	}

	if upstreamHit.Load() != 1 {
		t.Errorf("expected upstream to be called once, got %d", upstreamHit.Load())
	}
}

func TestMITMTLSFailure(t *testing.T) {
	ca, cc := setupMITMTest(t)

	mitmCfg := &MITMConfig{
		CA:        ca,
		CertCache: cc,
	}

	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	defer proxyLn.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := proxyLn.Accept()
		if err != nil {
			done <- err
			return
		}
		raw, _, readErr := ReadClientHello(conn)
		if readErr != nil {
			done <- readErr
			return
		}
		// Use a dialer that fails — we shouldn't even get to the upstream
		// because the client TLS handshake should fail first.
		done <- handleMITM(conn, raw, "chatgpt.com:443", &mockDialer{err: net.ErrClosed}, mitmCfg)
	}()

	rawConn, err := net.DialTimeout("tcp", proxyLn.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}

	// Client connects with NO trust for our CA → handshake should fail.
	tlsConn := tls.Client(rawConn, &tls.Config{
		ServerName: "chatgpt.com",
		// No RootCAs → system CAs, which don't include our test CA.
	})
	defer tlsConn.Close()

	_ = tlsConn.SetDeadline(time.Now().Add(5 * time.Second))
	err = tlsConn.Handshake()
	if err == nil {
		t.Error("expected TLS handshake to fail (untrusted CA)")
	}

	// handleMITM should return nil (graceful handling of untrusted client).
	select {
	case mitmErr := <-done:
		if mitmErr != nil {
			t.Logf("handleMITM returned error (expected nil): %v", mitmErr)
		}
	case <-time.After(5 * time.Second):
		t.Error("handleMITM didn't return within timeout")
	}
}

func TestIsRequirementsPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/api/codex/config/requirements", true},
		{"/wham/config/requirements", true},
		{"/api/other/endpoint", false},
		{"/", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isRequirementsPath(tt.path); got != tt.want {
			t.Errorf("isRequirementsPath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}
