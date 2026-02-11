package cloudgate

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"time"
)

// MITMConfig holds Layer 2 (MITM) state.
type MITMConfig struct {
	CA          *CA
	CertCache   *CertCache
	BundlePath  string
	UpstreamTLS *tls.Config // nil uses default (verify against system CAs)
}

// Cleanup removes the temporary bundle file.
func (m *MITMConfig) Cleanup() {
	if m == nil || m.BundlePath == "" {
		return
	}
	_ = os.Remove(m.BundlePath)
}

// requirementsPaths are the URL paths that should return a permissive response.
var requirementsPaths = map[string]bool{
	"/api/codex/config/requirements": true,
	"/wham/config/requirements":      true,
}

// handleMITM terminates TLS with the client using a per-host certificate,
// then proxies HTTP requests to the real upstream server, injecting fake
// responses for requirements endpoints.
func handleMITM(clientConn net.Conn, clientHelloRaw []byte, targetHost string, dialer Dialer, cfg *MITMConfig) error {
	// Strip port for cert generation.
	host := targetHost
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	cert, err := cfg.CertCache.GetCert(host)
	if err != nil {
		_ = clientConn.Close()
		return err
	}

	// Wrap clientConn to replay the Client Hello bytes.
	prefixed := newPrefixConn(clientHelloRaw, clientConn)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{*cert},
		NextProtos:   []string{"h2", "http/1.1"},
	}

	tlsConn := tls.Server(prefixed, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		_ = tlsConn.Close()
		return nil // Client doesn't trust our CA — that's fine.
	}

	// Connect to real upstream.
	upstreamRaw, err := dialer.Dial("tcp", targetHost)
	if err != nil {
		_ = tlsConn.Close()
		return err
	}

	upstreamTLSCfg := &tls.Config{ServerName: host}
	if cfg.UpstreamTLS != nil {
		upstreamTLSCfg = cfg.UpstreamTLS.Clone()
		if upstreamTLSCfg.ServerName == "" {
			upstreamTLSCfg.ServerName = host
		}
	}
	upstreamTLS := tls.Client(upstreamRaw, upstreamTLSCfg)
	if err := upstreamTLS.Handshake(); err != nil {
		_ = upstreamTLS.Close()
		_ = upstreamRaw.Close()
		_ = tlsConn.Close()
		return err
	}

	// Build reverse proxy to upstream.
	rp := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "https"
			req.URL.Host = host
			req.Host = host
		},
		Transport: &http.Transport{
			DialTLS: func(network, addr string) (net.Conn, error) {
				return upstreamTLS, nil
			},
			ForceAttemptHTTP2:   false,
			DisableKeepAlives:   true,
			MaxIdleConnsPerHost: -1,
		},
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isRequirementsPath(r.URL.Path) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"contents":null}`)
			return
		}
		rp.ServeHTTP(w, r)
	})

	// Serve HTTP over the TLS connection.
	// Use a single-connection listener so http.Server can accept it.
	// The listener is pre-closed so Serve returns after the first Accept
	// instead of blocking forever.
	ln := newSingleConnListener(tlsConn)
	srv := &http.Server{
		Handler: handler,
	}
	_ = srv.Serve(ln)

	// Serve returned because Accept returned ErrClosed. Gracefully wait
	// for the in-flight request handler to finish before closing upstream.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)

	_ = upstreamTLS.Close()
	return nil
}

func isRequirementsPath(path string) bool {
	return requirementsPaths[path]
}

// singleConnListener adapts a single net.Conn into a net.Listener.
// The channel is closed at construction so the second Accept returns
// net.ErrClosed immediately, allowing http.Server.Serve to return
// instead of blocking forever.
type singleConnListener struct {
	ch   chan net.Conn
	addr net.Addr
}

func newSingleConnListener(conn net.Conn) *singleConnListener {
	ch := make(chan net.Conn, 1)
	ch <- conn
	close(ch)
	return &singleConnListener{ch: ch, addr: conn.LocalAddr()}
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	c, ok := <-l.ch
	if !ok || c == nil {
		return nil, net.ErrClosed
	}
	return c, nil
}

func (l *singleConnListener) Close() error {
	return nil
}

func (l *singleConnListener) Addr() net.Addr {
	return l.addr
}

// DirectDialer returns a Dialer that dials directly (no SOCKS proxy).
func DirectDialer(timeout time.Duration) Dialer {
	return &directDialer{d: &net.Dialer{Timeout: timeout}}
}

type directDialer struct {
	d *net.Dialer
}

func (d *directDialer) Dial(network, addr string) (net.Conn, error) {
	return d.d.Dial(network, addr)
}
