package localproxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	xproxy "golang.org/x/net/proxy"
)

type Dialer interface {
	Dial(network, addr string) (net.Conn, error)
}

type HTTPProxy struct {
	instanceID string
	dialer     Dialer

	mu       sync.Mutex
	listener net.Listener
	server   *http.Server
	addr     string
}

type Options struct {
	InstanceID string
}

func NewHTTPProxy(d Dialer, opts Options) *HTTPProxy {
	return &HTTPProxy{
		instanceID: opts.InstanceID,
		dialer:     d,
	}
}

func NewSOCKS5Dialer(socksAddr string, timeout time.Duration) (Dialer, error) {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	forward := &net.Dialer{Timeout: timeout}
	fwd := dialerFunc(func(network, addr string) (net.Conn, error) {
		return forward.Dial(network, addr)
	})

	d, err := xproxy.SOCKS5("tcp", socksAddr, nil, fwd)
	if err != nil {
		return nil, err
	}
	return d, nil
}

type dialerFunc func(network, addr string) (net.Conn, error)

func (d dialerFunc) Dial(network, addr string) (net.Conn, error) { return d(network, addr) }

func (p *HTTPProxy) Start(listenAddr string) (actualAddr string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.listener != nil {
		return "", errors.New("proxy already started")
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return "", err
	}

	srv := &http.Server{
		Handler:           http.HandlerFunc(p.serveHTTP),
		ReadHeaderTimeout: 10 * time.Second,
	}

	p.listener = ln
	p.server = srv
	p.addr = ln.Addr().String()

	go func() {
		_ = srv.Serve(ln)
	}()

	return ln.Addr().String(), nil
}

func (p *HTTPProxy) Close(ctx context.Context) error {
	p.mu.Lock()
	srv := p.server
	ln := p.listener
	p.server = nil
	p.listener = nil
	p.addr = ""
	p.mu.Unlock()

	var closeErr error
	if ln != nil {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErr = err
		}
	}
	if srv != nil {
		if err := srv.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			closeErr = errors.Join(closeErr, err)
		}
	}
	return closeErr
}

func (p *HTTPProxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	// Local health check (not proxied).
	if r.Method == http.MethodGet && r.URL.Path == "/_codex_proxy/health" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":         true,
			"instanceId": p.instanceID,
		})
		return
	}

	if strings.EqualFold(r.Method, http.MethodConnect) {
		p.handleConnect(w, r)
		return
	}

	p.handleHTTP(w, r)
}

func (p *HTTPProxy) handleConnect(w http.ResponseWriter, r *http.Request) {
	dest := r.Host
	if dest == "" {
		http.Error(w, "missing host", http.StatusBadRequest)
		return
	}
	if p.isSelfTarget(dest) {
		http.Error(w, "refusing to proxy request back to this codex-proxy listener", http.StatusLoopDetected)
		return
	}

	upstream, err := p.dialer.Dial("tcp", dest)
	if err != nil {
		http.Error(w, "dial upstream: "+err.Error(), http.StatusBadGateway)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		_ = upstream.Close()
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}

	clientConn, _, err := hj.Hijack()
	if err != nil {
		_ = upstream.Close()
		http.Error(w, "hijack: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

	go func() {
		_, _ = io.Copy(upstream, clientConn)
		_ = upstream.Close()
	}()

	_, _ = io.Copy(clientConn, upstream)
	_ = clientConn.Close()
}

func (p *HTTPProxy) handleHTTP(w http.ResponseWriter, r *http.Request) {
	outReq := r.Clone(r.Context())
	outReq.RequestURI = ""
	outReq.Header.Del("Proxy-Connection")
	dest := outReq.URL.Host
	if dest == "" {
		dest = outReq.Host
	}
	if p.isSelfTarget(dest) {
		http.Error(w, "refusing to proxy request back to this codex-proxy listener", http.StatusLoopDetected)
		return
	}

	tr := &http.Transport{
		Proxy:                 nil,
		ForceAttemptHTTP2:     false,
		ResponseHeaderTimeout: 30 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return p.dialer.Dial(network, addr)
		},
	}

	resp, err := tr.RoundTrip(outReq)
	if err != nil {
		http.Error(w, "round trip: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *HTTPProxy) isSelfTarget(addr string) bool {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return false
	}

	p.mu.Lock()
	listenAddr := p.addr
	p.mu.Unlock()
	if listenAddr == "" {
		return false
	}

	_, listenPort, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return false
	}
	targetHost, targetPort, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if targetPort != listenPort {
		return false
	}
	return isLoopbackHost(targetHost)
}

func isLoopbackHost(host string) bool {
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	host = strings.TrimSuffix(host, ".")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		dst.Del(k)
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
