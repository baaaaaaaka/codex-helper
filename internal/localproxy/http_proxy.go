package localproxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	xproxy "golang.org/x/net/proxy"
)

type Dialer interface {
	Dial(network, addr string) (net.Conn, error)
}

// ContextDialer is an optional extension implemented by dialers that can
// cancel an in-flight connection attempt. Dialer is intentionally kept as the
// small compatibility interface used by existing callers; HTTPProxy prefers
// ContextDialer whenever it is available.
type ContextDialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

// HeaderContextDialer is implemented by a dialer that connects to another
// HTTP proxy before establishing the requested upstream connection. Headers
// are kept separate from the origin request so internal chain metadata is not
// leaked to the final destination.
type HeaderContextDialer interface {
	DialContextWithHeaders(ctx context.Context, network, addr string, headers http.Header) (net.Conn, error)
}

// HostResolver is injectable so DNS alias loop detection can be tested
// deterministically without changing the runner's resolver configuration.
type HostResolver func(context.Context, string) ([]net.IP, error)

const (
	// These headers are only forwarded to another HeaderContextDialer. They are
	// removed before an HTTP request is sent to an origin server.
	ProxyHopHeader      = "X-Codex-Proxy-Hop"
	ProxyChainHeader    = "X-Codex-Proxy-Chain"
	defaultProxyMaxHops = 8
)

var (
	ErrProxyHopLimitExceeded = errors.New("proxy hop limit exceeded")
	ErrProxyChainLoop        = errors.New("proxy chain loop detected")
)

// HealthStatus contains additive broker-level health information. The legacy
// ok/instanceId fields remain the compatibility contract for existing
// callers; these fields let newer callers distinguish a live HTTP listener
// from a ready backend generation.
type HealthStatus struct {
	Alive       bool `json:"alive"`
	TunnelReady bool `json:"tunnelReady"`
	// RouteEvidence records target-specific connectivity observed through the
	// active backend. It is intentionally additive so old health consumers can
	// continue using ok/tunnelReady.
	RouteEvidence        map[string]RouteEvidence `json:"routeEvidence,omitempty"`
	CapabilityRouteReady bool                     `json:"capabilityRouteReady"`
	BrokerID             string                   `json:"brokerId,omitempty"`
	BrokerEpoch          string                   `json:"brokerEpoch,omitempty"`
	ActiveGeneration     uint64                   `json:"activeGeneration,omitempty"`
	Recovery             string                   `json:"recovery,omitempty"`
	LastProbeAt          time.Time                `json:"lastProbeAt,omitempty"`
	LastProbeError       string                   `json:"lastProbeError,omitempty"`
	ProbeCount           uint64                   `json:"probeCount,omitempty"`
	RecoveryCount        uint64                   `json:"recoveryCount,omitempty"`
	BackendFailures      uint64                   `json:"backendFailures,omitempty"`
}

type RouteEvidence struct {
	Target        string    `json:"target"`
	Ready         bool      `json:"ready"`
	LastSuccessAt time.Time `json:"lastSuccessAt,omitempty"`
	LastFailureAt time.Time `json:"lastFailureAt,omitempty"`
	LastError     string    `json:"lastError,omitempty"`
	ExpiresAt     time.Time `json:"expiresAt,omitempty"`
}

type HTTPProxy struct {
	instanceID       string
	proxyID          string
	maxProxyHops     int
	dialer           Dialer
	health           func(context.Context) error
	details          func(context.Context) HealthStatus
	failure          func(error)
	targetFailure    func(string, error)
	resolveHost      HostResolver
	lookupTimeout    time.Duration
	forwardChainMeta bool
	idle             time.Duration
	tunnelIdle       time.Duration

	mu              sync.Mutex
	listener        net.Listener
	server          *http.Server
	addr            string
	listenerHost    string
	closing         bool
	tunnels         map[*proxyTunnel]struct{}
	probeCount      atomic.Uint64
	backendFailures atomic.Uint64

	healthMu          sync.Mutex
	healthCached      bool
	healthCachedAt    time.Time
	healthCachedError error
	healthInFlight    chan struct{}
	healthTTL         time.Duration
	healthTimeout     time.Duration
}

type proxyTunnel struct {
	client   net.Conn
	upstream net.Conn
}

type Options struct {
	InstanceID string
	// ProxyID identifies this HTTP hop in internal chain metadata. It defaults
	// to InstanceID and detects A->B->A before the generic hop budget ends.
	ProxyID           string
	MaxProxyHops      int
	IdleTimeout       time.Duration
	TunnelIdleTimeout time.Duration
	// ResolveHost is used only for a same-port hostname target. The default
	// resolver is bounded by SelfTargetLookupTimeout.
	ResolveHost             HostResolver
	SelfTargetLookupTimeout time.Duration
	// HealthProbe, when set, verifies the backend before health is reported.
	// This keeps a live HTTP listener with a dead SOCKS tunnel from being
	// mistaken for a reusable proxy instance.
	HealthProbe func(context.Context) error
	// OnBackendFailure receives a non-blocking notification when a request
	// cannot establish its upstream connection. Callers should coalesce the
	// notification and decide whether a stronger probe/recovery is needed.
	OnBackendFailure func(error)
	// OnTargetFailure records the concrete destination that failed. This is
	// deliberately separate from OnBackendFailure so health reporting can keep
	// per-target evidence without making every target failure restart a healthy
	// shared SOCKS backend.
	OnTargetFailure func(string, error)
	// HealthDetails supplies additive broker/generation information for the
	// health response. The basic alive/tunnelReady fields are filled by the
	// HTTP proxy itself.
	HealthDetails func(context.Context) HealthStatus
	// HealthProbeTTL coalesces health checks that arrive during the same short
	// observation window. It prevents a reuse/monitor burst from creating one
	// SOCKS handshake per HTTP health request.
	HealthProbeTTL time.Duration
	// HealthProbeTimeout bounds the shared probe independently from the first
	// request's context. A canceled waiter must not cancel the probe used by
	// the other waiters.
	HealthProbeTimeout time.Duration
}

const (
	defaultHTTPProxyIdleTimeout       = 2 * time.Minute
	defaultHTTPProxyTunnelIdleTimeout = 30 * time.Minute
	defaultHTTPProxyHealthProbeTTL    = 250 * time.Millisecond
	defaultHTTPProxyHealthTimeout     = 750 * time.Millisecond
	defaultSelfTargetLookupTimeout    = 100 * time.Millisecond
)

func NewHTTPProxy(d Dialer, opts Options) *HTTPProxy {
	idle := opts.IdleTimeout
	if idle == 0 {
		idle = defaultHTTPProxyIdleTimeout
	}
	tunnelIdle := opts.TunnelIdleTimeout
	if tunnelIdle == 0 {
		tunnelIdle = defaultHTTPProxyTunnelIdleTimeout
	}
	healthTTL := opts.HealthProbeTTL
	if healthTTL <= 0 {
		healthTTL = defaultHTTPProxyHealthProbeTTL
	}
	healthTimeout := opts.HealthProbeTimeout
	if healthTimeout <= 0 {
		healthTimeout = defaultHTTPProxyHealthTimeout
	}
	proxyID := strings.TrimSpace(opts.ProxyID)
	if proxyID == "" {
		proxyID = strings.TrimSpace(opts.InstanceID)
	}
	maxProxyHops := opts.MaxProxyHops
	if maxProxyHops <= 0 {
		maxProxyHops = defaultProxyMaxHops
	}
	lookupTimeout := opts.SelfTargetLookupTimeout
	if lookupTimeout <= 0 {
		lookupTimeout = defaultSelfTargetLookupTimeout
	}
	resolveHost := opts.ResolveHost
	if resolveHost == nil {
		resolveHost = defaultHostResolver
	}
	_, forwardChainMeta := d.(HeaderContextDialer)
	return &HTTPProxy{
		instanceID:       opts.InstanceID,
		proxyID:          proxyID,
		maxProxyHops:     maxProxyHops,
		dialer:           d,
		health:           opts.HealthProbe,
		details:          opts.HealthDetails,
		failure:          opts.OnBackendFailure,
		targetFailure:    opts.OnTargetFailure,
		resolveHost:      resolveHost,
		lookupTimeout:    lookupTimeout,
		forwardChainMeta: forwardChainMeta,
		idle:             idle,
		tunnelIdle:       tunnelIdle,
		tunnels:          make(map[*proxyTunnel]struct{}),
		healthTTL:        healthTTL,
		healthTimeout:    healthTimeout,
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
	return socks5Dialer{dialer: d}, nil
}

type socks5Dialer struct {
	dialer xproxy.Dialer
}

func (d socks5Dialer) Dial(network, addr string) (net.Conn, error) {
	return d.dialer.Dial(network, addr)
}

func (d socks5Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cd, ok := d.dialer.(xproxy.ContextDialer); ok {
		return cd.DialContext(ctx, network, addr)
	}
	return dialContextFallback(ctx, d.dialer, network, addr)
}

type dialerFunc func(network, addr string) (net.Conn, error)

func (d dialerFunc) Dial(network, addr string) (net.Conn, error) { return d(network, addr) }

func dialContextFallback(ctx context.Context, d Dialer, network, addr string) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	type result struct {
		conn net.Conn
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		conn, err := d.Dial(network, addr)
		select {
		case resultCh <- result{conn: conn, err: err}:
		case <-ctx.Done():
			if conn != nil {
				_ = conn.Close()
			}
		}
	}()

	select {
	case result := <-resultCh:
		return result.conn, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

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
		IdleTimeout:       p.idle,
	}

	p.listener = ln
	p.server = srv
	p.closing = false
	p.addr = ln.Addr().String()
	if host, _, splitErr := net.SplitHostPort(p.addr); splitErr == nil {
		p.listenerHost = normalizeListenerHost(host)
	}

	go func() {
		_ = srv.Serve(ln)
	}()

	return ln.Addr().String(), nil
}

func (p *HTTPProxy) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	srv := p.server
	ln := p.listener
	p.server = nil
	p.listener = nil
	p.addr = ""
	p.listenerHost = ""
	p.closing = true
	tunnels := make([]*proxyTunnel, 0, len(p.tunnels))
	for tunnel := range p.tunnels {
		tunnels = append(tunnels, tunnel)
	}
	p.tunnels = make(map[*proxyTunnel]struct{})
	p.mu.Unlock()
	for _, tunnel := range tunnels {
		_ = tunnel.client.Close()
		_ = tunnel.upstream.Close()
	}

	var closeErr error
	if ln != nil {
		if err := ln.Close(); err != nil && !isClosedNetworkError(err) {
			closeErr = err
		}
	}
	if srv != nil {
		if err := srv.Shutdown(ctx); err != nil && !isClosedNetworkError(err) {
			closeErr = errors.Join(closeErr, err)
			// Shutdown intentionally waits for active handlers. If the caller's
			// deadline expires, force-close ordinary HTTP connections as the final
			// cleanup fence so a stuck dialer cannot keep the proxy process alive.
			if ctx.Err() != nil {
				if forceErr := srv.Close(); forceErr != nil && !isClosedNetworkError(forceErr) {
					closeErr = errors.Join(closeErr, forceErr)
				}
			}
		}
	}
	return closeErr
}

func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed) {
		return true
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

func (p *HTTPProxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	// Local health check (not proxied).
	if r.Method == http.MethodGet && r.URL.Path == "/_codex_proxy/health" {
		status := http.StatusOK
		ok := true
		var healthErr string
		if p.health != nil {
			err := p.probeHealth(r.Context())
			if err != nil {
				status = http.StatusServiceUnavailable
				ok = false
				healthErr = err.Error()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		details := HealthStatus{Alive: true, TunnelReady: ok}
		if p.details != nil {
			extra := p.details(r.Context())
			details.BrokerID = extra.BrokerID
			details.BrokerEpoch = extra.BrokerEpoch
			details.ActiveGeneration = extra.ActiveGeneration
			details.Recovery = extra.Recovery
			details.LastProbeAt = extra.LastProbeAt
			details.LastProbeError = extra.LastProbeError
			details.RouteEvidence = extra.RouteEvidence
			details.CapabilityRouteReady = extra.CapabilityRouteReady
			details.ProbeCount = extra.ProbeCount
			details.RecoveryCount = extra.RecoveryCount
			details.BackendFailures = extra.BackendFailures
		}
		if probes := p.probeCount.Load(); probes > details.ProbeCount {
			details.ProbeCount = probes
		}
		if failures := p.backendFailures.Load(); failures > details.BackendFailures {
			details.BackendFailures = failures
		}
		_ = json.NewEncoder(w).Encode(struct {
			OK         bool   `json:"ok"`
			InstanceID string `json:"instanceId"`
			Error      string `json:"error"`
			HealthStatus
		}{
			OK:           ok,
			InstanceID:   p.instanceID,
			Error:        healthErr,
			HealthStatus: details,
		})
		return
	}

	if strings.EqualFold(r.Method, http.MethodConnect) {
		p.handleConnect(w, r)
		return
	}

	p.handleHTTP(w, r)
}

// probeHealth runs at most one backend health probe for a TTL window. Every
// caller still gets its own context cancellation semantics while the actual
// probe is bounded by the proxy's internal timeout and is shared by all
// waiters.
func (p *HTTPProxy) probeHealth(ctx context.Context) error {
	if p == nil || p.health == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		p.healthMu.Lock()
		now := time.Now()
		if p.healthCached && now.Sub(p.healthCachedAt) <= p.healthTTL {
			err := p.healthCachedError
			p.healthMu.Unlock()
			return err
		}
		if inFlight := p.healthInFlight; inFlight != nil {
			p.healthMu.Unlock()
			select {
			case <-inFlight:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		inFlight := make(chan struct{})
		p.healthInFlight = inFlight
		p.probeCount.Add(1)
		p.healthMu.Unlock()

		go p.completeHealthProbe(inFlight)
		select {
		case <-inFlight:
			p.healthMu.Lock()
			err := p.healthCachedError
			p.healthMu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *HTTPProxy) completeHealthProbe(inFlight chan struct{}) {
	probeCtx, cancel := context.WithTimeout(context.Background(), p.healthTimeout)
	err := p.health(probeCtx)
	cancel()

	p.healthMu.Lock()
	p.healthCached = true
	p.healthCachedAt = time.Now()
	p.healthCachedError = err
	p.healthInFlight = nil
	close(inFlight)
	p.healthMu.Unlock()
}

func (p *HTTPProxy) handleConnect(w http.ResponseWriter, r *http.Request) {
	dest := r.Host
	if dest == "" {
		http.Error(w, "missing host", http.StatusBadRequest)
		return
	}
	forwardHeaders, err := p.nextProxyHeaders(r.Context(), r.Header)
	if err != nil {
		http.Error(w, err.Error(), proxyAdmissionStatus(err))
		return
	}
	if p.isSelfTargetContext(r.Context(), dest) {
		http.Error(w, "refusing to proxy request back to this codex-proxy listener", http.StatusLoopDetected)
		return
	}

	upstream, err := p.dialContextWithHeaders(r.Context(), "tcp", dest, forwardHeaders)
	if err != nil {
		p.notifyBackendFailureContext(r.Context(), dest, err)
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
	tunnel, ok := p.registerTunnel(clientConn, upstream)
	if !ok {
		return
	}

	if _, err := tunnel.client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		p.unregisterTunnel(tunnel)
		_ = tunnel.client.Close()
		_ = tunnel.upstream.Close()
		return
	}

	p.copyTunnel(tunnel)
}

func (p *HTTPProxy) registerTunnel(clientConn, upstream net.Conn) (*proxyTunnel, bool) {
	tunnel := &proxyTunnel{client: clientConn, upstream: upstream}
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		_ = clientConn.Close()
		_ = upstream.Close()
		return nil, false
	}
	if p.tunnels == nil {
		p.tunnels = make(map[*proxyTunnel]struct{})
	}
	p.tunnels[tunnel] = struct{}{}
	p.mu.Unlock()
	return tunnel, true
}

func (p *HTTPProxy) unregisterTunnel(tunnel *proxyTunnel) {
	if tunnel == nil {
		return
	}
	p.mu.Lock()
	delete(p.tunnels, tunnel)
	p.mu.Unlock()
}

func (p *HTTPProxy) copyTunnel(tunnel *proxyTunnel) {
	if tunnel == nil {
		return
	}
	defer p.unregisterTunnel(tunnel)
	clientConn, upstream := tunnel.client, tunnel.upstream
	var once sync.Once
	closeBoth := func() {
		_ = clientConn.Close()
		_ = upstream.Close()
	}
	resetDeadline := func() {
		if p.tunnelIdle <= 0 {
			return
		}
		deadline := time.Now().Add(p.tunnelIdle)
		_ = clientConn.SetReadDeadline(deadline)
		_ = upstream.SetReadDeadline(deadline)
	}
	resetDeadline()
	done := make(chan struct{}, 2)
	go func() {
		copyTunnelDirection(upstream, clientConn, resetDeadline)
		once.Do(closeBoth)
		done <- struct{}{}
	}()
	go func() {
		copyTunnelDirection(clientConn, upstream, resetDeadline)
		once.Do(closeBoth)
		done <- struct{}{}
	}()
	<-done
	<-done
}

func copyTunnelDirection(dst net.Conn, src net.Conn, markActive func()) {
	buf := make([]byte, 32*1024)
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			markActive()
			if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
				return
			}
		}
		if readErr != nil {
			return
		}
	}
}

func (p *HTTPProxy) handleHTTP(w http.ResponseWriter, r *http.Request) {
	forwardHeaders, err := p.nextProxyHeaders(r.Context(), r.Header)
	if err != nil {
		http.Error(w, err.Error(), proxyAdmissionStatus(err))
		return
	}
	outReq := r.Clone(r.Context())
	outReq.RequestURI = ""
	outReq.Header.Del("Proxy-Connection")
	// Chain metadata is transported out-of-band to another proxy-capable
	// dialer. It must not be exposed to the origin server.
	outReq.Header.Del(ProxyHopHeader)
	outReq.Header.Del(ProxyChainHeader)
	dest := outReq.URL.Host
	if dest == "" {
		dest = outReq.Host
	}
	if p.isSelfTargetContext(r.Context(), dest) {
		http.Error(w, "refusing to proxy request back to this codex-proxy listener", http.StatusLoopDetected)
		return
	}

	tr := &http.Transport{
		Proxy:                 nil,
		DisableKeepAlives:     true,
		ForceAttemptHTTP2:     false,
		ResponseHeaderTimeout: 30 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return p.dialContextWithHeaders(ctx, network, addr, forwardHeaders)
		},
	}
	defer tr.CloseIdleConnections()

	resp, err := tr.RoundTrip(outReq)
	if err != nil {
		p.notifyBackendFailureContext(outReq.Context(), dest, err)
		http.Error(w, "round trip: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *HTTPProxy) notifyBackendFailure(target string, err error) {
	p.notifyBackendFailureContext(context.Background(), target, err)
}

func (p *HTTPProxy) notifyBackendFailureContext(ctx context.Context, target string, err error) {
	if err == nil {
		return
	}
	// A transport response-header timeout can wrap context.DeadlineExceeded
	// while the proxy request itself is still live. Suppress only a request
	// context cancellation; a live request that times out upstream is a real
	// backend failure and must participate in recovery admission.
	if ctx != nil && ctx.Err() != nil {
		return
	}
	p.backendFailures.Add(1)
	if p.failure != nil {
		p.failure(err)
	}
	if p.targetFailure != nil {
		p.targetFailure(target, err)
	}
}

func (p *HTTPProxy) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return p.dialContextWithHeaders(ctx, network, addr, nil)
}

func (p *HTTPProxy) dialContextWithHeaders(ctx context.Context, network, addr string, headers http.Header) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	d := p.dialer
	p.mu.Unlock()
	if d == nil {
		return nil, errors.New("proxy dialer is unavailable")
	}
	if hd, ok := d.(HeaderContextDialer); ok {
		return hd.DialContextWithHeaders(ctx, network, addr, headers.Clone())
	}
	if cd, ok := d.(ContextDialer); ok {
		return cd.DialContext(ctx, network, addr)
	}
	return dialContextFallback(ctx, d, network, addr)
}

func (p *HTTPProxy) isSelfTarget(addr string) bool {
	return p.isSelfTargetContext(context.Background(), addr)
}

func (p *HTTPProxy) isSelfTargetContext(ctx context.Context, addr string) bool {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return false
	}

	p.mu.Lock()
	listenAddr := p.addr
	listenHost := p.listenerHost
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
	if listenerOwnsTarget(listenHost, targetHost) {
		return true
	}
	if net.ParseIP(normalizeListenerHost(targetHost)) != nil || p.resolveHost == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lookupCtx, cancel := context.WithTimeout(ctx, p.lookupTimeout)
	defer cancel()
	ips, err := p.resolveHost(lookupCtx, strings.TrimSuffix(targetHost, "."))
	if err != nil {
		return false
	}
	for _, ip := range ips {
		if ip != nil && listenerOwnsTarget(listenHost, ip.String()) {
			return true
		}
	}
	return false
}

func defaultHostResolver(ctx context.Context, host string) ([]net.IP, error) {
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, addr := range addrs {
		ips = append(ips, addr.IP)
	}
	return ips, nil
}

func (p *HTTPProxy) nextProxyHeaders(ctx context.Context, incoming http.Header) (http.Header, error) {
	// GenerationRouter is the normal production dialer and does not implement
	// HeaderContextDialer. With no incoming chain metadata there is no
	// downstream hop to annotate, so keep the healthy single-proxy path
	// allocation-free. Incoming metadata is still parsed below so callers
	// cannot bypass hop-limit or cycle validation.
	if !p.forwardChainMeta && strings.TrimSpace(incoming.Get(ProxyHopHeader)) == "" && strings.TrimSpace(incoming.Get(ProxyChainHeader)) == "" {
		return nil, nil
	}
	hop := 0
	if raw := strings.TrimSpace(incoming.Get(ProxyHopHeader)); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return nil, fmt.Errorf("invalid %s header %q", ProxyHopHeader, raw)
		}
		hop = parsed
	}
	if hop >= p.maxProxyHops {
		return nil, fmt.Errorf("%w: hop=%d limit=%d", ErrProxyHopLimitExceeded, hop, p.maxProxyHops)
	}
	chain := splitProxyChain(incoming.Get(ProxyChainHeader))
	if p.proxyID != "" {
		for _, id := range chain {
			if id == p.proxyID {
				return nil, fmt.Errorf("%w: %s", ErrProxyChainLoop, p.proxyID)
			}
		}
		chain = append(chain, p.proxyID)
	}
	forward := make(http.Header)
	for key, values := range incoming {
		forward[key] = append([]string(nil), values...)
	}
	forward.Set(ProxyHopHeader, strconv.Itoa(hop+1))
	if len(chain) > 0 {
		forward.Set(ProxyChainHeader, strings.Join(chain, ","))
	} else {
		forward.Del(ProxyChainHeader)
	}
	return forward, nil
}

func splitProxyChain(raw string) []string {
	parts := strings.Split(raw, ",")
	chain := make([]string, 0, len(parts))
	for _, part := range parts {
		if id := strings.TrimSpace(part); id != "" {
			chain = append(chain, id)
		}
	}
	return chain
}

func proxyAdmissionStatus(err error) int {
	if errors.Is(err, ErrProxyHopLimitExceeded) || errors.Is(err, ErrProxyChainLoop) {
		return http.StatusLoopDetected
	}
	return http.StatusBadRequest
}

func normalizeListenerHost(host string) string {
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	if host == "" {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return strings.TrimSuffix(strings.ToLower(host), ".")
}

func listenerOwnsTarget(listenerHost, targetHost string) bool {
	listenerHost = normalizeListenerHost(listenerHost)
	targetHost = normalizeListenerHost(targetHost)
	if listenerHost == "" || targetHost == "" {
		return false
	}
	if targetHost == "localhost" {
		return listenerHost == "localhost" || isLoopbackHost(listenerHost) || isWildcardHost(listenerHost)
	}
	listenerIP := net.ParseIP(listenerHost)
	targetIP := net.ParseIP(targetHost)
	if listenerIP != nil && targetIP != nil {
		if listenerIP.IsUnspecified() {
			// A wildcard listener owns all local addresses, but do not treat a
			// same-port public destination as a self-loop without a resolver
			// proving that it maps to this host.
			if listenerIP.To4() != nil {
				return targetIP.To4() != nil && targetIP.IsLoopback()
			}
			return targetIP.To4() == nil && targetIP.IsLoopback()
		}
		return listenerIP.Equal(targetIP)
	}
	return listenerHost == targetHost
}

func isWildcardHost(host string) bool {
	host = normalizeListenerHost(host)
	return host == "0.0.0.0" || host == "::"
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
