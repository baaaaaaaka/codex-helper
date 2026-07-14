package localproxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// proxyFaultRelay is a test-only network boundary. It forwards real TCP
// connections to an upstream SOCKS5 server, but can close or blackhole only
// the connections belonging to the proxy under test. This gives hosted
// macOS/Windows jobs a real socket-level outage without changing the runner's
// default route or GitHub Actions control connection.
type proxyFaultRelayMode int32

const (
	proxyFaultRelayPass proxyFaultRelayMode = iota
	proxyFaultRelayReset
	proxyFaultRelayBlackhole
)

type proxyFaultRelay struct {
	listener net.Listener
	upstream string
	done     chan struct{}
	mode     atomic.Int32

	connectionsMu sync.Mutex
	connections   map[net.Conn]struct{}
	wg            sync.WaitGroup
	closeOnce     sync.Once
}

func startProxyFaultRelay(t *testing.T, upstream string) *proxyFaultRelay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fault relay: %v", err)
	}
	relay := &proxyFaultRelay{
		listener:    listener,
		upstream:    upstream,
		done:        make(chan struct{}),
		connections: make(map[net.Conn]struct{}),
	}
	relay.wg.Add(1)
	go relay.acceptLoop()
	t.Cleanup(relay.Close)
	return relay
}

func (r *proxyFaultRelay) Addr() string { return r.listener.Addr().String() }

func (r *proxyFaultRelay) SetMode(mode proxyFaultRelayMode) {
	r.mode.Store(int32(mode))
	// A fault transition must also terminate connections that were established
	// before the transition. This models a real link reset instead of merely
	// rejecting future dials.
	r.closeConnections()
}

func (r *proxyFaultRelay) Close() {
	r.closeOnce.Do(func() {
		close(r.done)
		_ = r.listener.Close()
		r.closeConnections()
		r.wg.Wait()
	})
}

func (r *proxyFaultRelay) acceptLoop() {
	defer r.wg.Done()
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			select {
			case <-r.done:
				return
			default:
			}
			continue
		}
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.handle(conn)
		}()
	}
}

func (r *proxyFaultRelay) handle(conn net.Conn) {
	r.track(conn)
	defer func() {
		r.untrack(conn)
		_ = conn.Close()
	}()

	switch proxyFaultRelayMode(r.mode.Load()) {
	case proxyFaultRelayReset:
		return
	case proxyFaultRelayBlackhole:
		// Keep the socket open and consume bytes until the client timeout or a
		// subsequent mode transition closes it. No response is ever produced.
		_, _ = io.Copy(io.Discard, conn)
		return
	}

	upstream, err := net.DialTimeout("tcp", r.upstream, time.Second)
	if err != nil {
		return
	}
	r.track(upstream)
	defer func() {
		r.untrack(upstream)
		_ = upstream.Close()
	}()
	if proxyFaultRelayMode(r.mode.Load()) != proxyFaultRelayPass {
		return
	}

	copyDone := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(upstream, conn)
		copyDone <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(conn, upstream)
		copyDone <- struct{}{}
	}()
	select {
	case <-copyDone:
	case <-r.done:
	}
}

func (r *proxyFaultRelay) track(conn net.Conn) {
	r.connectionsMu.Lock()
	r.connections[conn] = struct{}{}
	r.connectionsMu.Unlock()
}

func (r *proxyFaultRelay) untrack(conn net.Conn) {
	r.connectionsMu.Lock()
	delete(r.connections, conn)
	r.connectionsMu.Unlock()
}

func (r *proxyFaultRelay) closeConnections() {
	r.connectionsMu.Lock()
	connections := make([]net.Conn, 0, len(r.connections))
	for conn := range r.connections {
		connections = append(connections, conn)
	}
	r.connectionsMu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

func TestHTTPProxyRealSocketFaultRelayRecoversAfterResetAndBlackhole(t *testing.T) {
	echoAddr, closeEcho := startTCPEchoServer(t)
	defer closeEcho()
	socks := startSOCKS5Server(t)
	defer socks.Close()
	relay := startProxyFaultRelay(t, socks.Addr())

	targetHost, targetPortText, err := net.SplitHostPort(echoAddr)
	if err != nil {
		t.Fatalf("split echo address: %v", err)
	}
	targetPort, err := strconv.Atoi(targetPortText)
	if err != nil {
		t.Fatalf("parse echo port: %v", err)
	}

	dialer, err := NewSOCKS5Dialer(relay.Addr(), 250*time.Millisecond)
	if err != nil {
		t.Fatalf("NewSOCKS5Dialer: %v", err)
	}
	failures := make(chan error, 8)
	proxy := NewHTTPProxy(dialer, Options{
		InstanceID:         "real-socket-fault-relay",
		HealthProbeTTL:     20 * time.Millisecond,
		HealthProbeTimeout: 300 * time.Millisecond,
		HealthProbe: func(ctx context.Context) error {
			return ProbeSOCKS5Target(ctx, relay.Addr(), targetHost, targetPort, 250*time.Millisecond)
		},
		OnBackendFailure: func(err error) {
			select {
			case failures <- err:
			default:
			}
		},
	})
	proxyAddr, err := proxy.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start HTTP proxy: %v", err)
	}
	defer proxy.Close(context.Background())

	assertFaultRelayEcho(t, proxyAddr, echoAddr, "before-fault\n")

	// RESET closes established sockets and rejects new ones. The failure must
	// travel through the real HTTP -> SOCKS dial path and notify the recovery
	// owner rather than being a synthetic dialer error.
	relay.SetMode(proxyFaultRelayReset)
	if err := expectProxyConnectFailure(proxyAddr, echoAddr); err != nil {
		t.Fatal(err)
	}
	select {
	case <-failures:
	case <-time.After(time.Second):
		t.Fatal("real socket reset did not produce a backend failure notification")
	}

	// BLACKHOLE keeps TCP established but stops the SOCKS greeting. This is
	// the half-open/packet-loss case that a simple connection-close test misses.
	relay.SetMode(proxyFaultRelayBlackhole)
	waitForProxyHealth(t, proxyAddr, false)

	// Restoring the relay must make the existing stable HTTP listener usable
	// again without recreating the listener or requiring a runner-wide network
	// reset.
	relay.SetMode(proxyFaultRelayPass)
	waitForProxyHealth(t, proxyAddr, true)
	assertFaultRelayEcho(t, proxyAddr, echoAddr, "after-recovery\n")
}

func assertFaultRelayEcho(t *testing.T, proxyAddr, target, payload string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	reader := openConnectTunnelTo(t, conn, target)
	if _, err := fmt.Fprint(conn, payload); err != nil {
		t.Fatalf("write %q through proxy: %v", payload, err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	got, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read %q through proxy: %v", payload, err)
	}
	if got != payload {
		t.Fatalf("proxy echo = %q, want %q", got, payload)
	}
}

func expectProxyConnectFailure(proxyAddr, target string) error {
	conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
	if err != nil {
		return fmt.Errorf("dial proxy during fault: %w", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		return fmt.Errorf("write fault CONNECT: %w", err)
	}
	status, err := bufio.NewReader(conn).ReadString('\n')
	if err == nil && strings.HasPrefix(status, "HTTP/1.1 502") {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read fault CONNECT response: %w", err)
	}
	return fmt.Errorf("fault CONNECT status = %q, want HTTP/1.1 502", status)
}

func waitForProxyHealth(t *testing.T, proxyAddr string, wantHealthy bool) {
	t.Helper()
	client := &http.Client{
		Timeout:   500 * time.Millisecond,
		Transport: &http.Transport{Proxy: nil},
	}
	defer client.Transport.(*http.Transport).CloseIdleConnections()
	deadline := time.Now().Add(3 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + proxyAddr + "/_codex_proxy/health")
		observed := false
		healthy := false
		if err == nil {
			healthy = resp.StatusCode == http.StatusOK
			observed = resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusServiceUnavailable
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		} else {
			last = err.Error()
		}
		if observed && healthy == wantHealthy {
			return
		}
		if err == nil {
			last = resp.Status
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("proxy health did not become %t; last=%s", wantHealthy, last)
}
