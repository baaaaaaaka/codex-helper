package localproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
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

// proxyChainDialer is a real TCP HTTP CONNECT hop used by the chain tests.
// It deliberately implements HeaderContextDialer so the same hop metadata
// path is exercised for CONNECT and plain HTTP requests.
type proxyChainDialer struct {
	mu        sync.Mutex
	proxyAddr string
	statuses  []int
	headers   []http.Header
}

func (d *proxyChainDialer) SetProxyAddr(addr string) {
	d.mu.Lock()
	d.proxyAddr = addr
	d.mu.Unlock()
}

func (d *proxyChainDialer) target() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.proxyAddr
}

func (d *proxyChainDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContextWithHeaders(context.Background(), network, addr, nil)
}

func (d *proxyChainDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return d.DialContextWithHeaders(ctx, network, addr, nil)
}

func (d *proxyChainDialer) DialContextWithHeaders(ctx context.Context, network, addr string, headers http.Header) (net.Conn, error) {
	proxyAddr := d.target()
	if proxyAddr == "" {
		return nil, errors.New("proxy chain target is not configured")
	}
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, proxyAddr)
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n", addr, addr); err != nil {
		_ = conn.Close()
		return nil, err
	}
	for key, values := range headers {
		for _, value := range values {
			if _, err := fmt.Fprintf(conn, "%s: %s\r\n", key, value); err != nil {
				_ = conn.Close()
				return nil, err
			}
		}
	}
	if _, err := io.WriteString(conn, "\r\n"); err != nil {
		_ = conn.Close()
		return nil, err
	}

	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if response.Body != nil {
		_ = response.Body.Close()
	}
	d.mu.Lock()
	d.statuses = append(d.statuses, response.StatusCode)
	d.headers = append(d.headers, headers.Clone())
	d.mu.Unlock()
	if response.StatusCode != http.StatusOK {
		_ = conn.Close()
		return nil, fmt.Errorf("upstream proxy CONNECT returned %s", response.Status)
	}
	return conn, nil
}

func (d *proxyChainDialer) Results() ([]int, []http.Header) {
	d.mu.Lock()
	defer d.mu.Unlock()
	statuses := append([]int(nil), d.statuses...)
	headers := make([]http.Header, len(d.headers))
	for i, header := range d.headers {
		headers[i] = header.Clone()
	}
	return statuses, headers
}

func directTCPDialer() Dialer {
	return dialerFunc(func(network, addr string) (net.Conn, error) {
		return (&net.Dialer{Timeout: time.Second}).Dial(network, addr)
	})
}

func httpClientThroughProxy(t *testing.T, proxyAddr string) (*http.Client, *http.Transport) {
	t.Helper()
	proxyURL, err := url.Parse("http://" + proxyAddr)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyURL(proxyURL),
		DisableKeepAlives:     true,
		ResponseHeaderTimeout: 2 * time.Second,
	}
	return &http.Client{Transport: transport, Timeout: 3 * time.Second}, transport
}

func startChainOrigin(t *testing.T, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get(ProxyHopHeader) != "" || r.Header.Get(ProxyChainHeader) != "" {
			http.Error(w, "proxy chain metadata leaked to origin", http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, "origin-ok")
	}))
}

func TestHTTPProxyChainMatrix(t *testing.T) {
	t.Run("A-B-origin", func(t *testing.T) {
		var hits atomic.Int32
		origin := startChainOrigin(t, &hits)
		defer origin.Close()

		b := NewHTTPProxy(directTCPDialer(), Options{InstanceID: "B", ProxyID: "B", MaxProxyHops: 4})
		bAddr, err := b.Start("127.0.0.1:0")
		if err != nil {
			t.Fatalf("start B: %v", err)
		}
		defer b.Close(context.Background())

		aToB := &proxyChainDialer{}
		a := NewHTTPProxy(aToB, Options{InstanceID: "A", ProxyID: "A", MaxProxyHops: 4})
		aAddr, err := a.Start("127.0.0.1:0")
		if err != nil {
			t.Fatalf("start A: %v", err)
		}
		defer a.Close(context.Background())
		aToB.SetProxyAddr(bAddr)

		client, transport := httpClientThroughProxy(t, aAddr)
		defer transport.CloseIdleConnections()
		resp, err := client.Get(origin.URL + "/through-two-proxies")
		if err != nil {
			t.Fatalf("GET through A->B: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(body) != "origin-ok" {
			t.Fatalf("response = %d %q, want 200 origin-ok", resp.StatusCode, body)
		}
		if hits.Load() != 1 {
			t.Fatalf("origin hits = %d, want 1", hits.Load())
		}
		statuses, headers := aToB.Results()
		if len(statuses) != 1 || statuses[0] != http.StatusOK {
			t.Fatalf("A->B CONNECT statuses = %v, want [200]", statuses)
		}
		if got := headers[0].Get(ProxyHopHeader); got != "1" {
			t.Fatalf("A->B hop header = %q, want 1", got)
		}
		if got := headers[0].Get(ProxyChainHeader); got != "A" {
			t.Fatalf("A->B chain header = %q, want A", got)
		}
	})

	t.Run("A-B-A-cycle", func(t *testing.T) {
		var hits atomic.Int32
		origin := startChainOrigin(t, &hits)
		defer origin.Close()

		bToA := &proxyChainDialer{}
		b := NewHTTPProxy(bToA, Options{InstanceID: "B", ProxyID: "B", MaxProxyHops: 4})
		bAddr, err := b.Start("127.0.0.1:0")
		if err != nil {
			t.Fatalf("start B: %v", err)
		}
		defer b.Close(context.Background())

		aToB := &proxyChainDialer{}
		a := NewHTTPProxy(aToB, Options{InstanceID: "A", ProxyID: "A", MaxProxyHops: 4})
		aAddr, err := a.Start("127.0.0.1:0")
		if err != nil {
			t.Fatalf("start A: %v", err)
		}
		defer a.Close(context.Background())
		aToB.SetProxyAddr(bAddr)
		bToA.SetProxyAddr(aAddr)

		client, transport := httpClientThroughProxy(t, aAddr)
		defer transport.CloseIdleConnections()
		resp, err := client.Get(origin.URL + "/cycle")
		if err != nil {
			t.Fatalf("GET through A->B->A: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("cycle response status = %d, want 502 wrapper response", resp.StatusCode)
		}
		if hits.Load() != 0 {
			t.Fatalf("origin hits = %d, want 0", hits.Load())
		}
		statuses, headers := bToA.Results()
		if len(statuses) != 1 || statuses[0] != http.StatusLoopDetected {
			t.Fatalf("B->A CONNECT statuses = %v, want [508]", statuses)
		}
		if got := headers[0].Get(ProxyHopHeader); got != "2" {
			t.Fatalf("B->A hop header = %q, want 2", got)
		}
		if got := headers[0].Get(ProxyChainHeader); got != "A,B" {
			t.Fatalf("B->A chain header = %q, want A,B", got)
		}
	})

	t.Run("hop-limit", func(t *testing.T) {
		var hits atomic.Int32
		origin := startChainOrigin(t, &hits)
		defer origin.Close()

		c := NewHTTPProxy(directTCPDialer(), Options{InstanceID: "C", ProxyID: "C", MaxProxyHops: 2})
		cAddr, err := c.Start("127.0.0.1:0")
		if err != nil {
			t.Fatalf("start C: %v", err)
		}
		defer c.Close(context.Background())

		bToC := &proxyChainDialer{}
		b := NewHTTPProxy(bToC, Options{InstanceID: "B", ProxyID: "B", MaxProxyHops: 2})
		bAddr, err := b.Start("127.0.0.1:0")
		if err != nil {
			t.Fatalf("start B: %v", err)
		}
		defer b.Close(context.Background())
		bToC.SetProxyAddr(cAddr)

		aToB := &proxyChainDialer{}
		a := NewHTTPProxy(aToB, Options{InstanceID: "A", ProxyID: "A", MaxProxyHops: 2})
		aAddr, err := a.Start("127.0.0.1:0")
		if err != nil {
			t.Fatalf("start A: %v", err)
		}
		defer a.Close(context.Background())
		aToB.SetProxyAddr(bAddr)

		client, transport := httpClientThroughProxy(t, aAddr)
		defer transport.CloseIdleConnections()
		resp, err := client.Get(origin.URL + "/hop-limit")
		if err != nil {
			t.Fatalf("GET through A->B->C: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("hop-limit response status = %d, want 502 wrapper response", resp.StatusCode)
		}
		if hits.Load() != 0 {
			t.Fatalf("origin hits = %d, want 0", hits.Load())
		}
		statuses, headers := bToC.Results()
		if len(statuses) != 1 || statuses[0] != http.StatusLoopDetected {
			t.Fatalf("B->C CONNECT statuses = %v, want [508]", statuses)
		}
		if got := headers[0].Get(ProxyHopHeader); got != "2" {
			t.Fatalf("B->C hop header = %q, want 2", got)
		}
		if got := headers[0].Get(ProxyChainHeader); got != "A,B" {
			t.Fatalf("B->C chain header = %q, want A,B", got)
		}
	})
}

func TestHTTPProxyDNSAliasSelfTargetMatrix(t *testing.T) {
	type testCase struct {
		name       string
		listenHost string
		alias      string
		resolved   []net.IP
		resolveErr error
		wantStatus int
		wantDial   bool
	}
	cases := []testCase{
		{name: "alias maps to listener IPv4", listenHost: "127.0.0.1", alias: "proxy-a.test", resolved: []net.IP{net.ParseIP("127.0.0.1")}, wantStatus: http.StatusLoopDetected},
		{name: "alias trailing dot maps to listener", listenHost: "127.0.0.1", alias: "proxy-a.test.", resolved: []net.IP{net.ParseIP("127.0.0.1")}, wantStatus: http.StatusLoopDetected},
		{name: "alias maps to another IPv4 loopback", listenHost: "127.0.0.1", alias: "proxy-b.test", resolved: []net.IP{net.ParseIP("127.0.0.2")}, wantStatus: http.StatusBadGateway, wantDial: true},
		{name: "alias maps to IPv6 loopback", listenHost: "127.0.0.1", alias: "proxy-v6.test", resolved: []net.IP{net.ParseIP("::1")}, wantStatus: http.StatusBadGateway, wantDial: true},
		{name: "IPv6 wildcard owns resolved IPv6 loopback", listenHost: "::", alias: "proxy-wildcard-v6.test", resolved: []net.IP{net.ParseIP("::1")}, wantStatus: http.StatusLoopDetected},
		{name: "unresolved alias is not assumed local", listenHost: "127.0.0.1", alias: "proxy-unknown.test", resolveErr: errors.New("no such host"), wantStatus: http.StatusBadGateway, wantDial: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var dials atomic.Int32
			p := NewHTTPProxy(dialerFunc(func(string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, errors.New("deterministic route failure")
			}), Options{
				InstanceID: "dns-alias",
				ResolveHost: func(_ context.Context, host string) ([]net.IP, error) {
					if host != strings.TrimSuffix(tc.alias, ".") {
						return nil, fmt.Errorf("unexpected lookup host %q", host)
					}
					return tc.resolved, tc.resolveErr
				},
				SelfTargetLookupTimeout: 50 * time.Millisecond,
			})
			addr, err := p.Start(net.JoinHostPort(tc.listenHost, "0"))
			if err != nil {
				t.Fatalf("start proxy: %v", err)
			}
			defer p.Close(context.Background())
			_, port, err := net.SplitHostPort(addr)
			if err != nil {
				t.Fatalf("split proxy address: %v", err)
			}

			req := httptest.NewRequest(http.MethodGet, "http://"+net.JoinHostPort(tc.alias, port)+"/loop", nil)
			recorder := httptest.NewRecorder()
			p.serveHTTP(recorder, req)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			if got := dials.Load() > 0; got != tc.wantDial {
				t.Fatalf("dialer called = %v, want %v", got, tc.wantDial)
			}
		})
	}
}

func TestHTTPProxyDNSAliasLookupIsBounded(t *testing.T) {
	var dials atomic.Int32
	p := NewHTTPProxy(dialerFunc(func(string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("route failure after bounded lookup")
	}), Options{
		InstanceID: "dns-timeout",
		ResolveHost: func(ctx context.Context, _ string) ([]net.IP, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		SelfTargetLookupTimeout: 20 * time.Millisecond,
	})
	addr, err := p.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer p.Close(context.Background())
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split proxy address: %v", err)
	}

	started := time.Now()
	req := httptest.NewRequest(http.MethodGet, "http://bounded-lookup.test:"+port+"/loop", nil)
	recorder := httptest.NewRecorder()
	p.serveHTTP(recorder, req)
	if recorder.Code != http.StatusBadGateway || dials.Load() != 1 {
		t.Fatalf("bounded lookup response = %d, dials=%d, want 502 and one dial", recorder.Code, dials.Load())
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("DNS alias lookup was not bounded: %s", elapsed)
	}
}

func TestProxyHopHeaderParsingRejectsInvalidInput(t *testing.T) {
	p := NewHTTPProxy(nil, Options{InstanceID: "hop-parser", MaxProxyHops: 2})
	for _, raw := range []string{"-1", "not-a-number"} {
		t.Run(raw, func(t *testing.T) {
			headers := make(http.Header)
			headers.Set(ProxyHopHeader, raw)
			if _, err := p.nextProxyHeaders(context.Background(), headers); err == nil {
				t.Fatalf("nextProxyHeaders(%q) error = nil", raw)
			}
		})
	}
	if _, err := p.nextProxyHeaders(context.Background(), http.Header{ProxyHopHeader: []string{"2"}}); !errors.Is(err, ErrProxyHopLimitExceeded) {
		t.Fatalf("hop limit error = %v, want ErrProxyHopLimitExceeded", err)
	}
}

func BenchmarkHTTPProxyNextProxyHeaders(b *testing.B) {
	b.Run("single-proxy-fast-path", func(b *testing.B) {
		p := NewHTTPProxy(dialerFunc(func(string, string) (net.Conn, error) {
			return nil, errors.New("benchmark dialer")
		}), Options{InstanceID: "steady"})
		headers := make(http.Header)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			forward, err := p.nextProxyHeaders(context.Background(), headers)
			if err != nil {
				b.Fatal(err)
			}
			if forward != nil {
				b.Fatal("single-proxy path allocated chain headers")
			}
		}
	})

	b.Run("chain-capable-path", func(b *testing.B) {
		p := NewHTTPProxy(&proxyChainDialer{}, Options{InstanceID: "steady", ProxyID: "steady"})
		headers := make(http.Header)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := p.nextProxyHeaders(context.Background(), headers); err != nil {
				b.Fatal(err)
			}
		}
	})
}
