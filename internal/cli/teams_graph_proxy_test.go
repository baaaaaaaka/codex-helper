package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/manager"
	"github.com/baaaaaaaka/codex-helper/internal/stack"
)

func TestTeamsGraphHTTPClientDirectIgnoresInheritedProxyEnv(t *testing.T) {
	lockCLITestHooks(t)
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")

	store := newTeamsGraphProxyTestStore(t, config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: boolPtr(false),
		Profiles: []config.Profile{{
			ID:        "p1",
			Name:      "profile",
			Host:      "example.com",
			Port:      22,
			User:      "alice",
			CreatedAt: time.Now(),
		}},
	})
	lease, err := newTeamsGraphHTTPClientLease(context.Background(), &rootOptions{configPath: store.Path()}, nil)
	if err != nil {
		t.Fatalf("newTeamsGraphHTTPClientLease: %v", err)
	}
	defer lease.Close(context.Background())
	if lease.Mode != "direct" {
		t.Fatalf("mode = %q, want direct", lease.Mode)
	}
	tr, ok := lease.Client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", lease.Client.Transport)
	}
	if tr.Proxy != nil {
		t.Fatal("direct Teams Graph client must not inherit HTTP_PROXY/HTTPS_PROXY")
	}
	assertTeamsGraphTransportBounds(t, tr)
}

func TestTeamsGraphHTTPClientUsesDirectAfterProxyReset(t *testing.T) {
	lockCLITestHooks(t)
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")

	store := newTeamsGraphProxyTestStore(t, config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: boolPtr(true),
		Profiles: []config.Profile{{
			ID:        "p1",
			Name:      "profile",
			Host:      "example.com",
			Port:      22,
			User:      "alice",
			CreatedAt: time.Now(),
		}},
	})
	cmd := newProxyResetCmd(&rootOptions{configPath: store.Path()})
	cmd.SetArgs([]string{})
	cmd.SetOut(io.Discard)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("proxy reset: %v", err)
	}

	origStackStart := stackStart
	t.Cleanup(func() { stackStart = origStackStart })
	stackStart = func(config.Profile, string, stack.Options) (*stack.Stack, error) {
		t.Fatal("stackStart should not be called after proxy reset clears profiles")
		return nil, errors.New("unexpected stack start")
	}

	lease, err := newTeamsGraphHTTPClientLease(context.Background(), &rootOptions{configPath: store.Path()}, nil)
	if err != nil {
		t.Fatalf("newTeamsGraphHTTPClientLease: %v", err)
	}
	defer lease.Close(context.Background())
	if lease.Mode != "direct" {
		t.Fatalf("mode = %q, want direct", lease.Mode)
	}
	tr, ok := lease.Client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", lease.Client.Transport)
	}
	if tr.Proxy != nil {
		t.Fatal("direct Teams Graph client after reset must not inherit HTTP_PROXY/HTTPS_PROXY")
	}
}

func TestTeamsGraphHTTPClientUsesConfiguredReusableProxy(t *testing.T) {
	lockCLITestHooks(t)
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")

	instanceID := "inst-1"
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen reusable proxy health server: %v", err)
	}
	var connectSeen atomic.Bool
	proxyServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			connectSeen.Store(true)
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/_codex_proxy/health" {
			t.Fatalf("unexpected proxy probe path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "instanceId": instanceID})
	}))
	proxyServer.Listener = ln
	proxyServer.Start()
	defer proxyServer.Close()
	_, portText, err := net.SplitHostPort(proxyServer.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split proxy server addr: %v", err)
	}
	port, err := net.LookupPort("tcp", portText)
	if err != nil {
		t.Fatalf("parse proxy port: %v", err)
	}

	store := newTeamsGraphProxyTestStore(t, config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: boolPtr(true),
		Profiles: []config.Profile{{
			ID:        "p1",
			Name:      "profile",
			Host:      "example.com",
			Port:      22,
			User:      "alice",
			CreatedAt: time.Now(),
		}},
		Instances: []config.Instance{{
			ID:         instanceID,
			ProfileID:  "p1",
			Kind:       config.InstanceKindDaemon,
			HTTPPort:   port,
			DaemonPID:  os.Getpid(),
			StartedAt:  time.Now(),
			LastSeenAt: time.Now(),
		}},
	})
	origStackStart := stackStart
	t.Cleanup(func() { stackStart = origStackStart })
	stackStart = func(config.Profile, string, stack.Options) (*stack.Stack, error) {
		return nil, errors.New("fallback stack must not start when reusable proxy is healthy")
	}
	lease, err := newTeamsGraphHTTPClientLease(context.Background(), &rootOptions{configPath: store.Path()}, nil)
	if err != nil {
		t.Fatalf("newTeamsGraphHTTPClientLease: %v", err)
	}
	defer lease.Close(context.Background())
	if lease.Mode != "proxy-reuse" {
		t.Fatalf("mode = %q, want proxy-reuse", lease.Mode)
	}
	tr, ok := lease.Client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", lease.Client.Transport)
	}
	req, err := http.NewRequest(http.MethodGet, "https://graph.microsoft.com/v1.0/me", nil)
	if err != nil {
		t.Fatal(err)
	}
	gotURL, err := tr.Proxy(req)
	if err != nil {
		t.Fatalf("proxy func returned error: %v", err)
	}
	if gotURL == nil || gotURL.String() != lease.ProxyURL {
		t.Fatalf("proxy URL = %v, want %s", gotURL, lease.ProxyURL)
	}
	if !connectSeen.Load() {
		t.Fatal("reusable proxy must pass a CONNECT probe before use")
	}
	assertTeamsGraphTransportBounds(t, tr)
}

func TestTeamsGraphHTTPClientSkipsReusableProxyWhenConnectProbeFails(t *testing.T) {
	lockCLITestHooks(t)

	instanceID := "inst-half-broken"
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen reusable proxy health server: %v", err)
	}
	proxyServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			http.Error(w, "socks tunnel is unavailable", http.StatusBadGateway)
			return
		}
		if r.URL.Path != "/_codex_proxy/health" {
			t.Fatalf("unexpected proxy probe path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "instanceId": instanceID})
	}))
	proxyServer.Listener = ln
	proxyServer.Start()
	defer proxyServer.Close()
	_, portText, err := net.SplitHostPort(proxyServer.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split proxy server addr: %v", err)
	}
	port, err := net.LookupPort("tcp", portText)
	if err != nil {
		t.Fatalf("parse proxy port: %v", err)
	}

	store := newTeamsGraphProxyTestStore(t, config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: boolPtr(true),
		Profiles: []config.Profile{{
			ID:        "p1",
			Name:      "profile",
			Host:      "example.com",
			Port:      22,
			User:      "alice",
			CreatedAt: time.Now(),
		}},
		Instances: []config.Instance{{
			ID:         instanceID,
			ProfileID:  "p1",
			Kind:       config.InstanceKindDaemon,
			HTTPPort:   port,
			DaemonPID:  os.Getpid(),
			StartedAt:  time.Now(),
			LastSeenAt: time.Now(),
		}},
	})
	origTarget := teamsGraphProxyCONNECTTarget
	t.Cleanup(func() { teamsGraphProxyCONNECTTarget = origTarget })
	teamsGraphProxyCONNECTTarget = "graph.example.test:443"
	origStackStart := stackStart
	t.Cleanup(func() { stackStart = origStackStart })
	sentinel := errors.New("fresh proxy stack requested")
	stackStart = func(config.Profile, string, stack.Options) (*stack.Stack, error) {
		return nil, sentinel
	}

	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	_, suspect, ok, err := reusableTeamsGraphProxyLease(cfg, "p1", manager.HealthClient{Timeout: time.Second})
	if err != nil {
		t.Fatalf("reusableTeamsGraphProxyLease: %v", err)
	}
	if ok {
		t.Fatal("CONNECT-broken reusable proxy must not be reused")
	}
	if suspect == nil || suspect.ID != instanceID {
		t.Fatalf("suspect = %+v, want %s", suspect, instanceID)
	}

	_, err = newTeamsGraphHTTPClientLease(context.Background(), &rootOptions{configPath: store.Path()}, nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("newTeamsGraphHTTPClientLease error = %v, want fresh stack sentinel", err)
	}
}

func TestTeamsGraphProxyLocalStatusDoesNotProbeTeamsConnect(t *testing.T) {
	lockCLITestHooks(t)

	instanceID := "inst-local"
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen reusable proxy health server: %v", err)
	}
	proxyServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			t.Fatalf("local status check must not probe Teams via CONNECT")
		}
		if r.URL.Path != "/_codex_proxy/health" {
			t.Fatalf("unexpected proxy probe path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "instanceId": instanceID})
	}))
	proxyServer.Listener = ln
	proxyServer.Start()
	defer proxyServer.Close()
	_, portText, err := net.SplitHostPort(proxyServer.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split proxy server addr: %v", err)
	}
	port, err := net.LookupPort("tcp", portText)
	if err != nil {
		t.Fatalf("parse proxy port: %v", err)
	}

	status, err := teamsGraphProxyLocalStatusFromConfig(context.Background(), config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: boolPtr(true),
		Profiles: []config.Profile{{
			ID:        "p1",
			Name:      "profile",
			Host:      "example.com",
			Port:      22,
			User:      "alice",
			CreatedAt: time.Now(),
		}},
		Instances: []config.Instance{{
			ID:         instanceID,
			ProfileID:  "p1",
			Kind:       config.InstanceKindDaemon,
			HTTPPort:   port,
			DaemonPID:  os.Getpid(),
			StartedAt:  time.Now(),
			LastSeenAt: time.Now(),
		}},
	}, manager.HealthClient{Timeout: time.Second})
	if err != nil {
		t.Fatalf("teamsGraphProxyLocalStatusFromConfig: %v", err)
	}
	if !status.Enabled || status.Profile.ID != "p1" || status.RegisteredDaemons != 1 {
		t.Fatalf("unexpected local status: %+v", status)
	}
	if status.HealthyInstance == nil || status.HealthyInstance.ID != instanceID {
		t.Fatalf("healthy instance = %+v, want %s", status.HealthyInstance, instanceID)
	}
}

func TestTeamsGraphProxyLocalStatusHonorsCancellation(t *testing.T) {
	lockCLITestHooks(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := teamsGraphProxyLocalStatusFromConfig(ctx, config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: boolPtr(true),
		Profiles: []config.Profile{{
			ID:        "p1",
			Name:      "profile",
			Host:      "example.com",
			Port:      22,
			User:      "alice",
			CreatedAt: time.Now(),
		}},
		Instances: []config.Instance{{
			ID:        "inst-canceled",
			ProfileID: "p1",
			Kind:      config.InstanceKindDaemon,
			HTTPPort:  18080,
			DaemonPID: os.Getpid(),
		}},
	}, manager.HealthClient{Timeout: time.Second})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("teamsGraphProxyLocalStatusFromConfig error = %v, want context canceled", err)
	}
}

func TestTeamsGraphHTTPClientLeaseStartFailureExplainsSilentTeams(t *testing.T) {
	lockCLITestHooks(t)

	store := newTeamsGraphProxyTestStore(t, config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: boolPtr(true),
		Profiles: []config.Profile{{
			ID:        "p1",
			Name:      "profile",
			Host:      "example.com",
			Port:      22,
			User:      "alice",
			CreatedAt: time.Now(),
		}},
	})
	origStackStart := stackStart
	t.Cleanup(func() { stackStart = origStackStart })
	sentinel := errors.New("ssh auth failed")
	stackStart = func(config.Profile, string, stack.Options) (*stack.Stack, error) {
		return nil, sentinel
	}

	_, err := newTeamsGraphHTTPClientLease(context.Background(), &rootOptions{configPath: store.Path()}, nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("newTeamsGraphHTTPClientLease error = %v, want sentinel", err)
	}
	for _, want := range []string{
		"start Teams Graph proxy stack",
		"Teams service could not start the SSH proxy stack",
		"Teams may appear silent",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error missing %q: %v", want, err)
		}
	}
}

func TestRetireTeamsGraphProxySuspectTerminatesVerifiedDaemon(t *testing.T) {
	lockCLITestHooks(t)

	inst := config.Instance{
		ID:        "inst-stale",
		ProfileID: "p1",
		Kind:      config.InstanceKindDaemon,
		HTTPPort:  18080,
		DaemonPID: 4242,
	}
	store := newTeamsGraphProxyTestStore(t, config.Config{
		Version:   config.CurrentVersion,
		Instances: []config.Instance{inst},
	})

	prevAlive := proxyProcessAlive
	prevLooksLike := proxyLooksLikeProxyDaemon
	prevCheckHTTPProxy := proxyCheckHTTPProxy
	prevFindProcess := proxyFindProcess
	prevTerminate := proxyTerminate
	t.Cleanup(func() {
		proxyProcessAlive = prevAlive
		proxyLooksLikeProxyDaemon = prevLooksLike
		proxyCheckHTTPProxy = prevCheckHTTPProxy
		proxyFindProcess = prevFindProcess
		proxyTerminate = prevTerminate
	})

	proxyProcessAlive = func(pid int) bool { return pid == inst.DaemonPID }
	proxyLooksLikeProxyDaemon = func(pid int) (bool, error) {
		if pid != inst.DaemonPID {
			t.Fatalf("looks-like pid = %d, want %d", pid, inst.DaemonPID)
		}
		return true, nil
	}
	var checkedPort int
	var checkedID string
	proxyCheckHTTPProxy = func(_ manager.HealthClient, port int, expectedInstanceID string) error {
		checkedPort = port
		checkedID = expectedInstanceID
		return nil
	}
	proxyFindProcess = func(pid int) (*os.Process, error) {
		if pid != inst.DaemonPID {
			t.Fatalf("find pid = %d, want %d", pid, inst.DaemonPID)
		}
		return &os.Process{Pid: pid}, nil
	}
	terminatedPID := 0
	proxyTerminate = func(p *os.Process, grace time.Duration) error {
		if p == nil {
			t.Fatal("expected process")
		}
		if grace != teamsGraphProxyRetireGrace {
			t.Fatalf("grace = %v, want %v", grace, teamsGraphProxyRetireGrace)
		}
		terminatedPID = p.Pid
		return nil
	}

	if err := retireTeamsGraphProxySuspect(context.Background(), store, inst, manager.HealthClient{Timeout: time.Second}, nil); err != nil {
		t.Fatalf("retireTeamsGraphProxySuspect: %v", err)
	}
	if checkedPort != inst.HTTPPort || checkedID != inst.ID {
		t.Fatalf("health recheck = port %d id %q, want port %d id %q", checkedPort, checkedID, inst.HTTPPort, inst.ID)
	}
	if terminatedPID != inst.DaemonPID {
		t.Fatalf("terminated pid = %d, want %d", terminatedPID, inst.DaemonPID)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Instances) != 0 {
		t.Fatalf("stale instance still registered: %+v", cfg.Instances)
	}
}

func TestTeamsGraphHTTPClientLeaseRetireSuspectsDeduplicates(t *testing.T) {
	lockCLITestHooks(t)

	inst := config.Instance{
		ID:        "inst-duplicate",
		ProfileID: "p1",
		Kind:      config.InstanceKindDaemon,
		HTTPPort:  18080,
		DaemonPID: 4242,
	}
	store := newTeamsGraphProxyTestStore(t, config.Config{
		Version:   config.CurrentVersion,
		Instances: []config.Instance{inst},
	})

	prevAlive := proxyProcessAlive
	prevLooksLike := proxyLooksLikeProxyDaemon
	prevCheckHTTPProxy := proxyCheckHTTPProxy
	prevFindProcess := proxyFindProcess
	prevTerminate := proxyTerminate
	t.Cleanup(func() {
		proxyProcessAlive = prevAlive
		proxyLooksLikeProxyDaemon = prevLooksLike
		proxyCheckHTTPProxy = prevCheckHTTPProxy
		proxyFindProcess = prevFindProcess
		proxyTerminate = prevTerminate
	})

	proxyProcessAlive = func(pid int) bool { return pid == inst.DaemonPID }
	proxyLooksLikeProxyDaemon = func(pid int) (bool, error) { return pid == inst.DaemonPID, nil }
	proxyCheckHTTPProxy = func(manager.HealthClient, int, string) error { return nil }
	proxyFindProcess = func(pid int) (*os.Process, error) { return &os.Process{Pid: pid}, nil }
	terminates := 0
	proxyTerminate = func(*os.Process, time.Duration) error {
		terminates++
		return nil
	}

	lease := teamsGraphHTTPClientLease{
		store:                  store,
		suspectReusableProxies: []config.Instance{inst, {ID: ""}, inst},
	}
	lease.RetireSuspects(context.Background(), nil)
	if terminates != 1 {
		t.Fatalf("terminates = %d, want 1", terminates)
	}
}

func TestRetireTeamsGraphProxySuspectDoesNotTerminateUnverifiedProcess(t *testing.T) {
	lockCLITestHooks(t)

	inst := config.Instance{
		ID:        "inst-unverified",
		ProfileID: "p1",
		Kind:      config.InstanceKindDaemon,
		HTTPPort:  18080,
		DaemonPID: 4242,
	}
	store := newTeamsGraphProxyTestStore(t, config.Config{
		Version:   config.CurrentVersion,
		Instances: []config.Instance{inst},
	})

	prevAlive := proxyProcessAlive
	prevLooksLike := proxyLooksLikeProxyDaemon
	prevCheckHTTPProxy := proxyCheckHTTPProxy
	prevFindProcess := proxyFindProcess
	prevTerminate := proxyTerminate
	t.Cleanup(func() {
		proxyProcessAlive = prevAlive
		proxyLooksLikeProxyDaemon = prevLooksLike
		proxyCheckHTTPProxy = prevCheckHTTPProxy
		proxyFindProcess = prevFindProcess
		proxyTerminate = prevTerminate
	})

	proxyProcessAlive = func(pid int) bool { return pid == inst.DaemonPID }
	proxyLooksLikeProxyDaemon = func(pid int) (bool, error) {
		if pid != inst.DaemonPID {
			t.Fatalf("looks-like pid = %d, want %d", pid, inst.DaemonPID)
		}
		return false, nil
	}
	proxyCheckHTTPProxy = func(manager.HealthClient, int, string) error {
		t.Fatal("health recheck must not run for an unverified process")
		return nil
	}
	proxyFindProcess = func(int) (*os.Process, error) {
		t.Fatal("find process must not run for an unverified process")
		return nil, nil
	}
	proxyTerminate = func(*os.Process, time.Duration) error {
		t.Fatal("terminate must not run for an unverified process")
		return nil
	}

	if err := retireTeamsGraphProxySuspect(context.Background(), store, inst, manager.HealthClient{Timeout: time.Second}, nil); err != nil {
		t.Fatalf("retireTeamsGraphProxySuspect: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Instances) != 0 {
		t.Fatalf("unverified stale instance should be unregistered: %+v", cfg.Instances)
	}
}

func TestTeamsGraphHTTPClientTransportBoundsIdleConnections(t *testing.T) {
	direct := newTeamsDirectHTTPClient()
	directTransport, ok := direct.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("direct transport = %T, want *http.Transport", direct.Transport)
	}
	assertTeamsGraphTransportBounds(t, directTransport)
	if directTransport.Proxy != nil {
		t.Fatal("direct Teams Graph transport should not use a proxy")
	}

	proxied, err := newTeamsProxyHTTPClient("http://127.0.0.1:12345")
	if err != nil {
		t.Fatalf("newTeamsProxyHTTPClient: %v", err)
	}
	proxyTransport, ok := proxied.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("proxy transport = %T, want *http.Transport", proxied.Transport)
	}
	assertTeamsGraphTransportBounds(t, proxyTransport)
	req, err := http.NewRequest(http.MethodGet, "https://graph.microsoft.com/v1.0/me", nil)
	if err != nil {
		t.Fatal(err)
	}
	gotURL, err := proxyTransport.Proxy(req)
	if err != nil {
		t.Fatalf("proxy func returned error: %v", err)
	}
	if gotURL == nil || gotURL.String() != "http://127.0.0.1:12345" {
		t.Fatalf("proxy URL = %v, want http://127.0.0.1:12345", gotURL)
	}
}

func assertTeamsGraphTransportBounds(t *testing.T, tr *http.Transport) {
	t.Helper()
	if !tr.ForceAttemptHTTP2 {
		t.Fatal("Teams Graph transport should keep HTTP/2 enabled")
	}
	if tr.MaxIdleConns != 100 {
		t.Fatalf("MaxIdleConns = %d, want 100", tr.MaxIdleConns)
	}
	if tr.MaxIdleConnsPerHost != 16 {
		t.Fatalf("MaxIdleConnsPerHost = %d, want 16", tr.MaxIdleConnsPerHost)
	}
	if tr.IdleConnTimeout != 90*time.Second {
		t.Fatalf("IdleConnTimeout = %s, want 90s", tr.IdleConnTimeout)
	}
	if tr.TLSHandshakeTimeout != 10*time.Second {
		t.Fatalf("TLSHandshakeTimeout = %s, want 10s", tr.TLSHandshakeTimeout)
	}
}

func newTeamsGraphProxyTestStore(t *testing.T, cfg config.Config) *config.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.Save(cfg); err != nil {
		t.Fatalf("save store: %v", err)
	}
	return store
}
