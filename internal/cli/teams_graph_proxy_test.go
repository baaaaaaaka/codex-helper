package cli

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
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
}

func TestTeamsGraphHTTPClientUsesConfiguredReusableProxy(t *testing.T) {
	lockCLITestHooks(t)
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")

	instanceID := "inst-1"
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_codex_proxy/health" {
			t.Fatalf("unexpected proxy probe path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "instanceId": instanceID})
	}))
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
