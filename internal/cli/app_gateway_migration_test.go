package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/appgateway"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestEnsureCodexAppGatewayURLPreservesHealthyLegacyPort(t *testing.T) {
	stateDir := t.TempDir()
	resetAppGatewayEnsureHooks(t, stateDir)
	server, port := startLegacyHealthServer(t, "legacy-instance")
	defer server.Close()
	store, profile := appGatewayTestStore(t, []config.Instance{{ID: "legacy-instance", ProfileID: "profile-a", HTTPPort: port}})

	var serviceCalls atomic.Int32
	var startCalls atomic.Int32
	appGatewayEnsureServiceFn = func(context.Context, *config.Store, config.Profile, appgateway.Registration, string) (bool, error) {
		serviceCalls.Add(1)
		return false, nil
	}
	appGatewayStartDetachedFn = func(*config.Store, config.Profile, *appgateway.Registry, appgateway.Registration, string, io.Writer) (*detachedProcess, error) {
		startCalls.Add(1)
		return nil, nil
	}

	got, err := ensureCodexAppGatewayURL(context.Background(), store, profile, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("http://127.0.0.1:%d", port); got != want {
		t.Fatalf("gateway URL = %q, want %q", got, want)
	}
	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.Load(profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reg.HTTPPort != port || reg.ReplacedInstanceID != "legacy-instance" {
		t.Fatalf("legacy migration registration = %#v", reg)
	}
	if serviceCalls.Load() != 1 || startCalls.Load() != 1 {
		t.Fatalf("legacy takeover calls = service %d/start %d", serviceCalls.Load(), startCalls.Load())
	}
}

func TestEnsureCodexAppGatewayURLAdoptsPrunedDesktopPort(t *testing.T) {
	stateDir := t.TempDir()
	resetAppGatewayEnsureHooks(t, stateDir)
	server, port := startAppGatewayHealthServer(t, "legacy-owner", true)
	defer server.Close()
	store, profile := appGatewayTestStore(t, nil)

	appGatewayDiscoverDesktopProxyPorts = func(context.Context) ([]int, error) {
		return []int{port}, nil
	}
	var startCalls atomic.Int32
	appGatewayStartDetachedFn = func(*config.Store, config.Profile, *appgateway.Registry, appgateway.Registration, string, io.Writer) (*detachedProcess, error) {
		startCalls.Add(1)
		return nil, nil
	}
	got, err := ensureCodexAppGatewayURL(context.Background(), store, profile, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != fmt.Sprintf("http://127.0.0.1:%d", port) {
		t.Fatalf("adopted URL = %q", got)
	}
	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.Load(profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reg.HTTPPort != port || reg.ReplacedInstanceID != "legacy-owner" || startCalls.Load() != 1 {
		t.Fatalf("pruned-port migration = %#v/start calls %d", reg, startCalls.Load())
	}
}

func TestEnsureCodexAppGatewayURLRefusesAmbiguousOrOccupiedDesktopPorts(t *testing.T) {
	tests := []struct {
		name       string
		ports      func(t *testing.T) []int
		wantSubstr string
	}{
		{
			name:       "ambiguous",
			ports:      func(*testing.T) []int { return []int{3804, 2743} },
			wantSubstr: "multiple proxy ports",
		},
		{
			name: "occupied-without-identity",
			ports: func(t *testing.T) []int {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = listener.Close() })
				return []int{listener.Addr().(*net.TCPAddr).Port}
			},
			wantSubstr: "occupied proxy port",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stateDir := t.TempDir()
			resetAppGatewayEnsureHooks(t, stateDir)
			store, profile := appGatewayTestStore(t, nil)
			appGatewayDiscoverDesktopProxyPorts = func(context.Context) ([]int, error) {
				return tc.ports(t), nil
			}
			var startCalls atomic.Int32
			appGatewayStartDetachedFn = func(*config.Store, config.Profile, *appgateway.Registry, appgateway.Registration, string, io.Writer) (*detachedProcess, error) {
				startCalls.Add(1)
				return nil, nil
			}
			_, err := ensureCodexAppGatewayURL(context.Background(), store, profile, nil, nil)
			if err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("migration error = %v, want %q", err, tc.wantSubstr)
			}
			if startCalls.Load() != 0 {
				t.Fatalf("refused migration started a daemon: %d", startCalls.Load())
			}
		})
	}
}

func TestEnsureCodexAppGatewayURLAcceptsPendingStableGatewayWithoutDuplicate(t *testing.T) {
	stateDir := t.TempDir()
	resetAppGatewayEnsureHooks(t, stateDir)
	store, profile := appGatewayTestStore(t, nil)
	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.Ensure(context.Background(), profile.ID, "fingerprint")
	if err != nil {
		t.Fatal(err)
	}
	frontend, err := appgateway.NewFrontend(reg.ID, reg.HTTPPort)
	if err != nil {
		t.Fatal(err)
	}
	defer frontend.Close(context.Background())
	var serviceCalls atomic.Int32
	var startCalls atomic.Int32
	appGatewayEnsureServiceFn = func(context.Context, *config.Store, config.Profile, appgateway.Registration, string) (bool, error) {
		serviceCalls.Add(1)
		return false, nil
	}
	appGatewayStartDetachedFn = func(*config.Store, config.Profile, *appgateway.Registry, appgateway.Registration, string, io.Writer) (*detachedProcess, error) {
		startCalls.Add(1)
		return nil, nil
	}

	got, err := ensureCodexAppGatewayURL(context.Background(), store, profile, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != fmt.Sprintf("http://127.0.0.1:%d", reg.HTTPPort) || serviceCalls.Load() != 0 || startCalls.Load() != 0 {
		t.Fatalf("pending gateway reuse = %q, service %d/start %d", got, serviceCalls.Load(), startCalls.Load())
	}
}

func TestEnsureCodexAppGatewayURLRejectsWrongStableGatewayIdentity(t *testing.T) {
	stateDir := t.TempDir()
	resetAppGatewayEnsureHooks(t, stateDir)
	store, profile := appGatewayTestStore(t, nil)
	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.Ensure(context.Background(), profile.ID, "fingerprint")
	if err != nil {
		t.Fatal(err)
	}
	frontend, err := appgateway.NewFrontend("wrong-owner", reg.HTTPPort)
	if err != nil {
		t.Fatal(err)
	}
	defer frontend.Close(context.Background())
	appGatewayStartDetachedFn = func(*config.Store, config.Profile, *appgateway.Registry, appgateway.Registration, string, io.Writer) (*detachedProcess, error) {
		t.Fatal("wrong gateway identity must not start a replacement")
		return nil, nil
	}
	if _, err := ensureCodexAppGatewayURL(context.Background(), store, profile, nil, nil); err == nil || !strings.Contains(err.Error(), "owned by") {
		t.Fatalf("wrong identity error = %v", err)
	}
}

func TestEnsureAppGatewayProcessDoesNotDuplicatePartialServiceInstall(t *testing.T) {
	stateDir := t.TempDir()
	resetAppGatewayEnsureHooks(t, stateDir)
	store, profile := appGatewayTestStore(t, nil)
	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.Ensure(context.Background(), profile.ID, "fingerprint")
	if err != nil {
		t.Fatal(err)
	}
	var startCalls atomic.Int32
	appGatewayEnsureServiceFn = func(context.Context, *config.Store, config.Profile, appgateway.Registration, string) (bool, error) {
		return true, fmt.Errorf("launcher write failed after task registration")
	}
	appGatewayWaitFn = func(context.Context, int, string) error { return nil }
	appGatewayStartDetachedFn = func(*config.Store, config.Profile, *appgateway.Registry, appgateway.Registration, string, io.Writer) (*detachedProcess, error) {
		startCalls.Add(1)
		return nil, nil
	}
	got, err := ensureAppGatewayProcess(context.Background(), store, profile, registry, reg, stateDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != fmt.Sprintf("http://127.0.0.1:%d", reg.HTTPPort) || startCalls.Load() != 0 {
		t.Fatalf("partial service install fallback = %q/start calls %d", got, startCalls.Load())
	}
}

func TestEnsureCodexAppGatewayURLConcurrentCallsReuseOnePendingGateway(t *testing.T) {
	stateDir := t.TempDir()
	resetAppGatewayEnsureHooks(t, stateDir)
	store, profile := appGatewayTestStore(t, nil)
	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.Ensure(context.Background(), profile.ID, "fingerprint")
	if err != nil {
		t.Fatal(err)
	}
	frontend, err := appgateway.NewFrontend(reg.ID, reg.HTTPPort)
	if err != nil {
		t.Fatal(err)
	}
	defer frontend.Close(context.Background())
	var starts atomic.Int32
	appGatewayStartDetachedFn = func(*config.Store, config.Profile, *appgateway.Registry, appgateway.Registration, string, io.Writer) (*detachedProcess, error) {
		starts.Add(1)
		return nil, nil
	}
	const callers = 8
	results := make([]string, callers)
	errorsSeen := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index], errorsSeen[index] = ensureCodexAppGatewayURL(context.Background(), store, profile, nil, nil)
		}(i)
	}
	wg.Wait()
	for i := range results {
		if errorsSeen[i] != nil || results[i] != fmt.Sprintf("http://127.0.0.1:%d", reg.HTTPPort) {
			t.Fatalf("concurrent call %d = %q/%v", i, results[i], errorsSeen[i])
		}
	}
	if starts.Load() != 0 {
		t.Fatalf("concurrent pending reuse started %d replacement daemons", starts.Load())
	}
}

func resetAppGatewayEnsureHooks(t *testing.T, stateDir string) {
	t.Helper()
	oldRegistryDir := appGatewayRegistryDirFn
	oldDiscover := appGatewayDiscoverDesktopProxyPorts
	oldEnsureService := appGatewayEnsureServiceFn
	oldStart := appGatewayStartDetachedFn
	oldWait := appGatewayWaitFn
	t.Cleanup(func() {
		appGatewayRegistryDirFn = oldRegistryDir
		appGatewayDiscoverDesktopProxyPorts = oldDiscover
		appGatewayEnsureServiceFn = oldEnsureService
		appGatewayStartDetachedFn = oldStart
		appGatewayWaitFn = oldWait
	})
	appGatewayRegistryDirFn = func() (string, error) { return stateDir, nil }
	appGatewayDiscoverDesktopProxyPorts = func(context.Context) ([]int, error) { return nil, nil }
	appGatewayEnsureServiceFn = func(context.Context, *config.Store, config.Profile, appgateway.Registration, string) (bool, error) {
		return false, nil
	}
	appGatewayStartDetachedFn = func(*config.Store, config.Profile, *appgateway.Registry, appgateway.Registration, string, io.Writer) (*detachedProcess, error) {
		return nil, nil
	}
	appGatewayWaitFn = waitForAppGateway
}

func appGatewayTestStore(t *testing.T, instances []config.Instance) (*config.Store, config.Profile) {
	t.Helper()
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	profile := config.Profile{ID: "profile-a", Name: "profile-a", Host: "host", Port: 22}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Profiles: []config.Profile{profile}, Instances: instances}); err != nil {
		t.Fatal(err)
	}
	return store, profile
}

func startLegacyHealthServer(t *testing.T, instanceID string) (*httptest.Server, int) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_codex_proxy/health" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"ok":true,"instanceId":%q}`, instanceID)
	}))
	return server, server.Listener.Addr().(*net.TCPAddr).Port
}

func startAppGatewayHealthServer(t *testing.T, instanceID string, ok bool) (*httptest.Server, int) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_codex_proxy/health" {
			http.NotFound(w, r)
			return
		}
		status := http.StatusOK
		if !ok {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(w, `{"ok":%t,"instanceId":%q}`, ok, instanceID)
	}))
	return server, server.Listener.Addr().(*net.TCPAddr).Port
}
