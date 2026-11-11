package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appgateway"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestTUIGatewayWaitsForBackend(t *testing.T) {
	oldPoll := codexAppProxyPollInterval
	codexAppProxyPollInterval = time.Millisecond
	t.Cleanup(func() { codexAppProxyPollInterval = oldPoll })
	var ready atomic.Bool
	observed := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok := ready.Load()
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
			select {
			case observed <- struct{}{}:
			default:
			}
		}
		json.NewEncoder(w).Encode(appgateway.Health{InstanceID: "gateway", OK: ok, Error: "SSH recovering"})
	}))
	defer srv.Close()
	port, _ := strconv.Atoi(strings.Split(srv.Listener.Addr().String(), ":")[1])
	dir := t.TempDir()
	registry, err := appgateway.NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.EnsureWithPort(context.Background(), "profile", "fingerprint", port)
	registry.Close()
	if err != nil {
		t.Fatal(err)
	}
	// The listener reports the registry's actual identity; startup must reuse
	// that listener rather than attempt another gateway election.
	reg.ReplacedInstanceID = "gateway"
	registry, err = appgateway.NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Save(reg); err != nil {
		t.Fatal(err)
	}
	registry.Close()
	oldEnsure, oldDir := codexAppEnsureProxyURLFn, appGatewayRegistryDirFn
	codexAppEnsureProxyURLFn = func(context.Context, *config.Store, config.Profile, []config.Instance, io.Writer) (string, error) {
		return srv.URL, nil
	}
	appGatewayRegistryDirFn = func() (string, error) { return dir, nil }
	t.Cleanup(func() { codexAppEnsureProxyURLFn, appGatewayRegistryDirFn = oldEnsure, oldDir })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	launched := make(chan error, 1)
	go func() {
		_, err := ensureCodexTUIProxyURL(ctx, nil, config.Profile{ID: "profile"}, nil, io.Discard)
		launched <- err
	}()
	select {
	case <-observed:
	case <-ctx.Done():
		t.Fatal("health probe did not run")
	}
	// Observe the next probe before releasing recovery. A listener-only wait
	// returns after the first response and cannot reach this second probe.
	select {
	case err := <-launched:
		t.Fatalf("TUI would start before backend: %v", err)
	case <-observed:
	case <-ctx.Done():
		t.Fatal("wait did not continue probing the pending backend")
	}
	ready.Store(true)
	if err := <-launched; err != nil {
		t.Fatal(err)
	}
}

func TestTUIGatewayCancelTimeoutAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, owner string
		timeout     bool
	}{
		{"cancel", "gateway", false}, {"timeout", "gateway", true}, {"wrong-instance", "other", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(503)
				json.NewEncoder(w).Encode(appgateway.Health{InstanceID: tc.owner, Error: "SSH denied"})
			}))
			defer srv.Close()
			port, _ := strconv.Atoi(strings.Split(srv.Listener.Addr().String(), ":")[1])
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.name == "cancel" {
				cancel()
			}
			if tc.timeout {
				var done context.CancelFunc
				ctx, done = context.WithTimeout(ctx, 20*time.Millisecond)
				defer done()
			}
			err := waitForTUIAppGateway(ctx, appgateway.Registration{ID: "gateway", HTTPPort: port})
			if err == nil {
				t.Fatal("accepted unavailable gateway")
			}
			if tc.name == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if tc.timeout && (!errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "SSH denied")) {
				t.Fatal(err)
			}
			if tc.name == "wrong-instance" && !strings.Contains(err.Error(), "owned by") {
				t.Fatal(err)
			}
		})
	}
}
