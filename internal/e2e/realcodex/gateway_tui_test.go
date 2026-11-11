//go:build linux

package realcodex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
)

// Exercise the production startup boundary with a real selector and Codex TUI.
// The reused gateway's health is externally controlled, without sleeps or SSH.
func TestRealCodexGatewayRecoveryTUI(t *testing.T) {
	native := codexBin(t)
	cxp := os.Getenv("E2E_CXP_BIN")
	if cxp == "" {
		t.Fatal("E2E_CXP_BIN is required")
	}
	fg := fakegraph.New(fakegraph.Options{})
	t.Cleanup(fg.Close)
	env := harness.NewEnv(t, fg)
	resp := NewResponses()
	t.Cleanup(resp.Close)
	if err := WriteCodexHome(env.CodexHome, resp.URL()); err != nil {
		t.Fatal(err)
	}
	config := env.Vars["CODEX_HELPER_CONFIG"]
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, []byte(`{"version":6,"proxyEnabled":true,"agentAutoApproveEnabled":false,"profiles":[{"id":"recovery","name":"recovery","host":"localhost","port":22,"user":"owner"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	var ready atomic.Bool
	var waiting atomic.Bool
	observed := make(chan struct{}, 8)
	identity := fmt.Sprintf("%x", sha256.Sum256([]byte("recovery")))
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok := ready.Load()
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"instanceId": identity, "ok": ok, "error": "controlled SSH recovery"})
		if waiting.Load() && !ok {
			select {
			case observed <- struct{}{}:
			default:
			}
		}
	}))
	defer srv.Close()
	port, _ := strconv.Atoi(strings.Split(srv.Listener.Addr().String(), ":")[1])
	// A historical on-disk fixture is independent of product writers. The
	// child imports it through the supported startup path before reuse.
	registryDir := filepath.Join(env.Vars["CODEX_HELPER_STATE_DIR"], "app-gateway", "registrations")
	if err := os.MkdirAll(registryDir, 0700); err != nil {
		t.Fatal(err)
	}
	registration, err := json.Marshal(map[string]any{"schema": 1, "id": identity, "profileId": "recovery", "httpPort": port, "state": "pending"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(registryDir, identity+".json"), registration, 0600); err != nil {
		t.Fatal(err)
	}
	srv.Start()
	vars := env.Vars
	vars["PATH"], vars["TERM"], vars["PROBE_API_KEY"], vars["CXP_AUTO_UPDATE"] = "/usr/local/bin:/usr/bin:/bin", "xterm-256color", "probe-key", "0"
	var processEnv []string
	for k, v := range vars {
		processEnv = append(processEnv, k+"="+v)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	w := startNativePTY(t, ctx, cxp, []string{"--config", config, "history", "tui", "--profile", "recovery", "--codex-path", native, "--refresh-interval", "0"}, env.WorkDir(t, "gateway-recovery"), processEnv)
	waitNative(t, ctx, "CXP selector", func() bool { return strings.Contains(w.text(), "Ctrl+N: new") })
	w.send(t, "\x0e")
	waitNative(t, ctx, "gateway wait", func() bool { return strings.Contains(w.text(), "waiting for App Gateway SSH backend") })
	waiting.Store(true)
	// Two later failed probes prove startup is still waiting, rather than
	// merely observing an earlier listener-discovery response.
	for i := 0; i < 2; i++ {
		select {
		case <-observed:
		case <-ctx.Done():
			t.Fatal("gateway stopped probing before recovery")
		}
	}
	if strings.Contains(w.text(), "›") || len(resp.Requests()) != 0 {
		t.Fatal("native TUI started before its backend was ready")
	}
	processes, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range processes {
		cmdline, _ := os.ReadFile(filepath.Join("/proc", p.Name(), "cmdline"))
		if bytes.HasPrefix(cmdline, []byte(native+"\x00app-server\x00")) {
			t.Fatal("account-initialization backend started before gateway recovery")
		}
	}
	ready.Store(true)
	waitNative(t, ctx, "native Codex input after recovery", func() bool { return strings.Contains(w.text(), "›") })
	w.submit(t, ctx, "gateway-recovered-first")
	waitNative(t, ctx, "first response after recovery", func() bool { return strings.Contains(w.text(), "ANSWER(gateway-recovered-first)") })
}
