package stack

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/ssh"
)

func TestStartValidationErrors(t *testing.T) {
	profile := config.Profile{Host: "", Port: 22, User: "u"}
	if _, err := Start(profile, "id", Options{}); err == nil {
		t.Fatalf("expected error for missing host")
	}

	profile = config.Profile{Host: "h", Port: 0, User: "u"}
	if _, err := Start(profile, "id", Options{}); err == nil {
		t.Fatalf("expected error for missing port")
	}

	profile = config.Profile{Host: "h", Port: 22, User: ""}
	if _, err := Start(profile, "id", Options{}); err == nil {
		t.Fatalf("expected error for missing user")
	}

	profile = config.Profile{Host: "h", Port: 22, User: "u"}
	if _, err := Start(profile, "", Options{}); err == nil {
		t.Fatalf("expected error for missing instance id")
	}
}

func TestStackHelpers(t *testing.T) {
	s := &Stack{
		HTTPPort: 1234,
		fatalCh:  make(chan error),
		stopCh:   make(chan struct{}),
	}
	if got := s.HTTPProxyURL(); got != "http://127.0.0.1:1234" {
		t.Fatalf("unexpected HTTPProxyURL: %q", got)
	}
	if s.Fatal() == nil {
		t.Fatalf("expected fatal channel")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	select {
	case <-s.stopCh:
	default:
		t.Fatalf("expected stopCh to be closed")
	}
}

func TestNewTunnel(t *testing.T) {
	profile := config.Profile{Host: "example.com", Port: 22, User: "alice"}
	if _, err := newTunnel(profile, 12345); err != nil {
		t.Fatalf("newTunnel error: %v", err)
	}
}

func TestStartFailsWhenHTTPPortInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	profile := config.Profile{Host: "example.com", Port: 22, User: "alice"}
	_, err = Start(profile, "inst-1", Options{
		HTTPListenAddr:    ln.Addr().String(),
		SocksPort:         12345,
		SocksReadyTimeout: 10 * time.Millisecond,
	})
	if err == nil {
		t.Fatalf("expected Start to fail for occupied http port")
	}
}

func TestStartFailsWhenSSHMissing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip ssh PATH test on windows")
	}
	t.Setenv("PATH", "")
	profile := config.Profile{Host: "example.com", Port: 22, User: "alice"}
	_, err := Start(profile, "inst-ssh-missing", Options{
		SocksPort:         12345,
		SocksReadyTimeout: 10 * time.Millisecond,
	})
	if err == nil {
		t.Fatalf("expected Start to fail when ssh is missing")
	}
}

func TestSocksPortReservationPreventsCollision(t *testing.T) {
	// Verify the reservation pattern: holding a listener prevents
	// another :0 bind from grabbing the same port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	reserved := ln.Addr().(*net.TCPAddr).Port

	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		ln.Close()
		t.Fatalf("listen2: %v", err)
	}
	other := ln2.Addr().(*net.TCPAddr).Port
	ln2.Close()
	ln.Close()

	if reserved == other {
		t.Fatalf("expected different ports when first is held, both got %d", reserved)
	}
}

func TestStartWithAutoSocksPortFailsCleanly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip ssh PATH test on windows")
	}
	// Verify Start with SocksPort=0 (auto-select + reservation path)
	// cleans up properly when SSH is missing.
	t.Setenv("PATH", "")

	profile := config.Profile{Host: "example.com", Port: 22, User: "alice"}
	_, err := Start(profile, "inst-auto-socks", Options{
		SocksPort:         0,
		SocksReadyTimeout: 10 * time.Millisecond,
	})
	if err == nil {
		t.Fatalf("expected Start to fail when ssh is missing")
	}
}

// ---------------------------------------------------------------------------
// reservePort / setupHTTPProxy helpers
// ---------------------------------------------------------------------------

func TestReservePort(t *testing.T) {
	port, ln, err := reservePort()
	if err != nil {
		t.Fatalf("reservePort: %v", err)
	}
	defer ln.Close()

	if port <= 0 || port > 65535 {
		t.Fatalf("invalid port: %d", port)
	}
	// Port must be held — binding the same port should fail.
	_, err = net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err == nil {
		t.Fatalf("expected bind to fail while port is reserved")
	}
}

func TestReservePortReleaseMakesPortBindable(t *testing.T) {
	port, ln, err := reservePort()
	if err != nil {
		t.Fatalf("reservePort: %v", err)
	}
	ln.Close()

	ln2, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("expected port to be bindable after release: %v", err)
	}
	ln2.Close()
}

func TestSetupHTTPProxy(t *testing.T) {
	// socksAddr doesn't need a real SOCKS server — the dialer is lazy.
	ps, err := setupHTTPProxy("127.0.0.1:19999", "127.0.0.1:0", "test-id")
	if err != nil {
		t.Fatalf("setupHTTPProxy: %v", err)
	}
	defer ps.proxy.Close(context.Background())

	if ps.httpPort <= 0 {
		t.Fatalf("invalid httpPort: %d", ps.httpPort)
	}
	if ps.httpAddr == "" {
		t.Fatalf("empty httpAddr")
	}
	if ps.proxy == nil {
		t.Fatalf("nil proxy")
	}
}

func TestSetupHTTPProxyHealthEndpoint(t *testing.T) {
	ps, err := setupHTTPProxy("127.0.0.1:19999", "127.0.0.1:0", "inst-health-42")
	if err != nil {
		t.Fatalf("setupHTTPProxy: %v", err)
	}
	defer ps.proxy.Close(context.Background())

	client := &http.Client{
		Transport: &http.Transport{Proxy: nil},
		Timeout:   time.Second,
	}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/_codex_proxy/health", ps.httpPort))
	if err != nil {
		t.Fatalf("GET health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["instanceId"] != "inst-health-42" {
		t.Fatalf("unexpected instanceId: %v", body["instanceId"])
	}
}

func TestSetupHTTPProxyFailsOnOccupiedPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	_, err = setupHTTPProxy("127.0.0.1:19999", ln.Addr().String(), "test-id")
	if err == nil {
		t.Fatalf("expected error when HTTP port is occupied")
	}
}

// TestReservationEnsuresDifferentPorts verifies end-to-end that holding a
// reservation while the HTTP proxy binds :0 prevents port collision.
func TestReservationEnsuresDifferentPorts(t *testing.T) {
	port, ln, err := reservePort()
	if err != nil {
		t.Fatalf("reservePort: %v", err)
	}

	socksAddr := fmt.Sprintf("127.0.0.1:%d", port)
	ps, err := setupHTTPProxy(socksAddr, "127.0.0.1:0", "test-inst")
	if err != nil {
		ln.Close()
		t.Fatalf("setupHTTPProxy: %v", err)
	}
	defer ps.proxy.Close(context.Background())
	ln.Close()

	if ps.httpPort == port {
		t.Fatalf("HTTP port %d collided with reserved SOCKS port %d", ps.httpPort, port)
	}
}

// ---------------------------------------------------------------------------
// Start retry / canRetry behaviour
// ---------------------------------------------------------------------------

// fakeSSHScript returns a shell script that increments a counter file and
// then blocks indefinitely. Requires /usr/bin/sleep; callers should skip
// if it is unavailable.
func fakeSSHScript(counterFile string) string {
	return fmt.Sprintf("#!/bin/sh\necho x >> '%s'\nexec /usr/bin/sleep 30\n", counterFile)
}

func TestStartRetriesWithAutoPort(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip on windows")
	}
	if _, err := os.Stat("/usr/bin/sleep"); err != nil {
		t.Skip("skip: /usr/bin/sleep not available")
	}

	dir := t.TempDir()
	counterFile := filepath.Join(dir, "counter")
	script := filepath.Join(dir, "ssh")
	if err := os.WriteFile(script, []byte(fakeSSHScript(counterFile)), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}
	t.Setenv("PATH", dir)

	profile := config.Profile{Host: "example.com", Port: 22, User: "alice"}
	st, err := Start(profile, "inst-retry-auto", Options{
		SocksPort:         0,
		SocksReadyTimeout: 50 * time.Millisecond,
		TunnelStopGrace:   200 * time.Millisecond,
	})
	if st != nil {
		_ = st.Close(context.Background())
	}
	if err == nil {
		t.Fatalf("expected error with non-functional SSH")
	}

	data, _ := os.ReadFile(counterFile)
	count := strings.Count(string(data), "x")
	// maxPortRetries=3 → 4 attempts (initial + 3 retries).
	if count != 4 {
		t.Fatalf("expected 4 SSH invocations (initial + 3 retries), got %d", count)
	}
}

func TestStartExplicitPortDoesNotRetry(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip on windows")
	}
	if _, err := os.Stat("/usr/bin/sleep"); err != nil {
		t.Skip("skip: /usr/bin/sleep not available")
	}

	dir := t.TempDir()
	counterFile := filepath.Join(dir, "counter")
	script := filepath.Join(dir, "ssh")
	if err := os.WriteFile(script, []byte(fakeSSHScript(counterFile)), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}
	t.Setenv("PATH", dir)

	// Use a port that is (very likely) free.
	tmpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	explicitPort := tmpLn.Addr().(*net.TCPAddr).Port
	tmpLn.Close()

	profile := config.Profile{Host: "example.com", Port: 22, User: "alice"}
	st, err := Start(profile, "inst-retry-explicit", Options{
		SocksPort:         explicitPort,
		SocksReadyTimeout: 50 * time.Millisecond,
		TunnelStopGrace:   200 * time.Millisecond,
	})
	if st != nil {
		_ = st.Close(context.Background())
	}
	if err == nil {
		t.Fatalf("expected error with non-functional SSH")
	}

	data, _ := os.ReadFile(counterFile)
	count := strings.Count(string(data), "x")
	if count != 1 {
		t.Fatalf("expected 1 SSH invocation (no retry with explicit port), got %d", count)
	}
}

// ---------------------------------------------------------------------------
// monitor
// ---------------------------------------------------------------------------

func TestMonitorReportsFatalOnImmediateExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "ssh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}
	t.Setenv("PATH", dir)

	tun, err := ssh.NewTunnel(ssh.TunnelConfig{
		Host:      "example.com",
		Port:      22,
		User:      "alice",
		SocksPort: 12345,
	})
	if err != nil {
		t.Fatalf("NewTunnel error: %v", err)
	}
	if err := tun.Start(); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	s := &Stack{
		Profile:   config.Profile{Host: "example.com", Port: 22, User: "alice"},
		SocksPort: 12345,
		tunnel:    tun,
		fatalCh:   make(chan error, 1),
		stopCh:    make(chan struct{}),
	}

	go s.monitor(Options{MaxRestarts: 0, RestartBackoff: 5 * time.Millisecond})

	select {
	case err := <-s.fatalCh:
		if err == nil {
			t.Fatalf("expected fatal error")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for fatal error")
	}
}
