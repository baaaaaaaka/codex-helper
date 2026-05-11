//go:build linux

package ssh

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func TestTunnelIntegrationStartStop(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skipf("ssh not available: %v", err)
	}

	host, port, user, key := sshIntegrationEnv(t)

	socksPort := freeIntegrationPort(t)

	cfg := TunnelConfig{
		Host:      host,
		Port:      port,
		User:      user,
		SocksPort: socksPort,
		ExtraArgs: []string{
			"-i", key,
			"-o", "StrictHostKeyChecking=no",
			"-o", "UserKnownHostsFile=/dev/null",
		},
		BatchMode: true,
	}
	tun, err := NewTunnel(cfg)
	if err != nil {
		t.Fatalf("NewTunnel error: %v", err)
	}

	if err := tun.Start(); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	t.Cleanup(func() { _ = tun.Stop(1 * time.Second) })

	if tun.PID() == 0 {
		t.Fatalf("expected tunnel PID to be set")
	}

	select {
	case <-tun.Done():
		// Retry once to avoid flakiness if sshd isn't fully ready.
		if err := tun.Stop(200 * time.Millisecond); err != nil {
			// best effort cleanup
		}
		tun, err = NewTunnel(cfg)
		if err != nil {
			t.Fatalf("NewTunnel retry error: %v", err)
		}
		if err := tun.Start(); err != nil {
			t.Fatalf("Start retry error: %v", err)
		}
		select {
		case <-tun.Done():
			t.Fatalf("tunnel exited early")
		case <-time.After(300 * time.Millisecond):
		}
	case <-time.After(300 * time.Millisecond):
	}

	if err := tun.Stop(1 * time.Second); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("Stop error: %v", err)
		}
	}
}

func TestTunnelIntegrationAcceptsNewHostKey(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skipf("ssh not available: %v", err)
	}

	host, port, user, key := sshIntegrationEnv(t)
	knownHosts := t.TempDir() + "/known_hosts"
	socksPort := freeIntegrationPort(t)

	cfg := TunnelConfig{
		Host:      host,
		Port:      port,
		User:      user,
		SocksPort: socksPort,
		ExtraArgs: []string{
			"-i", key,
			"-o", "UserKnownHostsFile=" + knownHosts,
			"-o", "GlobalKnownHostsFile=/dev/null",
			"-o", "IdentitiesOnly=yes",
		},
		BatchMode: true,
	}
	tun, err := NewTunnel(cfg)
	if err != nil {
		t.Fatalf("NewTunnel error: %v", err)
	}

	if err := tun.Start(); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	t.Cleanup(func() { _ = tun.Stop(1 * time.Second) })

	waitForIntegrationSocks(t, socksPort, tun)

	data, err := os.ReadFile(knownHosts)
	if err != nil {
		t.Fatalf("expected accept-new to write known_hosts: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("expected known_hosts to contain accepted host key")
	}

	if err := tun.Stop(1 * time.Second); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("Stop error: %v", err)
		}
	}
}

func sshIntegrationEnv(t *testing.T) (host string, port int, user string, key string) {
	t.Helper()
	if os.Getenv("SSH_TEST_ENABLED") != "1" {
		t.Skip("SSH integration tests disabled")
	}
	host = os.Getenv("SSH_TEST_HOST")
	portStr := os.Getenv("SSH_TEST_PORT")
	user = os.Getenv("SSH_TEST_USER")
	key = os.Getenv("SSH_TEST_KEY")
	if host == "" || portStr == "" || user == "" || key == "" {
		t.Skip("missing SSH_TEST_* env vars")
	}
	var err error
	port, err = strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("invalid SSH_TEST_PORT: %v", err)
	}
	return host, port, user, key
}

func freeIntegrationPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	_ = ln.Close()
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse socks port: %v", err)
	}
	return port
}

func waitForIntegrationSocks(t *testing.T, socksPort int, tun *Tunnel) {
	t.Helper()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(socksPort))
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-tun.Done():
			t.Fatalf("tunnel exited before SOCKS listener was ready: %v", tun.Wait())
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for SOCKS listener on %s: %v", addr, lastErr)
}
