//go:build linux

package stack

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestStackIntegrationHTTPProxyConnectThroughSSHTunnel(t *testing.T) {
	if os.Getenv("SSH_TEST_ENABLED") != "1" {
		t.Skip("SSH integration tests disabled")
	}
	host := os.Getenv("SSH_TEST_HOST")
	portStr := os.Getenv("SSH_TEST_PORT")
	user := os.Getenv("SSH_TEST_USER")
	key := os.Getenv("SSH_TEST_KEY")
	if host == "" || portStr == "" || user == "" || key == "" {
		t.Skip("missing SSH_TEST_* env vars")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("invalid SSH_TEST_PORT: %v", err)
	}

	echoAddr, closeEcho := startStackIntegrationEcho(t)
	defer closeEcho()

	st, err := Start(config.Profile{
		ID:   "ssh-test",
		Name: "ssh-test",
		Host: host,
		Port: port,
		User: user,
		SSHArgs: []string{
			"-i", key,
			"-o", "StrictHostKeyChecking=no",
			"-o", "UserKnownHostsFile=/dev/null",
			"-o", "IdentitiesOnly=yes",
			"-o", "GSSAPIAuthentication=no",
		},
	}, "stack-integration", Options{
		SocksReadyTimeout: 5 * time.Second,
		RestartBackoff:    100 * time.Millisecond,
		TunnelStopGrace:   500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Start stack: %v", err)
	}
	defer func() { _ = st.Close(context.Background()) }()

	resp, err := http.Get(st.HTTPProxyURL() + "/_codex_proxy/health")
	if err != nil {
		t.Fatalf("GET health: %v", err)
	}
	defer resp.Body.Close()
	var health struct {
		OK         bool   `json:"ok"`
		InstanceID string `json:"instanceId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if !health.OK || health.InstanceID != "stack-integration" {
		t.Fatalf("unexpected health response: %+v", health)
	}

	conn, err := net.DialTimeout("tcp", st.HTTPAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial HTTP proxy: %v", err)
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", echoAddr, echoAddr)
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read CONNECT status: %v", err)
	}
	if status != "HTTP/1.1 200 Connection Established\r\n" {
		t.Fatalf("unexpected CONNECT status: %q", status)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read CONNECT headers: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}
	if _, err := conn.Write([]byte("ping\n")); err != nil {
		t.Fatalf("write echo payload: %v", err)
	}
	echo, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read echo payload: %v", err)
	}
	if echo != "ping\n" {
		t.Fatalf("expected echo payload, got %q", echo)
	}

	if err := st.Close(context.Background()); err != nil {
		t.Fatalf("Close stack: %v", err)
	}
	if c, err := net.DialTimeout("tcp", st.HTTPAddr, 100*time.Millisecond); err == nil {
		_ = c.Close()
		t.Fatalf("expected HTTP proxy listener to be closed after stack Close")
	}
}

func startStackIntegrationEcho(t *testing.T) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen echo: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}
