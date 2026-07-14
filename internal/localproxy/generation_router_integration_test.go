package localproxy

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestHTTPProxyGenerationRouterRecoversAfterSOCKSFailure(t *testing.T) {
	echoAddr, closeEcho := startTCPEchoServer(t)
	defer closeEcho()

	first := startSOCKS5Server(t)
	second := startSOCKS5Server(t)
	defer first.Close()
	defer second.Close()

	firstDialer, err := NewSOCKS5Dialer(first.Addr(), time.Second)
	if err != nil {
		t.Fatalf("first dialer: %v", err)
	}
	secondDialer, err := NewSOCKS5Dialer(second.Addr(), time.Second)
	if err != nil {
		t.Fatalf("second dialer: %v", err)
	}
	router, err := NewGenerationRouter(firstDialer)
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	defer router.Close(context.Background())

	proxy := NewHTTPProxy(router, Options{InstanceID: "generation-recovery", TunnelIdleTimeout: time.Second})
	proxyAddr, err := proxy.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy start: %v", err)
	}
	defer proxy.Close(context.Background())

	assertConnectEcho := func(label string) {
		t.Helper()
		conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
		if err != nil {
			t.Fatalf("%s dial proxy: %v", label, err)
		}
		defer conn.Close()
		reader := openConnectTunnelTo(t, conn, echoAddr)
		if _, err := fmt.Fprint(conn, label); err != nil {
			t.Fatalf("%s write: %v", label, err)
		}
		got, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("%s read: %v", label, err)
		}
		if got != label {
			t.Fatalf("%s echo = %q", label, got)
		}
	}

	assertConnectEcho("first\n")
	oldGeneration := router.CurrentGeneration()
	first.Close()

	newGeneration, err := router.Swap(secondDialer)
	if err != nil {
		t.Fatalf("swap after SOCKS failure: %v", err)
	}
	if newGeneration != oldGeneration {
		t.Fatalf("old generation = %d, want %d", newGeneration, oldGeneration)
	}
	assertConnectEcho("recovered\n")
	if router.CurrentGeneration() == oldGeneration {
		t.Fatal("router did not expose a new generation")
	}
}

func TestHTTPProxyGenerationSwapHasNoListenerRefusedWindow(t *testing.T) {
	first := startSOCKS5Server(t)
	second := startSOCKS5Server(t)
	defer first.Close()
	defer second.Close()
	firstDialer, err := NewSOCKS5Dialer(first.Addr(), time.Second)
	if err != nil {
		t.Fatalf("first dialer: %v", err)
	}
	secondDialer, err := NewSOCKS5Dialer(second.Addr(), time.Second)
	if err != nil {
		t.Fatalf("second dialer: %v", err)
	}
	router, err := NewGenerationRouter(firstDialer)
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	defer router.Close(context.Background())
	proxy := NewHTTPProxy(router, Options{InstanceID: "stable-listener"})
	proxyAddr, err := proxy.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy start: %v", err)
	}
	defer proxy.Close(context.Background())

	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			if _, err := router.Swap(secondDialer); err != nil {
				t.Fatalf("swap %d: %v", i, err)
			}
		} else if _, err := router.Swap(firstDialer); err != nil {
			t.Fatalf("swap %d: %v", i, err)
		}
		conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
		if err != nil {
			t.Fatalf("listener refused connection after swap %d: %v", i, err)
		}
		_ = conn.Close()
	}
}

func openConnectTunnelTo(t *testing.T, conn net.Conn, target string) *bufio.Reader {
	t.Helper()
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read CONNECT status: %v", err)
	}
	if status != "HTTP/1.1 200 Connection Established\r\n" {
		t.Fatalf("CONNECT status = %q", status)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read CONNECT header: %v", err)
		}
		if line == "\r\n" {
			return reader
		}
	}
}
