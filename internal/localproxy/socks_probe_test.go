package localproxy

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestProbeSOCKS5ChecksGreeting(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		greeting := make([]byte, 3)
		if _, err := io.ReadFull(conn, greeting); err == nil && string(greeting) == string([]byte{5, 1, 0}) {
			_, _ = conn.Write([]byte{5, 0})
		}
	}()

	if err := ProbeSOCKS5(context.Background(), ln.Addr().String(), time.Second); err != nil {
		t.Fatalf("ProbeSOCKS5: %v", err)
	}
	_ = ln.Close()
	<-done
}

func TestProbeSOCKS5RejectsUnsupportedMethod(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 3)
		_, _ = io.ReadFull(conn, buf)
		_, _ = conn.Write([]byte{5, 0xff})
	}()

	if err := ProbeSOCKS5(context.Background(), ln.Addr().String(), time.Second); err == nil {
		t.Fatal("expected unsupported method error")
	}
}

func TestProbeSOCKS5HonorsCancellation(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			defer conn.Close()
			_, _ = io.Copy(io.Discard, conn)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := ProbeSOCKS5(ctx, ln.Addr().String(), time.Second); err == nil {
		t.Fatal("expected cancellation error")
	}
	if elapsed := time.Since(started); elapsed > time.Second/2 {
		t.Fatalf("ProbeSOCKS5 did not honor cancellation promptly: %s", elapsed)
	}
}

func TestProbeSOCKS5TargetChecksRemotePath(t *testing.T) {
	targetAddr, closeTarget := startTCPEchoServer(t)
	defer closeTarget()
	targetHost, targetPortText, err := net.SplitHostPort(targetAddr)
	if err != nil {
		t.Fatalf("split target address: %v", err)
	}
	targetPort, err := strconv.Atoi(targetPortText)
	if err != nil {
		t.Fatalf("parse target port: %v", err)
	}
	socks := startSOCKS5Server(t)
	defer socks.Close()

	if err := ProbeSOCKS5Target(context.Background(), socks.Addr(), targetHost, targetPort, time.Second); err != nil {
		t.Fatalf("ProbeSOCKS5Target: %v", err)
	}
	if got := socks.ConnectCount(); got != 1 {
		t.Fatalf("SOCKS connect count = %d, want 1", got)
	}
}
