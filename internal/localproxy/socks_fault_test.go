package localproxy

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func TestSOCKS5DialerContextCancelsHalfOpenHandshake(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()

	dialer, err := NewSOCKS5Dialer(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("NewSOCKS5Dialer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	cd, ok := dialer.(ContextDialer)
	if !ok {
		t.Fatal("SOCKS5 dialer does not implement ContextDialer")
	}
	started := time.Now()
	_, err = cd.DialContext(ctx, "tcp", "example.com:443")
	if err == nil {
		t.Fatal("expected half-open handshake to fail")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("half-open handshake took too long: %s", elapsed)
	}
	_ = ln.Close()
	<-serverDone
}
