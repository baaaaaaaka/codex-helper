package cloudgate

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type mockDialer struct {
	conn net.Conn
	err  error
}

func (d *mockDialer) Dial(_, _ string) (net.Conn, error) {
	if d.err != nil {
		return nil, d.err
	}
	return d.conn, nil
}

func TestHandlerBlocksRustls(t *testing.T) {
	// Rustls cipher suites only.
	hello := buildClientHello([]uint16{
		0x1301, 0x1302, 0x1303,
		0xC02B, 0xC02C, 0xCCA9,
		0xC02F, 0xC030, 0xCCA8,
	}, "chatgpt.com", nil)

	clientSide, proxySide := net.Pipe()
	defer clientSide.Close()

	cfg := DefaultConfig()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = HandleConnect(proxySide, "chatgpt.com:443", &mockDialer{err: io.EOF}, cfg)
	}()

	// Write the Client Hello from the client side.
	_, _ = clientSide.Write(hello)

	// The proxy should close the connection. Reading should return EOF.
	buf := make([]byte, 1)
	_ = clientSide.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := clientSide.Read(buf)
	if err == nil {
		t.Error("expected connection to be closed (EOF or pipe error)")
	}

	wg.Wait()
}

func TestHandlerTunnelsNativeTLS(t *testing.T) {
	// Native-TLS suites (includes non-rustls suites).
	hello := buildClientHello([]uint16{
		0x1301, 0x1302, 0xC02B, 0x009C, 0x002F,
	}, "chatgpt.com", nil)

	clientSide, proxySide := net.Pipe()
	upstreamClient, upstreamServer := net.Pipe()
	defer clientSide.Close()
	defer upstreamServer.Close()

	cfg := DefaultConfig()
	dialer := &mockDialer{conn: upstreamClient}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = HandleConnect(proxySide, "chatgpt.com:443", dialer, cfg)
	}()

	// Write Client Hello.
	_, _ = clientSide.Write(hello)

	// The proxy should forward the hello to upstream.
	buf := make([]byte, len(hello))
	_ = upstreamServer.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := io.ReadFull(upstreamServer, buf)
	if err != nil {
		t.Fatalf("expected hello forwarded to upstream, got err: %v (read %d bytes)", err, n)
	}

	// Verify the upstream received the exact hello bytes.
	for i := range hello {
		if buf[i] != hello[i] {
			t.Fatalf("mismatch at byte %d", i)
		}
	}

	// Additional data should also be forwarded.
	testData := []byte("additional data from client")
	_, _ = clientSide.Write(testData)

	readBuf := make([]byte, len(testData))
	_ = upstreamServer.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(upstreamServer, readBuf)
	if err != nil {
		t.Fatalf("expected additional data forwarded: %v", err)
	}
	if string(readBuf) != string(testData) {
		t.Errorf("got %q, want %q", readBuf, testData)
	}

	_ = clientSide.Close()
	wg.Wait()
}

func TestHandlerFallsBackOnParseFailure(t *testing.T) {
	// Not a valid TLS record — garbage data.
	garbage := []byte{0x17, 0x03, 0x01, 0x00, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05}

	clientSide, proxySide := net.Pipe()
	upstreamClient, upstreamServer := net.Pipe()
	defer clientSide.Close()
	defer upstreamServer.Close()

	cfg := DefaultConfig()
	dialer := &mockDialer{conn: upstreamClient}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = HandleConnect(proxySide, "chatgpt.com:443", dialer, cfg)
	}()

	// Write garbage in a goroutine since net.Pipe is synchronous:
	// clientSide.Write blocks until all bytes are read from proxySide.
	go func() {
		_, _ = clientSide.Write(garbage)
		_ = clientSide.Close()
	}()

	// The proxy should tunnel transparently — all garbage bytes should reach upstream.
	buf := make([]byte, len(garbage))
	_ = upstreamServer.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := io.ReadFull(upstreamServer, buf)
	if err != nil {
		t.Fatalf("expected fallback tunnel: %v", err)
	}

	for i := range garbage {
		if buf[i] != garbage[i] {
			t.Fatalf("mismatch at byte %d: got 0x%02X, want 0x%02X", i, buf[i], garbage[i])
		}
	}

	wg.Wait()
}

func TestHandlerDialFailure(t *testing.T) {
	// Non-rustls hello but dial fails.
	hello := buildClientHello([]uint16{0x1301, 0x009C}, "chatgpt.com", nil)

	clientSide, proxySide := net.Pipe()
	defer clientSide.Close()

	cfg := DefaultConfig()
	dialer := &mockDialer{err: net.ErrClosed}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = HandleConnect(proxySide, "chatgpt.com:443", dialer, cfg)
	}()

	_, _ = clientSide.Write(hello)

	// Connection should be closed.
	buf := make([]byte, 1)
	_ = clientSide.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := clientSide.Read(buf)
	if err == nil {
		t.Error("expected connection closed on dial failure")
	}

	wg.Wait()
}
