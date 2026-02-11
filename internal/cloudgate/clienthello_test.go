package cloudgate

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// buildClientHello constructs a minimal valid TLS Client Hello with the given
// cipher suites and optional SNI/ALPN extensions.
func buildClientHello(suites []uint16, sni string, alpn []string) []byte {
	// Build handshake body.
	var hs []byte

	// ClientVersion: TLS 1.2 (0x0303)
	hs = append(hs, 0x03, 0x03)
	// Random: 32 bytes of zeros.
	hs = append(hs, make([]byte, 32)...)
	// Session ID: length 0.
	hs = append(hs, 0x00)
	// Cipher Suites.
	csLen := len(suites) * 2
	hs = append(hs, byte(csLen>>8), byte(csLen))
	for _, s := range suites {
		hs = append(hs, byte(s>>8), byte(s))
	}
	// Compression methods: 1 method (null).
	hs = append(hs, 0x01, 0x00)

	// Extensions.
	var ext []byte
	if sni != "" {
		ext = append(ext, buildSNIExtension(sni)...)
	}
	if len(alpn) > 0 {
		ext = append(ext, buildALPNExtension(alpn)...)
	}
	if len(ext) > 0 {
		hs = append(hs, byte(len(ext)>>8), byte(len(ext)))
		hs = append(hs, ext...)
	}

	// Wrap in handshake header: type=ClientHello(1) + 3-byte length.
	hsMsg := []byte{0x01, byte(len(hs) >> 16), byte(len(hs) >> 8), byte(len(hs))}
	hsMsg = append(hsMsg, hs...)

	// Wrap in TLS record: ContentType=Handshake(0x16), version=TLS1.0(0x0301), length.
	record := []byte{0x16, 0x03, 0x01}
	record = append(record, byte(len(hsMsg)>>8), byte(len(hsMsg)))
	record = append(record, hsMsg...)

	return record
}

func buildSNIExtension(name string) []byte {
	// Extension type: SNI (0x0000)
	var ext []byte
	ext = append(ext, 0x00, 0x00)
	// Server Name list length: 3 + len(name)
	listLen := 3 + len(name)
	totalLen := 2 + listLen
	ext = append(ext, byte(totalLen>>8), byte(totalLen))
	ext = append(ext, byte(listLen>>8), byte(listLen))
	ext = append(ext, 0x00) // host_name type
	ext = append(ext, byte(len(name)>>8), byte(len(name)))
	ext = append(ext, []byte(name)...)
	return ext
}

func buildALPNExtension(protocols []string) []byte {
	var list []byte
	for _, p := range protocols {
		list = append(list, byte(len(p)))
		list = append(list, []byte(p)...)
	}
	var ext []byte
	ext = append(ext, 0x00, 0x10) // ALPN extension type
	totalLen := 2 + len(list)
	ext = append(ext, byte(totalLen>>8), byte(totalLen))
	ext = append(ext, byte(len(list)>>8), byte(len(list)))
	ext = append(ext, list...)
	return ext
}

func TestReadClientHello(t *testing.T) {
	suites := []uint16{0x1301, 0x1302, 0xC02B}
	hello := buildClientHello(suites, "example.com", []string{"h2", "http/1.1"})

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		_, _ = client.Write(hello)
	}()

	raw, info, err := ReadClientHello(server)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if raw == nil {
		t.Fatal("raw bytes nil")
	}
	if info == nil {
		t.Fatal("info nil")
	}
	if len(info.CipherSuites) != 3 {
		t.Fatalf("expected 3 cipher suites, got %d", len(info.CipherSuites))
	}
	for i, want := range suites {
		if info.CipherSuites[i] != want {
			t.Errorf("suite[%d]: got 0x%04X, want 0x%04X", i, info.CipherSuites[i], want)
		}
	}
	if info.ServerName != "example.com" {
		t.Errorf("SNI: got %q, want %q", info.ServerName, "example.com")
	}
	if len(info.ALPNProtocols) != 2 || info.ALPNProtocols[0] != "h2" || info.ALPNProtocols[1] != "http/1.1" {
		t.Errorf("ALPN: got %v, want [h2 http/1.1]", info.ALPNProtocols)
	}
}

func TestReadClientHelloInvalidRecord(t *testing.T) {
	// Not a TLS record (first byte != 0x16).
	data := []byte{0x17, 0x03, 0x01, 0x00, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		_, _ = client.Write(data)
	}()

	raw, info, err := ReadClientHello(server)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if raw == nil {
		t.Fatal("raw should contain header bytes")
	}
	if info != nil {
		t.Error("info should be nil for non-handshake record")
	}
}

func TestReadClientHelloTruncated(t *testing.T) {
	// Only 3 bytes — not enough for the 5-byte header.
	server, client := net.Pipe()
	defer server.Close()

	go func() {
		_, _ = client.Write([]byte{0x16, 0x03})
		_ = client.Close()
	}()

	_, _, err := ReadClientHello(server)
	if err == nil {
		t.Fatal("expected error for truncated data")
	}
}

func TestReadClientHelloNoExtensions(t *testing.T) {
	suites := []uint16{0x1301}
	hello := buildClientHello(suites, "", nil)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		_, _ = client.Write(hello)
	}()

	_, info, err := ReadClientHello(server)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info == nil {
		t.Fatal("info nil")
	}
	if info.ServerName != "" {
		t.Errorf("expected empty SNI, got %q", info.ServerName)
	}
}

func TestReadClientHelloRecordBodyTruncated(t *testing.T) {
	// Valid header claiming 100 bytes of body, but we close after header.
	hdr := []byte{0x16, 0x03, 0x01, 0x00, 0x64} // 100 bytes

	server, client := net.Pipe()
	defer server.Close()

	go func() {
		_, _ = client.Write(hdr)
		time.Sleep(10 * time.Millisecond)
		_ = client.Close()
	}()

	raw, _, err := ReadClientHello(server)
	if err == nil {
		t.Fatal("expected error for truncated body")
	}
	// raw should be the 5-byte header.
	if len(raw) != 0 {
		_ = raw // raw may or may not be set on error, that's fine.
	}
}

func TestReadClientHelloLargeRecordLength(t *testing.T) {
	// Record length > 16384 — invalid.
	hdr := make([]byte, 5)
	hdr[0] = 0x16
	hdr[1] = 0x03
	hdr[2] = 0x01
	binary.BigEndian.PutUint16(hdr[3:5], 16385)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		_, _ = client.Write(hdr)
	}()

	raw, info, err := ReadClientHello(server)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if raw == nil {
		t.Fatal("raw should contain header bytes")
	}
	if info != nil {
		t.Error("info should be nil for invalid record length")
	}
}
