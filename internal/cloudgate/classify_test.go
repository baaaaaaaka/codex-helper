package cloudgate

import "testing"

func TestIsRustlsWithRustlsSuites(t *testing.T) {
	info := &ClientHelloInfo{
		CipherSuites: []uint16{
			0x1301, 0x1302, 0x1303,
			0xC02B, 0xC02C, 0xCCA9,
			0xC02F, 0xC030, 0xCCA8,
		},
	}
	if !IsRustlsClient(info) {
		t.Error("expected rustls classification for exact rustls suites")
	}
}

func TestIsRustlsWithSignalingSuites(t *testing.T) {
	info := &ClientHelloInfo{
		CipherSuites: []uint16{
			0x1301, 0x1302, 0x1303,
			0xC02B, 0xC02C, 0xCCA9,
			0xC02F, 0xC030, 0xCCA8,
			0x00FF, 0x5600,
		},
	}
	if !IsRustlsClient(info) {
		t.Error("expected rustls classification with signaling suites")
	}
}

func TestIsRustlsWithOpenSSLSuites(t *testing.T) {
	// OpenSSL/native-tls typically includes RSA/CBC legacy suites.
	info := &ClientHelloInfo{
		CipherSuites: []uint16{
			0x1301, 0x1302, 0x1303,
			0xC02B, 0xC02C, 0xCCA9,
			0xC02F, 0xC030, 0xCCA8,
			0x009C, 0x009D, // RSA_WITH_AES_128/256_GCM
			0x002F, 0x0035, // RSA_WITH_AES_128/256_CBC
			0xC013, 0xC014, // ECDHE_RSA_WITH_AES_128/256_CBC
			0x003C, 0x003D,
			0x0067, 0x006B,
			0xC009, 0xC00A,
			0x00FF,
		},
	}
	if IsRustlsClient(info) {
		t.Error("expected non-rustls classification for OpenSSL suites")
	}
}

func TestIsRustlsWithMixedSuites(t *testing.T) {
	// All rustls + one extra unknown suite.
	info := &ClientHelloInfo{
		CipherSuites: []uint16{
			0x1301, 0x1302, 0x1303,
			0xC02B, 0xC02C, 0xCCA9,
			0xC02F, 0xC030, 0xCCA8,
			0x009C, // Extra suite not in rustls set.
		},
	}
	if IsRustlsClient(info) {
		t.Error("expected non-rustls for mixed suites")
	}
}

func TestIsRustlsEmpty(t *testing.T) {
	info := &ClientHelloInfo{CipherSuites: nil}
	if IsRustlsClient(info) {
		t.Error("expected false for empty suites")
	}
}

func TestIsRustlsNilInfo(t *testing.T) {
	if IsRustlsClient(nil) {
		t.Error("expected false for nil info")
	}
}

func TestIsRustlsSubsetOfRustls(t *testing.T) {
	// Only TLS 1.3 suites — a subset of rustls.
	info := &ClientHelloInfo{
		CipherSuites: []uint16{0x1301, 0x1302, 0x1303},
	}
	if !IsRustlsClient(info) {
		t.Error("expected rustls classification for subset of rustls suites")
	}
}
