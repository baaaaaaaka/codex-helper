package cloudgate

// knownRustlsSuites is the complete set of cipher suites offered by rustls.
var knownRustlsSuites = map[uint16]bool{
	// TLS 1.3
	0x1301: true, // TLS_AES_128_GCM_SHA256
	0x1302: true, // TLS_AES_256_GCM_SHA384
	0x1303: true, // TLS_CHACHA20_POLY1305_SHA256
	// TLS 1.2 ECDHE_ECDSA
	0xC02B: true, // TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
	0xC02C: true, // TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
	0xCCA9: true, // TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256
	// TLS 1.2 ECDHE_RSA
	0xC02F: true, // TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
	0xC030: true, // TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
	0xCCA8: true, // TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
	// Signaling
	0x00FF: true, // TLS_EMPTY_RENEGOTIATION_INFO_SCSV
	0x5600: true, // TLS_FALLBACK_SCSV (sometimes present)
}

// IsRustlsClient returns true when every cipher suite in the Client Hello
// belongs to the known rustls set.  An empty suite list returns false (safe
// default: don't block).
func IsRustlsClient(info *ClientHelloInfo) bool {
	if info == nil || len(info.CipherSuites) == 0 {
		return false
	}
	for _, cs := range info.CipherSuites {
		if !knownRustlsSuites[cs] {
			return false
		}
	}
	return true
}
