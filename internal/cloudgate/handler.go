package cloudgate

import (
	"io"
	"net"
)

// Dialer abstracts net dialing for testing.
type Dialer interface {
	Dial(network, addr string) (net.Conn, error)
}

// HandleConnect processes a CONNECT-hijacked connection for a target host.
// The caller must already have hijacked the HTTP connection and sent "200
// Connection Established". clientConn is the raw TCP connection to the client.
//
// Decision tree:
//  1. Read TLS Client Hello.
//  2. If rustls → close connection (Layer 1: fail-open bypass).
//  3. If !rustls + MITM available → MITM (Layer 2: inject fake response).
//  4. If !rustls + no MITM → transparent tunnel with replayed bytes.
//  5. Parse failure → transparent tunnel (safe fallback).
func HandleConnect(clientConn net.Conn, targetHost string, dialer Dialer, cfg *Config) error {
	raw, info, err := ReadClientHello(clientConn)
	if err != nil || raw == nil {
		_ = clientConn.Close()
		return err
	}

	if info == nil {
		return tunnelWithPrefix(clientConn, raw, targetHost, dialer)
	}

	if IsRustlsClient(info) {
		_ = clientConn.Close()
		return nil
	}

	if cfg != nil && cfg.MITM != nil {
		return handleMITM(clientConn, raw, targetHost, dialer, cfg.MITM)
	}

	return tunnelWithPrefix(clientConn, raw, targetHost, dialer)
}

// tunnelWithPrefix dials the target and tunnels data, replaying the already-read
// Client Hello bytes first.
func tunnelWithPrefix(clientConn net.Conn, prefix []byte, targetHost string, dialer Dialer) error {
	upstream, err := dialer.Dial("tcp", targetHost)
	if err != nil {
		_ = clientConn.Close()
		return err
	}

	// Write the already-read bytes to upstream.
	if _, err := upstream.Write(prefix); err != nil {
		_ = upstream.Close()
		_ = clientConn.Close()
		return err
	}

	// Bidirectional copy.
	go func() {
		_, _ = io.Copy(upstream, clientConn)
		_ = upstream.Close()
	}()
	_, _ = io.Copy(clientConn, upstream)
	_ = clientConn.Close()
	return nil
}
