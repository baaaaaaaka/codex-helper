package localproxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// ProbeSOCKS5 verifies the SOCKS5 greeting without opening an upstream
// connection. It is deliberately local and deterministic: callers can use it
// after a tunnel process starts or after a resume event without probing an
// external account or target.
func ProbeSOCKS5(ctx context.Context, addr string, timeout time.Duration) error {
	conn, deadline, stop, err := openSOCKS5Probe(ctx, addr, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	defer stop()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	return probeSOCKS5Greeting(conn)
}

// ProbeSOCKS5Target verifies both the local SOCKS5 listener and the remote
// path by asking it to connect to targetHost:targetPort. The connection is
// closed immediately after the SOCKS5 success reply; no application payload
// is sent.
func ProbeSOCKS5Target(ctx context.Context, socksAddr, targetHost string, targetPort int, timeout time.Duration) error {
	if targetPort <= 0 || targetPort > 65535 {
		return fmt.Errorf("invalid SOCKS5 probe target port %d", targetPort)
	}
	if targetHost == "" {
		return fmt.Errorf("SOCKS5 probe target host is required")
	}
	if len(targetHost) > 255 {
		return fmt.Errorf("SOCKS5 probe target host is too long")
	}
	conn, deadline, stop, err := openSOCKS5Probe(ctx, socksAddr, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	defer stop()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	if err := probeSOCKS5Greeting(conn); err != nil {
		return err
	}
	request := []byte{0x05, 0x01, 0x00, 0x03, byte(len(targetHost))}
	request = append(request, targetHost...)
	var port [2]byte
	binary.BigEndian.PutUint16(port[:], uint16(targetPort))
	request = append(request, port[:]...)
	if _, err := conn.Write(request); err != nil {
		return fmt.Errorf("write SOCKS5 connect request: %w", err)
	}
	var reply [4]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return fmt.Errorf("read SOCKS5 connect reply: %w", err)
	}
	if reply[0] != 0x05 {
		return fmt.Errorf("unexpected SOCKS5 reply version %d", reply[0])
	}
	if reply[1] != 0x00 {
		return fmt.Errorf("SOCKS5 connect rejected with code 0x%02x", reply[1])
	}
	if err := discardSOCKS5BoundAddress(conn, reply[3]); err != nil {
		return err
	}
	return nil
}

func openSOCKS5Probe(ctx context.Context, addr string, timeout time.Duration) (net.Conn, time.Time, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(probeCtx, "tcp", addr)
	if err != nil {
		return nil, time.Time{}, func() {}, err
	}
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	return conn, deadline, func() { _ = stop() }, nil
}

func probeSOCKS5Greeting(conn net.Conn) error {
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		return fmt.Errorf("write SOCKS5 greeting: %w", err)
	}
	var response [2]byte
	if _, err := io.ReadFull(conn, response[:]); err != nil {
		return fmt.Errorf("read SOCKS5 greeting: %w", err)
	}
	if response[0] != 0x05 {
		return fmt.Errorf("unexpected SOCKS5 version %d", response[0])
	}
	if response[1] != 0x00 {
		return fmt.Errorf("SOCKS5 server rejected no-auth method: 0x%02x", response[1])
	}
	return nil
}

func discardSOCKS5BoundAddress(conn net.Conn, atyp byte) error {
	var length int
	switch atyp {
	case 0x01:
		length = net.IPv4len
	case 0x04:
		length = net.IPv6len
	case 0x03:
		var size [1]byte
		if _, err := io.ReadFull(conn, size[:]); err != nil {
			return fmt.Errorf("read SOCKS5 bound host length: %w", err)
		}
		length = int(size[0])
	default:
		return fmt.Errorf("unsupported SOCKS5 bound address type 0x%02x", atyp)
	}
	bound := make([]byte, length+2)
	if _, err := io.ReadFull(conn, bound); err != nil {
		return fmt.Errorf("read SOCKS5 bound address: %w", err)
	}
	return nil
}
