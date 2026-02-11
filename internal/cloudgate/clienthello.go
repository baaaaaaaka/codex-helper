package cloudgate

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// ClientHelloInfo contains parsed fields from a TLS Client Hello message.
type ClientHelloInfo struct {
	CipherSuites  []uint16
	ServerName    string
	ALPNProtocols []string
}

// ReadClientHello reads a TLS Client Hello from conn and returns both the raw
// bytes (for replay) and parsed info. On parse failure, raw bytes may still be
// returned with a nil info so the caller can replay them transparently.
func ReadClientHello(conn net.Conn) (raw []byte, info *ClientHelloInfo, err error) {
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	// Read 5-byte TLS record header.
	hdr := make([]byte, 5)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return nil, nil, fmt.Errorf("read TLS record header: %w", err)
	}

	// ContentType must be Handshake (0x16).
	if hdr[0] != 0x16 {
		return hdr, nil, nil
	}

	recordLen := int(binary.BigEndian.Uint16(hdr[3:5]))
	if recordLen <= 0 || recordLen > 16384 {
		return hdr, nil, nil
	}

	body := make([]byte, recordLen)
	if _, err := io.ReadFull(conn, body); err != nil {
		return hdr, nil, fmt.Errorf("read TLS record body: %w", err)
	}

	raw = append(hdr, body...)

	parsed, parseErr := parseClientHello(body)
	if parseErr != nil {
		return raw, nil, nil
	}
	return raw, parsed, nil
}

func parseClientHello(data []byte) (*ClientHelloInfo, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("empty handshake data")
	}
	// HandshakeType must be ClientHello (0x01).
	if data[0] != 0x01 {
		return nil, fmt.Errorf("not a ClientHello: type=%d", data[0])
	}
	if len(data) < 4 {
		return nil, fmt.Errorf("handshake too short for length")
	}
	hsLen := int(data[1])<<16 | int(data[2])<<8 | int(data[3])
	data = data[4:]
	if len(data) < hsLen {
		return nil, fmt.Errorf("handshake body truncated")
	}
	data = data[:hsLen]

	// ClientVersion (2) + Random (32) = 34 bytes.
	if len(data) < 34 {
		return nil, fmt.Errorf("ClientHello too short for version+random")
	}
	data = data[34:]

	// Session ID (variable).
	if len(data) < 1 {
		return nil, fmt.Errorf("missing session ID length")
	}
	sidLen := int(data[0])
	data = data[1:]
	if len(data) < sidLen {
		return nil, fmt.Errorf("session ID truncated")
	}
	data = data[sidLen:]

	// Cipher Suites.
	if len(data) < 2 {
		return nil, fmt.Errorf("missing cipher suites length")
	}
	csLen := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]
	if len(data) < csLen || csLen%2 != 0 {
		return nil, fmt.Errorf("cipher suites truncated or invalid")
	}
	numSuites := csLen / 2
	suites := make([]uint16, numSuites)
	for i := 0; i < numSuites; i++ {
		suites[i] = binary.BigEndian.Uint16(data[i*2 : i*2+2])
	}
	data = data[csLen:]

	info := &ClientHelloInfo{CipherSuites: suites}

	// Compression methods (skip).
	if len(data) < 1 {
		return info, nil
	}
	compLen := int(data[0])
	data = data[1:]
	if len(data) < compLen {
		return info, nil
	}
	data = data[compLen:]

	// Extensions.
	if len(data) < 2 {
		return info, nil
	}
	extLen := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]
	if len(data) < extLen {
		return info, nil
	}
	data = data[:extLen]

	for len(data) >= 4 {
		extType := binary.BigEndian.Uint16(data[:2])
		extDataLen := int(binary.BigEndian.Uint16(data[2:4]))
		data = data[4:]
		if len(data) < extDataLen {
			break
		}
		extData := data[:extDataLen]
		data = data[extDataLen:]

		switch extType {
		case 0x0000: // SNI
			info.ServerName = parseSNI(extData)
		case 0x0010: // ALPN
			info.ALPNProtocols = parseALPN(extData)
		}
	}

	return info, nil
}

func parseSNI(data []byte) string {
	if len(data) < 2 {
		return ""
	}
	listLen := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]
	if len(data) < listLen {
		return ""
	}
	data = data[:listLen]
	for len(data) >= 3 {
		nameType := data[0]
		nameLen := int(binary.BigEndian.Uint16(data[1:3]))
		data = data[3:]
		if len(data) < nameLen {
			break
		}
		if nameType == 0 { // host_name
			return string(data[:nameLen])
		}
		data = data[nameLen:]
	}
	return ""
}

func parseALPN(data []byte) []string {
	if len(data) < 2 {
		return nil
	}
	listLen := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]
	if len(data) < listLen {
		return nil
	}
	data = data[:listLen]
	var protocols []string
	for len(data) >= 1 {
		pLen := int(data[0])
		data = data[1:]
		if len(data) < pLen {
			break
		}
		protocols = append(protocols, string(data[:pLen]))
		data = data[pLen:]
	}
	return protocols
}
