package cloudgate

import "net"

// prefixConn replays pre-read bytes before delegating to the underlying Conn.
type prefixConn struct {
	prefix []byte
	offset int
	net.Conn
}

func newPrefixConn(prefix []byte, conn net.Conn) *prefixConn {
	return &prefixConn{prefix: prefix, Conn: conn}
}

func (c *prefixConn) Read(p []byte) (int, error) {
	if c.offset < len(c.prefix) {
		n := copy(p, c.prefix[c.offset:])
		c.offset += n
		return n, nil
	}
	return c.Conn.Read(p)
}
