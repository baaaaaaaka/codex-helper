//go:build !windows

package userpath

import (
	"fmt"
	"net"
	"strings"
	"time"
)

// WriteProbeSocket connects back to the private listener created by the
// service-side resolver. Unlike an inherited descriptor, a socket path
// survives bash, PowerShell, Nushell, and other launchers that close unknown
// file descriptors before starting external commands.
func WriteProbeSocket(path string, nonce string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("empty user PATH probe socket")
	}
	connection, err := net.DialTimeout("unix", path, 5*time.Second)
	if err != nil {
		return fmt.Errorf("connect user PATH probe socket: %w", err)
	}
	defer connection.Close()
	return writeCurrentProbe(connection, nonce)
}
