//go:build linux

package cli

import (
	"context"
)

func discoverDesktopProxyPorts(ctx context.Context) ([]int, error) {
	return discoverDesktopProxyPortsFromProcRoot(ctx, "/proc")
}
