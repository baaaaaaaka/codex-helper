//go:build darwin

package cli

import (
	"context"
	"os/exec"
)

// macOS does not expose Linux's /proc command-line files. `ps` is available
// on every supported macOS release and gives us a read-only migration hint;
// the port is still adopted only after the gateway health/ownership checks.
func discoverDesktopProxyPorts(ctx context.Context) ([]int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	out, err := exec.CommandContext(ctx, "ps", "-axo", "pid=,comm=,args=").Output()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	return desktopProxyPortsFromRows(parseDarwinDesktopProcessRows(string(out))), nil
}
