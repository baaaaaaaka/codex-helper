//go:build !windows && !linux && !darwin

package cli

import (
	"context"
	"errors"
)

func discoverDesktopProxyPorts(ctx context.Context) ([]int, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return nil, errors.New("desktop process port discovery is unsupported on this platform")
}
