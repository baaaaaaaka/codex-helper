//go:build !linux && !windows

package codex

import "os/exec"

// Other platforms retain the host-scoped PID-file recovery path.
func configureSharedLifetime(*exec.Cmd)              {}
func attachSharedLifetime(*exec.Cmd) (func(), error) { return func() {}, nil }
