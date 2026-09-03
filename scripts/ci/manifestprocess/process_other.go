//go:build !windows && !aix && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !linux && !netbsd && !openbsd && !solaris

package manifestprocess

import "os/exec"

// Unsupported targets still get bounded direct-process cancellation. The CI
// workflow runs on Unix and Windows, where the platform files provide full
// process-tree cleanup.
type processHandle struct{}

func configure(*exec.Cmd)            {}
func attach(*exec.Cmd) processHandle { return processHandle{} }
func terminate(cmd *exec.Cmd, _ processHandle) {
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
func closeHandle(processHandle) {}
