//go:build !windows

package cli

import "os/exec"

func configureTeamsServiceDetachedCommand(cmd *exec.Cmd) {
	configureTeamsLocalSupervisorDetachedCommand(cmd)
}
