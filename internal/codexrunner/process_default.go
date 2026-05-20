//go:build !windows

package codexrunner

import "os/exec"

func configureBackgroundProcess(_ *exec.Cmd) {}
