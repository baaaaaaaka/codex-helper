//go:build windows

package helperruntime

import (
	"errors"
	"os"
	"os/exec"
)

func launchRuntime(target string, args []string, env []string) (int, bool, error) {
	cmd := exec.Command(target, args[1:]...)
	cmd.Args[0] = target
	cmd.Env = env
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err == nil {
		return 0, true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), true, nil
	}
	return 0, false, err
}
