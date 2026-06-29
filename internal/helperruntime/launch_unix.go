//go:build !windows

package helperruntime

import "syscall"

func launchRuntime(target string, args []string, env []string) (int, bool, error) {
	if err := syscall.Exec(target, args, env); err != nil {
		return 0, false, err
	}
	return 0, true, nil
}
