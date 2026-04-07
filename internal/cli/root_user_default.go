//go:build !windows

package cli

import "os"

func cliRunningAsRoot() bool {
	return os.Geteuid() == 0
}
