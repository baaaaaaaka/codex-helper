//go:build !windows

package codexhistory

import "os"

func runningAsRoot() bool {
	return os.Geteuid() == 0
}
