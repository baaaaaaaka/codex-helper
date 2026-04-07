//go:build windows

package cli

import "os"

func cliSharedTempRoot() string {
	return os.TempDir()
}
