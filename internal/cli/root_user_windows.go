//go:build windows

package cli

func cliRunningAsRoot() bool {
	return false
}
