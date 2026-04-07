//go:build !windows

package cli

func cliSharedTempRoot() string {
	return "/tmp"
}
