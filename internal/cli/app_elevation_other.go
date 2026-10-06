//go:build !windows

package cli

func currentWindowsTokenElevated() (bool, error) {
	return false, nil
}
