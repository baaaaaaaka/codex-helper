//go:build !windows

package helperruntime

import "os"

func replaceActiveFile(from string, to string) error {
	return os.Rename(from, to)
}
