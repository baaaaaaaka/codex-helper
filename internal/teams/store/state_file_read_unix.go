//go:build !windows

package store

import "os"

func openRuntimeStateFile(path string) (*os.File, error) {
	return os.Open(path)
}
