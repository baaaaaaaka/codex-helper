//go:build !windows

package sqlitestate

import "os"

func openSharedReadFile(path string) (*os.File, error) { return os.Open(path) }
