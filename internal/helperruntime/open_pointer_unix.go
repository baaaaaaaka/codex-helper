//go:build !windows

package helperruntime

import "os"

func openVersionPointerFile(path string) (*os.File, error) { return os.Open(path) }
