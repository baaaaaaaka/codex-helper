//go:build !windows

package store

import "os"

func stateFileStampRevision(_ string, _ os.FileInfo) (string, error) {
	return "", nil
}
