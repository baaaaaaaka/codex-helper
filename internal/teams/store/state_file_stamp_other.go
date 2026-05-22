//go:build !windows

package store

import "os"

func stateFileStampRevision(_ string, _ os.FileInfo) (stateFileRevision, error) {
	return stateFileRevision{}, nil
}
