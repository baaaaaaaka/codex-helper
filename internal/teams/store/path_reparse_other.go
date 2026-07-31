//go:build !windows

package store

import "os"

func sqliteStorePathIsReparsePoint(_ string, info os.FileInfo) (bool, error) {
	return info.Mode()&os.ModeSymlink != 0, nil
}
