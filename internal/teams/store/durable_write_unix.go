//go:build !windows

package store

import "os"

func defaultDurableReplaceFile(src string, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	return syncParentDir(dst)
}

func finalizeAtomicWriteFile(path string, perm os.FileMode) error {
	return os.Chmod(path, perm)
}
