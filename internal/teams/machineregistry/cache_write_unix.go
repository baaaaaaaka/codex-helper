//go:build !windows

package machineregistry

import "os"

func defaultCacheReplaceFile(src string, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	_ = syncCacheParentDir(dst)
	return nil
}
