//go:build !windows

package cli

import "golang.org/x/sys/unix"

func platformDiskFreeBytes(path string) (uint64, error) {
	existing, err := existingDiskCheckPath(path)
	if err != nil {
		return 0, err
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(existing, &stat); err != nil {
		return 0, err
	}
	blockSize := uint64(stat.Bsize)
	availableBlocks := uint64(stat.Bavail)
	return availableBlocks * blockSize, nil
}
