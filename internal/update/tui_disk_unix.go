//go:build !windows

package update

import "golang.org/x/sys/unix"

func platformTUIDiskFreeBytes(path string) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}
	if stat.Bsize <= 0 || stat.Bavail <= 0 {
		return 0, nil
	}
	size, blocks := uint64(stat.Bsize), uint64(stat.Bavail)
	if blocks > ^uint64(0)/size {
		return ^uint64(0), nil
	}
	return size * blocks, nil
}
