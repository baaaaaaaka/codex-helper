package codexhistory

import (
	"os"

	"golang.org/x/sys/unix"
)

var cacheStatfs = unix.Statfs

func populatePlatformFileCacheKey(path string, _ os.FileInfo, key *fileCacheKey) {
	if key == nil || !key.HasFileID {
		return
	}
	var fs unix.Statfs_t
	// ext4 derives f_fsid from its on-disk UUID. Do not assume that other
	// filesystems expose an ID with the same persistence contract.
	if err := cacheStatfs(path, &fs); err != nil || fs.Type != unix.EXT4_SUPER_MAGIC {
		return
	}
	id := uint64(uint32(fs.Fsid.Val[0]))<<32 | uint64(uint32(fs.Fsid.Val[1]))
	if id == 0 {
		return
	}
	key.DevKind, key.Dev = "ext4-fsid", id
}
