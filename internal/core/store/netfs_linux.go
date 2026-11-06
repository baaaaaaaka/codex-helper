//go:build linux

package store

import "syscall"

// networkFS reports whether dir is on a network filesystem, where another
// host may open the store's files after this one.
func networkFS(dir string) bool {
	var st syscall.Statfs_t
	if syscall.Statfs(dir, &st) != nil {
		return false
	}
	switch uint32(st.Type) {
	case 0x6969, // NFS
		0x517b,     // SMB
		0xff534d42, // CIFS
		0xfe534d42, // SMB2
		0x00c36400, // Ceph
		0x65735546, // FUSE (sshfs, …)
		0x5346414f, // AFS
		0x0bd00bd0, // Lustre
		0x47504653, // GPFS
		0x013111a8, // IBRIX
		0x01021997: // 9P
		return true
	}
	return false
}
