//go:build !windows

package userpath

import "os"

func currentProcessIDs() (uint32, uint32, uint32) {
	return uint32(os.Getuid()), uint32(os.Geteuid()), uint32(os.Getgid())
}

func currentProcessGroups() []uint32 {
	groups, err := os.Getgroups()
	if err != nil {
		return nil
	}
	out := make([]uint32, 0, len(groups))
	for _, group := range groups {
		if group >= 0 {
			out = append(out, uint32(group))
		}
	}
	return out
}
