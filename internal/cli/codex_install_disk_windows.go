//go:build windows

package cli

import "golang.org/x/sys/windows"

func platformDiskFreeBytes(path string) (uint64, error) {
	existing, err := existingDiskCheckPath(path)
	if err != nil {
		return 0, err
	}
	ptr, err := windows.UTF16PtrFromString(existing)
	if err != nil {
		return 0, err
	}
	var freeBytes uint64
	if err := windows.GetDiskFreeSpaceEx(ptr, &freeBytes, nil, nil); err != nil {
		return 0, err
	}
	return freeBytes, nil
}
