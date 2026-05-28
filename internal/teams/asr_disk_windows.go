//go:build windows

package teams

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

var managedASRDiskFreeBytes = platformManagedASRDiskFreeBytes

func platformManagedASRDiskFreeBytes(path string) (uint64, error) {
	existing, err := existingManagedASRDiskPath(path)
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

func existingManagedASRDiskPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("empty path")
	}
	if !filepath.IsAbs(path) {
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
	}
	path = filepath.Clean(path)
	for {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		} else if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return "", fmt.Errorf("no existing ancestor for %s", path)
		}
		path = parent
	}
}
