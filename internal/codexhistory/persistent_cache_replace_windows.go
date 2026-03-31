//go:build windows

package codexhistory

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func replacePersistentCacheFilePlatform(src, dst string) error {
	from, err := windows.UTF16PtrFromString(src)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(dst)
	if err != nil {
		return err
	}
	if err := windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH); err != nil {
		return fmt.Errorf("replace file: %w", err)
	}
	return nil
}
