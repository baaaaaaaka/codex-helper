//go:build windows

package helperruntime

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func replaceActiveFile(from string, to string) error {
	fromPtr, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return err
	}
	toPtr, err := windows.UTF16PtrFromString(to)
	if err != nil {
		return err
	}
	if err := windows.MoveFileEx(fromPtr, toPtr, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH); err != nil {
		return fmt.Errorf("replace active runtime pointer: %w", err)
	}
	return nil
}
