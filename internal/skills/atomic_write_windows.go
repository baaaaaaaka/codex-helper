//go:build windows

package skills

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func replaceSkillFile(tmp, path string) error {
	from, err := windows.UTF16PtrFromString(tmp)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	if err := windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH); err != nil {
		return fmt.Errorf("replace file: %w", err)
	}
	return nil
}
