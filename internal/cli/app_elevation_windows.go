//go:build windows

package cli

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

func currentWindowsTokenElevated() (bool, error) {
	token, err := windows.OpenCurrentProcessToken()
	if err != nil {
		return false, fmt.Errorf("open current process token: %w", err)
	}
	defer token.Close()

	var elevation uint32
	var returned uint32
	if err := windows.GetTokenInformation(token, windows.TokenElevation, (*byte)(unsafe.Pointer(&elevation)), uint32(unsafe.Sizeof(elevation)), &returned); err != nil {
		return false, fmt.Errorf("query Windows token elevation: %w", err)
	}
	if returned != uint32(unsafe.Sizeof(elevation)) {
		return false, fmt.Errorf("query Windows token elevation: returned %d bytes, want %d", returned, unsafe.Sizeof(elevation))
	}
	return elevation != 0, nil
}
