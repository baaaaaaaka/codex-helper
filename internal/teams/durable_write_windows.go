//go:build windows

package teams

import (
	"errors"
	"syscall"
	"unsafe"
)

const (
	movefileReplaceExisting = 0x1
	movefileWriteThrough    = 0x8
)

var kernel32ProcMoveFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

func defaultDurableReplaceFile(src string, dst string) error {
	return replaceFileWithRetry(src, dst, moveFileExReplace, windowsReplaceRetryable)
}

func moveFileExReplace(src string, dst string) error {
	srcPtr, err := syscall.UTF16PtrFromString(src)
	if err != nil {
		return err
	}
	dstPtr, err := syscall.UTF16PtrFromString(dst)
	if err != nil {
		return err
	}
	ok, _, callErr := kernel32ProcMoveFileEx.Call(
		uintptr(unsafe.Pointer(srcPtr)),
		uintptr(unsafe.Pointer(dstPtr)),
		uintptr(movefileReplaceExisting|movefileWriteThrough),
	)
	if ok != 0 {
		return nil
	}
	if callErr != syscall.Errno(0) {
		return callErr
	}
	return syscall.EINVAL
}

func windowsReplaceRetryable(err error) bool {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return false
	}
	switch errno {
	case syscall.Errno(5), syscall.Errno(32), syscall.Errno(33):
		return true
	default:
		return false
	}
}
