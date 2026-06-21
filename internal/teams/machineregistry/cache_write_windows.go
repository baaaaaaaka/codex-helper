//go:build windows

package machineregistry

import (
	"errors"
	"syscall"
	"unsafe"
)

const (
	cacheMoveFileReplaceExisting = 0x1
	cacheMoveFileWriteThrough    = 0x8
)

var cacheKernel32ProcMoveFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

func defaultCacheReplaceFile(src string, dst string) error {
	return cacheReplaceFileWithRetry(src, dst, cacheMoveFileExReplace, cacheWindowsReplaceRetryable)
}

func cacheMoveFileExReplace(src string, dst string) error {
	srcPtr, err := syscall.UTF16PtrFromString(src)
	if err != nil {
		return err
	}
	dstPtr, err := syscall.UTF16PtrFromString(dst)
	if err != nil {
		return err
	}
	ok, _, callErr := cacheKernel32ProcMoveFileEx.Call(
		uintptr(unsafe.Pointer(srcPtr)),
		uintptr(unsafe.Pointer(dstPtr)),
		uintptr(cacheMoveFileReplaceExisting|cacheMoveFileWriteThrough),
	)
	if ok != 0 {
		return nil
	}
	if callErr != syscall.Errno(0) {
		return callErr
	}
	return syscall.EINVAL
}

func cacheWindowsReplaceRetryable(err error) bool {
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
