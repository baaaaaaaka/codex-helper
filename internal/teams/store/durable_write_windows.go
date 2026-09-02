//go:build windows

package store

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

const (
	movefileReplaceExisting = 0x1
	movefileWriteThrough    = 0x8
)

var (
	kernel32ProcMoveFileEx  = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")
	kernel32ProcReplaceFile = syscall.NewLazyDLL("kernel32.dll").NewProc("ReplaceFileW")
)

func defaultDurableReplaceFile(src string, dst string) error {
	return replaceFileWithRetry(src, dst, windowsDurableReplace, windowsReplaceRetryable)
}

// The temporary file receives the requested mode before MoveFileEx replaces
// the target.  Do not reopen the new target for a second Chmod here: a
// concurrent liveness reader may still hold the old target handle while
// Windows is completing the replace, and the extra open can reintroduce the
// sharing race that the explicit runtime reader avoids.
func finalizeAtomicWriteFile(_ string, _ os.FileMode) error {
	return nil
}

// MoveFileEx is the fast path for both a new target and the usual replacement.
// On Windows, however, it can return ERROR_ACCESS_DENIED or a sharing error
// when a reader has the existing target open even with FILE_SHARE_DELETE.
// ReplaceFileW has the required sharing semantics for that case. Keep the
// fallback inside the same bounded retry loop; if the target disappeared, its
// FILE_NOT_FOUND result must not hide the original MoveFileEx error.
func windowsDurableReplace(src string, dst string) error {
	err := moveFileExReplace(src, dst)
	if err == nil || !windowsReplaceRetryable(err) {
		return err
	}
	replacedErr := replaceFileWReplace(src, dst)
	if replacedErr == nil {
		return nil
	}
	var errno syscall.Errno
	if errors.As(replacedErr, &errno) && errno == syscall.Errno(2) {
		return err
	}
	return replacedErr
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

func replaceFileWReplace(src string, dst string) error {
	srcPtr, err := syscall.UTF16PtrFromString(src)
	if err != nil {
		return err
	}
	dstPtr, err := syscall.UTF16PtrFromString(dst)
	if err != nil {
		return err
	}
	ok, _, callErr := kernel32ProcReplaceFile.Call(
		uintptr(unsafe.Pointer(dstPtr)),
		uintptr(unsafe.Pointer(srcPtr)),
		0,
		0,
		0,
		0,
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
