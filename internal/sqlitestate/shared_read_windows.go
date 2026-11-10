package sqlitestate

import (
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func openSharedReadFile(path string) (*os.File, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if len(abs) >= 248 && !strings.HasPrefix(abs, `\\?\`) && !strings.HasPrefix(abs, `\\.\`) {
		if strings.HasPrefix(abs, `\\`) {
			abs = `\\?\UNC\` + abs[2:]
		} else {
			abs = `\\?\` + abs
		}
	}
	p, err := windows.UTF16PtrFromString(abs)
	if err != nil {
		return nil, err
	}
	h, err := windows.CreateFile(p, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}
