//go:build windows

package codexhistory

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func populatePlatformPersistentCacheOwnerID(path string, _ os.FileInfo) (string, bool) {
	sd, err := windows.GetNamedSecurityInfo(filepath.Clean(path), windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil || sd == nil {
		return "", false
	}

	owner, _, err := sd.Owner()
	if err != nil || owner == nil {
		return "", false
	}

	ownerID := owner.String()
	if ownerID == "" {
		return "", false
	}
	return "sid:" + ownerID, true
}
