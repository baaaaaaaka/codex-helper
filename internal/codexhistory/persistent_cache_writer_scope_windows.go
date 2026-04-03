//go:build windows

package codexhistory

import (
	"fmt"
	"strings"

	"golang.org/x/sys/windows"
)

func defaultPersistentCacheWriterScopeID() (string, error) {
	tokenUser, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", err
	}
	if tokenUser == nil || tokenUser.User.Sid == nil {
		return "", fmt.Errorf("missing current process SID")
	}
	scopeID := strings.ToLower(strings.TrimSpace(tokenUser.User.Sid.String()))
	if scopeID == "" {
		return "", fmt.Errorf("empty current process SID")
	}
	return "sid-" + scopeID, nil
}
