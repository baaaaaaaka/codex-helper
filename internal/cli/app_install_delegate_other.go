//go:build !windows

package cli

import (
	"context"
	"errors"
)

func currentWindowsInstallIdentity() (windowsInstallIdentity, error) {
	return windowsInstallIdentity{}, errors.New("normal-token delegation requires native Windows")
}

func checkWindowsInstallDelegation() error {
	return errors.New("normal-token delegation requires native Windows")
}

func delegateWindowsInstall(context.Context, string, codexDesktopAppOptions, bool) (codexWindowsManagedInstallState, bool, error) {
	return codexWindowsManagedInstallState{}, false, checkWindowsInstallDelegation()
}
