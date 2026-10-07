//go:build windows

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"golang.org/x/sys/windows"
)

func windowsInstallTokenIdentity(token windows.Token) (windowsInstallIdentity, error) {
	var identity windowsInstallIdentity
	user, err := token.GetTokenUser()
	if err != nil {
		return identity, err
	}
	identity.SID = user.User.Sid.String()
	var returned uint32
	if err := windows.GetTokenInformation(token, windows.TokenSessionId, (*byte)(unsafe.Pointer(&identity.Session)), 4, &returned); err != nil {
		return identity, err
	}
	if returned != 4 {
		return identity, fmt.Errorf("unexpected Windows session token length: %d", returned)
	}
	var elevated uint32
	if err := windows.GetTokenInformation(token, windows.TokenElevation, (*byte)(unsafe.Pointer(&elevated)), 4, &returned); err != nil {
		return identity, err
	}
	if returned != 4 {
		return identity, fmt.Errorf("unexpected Windows elevation token length: %d", returned)
	}
	identity.Elevated = elevated != 0
	identity.Profile, err = token.GetUserProfileDirectory()
	if err != nil {
		return identity, err
	}
	identity.Cache, err = token.KnownFolderPath(windows.FOLDERID_LocalAppData, 0)
	return identity, err
}

func currentWindowsInstallIdentity() (windowsInstallIdentity, error) {
	token, err := windows.OpenCurrentProcessToken()
	if err != nil {
		return windowsInstallIdentity{}, err
	}
	defer token.Close()
	identity, err := windowsInstallTokenIdentity(token)
	if err != nil {
		return identity, err
	}
	if !strings.EqualFold(filepath.Clean(os.Getenv("USERPROFILE")), filepath.Clean(identity.Profile)) ||
		!strings.EqualFold(filepath.Clean(os.Getenv("LOCALAPPDATA")), filepath.Clean(identity.Cache)) {
		return identity, fmt.Errorf("Windows profile environment differs from the actual token profile")
	}
	return identity, nil
}

func normalWindowsInstallParent() (windows.Handle, windowsInstallIdentity, error) {
	identity, err := currentWindowsInstallIdentity()
	if err != nil {
		return 0, identity, err
	}
	if !identity.Elevated {
		return 0, identity, fmt.Errorf("delegation requires an elevated parent token")
	}
	var shellPID uint32
	if _, err := windows.GetWindowThreadProcessId(windows.GetShellWindow(), &shellPID); err != nil || shellPID == 0 {
		return 0, identity, fmt.Errorf("no accessible normal desktop context in this Windows session: %v", err)
	}
	parent, err := windows.OpenProcess(windows.PROCESS_CREATE_PROCESS|windows.PROCESS_DUP_HANDLE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, shellPID)
	if err != nil {
		return 0, identity, err
	}
	var token windows.Token
	err = windows.OpenProcessToken(parent, windows.TOKEN_QUERY, &token)
	if err != nil {
		windows.CloseHandle(parent)
		return 0, identity, err
	}
	defer token.Close()
	actual, err := windowsInstallTokenIdentity(token)
	if err == nil {
		err = validateWindowsInstallIdentity(identity, actual)
	}
	if err != nil {
		windows.CloseHandle(parent)
		return 0, identity, err
	}
	return parent, identity, nil
}

func checkWindowsInstallDelegation() error {
	parent, _, err := normalWindowsInstallParent()
	if parent != 0 {
		windows.CloseHandle(parent)
	}
	return err
}

func windowsInstallWorkerCommand(ctx context.Context, executable string, parent windows.Handle, profile string) *exec.Cmd {
	command := exec.CommandContext(ctx, executable, windowsInstallWorkerArgument)
	command.Dir = profile
	command.Env = os.Environ()
	command.SysProcAttr = &syscall.SysProcAttr{ParentProcess: syscall.Handle(parent)}
	configureTargetProcessGroup(command)
	command.Cancel = func() error { return terminateTargetCommand(command, time.Second) }
	command.WaitDelay = 5 * time.Second
	return command
}

func delegateWindowsInstall(ctx context.Context, root string, opts codexDesktopAppOptions, refresh bool) (codexWindowsManagedInstallState, bool, error) {
	parent, identity, err := normalWindowsInstallParent()
	if err != nil {
		return codexWindowsManagedInstallState{}, false, err
	}
	defer windows.CloseHandle(parent)
	executable, err := helperpath.RawExecutable()
	if err != nil {
		return codexWindowsManagedInstallState{}, false, err
	}
	request, err := json.Marshal(windowsInstallRequest{Identity: identity, Root: root, ProxyURL: opts.ProxyURL, Refresh: refresh})
	if err != nil {
		return codexWindowsManagedInstallState{}, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	command := windowsInstallWorkerCommand(ctx, executable, parent, identity.Profile)
	command.Stdin = bytes.NewReader(request)
	command.Stderr = opts.Log
	var output bytes.Buffer
	command.Stdout = &output
	if err := command.Run(); err != nil {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("same-user normal-token installation failed; no elevated retry was attempted. Rerun CXP without Administrator privileges: %w", err)
	}
	var result windowsInstallResult
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("verify installation worker result: %w", err)
	}
	state, valid, err := readValidCodexWindowsManagedState(root)
	if err != nil || !valid || state != result.State {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("installation worker did not publish the expected verified state: %v", err)
	}
	return state, result.Changed, nil
}
