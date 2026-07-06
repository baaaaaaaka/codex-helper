//go:build windows

package userpath

import (
	"context"
	"fmt"

	"golang.org/x/sys/windows"
)

// resolveAccountDefault uses the token of the current Teams Scheduled Task.
// CreateEnvironmentBlock with inheritExisting=false is the Windows account
// default; it intentionally does not use the active console/RDP user.
func resolveAccountDefault(_ context.Context, req Request) (Result, error) {
	token, err := windows.OpenCurrentProcessToken()
	if err != nil {
		return Result{}, fmt.Errorf("open Teams service process token: %w", err)
	}
	defer token.Close()
	tokenUser, err := token.GetTokenUser()
	if err != nil {
		return Result{}, fmt.Errorf("read Teams service token user: %w", err)
	}
	sid := tokenUser.User.Sid.String()
	environment, err := token.Environ(false)
	if err != nil {
		return Result{}, fmt.Errorf("create account environment from Teams service token: %w", err)
	}
	pathValue, home, err := validateWindowsAccountEnvironment(environment, sid, req.Target.SID)
	if err != nil {
		return Result{}, err
	}
	target := req.Target
	target.SID = sid
	if target.Home == "" {
		target.Home = home
	}
	return Result{
		Path:           pathValue,
		Source:         "windows-token-environment",
		Mode:           ModeAccountDefault,
		Target:         target,
		Adapter:        "CreateEnvironmentBlock",
		BaselineSource: "task-user-token",
	}, nil
}
