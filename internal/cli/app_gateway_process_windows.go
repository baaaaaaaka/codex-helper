//go:build windows

package cli

import (
	"context"
	"os/exec"
)

func discoverDesktopProxyPorts(ctx context.Context) ([]int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// WMI exposes the command line without requiring the desktop process to
	// cooperate with CXP. Filter executable names in PowerShell, then parse the
	// same --proxy-server form used by the Unix implementation.
	script := `$names=@('ChatGPT.exe','Codex.exe'); Get-CimInstance Win32_Process -ErrorAction Stop | Where-Object { $names -contains $_.Name } | ForEach-Object { [pscustomobject]@{Name=$_.Name; CommandLine=[string]$_.CommandLine} } | ConvertTo-Json -Compress`
	out, err := exec.CommandContext(ctx, "powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script).Output()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	rows, err := parseWindowsDesktopProcessRows(out)
	if err != nil {
		return nil, err
	}
	return desktopProxyPortsFromRows(rows), nil
}
