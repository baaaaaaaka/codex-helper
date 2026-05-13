//go:build windows

package update

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func replaceBinary(tmpPath, destPath string) (replaceResult, error) {
	if err := os.Rename(tmpPath, destPath); err == nil {
		return replaceResult{restartRequired: false}, nil
	}
	if err := scheduleWindowsMove(tmpPath, destPath); err != nil {
		return replaceResult{}, err
	}
	return replaceResult{restartRequired: true, pendingReplacePath: tmpPath}, nil
}

func scheduleWindowsMove(src, dest string) error {
	script := windowsDeferredMoveScript(src, dest, os.Getpid())
	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script)
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("schedule update: %w", err)
	}
	return nil
}

func windowsDeferredMoveScript(src, dest string, parentPID int) string {
	logPath := dest + ".update.log"
	return fmt.Sprintf(
		"$ErrorActionPreference='Continue'; "+
			"$src='%s'; $dest='%s'; $log='%s'; $parent=%s; "+
			"function Write-UpdateLog([string]$m) { try { Add-Content -LiteralPath $log -Value ((Get-Date).ToString('o') + ' ' + $m) } catch {} }; "+
			"Write-UpdateLog ('waiting for parent pid ' + $parent); "+
			"try { Wait-Process -Id $parent -Timeout 60 -ErrorAction SilentlyContinue } catch {}; "+
			"for ($i = 0; $i -lt 120; $i++) { "+
			"if (-not (Test-Path -LiteralPath $src)) { Write-UpdateLog ('source missing: ' + $src); exit 2 }; "+
			"try { Move-Item -Force -LiteralPath $src -Destination $dest; Write-UpdateLog ('moved ' + $src + ' -> ' + $dest); exit 0 } "+
			"catch { Write-UpdateLog ('move attempt ' + ($i + 1) + ' failed: ' + $_.Exception.Message); Start-Sleep -Milliseconds 500 } "+
			"}; "+
			"Write-UpdateLog ('failed to move after retries: ' + $src + ' -> ' + $dest); exit 1",
		escapePowerShellString(src),
		escapePowerShellString(dest),
		escapePowerShellString(logPath),
		strconv.Itoa(parentPID),
	)
}

func escapePowerShellString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
