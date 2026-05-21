//go:build windows

package update

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

func replaceBinary(tmpPath, destPath string, opts replaceOptions) (replaceResult, error) {
	if err := os.Rename(tmpPath, destPath); err == nil {
		return replaceResult{restartRequired: false}, nil
	}
	if opts.returnPendingOnly {
		return replaceResult{restartRequired: true, pendingReplacePath: tmpPath}, nil
	}
	if err := scheduleWindowsMove(tmpPath, destPath); err != nil {
		return replaceResult{}, err
	}
	return replaceResult{restartRequired: true, pendingReplacePath: tmpPath}, nil
}

func scheduleWindowsMove(src, dest string) error {
	script := windowsDeferredMoveScript(src, dest, os.Getpid())
	cmd := exec.Command(windowsPowerShellExecutable(), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-WindowStyle", "Hidden", "-Command", script)
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("schedule update: %w", err)
	}
	return nil
}

func windowsPowerShellExecutable() string {
	if root := strings.TrimSpace(os.Getenv("SystemRoot")); root != "" {
		path := filepath.Join(root, "System32", "WindowsPowerShell", "v1.0", "powershell.exe")
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path
		}
	}
	return "powershell.exe"
}

func windowsDeferredMoveScript(src, dest string, parentPID int) string {
	logPath := dest + ".update.log"
	return fmt.Sprintf(
		"$ErrorActionPreference='Continue'; "+
			"$src='%s'; $dest='%s'; $log='%s'; $parent=%s; "+
			"function Write-UpdateLog([string]$m) { "+
			"$line=((Get-Date).ToString('o') + ' ' + $m); "+
			"foreach ($p in @($log, (Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'codex-helper\\updates\\codex-proxy-update.log'))) { "+
			"try { $d=Split-Path -Parent $p; if (-not [string]::IsNullOrWhiteSpace($d)) { New-Item -ItemType Directory -Force -Path $d | Out-Null }; Add-Content -LiteralPath $p -Value $line } catch {} "+
			"} }; "+
			"Write-UpdateLog ('starting pending replacement src=' + $src + ' dest=' + $dest + ' parent=' + $parent); "+
			"try { Wait-Process -Id $parent -Timeout 120 -ErrorAction SilentlyContinue } catch { Write-UpdateLog ('parent wait failed: ' + $_.Exception.Message) }; "+
			"$destFull=[System.IO.Path]::GetFullPath($dest); "+
			"for ($j = 0; $j -lt 240; $j++) { "+
			"$procs=@(); try { $procs=@(Get-CimInstance Win32_Process -Filter \"Name = 'codex-proxy.exe'\" -ErrorAction SilentlyContinue | Where-Object { try { $_.ExecutablePath -and ([System.IO.Path]::GetFullPath($_.ExecutablePath) -ieq $destFull) } catch { $false } }) } catch { Write-UpdateLog ('process scan failed: ' + $_.Exception.Message) }; "+
			"if ($procs.Count -eq 0) { break }; "+
			"if (($j %% 10) -eq 0) { Write-UpdateLog ('waiting for old process(es): ' + (($procs | ForEach-Object { $_.ProcessId }) -join ',')) }; "+
			"Start-Sleep -Milliseconds 500 "+
			"}; "+
			"for ($i = 0; $i -lt 240; $i++) { "+
			"if (-not (Test-Path -LiteralPath $src)) { Write-UpdateLog ('source missing: ' + $src); exit 2 }; "+
			"try { Move-Item -Force -LiteralPath $src -Destination $dest -ErrorAction Stop; if (Test-Path -LiteralPath $src) { throw 'pending helper still exists after Move-Item' }; Write-UpdateLog ('moved ' + $src + ' -> ' + $dest); try { $v = & $dest --version 2>&1; Write-UpdateLog ('installed version: ' + ($v -join ' ')) } catch { Write-UpdateLog ('installed version probe failed: ' + $_.Exception.Message) }; exit 0 } "+
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
