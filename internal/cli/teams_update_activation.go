package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

type teamsPendingHelperActivation struct {
	InstallPath string
	PendingPath string
	Version     string
}

var (
	teamsUpdateFindPendingReplacementsForPlatform = update.FindPendingReplacementsForPlatform
	teamsUpdateProbeBinaryVersion                 = update.ProbeBinaryVersion
)

func discoverTeamsPendingHelperActivation(ctx context.Context, installPath string, targetVersion string) (teamsPendingHelperActivation, bool, error) {
	if teamsServiceGOOS() != "windows" {
		return teamsPendingHelperActivation{}, false, nil
	}
	installPath = strings.TrimSpace(installPath)
	if installPath == "" {
		return teamsPendingHelperActivation{}, false, nil
	}
	pending, err := teamsUpdateFindPendingReplacementsForPlatform(installPath, teamsServiceGOOS(), runtime.GOARCH)
	if err != nil {
		return teamsPendingHelperActivation{}, false, err
	}
	targetVersion = strings.TrimPrefix(strings.TrimSpace(targetVersion), "v")
	formalVersion := ""
	formalVersionComparable := false
	formalProbeFailed := false
	if formal, err := teamsUpdateProbeBinaryVersion(ctx, installPath, 5*time.Second); err == nil {
		formalVersion = strings.TrimPrefix(formal.Version, "v")
		if formalVersion != "" {
			_, formalVersionComparable = update.CompareVersions(formalVersion, formalVersion)
		}
	} else if !os.IsNotExist(err) {
		formalProbeFailed = true
	}
	for _, candidate := range pending {
		version := strings.TrimPrefix(strings.TrimSpace(candidate.Version), "v")
		if version == "" {
			continue
		}
		if targetVersion != "" && !strings.EqualFold(version, targetVersion) {
			continue
		}
		if targetVersion == "" && formalProbeFailed {
			continue
		}
		if formalVersion != "" {
			cmp, ok := update.CompareVersions(version, formalVersion)
			if ok {
				if cmp <= 0 {
					continue
				}
			} else if formalVersionComparable {
				continue
			} else if targetVersion == "" {
				continue
			} else if strings.EqualFold(formalVersion, version) {
				continue
			}
		}
		probed, err := teamsUpdateProbeBinaryVersion(ctx, candidate.Path, 5*time.Second)
		if err != nil {
			continue
		}
		probedVersion := strings.TrimPrefix(strings.TrimSpace(probed.Version), "v")
		if !strings.EqualFold(probedVersion, version) {
			continue
		}
		return teamsPendingHelperActivation{
			InstallPath: installPath,
			PendingPath: candidate.Path,
			Version:     version,
		}, true, nil
	}
	return teamsPendingHelperActivation{}, false, nil
}

func scheduleTeamsPendingHelperActivation(ctx context.Context, activation teamsPendingHelperActivation) error {
	if teamsServiceGOOS() != "windows" {
		return nil
	}
	if strings.TrimSpace(activation.PendingPath) == "" || strings.TrimSpace(activation.InstallPath) == "" {
		return fmt.Errorf("pending helper activation requires both pending and install paths")
	}
	var err error
	activation, err = normalizeTeamsPendingHelperActivation(activation)
	if err != nil {
		return err
	}
	command := windowsTeamsPendingHelperActivationPowerShell(activation.PendingPath, activation.InstallPath, activation.Version)
	return teamsServiceStartDetached(ctx, teamsServicePowerShellExecutable(), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-WindowStyle", "Hidden", "-Command", command)
}

func scheduleTeamsPendingHelperActivationForPath(ctx context.Context, pendingPath string) error {
	return scheduleTeamsPendingHelperActivationForReplacement(ctx, pendingPath, "")
}

func scheduleTeamsPendingHelperActivationForReplacement(ctx context.Context, pendingPath string, installPath string) error {
	pendingPath = strings.TrimSpace(pendingPath)
	if teamsServiceGOOS() != "windows" || pendingPath == "" {
		return nil
	}
	return scheduleTeamsPendingHelperActivation(ctx, teamsPendingHelperActivationForReplacement(pendingPath, installPath))
}

func scheduleTeamsPendingHelperProcessRestart(ctx context.Context, pendingPath string, installPath string, args []string) error {
	if teamsServiceGOOS() != "windows" {
		return nil
	}
	activation := teamsPendingHelperActivationForReplacement(pendingPath, installPath)
	if strings.TrimSpace(activation.PendingPath) == "" || strings.TrimSpace(activation.InstallPath) == "" {
		return fmt.Errorf("pending helper process restart requires both pending and install paths")
	}
	var err error
	activation, err = normalizeTeamsPendingHelperActivation(activation)
	if err != nil {
		return err
	}
	command := windowsTeamsPendingHelperProcessRestartPowerShell(activation.PendingPath, activation.InstallPath, activation.Version, args)
	return teamsServiceStartDetached(ctx, teamsServicePowerShellExecutable(), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-WindowStyle", "Hidden", "-Command", command)
}

func normalizeTeamsPendingHelperActivation(activation teamsPendingHelperActivation) (teamsPendingHelperActivation, error) {
	resolved, err := helperpath.StableInstallTarget(strings.TrimSpace(activation.InstallPath), "", "", helperpath.Options{GOOS: "windows"})
	if err != nil {
		return teamsPendingHelperActivation{}, fmt.Errorf("pending helper activation install path is not stable: %w", err)
	}
	activation.InstallPath = resolved.Path
	return activation, nil
}

func teamsPendingHelperActivationForPendingPath(pendingPath string) teamsPendingHelperActivation {
	return teamsPendingHelperActivationForReplacement(pendingPath, "")
}

func teamsPendingHelperActivationForReplacement(pendingPath string, installPath string) teamsPendingHelperActivation {
	pendingPath = strings.TrimSpace(pendingPath)
	dir := filepath.Dir(pendingPath)
	installPath = strings.TrimSpace(installPath)
	if installPath == "" {
		installPath = filepath.Join(dir, "codex-proxy.exe")
	}
	version := helperVersionFromPendingPath(pendingPath)
	if pending, err := teamsUpdateFindPendingReplacementsForPlatform(installPath, teamsServiceGOOS(), runtime.GOARCH); err == nil {
		for _, candidate := range pending {
			if filepath.Clean(candidate.Path) == filepath.Clean(pendingPath) {
				version = strings.TrimPrefix(strings.TrimSpace(candidate.Version), "v")
				break
			}
		}
	}
	return teamsPendingHelperActivation{
		InstallPath: installPath,
		PendingPath: pendingPath,
		Version:     version,
	}
}

func helperVersionFromPendingPath(pendingPath string) string {
	base := filepath.Base(strings.TrimSpace(pendingPath))
	rest := strings.TrimPrefix(base, ".codex-proxy_")
	if rest == base {
		return ""
	}
	for _, marker := range []string{"_windows_", "_linux_", "_darwin_"} {
		if idx := strings.Index(rest, marker); idx > 0 {
			return strings.TrimPrefix(strings.TrimSpace(rest[:idx]), "v")
		}
	}
	return ""
}

func windowsTeamsPendingHelperActivationPowerShell(pendingPath string, installPath string, version string) string {
	return "$ErrorActionPreference='Continue'; " +
		"$src=" + powershellSingleQuote(pendingPath) + "; " +
		"$dest=" + powershellSingleQuote(installPath) + "; " +
		"$want=" + powershellSingleQuote(strings.TrimPrefix(strings.TrimSpace(version), "v")) + "; " +
		"$statusPath=$src + '.activation.json'; " +
		"$parent=" + fmt.Sprintf("%d", currentProcessID()) + "; " +
		"$tasks=@(" + powershellSingleQuote(teamsServiceWindowsWatchdogTaskName) + "," + powershellSingleQuote(teamsServiceWindowsTaskName) + "); " +
		"$logDir=Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'codex-helper\\updates'; " +
		"New-Item -ItemType Directory -Force -Path $logDir | Out-Null; " +
		"$log=Join-Path $logDir 'teams-helper-activation.log'; " +
		"function Log([string]$m) { try { Add-Content -LiteralPath $log -Value ((Get-Date).ToString('o') + ' ' + $m) } catch {} }; " +
		"function Write-Status([string]$s,[string]$m) { try { $tmp=$statusPath + '.tmp'; [pscustomobject]@{version=1;status=$s;message=$m;source=$src;dest=$dest;want=$want;updated_at=(Get-Date).ToString('o')} | ConvertTo-Json -Compress | Set-Content -LiteralPath $tmp -Encoding UTF8; Move-Item -Force -LiteralPath $tmp -Destination $statusPath } catch { Log ('status write failed: ' + $_.Exception.Message) } }; " +
		"Log ('activation starting src=' + $src + ' dest=' + $dest + ' want=' + $want + ' parent=' + $parent); " +
		"Write-Status 'running' 'activation started'; " +
		"try { Wait-Process -Id $parent -Timeout 120 -ErrorAction SilentlyContinue } catch { Log ('parent wait failed: ' + $_.Exception.Message) }; " +
		"foreach ($task in $tasks) { try { Stop-ScheduledTask -TaskName $task -ErrorAction SilentlyContinue } catch { Log ('stop task failed ' + $task + ': ' + $_.Exception.Message) } }; " +
		"$destFull=[System.IO.Path]::GetFullPath($dest); " +
		"for ($j=0; $j -lt 240; $j++) { " +
		"$procs=@(); try { $procs=@(Get-CimInstance Win32_Process -Filter \"Name = 'codex-proxy.exe'\" -ErrorAction SilentlyContinue | Where-Object { try { $_.ExecutablePath -and ([System.IO.Path]::GetFullPath($_.ExecutablePath) -ieq $destFull) } catch { $false } }) } catch { Log ('process scan failed: ' + $_.Exception.Message) }; " +
		"if ($procs.Count -eq 0) { break }; " +
		"if (($j % 10) -eq 0) { Log ('waiting for process(es): ' + (($procs | ForEach-Object { $_.ProcessId }) -join ',')) }; " +
		"Start-Sleep -Milliseconds 500 }; " +
		"$ready=$false; " +
		"$lastErr=''; " +
		"function Test-DestVersion { try { $v=& $dest --version 2>&1; $text=($v -join ' '); Log ('formal version: ' + $text); if ([string]::IsNullOrWhiteSpace($want)) { $script:lastErr='pending helper target version is unknown'; return $false }; if ($text -like ('*' + $want + '*')) { return $true }; $script:lastErr='formal entry version did not match target: ' + $text; return $false } catch { $script:lastErr='formal version probe failed: ' + $_.Exception.Message; Log $script:lastErr; return $false } }; " +
		"for ($i=0; $i -lt 240; $i++) { " +
		"if (-not (Test-Path -LiteralPath $src)) { Log ('source missing: ' + $src); if (Test-DestVersion) { $ready=$true; Log 'formal entry already matches pending target' }; break }; " +
		"try { Move-Item -Force -LiteralPath $src -Destination $dest; Log ('moved pending helper to formal entry'); if (Test-DestVersion) { $ready=$true }; break } " +
		"catch { $lastErr='move attempt ' + ($i + 1) + ' failed: ' + $_.Exception.Message; Log $lastErr; Start-Sleep -Milliseconds 500 } }; " +
		"if ($ready) { Write-Status 'success' 'activated pending helper' } else { if ([string]::IsNullOrWhiteSpace($lastErr)) { $lastErr='activation failed before service start' }; Log ('activation failed before service start: ' + $lastErr); Write-Status 'failed' $lastErr }; " +
		"foreach ($task in @(" + powershellSingleQuote(teamsServiceWindowsTaskName) + "," + powershellSingleQuote(teamsServiceWindowsWatchdogTaskName) + ")) { try { Enable-ScheduledTask -TaskName $task -ErrorAction SilentlyContinue | Out-Null; Start-ScheduledTask -TaskName $task -ErrorAction SilentlyContinue } catch { Log ('start task failed ' + $task + ': ' + $_.Exception.Message) } }"
}

func windowsTeamsPendingHelperProcessRestartPowerShell(pendingPath string, installPath string, version string, args []string) string {
	return "$ErrorActionPreference='Continue'; " +
		"$src=" + powershellSingleQuote(pendingPath) + "; " +
		"$dest=" + powershellSingleQuote(installPath) + "; " +
		"$want=" + powershellSingleQuote(strings.TrimPrefix(strings.TrimSpace(version), "v")) + "; " +
		"$statusPath=$src + '.activation.json'; " +
		"$argList=" + powershellArrayLiteral(args) + "; " +
		"$parent=" + fmt.Sprintf("%d", currentProcessID()) + "; " +
		"$logDir=Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'codex-helper\\updates'; " +
		"New-Item -ItemType Directory -Force -Path $logDir | Out-Null; " +
		"$log=Join-Path $logDir 'teams-helper-process-restart.log'; " +
		"function Log([string]$m) { try { Add-Content -LiteralPath $log -Value ((Get-Date).ToString('o') + ' ' + $m) } catch {} }; " +
		"function Write-Status([string]$s,[string]$m) { try { $tmp=$statusPath + '.tmp'; [pscustomobject]@{version=1;status=$s;message=$m;source=$src;dest=$dest;want=$want;updated_at=(Get-Date).ToString('o')} | ConvertTo-Json -Compress | Set-Content -LiteralPath $tmp -Encoding UTF8; Move-Item -Force -LiteralPath $tmp -Destination $statusPath } catch { Log ('status write failed: ' + $_.Exception.Message) } }; " +
		"Log ('process restart starting src=' + $src + ' dest=' + $dest + ' want=' + $want + ' parent=' + $parent); " +
		"Write-Status 'running' 'activation started'; " +
		"try { Wait-Process -Id $parent -Timeout 120 -ErrorAction SilentlyContinue } catch { Log ('parent wait failed: ' + $_.Exception.Message) }; " +
		"$destFull=[System.IO.Path]::GetFullPath($dest); " +
		"for ($j=0; $j -lt 240; $j++) { " +
		"$procs=@(); try { $procs=@(Get-CimInstance Win32_Process -Filter \"Name = 'codex-proxy.exe'\" -ErrorAction SilentlyContinue | Where-Object { try { $_.ExecutablePath -and ([System.IO.Path]::GetFullPath($_.ExecutablePath) -ieq $destFull) } catch { $false } }) } catch { Log ('process scan failed: ' + $_.Exception.Message) }; " +
		"if ($procs.Count -eq 0) { break }; " +
		"if (($j % 10) -eq 0) { Log ('waiting for process(es): ' + (($procs | ForEach-Object { $_.ProcessId }) -join ',')) }; " +
		"Start-Sleep -Milliseconds 500 }; " +
		"$ready=$false; " +
		"$lastErr=''; " +
		"function Test-DestVersion { try { $v=& $dest --version 2>&1; $text=($v -join ' '); Log ('formal version: ' + $text); if ([string]::IsNullOrWhiteSpace($want)) { $script:lastErr='pending helper target version is unknown'; return $false }; if ($text -like ('*' + $want + '*')) { return $true }; $script:lastErr='formal entry version did not match target: ' + $text; return $false } catch { $script:lastErr='formal version probe failed: ' + $_.Exception.Message; Log $script:lastErr; return $false } }; " +
		"for ($i=0; $i -lt 240; $i++) { " +
		"if (-not (Test-Path -LiteralPath $src)) { Log ('source missing: ' + $src); if (Test-DestVersion) { $ready=$true; Log 'formal entry already matches pending target' }; break }; " +
		"try { Move-Item -Force -LiteralPath $src -Destination $dest; Log ('moved pending helper to formal entry'); if (Test-DestVersion) { $ready=$true }; break } " +
		"catch { $lastErr='move attempt ' + ($i + 1) + ' failed: ' + $_.Exception.Message; Log $lastErr; Start-Sleep -Milliseconds 500 } }; " +
		"if ($ready) { $null = Test-DestVersion; Write-Status 'success' 'activated pending helper' } else { if ([string]::IsNullOrWhiteSpace($lastErr)) { $lastErr='process restart failed before start' }; Log $lastErr; Write-Status 'failed' $lastErr }; " +
		"if (Test-Path -LiteralPath $dest) { try { Start-Process -FilePath $dest -ArgumentList $argList -WindowStyle Hidden | Out-Null; Log 'started helper process' } catch { Log ('start process failed: ' + $_.Exception.Message) } }"
}

func currentProcessID() int {
	return os.Getpid()
}
