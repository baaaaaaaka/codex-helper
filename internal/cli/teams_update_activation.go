package cli

import (
	"context"
	"encoding/json"
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
	teamsUpdatePendingHelperActivationOwned       = teamsPendingHelperActivationOwnerMatches
)

type teamsPendingHelperActivationOwner struct {
	Version       int       `json:"version"`
	Kind          string    `json:"kind"`
	TargetVersion string    `json:"target_version"`
	CreatedAt     time.Time `json:"created_at"`
}

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
		if targetVersion == "" && !teamsUpdatePendingHelperActivationOwned(candidate.Path, version) {
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
	if err := writeTeamsPendingHelperActivationOwner(activation.PendingPath, activation.Version); err != nil {
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
	if err := writeTeamsPendingHelperActivationOwner(activation.PendingPath, activation.Version); err != nil {
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

func teamsPendingHelperActivationOwnerPath(pendingPath string) string {
	pendingPath = strings.TrimSpace(pendingPath)
	if pendingPath == "" {
		return ""
	}
	return pendingPath + ".teams-activation.json"
}

func writeTeamsPendingHelperActivationOwner(pendingPath string, version string) error {
	path := teamsPendingHelperActivationOwnerPath(pendingPath)
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	if path == "" || version == "" {
		return fmt.Errorf("pending helper activation owner requires path and version")
	}
	raw, err := json.Marshal(teamsPendingHelperActivationOwner{
		Version:       1,
		Kind:          "teams-helper-update",
		TargetVersion: version,
		CreatedAt:     time.Now(),
	})
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func teamsPendingHelperActivationOwnerMatches(pendingPath string, version string) bool {
	path := teamsPendingHelperActivationOwnerPath(pendingPath)
	if path == "" {
		return false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var owner teamsPendingHelperActivationOwner
	if err := json.Unmarshal(raw, &owner); err != nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(owner.Kind), "teams-helper-update") {
		return false
	}
	return update.VersionMatchesTarget(owner.TargetVersion, version)
}

func windowsTeamsDestVersionProbePowerShell() string {
	return "function Version-FromText([string]$text) { " +
		"foreach ($field in ($text -split '\\s+')) { " +
		"$candidate=$field.Trim(); if ($candidate.StartsWith('v')) { $candidate=$candidate.Substring(1) }; " +
		"if ($candidate -ieq 'dev') { return $candidate }; " +
		"$parts=$candidate -split '\\.',3; " +
		"if ($parts.Length -ge 2 -and $parts[0] -match '^\\d+$' -and $parts[1] -match '^\\d+$') { return $candidate } " +
		"}; return '' }; " +
		"function Test-DestVersion { try { $v=& $dest --version 2>&1; $code=$LASTEXITCODE; $text=($v -join ' '); Log ('formal version: ' + $text); " +
		"if ($null -ne $code -and $code -ne 0) { $script:lastErr='formal version probe exited with code ' + $code + ': ' + $text; return $false }; " +
		"if ([string]::IsNullOrWhiteSpace($want)) { $script:lastErr='pending helper target version is unknown'; return $false }; " +
		"$actual=Version-FromText $text; if ([string]::IsNullOrWhiteSpace($actual)) { if ([string]::IsNullOrWhiteSpace($text)) { $script:lastErr='could not parse formal entry version from empty output' } else { $script:lastErr='could not parse formal entry version from: ' + $text }; return $false }; " +
		"if ($actual -ieq $want) { return $true }; $script:lastErr='formal entry version ' + $actual + ' did not match target ' + $want; return $false " +
		"} catch { $script:lastErr='formal version probe failed: ' + $_.Exception.Message; Log $script:lastErr; return $false } }; "
}

func windowsTeamsHelperBlockerPowerShell() string {
	return "function Get-HelperBlockers { " +
		"try { @(" +
		"Get-CimInstance Win32_Process -Filter \"Name = 'codex-proxy.exe'\" -ErrorAction SilentlyContinue | " +
		"Where-Object { try { $_.ExecutablePath -and ([System.IO.Path]::GetFullPath($_.ExecutablePath) -ieq $destFull) } catch { $false } }" +
		") } catch { Log ('process scan failed: ' + $_.Exception.Message); @() } }; " +
		"function Format-HelperBlockers($procs) { " +
		"if ($null -eq $procs -or @($procs).Count -eq 0) { return 'none' }; " +
		"return ((@($procs) | ForEach-Object { $cmd=[string]$_.CommandLine; if ($cmd.Length -gt 180) { $cmd=$cmd.Substring(0,180) + '...' }; ([string]$_.ProcessId) + ':' + $cmd }) -join '; ') }; " +
		"function Test-RetirableTeamsHelperProcess($proc) { " +
		"try { $cmd=[string]$proc.CommandLine; return ($cmd -match '(?i)(^|\\s)teams\\s+(run|listen)(\\s|$)' -or $cmd -match '(?i)(^|\\s)teams\\s+service\\s+watchdog(\\s|$)') } catch { return $false } }; " +
		"function Stop-RetirableTeamsHelperBlockers($procs) { " +
		"$stopped=0; foreach ($proc in @($procs)) { " +
		"if (-not (Test-RetirableTeamsHelperProcess $proc)) { continue }; " +
		"try { Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop; $stopped++; Log ('stopped old Teams helper process pid=' + $proc.ProcessId) } " +
		"catch { Log ('stop old Teams helper process failed pid=' + $proc.ProcessId + ': ' + $_.Exception.Message) } }; " +
		"return $stopped }; "
}

func windowsTeamsWaitForFormalHelperUnlockPowerShell() string {
	return "$blockerSummary=''; " +
		"for ($j=0; $j -lt 240; $j++) { " +
		"$procs=@(Get-HelperBlockers); " +
		"if ($procs.Count -eq 0) { $blockerSummary=''; break }; " +
		"$blockerSummary=Format-HelperBlockers $procs; " +
		"if (($j -eq 20) -or ($j -eq 60) -or ($j -eq 120)) { $stopped=Stop-RetirableTeamsHelperBlockers $procs; if ($stopped -gt 0) { Start-Sleep -Milliseconds 500; continue } }; " +
		"if (($j % 10) -eq 0) { Log ('waiting for process(es): ' + $blockerSummary) }; " +
		"Start-Sleep -Milliseconds 500 }; " +
		"$procs=@(Get-HelperBlockers); " +
		"if ($procs.Count -gt 0) { $blockerSummary=Format-HelperBlockers $procs; $lastErr='formal helper is still locked by process(es): ' + $blockerSummary; Log $lastErr } else { $blockerSummary='' }; "
}

func windowsTeamsMovePendingHelperPowerShell() string {
	return "try { " +
		"Move-Item -Force -LiteralPath $src -Destination $dest -ErrorAction Stop; " +
		"if (Test-Path -LiteralPath $src) { throw 'pending helper still exists after Move-Item' }; " +
		"Log ('moved pending helper to formal entry'); " +
		"if (Test-DestVersion) { $ready=$true } else { $lastErr=$script:lastErr }; " +
		"break } " +
		"catch { $lastErr='move attempt ' + ($i + 1) + ' failed: ' + $_.Exception.Message; if (-not [string]::IsNullOrWhiteSpace($blockerSummary)) { $lastErr=$lastErr + '; formal helper locked by process(es): ' + $blockerSummary }; Log $lastErr; Start-Sleep -Milliseconds 500 } }; "
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
		"function Write-Status([string]$s,[string]$m) { try { $tmp=$statusPath + '.tmp'; [pscustomobject]@{version=1;status=$s;message=$m;source=$src;dest=$dest;want=$want;updated_at=(Get-Date).ToString('o')} | ConvertTo-Json -Compress | Set-Content -LiteralPath $tmp -Encoding UTF8 -ErrorAction Stop; Move-Item -Force -LiteralPath $tmp -Destination $statusPath -ErrorAction Stop } catch { Log ('status write failed: ' + $_.Exception.Message) } }; " +
		"Log ('activation starting src=' + $src + ' dest=' + $dest + ' want=' + $want + ' parent=' + $parent); " +
		"Write-Status 'running' 'activation started'; " +
		"try { Wait-Process -Id $parent -Timeout 120 -ErrorAction SilentlyContinue } catch { Log ('parent wait failed: ' + $_.Exception.Message) }; " +
		"foreach ($task in $tasks) { try { Stop-ScheduledTask -TaskName $task -ErrorAction SilentlyContinue } catch { Log ('stop task failed ' + $task + ': ' + $_.Exception.Message) } }; " +
		"$destFull=[System.IO.Path]::GetFullPath($dest); " +
		"$lastErr=''; " +
		windowsTeamsHelperBlockerPowerShell() +
		windowsTeamsWaitForFormalHelperUnlockPowerShell() +
		"$ready=$false; " +
		windowsTeamsDestVersionProbePowerShell() +
		"for ($i=0; $i -lt 240; $i++) { " +
		"if (-not (Test-Path -LiteralPath $src)) { Log ('source missing: ' + $src); if (Test-DestVersion) { $ready=$true; Log 'formal entry already matches pending target' }; break }; " +
		windowsTeamsMovePendingHelperPowerShell() +
		"if ($ready) { Write-Status 'success' 'activated pending helper' } else { if ([string]::IsNullOrWhiteSpace($lastErr)) { $lastErr='activation failed before service start' }; Log ('activation failed before service start: ' + $lastErr); Write-Status 'failed' $lastErr }; " +
		teamsServiceStartScheduledTaskIfStoppedFunctionPowerShell() +
		"foreach ($task in @(" + powershellSingleQuote(teamsServiceWindowsTaskName) + "," + powershellSingleQuote(teamsServiceWindowsWatchdogTaskName) + ")) { try { Enable-ScheduledTask -TaskName $task -ErrorAction SilentlyContinue | Out-Null; Start-CodexHelperScheduledTaskIfStopped $task } catch { Log ('start task failed ' + $task + ': ' + $_.Exception.Message) } }"
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
		"$stdoutLog=Join-Path $logDir 'teams-helper-process-restart.stdout.log'; " +
		"$stderrLog=Join-Path $logDir 'teams-helper-process-restart.stderr.log'; " +
		"function Log([string]$m) { try { Add-Content -LiteralPath $log -Value ((Get-Date).ToString('o') + ' ' + $m) } catch {} }; " +
		"function Write-Status([string]$s,[string]$m) { try { $tmp=$statusPath + '.tmp'; [pscustomobject]@{version=1;status=$s;message=$m;source=$src;dest=$dest;want=$want;updated_at=(Get-Date).ToString('o')} | ConvertTo-Json -Compress | Set-Content -LiteralPath $tmp -Encoding UTF8 -ErrorAction Stop; Move-Item -Force -LiteralPath $tmp -Destination $statusPath -ErrorAction Stop } catch { Log ('status write failed: ' + $_.Exception.Message) } }; " +
		"Log ('process restart starting src=' + $src + ' dest=' + $dest + ' want=' + $want + ' parent=' + $parent); " +
		"Write-Status 'running' 'activation started'; " +
		"try { Wait-Process -Id $parent -Timeout 120 -ErrorAction SilentlyContinue } catch { Log ('parent wait failed: ' + $_.Exception.Message) }; " +
		"$destFull=[System.IO.Path]::GetFullPath($dest); " +
		"$lastErr=''; " +
		windowsTeamsHelperBlockerPowerShell() +
		windowsTeamsWaitForFormalHelperUnlockPowerShell() +
		"$ready=$false; " +
		windowsTeamsDestVersionProbePowerShell() +
		"for ($i=0; $i -lt 240; $i++) { " +
		"if (-not (Test-Path -LiteralPath $src)) { Log ('source missing: ' + $src); if (Test-DestVersion) { $ready=$true; Log 'formal entry already matches pending target' }; break }; " +
		windowsTeamsMovePendingHelperPowerShell() +
		"if ($ready) { $null = Test-DestVersion; Write-Status 'success' 'activated pending helper' } else { if ([string]::IsNullOrWhiteSpace($lastErr)) { $lastErr='process restart failed before start' }; Log $lastErr; Write-Status 'failed' $lastErr }; " +
		"if (Test-Path -LiteralPath $dest) { try { Remove-Item -LiteralPath $stdoutLog,$stderrLog -Force -ErrorAction SilentlyContinue; Start-Process -FilePath $dest -ArgumentList $argList -WindowStyle Hidden -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog | Out-Null; Log ('started helper process stdout=' + $stdoutLog + ' stderr=' + $stderrLog) } catch { Log ('start process failed: ' + $_.Exception.Message) } }"
}

func currentProcessID() int {
	return os.Getpid()
}
