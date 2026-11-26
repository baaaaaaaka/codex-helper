package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
)

func teamsServiceWindowsRestartPowerShell(task, watchdog string) string {
	start := teamsServiceStartScheduledTaskIfStoppedFunctionPowerShell() + teamsServiceWaitScheduledTaskStoppedFunctionPowerShell() +
		teamsServiceWindowsBridgeIdentityPowerShell() + teamsServiceWindowsCaptureTaskBridgePowerShell(task)
	stop := "Stop-ScheduledTask -TaskName " + task + " -ErrorAction Stop; " +
		teamsServiceWindowsStopCapturedBridgePowerShell + teamsServiceWindowsStopIdentifiedBridgePowerShell(task) +
		"Enable-ScheduledTask -TaskName " + task + " -ErrorAction Stop | Out-Null; if (-not (Wait-CodexHelperScheduledTaskStopped " + task + " 20)) { throw 'Teams bridge Scheduled Task did not stop before restart' }; "
	bridge := "Start-CodexHelperScheduledTaskIfStopped " + task
	if watchdog == "" {
		return start + stop + bridge
	}
	return start + stop + teamsServiceWindowsResolveOptionalTaskPowerShell(watchdog) +
		"if ($null -ne $watchdogTask) { Enable-ScheduledTask -TaskName " + watchdog + " -ErrorAction Stop | Out-Null }; " + bridge +
		"; if ($null -ne $watchdogTask) { Start-CodexHelperScheduledTaskIfStopped " + watchdog + " }"
}

// The installed VBS calls its PowerShell launcher, which starts the
// stable entry. On Windows that entry waits for a versioned child. Record
// the bridge processes while that lineage still exists: stopping the Scheduled Task
// can remove the launcher or entry while leaving the bridge alive.
func teamsServiceWindowsCaptureTaskBridgePowerShell(task string) string {
	return "$bridgeRuntimeIdentity = Get-CodexHelperBridgeTaskIdentity " + task + "; " + `
function Get-CodexHelperBridgePath([string]$path) {
    if ($path) {
        try { return [IO.Path]::GetFullPath($path) } catch { }
    }
    return ''
}
$bridgeRuntimeProcesses = @()
$bridgeRuntimeEntry = [string]$bridgeRuntimeIdentity['Exe']
$bridgeRuntimeArgs = [string]$bridgeRuntimeIdentity['ScriptArgs']
$bridgeRuntimeScript = Get-CodexHelperBridgePath $bridgeRuntimeIdentity['Script']
if ($bridgeRuntimeEntry -and $bridgeRuntimeArgs -and $bridgeRuntimeScript) {
    $versions = Join-Path ([IO.Path]::GetDirectoryName($bridgeRuntimeEntry)) '.cxp-runtime\versions'
    $byPID = @{}
    $processes = @(Get-CimInstance Win32_Process -ErrorAction Stop)
    foreach ($process in $processes) { $byPID[[int]$process.ProcessId] = $process }
    foreach ($bridge in $processes) {
        if (-not (Test-CodexHelperTeamsBridgeCommand $bridge.CommandLine $bridgeRuntimeArgs)) { continue }
        $path = Get-CodexHelperBridgePath $bridge.ExecutablePath
        if (-not $path) { continue }
        $entry = $bridge
        if ($path -ine $bridgeRuntimeEntry) {
            if ([IO.Path]::GetFileName($path) -ine 'cxp.exe') { continue }
            $version = [IO.Path]::GetDirectoryName($path)
            if ([IO.Path]::GetDirectoryName($version) -ine $versions) { continue }
            $entry = $byPID[[int]$bridge.ParentProcessId]
            if ($null -eq $entry -or (Get-CodexHelperBridgePath $entry.ExecutablePath) -ine $bridgeRuntimeEntry -or $entry.CreationDate -gt $bridge.CreationDate) { continue }
            if (-not (Test-CodexHelperTeamsBridgeCommand $entry.CommandLine $bridgeRuntimeArgs)) { continue }
        }
        $launcher = $byPID[[int]$entry.ParentProcessId]
        if ($null -eq $launcher -or $launcher.Name -ine 'powershell.exe' -or $launcher.CreationDate -gt $entry.CreationDate) { continue }
        $file = [regex]::Match([string]$launcher.CommandLine, '(?i)(?:^|\s)-File\s+(?:"([^"]+)"|([^\s"]+))(?=\s|$)')
        $launcherPath = Get-CodexHelperBridgePath ($file.Groups[1].Value + $file.Groups[2].Value)
        if ($launcherPath -ine $bridgeRuntimeScript) { continue }
        $pinned = Get-Process -Id $bridge.ProcessId -ErrorAction SilentlyContinue
        if ($null -eq $pinned) { continue }
        try {
            # Pin the kernel handle before stopping the task. A reused PID
            # must never be mistaken for this task's bridge.
            [void]$pinned.Handle
            if ((Get-CodexHelperBridgePath $pinned.Path) -ieq $path -and [Math]::Abs(($pinned.StartTime.ToUniversalTime() - $bridge.CreationDate.ToUniversalTime()).TotalMilliseconds) -lt 1) {
                $bridgeRuntimeProcesses += $pinned
                $pinned = $null
            }
        } catch {
            if (-not $pinned.HasExited) { throw "Cannot verify task-owned Teams runtime: $_" }
        } finally {
            if ($null -ne $pinned) { $pinned.Dispose() }
        }
    }
}
`
}

const teamsServiceWindowsStopCapturedBridgePowerShell = `
foreach ($pinned in $bridgeRuntimeProcesses) {
    try {
        if (-not $pinned.HasExited) { $pinned.Kill() }
        if (-not $pinned.WaitForExit(5000)) { throw "Task-owned Teams runtime pid $($pinned.Id) did not exit" }
    } catch {
        if (-not $pinned.HasExited) { throw }
    } finally {
        $pinned.Dispose()
    }
}
`

func restartTeamsWindowsService(ctx context.Context, out io.Writer, backend teamsServiceWindowsTaskBackend) error {
	xmlPath, err := backend.Path()
	if err != nil {
		return err
	}
	anchor, err := teamsWindowsRestartAnchor(strings.TrimSuffix(xmlPath, filepath.Ext(xmlPath)) + ".ps1")
	if err != nil {
		return fmt.Errorf("resolve Teams Scheduled Task state: %w", err)
	}
	host, err := teamsHostname()
	if err != nil {
		return fmt.Errorf("identify Teams Scheduled Task host: %w", err)
	}
	// An inaccessible old Windows process is not assumed dead. Leave time
	// for the existing stale-lease and quiet-period fallback.
	timing := lease.Production()
	wait := timing.Stale + timing.Poll + timing.Quiet + 20*time.Second
	return restartTeamsWindowsTask(ctx, out, anchor, host, teamsBootID(), wait, 15*time.Second, backend)
}

var teamsWindowsLauncherStateEnv = regexp.MustCompile(`(?i)(?:^|;\s*)\$env:(CODEX_HELPER_STATE_DIR|XDG_STATE_HOME|CODEX_HELPER_TEAMS_CORE_STORE)\s*=\s*'((?:''|[^'])*)'`)

func teamsWindowsRestartAnchor(launcher string) (string, error) {
	data, err := os.ReadFile(launcher)
	if errors.Is(err, os.ErrNotExist) {
		state, err := appdirs.StateDir()
		if err != nil {
			return "", err
		}
		return planTeamsCoreStore(state)
	}
	if err != nil {
		return "", err
	}
	env := map[string]string{}
	for _, match := range teamsWindowsLauncherStateEnv.FindAllStringSubmatch(string(data), -1) {
		env[strings.ToUpper(match[1])] = strings.ReplaceAll(match[2], "''", "'")
	}
	state := env[appdirs.EnvStateDir]
	if state == "" {
		if base := env["XDG_STATE_HOME"]; base != "" {
			state = filepath.Join(base, appdirs.AppName)
		} else {
			base, err := os.UserConfigDir()
			if err != nil {
				return "", err
			}
			state = filepath.Join(base, appdirs.AppName, "state")
		}
	}
	return planTeamsCoreStoreWithOverride(state, env[envTeamsCoreStore])
}

// Native Windows pins the process handle across the restart. An unavailable
// handle leaves the lease as the authority. This probe does not affect lease
// acquisition or signal a process.
var teamsWindowsBridgeExitProbe = func(lease.Record) (exited func() bool, release func()) {
	return nil, nil
}

func restartTeamsWindowsTask(ctx context.Context, out io.Writer, anchor, host, boot string, handoffWait, pollWait time.Duration, backend teamsServiceBackend) error {
	old, err := lease.Read(anchor)
	if err != nil {
		return err
	}
	oldLocal := old.HeadOK && (old.Head.State == lease.Active || old.Head.State == lease.Claimed) &&
		teamsHostEqual(old.Head.Host, host) && (old.Head.Boot == "" || boot == "" || old.Head.Boot == boot)
	var oldExited func() bool
	if oldLocal {
		var release func()
		oldExited, release = teamsWindowsBridgeExitProbe(old.Head)
		if release != nil {
			defer release()
		}
	}
	requestedAt := time.Now()
	if err := runTeamsServiceCommand(ctx, out, backend, "restart"); err != nil {
		return fmt.Errorf("restart Windows Teams Scheduled Task: %w", err)
	}
	_, _ = fmt.Fprintln(out, "Windows Teams restart requested; checking bridge ownership.")
	deadline := time.Now().Add(handoffWait)
	var firstActive time.Time
	var owner lease.Record
	for {
		now := time.Now()
		view, err := teamsStoreViewAt(anchor)
		if err != nil {
			return err
		}
		info := view.Info
		if info.Disabled {
			return errors.New("Teams was stopped everywhere while waiting for the Windows bridge")
		}
		// If the old process was verified, require its exit as well as a
		// successor's lease. Otherwise, rely on the lease's own fencing.
		oldStillRunning := oldExited != nil && !oldExited()
		if !oldStillRunning && info.Live(now, lease.ProductionStale) && info.Head.State == lease.Active {
			if !teamsHostEqual(info.Head.Host, host) {
				if !oldLocal || info.HeadGen > old.HeadGen {
					_, _ = fmt.Fprintf(out, "Windows Teams task started in standby; %s (pid %d) owns the Teams state.\n", info.Head.Host, info.Head.PID)
					return nil
				}
			} else if info.HeadGen > old.HeadGen && (boot == "" || info.Head.Boot == boot) {
				if owner.Gen != info.Head.Gen {
					owner, firstActive = info.Head, now
				}
				stamp, ok := readTeamsPollStamp(filepath.Dir(anchor))
				if ok && stamp.At >= requestedAt.UnixMilli() && teamsPollFromOwner(view, stamp, now) {
					_, _ = fmt.Fprintf(out, "Restarted Teams service: Windows Scheduled Task. Bridge pid %d owns generation %d and has polled Teams.\n", owner.PID, owner.Gen)
					return nil
				}
				if now.Sub(firstActive) >= pollWait || !now.Before(deadline) {
					_, _ = fmt.Fprintf(out, "Restarted Teams service: Windows Scheduled Task. Bridge pid %d owns generation %d; a successful Teams poll is pending.\n", owner.PID, owner.Gen)
					return nil
				}
			}
		}
		if !now.Before(deadline) {
			if oldStillRunning {
				return fmt.Errorf("old Teams bridge pid %d is still running; Windows restart did not complete", old.Head.PID)
			}
			if oldLocal && info.HeadOK && info.HeadGen == old.HeadGen && info.Head.Beat > old.Head.Beat && info.Head.State != lease.Released {
				return fmt.Errorf("old Teams bridge pid %d kept renewing its lease; Windows restart did not complete", old.Head.PID)
			}
			active, err := backend.Active(ctx)
			if err != nil {
				return fmt.Errorf("check Windows Teams Scheduled Task after restart: %w", err)
			}
			if !active {
				return fmt.Errorf("Windows Teams Scheduled Task exited before a new bridge acquired %s; see the task launcher output and Last Task Result", anchor)
			}
			return fmt.Errorf("Windows Teams Scheduled Task is running but no new bridge acquired %s before the timeout", anchor)
		}
		if err := sleepContext(ctx, 500*time.Millisecond); err != nil {
			return fmt.Errorf("wait for Windows Teams bridge handoff: %w", err)
		}
	}
}
