$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$root = Join-Path $env:RUNNER_TEMP ("codex-helper-teams-supervisor-smoke-" + $PID)
New-Item -ItemType Directory -Force -Path $root | Out-Null
$taskName = "Codex Helper Teams CI Smoke $PID"
$heartbeat = Join-Path $root "scheduled-task-heartbeat.log"
$body = Join-Path $root "scheduled-task-body.ps1"

function Log($message) {
  Write-Host "[teams-supervisor-smoke] $message"
}

function Wait-HeartbeatLines($path, $minLines) {
  $deadline = (Get-Date).AddSeconds(45)
  while ((Get-Date) -lt $deadline) {
    if (Test-Path $path) {
      $count = (Get-Content -Path $path -ErrorAction SilentlyContinue | Measure-Object -Line).Lines
      if ($count -ge $minLines) {
        return
      }
    }
    Start-Sleep -Seconds 1
  }
  if (Test-Path $path) {
    Get-Content $path | ForEach-Object { Write-Host "[heartbeat] $_" }
  }
  throw "Timed out waiting for $path to reach $minLines lines"
}

try {
  Log "Windows Scheduled Task smoke: register a per-user task and start it manually"
  @"
`$heartbeat = '$heartbeat'
`$end = (Get-Date).AddSeconds(20)
while ((Get-Date) -lt `$end) {
  Add-Content -Path `$heartbeat -Value ("{0:o} pid={1}" -f (Get-Date), `$PID)
  Start-Sleep -Seconds 1
}
"@ | Set-Content -Path $body -Encoding UTF8

  $pwsh = (Get-Command pwsh -ErrorAction SilentlyContinue).Source
  if (-not $pwsh) {
    $pwsh = (Get-Command powershell.exe -ErrorAction Stop).Source
  }
  $action = New-ScheduledTaskAction -Execute $pwsh -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$body`""
  $trigger = New-ScheduledTaskTrigger -AtLogOn
  $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
  $userId = if ($env:USERDOMAIN) { "$env:USERDOMAIN\$env:USERNAME" } else { $env:USERNAME }
  $principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive -RunLevel Limited
  Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null
  Start-ScheduledTask -TaskName $taskName
  Wait-HeartbeatLines $heartbeat 2

  $task = Get-ScheduledTask -TaskName $taskName
  if ($task.State -eq "Disabled") {
    throw "Scheduled task was unexpectedly disabled"
  }

  if (Get-Command wsl.exe -ErrorAction SilentlyContinue) {
    Log "wsl.exe exists; checking whether a distro is available for WSL task smoke"
    $distros = @(wsl.exe -l -q 2>$null | Where-Object { $_ -and $_.Trim() })
    if ($distros.Count -gt 0) {
      wsl.exe -d $distros[0] -e sh -lc "printf wsl-ok"
    } else {
      Write-Warning "wsl.exe exists but no distro is installed on this runner; WSL launch is covered by command-generation tests"
    }
  } else {
    Write-Warning "wsl.exe is not available on this runner; WSL launch is covered by command-generation tests"
  }
} finally {
  Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue | Out-Null
  Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
  Remove-Item -Recurse -Force $root -ErrorAction SilentlyContinue
}
