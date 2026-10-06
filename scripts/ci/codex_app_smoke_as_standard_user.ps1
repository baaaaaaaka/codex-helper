param(
  [Parameter(Mandatory = $true)]
  [string]$Helper,
  [switch]$NetworkInstall,
  [switch]$ManagedInstall,
  [string]$RecordingProxy = "",
  [string]$FakeChatGPT = "",
  [switch]$Child,
  [string]$SettingsPath = ""
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

if ($Child) {
  $settings = $null
  $exitCode = 1
  $errorMessage = ""
  try {
    $settings = Get-Content -Raw -LiteralPath $SettingsPath | ConvertFrom-Json
    $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    if (![string]::Equals($identity.Name, [string]$settings.ExpectedIdentity, [StringComparison]::OrdinalIgnoreCase)) {
      throw "desktop app smoke identity is $($identity.Name), expected $($settings.ExpectedIdentity)"
    }
    $principal = [System.Security.Principal.WindowsPrincipal]::new($identity)
    $administratorsSID = [System.Security.Principal.SecurityIdentifier]::new(
      [System.Security.Principal.WellKnownSidType]::BuiltinAdministratorsSid,
      $null
    )
    if ($principal.IsInRole($administratorsSID)) {
      throw "desktop app smoke is still elevated: $($identity.Name)"
    }
    foreach ($name in @([Environment]::GetEnvironmentVariables().Keys)) {
      if (
        $name -match '^(GITHUB|ACTIONS)_' -or
        $name -match '^CXP_(RUNTIME|WINDOWS|TEST)_' -or
        $name -match '^CODEX_(RUNTIME|PROXY)_' -or
        $name -match '^(CODEX_HOME|CODEX_DIR|GH_TOKEN|OPENAI_API_KEY|MIMO_API_KEY|ANTHROPIC_API_KEY|CODEX_LIVE_AUTH_JSON|CHATGPT_AUTH_TOKEN|CODEX_AUTH_TOKEN)$' -or
        $name -match '^CODEX_HELPER_TEAMS_.*TOKEN_CACHE$'
      ) {
        Remove-Item ("Env:" + $name) -ErrorAction SilentlyContinue
      }
    }
    $env:RUNNER_TEMP = [string]$settings.RunnerTemp
    $env:CXP_RUNTIME_DISABLE = "1"
    $windowsApps = Join-Path $env:LOCALAPPDATA "Microsoft\WindowsApps"
    $machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    $env:Path = "$windowsApps;$machinePath"
    Set-Location -LiteralPath ([string]$settings.WorkingDirectory)

    for ($attempt = 1; $attempt -le 2; $attempt++) {
      try {
        if ($settings.NetworkInstall) {
          $env:CXP_WINDOWS_APP_BACKEND = "legacy"
          & (Join-Path $PSScriptRoot "codex_app_network_install_smoke.ps1") -Helper ([string]$settings.Helper) *> ([string]$settings.OutputPath)
        } else {
          & (Join-Path $PSScriptRoot "codex_app_managed_install_smoke.ps1") `
            -Helper ([string]$settings.Helper) `
            -RecordingProxy ([string]$settings.RecordingProxy) `
            -FakeChatGPT ([string]$settings.FakeChatGPT) *> ([string]$settings.OutputPath)
        }
        if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) {
          throw "desktop app smoke command failed with exit code $LASTEXITCODE"
        }
        $exitCode = 0
        break
      } catch {
        $errorMessage = $_.Exception.ToString()
        Add-Content -LiteralPath ([string]$settings.OutputPath) -Value $errorMessage -Encoding UTF8
        if ($attempt -eq 2) { break }
        Add-Content -LiteralPath ([string]$settings.OutputPath) -Value "Desktop app smoke failed on attempt $attempt; retrying in 10 seconds." -Encoding UTF8
        Start-Sleep -Seconds 10
      }
    }
  } catch {
    $errorMessage = $_.Exception.ToString()
    if ($settings -and $settings.OutputPath) {
      Add-Content -LiteralPath ([string]$settings.OutputPath) -Value $errorMessage -Encoding UTF8
    }
  } finally {
    if ($settings -and $settings.ResultPath) {
      [ordered]@{ ExitCode = $exitCode; Error = $errorMessage } |
        ConvertTo-Json -Compress |
        Set-Content -LiteralPath ([string]$settings.ResultPath) -Encoding UTF8
    }
  }
  exit $exitCode
}

if ($NetworkInstall -eq $ManagedInstall) {
  throw "select exactly one of -NetworkInstall or -ManagedInstall"
}
if (!(Test-Path -LiteralPath $Helper -PathType Leaf)) {
  throw "helper does not exist: $Helper"
}
if ($ManagedInstall -and (!(Test-Path -LiteralPath $RecordingProxy -PathType Leaf) -or !(Test-Path -LiteralPath $FakeChatGPT -PathType Leaf))) {
  throw "managed install smoke requires existing recording proxy and fake ChatGPT fixtures"
}

$runnerTemp = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [IO.Path]::GetTempPath() }
$smokeRoot = Join-Path $runnerTemp ("cxp-desktop-limited-token-smoke-" + [guid]::NewGuid().ToString("N"))
$taskName = "CXP-Codex-DesktopSmoke-" + [guid]::NewGuid().ToString("N")
$taskRegistered = $false

try {
  New-Item -ItemType Directory -Force -Path $smokeRoot | Out-Null
  $settingsPath = Join-Path $smokeRoot "settings.json"
  $outputPath = Join-Path $smokeRoot "smoke.output.log"
  $resultPath = Join-Path $smokeRoot "smoke.result.json"
  $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
  $helperPath = [IO.Path]::GetFullPath($Helper)
  $scriptPath = [IO.Path]::GetFullPath($PSCommandPath)
  $workingDirectory = [IO.Path]::GetFullPath((Get-Location).Path)
  [ordered]@{
    ExpectedIdentity = $identity.Name
    RunnerTemp = $runnerTemp
    WorkingDirectory = $workingDirectory
    Helper = $helperPath
    NetworkInstall = [bool]$NetworkInstall
    RecordingProxy = if ($ManagedInstall) { [IO.Path]::GetFullPath($RecordingProxy) } else { "" }
    FakeChatGPT = if ($ManagedInstall) { [IO.Path]::GetFullPath($FakeChatGPT) } else { "" }
    OutputPath = $outputPath
    ResultPath = $resultPath
  } | ConvertTo-Json | Set-Content -LiteralPath $settingsPath -Encoding UTF8
  Set-Content -LiteralPath $outputPath -Value "" -Encoding UTF8

  $powerShell = Join-Path $PSHOME "pwsh.exe"
  $argumentLine = "-NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$scriptPath`" -Helper `"$helperPath`" -Child -SettingsPath `"$settingsPath`""
  $action = New-ScheduledTaskAction -Execute $powerShell -Argument $argumentLine -WorkingDirectory $workingDirectory
  $principal = New-ScheduledTaskPrincipal -UserId $identity.Name -LogonType Interactive -RunLevel Limited
  $settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 15) `
    -MultipleInstances IgnoreNew `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries
  Register-ScheduledTask -TaskName $taskName -Action $action -Principal $principal -Settings $settings -Force | Out-Null
  $taskRegistered = $true
  $startedAt = Get-Date
  Start-ScheduledTask -TaskName $taskName

  $deadline = [DateTime]::UtcNow.AddMinutes(15)
  while (!(Test-Path -LiteralPath $resultPath -PathType Leaf) -and [DateTime]::UtcNow -lt $deadline) {
    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    $taskInfo = Get-ScheduledTaskInfo -TaskName $taskName -ErrorAction SilentlyContinue
    if ($task.State -eq "Ready" -and $taskInfo.LastRunTime -ge $startedAt) {
      throw "limited-token desktop app smoke task exited without a result file; task result: $($taskInfo.LastTaskResult)"
    }
    Start-Sleep -Seconds 1
  }
  if (!(Test-Path -LiteralPath $resultPath -PathType Leaf)) {
    $taskInfo = Get-ScheduledTaskInfo -TaskName $taskName -ErrorAction SilentlyContinue
    throw "limited-token desktop app smoke timed out; task result: $($taskInfo.LastTaskResult)"
  }

  $output = if (Test-Path -LiteralPath $outputPath) { Get-Content -Raw -LiteralPath $outputPath } else { "" }
  if (![string]::IsNullOrWhiteSpace($output)) { Write-Host $output }
  $result = Get-Content -Raw -LiteralPath $resultPath | ConvertFrom-Json
  if ($result.ExitCode -ne 0) {
    throw "limited-token desktop app smoke failed with exit code $($result.ExitCode): $($result.Error)"
  }
} finally {
  if ($taskRegistered) {
    Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
  }
  Remove-Item -Recurse -Force -LiteralPath $smokeRoot -ErrorAction SilentlyContinue
}
