$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$root = Join-Path ([IO.Path]::GetTempPath()) ("cxp-process-" + [guid]::NewGuid().ToString("N"))
$process = $null
$hadHome = Test-Path Env:\HOME
$oldHome = $env:HOME
$hadStateHome = Test-Path Env:\XDG_STATE_HOME
$oldStateHome = $env:XDG_STATE_HOME
$runtimeEnvironment = @{}
foreach ($name in @("CXP_RUNTIME", "CXP_RUNTIME_ROOT", "CXP_RUNTIME_VERSION", "CXP_ENTRY_PATH", "CXP_RUNTIME_DISABLE", "CXP_RUNTIME_FORCE")) {
  $runtimeEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
  Remove-Item ("Env:" + $name) -ErrorAction SilentlyContinue
}

function Assert-FreshLauncherDispatchesActiveRuntime {
  $fixture = Join-Path $root "fresh-launch-restart"
  $entry = Join-Path $fixture "cxp.exe"
  $runtimeRoot = Join-Path $fixture ".cxp-runtime"
  $oldVersion = "0.1.13-rc.56"
  $targetVersion = "0.1.13-rc.57"
  $oldRuntime = Join-Path $runtimeRoot "versions\v$oldVersion\cxp.exe"
  $targetRuntime = Join-Path $runtimeRoot "versions\v$targetVersion\cxp.exe"

  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $oldRuntime),(Split-Path -Parent $targetRuntime) | Out-Null
  Push-Location $repoRoot
  try {
    & go build -trimpath `
      -ldflags "-X github.com/baaaaaaaka/codex-helper/internal/cli.version=$oldVersion" `
      -o $entry `
      .\cmd\codex-proxy
    Copy-Item -Force -LiteralPath $entry -Destination $oldRuntime
    & go build -trimpath `
      -ldflags "-X github.com/baaaaaaaka/codex-helper/internal/cli.version=$targetVersion" `
      -o $targetRuntime `
      .\cmd\codex-proxy
  } finally {
    Pop-Location
  }
  # Publish the native boot record with the verified target, then leave an
  # older physical entry in place, as an ordinary candidate update does.
  Copy-Item -Force -LiteralPath $targetRuntime -Destination $entry
  & $entry --cxp-install-bootstrap --target $entry
  Copy-Item -Force -LiteralPath $oldRuntime -Destination $entry
  $boot = Join-Path $runtimeRoot "boot.sqlite"
  $bootHash = (Get-FileHash -LiteralPath $boot -Algorithm SHA256).Hash

  try {
    $env:CXP_RUNTIME = "1"
    $env:CXP_RUNTIME_ROOT = $runtimeRoot
    $env:CXP_RUNTIME_VERSION = "v$oldVersion"
    $env:CXP_ENTRY_PATH = $entry
    $staleOutput = (& $entry --version | Out-String)
    $staleOutput | Write-Host
    if ($staleOutput -notmatch [regex]::Escape($oldVersion)) {
      throw "runtime-marked launcher did not stay pinned to $oldVersion`: $staleOutput"
    }
  } finally {
    foreach ($name in @("CXP_RUNTIME", "CXP_RUNTIME_ROOT", "CXP_RUNTIME_VERSION", "CXP_ENTRY_PATH", "CXP_RUNTIME_DISABLE", "CXP_RUNTIME_FORCE")) {
      Remove-Item ("Env:" + $name) -ErrorAction SilentlyContinue
    }
  }

  $freshOutput = (& $entry --version | Out-String)
  $freshOutput | Write-Host
  if ($freshOutput -notmatch [regex]::Escape($targetVersion)) {
    throw "fresh stable launcher did not dispatch active runtime $targetVersion`: $freshOutput"
  }
  if ((Get-FileHash -LiteralPath $boot -Algorithm SHA256).Hash -ne $bootHash) {
    throw "fresh stable launcher changed the native boot selection"
  }
}

try {
  New-Item -ItemType Directory -Force -Path $root | Out-Null
  Push-Location $repoRoot
  try {
    & go build -o (Join-Path $root "cxp.exe") .\cmd\codex-proxy
  } finally {
    Pop-Location
  }

  Assert-FreshLauncherDispatchesActiveRuntime

  $stdout = Join-Path $root "stdout.log"
  $stderr = Join-Path $root "stderr.log"
  $env:HOME = Join-Path $root "home"
  $env:XDG_STATE_HOME = Join-Path $root "state"
  # A long-lived CXP process with no network or account: `cxp run` in
  # direct mode (stdin is not a terminal) waiting on a loopback ping.
  $stdin = Join-Path $root "stdin.txt"
  Set-Content -LiteralPath $stdin -NoNewline -Value ""
  $process = Start-Process -FilePath (Join-Path $root "cxp.exe") `
    -ArgumentList @("run", "--", "ping.exe", "-n", "600", "127.0.0.1") `
    -RedirectStandardInput $stdin -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru

  $boot = Join-Path $root ".cxp-runtime\boot.sqlite"
  $ready = $false
  for ($attempt = 0; $attempt -lt 300; $attempt++) {
    if ($process.HasExited) {
      throw "CXP process exited before runtime activation: $(Get-Content -Raw -LiteralPath $stderr -ErrorAction SilentlyContinue)"
    }
    if (Test-Path -LiteralPath $boot) {
      $ready = $true
      break
    }
    Start-Sleep -Milliseconds 50
    $process.Refresh()
  }
  if (!$ready) {
    throw "timed out waiting for CXP runtime activation"
  }

  $rows = @()
  $related = @()
  for ($attempt = 0; $attempt -lt 300; $attempt++) {
    $related = @(Get-CimInstance Win32_Process | Where-Object {
      $_.ProcessId -eq $process.Id -or $_.ParentProcessId -eq $process.Id
    })
    $rows = @($related | Where-Object {
      $_.Name -ieq "cxp.exe" -and [IO.Path]::GetFileName($_.ExecutablePath) -ieq "cxp.exe"
    })
    if ($rows.Count -ge 2) {
      break
    }
    Start-Sleep -Milliseconds 50
    $process.Refresh()
  }
  if ($rows.Count -lt 2) {
    throw "expected stable cxp.exe parent and immutable runtime child; found $($rows.Count) CXP process(es)"
  }
  foreach ($row in $related) {
    $observed = "$($row.Name)`n$($row.ExecutablePath)`n$($row.CommandLine)"
    Write-Host $observed
    if ($observed -match "(?i)codex") {
      throw "CXP-owned process metadata contains the forbidden compatibility keyword"
    }
  }
  foreach ($row in $rows) {
    if ([IO.Path]::GetFileName($row.ExecutablePath) -ine "cxp.exe") {
      throw "unexpected CXP process image: $($row.ExecutablePath)"
    }
  }
  Write-Host "actual CXP process identity smoke passed"
} finally {
  if ($process) {
    $owned = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
      ($_.ProcessId -eq $process.Id -or $_.ParentProcessId -eq $process.Id) -and
      $_.Name -ieq "cxp.exe"
    } | Sort-Object { if ($_.ProcessId -eq $process.Id) { 1 } else { 0 } })
    $ownedIds = @($owned | ForEach-Object { $_.ProcessId })
    $waited = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
      $ownedIds -contains $_.ParentProcessId -and $_.Name -ieq "ping.exe"
    })
    foreach ($row in $owned) {
      Stop-Process -Id $row.ProcessId -Force -ErrorAction SilentlyContinue
    }
    foreach ($row in $waited) {
      Stop-Process -Id $row.ProcessId -Force -ErrorAction SilentlyContinue
    }
    foreach ($row in $owned) {
      Wait-Process -Id $row.ProcessId -Timeout 10 -ErrorAction SilentlyContinue
    }
  }
  if ($hadHome) { $env:HOME = $oldHome } else { Remove-Item Env:\HOME -ErrorAction SilentlyContinue }
  if ($hadStateHome) { $env:XDG_STATE_HOME = $oldStateHome } else { Remove-Item Env:\XDG_STATE_HOME -ErrorAction SilentlyContinue }
  foreach ($name in $runtimeEnvironment.Keys) {
    $value = $runtimeEnvironment[$name]
    if ($null -eq $value) {
      Remove-Item ("Env:" + $name) -ErrorAction SilentlyContinue
    } else {
      [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
  }
  Remove-Item -Recurse -Force -LiteralPath $root -ErrorAction SilentlyContinue
}
