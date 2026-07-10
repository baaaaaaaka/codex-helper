param(
  [Parameter(Mandatory = $true)]
  [string]$Helper
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

if (!(Test-Path -LiteralPath $Helper)) {
  throw "helper does not exist: $Helper"
}

$root = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [IO.Path]::GetTempPath() }
$base = Join-Path $root "codex-desktop-network-install-smoke"
Remove-Item -Recurse -Force -LiteralPath $base -ErrorAction SilentlyContinue
$work = Join-Path $base "work"
New-Item -ItemType Directory -Force -Path $work | Out-Null
$config = Join-Path $base "config.json"
$out = Join-Path $base "app-launch.out"
$desktopProcessNames = @("ChatGPT", "Codex")
$existingProcessIds = @{}
Get-Process -Name $desktopProcessNames -ErrorAction SilentlyContinue | ForEach-Object {
  $existingProcessIds[$_.Id] = $true
}
$launchedProcesses = @()

try {
  try {
    "n" | & $Helper --config $config app --cwd $work *> $out
  } catch {
    $appOut = if (Test-Path -LiteralPath $out) { Get-Content -Raw -LiteralPath $out } else { "" }
    throw "cxp app failed during Codex desktop app network install smoke`napp output:`n$appOut`nerror:`n$($_.Exception.Message)"
  }

  $pkg = Get-AppxPackage -Name OpenAI.Codex -ErrorAction SilentlyContinue |
    Sort-Object Version -Descending |
    Select-Object -First 1
  if ($null -eq $pkg) {
    $appOut = if (Test-Path -LiteralPath $out) { Get-Content -Raw -LiteralPath $out } else { "" }
    throw "cxp app did not install the OpenAI.Codex desktop package`napp output:`n$appOut"
  }

  $manifest = Get-AppxPackageManifest -Package $pkg.PackageFullName
  $applications = @($manifest.Package.Applications.Application)
  $supportedEntries = @($applications | Where-Object {
    $name = [IO.Path]::GetFileName([string]$_.Executable)
    $name -ieq "ChatGPT.exe" -or $name -ieq "Codex.exe"
  })
  if ($supportedEntries.Count -eq 0) {
    $entries = ($applications | ForEach-Object { "Id=$($_.Id) Executable=$($_.Executable)" }) -join "; "
    throw "OpenAI.Codex manifest has no ChatGPT.exe or Codex.exe application entry: $entries"
  }
  $packagedExecutables = @($supportedEntries | ForEach-Object {
    Join-Path $pkg.InstallLocation ([string]$_.Executable)
  })
  $installedExecutables = @($packagedExecutables | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf })
  if ($installedExecutables.Count -eq 0) {
    throw "OpenAI.Codex package manifest points to no installed ChatGPT.exe or Codex.exe under $($pkg.InstallLocation)"
  }
  Write-Host "Desktop package entry contract: $($supportedEntries[0].Executable); installed executable: $($installedExecutables[0])"

  $found = $false
  for ($i = 0; $i -lt 90; $i++) {
    $proc = @(Get-Process -Name $desktopProcessNames -ErrorAction SilentlyContinue | Where-Object { -not $existingProcessIds.ContainsKey($_.Id) })
    if ($proc.Count -gt 0) {
      $launchedProcesses = $proc
      $found = $true
      break
    }
    Start-Sleep -Seconds 1
  }
  if (-not $found) {
    $appOut = if (Test-Path -LiteralPath $out) { Get-Content -Raw -LiteralPath $out } else { "" }
    throw "ChatGPT/Codex desktop app was installed but no supported process was observed`napp output:`n$appOut"
  }

  Write-Host "Codex desktop app network install smoke passed: $($pkg.PackageFullName)"
} finally {
  if ($launchedProcesses.Count -gt 0) {
    $launchedProcesses | Stop-Process -Force -ErrorAction SilentlyContinue
  }
}
