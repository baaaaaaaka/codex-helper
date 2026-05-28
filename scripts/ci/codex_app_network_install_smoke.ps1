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
$existingProcessIds = @{}
Get-Process -Name Codex -ErrorAction SilentlyContinue | ForEach-Object {
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

  $found = $false
  for ($i = 0; $i -lt 90; $i++) {
    $proc = @(Get-Process -Name Codex -ErrorAction SilentlyContinue | Where-Object { -not $existingProcessIds.ContainsKey($_.Id) })
    if ($proc.Count -gt 0) {
      $launchedProcesses = $proc
      $found = $true
      break
    }
    Start-Sleep -Seconds 1
  }
  if (-not $found) {
    $appOut = if (Test-Path -LiteralPath $out) { Get-Content -Raw -LiteralPath $out } else { "" }
    throw "Codex desktop app was installed but no launched Codex process was observed`napp output:`n$appOut"
  }

  Write-Host "Codex desktop app network install smoke passed: $($pkg.PackageFullName)"
} finally {
  if ($launchedProcesses.Count -gt 0) {
    $launchedProcesses | Stop-Process -Force -ErrorAction SilentlyContinue
  }
}
