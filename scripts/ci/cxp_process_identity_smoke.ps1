$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$root = Join-Path ([IO.Path]::GetTempPath()) ("cxp-process-" + [guid]::NewGuid().ToString("N"))
$process = $null
$hadHome = Test-Path Env:\HOME
$oldHome = $env:HOME
$hadStateHome = Test-Path Env:\XDG_STATE_HOME
$oldStateHome = $env:XDG_STATE_HOME

try {
  New-Item -ItemType Directory -Force -Path $root | Out-Null
  Push-Location $repoRoot
  try {
    & go build -o (Join-Path $root "cxp.exe") .\cmd\codex-proxy
  } finally {
    Pop-Location
  }

  $stdout = Join-Path $root "stdout.log"
  $stderr = Join-Path $root "stderr.log"
  $env:HOME = Join-Path $root "home"
  $env:XDG_STATE_HOME = Join-Path $root "state"
  $process = Start-Process -FilePath (Join-Path $root "cxp.exe") `
    -ArgumentList @("responses", "serve", "--listen", "127.0.0.1:0", "--base-url", "http://127.0.0.1:9/v1", "--api-key", "process-identity-smoke", "--model", "process-identity-smoke", "--store-path", (Join-Path $root "responses.sqlite")) `
    -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru

  $active = Join-Path $root ".cxp-runtime\active"
  $ready = $false
  for ($attempt = 0; $attempt -lt 300; $attempt++) {
    if ($process.HasExited) {
      throw "CXP process exited before runtime activation: $(Get-Content -Raw -LiteralPath $stderr -ErrorAction SilentlyContinue)"
    }
    if (Test-Path -LiteralPath $active) {
      $ready = $true
      break
    }
    Start-Sleep -Milliseconds 50
    $process.Refresh()
  }
  if (!$ready) {
    throw "timed out waiting for CXP runtime activation"
  }

  $rows = @(Get-CimInstance Win32_Process | Where-Object {
    $_.ProcessId -eq $process.Id -or $_.ParentProcessId -eq $process.Id
  })
  if ($rows.Count -lt 2) {
    throw "expected stable cxp.exe parent and immutable runtime child; found $($rows.Count) process(es)"
  }
  foreach ($row in $rows) {
    $observed = "$($row.Name)`n$($row.ExecutablePath)`n$($row.CommandLine)"
    Write-Host $observed
    if ($observed -match "(?i)codex") {
      throw "CXP-owned process metadata contains the forbidden compatibility keyword"
    }
    if ([IO.Path]::GetFileName($row.ExecutablePath) -ine "cxp.exe") {
      throw "unexpected CXP process image: $($row.ExecutablePath)"
    }
  }
  Write-Host "actual CXP process identity smoke passed"
} finally {
  if ($process) {
    $owned = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
      $_.ProcessId -eq $process.Id -or $_.ParentProcessId -eq $process.Id
    } | Sort-Object { if ($_.ProcessId -eq $process.Id) { 1 } else { 0 } })
    foreach ($row in $owned) {
      Stop-Process -Id $row.ProcessId -Force -ErrorAction SilentlyContinue
    }
    foreach ($row in $owned) {
      Wait-Process -Id $row.ProcessId -Timeout 10 -ErrorAction SilentlyContinue
    }
  }
  if ($hadHome) { $env:HOME = $oldHome } else { Remove-Item Env:\HOME -ErrorAction SilentlyContinue }
  if ($hadStateHome) { $env:XDG_STATE_HOME = $oldStateHome } else { Remove-Item Env:\XDG_STATE_HOME -ErrorAction SilentlyContinue }
  Remove-Item -Recurse -Force -LiteralPath $root -ErrorAction SilentlyContinue
}
