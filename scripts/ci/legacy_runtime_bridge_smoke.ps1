$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("cxp-legacy-bridge-" + [Guid]::NewGuid().ToString("N"))
$root = Join-Path $tmp "install\.cxp-runtime"
$entry = Join-Path $tmp "install\cxp.exe"
$oldRuntime = Join-Path $root "versions\v0.1.13-rc.31\cxp.exe"
$candidateDir = Join-Path $root "versions\v0.1.13-rc.36"
$candidate = Join-Path $candidateDir ".codex-proxy_0.1.13-rc.36_windows_amd64.12345.exe"
$userEnvironment = @{}
foreach ($name in @("HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME")) {
  $userEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}

try {
  New-Item -ItemType Directory -Force -Path (Split-Path $oldRuntime), $candidateDir | Out-Null
  Push-Location $repoRoot
  try {
    go build -trimpath -o $oldRuntime .\scripts\tests\legacy_bridge_parent
    if ($LASTEXITCODE -ne 0) { throw "failed to build legacy bridge parent" }
    go build -trimpath `
      -ldflags "-X github.com/baaaaaaaka/codex-helper/internal/cli.version=v0.1.13-rc.36" `
      -o $candidate `
      .\cmd\codex-proxy
    if ($LASTEXITCODE -ne 0) { throw "failed to build candidate" }
  }
  finally {
    Pop-Location
  }
  Copy-Item -LiteralPath $oldRuntime -Destination $entry -Force
  Set-Content -LiteralPath (Join-Path $root "active") -Value "v0.1.13-rc.31" -NoNewline

  $profileRoot = Join-Path $tmp "profile"
  $env:HOME = $profileRoot
  $env:USERPROFILE = $profileRoot
  $env:APPDATA = Join-Path $profileRoot "AppData\Roaming"
  $env:LOCALAPPDATA = Join-Path $profileRoot "AppData\Local"
  $env:XDG_CONFIG_HOME = Join-Path $profileRoot "config"
  $env:XDG_CACHE_HOME = Join-Path $profileRoot "cache"
  $env:XDG_STATE_HOME = Join-Path $profileRoot "state"
  New-Item -ItemType Directory -Force -Path $env:APPDATA,$env:LOCALAPPDATA,$env:XDG_CONFIG_HOME,$env:XDG_CACHE_HOME,$env:XDG_STATE_HOME | Out-Null

  $saved = @{}
  foreach ($name in @("CXP_RUNTIME", "CXP_RUNTIME_ROOT", "CXP_RUNTIME_VERSION", "CXP_ENTRY_PATH", "CXP_RUNTIME_DISABLE", "CXP_RUNTIME_FORCE")) {
    $saved[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
  }
  try {
    $env:CXP_RUNTIME = "1"
    $env:CXP_RUNTIME_ROOT = $root
    $env:CXP_RUNTIME_VERSION = "v0.1.13-rc.31"
    $env:CXP_ENTRY_PATH = $entry
    $env:CXP_RUNTIME_DISABLE = "1"
    Remove-Item Env:CXP_RUNTIME_FORCE -ErrorAction SilentlyContinue

    $bridgeOutput = (& $oldRuntime $candidate | Out-String)
    if ($LASTEXITCODE -ne 0 -or $bridgeOutput -notmatch "0\.1\.13-rc\.36") {
      throw "legacy candidate bridge failed: $bridgeOutput"
    }
    if ((Get-Content -Raw (Join-Path $root "active")).Trim() -ne "v0.1.13-rc.36") {
      throw "bridge did not activate rc.36"
    }
    if ((Get-Content -Raw (Join-Path $root "previous")).Trim() -ne "v0.1.13-rc.31") {
      throw "bridge did not retain rc.31 as previous"
    }
    if (Test-Path -LiteralPath (Join-Path $root "pending-update.json")) {
      throw "Windows candidate did not finish its immediate pending reconciliation"
    }
    if ((Get-FileHash -Algorithm SHA256 $entry).Hash -ne (Get-FileHash -Algorithm SHA256 $oldRuntime).Hash) {
      throw "running Windows launcher was unexpectedly replaced"
    }
    $physicalVersion = (& $entry --version | Out-String)
    if ($physicalVersion -notmatch "0\.1\.13-rc\.31") {
      throw "inherited runtime probe did not model the old finalizer: $physicalVersion"
    }
  }
  finally {
    foreach ($name in $saved.Keys) {
      $value = $saved[$name]
      if ($null -eq $value) {
        Remove-Item ("Env:" + $name) -ErrorAction SilentlyContinue
      }
      else {
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
      }
    }
  }

  $freshVersion = (& $entry --version | Out-String)
  if ($LASTEXITCODE -ne 0 -or $freshVersion -notmatch "0\.1\.13-rc\.36") {
    throw "fresh Windows launcher did not dispatch rc.36: $freshVersion"
  }
  if (Test-Path -LiteralPath (Join-Path $root "pending-update.json")) {
    throw "startup reconciliation did not clear pending state"
  }
  Write-Host "legacy Windows runtime bridge smoke passed"
}
finally {
  foreach ($name in $userEnvironment.Keys) {
    $value = $userEnvironment[$name]
    if ($null -eq $value) {
      Remove-Item ("Env:" + $name) -ErrorAction SilentlyContinue
    }
    else {
      [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
  }
  Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue
}
