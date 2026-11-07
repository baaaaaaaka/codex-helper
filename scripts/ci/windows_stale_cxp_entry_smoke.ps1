# A Windows field state (2026-11-07): an earlier candidate-owned update could
# not replace the copied cxp.exe while the Teams service held it, so cxp.exe
# still holds an older helper (v0.1.20) while the active runtime moved on
# (rc.66). The install record names codex-proxy.exe beside it, as install.ps1
# writes it. The next update refused cxp.exe ("is not a recorded managed
# helper") and so did every later one.
#
# This smoke builds the three helpers from source, lays them out that way and
# runs the real candidate-owned update the way a running helper does. With
# the install record the update converges; without it the stale cxp.exe is
# still refused and left alone.
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

if (-not $IsWindows) {
  throw "windows_stale_cxp_entry_smoke.ps1 must run on Windows"
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("cxp-stale-entry-" + [Guid]::NewGuid().ToString("N"))
$builds = Join-Path $tmp "builds"
$staleTag = "v0.1.20"
$activeTag = "v0.1.22-rc.66"
$targetTag = "v0.1.22-rc.69"
$userEnvironment = @{}
foreach ($name in @("HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME", "CXP_RUNTIME", "CXP_RUNTIME_ROOT", "CXP_RUNTIME_VERSION", "CXP_ENTRY_PATH", "CXP_RUNTIME_DISABLE", "CXP_RUNTIME_FORCE")) {
  $userEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}

function Build-Helper([string]$Tag) {
  $out = Join-Path $builds "$Tag\codex-proxy.exe"
  New-Item -ItemType Directory -Force -Path (Split-Path $out) | Out-Null
  Push-Location $repoRoot
  try {
    go build -trimpath -ldflags "-X github.com/baaaaaaaka/codex-helper/internal/cli.version=$Tag" -o $out .\cmd\codex-proxy
    if ($LASTEXITCODE -ne 0) { throw "failed to build $Tag" }
  }
  finally {
    Pop-Location
  }
  return $out
}

function Hash([string]$Path) {
  return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash
}

# Lays out the field state under $Dir and runs the candidate-owned update.
# Returns the candidate's exit code, output, and paths.
function Invoke-StaleEntryUpdate([string]$Dir, [bool]$WithRecord) {
  $install = Join-Path $Dir "install"
  $root = Join-Path $install ".cxp-runtime"
  $entry = Join-Path $install "cxp.exe"
  $managed = Join-Path $install "codex-proxy.exe"
  $profileRoot = Join-Path $Dir "profile"
  $env:HOME = $profileRoot
  $env:USERPROFILE = $profileRoot
  $env:APPDATA = Join-Path $profileRoot "AppData\Roaming"
  $env:LOCALAPPDATA = Join-Path $profileRoot "AppData\Local"
  $env:XDG_CONFIG_HOME = Join-Path $profileRoot "config"
  $env:XDG_CACHE_HOME = Join-Path $profileRoot "cache"
  $env:XDG_STATE_HOME = Join-Path $profileRoot "state"
  New-Item -ItemType Directory -Force -Path $env:APPDATA, $env:LOCALAPPDATA, $env:XDG_CONFIG_HOME, $env:XDG_CACHE_HOME, $env:XDG_STATE_HOME | Out-Null
  New-Item -ItemType Directory -Force -Path (Join-Path $root "versions\$activeTag") | Out-Null

  Copy-Item -LiteralPath $script:staleBuild -Destination $entry -Force
  Copy-Item -LiteralPath $script:activeBuild -Destination $managed -Force
  Copy-Item -LiteralPath $script:activeBuild -Destination (Join-Path $root "versions\$activeTag\cxp.exe") -Force
  Set-Content -LiteralPath (Join-Path $install "cxp.cmd") -NoNewline -Encoding ASCII -Value "@echo off`r`n`"%~dp0cxp.exe`" %*`r`n"
  Set-Content -LiteralPath (Join-Path $root "active") -Value $activeTag -NoNewline

  $recordPath = Join-Path $env:APPDATA "codex-helper\install.json"
  if ($WithRecord) {
    New-Item -ItemType Directory -Force -Path (Split-Path $recordPath) | Out-Null
    $record = [ordered]@{
      schema_version = 1
      target_path    = $managed
      target_source  = "installer"
      target_state   = "managed"
      version        = $activeTag.TrimStart("v")
      goos           = "windows"
      goarch         = "amd64"
      shims          = @((Join-Path $install "cxp.cmd"))
    }
    [System.IO.File]::WriteAllText($recordPath, ($record | ConvertTo-Json -Depth 4))
  }

  # The candidate is downloaded beside the runtime, as the updater does.
  $candidate = Join-Path $root (".codex-proxy_" + $targetTag.TrimStart("v") + "_windows_amd64.exe")
  Copy-Item -LiteralPath $script:targetBuild -Destination $candidate -Force
  $context = [ordered]@{
    schema           = 1
    candidate_path   = $candidate
    candidate_sha256 = (Hash $candidate).ToLowerInvariant()
    target_version   = $targetTag
    runtime_root     = $root
    entry_path       = $entry
    record_path      = $recordPath
    request_id       = "stale-cxp-entry-smoke"
  }
  $contextPath = Join-Path $Dir "context.json"
  [System.IO.File]::WriteAllText($contextPath, ($context | ConvertTo-Json -Depth 4))

  $env:CXP_RUNTIME_DISABLE = "1"
  foreach ($name in @("CXP_RUNTIME", "CXP_RUNTIME_ROOT", "CXP_RUNTIME_VERSION", "CXP_ENTRY_PATH", "CXP_RUNTIME_FORCE")) {
    Remove-Item ("Env:" + $name) -ErrorAction SilentlyContinue
  }
  $oldPreference = $PSNativeCommandUseErrorActionPreference
  $PSNativeCommandUseErrorActionPreference = $false
  try {
    $output = (& $candidate "__internal-update-apply" "--protocol=1" "--context-file=$contextPath" 2>&1 | Out-String)
    $code = $LASTEXITCODE
  }
  finally {
    $PSNativeCommandUseErrorActionPreference = $oldPreference
    Remove-Item Env:CXP_RUNTIME_DISABLE -ErrorAction SilentlyContinue
  }
  return @{ Code = $code; Output = $output; Entry = $entry; Root = $root }
}

try {
  $script:staleBuild = Build-Helper $staleTag
  $script:activeBuild = Build-Helper $activeTag
  $script:targetBuild = Build-Helper $targetTag

  # Without an install record the stale entry is not provably ours.
  $unowned = Invoke-StaleEntryUpdate (Join-Path $tmp "unowned") $false
  $unowned.Output | Write-Host
  if ($unowned.Code -eq 0 -or $unowned.Output -notmatch "is not a recorded managed helper") {
    throw "an unowned stale cxp.exe was not refused (exit=$($unowned.Code))"
  }
  if ((Hash $unowned.Entry) -ne (Hash $script:staleBuild)) {
    throw "an unowned stale cxp.exe was changed"
  }

  # With the record (install.ps1's layout) the update converges.
  $owned = Invoke-StaleEntryUpdate (Join-Path $tmp "owned") $true
  $owned.Output | Write-Host
  if ($owned.Code -ne 0) {
    throw "the update refused the install's own stale cxp.exe (exit=$($owned.Code)): $($owned.Output)"
  }
  if ((Get-Content -Raw (Join-Path $owned.Root "active")).Trim() -ne $targetTag) {
    throw "the update did not activate $targetTag"
  }
  if ((Hash $owned.Entry) -ne (Hash $script:targetBuild)) {
    throw "cxp.exe was not refreshed to $targetTag"
  }
  $version = (& $owned.Entry --version | Out-String)
  if ($LASTEXITCODE -ne 0 -or $version -notmatch [regex]::Escape($targetTag.TrimStart("v"))) {
    throw "cxp.exe does not run $targetTag after the update: $version"
  }
  if (Test-Path -LiteralPath (Join-Path $owned.Root "pending-update.json")) {
    throw "the first run after the update did not finish the pending reconciliation"
  }
  Write-Host "stale cxp.exe entry smoke passed: $staleTag entry, $activeTag active -> $targetTag"
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
