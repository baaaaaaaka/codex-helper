[CmdletBinding()]
param(
  [string]$Repo = $(if ($env:REPO) { $env:REPO } else { "baaaaaaaka/codex-helper" }),
  [string]$OldTag = $(if ($env:OLD_TAG) { $env:OLD_TAG } else { "v0.1.12" }),
  [string]$TargetTag = $(if ($env:TARGET_TAG) { $env:TARGET_TAG } else { "v0.1.13-rc.53" }),
  [ValidateSet("Split", "Converged")]
  [string]$ExpectedState = "Split"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

if ($env:OS -ne "Windows_NT") {
  throw "windows_locked_cxp_self_upgrade.ps1 must run on Windows"
}
if ([string]::IsNullOrWhiteSpace($Repo) -or [string]::IsNullOrWhiteSpace($OldTag) -or [string]::IsNullOrWhiteSpace($TargetTag)) {
  throw "Repo, OldTag, and TargetTag are required"
}
if ($OldTag -eq $TargetTag) {
  throw "OldTag and TargetTag must differ"
}

function Version-NoV([string]$Tag) {
  return $Tag.Trim().TrimStart("v")
}

function Assert-Contains([string]$Text, [string]$Needle, [string]$Description) {
  if ($Text.IndexOf($Needle, [StringComparison]::OrdinalIgnoreCase) -lt 0) {
    throw "$Description did not contain '$Needle'. Actual:`n$Text"
  }
}

function Get-VersionOutput([string]$Path, [bool]$DisableRuntime) {
  $hadDisable = Test-Path Env:CXP_RUNTIME_DISABLE
  $oldDisable = $env:CXP_RUNTIME_DISABLE
  if ($DisableRuntime) {
    $env:CXP_RUNTIME_DISABLE = "1"
  } else {
    Remove-Item Env:CXP_RUNTIME_DISABLE -ErrorAction SilentlyContinue
  }
  try {
    $output = (& $Path --version 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
      throw "$Path --version failed with exit code $LASTEXITCODE`: $output"
    }
    Write-Host "$Path --version => $output"
    return $output
  } finally {
    if ($hadDisable) {
      $env:CXP_RUNTIME_DISABLE = $oldDisable
    } else {
      Remove-Item Env:CXP_RUNTIME_DISABLE -ErrorAction SilentlyContinue
    }
  }
}

function Assert-Version([string]$Path, [string]$Tag, [bool]$DisableRuntime = $false) {
  if (!(Test-Path -LiteralPath $Path -PathType Leaf)) {
    throw "missing executable entrypoint: $Path"
  }
  $output = Get-VersionOutput $Path $DisableRuntime
  $expected = [regex]::Escape((Version-NoV $Tag))
  if ($output -notmatch "(?<![0-9A-Za-z.-])$expected(?![0-9A-Za-z.-])") {
    throw "$Path reported '$output', want $Tag"
  }
}

function Normalize-Path([string]$Path) {
  return [IO.Path]::GetFullPath($Path).TrimEnd('\')
}

function Assert-InstallRecord([string]$RecordPath, [string]$TargetPath, [string]$ShimPath, [string]$Tag, [bool]$RequireShim = $true) {
  if (!(Test-Path -LiteralPath $RecordPath -PathType Leaf)) {
    throw "missing managed install record: $RecordPath"
  }
  $record = Get-Content -Raw -LiteralPath $RecordPath | ConvertFrom-Json
  if ((Normalize-Path $record.target_path) -ine (Normalize-Path $TargetPath)) {
    throw "install record target_path is '$($record.target_path)', want '$TargetPath'"
  }
  if ((Version-NoV ([string]$record.version)) -ne (Version-NoV $Tag)) {
    throw "install record version is '$($record.version)', want '$Tag'"
  }
  if ($RequireShim) {
    $shimFound = $false
    foreach ($shim in @($record.shims)) {
      if ((Normalize-Path ([string]$shim)) -ieq (Normalize-Path $ShimPath)) {
        $shimFound = $true
        break
      }
    }
    if (!$shimFound) {
      throw "install record shims do not contain '$ShimPath': $($record.shims | ConvertTo-Json -Compress)"
    }
  }
}

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$installer = Join-Path $repoRoot "install.ps1"
$base = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [IO.Path]::GetTempPath() }
$root = Join-Path $base ("locked-cxp-self-upgrade-" + [guid]::NewGuid().ToString("N"))
$fixtureHome = Join-Path $root "home"
$installDir = Join-Path $fixtureHome ".local\bin"
$target = Join-Path $installDir "codex-proxy.exe"
$cxpExe = Join-Path $installDir "cxp.exe"
$cxpCmd = Join-Path $installDir "cxp.cmd"
$recordPath = Join-Path $fixtureHome "AppData\Roaming\codex-helper\install.json"
$runtimeActive = Join-Path $installDir ".cxp-runtime\active"
$stdoutLog = Join-Path $root "upgrade.stdout.log"
$stderrLog = Join-Path $root "upgrade.stderr.log"
$locationPushed = $false

try {
  New-Item -ItemType Directory -Force -Path $root,$fixtureHome,$installDir | Out-Null
  Push-Location $root
  $locationPushed = $true

  # Keep every installer/updater side effect inside this fixture. In particular,
  # never touch the runner's persistent PATH, PowerShell profile, Codex home, or
  # Teams configuration.
  $env:USERPROFILE = $fixtureHome
  $env:HOME = $fixtureHome
  $env:APPDATA = Join-Path $fixtureHome "AppData\Roaming"
  $env:LOCALAPPDATA = Join-Path $fixtureHome "AppData\Local"
  $env:TEMP = Join-Path $root "temp"
  $env:TMP = $env:TEMP
  $env:CODEX_HOME = Join-Path $fixtureHome ".codex"
  $env:CODEX_NPM_PREFIX = Join-Path $root "npm-global"
  $env:CODEX_PROXY_PROFILE_PATH = Join-Path $fixtureHome "Documents\PowerShell\Microsoft.PowerShell_profile.ps1"
  $env:CODEX_PROXY_SKIP_PATH_UPDATE = "1"
  $env:CODEX_PROXY_SKIP_BUILTIN_SKILLS = "1"
  New-Item -ItemType Directory -Force -Path $env:APPDATA,$env:LOCALAPPDATA,$env:TEMP,$env:CODEX_HOME | Out-Null
  foreach ($name in @(
    "CXP_RUNTIME", "CXP_RUNTIME_ROOT", "CXP_RUNTIME_VERSION", "CXP_ENTRY_PATH", "CXP_RUNTIME_DISABLE", "CXP_RUNTIME_FORCE",
    "CODEX_PROXY_INSTALL_PATH", "CODEX_PROXY_INSTALL_DIR", "CODEX_HELPER_TEAMS_TENANT_ID", "CODEX_HELPER_TEAMS_CLIENT_ID"
  )) {
    Remove-Item ("Env:" + $name) -ErrorAction SilentlyContinue
  }

  Write-Host "Installing the historical fixture with the current installer: $OldTag"
  & $installer -Repo $Repo -Version $OldTag -InstallDir $installDir
  if (!$?) {
    throw "install.ps1 failed"
  }

  Assert-Version $target $OldTag $true
  Assert-Version $cxpExe $OldTag $true
  Assert-Version $cxpCmd $OldTag
  Assert-InstallRecord $recordPath $target $cxpExe $OldTag
  $oldHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $cxpExe).Hash
  if ((Get-FileHash -Algorithm SHA256 -LiteralPath $target).Hash -ne $oldHash) {
    throw "installer did not seed codex-proxy.exe and cxp.exe from the same $OldTag binary"
  }

  Write-Host "Launching the locked stable entrypoint itself: $cxpExe upgrade $OldTag -> $TargetTag"
  $upgrade = Start-Process -FilePath $cxpExe `
    -ArgumentList @("upgrade", "--repo", $Repo, "--version", $TargetTag) `
    -WindowStyle Hidden -Wait -PassThru `
    -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog
  $stdout = Get-Content -Raw -LiteralPath $stdoutLog -ErrorAction SilentlyContinue
  $stderr = Get-Content -Raw -LiteralPath $stderrLog -ErrorAction SilentlyContinue
  $combined = $stdout + [Environment]::NewLine + $stderr
  Write-Host "----- upgrade stdout -----"
  Write-Host $stdout
  Write-Host "----- upgrade stderr -----"
  Write-Host $stderr

  if ($upgrade.ExitCode -ne 0) {
    throw "historical cxp self-upgrade exited $($upgrade.ExitCode):`n$combined"
  }
  Assert-Contains $combined "Updated to $TargetTag" "self-upgrade output"
  Assert-InstallRecord $recordPath $target $cxpExe $TargetTag $false
  if (!(Test-Path -LiteralPath $runtimeActive -PathType Leaf)) {
    throw "candidate did not publish the immutable runtime active marker: $runtimeActive"
  }
  $activeVersion = (Get-Content -Raw -LiteralPath $runtimeActive).Trim()
  if ((Version-NoV $activeVersion) -ne (Version-NoV $TargetTag)) {
    throw "runtime active marker is '$activeVersion', want '$TargetTag'"
  }

  if ($ExpectedState -eq "Split") {
    Assert-Contains $combined "failed to unify helper entrypoint" "known-regression warning"
    Assert-Contains $combined "Access is denied" "known-regression warning"
    Assert-Version $target $TargetTag $true
    Assert-Version $cxpExe $OldTag $true
    Assert-Version $cxpCmd $OldTag

    $targetHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $target).Hash
    $cxpHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $cxpExe).Hash
    if ($cxpHash -ne $oldHash) {
      throw "locked cxp.exe changed unexpectedly during the characterization run"
    }
    if ($targetHash -eq $cxpHash) {
      throw "expected split entrypoints, but codex-proxy.exe and cxp.exe are identical"
    }
    Write-Host "Known locked-cxp regression reproduced: managed target/runtime/record=$TargetTag, user-facing cxp=$OldTag"
  } else {
    if ($combined.IndexOf("failed to unify helper entrypoint", [StringComparison]::OrdinalIgnoreCase) -ge 0) {
      throw "self-upgrade still emitted the locked-entrypoint warning: $combined"
    }
    Assert-Version $target $TargetTag $true
    Assert-Version $cxpExe $TargetTag $true
    Assert-Version $cxpCmd $TargetTag
    if ((Get-FileHash -Algorithm SHA256 -LiteralPath $target).Hash -ne (Get-FileHash -Algorithm SHA256 -LiteralPath $cxpExe).Hash) {
      throw "converged entrypoints report the target version but contain different binaries"
    }
    Write-Host "Locked cxp self-upgrade converged without user intervention: all entrypoints=$TargetTag"
  }
} finally {
  if ($locationPushed) {
    Pop-Location
  }
  Remove-Item -Recurse -Force -LiteralPath $root -ErrorAction SilentlyContinue
}
