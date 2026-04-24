[CmdletBinding()]
param(
  [Parameter(Mandatory = $false)]
  [string]$Repo = "baaaaaaaka/codex-helper",

  [Parameter(Mandatory = $false)]
  [string]$Version = "latest",

  [Parameter(Mandatory = $false)]
  [string]$InstallDir = "$env:USERPROFILE\\.local\\bin"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:InstallFailureReason = $null
$script:InstallSuccessDetails = @()
$script:InstallShowSummary = $true
$installMinFreeKB = 131072
if (-not [string]::IsNullOrWhiteSpace($env:CODEX_PROXY_INSTALL_MIN_FREE_KB)) {
  $parsedMinFreeKB = 0L
  if ([long]::TryParse($env:CODEX_PROXY_INSTALL_MIN_FREE_KB, [ref]$parsedMinFreeKB) -and $parsedMinFreeKB -gt 0) {
    $installMinFreeKB = $parsedMinFreeKB
  }
}
$script:InstallMinFreeBytes = $installMinFreeKB * 1024L

function Write-InstallBanner([string]$title) {
  Write-Host ""
  Write-Host "============================================================"
  Write-Host ("  " + $title)
  Write-Host "============================================================"
}

trap {
  if ([string]::IsNullOrWhiteSpace($script:InstallFailureReason)) {
    $script:InstallFailureReason = $_.Exception.Message
  }
  if ($script:InstallShowSummary) {
    Write-InstallBanner "CODEX-PROXY INSTALL FAILED"
    Write-Host ("Reason: " + $script:InstallFailureReason)
  }
  exit 1
}

$apiBase = $env:CODEX_PROXY_API_BASE
if ([string]::IsNullOrWhiteSpace($apiBase)) {
  $apiBase = "https://api.github.com"
}
$apiBase = $apiBase.TrimEnd("/")

$releaseBase = $env:CODEX_PROXY_RELEASE_BASE
if ([string]::IsNullOrWhiteSpace($releaseBase)) {
  $releaseBase = "https://github.com"
}
$releaseBase = $releaseBase.TrimEnd("/")

function Ensure-ProfileLine([string]$path, [string]$line) {
  if ([string]::IsNullOrWhiteSpace($path) -or [string]::IsNullOrWhiteSpace($line)) {
    return $false
  }
  $dir = Split-Path -Parent $path
  if (-not (Test-Path -LiteralPath $dir)) {
    Invoke-DiskWrite -Label "shell profile directory" -PathValue $dir -DefaultReason "Failed to create shell profile directory: $dir" -Action {
      New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
  }
  if (-not (Test-Path -LiteralPath $path)) {
    Invoke-DiskWrite -Label "shell profile" -PathValue $path -DefaultReason "Failed to create shell profile: $path" -Action {
      New-Item -ItemType File -Force -Path $path | Out-Null
    }
  }
  if (-not (Select-String -Path $path -SimpleMatch -Quiet -Pattern $line)) {
    Invoke-DiskWrite -Label "shell profile" -PathValue $path -DefaultReason "Failed to update shell profile: $path" -Action {
      Add-Content -Path $path -Value $line
    }
    return $true
  }
  return $false
}

function Remove-ProfileLine([string]$path, [string]$line) {
  if ([string]::IsNullOrWhiteSpace($path) -or [string]::IsNullOrWhiteSpace($line) -or -not (Test-Path -LiteralPath $path)) {
    return $false
  }
  $lines = @(Get-Content -Path $path)
  $filtered = @($lines | Where-Object { $_ -ne $line })
  if ($filtered.Count -eq $lines.Count) {
    return $false
  }
  Invoke-DiskWrite -Label "shell profile" -PathValue $path -DefaultReason "Failed to update shell profile: $path" -Action {
    Set-Content -Path $path -Value $filtered -Encoding UTF8
  }
  return $true
}

function Test-FileContainsMarker([string]$path, [string[]]$markers) {
  if ([string]::IsNullOrWhiteSpace($path) -or -not (Test-Path -LiteralPath $path)) {
    return $false
  }
  try {
    $bytes = [System.IO.File]::ReadAllBytes($path)
  } catch {
    return $false
  }
  $text = [System.Text.Encoding]::ASCII.GetString($bytes)
  foreach ($marker in $markers) {
    if ([string]::IsNullOrWhiteSpace($marker)) {
      continue
    }
    if ($text.IndexOf($marker, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
      return $true
    }
  }
  return $false
}

function Test-IsCodexOwnedLegacyFile([string]$path) {
  return Test-FileContainsMarker -path $path -markers @(
    "github.com/baaaaaaaka/codex-helper",
    "codex-proxy"
  )
}

function Normalize-PathEntry([string]$pathValue) {
  if ([string]::IsNullOrWhiteSpace($pathValue)) {
    return ""
  }
  try {
    $pathValue = [IO.Path]::GetFullPath($pathValue)
  } catch {
    # Leave the original value when it cannot be normalized further.
  }
  return $pathValue.TrimEnd("\")
}

function Test-PathInValue([string]$pathText, [string]$pathValue) {
  $target = Normalize-PathEntry $pathValue
  if ([string]::IsNullOrWhiteSpace($target)) {
    return $false
  }
  $parts = @()
  if (-not [string]::IsNullOrWhiteSpace($pathText)) {
    $parts = $pathText -split ";"
  }
  foreach ($part in $parts) {
    if ([string]::IsNullOrWhiteSpace($part)) { continue }
    if ((Normalize-PathEntry $part) -ieq $target) {
      return $true
    }
  }
  return $false
}

function Test-PathInEnv([string]$pathValue) {
  return Test-PathInValue -pathText $env:Path -pathValue $pathValue
}

function Prepend-PathEntries([string]$pathText, [string[]]$pathValues) {
  $parts = New-Object System.Collections.Generic.List[string]
  if (-not [string]::IsNullOrWhiteSpace($pathText)) {
    foreach ($part in ($pathText -split ";")) {
      if (-not [string]::IsNullOrWhiteSpace($part)) {
        $parts.Add($part)
      }
    }
  }

  $seen = @{}
  foreach ($part in $parts) {
    $normalized = Normalize-PathEntry $part
    if (-not [string]::IsNullOrWhiteSpace($normalized)) {
      $seen[$normalized.ToLowerInvariant()] = $true
    }
  }

  $prepend = New-Object System.Collections.Generic.List[string]
  foreach ($pathValue in $pathValues) {
    $normalized = Normalize-PathEntry $pathValue
    if ([string]::IsNullOrWhiteSpace($normalized)) {
      continue
    }
    $key = $normalized.ToLowerInvariant()
    if ($seen.ContainsKey($key)) {
      continue
    }
    $seen[$key] = $true
    $prepend.Add($normalized)
  }

  if ($prepend.Count -eq 0) {
    return $pathText
  }

  $combined = New-Object System.Collections.Generic.List[string]
  foreach ($entry in $prepend) {
    $combined.Add($entry)
  }
  foreach ($entry in $parts) {
    $combined.Add($entry)
  }
  return ($combined | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join ";"
}

function Add-PathPersistent([string[]]$pathValues) {
  if ($env:CODEX_PROXY_SKIP_PATH_UPDATE -eq "1") {
    return
  }
  try {
    $current = [Environment]::GetEnvironmentVariable("Path", "User")
    $newPath = Prepend-PathEntries -pathText $current -pathValues $pathValues
    if ($newPath -ceq $current) {
      return
    }
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
  } catch {
    Write-Warning "Failed to persist PATH update: $_"
  }
}

function Get-ExistingPathForSpaceCheck([string]$pathValue) {
  if ([string]::IsNullOrWhiteSpace($pathValue)) {
    return $null
  }
  try {
    $candidate = [IO.Path]::GetFullPath($pathValue)
  } catch {
    return $null
  }
  while (-not [string]::IsNullOrWhiteSpace($candidate) -and -not (Test-Path -LiteralPath $candidate)) {
    $parent = Split-Path -Parent $candidate
    if ([string]::IsNullOrWhiteSpace($parent) -or $parent -eq $candidate) {
      return $null
    }
    $candidate = $parent
  }
  return $candidate
}

function Get-FreeBytesForPath([string]$pathValue) {
  $existing = Get-ExistingPathForSpaceCheck $pathValue
  if ([string]::IsNullOrWhiteSpace($existing)) {
    return $null
  }
  try {
    $full = [IO.Path]::GetFullPath($existing)
    $root = [IO.Path]::GetPathRoot($full)
    if ([string]::IsNullOrWhiteSpace($root)) {
      return $null
    }
    $drive = [System.IO.DriveInfo]::new($root)
    if (-not $drive.IsReady) {
      return $null
    }
    return [long]$drive.AvailableFreeSpace
  } catch {
    return $null
  }
}

function Get-DiskSpaceFailureReason([string]$label, [string]$pathValue, [long]$minBytes) {
  if ($minBytes -le 0) { return $null }
  $freeBytes = Get-FreeBytesForPath $pathValue
  if ($null -eq $freeBytes) { return $null }
  if ($freeBytes -lt $minBytes) {
    $haveMiB = [math]::Floor($freeBytes / 1MB)
    $needMiB = [math]::Ceiling($minBytes / 1MB)
    return "Not enough disk space for $label ($pathValue): $haveMiB MiB available, need at least $needMiB MiB."
  }
  return $null
}

function Test-DiskSpaceError([object]$errorRecord) {
  $text = ""
  if ($null -ne $errorRecord) {
    $text = $errorRecord.ToString()
    if ($errorRecord.Exception -and $errorRecord.Exception.Message) {
      $text = $text + "`n" + $errorRecord.Exception.Message
    }
  }
  return ($text -match '(?i)(no space left|not enough space|disk full|insufficient disk|quota)')
}

function Assert-DiskSpace([string]$label, [string]$pathValue, [long]$minBytes) {
  if ($minBytes -le 0) {
    return
  }
  $reason = Get-DiskSpaceFailureReason -label $label -pathValue $pathValue -minBytes $minBytes
  if (-not [string]::IsNullOrWhiteSpace($reason)) {
    $script:InstallFailureReason = $reason
    throw $script:InstallFailureReason
  }
  if ($null -eq (Get-FreeBytesForPath $pathValue)) {
    Write-Warning "Could not reliably check free disk space for $label ($pathValue); continuing."
  }
}

function Invoke-DiskWrite([string]$Label, [string]$PathValue, [string]$DefaultReason, [scriptblock]$Action) {
  Assert-DiskSpace -label $Label -pathValue $PathValue -minBytes $script:InstallMinFreeBytes
  try {
    & $Action
  } catch {
    $reason = Get-DiskSpaceFailureReason -label $Label -pathValue $PathValue -minBytes $script:InstallMinFreeBytes
    if ([string]::IsNullOrWhiteSpace($reason) -and (Test-DiskSpaceError $_)) {
      $reason = "Not enough disk space for $Label ($PathValue)."
    }
    if (-not [string]::IsNullOrWhiteSpace($reason)) {
      $script:InstallFailureReason = $reason
      throw $script:InstallFailureReason
    }
    if (-not [string]::IsNullOrWhiteSpace($DefaultReason)) {
      $script:InstallFailureReason = "$DefaultReason`: $($_.Exception.Message)"
      throw $script:InstallFailureReason
    }
    throw
  }
}

function New-ProfilePathLine([string]$pathValue) {
  $normalized = Normalize-PathEntry $pathValue
  if ([string]::IsNullOrWhiteSpace($normalized)) {
    return ""
  }
  $literal = $normalized.Replace("'", "''")
  return "if (-not ((`$env:Path -split ';') | Where-Object { -not [string]::IsNullOrWhiteSpace(`$_) -and `$_.TrimEnd('\') -ieq '$literal' })) { `$env:Path = '$literal;' + `$env:Path }"
}

function Get-LatestTag([string]$repo) {
  $apiUri = "$apiBase/repos/$repo/releases/latest"
  try {
    $resp = Invoke-RestMethod -Uri $apiUri -Headers @{ "User-Agent" = "codex-proxy-install" }
    if ($resp.tag_name) { return [string]$resp.tag_name }
  } catch {
    # Fall back to parsing the redirect URL below.
  }

  $latestUri = "$releaseBase/$repo/releases/latest"
  try {
    $resp = Invoke-WebRequest -Uri $latestUri -Headers @{ "User-Agent" = "codex-proxy-install" } -UseBasicParsing
    $finalUri = $resp.BaseResponse.ResponseUri
    if ($finalUri -and $finalUri.AbsolutePath) {
      $tag = ($finalUri.AbsolutePath.TrimEnd("/") -split "/")[-1]
      if (-not [string]::IsNullOrWhiteSpace($tag) -and $tag -ne "latest") {
        return [string]$tag
      }
    }
  } catch {
    throw "Failed to determine latest tag from $latestUri"
  }

  throw "Failed to determine latest tag from $apiUri"
}

Assert-DiskSpace -label "temporary download directory" -pathValue $env:TEMP -minBytes $script:InstallMinFreeBytes
Assert-DiskSpace -label "install directory" -pathValue $InstallDir -minBytes $script:InstallMinFreeBytes

$tag = $Version
if ([string]::IsNullOrWhiteSpace($tag) -or $tag -eq "latest") {
  $tag = Get-LatestTag -repo $Repo
}

$verNoV = $tag.TrimStart("v")
$arch = "amd64"
$asset = "codex-proxy_${verNoV}_windows_${arch}.exe"
$url = "$releaseBase/$Repo/releases/download/$tag/$asset"
$checksumsUrl = "$releaseBase/$Repo/releases/download/$tag/checksums.txt"

Invoke-DiskWrite -Label "install directory" -PathValue $InstallDir -DefaultReason "Failed to create install directory: $InstallDir" -Action {
  New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
}
$installDirResolved = [IO.Path]::GetFullPath($InstallDir)

$tmp = Join-Path $env:TEMP "$asset"
Invoke-DiskWrite -Label "release asset download" -PathValue $tmp -DefaultReason "Failed to download release asset: $url" -Action {
  Invoke-WebRequest -Uri $url -OutFile $tmp -UseBasicParsing
}

# Optional checksum verification.
try {
  $checksumsTmp = Join-Path $env:TEMP "checksums.txt"
  Assert-DiskSpace -label "checksum download" -pathValue $checksumsTmp -minBytes $script:InstallMinFreeBytes
  try {
    Invoke-WebRequest -Uri $checksumsUrl -OutFile $checksumsTmp -UseBasicParsing
  } catch {
    $reason = Get-DiskSpaceFailureReason -label "checksum download" -pathValue $checksumsTmp -minBytes $script:InstallMinFreeBytes
    if ([string]::IsNullOrWhiteSpace($reason) -and (Test-DiskSpaceError $_)) {
      $reason = "Not enough disk space for checksum download ($checksumsTmp)."
    }
    if (-not [string]::IsNullOrWhiteSpace($reason)) {
      $script:InstallFailureReason = $reason
      throw $script:InstallFailureReason
    }
    throw
  }
  $expected = (Select-String -Path $checksumsTmp -Pattern ("\s{1}" + [regex]::Escape($asset) + "$") | Select-Object -First 1).Line.Split(" ", [System.StringSplitOptions]::RemoveEmptyEntries)[0]
  if ($expected) {
    $actual = (Get-FileHash -Algorithm SHA256 -Path $tmp).Hash.ToLowerInvariant()
    if ($expected.ToLowerInvariant() -ne $actual) {
      throw "Checksum mismatch for $asset (expected $expected, got $actual)"
    }
  }
} catch {
  if (Test-DiskSpaceError $_) {
    $reason = Get-DiskSpaceFailureReason -label "checksum download" -pathValue $checksumsTmp -minBytes $script:InstallMinFreeBytes
    if ([string]::IsNullOrWhiteSpace($reason)) {
      $reason = "Not enough disk space for checksum download ($checksumsTmp)."
    }
    $script:InstallFailureReason = $reason
    throw $script:InstallFailureReason
  }
  if (-not [string]::IsNullOrWhiteSpace($script:InstallFailureReason)) {
    throw
  }
  # Best-effort only; do not fail installation if checksum fetch/parse fails.
}

$dst = Join-Path $installDirResolved "codex-proxy.exe"
Invoke-DiskWrite -Label "codex-proxy binary install" -PathValue $dst -DefaultReason "Failed to move codex-proxy into $dst" -Action {
  Move-Item -Force -Path $tmp -Destination $dst
}

$cxpCmd = Join-Path $installDirResolved "cxp.cmd"
$cxpContent = "@echo off`r`n`"%~dp0codex-proxy.exe`" %*`r`n"
Invoke-DiskWrite -Label "cxp shim install" -PathValue $cxpCmd -DefaultReason "Failed to install cxp shim: $cxpCmd" -Action {
  Set-Content -Path $cxpCmd -Value $cxpContent -Encoding ASCII
}
$managedPrefix = $env:CODEX_NPM_PREFIX
if ([string]::IsNullOrWhiteSpace($managedPrefix)) {
  $localAppData = [Environment]::GetFolderPath('LocalApplicationData')
  if (-not [string]::IsNullOrWhiteSpace($localAppData)) {
    $managedPrefix = Join-Path $localAppData "codex-proxy\npm-global"
  }
}
$pathEntries = New-Object System.Collections.Generic.List[string]
$pathEntries.Add($installDirResolved)
if (-not [string]::IsNullOrWhiteSpace($managedPrefix)) {
  $managedPrefixResolved = [IO.Path]::GetFullPath($managedPrefix)
  $pathEntries.Add($managedPrefixResolved)
  $pathEntries.Add((Join-Path $managedPrefixResolved "bin"))
}

$env:Path = Prepend-PathEntries -pathText $env:Path -pathValues $pathEntries
Add-PathPersistent -pathValues $pathEntries

$profilePath = $env:CODEX_PROXY_PROFILE_PATH
if ([string]::IsNullOrWhiteSpace($profilePath)) {
  $profilePath = $PROFILE
}
$aliasLine = 'Set-Alias -Name cxp -Value codex-proxy'

$profileUpdated = $false
foreach ($pathValue in $pathEntries) {
  $legacyLine = '$env:Path = "' + (Normalize-PathEntry $pathValue) + ';$env:Path"'
  if (Remove-ProfileLine -path $profilePath -line $legacyLine) {
    $profileUpdated = $true
  }
  $pathLine = New-ProfilePathLine -pathValue $pathValue
  if (-not [string]::IsNullOrWhiteSpace($pathLine) -and (Ensure-ProfileLine -path $profilePath -line $pathLine)) {
    $profileUpdated = $true
  }
}
if (Ensure-ProfileLine -path $profilePath -line $aliasLine) {
  $profileUpdated = $true
}

if ($profileUpdated) {
  try {
    . $profilePath
  } catch {
    Write-Warning "Failed to reload profile: $_"
  }
}

# Clean up legacy command names when they can be positively identified as
# codex-proxy-owned leftovers from earlier installs.
$legacyClaudeProxyExe = Join-Path $installDirResolved "claude-proxy.exe"
$legacyClaudeProxyOwned = Test-IsCodexOwnedLegacyFile -path $legacyClaudeProxyExe
foreach ($legacyName in @("claude-proxy.exe", "clp.exe", "clp.cmd")) {
  $legacyPath = Join-Path $installDirResolved $legacyName
  if (Test-Path $legacyPath) {
    $shouldRemove = $false
    $content = Get-Content -Path $legacyPath -Raw -ErrorAction SilentlyContinue
    $isCodexOwned = Test-IsCodexOwnedLegacyFile -path $legacyPath
    if ($content -and ($content -match "codex-proxy")) {
      $shouldRemove = $true
    }
    if ($legacyName -like "*.exe") {
      try {
        $ver = & $legacyPath --version 2>&1 | Out-String
        if ($ver -match "codex-proxy") { $shouldRemove = $true }
        if (-not $shouldRemove -and $legacyName -ieq "claude-proxy.exe" -and $ver -match "claude-proxy" -and $isCodexOwned) {
          $shouldRemove = $true
        }
        if (-not $shouldRemove -and $legacyName -ieq "clp.exe" -and $ver -match "claude-proxy" -and $isCodexOwned) {
          $shouldRemove = $true
        }
      } catch { }
    }
    if (-not $shouldRemove -and $legacyName -ieq "claude-proxy.exe" -and $isCodexOwned) {
      $shouldRemove = $true
    }
    if (-not $shouldRemove -and $legacyName -ieq "clp.exe" -and $isCodexOwned) {
      $shouldRemove = $true
    }
    if (-not $shouldRemove -and $legacyName -ieq "clp.cmd" -and $content -match "claude-proxy\.exe") {
      if ($legacyClaudeProxyOwned) {
        $shouldRemove = $true
      }
    }
    if (-not $shouldRemove -and $legacyName -ieq "clp.cmd" -and $isCodexOwned) {
      $shouldRemove = $true
    }
    if ($shouldRemove) {
      Remove-Item -Force $legacyPath -ErrorAction SilentlyContinue
      Write-Host "Removed legacy: $legacyPath"
    }
  }
}

$script:InstallSuccessDetails = @(
  "Installed: $dst",
  "Shell profile and user PATH checked for install/managed CLI directories (reload attempted):",
  "  $profilePath"
)
Write-InstallBanner "CODEX-PROXY INSTALL SUCCESS"
foreach ($line in $script:InstallSuccessDetails) {
  Write-Host $line
}
