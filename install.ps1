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
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
  }
  if (-not (Test-Path -LiteralPath $path)) {
    New-Item -ItemType File -Force -Path $path | Out-Null
  }
  if (-not (Select-String -Path $path -SimpleMatch -Quiet -Pattern $line)) {
    Add-Content -Path $path -Value $line
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
  Set-Content -Path $path -Value $filtered -Encoding UTF8
  return $true
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

$tag = $Version
if ([string]::IsNullOrWhiteSpace($tag) -or $tag -eq "latest") {
  $tag = Get-LatestTag -repo $Repo
}

$verNoV = $tag.TrimStart("v")
$arch = "amd64"
$asset = "codex-proxy_${verNoV}_windows_${arch}.exe"
$url = "$releaseBase/$Repo/releases/download/$tag/$asset"
$checksumsUrl = "$releaseBase/$Repo/releases/download/$tag/checksums.txt"

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$installDirResolved = [IO.Path]::GetFullPath($InstallDir)

$tmp = Join-Path $env:TEMP "$asset"
Invoke-WebRequest -Uri $url -OutFile $tmp -UseBasicParsing

# Optional checksum verification.
try {
  $checksumsTmp = Join-Path $env:TEMP "checksums.txt"
  Invoke-WebRequest -Uri $checksumsUrl -OutFile $checksumsTmp -UseBasicParsing
  $expected = (Select-String -Path $checksumsTmp -Pattern ("\s{1}" + [regex]::Escape($asset) + "$") | Select-Object -First 1).Line.Split(" ", [System.StringSplitOptions]::RemoveEmptyEntries)[0]
  if ($expected) {
    $actual = (Get-FileHash -Algorithm SHA256 -Path $tmp).Hash.ToLowerInvariant()
    if ($expected.ToLowerInvariant() -ne $actual) {
      throw "Checksum mismatch for $asset (expected $expected, got $actual)"
    }
  }
} catch {
  # Best-effort only; do not fail installation if checksum fetch/parse fails.
}

$dst = Join-Path $installDirResolved "codex-proxy.exe"
Move-Item -Force -Path $tmp -Destination $dst

$cxpCmd = Join-Path $installDirResolved "cxp.cmd"
$cxpContent = "@echo off`r`n`"%~dp0codex-proxy.exe`" %*`r`n"
Set-Content -Path $cxpCmd -Value $cxpContent -Encoding ASCII
$legacyClpExe = Join-Path $installDirResolved "clp.exe"
if (Test-Path $legacyClpExe) {
  Remove-Item -Force $legacyClpExe -ErrorAction SilentlyContinue
}
$clpCmd = Join-Path $installDirResolved "clp.cmd"
Set-Content -Path $clpCmd -Value $cxpContent -Encoding ASCII
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

# Clean up legacy binary names from before the rename (claude-proxy -> codex-proxy).
foreach ($legacyName in @("claude-proxy.exe")) {
  $legacyPath = Join-Path $installDirResolved $legacyName
  if (Test-Path $legacyPath) {
    $shouldRemove = $false
    if ($legacyName -like "clp*") {
      # clp.exe is always a legacy wrapper/binary in this directory.
      $content = Get-Content -Path $legacyPath -Raw -ErrorAction SilentlyContinue
      if ($content -and ($content -match "claude-proxy" -or $content -match "codex-proxy")) {
        $shouldRemove = $true
      }
    } else {
      # claude-proxy.exe — check if it identifies as codex-proxy.
      try {
        $ver = & $legacyPath --version 2>&1 | Out-String
        if ($ver -match "codex-proxy") { $shouldRemove = $true }
      } catch { }
    }
    if ($shouldRemove) {
      Remove-Item -Force $legacyPath -ErrorAction SilentlyContinue
      Write-Host "Removed legacy: $legacyPath"
    }
  }
}

Write-Host "Installed: $dst"
Write-Host "Shell profile and user PATH checked for install/managed CLI directories (reload attempted):"
Write-Host "  $profilePath"
