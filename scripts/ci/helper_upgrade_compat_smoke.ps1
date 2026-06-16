param(
  [string]$Repo = $env:REPO,
  [string]$OldTag = $env:OLD_TAG,
  [string]$TargetTag = $(if ($env:TARGET_TAG) { $env:TARGET_TAG } elseif ($env:TAG) { $env:TAG } else { $env:GITHUB_REF_NAME })
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

if ([string]::IsNullOrWhiteSpace($Repo) -or [string]::IsNullOrWhiteSpace($OldTag) -or [string]::IsNullOrWhiteSpace($TargetTag)) {
  throw "REPO, OLD_TAG, and TARGET_TAG are required"
}

if ($OldTag -eq $TargetTag) {
  Write-Host "old tag equals target tag ($OldTag); nothing to upgrade"
  exit 0
}

function Version-NoV([string]$Tag) {
  return $Tag.TrimStart("v")
}

function Asset-Name([string]$Tag) {
  return "codex-proxy_$(Version-NoV $Tag)_windows_amd64.exe"
}

function Invoke-Retry([int]$Attempts, [int]$SleepSeconds, [scriptblock]$Action) {
  for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
    try {
      & $Action
      return
    } catch {
      if ($attempt -eq $Attempts) {
        throw
      }
      Write-Warning ("command failed (attempt {0}/{1}), retrying in {2}s: {3}" -f $attempt, $Attempts, $SleepSeconds, $_.Exception.Message)
      Start-Sleep -Seconds $SleepSeconds
    }
  }
}

function Download-Binary([string]$Tag, [string]$Destination) {
  $asset = Asset-Name $Tag
  $url = "https://github.com/$Repo/releases/download/$Tag/$asset"
  $tmp = Join-Path ([IO.Path]::GetTempPath()) ([IO.Path]::GetRandomFileName())
  try {
    Invoke-Retry 5 5 {
      Invoke-WebRequest -Uri $url -OutFile $tmp -UseBasicParsing
    }
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Destination) | Out-Null
    Move-Item -Force -LiteralPath $tmp -Destination $Destination
  } finally {
    Remove-Item -Force -LiteralPath $tmp -ErrorAction SilentlyContinue
  }
}

function Assert-Version([string]$Path, [string]$Tag) {
  $out = (& $Path --version | Out-String)
  $out | Write-Host
  if ($out -notmatch [regex]::Escape((Version-NoV $Tag))) {
    throw "$Path reported unexpected version for $Tag`: $out"
  }
}

function Write-CXPShim([string]$Path, [string]$Body) {
  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Path) | Out-Null
  Set-Content -LiteralPath $Path -NoNewline -Encoding ASCII -Value $Body
}

function Copy-Binary([string]$Source, [string]$Destination) {
  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Destination) | Out-Null
  Copy-Item -Force -LiteralPath $Source -Destination $Destination
}

function Expected-CXPShimBody() {
  return "@echo off`r`n`"%~dp0codex-proxy.exe`" %*`r`n"
}

function Configure-IsolatedEnvironment([string]$Root) {
  $env:USERPROFILE = Join-Path $Root "home"
  $env:APPDATA = Join-Path $env:USERPROFILE "AppData\Roaming"
  $env:LOCALAPPDATA = Join-Path $env:USERPROFILE "AppData\Local"
  $env:TEMP = Join-Path $Root "temp"
  $env:TMP = $env:TEMP
  $env:CODEX_HOME = Join-Path $env:USERPROFILE ".codex"
  $env:CODEX_PROXY_SKIP_BUILTIN_SKILLS = "1"
  $env:CODEX_HELPER_TEAMS_TENANT_ID = "ci-tenant"
  $env:CODEX_HELPER_TEAMS_CLIENT_ID = "ci-client"
  $env:CODEX_HELPER_TEAMS_READ_CLIENT_ID = "ci-read-client"
  $env:CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID = "ci-file-client"
  $env:CODEX_HELPER_TEAMS_TOKEN_CACHE = Join-Path $env:LOCALAPPDATA "teams-token.json"
  $env:CODEX_HELPER_TEAMS_READ_TOKEN_CACHE = Join-Path $env:LOCALAPPDATA "teams-read-token.json"
  $env:CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE = Join-Path $env:LOCALAPPDATA "teams-file-token.json"
  New-Item -ItemType Directory -Force -Path $env:USERPROFILE,$env:APPDATA,$env:LOCALAPPDATA,$env:TEMP,$env:CODEX_HOME | Out-Null
}

function Run-UpgradeScenario([string]$Scenario, [string]$SeedMode) {
  $scenarioRoot = Join-Path $script:BaseRoot $Scenario
  Remove-Item -Recurse -Force -LiteralPath $scenarioRoot -ErrorAction SilentlyContinue
  New-Item -ItemType Directory -Force -Path $scenarioRoot | Out-Null
  Configure-IsolatedEnvironment $scenarioRoot

  $installDir = Join-Path $env:USERPROFILE ".local\bin"
  $helper = Join-Path $installDir "codex-proxy.exe"
  $cxp = Join-Path $installDir "cxp.cmd"
  $runner = Join-Path $scenarioRoot "runner\codex-proxy.exe"
  $currentRunner = Join-Path $scenarioRoot "current-runner\codex-proxy.exe"

  Write-Host "helper upgrade compatibility smoke: scenario=$Scenario mode=$SeedMode os=windows repo=$Repo old=$OldTag target=$TargetTag"

  switch ($SeedMode) {
    "current-missing-cxp" {
      Download-Binary $TargetTag $helper
      Download-Binary $TargetTag $currentRunner
      Remove-Item -Force -LiteralPath $cxp -ErrorAction SilentlyContinue
      Assert-Version $helper $TargetTag
      if (Test-Path -LiteralPath $cxp) { throw "cxp.cmd should be missing before repair" }
      Invoke-Retry 5 10 {
        & $currentRunner upgrade --repo $Repo --version $TargetTag --install-path $helper
      }
    }
    "missing-cxp" {
      Download-Binary $OldTag $runner
      Download-Binary $OldTag $helper
      Remove-Item -Force -LiteralPath $cxp -ErrorAction SilentlyContinue
      Assert-Version $helper $OldTag
      if (Test-Path -LiteralPath $cxp) { throw "cxp.cmd should be missing before upgrade" }
      Invoke-Retry 5 10 {
        & $runner upgrade --repo $Repo --version $TargetTag --install-path $helper
      }
      Assert-Version $helper $TargetTag
      if (!(Test-Path -LiteralPath $cxp)) {
        Copy-Binary $helper $currentRunner
        Invoke-Retry 5 10 {
          & $currentRunner upgrade --repo $Repo --version $TargetTag --install-path $helper
        }
      }
    }
    "existing-cmd" {
      Download-Binary $OldTag $runner
      Download-Binary $OldTag $helper
      Write-CXPShim $cxp (Expected-CXPShimBody)
      Assert-Version $helper $OldTag
      Assert-Version $cxp $OldTag
      Invoke-Retry 5 10 {
        & $runner upgrade --repo $Repo --version $TargetTag --install-path $helper
      }
    }
    "stale-helper-cmd" {
      Download-Binary $OldTag $runner
      Download-Binary $OldTag $helper
      Write-CXPShim $cxp "@echo off`r`n`"$helper`" %*`r`n"
      Assert-Version $helper $OldTag
      Assert-Version $cxp $OldTag
      Invoke-Retry 5 10 {
        & $runner upgrade --repo $Repo --version $TargetTag --install-path $helper
      }
      Assert-Version $helper $TargetTag
      Copy-Binary $helper $currentRunner
      Invoke-Retry 5 10 {
        & $currentRunner upgrade --repo $Repo --version $TargetTag --install-path $helper
      }
    }
    default {
      throw "unknown helper upgrade compatibility seed mode: $SeedMode"
    }
  }

  Assert-Version $helper $TargetTag
  Assert-Version $cxp $TargetTag
  $shimBody = Get-Content -Raw -LiteralPath $cxp
  if ($shimBody -ne (Expected-CXPShimBody)) {
    throw "cxp.cmd was not repaired to the canonical relative shim. Actual:`n$shimBody"
  }

  $secondTarget = Join-Path $scenarioRoot "second-hop\codex-proxy.exe"
  Download-Binary $OldTag $secondTarget
  Assert-Version $secondTarget $OldTag

  Write-Host "helper upgrade compatibility smoke: scenario=$Scenario second-hop via cxp.cmd"
  Invoke-Retry 5 10 {
    & $cxp upgrade --repo $Repo --version $TargetTag --install-path $secondTarget
  }
  Assert-Version $secondTarget $TargetTag
  Assert-Version $helper $TargetTag
  Assert-Version $cxp $TargetTag
}

$safeOld = $OldTag -replace '[^A-Za-z0-9._-]', '_'
$safeTarget = $TargetTag -replace '[^A-Za-z0-9._-]', '_'
$root = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [IO.Path]::GetTempPath() }
$script:BaseRoot = Join-Path $root "codex-helper-upgrade-compat-$safeOld-to-$safeTarget"
Remove-Item -Recurse -Force -LiteralPath $script:BaseRoot -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $script:BaseRoot | Out-Null

Run-UpgradeScenario "existing-cxp-cmd" "existing-cmd"
Run-UpgradeScenario "stale-helper-cxp-cmd" "stale-helper-cmd"
Run-UpgradeScenario "missing-cxp-cmd" "missing-cxp"
Run-UpgradeScenario "current-helper-missing-cxp-cmd" "current-missing-cxp"

Write-Host "helper upgrade compatibility smoke passed"
