[CmdletBinding()]
param(
  [string]$CodexProxy = "",
  [string]$TenantId = "",
  [string]$ClientId = "",
  [switch]$NoOpenControl,
  [switch]$Help
)

$ErrorActionPreference = "Stop"
if ($PSVersionTable.PSVersion.Major -ge 7 -or $PSVersionTable.PSVersion.Major -eq 5) {
  $PSNativeCommandUseErrorActionPreference = $true
}

function Show-Usage {
  @"
Usage: .\teams-auth-bootstrap.ps1 [options]

Interactively configure Teams Graph auth, run full Teams auth, and bootstrap
the Teams helper service.

Options:
  -CodexProxy PATH   codex-proxy/cxp executable to run
  -TenantId ID       Microsoft Entra tenant ID
  -ClientId ID       Teams Graph public client ID
  -NoOpenControl     pass --no-open-control to teams service bootstrap

Environment defaults:
  CODEX_HELPER_TEAMS_SETUP_CXP
  CODEX_HELPER_TEAMS_SETUP_TENANT_ID or CODEX_HELPER_TEAMS_TENANT_ID
  CODEX_HELPER_TEAMS_SETUP_CLIENT_ID or CODEX_HELPER_TEAMS_CLIENT_ID
"@
}

function Write-Section([string]$Title) {
  Write-Host ""
  Write-Host "============================================================"
  Write-Host $Title
  Write-Host "============================================================"
  Write-Host ""
}

function Resolve-CodexProxy {
  if (-not [string]::IsNullOrWhiteSpace($script:CodexProxy)) {
    return
  }
  if (-not [string]::IsNullOrWhiteSpace($env:CODEX_HELPER_TEAMS_SETUP_CXP)) {
    $script:CodexProxy = $env:CODEX_HELPER_TEAMS_SETUP_CXP
    return
  }
  $cmd = Get-Command codex-proxy -ErrorAction SilentlyContinue
  if ($cmd) {
    $script:CodexProxy = $cmd.Source
    return
  }
  $cmd = Get-Command cxp -ErrorAction SilentlyContinue
  if ($cmd) {
    $script:CodexProxy = $cmd.Source
    return
  }
  throw "could not find codex-proxy or cxp in PATH; rerun with -CodexProxy PATH"
}

function Prompt-Required([string]$Name, [string]$Prompt, [string]$Value) {
  while ([string]::IsNullOrWhiteSpace($Value)) {
    $Value = Read-Host $Prompt
    if ([string]::IsNullOrWhiteSpace($Value) -and [Console]::IsInputRedirected) {
      throw "$Name is required"
    }
  }
  return $Value
}

function Invoke-Cxp([string[]]$ArgsList) {
  $global:LASTEXITCODE = $null
  & $script:CodexProxy @ArgsList
  $exitCode = $LASTEXITCODE
  if ($null -ne $exitCode -and $exitCode -ne 0) {
    throw "codex-proxy failed with exit code $LASTEXITCODE: $($ArgsList -join ' ')"
  }
}

if ($MyInvocation.BoundParameters.ContainsKey("Help")) {
  Show-Usage
  exit 0
}

if ([string]::IsNullOrWhiteSpace($TenantId)) {
  $TenantId = if (-not [string]::IsNullOrWhiteSpace($env:CODEX_HELPER_TEAMS_SETUP_TENANT_ID)) {
    $env:CODEX_HELPER_TEAMS_SETUP_TENANT_ID
  } else {
    $env:CODEX_HELPER_TEAMS_TENANT_ID
  }
}
if ([string]::IsNullOrWhiteSpace($ClientId)) {
  $ClientId = if (-not [string]::IsNullOrWhiteSpace($env:CODEX_HELPER_TEAMS_SETUP_CLIENT_ID)) {
    $env:CODEX_HELPER_TEAMS_SETUP_CLIENT_ID
  } else {
    $env:CODEX_HELPER_TEAMS_CLIENT_ID
  }
}

Write-Section "STEP 1/4: Configure Teams Graph auth"
Resolve-CodexProxy
$TenantId = Prompt-Required "tenant id" "Microsoft Entra tenant ID" $TenantId
$ClientId = Prompt-Required "client id" "Teams Graph public client ID" $ClientId

Write-Host "Using: $CodexProxy"
Write-Host "This writes local auth metadata only. The client ID is not a secret."
Invoke-Cxp @(
  "teams", "auth", "config",
  "--tenant-id", $TenantId,
  "--read-client-id", $ClientId,
  "--client-id", $ClientId,
  "--file-write-client-id", $ClientId,
  "--full-client-id", $ClientId
)

Write-Section "STEP 2/4: Sign in with Microsoft device login"
Write-Host "A device login code may appear next. Open the shown URL, enter the code, and finish SSO/MFA."
Invoke-Cxp @("teams", "auth", "full")

Write-Section "STEP 3/4: Verify local Teams auth cache"
Invoke-Cxp @("teams", "auth", "full-status")

Write-Section "STEP 4/4: Bootstrap the Teams helper service"
Write-Host "If Windows or WSL asks for permission, follow the prompt. When bootstrap asks for confirmation, type yes and press Enter."
$bootstrapArgs = @("teams", "service", "bootstrap")
if ($NoOpenControl) {
  $bootstrapArgs += "--no-open-control"
}
Invoke-Cxp $bootstrapArgs

Write-Section "DONE"
Write-Host "Teams auth and service bootstrap completed."
Write-Host "Next: open the Teams control chat shown by bootstrap and send help."
