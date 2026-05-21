[CmdletBinding()]
param(
  [string]$CodexProxy = "",
  [string]$TenantId = "",
  [string]$ClientId = "",
  [string]$ReadClientId = "",
  [Alias("ChatClientId")]
  [string]$WriteClientId = "",
  [switch]$NoOpenControl,
  [switch]$Help
)

$ErrorActionPreference = "Stop"
if ($PSVersionTable.PSVersion.Major -ge 7 -or $PSVersionTable.PSVersion.Major -eq 5) {
  $PSNativeCommandUseErrorActionPreference = $true
}
$ClientIdWasPassed = $PSBoundParameters.ContainsKey("ClientId")
$ReadClientIdWasPassed = $PSBoundParameters.ContainsKey("ReadClientId")
$WriteClientIdWasPassed = $PSBoundParameters.ContainsKey("WriteClientId") -or $PSBoundParameters.ContainsKey("ChatClientId")

function Show-Usage {
  @"
Usage: .\teams-auth-bootstrap.ps1 [options]

Interactively configure Teams Graph auth, run full Teams auth, and bootstrap
the Teams helper service.

Options:
  -CodexProxy PATH   codex-proxy/cxp executable to run
  -TenantId ID       Microsoft Entra tenant ID
  -ClientId ID       Teams Graph public client ID for read and write
  -ReadClientId ID   Teams Graph read-only public client ID
  -WriteClientId ID  Teams Graph write-capable public client ID
                    (-ChatClientId is accepted as an alias)
  -NoOpenControl     pass --no-open-control to teams service bootstrap

Environment defaults:
  CODEX_HELPER_TEAMS_SETUP_CXP
  CODEX_HELPER_TEAMS_SETUP_TENANT_ID or CODEX_HELPER_TEAMS_TENANT_ID
  CODEX_HELPER_TEAMS_SETUP_CLIENT_ID or CODEX_HELPER_TEAMS_CLIENT_ID
  CODEX_HELPER_TEAMS_SETUP_READ_CLIENT_ID or CODEX_HELPER_TEAMS_READ_CLIENT_ID
  CODEX_HELPER_TEAMS_SETUP_WRITE_CLIENT_ID,
  CODEX_HELPER_TEAMS_SETUP_CHAT_CLIENT_ID, or CODEX_HELPER_TEAMS_CLIENT_ID
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
    throw "codex-proxy failed with exit code ${exitCode}: $($ArgsList -join ' ')"
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
if ([string]::IsNullOrWhiteSpace($ReadClientId)) {
  $ReadClientId = if (-not [string]::IsNullOrWhiteSpace($env:CODEX_HELPER_TEAMS_SETUP_READ_CLIENT_ID)) {
    $env:CODEX_HELPER_TEAMS_SETUP_READ_CLIENT_ID
  } else {
    $env:CODEX_HELPER_TEAMS_READ_CLIENT_ID
  }
}
if ([string]::IsNullOrWhiteSpace($WriteClientId)) {
  $WriteClientId = if (-not [string]::IsNullOrWhiteSpace($env:CODEX_HELPER_TEAMS_SETUP_WRITE_CLIENT_ID)) {
    $env:CODEX_HELPER_TEAMS_SETUP_WRITE_CLIENT_ID
  } elseif (-not [string]::IsNullOrWhiteSpace($env:CODEX_HELPER_TEAMS_SETUP_CHAT_CLIENT_ID)) {
    $env:CODEX_HELPER_TEAMS_SETUP_CHAT_CLIENT_ID
  } else {
    $env:CODEX_HELPER_TEAMS_CLIENT_ID
  }
}
if ($ClientIdWasPassed) {
  if (-not $ReadClientIdWasPassed) {
    $ReadClientId = $ClientId
  }
  if (-not $WriteClientIdWasPassed) {
    $WriteClientId = $ClientId
  }
}

Write-Section "STEP 1/4: Configure Teams Graph auth"
Resolve-CodexProxy
$TenantId = Prompt-Required "tenant id" "Microsoft Entra tenant ID" $TenantId
if (-not [string]::IsNullOrWhiteSpace($ClientId)) {
  if ([string]::IsNullOrWhiteSpace($ReadClientId)) {
    $ReadClientId = $ClientId
  }
  if ([string]::IsNullOrWhiteSpace($WriteClientId)) {
    $WriteClientId = $ClientId
  }
}
if ([string]::IsNullOrWhiteSpace($ReadClientId) -and [string]::IsNullOrWhiteSpace($WriteClientId)) {
  $ClientId = Prompt-Required "client id" "Teams Graph public client ID" $ClientId
  $ReadClientId = $ClientId
  $WriteClientId = $ClientId
} else {
  $ReadClientId = Prompt-Required "read client id" "Teams Graph read-only public client ID" $ReadClientId
  $WriteClientId = Prompt-Required "write client id" "Teams Graph write-capable public client ID" $WriteClientId
}

Write-Host "Using: $CodexProxy"
Write-Host "This writes local auth metadata only. Client IDs are not secrets."
Invoke-Cxp @(
  "teams", "auth", "config",
  "--tenant-id", $TenantId,
  "--read-client-id", $ReadClientId,
  "--client-id", $WriteClientId,
  "--file-write-client-id", $WriteClientId,
  "--full-client-id", $WriteClientId
)

Write-Section "STEP 2/4: Sign in with Microsoft device login"
$SplitClientIds = $ReadClientId.Trim() -ne $WriteClientId.Trim()
if ($SplitClientIds) {
  Write-Host "Device login codes may appear next for read-only access and write-capable access."
  Write-Host "Open each shown URL, enter the code, and finish SSO/MFA."
  Invoke-Cxp @("teams", "auth", "read")
} else {
  Write-Host "A device login code may appear next. Open the shown URL, enter the code, and finish SSO/MFA."
}
Invoke-Cxp @("teams", "auth", "full")

Write-Section "STEP 3/4: Verify local Teams auth cache"
if ($SplitClientIds) {
  Invoke-Cxp @("teams", "auth", "read-status")
}
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
