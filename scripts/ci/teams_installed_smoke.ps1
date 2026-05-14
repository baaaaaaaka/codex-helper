$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$runnerTemp = $env:RUNNER_TEMP
if (-not $runnerTemp) {
  $runnerTemp = [System.IO.Path]::GetTempPath()
}
$base = Join-Path $runnerTemp "codex-helper-teams-installed-smoke"
if (Test-Path $base) {
  Remove-Item -Recurse -Force $base
}
New-Item -ItemType Directory -Force -Path (Join-Path $base "bin") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $base "codex-home") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $base "cache") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $base "appdata") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $base "localappdata") | Out-Null

$bin = Join-Path $base "bin\codex-proxy.exe"
go build -trimpath -o $bin ./cmd/codex-proxy

$env:CODEX_HOME = Join-Path $base "codex-home"
$env:APPDATA = Join-Path $base "appdata"
$env:LOCALAPPDATA = Join-Path $base "localappdata"
$env:NO_COLOR = "1"
$env:CODEX_HELPER_TEAMS_PROFILE = "ci-installed-smoke"
$env:CODEX_HELPER_TEAMS_TENANT_ID = "ci-tenant"
$env:CODEX_HELPER_TEAMS_CLIENT_ID = "ci-client"
$env:CODEX_HELPER_TEAMS_READ_CLIENT_ID = "ci-read-client"
$env:CODEX_HELPER_TEAMS_FULL_CLIENT_ID = "ci-full-client"
$env:CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID = "ci-file-client"
$env:CODEX_HELPER_TEAMS_TOKEN_CACHE = Join-Path $base "cache\teams-token.json"
$env:CODEX_HELPER_TEAMS_READ_TOKEN_CACHE = Join-Path $base "cache\teams-read-token.json"
$env:CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE = Join-Path $base "cache\teams-file-token.json"

function Invoke-Smoke {
  param(
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][string[]]$CommandArgs
  )
  $out = Join-Path $base "$Name.txt"
  $nativePreference = $PSNativeCommandUseErrorActionPreference
  $PSNativeCommandUseErrorActionPreference = $false
  try {
    $commandOutput = & $bin @CommandArgs 2>&1
    $exitCode = $LASTEXITCODE
  } finally {
    $PSNativeCommandUseErrorActionPreference = $nativePreference
  }
  $commandOutput | Set-Content -LiteralPath $out
  $commandOutput | ForEach-Object { Write-Host $_ }
  if ($exitCode -ne 0) {
    throw "smoke command $Name failed with exit code $exitCode"
  }
  return $out
}

function Assert-Contains {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][string]$Text
  )
  $content = Get-Content -LiteralPath $Path -Raw
  if (-not $content.Contains($Text)) {
    throw "expected $Path to contain: $Text`n---- $Path ----`n$content"
  }
}

& $bin --version | Out-Null

$setup = Invoke-Smoke -Name "setup" -CommandArgs @("teams", "setup")
$status = Invoke-Smoke -Name "status" -CommandArgs @("teams", "status")
$control = Invoke-Smoke -Name "control-print" -CommandArgs @("teams", "control", "--print")
$doctor = Invoke-Smoke -Name "doctor" -CommandArgs @("teams", "doctor")
$serviceDoctor = Invoke-Smoke -Name "service-doctor" -CommandArgs @("teams", "service", "doctor")

Assert-Contains -Path $setup -Text "Teams setup checklist"
Assert-Contains -Path $status -Text "Teams status"
Assert-Contains -Path $status -Text "Control chat: unavailable"
Assert-Contains -Path $control -Text "Teams control chat: unavailable"
Assert-Contains -Path $doctor -Text "Teams doctor"
Assert-Contains -Path $doctor -Text "Graph: not checked"
Assert-Contains -Path $serviceDoctor -Text "Teams service backend:"
