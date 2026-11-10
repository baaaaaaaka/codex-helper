param([string]$Repo = $env:REPO, [string]$OldTag = $env:OLD_TAG, [string]$TargetTag = $env:TARGET_TAG)
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$env:REPO = $Repo
$env:OLD_TAG = $OldTag
$env:TARGET_TAG = $TargetTag
python (Join-Path $PSScriptRoot "sqlite_upgrade_artifacts.py")
if ($LASTEXITCODE -ne 0) { throw "SQLite artifact upgrade failed" }
