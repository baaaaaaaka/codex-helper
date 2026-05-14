$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$tempRoot = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [System.IO.Path]::GetTempPath() }
$root = Join-Path $tempRoot ("skills-smoke-" + [guid]::NewGuid().ToString("N"))
$repo = Join-Path $root "repo"
$config = Join-Path $root "config.json"
$codexDir = Join-Path $root "codex"
$goBin = if ($env:GO) { $env:GO } else { "go" }
if ($env:CODEX_HELPER_BIN) {
    $helperExe = $env:CODEX_HELPER_BIN
    $helperArgs = @()
} else {
    $helperExe = $goBin
    $helperArgs = @("run", "./cmd/codex-proxy")
}

function Write-SmokeFile {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Content
    )
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Path) | Out-Null
    Set-Content -LiteralPath $Path -Value $Content -NoNewline -Encoding UTF8
}

New-Item -ItemType Directory -Force -Path (Join-Path $repo "skills\review\scripts"), $codexDir | Out-Null
& git -C $repo init
& git -C $repo config user.name "Skill Smoke"
& git -C $repo config user.email "skill-smoke@example.invalid"
Write-SmokeFile (Join-Path $repo "skills\review\SKILL.md") @'
---
name: review
description: Review code
---
initial body
'@
Write-SmokeFile (Join-Path $repo "skills\review\scripts\check.sh") @'
#!/bin/sh
echo ok
'@
& git -C $repo add -A
& git -C $repo commit -m "initial skill"

& $helperExe @helperArgs --config $config skills --codex-dir $codexDir add $repo --name acme --ref HEAD --path skills/review --yes
$installed = Join-Path $codexDir "skills\acme__review"
if (!(Test-Path -LiteralPath (Join-Path $installed "SKILL.md"))) { throw "installed SKILL.md missing" }
if (!(Test-Path -LiteralPath (Join-Path $installed "scripts\check.sh"))) { throw "installed script missing" }

Write-SmokeFile (Join-Path $repo "skills\review\SKILL.md") @'
---
name: review
description: Review code
---
remote update
'@
Remove-Item -LiteralPath (Join-Path $repo "skills\review\scripts\check.sh")
Write-SmokeFile (Join-Path $repo "skills\review\agents\openai.yaml") @'
version: 1
'@
& git -C $repo add -A
& git -C $repo commit -m "remote update"

& $helperExe @helperArgs --config $config skills --codex-dir $codexDir sync
if ((Get-Content -Raw -LiteralPath (Join-Path $installed "SKILL.md")) -notmatch "remote update") { throw "synced skill did not update" }
if (!(Test-Path -LiteralPath (Join-Path $installed "agents\openai.yaml"))) { throw "synced agent sidecar missing" }
if (Test-Path -LiteralPath (Join-Path $installed "scripts\check.sh")) { throw "removed script was not pruned" }

Write-SmokeFile (Join-Path $installed "SKILL.md") @'
---
name: review
description: Local smoke edit
---
local smoke edit
'@
Write-SmokeFile (Join-Path $repo "skills\review\SKILL.md") @'
---
name: review
description: Remote smoke edit
---
remote smoke edit
'@
& git -C $repo add -A
& git -C $repo commit -m "remote edit while local is dirty"

$oldNativePreference = $PSNativeCommandUseErrorActionPreference
$PSNativeCommandUseErrorActionPreference = $false
$syncOutput = & $helperExe @helperArgs --config $config skills --codex-dir $codexDir sync 2>&1
$syncStatus = $LASTEXITCODE
$PSNativeCommandUseErrorActionPreference = $oldNativePreference
if ($syncStatus -eq 0) { throw "skills sync unexpectedly succeeded with local edits" }
if (($syncOutput -join "`n") -notmatch "local modifications") { throw "sync failure did not mention local modifications: $syncOutput" }
if ((Get-Content -Raw -LiteralPath (Join-Path $installed "SKILL.md")) -notmatch "local smoke edit") { throw "local edit was overwritten" }

"y`ny`ny`n" | & $helperExe @helperArgs --config $config skills --codex-dir $codexDir push
$branch = (& git -C $repo for-each-ref "--format=%(refname:short)" refs/heads/skill | Select-Object -First 1).Trim()
if (!$branch) { throw "skills push did not create a review branch" }
$pushedSkill = & git -C $repo show "${branch}:skills/review/SKILL.md"
if (($pushedSkill -join "`n") -notmatch "local smoke edit") { throw "review branch did not contain local edit" }
