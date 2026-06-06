$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$tempRoot = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [System.IO.Path]::GetTempPath() }
$root = Join-Path $tempRoot ("skills-migration-smoke-" + [guid]::NewGuid().ToString("N"))
$repo = Join-Path $root "repo"
$config = Join-Path $root "config.json"
$codexDir = Join-Path $root "codex"
$homeDir = Join-Path $root "home"
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

function Restore-SmokeEnv {
    foreach ($name in $script:smokeEnv.Keys) {
        if ($null -eq $script:smokeEnv[$name]) {
            Remove-Item -Path ("Env:" + $name) -ErrorAction SilentlyContinue
        } else {
            Set-Item -Path ("Env:" + $name) -Value $script:smokeEnv[$name]
        }
    }
}

$script:smokeEnv = @{
    HOME = $env:HOME
    USERPROFILE = $env:USERPROFILE
    XDG_CONFIG_HOME = $env:XDG_CONFIG_HOME
    XDG_CACHE_HOME = $env:XDG_CACHE_HOME
    LOCALAPPDATA = $env:LOCALAPPDATA
}

try {
    $env:HOME = $homeDir
    $env:USERPROFILE = $homeDir
    $env:XDG_CONFIG_HOME = Join-Path $homeDir ".config"
    $env:XDG_CACHE_HOME = Join-Path $homeDir ".cache"
    $env:LOCALAPPDATA = Join-Path $homeDir "AppData\Local"

    New-Item -ItemType Directory -Force -Path (Join-Path $repo "skills\review"), $codexDir, $homeDir | Out-Null
    $agentsRoot = Join-Path $homeDir ".agents\skills"

    Write-SmokeFile (Join-Path $repo "skills\review\SKILL.md") @'
---
name: review
description: Review code
---
legacy body
'@
    & git -C $repo init
    & git -C $repo config user.name "Skill Migration Smoke"
    & git -C $repo config user.email "skill-migration-smoke@example.invalid"
    & git -C $repo add -A
    & git -C $repo commit -m "initial legacy skill"

    & $helperExe @helperArgs --config $config skills --codex-dir $codexDir add $repo --name acme --ref HEAD --path skills/review --target codex-home --yes
    $legacy = Join-Path $codexDir "skills\acme__review"
    $agents = Join-Path $agentsRoot "acme__review"
    if (!(Test-Path -LiteralPath (Join-Path $legacy "SKILL.md"))) { throw "legacy skill missing before migration" }
    if (Test-Path -LiteralPath $agents) { throw "agents skill unexpectedly exists before migration" }

    $dryRunOutput = & $helperExe @helperArgs --config $config skills --codex-dir $codexDir migrate --dry-run
    if (($dryRunOutput -join "`n") -notmatch "Migration: dry_run") { throw "migration dry run did not report dry_run: $dryRunOutput" }
    if (!(Test-Path -LiteralPath (Join-Path $legacy "SKILL.md"))) { throw "dry run removed legacy skill" }
    if (Test-Path -LiteralPath $agents) { throw "dry run created agents skill" }

    $listOutput = & $helperExe @helperArgs --config $config skills --codex-dir $codexDir list
    if (($listOutput -join "`n") -notmatch "target agents") { throw "self-heal list did not retarget to agents: $listOutput" }
    if (!(Test-Path -LiteralPath (Join-Path $agents "SKILL.md"))) { throw "agents skill missing after self-heal migration" }
    if (Test-Path -LiteralPath $legacy) { throw "legacy direct skill still visible after migration" }
    $backupRoot = Join-Path $codexDir "skills\.cxp-migrated-backups"
    if (!(Get-ChildItem -LiteralPath $backupRoot -Recurse -Filter SKILL.md -ErrorAction SilentlyContinue | Select-Object -First 1)) {
        throw "legacy backup SKILL.md missing"
    }

    Write-SmokeFile (Join-Path $repo "skills\review\SKILL.md") @'
---
name: review
description: Review code
---
post-migration sync
'@
    & git -C $repo add -A
    & git -C $repo commit -m "post migration sync"

    & $helperExe @helperArgs --config $config skills --codex-dir $codexDir sync acme
    if ((Get-Content -Raw -LiteralPath (Join-Path $agents "SKILL.md")) -notmatch "post-migration sync") { throw "post-migration sync did not update agents skill" }
    $doctorOutput = & $helperExe @helperArgs --config $config skills --codex-dir $codexDir doctor acme
    if (($doctorOutput -join "`n") -notmatch [regex]::Escape((Join-Path $homeDir ".agents\skills"))) { throw "doctor did not show agents root: $doctorOutput" }

    Write-SmokeFile (Join-Path $repo "skills\review\SKILL.md") @'
---
name: review
description: Review code
---
dirty legacy body
'@
    & git -C $repo add -A
    & git -C $repo commit -m "dirty legacy skill"

    & $helperExe @helperArgs --config $config skills --codex-dir $codexDir add $repo --name dirty --ref HEAD --path skills/review --target codex-home --yes
    $dirtyLegacy = Join-Path $codexDir "skills\dirty__review"
    $dirtyAgents = Join-Path $agentsRoot "dirty__review"
    if (!(Test-Path -LiteralPath (Join-Path $dirtyLegacy "SKILL.md"))) { throw "dirty legacy skill missing before migration" }
    Write-SmokeFile (Join-Path $dirtyLegacy "SKILL.md") @'
---
name: review
description: Local dirty edit
---
local dirty edit
'@

    $migrateOutput = & $helperExe @helperArgs --config $config skills --codex-dir $codexDir migrate --yes
    if (($migrateOutput -join "`n") -notmatch "local_modified") { throw "dirty migration did not report local_modified: $migrateOutput" }
    if (!(Test-Path -LiteralPath (Join-Path $dirtyLegacy "SKILL.md"))) { throw "dirty legacy skill was removed" }
    if (Test-Path -LiteralPath $dirtyAgents) { throw "dirty agents skill should not exist" }
    if ((Get-Content -Raw -LiteralPath (Join-Path $dirtyLegacy "SKILL.md")) -notmatch "local dirty edit") { throw "dirty local edit was overwritten" }

    $fixtureConfigDir = Join-Path $root "old-shape-config"
    $fixtureConfig = Join-Path $fixtureConfigDir "proxy.json"
    $fixtureSkillConfig = Join-Path $fixtureConfigDir "skill-subscriptions.json"
    $fixtureCodex = Join-Path $root "old-shape-codex"
    $fixtureHome = Join-Path $root "old-shape-home"
    New-Item -ItemType Directory -Force -Path $fixtureConfigDir, $fixtureCodex, $fixtureHome | Out-Null
    $env:HOME = $fixtureHome
    $env:USERPROFILE = $fixtureHome
    $env:XDG_CONFIG_HOME = Join-Path $fixtureHome ".config"
    $env:XDG_CACHE_HOME = Join-Path $fixtureHome ".cache"
    $env:LOCALAPPDATA = Join-Path $fixtureHome "AppData\Local"
    & $helperExe @helperArgs --config $fixtureConfig skills --codex-dir $fixtureCodex add $repo --name oldshape --ref HEAD --path skills/review --target codex-home --yes
    $oldShapeConfig = Get-Content -Raw -LiteralPath $fixtureSkillConfig | ConvertFrom-Json
    foreach ($source in $oldShapeConfig.sources) {
        $source.PSObject.Properties.Remove("target_kind")
        $source.PSObject.Properties.Remove("target_root")
    }
    $oldShapeConfig | ConvertTo-Json -Depth 32 | Set-Content -LiteralPath $fixtureSkillConfig -NoNewline -Encoding UTF8
    $oldShapeLegacy = Join-Path $fixtureCodex "skills\oldshape__review"
    $oldShapeAgents = Join-Path $fixtureHome ".agents\skills\oldshape__review"
    if (!(Test-Path -LiteralPath (Join-Path $oldShapeLegacy "SKILL.md"))) { throw "old-shape legacy skill missing before migration" }
    $oldShapeList = & $helperExe @helperArgs --config $fixtureConfig skills --codex-dir $fixtureCodex list
    if (($oldShapeList -join "`n") -notmatch "target agents") { throw "old-shape self-heal did not retarget to agents: $oldShapeList" }
    if (!(Test-Path -LiteralPath (Join-Path $oldShapeAgents "SKILL.md"))) { throw "old-shape agents skill missing after migration" }
    if (Test-Path -LiteralPath $oldShapeLegacy) { throw "old-shape legacy direct skill still visible after migration" }
} finally {
    Restore-SmokeEnv
    Remove-Item -Recurse -Force -LiteralPath $root -ErrorAction SilentlyContinue
}
