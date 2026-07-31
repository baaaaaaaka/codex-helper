$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$probeRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("codex-helper-teams-isolation-" + [guid]::NewGuid().ToString("N"))
$roots = @{
  HOME = Join-Path $probeRoot "home"
  USERPROFILE = Join-Path $probeRoot "profile"
  APPDATA = Join-Path $probeRoot "appdata"
  LOCALAPPDATA = Join-Path $probeRoot "localappdata"
  XDG_CONFIG_HOME = Join-Path $probeRoot "config"
  XDG_STATE_HOME = Join-Path $probeRoot "state"
  XDG_CACHE_HOME = Join-Path $probeRoot "cache"
  XDG_DATA_HOME = Join-Path $probeRoot "data"
  CODEX_HOME = Join-Path $probeRoot "codex-home"
  CODEX_DIR = Join-Path $probeRoot "codex-home"
  CODEX_CONFIG_DIR = Join-Path $probeRoot "codex-config"
}
$goModCache = (& go env GOMODCACHE).Trim()
$goBuildCache = (& go env GOCACHE).Trim()

function Get-ProbeSnapshot {
  $goToolRoots = @(
    (Join-Path $roots.HOME "go"),
    (Join-Path $roots.USERPROFILE "go"),
    (Join-Path $roots.APPDATA "go"),
    (Join-Path $roots.LOCALAPPDATA "go"),
    (Join-Path $roots.XDG_CONFIG_HOME "go")
  )
  $items = foreach ($root in ($roots.Values | Sort-Object -Unique)) {
    if (-not (Test-Path -LiteralPath $root)) {
      continue
    }
    Get-ChildItem -LiteralPath $root -Force -Recurse |
      Where-Object {
        $path = $_.FullName
        -not ($goToolRoots | Where-Object {
          $path.Equals($_, [System.StringComparison]::OrdinalIgnoreCase) -or
            $path.StartsWith(
              $_ + [System.IO.Path]::DirectorySeparatorChar,
              [System.StringComparison]::OrdinalIgnoreCase
            )
        })
      } |
      Sort-Object FullName | ForEach-Object {
      $hash = if (-not $_.PSIsContainer) {
        (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
      } else {
        ""
      }
      $lastWriteTicks = if (-not $_.PSIsContainer) {
        $_.LastWriteTimeUtc.Ticks
      } else {
        # NTFS directory LastWriteTime can be committed after the creating
        # operation returns, so it is not a stable cross-process sentinel.
        ""
      }
      "{0}|{1}|{2}|{3}|{4}|{5}" -f $_.FullName, $_.Attributes, $_.Length, $_.CreationTimeUtc.Ticks, $lastWriteTicks, $hash
    }
  }
  return ($items -join "`n")
}

try {
  foreach ($path in ($roots.Values | Sort-Object -Unique)) {
    New-Item -ItemType Directory -Force -Path $path | Out-Null
    $sentinelRoot = Join-Path $path "codex-helper"
    New-Item -ItemType Directory -Force -Path $sentinelRoot | Out-Null
    Set-Content -LiteralPath (Join-Path $sentinelRoot "caller-owned-sentinel.txt") -Value "caller-owned sentinel"
  }
  $serviceDir = Join-Path $roots.XDG_CONFIG_HOME "codex-helper\teams"
  New-Item -ItemType Directory -Force -Path $serviceDir | Out-Null
  New-Item -ItemType Directory -Force -Path (Join-Path $roots.XDG_CONFIG_HOME "go") | Out-Null
  Set-Content -LiteralPath (Join-Path $serviceDir "local-supervisor.json") -Value '{"version":1,"enabled":true,"spec":{"Executable":"C:\\opt\\cxp.exe","WorkingDir":"C:\\opt"}}'

  $before = Get-ProbeSnapshot
  foreach ($entry in $roots.GetEnumerator()) {
    [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, "Process")
  }
  # Keep the Go tool's own caches outside the caller-owned probe roots. Without
  # these explicit values, changing USERPROFILE and LOCALAPPDATA makes `go
  # test` populate module and build caches inside the directories this script
  # is intentionally watching.
  [Environment]::SetEnvironmentVariable("GOMODCACHE", $goModCache, "Process")
  [Environment]::SetEnvironmentVariable("GOCACHE", $goBuildCache, "Process")
  [Environment]::SetEnvironmentVariable("GOENV", "off", "Process")
  [Environment]::SetEnvironmentVariable("GOTELEMETRY", "off", "Process")

  $env:CXP_TEAMS_TEST_PRESERVE_USER_DIRS = "1"
  & go test ./internal/cli -count=1 -run '^Test(TeamsServiceBootstrapFailsWhenDuplicateProcessCannotBeRetired|TeamsServiceUpgradeRetiresLocalDuplicateProcessesBeforeRestart|RestartTeamsHelperAfterActivationPendingUsesInstalledPath|TeamsRuntimeSafetyPackageTestMainIsolatesEveryUserDirectoryCI)$'
  $testStatus = $LASTEXITCODE
  $after = Get-ProbeSnapshot

  if ($before -ne $after) {
    Write-Host "Caller-owned directory snapshot differences:"
    Compare-Object ($before -split "`n") ($after -split "`n") | ForEach-Object {
      Write-Host "$($_.SideIndicator) $($_.InputObject)"
    }
    throw "Teams CLI tests modified caller-owned Windows profile/config/state sentinels."
  }
  if ($testStatus -ne 0) {
    throw "Teams service/bootstrap isolation probes failed with exit code $testStatus."
  }
} finally {
  Remove-Item -LiteralPath $probeRoot -Recurse -Force -ErrorAction SilentlyContinue
}
