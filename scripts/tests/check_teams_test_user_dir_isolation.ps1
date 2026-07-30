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

function Get-ProbeSnapshot {
  $goConfigRoot = Join-Path $roots.XDG_CONFIG_HOME "go"
  $items = foreach ($root in ($roots.Values | Sort-Object -Unique)) {
    if (-not (Test-Path -LiteralPath $root)) {
      continue
    }
    Get-ChildItem -LiteralPath $root -Force -Recurse |
      Where-Object { -not $_.FullName.StartsWith($goConfigRoot, [System.StringComparison]::OrdinalIgnoreCase) } |
      Sort-Object FullName | ForEach-Object {
      $hash = if (-not $_.PSIsContainer) {
        (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
      } else {
        ""
      }
      "{0}|{1}|{2}|{3}|{4}|{5}" -f $_.FullName, $_.Attributes, $_.Length, $_.CreationTimeUtc.Ticks, $_.LastWriteTimeUtc.Ticks, $hash
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
  [Environment]::SetEnvironmentVariable("GOENV", "off", "Process")
  [Environment]::SetEnvironmentVariable("GOTELEMETRY", "off", "Process")

  & go test ./internal/cli -count=1 -run '^Test(TeamsServiceBootstrapFailsWhenDuplicateProcessCannotBeRetired|TeamsRuntimeSafetyPackageTestMainIsolatesEveryUserDirectoryCI)$'
  $testStatus = $LASTEXITCODE
  $after = Get-ProbeSnapshot

  if ($before -ne $after) {
    throw "Teams CLI tests modified caller-owned Windows profile/config/state sentinels."
  }
  if ($testStatus -ne 0) {
    throw "Teams service/bootstrap isolation probes failed with exit code $testStatus."
  }
} finally {
  Remove-Item -LiteralPath $probeRoot -Recurse -Force -ErrorAction SilentlyContinue
}
