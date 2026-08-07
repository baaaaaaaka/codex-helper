param(
  [Parameter(Mandatory = $true)]
  [string]$Helper
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

if (!(Test-Path -LiteralPath $Helper -PathType Leaf)) {
  throw "helper does not exist: $Helper"
}

function Get-ProxySnapshot {
  $internetSettings = Get-ItemProperty -Path "HKCU:\Software\Microsoft\Windows\CurrentVersion\Internet Settings" -ErrorAction SilentlyContinue
  [ordered]@{
    WinHTTP = (netsh winhttp show proxy | Out-String)
    ProxyEnable = if ($null -ne $internetSettings) { [string]$internetSettings.ProxyEnable } else { "<missing>" }
    ProxyServer = if ($null -ne $internetSettings) { [string]$internetSettings.ProxyServer } else { "<missing>" }
    AutoConfigURL = if ($null -ne $internetSettings) { [string]$internetSettings.AutoConfigURL } else { "<missing>" }
  } | ConvertTo-Json -Compress
}

function Get-NewDesktopProcesses([hashtable]$Existing) {
  @(Get-Process -Name @("ChatGPT", "Codex") -ErrorAction SilentlyContinue | Where-Object { -not $Existing.ContainsKey($_.Id) })
}

function Stop-DesktopProcesses([hashtable]$Existing) {
  for ($i = 0; $i -lt 50; $i++) {
    # Re-scan on every iteration: ChatGPT can create a Codex child after the
    # first process snapshot. Kill only processes that were not present before
    # this smoke so a hosted runner's unrelated desktop process is preserved.
    $alive = @(Get-Process -Name @("ChatGPT", "Codex") -ErrorAction SilentlyContinue | Where-Object { -not $Existing.ContainsKey($_.Id) })
    if ($alive.Count -eq 0) { return $true }
    foreach ($process in $alive) {
      try { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue } catch { }
    }
    Start-Sleep -Milliseconds 200
  }
  return $false
}

function Wait-File([string]$Path, [System.Diagnostics.Process]$Process) {
  for ($i = 0; $i -lt 100; $i++) {
    if (Test-Path -LiteralPath $Path -PathType Leaf) { return }
    if ($Process.HasExited) { throw "fixture process exited before creating $Path" }
    Start-Sleep -Milliseconds 100
  }
  throw "timed out waiting for $Path"
}

$root = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [IO.Path]::GetTempPath() }
$base = Join-Path $root "codex-managed-chatgpt-smoke"
$managedRoot = Join-Path $base "managed"
$work = Join-Path $base "work"
$codexHome = Join-Path $base "codex-home"
$config = Join-Path $base "config.json"
$out = Join-Path $base "app-launch.out"
$proxyPortFile = Join-Path $base "proxy.port"
$proxyLog = Join-Path $base "proxy.log"
$proxyExe = Join-Path $base "recording-proxy.exe"
$fakeExe = Join-Path $base "fake-chatgpt.exe"
$fakeLog = Join-Path $base "fake-child.log"
$instanceId = "ci-recording"
$profileId = "ci-recording-profile"
$nonce = [guid]::NewGuid().ToString("N")
$desktopProcessNames = @("ChatGPT", "Codex")
$existingProcessIds = @{}
Get-Process -Name $desktopProcessNames -ErrorAction SilentlyContinue | ForEach-Object { $existingProcessIds[$_.Id] = $true }
$launchedProcesses = @()
$firstLaunchProcesses = @()
$proxyProcess = $null
$oldBackend = $env:CXP_WINDOWS_APP_BACKEND
$oldManagedRoot = $env:CXP_WINDOWS_MANAGED_ROOT
$oldRuntimeDisable = $env:CXP_RUNTIME_DISABLE
$oldChildLog = $env:CXP_TEST_CHILD_LOG
$oldNonce = $env:CXP_TEST_NONCE
$oldNoProxy = $env:NO_PROXY
$oldLowerNoProxy = $env:no_proxy
$beforeProxy = Get-ProxySnapshot

try {
  Remove-Item -Recurse -Force -LiteralPath $base -ErrorAction SilentlyContinue
  New-Item -ItemType Directory -Force -Path $work, $managedRoot, $codexHome | Out-Null

  # The fixture is a local process. No SSH, GitHub token, ChatGPT auth, or
  # third-party credential is involved in reusing this proxy instance.
  & go build -o $proxyExe .\scripts\ci\windows_managed_recording_proxy
  if ($LASTEXITCODE -ne 0) { throw "building recording proxy fixture failed" }
  $proxyProcess = Start-Process -FilePath $proxyExe -ArgumentList @("--port-file", $proxyPortFile, "--log", $proxyLog, "--instance-id", $instanceId) -PassThru -WindowStyle Hidden
  Wait-File $proxyPortFile $proxyProcess
  $proxyPort = [int](Get-Content -Raw -LiteralPath $proxyPortFile)
  $proxyURL = "http://127.0.0.1:$proxyPort"

  $configObject = [ordered]@{
    version = 6
    proxyEnabled = $true
    profiles = @([ordered]@{ id = $profileId; name = "CI recording proxy"; host = "fixture.invalid"; port = 22; user = "ci" })
    instances = @([ordered]@{ id = $instanceId; profileId = $profileId; kind = "daemon"; httpPort = $proxyPort; daemonPid = $proxyProcess.Id; lastSeenAt = [DateTime]::UtcNow.ToString("o") })
  }
  $configObject | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $config -Encoding UTF8

  $env:CXP_WINDOWS_APP_BACKEND = "managed-only"
  $env:CXP_WINDOWS_MANAGED_ROOT = $managedRoot
  $env:CXP_RUNTIME_DISABLE = "1"
  # Deliberately inherit a bypass for the fake child's hostname. The managed
  # launcher must clear it in the child environment; otherwise this request
  # would go direct and the recording proxy assertion below would be a real
  # negative test rather than a proxy-looking fake pass.
  $env:NO_PROXY = "cxp-managed-child.invalid"
  $env:no_proxy = "cxp-managed-child.invalid"
  $env:CXP_TEST_NONCE = $nonce

  try {
    & $Helper --config $config app --cwd $work --codex-dir $codexHome *> $out
  } catch {
    $appOut = if (Test-Path -LiteralPath $out) { Get-Content -Raw -LiteralPath $out } else { "" }
    throw "managed install/launch failed`napp output:`n$appOut`nerror:`n$($_.Exception.Message)"
  }
  for ($i = 0; $i -lt 100; $i++) {
    $firstLaunchProcesses = Get-NewDesktopProcesses $existingProcessIds
    if ($firstLaunchProcesses.Count -gt 0) { break }
    Start-Sleep -Milliseconds 100
  }
  if ($firstLaunchProcesses.Count -eq 0) {
    throw "managed install did not launch a desktop process"
  }
  if (-not (Stop-DesktopProcesses $existingProcessIds)) {
    throw "managed install left a ChatGPT/Codex process running after cleanup"
  }

  $statePath = Join-Path $managedRoot "current.json"
  if (!(Test-Path -LiteralPath $statePath -PathType Leaf)) { throw "managed backend did not publish current.json" }
  $state = Get-Content -Raw -LiteralPath $statePath | ConvertFrom-Json
  $runtime = Join-Path $managedRoot ([string]$state.runtime)
  $managedExe = Join-Path $runtime "app\ChatGPT.exe"
  if (!(Test-Path -LiteralPath $managedExe -PathType Leaf)) { throw "managed runtime has no ChatGPT.exe: $managedExe" }
  if ($managedExe -like "*\WindowsApps\*") { throw "managed ChatGPT executable unexpectedly resides below WindowsApps: $managedExe" }

  $rootHelp = (& $Helper --help | Out-String)
  if ($rootHelp -notmatch "--upgrade-codex-app") { throw "built helper root help does not expose --upgrade-codex-app" }
  $stateBeforeUpgrade = Get-Content -Raw -LiteralPath $statePath
  $oldRuntime = $runtime
  $oldManagedExe = $managedExe
  $proxyLogBeforeUpgrade = if (Test-Path -LiteralPath $proxyLog -PathType Leaf) { Get-Content -Raw -LiteralPath $proxyLog } else { "" }
  $upgradeOut = Join-Path $base "app-upgrade.out"
  try {
    & $Helper --config $config --upgrade-codex-app "CI recording proxy" *> $upgradeOut
  } catch {
    $upgradeText = if (Test-Path -LiteralPath $upgradeOut) { Get-Content -Raw -LiteralPath $upgradeOut } else { "" }
    throw "managed app update command failed`nupdate output:`n$upgradeText`nerror:`n$($_.Exception.Message)"
  }
  $upgradeText = Get-Content -Raw -LiteralPath $upgradeOut
  $updatePublished = $upgradeText -match "Codex desktop app upgraded:"
  if (-not $updatePublished -and $upgradeText -notmatch "Codex desktop app is already up to date") {
    throw "managed app update did not report a verified package result`n$upgradeText"
  }
  $stateAfterUpgrade = Get-Content -Raw -LiteralPath $statePath
  if (-not $updatePublished -and $stateAfterUpgrade -ne $stateBeforeUpgrade) {
    throw "unchanged managed app update rewrote current.json"
  }
  if ($updatePublished -and $stateAfterUpgrade -eq $stateBeforeUpgrade) {
    throw "managed app update reported a publish without changing current.json"
  }
  if ($updatePublished -and !(Test-Path -LiteralPath $oldManagedExe -PathType Leaf)) {
    throw "managed app update removed the old runtime: $oldRuntime"
  }
  $state = Get-Content -Raw -LiteralPath $statePath | ConvertFrom-Json
  $runtime = Join-Path $managedRoot ([string]$state.runtime)
  $managedExe = Join-Path $runtime "app\ChatGPT.exe"
  if (!(Test-Path -LiteralPath $managedExe -PathType Leaf)) { throw "managed app update left no current ChatGPT.exe: $managedExe" }
  $proxyLogAfterUpgrade = Get-Content -Raw -LiteralPath $proxyLog
  $proxyLogDelta = if ($proxyLogAfterUpgrade.Length -gt $proxyLogBeforeUpgrade.Length) {
    $proxyLogAfterUpgrade.Substring($proxyLogBeforeUpgrade.Length)
  } else { "" }
  if ($proxyLogDelta -notmatch "CONNECT target=") {
    throw "managed app update did not download the official MSIX through the selected proxy"
  }

  $env:CXP_TEST_CHILD_LOG = $fakeLog
  & go build -o $fakeExe .\scripts\ci\windows_managed_fake_chatgpt
  if ($LASTEXITCODE -ne 0) { throw "building fake ChatGPT child failed" }
  Copy-Item -LiteralPath $fakeExe -Destination $managedExe -Force
  $state.executableSha256 = (Get-FileHash -LiteralPath $managedExe -Algorithm SHA256).Hash.ToLowerInvariant()
  $state | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $statePath -Encoding UTF8

  $existingProcessIds = @{}
  Get-Process -Name $desktopProcessNames -ErrorAction SilentlyContinue | ForEach-Object { $existingProcessIds[$_.Id] = $true }
  Remove-Item -Force -LiteralPath $out -ErrorAction SilentlyContinue
  try {
    & $Helper --config $config app --cwd $work --codex-dir $codexHome *> $out
  } catch {
    $appOut = if (Test-Path -LiteralPath $out) { Get-Content -Raw -LiteralPath $out } else { "" }
    throw "managed cached launch failed`napp output:`n$appOut`nerror:`n$($_.Exception.Message)"
  }
  for ($i = 0; $i -lt 100; $i++) {
    $launchedProcesses = Get-NewDesktopProcesses $existingProcessIds
    if ($launchedProcesses.Count -gt 0 -and (Test-Path -LiteralPath $fakeLog -PathType Leaf)) { break }
    Start-Sleep -Milliseconds 100
  }
  if ($launchedProcesses.Count -eq 0) { throw "managed cached launch did not create the fake ChatGPT process" }
  $paths = @($launchedProcesses | ForEach-Object { try { $_.Path } catch { "" } })
  if (-not ($paths | Where-Object { $_ -and $_ -like "$managedRoot\*" })) {
    throw "observed desktop process did not run from managed runtime: $($paths -join '; ')"
  }
  $child = Get-Content -Raw -LiteralPath $fakeLog
  foreach ($want in @("--proxy-server=$proxyURL", "HTTP_PROXY=$proxyURL", "HTTPS_PROXY=$proxyURL", "ALL_PROXY=$proxyURL", "NO_PROXY=", "no_proxy=", "request_status=200", "CXP_RUNTIME=")) {
    if (-not $child.Contains($want)) { throw "fake child output missing $want`n$child" }
  }
  $runtimeLine = (($child -split "\n" | Where-Object { $_ -like "CXP_RUNTIME=*" } | Select-Object -First 1).Trim())
  if ($runtimeLine -ne "CXP_RUNTIME=") { throw "CXP runtime marker leaked to fake child: $runtimeLine" }
  if (-not $child.Contains("CODEX_HOME=") -or [string]::IsNullOrWhiteSpace(($child -split "\n" | Where-Object { $_ -like "CODEX_HOME=*" } | Select-Object -First 1))) {
    throw "fake child did not receive isolated CODEX_HOME`n$child"
  }
  if ($child -match "request_error=[^\r\n]+") { throw "fake child request bypassed or could not use recording proxy`n$child" }
  $proxyOutput = Get-Content -Raw -LiteralPath $proxyLog
  if (-not $proxyOutput.Contains("$nonce")) { throw "recording proxy did not observe the unique child nonce`n$proxyOutput" }
  if (-not $proxyOutput.Contains("CONNECT target=")) { throw "recording proxy did not observe the public MSIX HTTPS download`n$proxyOutput" }
  $appOutput = if (Test-Path -LiteralPath $out) { Get-Content -Raw -LiteralPath $out } else { "" }
  foreach ($forbidden in @("winget", "shell:AppsFolder")) {
    if ($appOutput.Contains($forbidden)) { throw "managed-only path unexpectedly used legacy/AppX mechanism: $forbidden`n$appOutput" }
  }

  $afterProxy = Get-ProxySnapshot
  if ($afterProxy -ne $beforeProxy) { throw "managed launch changed the system or user proxy settings" }
  Write-Host "Managed ChatGPT credential-free full-chain smoke passed: $managedExe"
} finally {
  try { [void](Stop-DesktopProcesses $existingProcessIds) } catch { }
  if ($null -ne $proxyProcess) {
    try { Stop-Process -Id $proxyProcess.Id -Force -ErrorAction SilentlyContinue } catch { }
  }
  if ($null -eq $oldBackend) { Remove-Item Env:CXP_WINDOWS_APP_BACKEND -ErrorAction SilentlyContinue } else { $env:CXP_WINDOWS_APP_BACKEND = $oldBackend }
  if ($null -eq $oldManagedRoot) { Remove-Item Env:CXP_WINDOWS_MANAGED_ROOT -ErrorAction SilentlyContinue } else { $env:CXP_WINDOWS_MANAGED_ROOT = $oldManagedRoot }
  if ($null -eq $oldRuntimeDisable) { Remove-Item Env:CXP_RUNTIME_DISABLE -ErrorAction SilentlyContinue } else { $env:CXP_RUNTIME_DISABLE = $oldRuntimeDisable }
  if ($null -eq $oldChildLog) { Remove-Item Env:CXP_TEST_CHILD_LOG -ErrorAction SilentlyContinue } else { $env:CXP_TEST_CHILD_LOG = $oldChildLog }
  if ($null -eq $oldNoProxy) { Remove-Item Env:NO_PROXY -ErrorAction SilentlyContinue } else { $env:NO_PROXY = $oldNoProxy }
  if ($null -eq $oldLowerNoProxy) { Remove-Item Env:no_proxy -ErrorAction SilentlyContinue } else { $env:no_proxy = $oldLowerNoProxy }
  if ($null -eq $oldNonce) { Remove-Item Env:CXP_TEST_NONCE -ErrorAction SilentlyContinue } else { $env:CXP_TEST_NONCE = $oldNonce }
  Remove-Item -Recurse -Force -LiteralPath $base -ErrorAction SilentlyContinue
}
