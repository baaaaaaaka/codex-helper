# Run installer functions without invoking download, installation, or live tasks.
# These tests exercise PowerShell error propagation and native executable locks.
$ErrorActionPreference = 'Stop'
if ($env:OS -ne 'Windows_NT') { throw 'Windows required' }
$repo = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$scriptPath = Join-Path $repo 'install.ps1'
$tokens = $null; $parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($scriptPath, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
foreach ($name in @('Get-CodexProxyProcessesForPath', 'Stop-CodexProxyTeamsTasksForInstall', 'Wait-CodexProxyInstallPathReleased')) {
    $f = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true)
    if ($null -eq $f) { throw "Missing installer function $name" }
    Invoke-Expression $f.Extent.Text
}
# A non-terminating access-denied error must stop before any process cleanup.
function Get-ScheduledTask { [CmdletBinding()]param(); [pscustomobject]@{TaskName='Codex Helper Teams Watchdog'; TaskPath='\'} }
function Disable-ScheduledTask { [CmdletBinding()]param($TaskName); Write-Error 'simulated access denied' }
$denied = $false
try { Stop-CodexProxyTeamsTasksForInstall @() } catch { $denied = $_.Exception.Message -match 'simulated access denied' }
if (-not $denied) { throw 'Task denial was ignored' }
Remove-Item Function:Get-ScheduledTask, Function:Disable-ScheduledTask

$scratch = Join-Path $env:RUNNER_TEMP ('cxp-installer-contract-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $scratch | Out-Null
$locked = $null
$registered = @()
try {
    $fixture = Join-Path $scratch 'cxp.exe'
    go build -o $fixture (Join-Path $repo 'internal/cli/testdata/servicefixture')
    if ($LASTEXITCODE -ne 0) { throw 'Could not build inert native service fixture' }
    function Start-Fixture([string]$path, [string]$arguments) {
        $p = New-Object System.Diagnostics.Process
        $p.StartInfo.FileName = $path
        $p.StartInfo.Arguments = $arguments
        $p.StartInfo.UseShellExecute = $false
        $p.StartInfo.CreateNoWindow = $true
        $p.StartInfo.RedirectStandardInput = $true
        $p.StartInfo.RedirectStandardOutput = $true
        [void]$p.Start()
        $ready = $p.StandardOutput.ReadLineAsync()
        if (-not $ready.Wait(5000) -or $ready.Result -ne 'ready') { $p.Kill(); throw 'Fixture did not start' }
        return $p
    }
    $binary = Join-Path $scratch 'codex-proxy.exe'
    Copy-Item $fixture $binary
    $locked = Start-Fixture $binary ''
    $refused = $false
    try { Wait-CodexProxyInstallPathReleased $binary -attempts 2 -delayMs 10 } catch {
        $refused = $_.Exception.Message.Contains($binary) -and $_.Exception.Message.Contains([string]$locked.Id)
    }
    if (-not $refused) { throw 'Installer accepted a running target or hid its identity' }
    # The busy entry has not been replaced or deleted.
    if ((Get-FileHash $binary).Hash -ne (Get-FileHash $fixture).Hash) { throw 'Busy entry was changed' }
    Stop-Process -Id $locked.Id -Force
    $locked.WaitForExit(); $locked = $null
    Wait-CodexProxyInstallPathReleased $binary -attempts 2 -delayMs 10
    # Hosted CI is ephemeral. Refuse to touch any pre-existing helper task;
    # register our own inert actions and exercise the actual scheduler cmdlets.
    $names = @('Codex Helper Teams Watchdog', 'Codex Helper Teams Bridge')
    $existing = @(Get-ScheduledTask -ErrorAction Stop | Where-Object { $_.TaskName -in $names -and $_.TaskPath -eq '\' })
    if ($existing.Count) { throw 'Refusing to replace existing helper tasks' }
    $action = New-ScheduledTaskAction -Execute (Join-Path $PSHOME 'powershell.exe') -Argument '-NoProfile -NonInteractive -Command Start-Sleep -Seconds 60'
    $principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount
    foreach ($name in $names) {
        Register-ScheduledTask -TaskName $name -Action $action -Principal $principal -ErrorAction Stop | Out-Null
        $registered += $name
        Start-ScheduledTask -TaskName $name -ErrorAction Stop
        for ($i = 0; $i -lt 50 -and (Get-ScheduledTask -TaskName $name).State -ne 'Running'; $i++) { Start-Sleep -Milliseconds 100 }
        if ((Get-ScheduledTask -TaskName $name).State -ne 'Running') { throw "Task did not start: $name" }
    }
    $service = Start-Fixture $fixture 'teams service watchdog --loop --interval 10s --quiet'
    $tui = Start-Fixture $fixture ''
    try {
        Stop-CodexProxyTeamsTasksForInstall @($fixture)
        if (-not $service.WaitForExit(5000)) { throw 'Detached watchdog survived installer maintenance' }
        if ($tui.HasExited) { throw 'Installer killed an unrelated interactive session' }
        foreach ($name in $names) {
            for ($i = 0; $i -lt 50 -and (Get-ScheduledTask -TaskName $name).State -ne 'Disabled'; $i++) { Start-Sleep -Milliseconds 100 }
            if ((Get-ScheduledTask -TaskName $name -ErrorAction Stop).State -ne 'Disabled') { throw "Task can restart after maintenance: $name" }
            $blocked = $false
            try { Start-ScheduledTask -TaskName $name -ErrorAction Stop } catch { $blocked = $true }
            if (-not $blocked) { throw "Disabled task restarted: $name" }
        }
    } finally {
        foreach ($p in @($service, $tui)) {
            if (-not $p.HasExited) { $p.Kill(); $p.WaitForExit() }
            $p.Dispose()
        }
    }
    Write-Host 'PASS: denied tasks, native locks, actual task disable/stop, detached watchdog cleanup and interactive-session preservation'
} finally {
    if ($null -ne $locked -and -not $locked.HasExited) { Stop-Process -Id $locked.Id -Force }
    foreach ($name in $registered) {
        Stop-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
        Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
    }
    Remove-Item -LiteralPath $scratch -Recurse -Force
}
