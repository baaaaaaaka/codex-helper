param(
  [Parameter(Mandatory = $true)]
  [string]$Helper,
  [Parameter(Mandatory = $true)]
  [string]$TokenProbe,
  [Parameter(Mandatory = $true)]
  [ValidateSet("managed", "store", "appx")]
  [string]$Mode,
  [string]$RecordingProxy = "",
  [string]$FakeChatGPT = "",
  [string]$SettingsPath = ""
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
if (!$IsWindows) { throw "the standard-user app smoke requires native Windows" }
. (Join-Path $PSScriptRoot "codex_app_smoke_process.ps1")

if ($SettingsPath) {
  try {
    $settings = Get-Content -Raw -LiteralPath $SettingsPath | ConvertFrom-Json
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    if ($identity.User.Value -ne $settings.AccountSid) {
      throw "unexpected smoke identity: $($identity.User.Value), expected $($settings.AccountSid)"
    }
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    if ($principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
      throw "the test account has effective Administrator membership"
    }
    $profile = [Environment]::GetFolderPath([Environment+SpecialFolder]::UserProfile)
    $profileKey = "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\ProfileList\$($identity.User.Value)"
    $registeredProfile = [Environment]::ExpandEnvironmentVariables((Get-ItemPropertyValue -LiteralPath $profileKey -Name ProfileImagePath))
    if (!$profile -or $profile -ine $registeredProfile -or !(Test-Path -LiteralPath "Registry::HKEY_USERS\$($identity.User.Value)")) {
      throw "the test account's native profile and registry hive were not loaded correctly"
    }
    foreach ($name in @([Environment]::GetEnvironmentVariables().Keys)) {
      if ($name -match '^(GITHUB|ACTIONS)_' -or $name -match '^CXP_(RUNTIME|WINDOWS|TEST)_' -or $name -match '^CODEX_(RUNTIME|PROXY)_' -or $name -match '^(CODEX_HOME|CODEX_DIR|GH_TOKEN|OPENAI_API_KEY|MIMO_API_KEY|ANTHROPIC_API_KEY|CODEX_LIVE_AUTH_JSON|CHATGPT_AUTH_TOKEN|CODEX_AUTH_TOKEN)$' -or $name -match '^CODEX_HELPER_TEAMS_.*TOKEN_CACHE$') {
        Remove-Item ("Env:" + $name) -ErrorAction SilentlyContinue
      }
    }
    $env:USERPROFILE = $profile
    $env:USERNAME = [Environment]::UserName
    $env:USERDOMAIN = [Environment]::UserDomainName
    $env:HOME = $profile
    $env:APPDATA = [Environment]::GetFolderPath([Environment+SpecialFolder]::ApplicationData)
    $env:LOCALAPPDATA = [Environment]::GetFolderPath([Environment+SpecialFolder]::LocalApplicationData)
    $env:RUNNER_TEMP = [string]$settings.WorkingDirectory
    $env:TEMP = $env:RUNNER_TEMP
    $env:TMP = $env:RUNNER_TEMP
    $env:PATH = (Join-Path $env:LOCALAPPDATA "Microsoft\WindowsApps") + ";" + [Environment]::GetEnvironmentVariable("PATH", "Machine")
    $env:CXP_RUNTIME_DISABLE = "1"
    Set-Location -LiteralPath $env:RUNNER_TEMP
    Write-Host "Smoke identity=$($identity.Name); SID=$($identity.User.Value); profile=$profile; session=$([Diagnostics.Process]::GetCurrentProcess().SessionId); mode=$Mode"
    $env:CXP_TEST_WINDOWS_TOKEN_EXPECTATION = "standard"
    $probeOutput = Invoke-SmokeProcess -FilePath $TokenProbe -Arguments @("-test.run", "^TestCurrentWindowsTokenElevationQuery$", "-test.v") -OutputPath (Join-Path $env:RUNNER_TEMP "token-probe.out")
    Write-Host $probeOutput
    if (!$probeOutput.Contains("actual Windows TokenElevation=false")) { throw "the native token probe did not execute its standard-token assertion" }
    Remove-Item Env:CXP_TEST_WINDOWS_TOKEN_EXPECTATION
    Invoke-SmokeProcess -FilePath $Helper -Arguments @("--version") -OutputPath (Join-Path $env:RUNNER_TEMP "helper-version.out") | Write-Host
    Write-Host "Standard-user environment probe passed"
    switch ($Mode) {
      "managed" {
        & (Join-Path $PSScriptRoot "codex_app_managed_install_smoke.ps1") -Helper $Helper -RecordingProxy ([string]$settings.RecordingProxy) -FakeChatGPT ([string]$settings.FakeChatGPT)
      }
      "store" {
        $windowsPowerShell = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
        $register = '$ErrorActionPreference = "Stop"; Add-AppxPackage -RegisterByFamilyName -MainPackage Microsoft.DesktopAppInstaller_8wekyb3d8bbwe'
        Write-Host "Preparing the current CI account's App Installer registration using the documented Microsoft command"
        Invoke-SmokeProcess -FilePath $windowsPowerShell -Arguments @("-NoProfile", "-NonInteractive", "-Command", $register) -OutputPath (Join-Path $env:RUNNER_TEMP "app-installer-registration.out") | Write-Host
        if (!(Get-Command winget.exe -ErrorAction SilentlyContinue)) {
          throw "Store smoke capability unavailable after documented App Installer registration; managed and signed-AppX smokes do not require winget"
        }
        $env:CXP_WINDOWS_APP_BACKEND = "legacy"
        try {
          & (Join-Path $PSScriptRoot "codex_app_network_install_smoke.ps1") -Helper $Helper
        } finally {
          $diagnostics = Join-Path $env:LOCALAPPDATA "Packages\Microsoft.DesktopAppInstaller_8wekyb3d8bbwe\LocalState\DiagOutputDir"
          Save-SmokeWingetDiagnostics -SourceDirectory $diagnostics -OutputDirectory $env:RUNNER_TEMP
        }
      }
      "appx" {
        $family = [string]$settings.PackageFamilyName
        if ($family -notmatch '^OpenAI\.Codex_[a-z0-9]+$') { throw "unexpected fixture package family: $family" }
        $register = '$ErrorActionPreference = "Stop"; Add-AppxPackage -RegisterByFamilyName -MainPackage ' + "'$family'"
        Invoke-SmokeProcess -FilePath (Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe") -Arguments @("-NoProfile", "-NonInteractive", "-Command", $register) -OutputPath (Join-Path $env:RUNNER_TEMP "appx-registration.out") | Write-Host
        $env:CXP_WINDOWS_APP_BACKEND = "legacy"
        & (Join-Path $PSScriptRoot "codex_app_network_install_smoke.ps1") -Helper $Helper -RegisteredPackage
      }
      default { throw "unsupported smoke mode: $Mode" }
    }
    exit 0
  } catch {
    Write-Error -ErrorRecord $_ -ErrorAction Continue
    exit 1
  }
}

if ($env:RUNNER_ENVIRONMENT -ne "github-hosted") { throw "the temporary-account smoke is limited to disposable GitHub-hosted runners" }
function Get-SmokeUserProcesses([string]$AccountSid) {
  foreach ($candidate in @(Get-CimInstance -ClassName Win32_Process)) {
    $owner = Invoke-CimMethod -InputObject $candidate -MethodName GetOwnerSid -ErrorAction SilentlyContinue
    if ($null -ne $owner -and $owner.Sid -eq $AccountSid) { $candidate }
  }
}

foreach ($path in @($Helper, $TokenProbe)) {
  if (!(Test-Path -LiteralPath $path -PathType Leaf)) { throw "smoke executable does not exist: $path" }
}
if ($Mode -eq "managed" -and (!(Test-Path -LiteralPath $RecordingProxy -PathType Leaf) -or !(Test-Path -LiteralPath $FakeChatGPT -PathType Leaf))) {
  throw "the managed smoke requires prebuilt recording proxy and ChatGPT fixtures"
}
$runnerTemp = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [IO.Path]::GetTempPath() }
$smokeRoot = Join-Path $runnerTemp ("cxp-desktop-smoke-" + $Mode + "-" + [guid]::NewGuid().ToString("N"))
$accountName = "CxpSmk" + [guid]::NewGuid().ToString("N").Substring(0, 12)
$account = $null
$process = $null
$stdout = Join-Path $smokeRoot "stdout.log"
$stderr = Join-Path $smokeRoot "stderr.log"

try {
  New-Item -ItemType Directory -Path $smokeRoot | Out-Null
  $packageFamily = ""
  if ($Mode -eq "appx") {
    try {
      $env:CXP_TEST_WINDOWS_TOKEN_EXPECTATION = "elevated"
      $probeOutput = Invoke-SmokeProcess -FilePath $TokenProbe -Arguments @("-test.run", "^TestCurrentWindowsTokenElevationQuery$", "-test.v") -OutputPath (Join-Path $smokeRoot "fixture-token-probe.out")
      Write-Host $probeOutput
      if (!$probeOutput.Contains("actual Windows TokenElevation=true")) { throw "the fixture-preparation token probe did not execute its elevated-token assertion" }
    } finally { Remove-Item Env:CXP_TEST_WINDOWS_TOKEN_EXPECTATION -ErrorAction SilentlyContinue }
    $package = Join-Path $smokeRoot "ChatGPT-x64.msix"
    Invoke-WebRequest -Uri "https://persistent.oaistatic.com/codex-app-prod/ChatGPT-x64.msix" -OutFile $package
    Write-Host "Official MSIX SHA256=$((Get-FileHash -LiteralPath $package -Algorithm SHA256).Hash)"
    $packagePath = $package.Replace("'", "''")
    $install = '$ErrorActionPreference = "Stop"; Add-AppxPackage -Path ' + "'$packagePath'" + '; $package = Get-AppxPackage -Name OpenAI.Codex; if ($null -eq $package) { throw "OpenAI.Codex was not installed" }; $package.PackageFamilyName'
    Write-Host "Preparing the signed package and its LocalSystem service with the CI runner; CXP will launch only in the standard account"
    $packageFamily = (Invoke-SmokeProcess -FilePath (Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe") -Arguments @("-NoProfile", "-NonInteractive", "-Command", $install) -OutputPath (Join-Path $smokeRoot "signed-package-install.out")).Trim()
  }
  $password = ConvertTo-SecureString ([guid]::NewGuid().ToString("N") + "Aa1!") -AsPlainText -Force
  $account = New-LocalUser -Name $accountName -Password $password -PasswordNeverExpires -Description "Ephemeral CXP desktop smoke user"
  $acl = Get-Acl -LiteralPath $smokeRoot
  $access = [Security.AccessControl.FileSystemAccessRule]::new($account.SID, "Modify", "ContainerInherit, ObjectInherit", "None", "Allow")
  $acl.AddAccessRule($access)
  Set-Acl -LiteralPath $smokeRoot -AclObject $acl
  $work = Join-Path $smokeRoot "work"
  New-Item -ItemType Directory -Path $work | Out-Null
  foreach ($name in @("codex_app_smoke_as_standard_user.ps1", "codex_app_smoke_process.ps1", "codex_app_managed_install_smoke.ps1", "codex_app_network_install_smoke.ps1")) {
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot $name) -Destination (Join-Path $smokeRoot $name)
  }
  $childHelper = Join-Path $smokeRoot "cxp.exe"
  $childProbe = Join-Path $smokeRoot "token-probe.exe"
  Copy-Item -LiteralPath $Helper -Destination $childHelper
  Copy-Item -LiteralPath $TokenProbe -Destination $childProbe
  $childProxy = ""
  $childApp = ""
  if ($Mode -eq "managed") {
    $childProxy = Join-Path $smokeRoot "recording-proxy.exe"
    $childApp = Join-Path $smokeRoot "fake-chatgpt.exe"
    Copy-Item -LiteralPath $RecordingProxy -Destination $childProxy
    Copy-Item -LiteralPath $FakeChatGPT -Destination $childApp
  }
  $settings = Join-Path $smokeRoot "settings.json"
  @{ AccountSid = $account.SID.Value; WorkingDirectory = $work; RecordingProxy = $childProxy; FakeChatGPT = $childApp; PackageFamilyName = $packageFamily } | ConvertTo-Json | Set-Content -LiteralPath $settings -Encoding UTF8
  $credential = [PSCredential]::new("$env:COMPUTERNAME\$accountName", $password)
  $childScript = Join-Path $smokeRoot "codex_app_smoke_as_standard_user.ps1"
  $arguments = "-NoLogo -NoProfile -NonInteractive -File `"$childScript`" -Helper `"$childHelper`" -TokenProbe `"$childProbe`" -Mode $Mode -SettingsPath `"$settings`""
  $process = Start-Process -FilePath (Join-Path $PSHOME "pwsh.exe") -Credential $credential -LoadUserProfile -UseNewEnvironment -WorkingDirectory $work -ArgumentList $arguments -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
  if (!$process.WaitForExit(900000)) { throw "the $Mode smoke exceeded its 15-minute timeout" }
  if ($process.ExitCode -ne 0) { throw "the $Mode smoke failed with exit code $($process.ExitCode); diagnostics: $smokeRoot" }
} finally {
  try {
    if ($null -ne $account) {
      foreach ($candidate in @(Get-SmokeUserProcesses $account.SID.Value)) {
        Stop-Process -Id $candidate.ProcessId -Force -ErrorAction SilentlyContinue
      }
      $remaining = @(Get-SmokeUserProcesses $account.SID.Value)
      if ($remaining.Count -gt 0) { throw "smoke cleanup left test-account processes: $($remaining.ProcessId -join ', ')" }
    }
  } finally {
    if ($null -ne $account) { Remove-LocalUser -SID $account.SID }
    if ($null -ne $process) { $process.Dispose() }
    foreach ($log in @($stdout, $stderr)) {
      if (Test-Path -LiteralPath $log) { Get-Content -Raw -LiteralPath $log | Write-Host }
    }
    Write-Host "Desktop smoke diagnostics retained at $smokeRoot"
  }
}
