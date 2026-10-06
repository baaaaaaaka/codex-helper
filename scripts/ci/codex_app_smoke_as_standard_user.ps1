param(
  [Parameter(Mandatory = $true)]
  [string]$Helper,
  [switch]$NetworkInstall,
  [switch]$ManagedInstall,
  [string]$RecordingProxy = "",
  [string]$FakeChatGPT = "",
  [switch]$Child,
  [string]$SettingsPath = ""
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

if ($Child) {
  $settings = Get-Content -Raw -LiteralPath $SettingsPath | ConvertFrom-Json
  $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
  $expectedIdentity = "$env:COMPUTERNAME\$($settings.AccountName)"
  if (![string]::Equals($identity.Name, $expectedIdentity, [StringComparison]::OrdinalIgnoreCase)) {
    throw "desktop app smoke identity is $($identity.Name), expected $expectedIdentity"
  }
  $administratorsSID = [System.Security.Principal.SecurityIdentifier]::new(
    [System.Security.Principal.WellKnownSidType]::BuiltinAdministratorsSid,
    $null
  )
  if ($identity.Groups -contains $administratorsSID) {
    throw "desktop app smoke account unexpectedly belongs to the local Administrators group: $($identity.Name)"
  }
  foreach ($name in @([Environment]::GetEnvironmentVariables().Keys)) {
    if (
      $name -match '^(GITHUB|ACTIONS)_' -or
      $name -match '^CXP_(RUNTIME|WINDOWS|TEST)_' -or
      $name -match '^CODEX_(RUNTIME|PROXY)_' -or
      $name -match '^(CODEX_HOME|CODEX_DIR|GH_TOKEN|OPENAI_API_KEY|MIMO_API_KEY|ANTHROPIC_API_KEY|CODEX_LIVE_AUTH_JSON|CHATGPT_AUTH_TOKEN|CODEX_AUTH_TOKEN)$' -or
      $name -match '^CODEX_HELPER_TEAMS_.*TOKEN_CACHE$'
    ) {
      Remove-Item ("Env:" + $name) -ErrorAction SilentlyContinue
    }
  }
  $profileRoot = [string]$settings.ProfileRoot
  $env:USERPROFILE = $profileRoot
  $env:HOME = $profileRoot
  $env:USERNAME = [string]$settings.AccountName
  $env:USERDOMAIN = $env:COMPUTERNAME
  $env:HOMEDRIVE = $env:SystemDrive
  $env:HOMEPATH = "\Users\$($settings.AccountName)"
  $env:APPDATA = Join-Path $profileRoot "AppData\Roaming"
  $env:LOCALAPPDATA = Join-Path $profileRoot "AppData\Local"
  $env:TEMP = Join-Path $env:LOCALAPPDATA "Temp"
  $env:TMP = $env:TEMP
  New-Item -ItemType Directory -Force -Path $env:TEMP, (Join-Path $env:LOCALAPPDATA "Microsoft\WindowsApps") | Out-Null
  $systemPath = [Environment]::GetEnvironmentVariable("Path", "Machine")
  $env:Path = (Join-Path $env:LOCALAPPDATA "Microsoft\WindowsApps") + ";" + $systemPath
  $env:RUNNER_TEMP = [string]$settings.SmokeRoot
  $env:CXP_RUNTIME_DISABLE = "1"
  Set-Location $env:RUNNER_TEMP

  for ($attempt = 1; $attempt -le 2; $attempt++) {
    try {
      if ($settings.NetworkInstall) {
        $env:CXP_WINDOWS_APP_BACKEND = "legacy"
        if (!(Get-Command winget -ErrorAction SilentlyContinue)) {
          $manifest = ([string]$settings.AppInstallerManifest).Replace("'", "''")
          if ([string]::IsNullOrWhiteSpace($manifest) -or !(Test-Path -LiteralPath $manifest -PathType Leaf)) {
            throw "the hosted runner's App Installer manifest is unavailable for the standard-user smoke"
          }
          $registrationCommand = "Add-AppxPackage -Path '$manifest' -Register -DisableDevelopmentMode -ErrorAction Stop"
          $windowsPowerShell = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
          & $windowsPowerShell -NoProfile -NonInteractive -ExecutionPolicy Bypass -Command $registrationCommand
          if ($LASTEXITCODE -ne 0) { throw "registering App Installer for the standard-user smoke failed with exit code $LASTEXITCODE" }
          if (!(Get-Command winget -ErrorAction SilentlyContinue)) {
            throw "App Installer registration did not expose winget to the standard-user smoke"
          }
        }
        & (Join-Path $PSScriptRoot "codex_app_network_install_smoke.ps1") -Helper $settings.Helper
      } else {
        & (Join-Path $PSScriptRoot "codex_app_managed_install_smoke.ps1") `
          -Helper $settings.Helper `
          -RecordingProxy $settings.RecordingProxy `
          -FakeChatGPT $settings.FakeChatGPT
      }
      exit 0
    } catch {
      if ($attempt -eq 2) { throw }
      Write-Warning "Desktop app smoke failed on attempt $attempt; retrying in 10 seconds: $($_.Exception.Message)"
      Start-Sleep -Seconds 10
    }
  }
  exit 1
}

if ($NetworkInstall -eq $ManagedInstall) {
  throw "select exactly one of -NetworkInstall or -ManagedInstall"
}
  if (!(Test-Path -LiteralPath $Helper -PathType Leaf)) {
  throw "helper does not exist: $Helper"
}
if ($ManagedInstall -and (!(Test-Path -LiteralPath $RecordingProxy -PathType Leaf) -or !(Test-Path -LiteralPath $FakeChatGPT -PathType Leaf))) {
  throw "managed install smoke requires existing recording proxy and fake ChatGPT fixtures"
}

$runnerTemp = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [IO.Path]::GetTempPath() }
$appInstallerManifest = ""
if ($NetworkInstall) {
  $appInstallerPackage = Get-AppxPackage -Name Microsoft.DesktopAppInstaller -ErrorAction SilentlyContinue |
    Sort-Object Version -Descending |
    Select-Object -First 1
  if ($null -eq $appInstallerPackage) {
    throw "the hosted runner does not have the App Installer package needed to register winget for the standard-user smoke"
  }
  $appInstallerManifest = Join-Path $appInstallerPackage.InstallLocation "AppxManifest.xml"
  if (!(Test-Path -LiteralPath $appInstallerManifest -PathType Leaf)) {
    throw "the hosted runner's App Installer manifest does not exist: $appInstallerManifest"
  }
}
$smokeRoot = Join-Path $runnerTemp ("cxp-desktop-standard-user-" + [guid]::NewGuid().ToString("N"))
$accountName = "CxpSmk" + [guid]::NewGuid().ToString("N").Substring(0, 12)
$account = $null

try {
  New-Item -ItemType Directory -Force -Path $smokeRoot | Out-Null
  $passwordText = [guid]::NewGuid().ToString("N") + [guid]::NewGuid().ToString("N") + "Aa1!"
  $password = ConvertTo-SecureString $passwordText -AsPlainText -Force
  $account = New-LocalUser -Name $accountName -Password $password -PasswordNeverExpires -Description "Ephemeral CXP app smoke standard user"
  $acl = Get-Acl -LiteralPath $smokeRoot
  $rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
    $account.SID,
    [System.Security.AccessControl.FileSystemRights]::Modify,
    [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [System.Security.AccessControl.InheritanceFlags]::ObjectInherit,
    [System.Security.AccessControl.PropagationFlags]::None,
    [System.Security.AccessControl.AccessControlType]::Allow
  )
  $acl.AddAccessRule($rule)
  Set-Acl -LiteralPath $smokeRoot -AclObject $acl

  $supportFiles = @(
    "codex_app_smoke_as_standard_user.ps1",
    "codex_app_network_install_smoke.ps1",
    "codex_app_managed_install_smoke.ps1"
  )
  foreach ($name in $supportFiles) {
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot $name) -Destination (Join-Path $smokeRoot $name)
  }
  $smokeHelper = Join-Path $smokeRoot (Split-Path -Leaf $Helper)
  Copy-Item -LiteralPath $Helper -Destination $smokeHelper
  $smokeProxy = ""
  $smokeFake = ""
  if ($ManagedInstall) {
    $smokeProxy = Join-Path $smokeRoot (Split-Path -Leaf $RecordingProxy)
    $smokeFake = Join-Path $smokeRoot (Split-Path -Leaf $FakeChatGPT)
    Copy-Item -LiteralPath $RecordingProxy -Destination $smokeProxy
    Copy-Item -LiteralPath $FakeChatGPT -Destination $smokeFake
  }

  $settingsPath = Join-Path $smokeRoot "settings.json"
  [ordered]@{
    SmokeRoot = $smokeRoot
    AccountName = $accountName
    ProfileRoot = Join-Path $env:SystemDrive ("Users\" + $accountName)
    Helper = $smokeHelper
    NetworkInstall = [bool]$NetworkInstall
    AppInstallerManifest = $appInstallerManifest
    RecordingProxy = $smokeProxy
    FakeChatGPT = $smokeFake
  } | ConvertTo-Json | Set-Content -LiteralPath $settingsPath -Encoding UTF8

  $credential = [PSCredential]::new("$env:COMPUTERNAME\$accountName", $password)
  $childScript = Join-Path $smokeRoot "codex_app_smoke_as_standard_user.ps1"
  $stdout = Join-Path $smokeRoot "standard-user.stdout.log"
  $stderr = Join-Path $smokeRoot "standard-user.stderr.log"
  $argumentLine = "-NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$childScript`" -Helper `"$smokeHelper`" -Child -SettingsPath `"$settingsPath`""
  $process = Start-Process `
    -FilePath (Join-Path $PSHOME "pwsh.exe") `
    -Credential $credential `
    -LoadUserProfile `
    -WorkingDirectory $smokeRoot `
    -ArgumentList $argumentLine `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr `
    -PassThru `
    -Wait
  if (Test-Path -LiteralPath $stdout) { Get-Content -Raw -LiteralPath $stdout | Write-Host }
  if (Test-Path -LiteralPath $stderr) { Get-Content -Raw -LiteralPath $stderr | Write-Warning }
  if ($process.ExitCode -ne 0) {
    throw "standard-user desktop app smoke failed with exit code $($process.ExitCode)"
  }
} finally {
  if ($null -ne $account) {
    Remove-LocalUser -Name $accountName -ErrorAction SilentlyContinue
  }
  Remove-Item -Recurse -Force -LiteralPath $smokeRoot -ErrorAction SilentlyContinue
}
