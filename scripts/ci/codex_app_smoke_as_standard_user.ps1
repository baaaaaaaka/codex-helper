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
  $settings = $null
  $exitCode = 1
  $errorMessage = ""
  try {
    $settings = Get-Content -Raw -LiteralPath $SettingsPath | ConvertFrom-Json
    $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    if (![string]::Equals($identity.Name, [string]$settings.ExpectedIdentity, [StringComparison]::OrdinalIgnoreCase)) {
      throw "desktop app smoke identity is $($identity.Name), expected $($settings.ExpectedIdentity)"
    }
    $principal = [System.Security.Principal.WindowsPrincipal]::new($identity)
    $administratorsSID = [System.Security.Principal.SecurityIdentifier]::new(
      [System.Security.Principal.WellKnownSidType]::BuiltinAdministratorsSid,
      $null
    )
    if ($principal.IsInRole($administratorsSID)) {
      throw "desktop app smoke is still elevated: $($identity.Name)"
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
    $env:RUNNER_TEMP = [string]$settings.RunnerTemp
    $env:CXP_RUNTIME_DISABLE = "1"
    $windowsApps = Join-Path $env:LOCALAPPDATA "Microsoft\WindowsApps"
    $machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    $env:Path = "$windowsApps;$machinePath"
    Set-Location -LiteralPath ([string]$settings.WorkingDirectory)

    for ($attempt = 1; $attempt -le 2; $attempt++) {
      try {
        if ($settings.NetworkInstall) {
          $env:CXP_WINDOWS_APP_BACKEND = "legacy"
          & (Join-Path $PSScriptRoot "codex_app_network_install_smoke.ps1") -Helper ([string]$settings.Helper) *> ([string]$settings.OutputPath)
        } else {
          & (Join-Path $PSScriptRoot "codex_app_managed_install_smoke.ps1") `
            -Helper ([string]$settings.Helper) `
            -RecordingProxy ([string]$settings.RecordingProxy) `
            -FakeChatGPT ([string]$settings.FakeChatGPT) *> ([string]$settings.OutputPath)
        }
        if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) {
          throw "desktop app smoke command failed with exit code $LASTEXITCODE"
        }
        $exitCode = 0
        break
      } catch {
        $errorMessage = $_.Exception.ToString()
        Add-Content -LiteralPath ([string]$settings.OutputPath) -Value $errorMessage -Encoding UTF8
        if ($attempt -eq 2) { break }
        Add-Content -LiteralPath ([string]$settings.OutputPath) -Value "Desktop app smoke failed on attempt $attempt; retrying in 10 seconds." -Encoding UTF8
        Start-Sleep -Seconds 10
      }
    }
  } catch {
    $errorMessage = $_.Exception.ToString()
    if ($settings -and $settings.OutputPath) {
      Add-Content -LiteralPath ([string]$settings.OutputPath) -Value $errorMessage -Encoding UTF8
    }
  } finally {
    if ($settings -and $settings.ResultPath) {
      [ordered]@{ ExitCode = $exitCode; Error = $errorMessage } |
        ConvertTo-Json -Compress |
        Set-Content -LiteralPath ([string]$settings.ResultPath) -Encoding UTF8
    }
  }
  exit $exitCode
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
$smokeRoot = Join-Path $runnerTemp ("cxp-desktop-limited-token-smoke-" + [guid]::NewGuid().ToString("N"))
$nativeSource = @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text;

public static class CxpLimitedTokenProcess
{
    private const uint TokenQuery = 0x0008;
    private const int TokenLinkedToken = 19;
    private const uint CreateUnicodeEnvironment = 0x00000400;
    private const uint WaitObject0 = 0x00000000;
    private const uint WaitTimeout = 0x00000102;
    private const uint Infinite = 0xFFFFFFFF;

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    private struct StartupInfo
    {
        public int cb;
        public string lpReserved;
        public string lpDesktop;
        public string lpTitle;
        public int dwX;
        public int dwY;
        public int dwXSize;
        public int dwYSize;
        public int dwXCountChars;
        public int dwYCountChars;
        public int dwFillAttribute;
        public int dwFlags;
        public short wShowWindow;
        public short cbReserved2;
        public IntPtr lpReserved2;
        public IntPtr hStdInput;
        public IntPtr hStdOutput;
        public IntPtr hStdError;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ProcessInformation
    {
        public IntPtr hProcess;
        public IntPtr hThread;
        public uint dwProcessId;
        public uint dwThreadId;
    }

    [DllImport("kernel32.dll")]
    private static extern IntPtr GetCurrentProcess();

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool CloseHandle(IntPtr handle);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern uint WaitForSingleObject(IntPtr handle, uint milliseconds);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetExitCodeProcess(IntPtr process, out uint exitCode);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool TerminateProcess(IntPtr process, uint exitCode);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool OpenProcessToken(IntPtr process, uint access, out IntPtr token);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool GetTokenInformation(IntPtr token, int informationClass, out IntPtr information, uint informationLength, out uint returnedLength);

    [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool CreateProcessWithTokenW(IntPtr token, uint logonFlags, string applicationName, StringBuilder commandLine, uint creationFlags, IntPtr environment, string currentDirectory, ref StartupInfo startupInfo, out ProcessInformation processInformation);

    public static int Run(string application, string arguments, string currentDirectory, uint timeoutMilliseconds)
    {
        IntPtr currentToken = IntPtr.Zero;
        IntPtr limitedToken = IntPtr.Zero;
        ProcessInformation process = new ProcessInformation();
        try
        {
            if (!OpenProcessToken(GetCurrentProcess(), TokenQuery, out currentToken))
                throw new Win32Exception(Marshal.GetLastWin32Error(), "open current Windows token");
            uint returnedLength;
            if (!GetTokenInformation(currentToken, TokenLinkedToken, out limitedToken, (uint)IntPtr.Size, out returnedLength))
                throw new Win32Exception(Marshal.GetLastWin32Error(), "query the current user's linked limited token");
            if (limitedToken == IntPtr.Zero)
                throw new InvalidOperationException("the current Windows token has no linked limited token");

            StartupInfo startup = new StartupInfo();
            startup.cb = Marshal.SizeOf(typeof(StartupInfo));
            StringBuilder commandLine = new StringBuilder("\"" + application + "\" " + arguments);
            if (!CreateProcessWithTokenW(limitedToken, 0, application, commandLine, CreateUnicodeEnvironment, IntPtr.Zero, currentDirectory, ref startup, out process))
                throw new Win32Exception(Marshal.GetLastWin32Error(), "start the desktop smoke with the linked limited token");

            uint waitResult = WaitForSingleObject(process.hProcess, timeoutMilliseconds);
            if (waitResult == WaitTimeout)
            {
                TerminateProcess(process.hProcess, 1);
                WaitForSingleObject(process.hProcess, Infinite);
                throw new TimeoutException("limited-token desktop app smoke exceeded its time limit");
            }
            if (waitResult != WaitObject0)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "wait for the limited-token desktop app smoke");

            uint exitCode;
            if (!GetExitCodeProcess(process.hProcess, out exitCode))
                throw new Win32Exception(Marshal.GetLastWin32Error(), "read the limited-token desktop app smoke exit code");
            return unchecked((int)exitCode);
        }
        finally
        {
            if (process.hThread != IntPtr.Zero) CloseHandle(process.hThread);
            if (process.hProcess != IntPtr.Zero) CloseHandle(process.hProcess);
            if (limitedToken != IntPtr.Zero) CloseHandle(limitedToken);
            if (currentToken != IntPtr.Zero) CloseHandle(currentToken);
        }
    }
}
'@
if (-not ("CxpLimitedTokenProcess" -as [type])) {
  Add-Type -TypeDefinition $nativeSource
}

try {
  New-Item -ItemType Directory -Force -Path $smokeRoot | Out-Null
  $settingsPath = Join-Path $smokeRoot "settings.json"
  $outputPath = Join-Path $smokeRoot "smoke.output.log"
  $resultPath = Join-Path $smokeRoot "smoke.result.json"
  $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
  $helperPath = [IO.Path]::GetFullPath($Helper)
  $scriptPath = [IO.Path]::GetFullPath($PSCommandPath)
  $workingDirectory = [IO.Path]::GetFullPath((Get-Location).Path)
  [ordered]@{
    ExpectedIdentity = $identity.Name
    RunnerTemp = $runnerTemp
    WorkingDirectory = $workingDirectory
    Helper = $helperPath
    NetworkInstall = [bool]$NetworkInstall
    RecordingProxy = if ($ManagedInstall) { [IO.Path]::GetFullPath($RecordingProxy) } else { "" }
    FakeChatGPT = if ($ManagedInstall) { [IO.Path]::GetFullPath($FakeChatGPT) } else { "" }
    OutputPath = $outputPath
    ResultPath = $resultPath
  } | ConvertTo-Json | Set-Content -LiteralPath $settingsPath -Encoding UTF8
  Set-Content -LiteralPath $outputPath -Value "" -Encoding UTF8

  $powerShell = Join-Path $PSHOME "pwsh.exe"
  $argumentLine = "-NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$scriptPath`" -Helper `"$helperPath`" -Child -SettingsPath `"$settingsPath`""
  $processExitCode = [CxpLimitedTokenProcess]::Run($powerShell, $argumentLine, $workingDirectory, 900000)

  $output = if (Test-Path -LiteralPath $outputPath) { Get-Content -Raw -LiteralPath $outputPath } else { "" }
  if (![string]::IsNullOrWhiteSpace($output)) { Write-Host $output }
  if (!(Test-Path -LiteralPath $resultPath -PathType Leaf)) {
    throw "limited-token desktop app smoke exited with code $processExitCode without a result file"
  }
  $result = Get-Content -Raw -LiteralPath $resultPath | ConvertFrom-Json
  if ($processExitCode -ne 0 -or $result.ExitCode -ne 0) {
    throw "limited-token desktop app smoke failed with exit code $($result.ExitCode): $($result.Error)"
  }
} finally {
  Remove-Item -Recurse -Force -LiteralPath $smokeRoot -ErrorAction SilentlyContinue
}
