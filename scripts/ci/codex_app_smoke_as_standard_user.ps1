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
    private const uint TokenAssignPrimary = 0x0001;
    private const uint TokenDuplicate = 0x0002;
    private const uint TokenAdjustDefault = 0x0080;
    private const uint DisableMaxPrivilege = 0x0001;
    private const uint LuaToken = 0x0004;
    private const int TokenLinkedToken = 19;
    private const int TokenElevation = 20;
    private const int TokenIntegrityLevel = 25;
    private const uint CreateUnicodeEnvironment = 0x00000400;
    private const uint WaitObject0 = 0x00000000;
    private const uint WaitTimeout = 0x00000102;
    private const uint Infinite = 0xFFFFFFFF;
    private const uint SeGroupIntegrity = 0x00000020;
    private const int MediumIntegrityRid = 8192;

    [StructLayout(LayoutKind.Sequential)]
    private struct SidAndAttributes
    {
        public IntPtr Sid;
        public uint Attributes;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct TokenMandatoryLabel
    {
        public SidAndAttributes Label;
    }

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
    private static extern bool CreateRestrictedToken(IntPtr existingToken, uint flags, uint disableSidCount, ref SidAndAttributes sidsToDisable, uint deletePrivilegeCount, IntPtr privilegesToDelete, uint restrictedSidCount, IntPtr sidsToRestrict, out IntPtr newToken);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool GetTokenInformation(IntPtr token, int informationClass, out IntPtr information, uint informationLength, out uint returnedLength);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool GetTokenInformation(IntPtr token, int informationClass, IntPtr information, uint informationLength, out uint returnedLength);

    [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool ConvertStringSidToSidW(string stringSid, out IntPtr sid);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern uint GetLengthSid(IntPtr sid);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool SetTokenInformation(IntPtr token, int informationClass, IntPtr information, uint informationLength);

    [DllImport("kernel32.dll")]
    private static extern IntPtr LocalFree(IntPtr memory);

    [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool CreateProcessAsUserW(IntPtr token, string applicationName, StringBuilder commandLine, IntPtr processAttributes, IntPtr threadAttributes, bool inheritHandles, uint creationFlags, IntPtr environment, string currentDirectory, ref StartupInfo startupInfo, out ProcessInformation processInformation);

    private static void SetMediumIntegrity(IntPtr token)
    {
        IntPtr mediumSid = IntPtr.Zero;
        IntPtr labelBuffer = IntPtr.Zero;
        try
        {
            if (!ConvertStringSidToSidW("S-1-16-" + MediumIntegrityRid, out mediumSid))
                throw new Win32Exception(Marshal.GetLastWin32Error(), "create the standard-user integrity SID");
            uint sidLength = GetLengthSid(mediumSid);
            if (sidLength == 0)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "measure the standard-user integrity SID");

            int labelSize = Marshal.SizeOf(typeof(TokenMandatoryLabel));
            labelBuffer = Marshal.AllocHGlobal(labelSize + (int)sidLength);
            IntPtr embeddedSid = IntPtr.Add(labelBuffer, labelSize);
            byte[] sidBytes = new byte[sidLength];
            Marshal.Copy(mediumSid, sidBytes, 0, (int)sidLength);
            Marshal.Copy(sidBytes, 0, embeddedSid, (int)sidLength);
            TokenMandatoryLabel label = new TokenMandatoryLabel();
            label.Label.Sid = embeddedSid;
            label.Label.Attributes = SeGroupIntegrity;
            Marshal.StructureToPtr(label, labelBuffer, false);
            if (!SetTokenInformation(token, TokenIntegrityLevel, labelBuffer, (uint)(labelSize + sidLength)))
                throw new Win32Exception(Marshal.GetLastWin32Error(), "set the smoke process to medium integrity");
        }
        finally
        {
            if (labelBuffer != IntPtr.Zero) Marshal.FreeHGlobal(labelBuffer);
            if (mediumSid != IntPtr.Zero) LocalFree(mediumSid);
        }
    }

    private static bool IsElevated(IntPtr token)
    {
        uint returnedLength;
        IntPtr buffer = Marshal.AllocHGlobal(sizeof(uint));
        try
        {
            if (!GetTokenInformation(token, TokenElevation, buffer, sizeof(uint), out returnedLength))
                throw new Win32Exception(Marshal.GetLastWin32Error(), "query limited-token elevation");
            if (returnedLength < sizeof(uint))
                throw new InvalidOperationException("Windows returned an incomplete token elevation value");
            return Marshal.ReadInt32(buffer) != 0;
        }
        finally
        {
            Marshal.FreeHGlobal(buffer);
        }
    }

    private static IntPtr GetLimitedToken(IntPtr currentToken)
    {
        IntPtr limitedToken = IntPtr.Zero;
        uint returnedLength;
        bool linkedTokenQuerySucceeded = GetTokenInformation(currentToken, TokenLinkedToken, out limitedToken, (uint)IntPtr.Size, out returnedLength);
        int linkedTokenError = linkedTokenQuerySucceeded ? 0 : Marshal.GetLastWin32Error();
        string linkedTokenStatus = linkedTokenQuerySucceeded ? "the linked token was unavailable or elevated" : "linked-token query failed with Win32 error " + linkedTokenError;
        if (linkedTokenQuerySucceeded && limitedToken != IntPtr.Zero)
        {
            try
            {
                if (!IsElevated(limitedToken)) return limitedToken;
            }
            catch
            {
                CloseHandle(limitedToken);
                throw;
            }
            CloseHandle(limitedToken);
            limitedToken = IntPtr.Zero;
            linkedTokenStatus = "the linked token was elevated";
        }

        IntPtr administratorsSid = IntPtr.Zero;
        try
        {
            if (!ConvertStringSidToSidW("S-1-5-32-544", out administratorsSid))
                throw new Win32Exception(Marshal.GetLastWin32Error(), "create the Administrators SID");
            SidAndAttributes disabledSid = new SidAndAttributes();
            disabledSid.Sid = administratorsSid;
            if (!CreateRestrictedToken(currentToken, DisableMaxPrivilege | LuaToken, 1, ref disabledSid, 0, IntPtr.Zero, 0, IntPtr.Zero, out limitedToken))
            {
                int error = Marshal.GetLastWin32Error();
                throw new Win32Exception(error, "create LUA token after " + linkedTokenStatus);
            }
            if (limitedToken == IntPtr.Zero)
                throw new InvalidOperationException("CreateRestrictedToken returned no token after " + linkedTokenStatus);
        }
        finally
        {
            if (administratorsSid != IntPtr.Zero) LocalFree(administratorsSid);
        }

        try
        {
            SetMediumIntegrity(limitedToken);
            if (IsElevated(limitedToken))
                throw new InvalidOperationException("the restricted Windows token is still elevated after " + linkedTokenStatus);
            return limitedToken;
        }
        catch
        {
            CloseHandle(limitedToken);
            throw;
        }
    }

    public static int Run(string application, string arguments, string currentDirectory, uint timeoutMilliseconds)
    {
        IntPtr currentToken = IntPtr.Zero;
        IntPtr limitedToken = IntPtr.Zero;
        ProcessInformation process = new ProcessInformation();
        try
        {
            if (!OpenProcessToken(GetCurrentProcess(), TokenQuery | TokenAssignPrimary | TokenDuplicate | TokenAdjustDefault, out currentToken))
                throw new Win32Exception(Marshal.GetLastWin32Error(), "open current Windows token");
            limitedToken = GetLimitedToken(currentToken);

            StartupInfo startup = new StartupInfo();
            startup.cb = Marshal.SizeOf(typeof(StartupInfo));
            StringBuilder commandLine = new StringBuilder("\"" + application + "\" " + arguments);
            if (!CreateProcessAsUserW(limitedToken, application, commandLine, IntPtr.Zero, IntPtr.Zero, false, CreateUnicodeEnvironment, IntPtr.Zero, currentDirectory, ref startup, out process))
            {
                int error = Marshal.GetLastWin32Error();
                throw new Win32Exception(error, "start the desktop smoke in the selected token's session (Win32 error " + error + ")");
            }

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
  $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
  $directorySecurity = Get-Acl -LiteralPath $smokeRoot
  $userAccess = [System.Security.AccessControl.FileSystemAccessRule]::new(
    $identity.User,
    [System.Security.AccessControl.FileSystemRights]::Modify,
    [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [System.Security.AccessControl.InheritanceFlags]::ObjectInherit,
    [System.Security.AccessControl.PropagationFlags]::None,
    [System.Security.AccessControl.AccessControlType]::Allow
  )
  $directorySecurity.AddAccessRule($userAccess)
  Set-Acl -LiteralPath $smokeRoot -AclObject $directorySecurity
  $settingsPath = Join-Path $smokeRoot "settings.json"
  $outputPath = Join-Path $smokeRoot "smoke.output.log"
  $resultPath = Join-Path $smokeRoot "smoke.result.json"
  $helperPath = [IO.Path]::GetFullPath($Helper)
  $scriptPath = [IO.Path]::GetFullPath($PSCommandPath)
  $workingDirectory = [IO.Path]::GetFullPath((Get-Location).Path)
  [ordered]@{
    ExpectedIdentity = $identity.Name
    RunnerTemp = $smokeRoot
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
