//go:build windows

package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestWindowsBootstrapVCRedistRuntimeFunctions(t *testing.T) {
	script := windowsVCRedistFunctionBlock(t) + `
$ErrorActionPreference = 'Stop'

function New-CodexExe([string]$Path) {
  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Path) | Out-Null
  New-Item -ItemType File -Force -Path $Path | Out-Null
}

$npmPrefix = Join-Path $env:TEMP ("codex-npm-" + [Guid]::NewGuid().ToString('N'))
try {
  $openaiRoot = Join-Path $npmPrefix 'node_modules\@openai'
  New-CodexExe (Join-Path $openaiRoot 'codex-win32-arm64\vendor\aarch64-pc-windows-msvc\codex\codex.exe')
  New-CodexExe (Join-Path $openaiRoot 'codex-win32-x64\vendor\x86_64-pc-windows-msvc\codex\codex.exe')

  $nodeDir = Join-Path $env:TEMP 'node-v22.0.0-win-x64'
  if ((Resolve-CodexNativeRuntimeArch) -ne 'x64') {
    throw 'x64 Node must win when stale ARM64 and x64 Codex packages both exist'
  }
  $target = Get-CodexVCRedistTarget
  if ($target.WingetId -ne 'Microsoft.VCRedist.2015+.x64') {
    throw "expected x64 winget id, got $($target.WingetId)"
  }

  $nodeDir = Join-Path $env:TEMP 'node-v22.0.0-win-arm64'
  if ((Resolve-CodexNativeRuntimeArch) -ne 'arm64') {
    throw 'ARM64 Node must select the ARM64 Codex package'
  }
  $target = Get-CodexVCRedistTarget
  if ($target.WingetId -ne 'Microsoft.VCRedist.2015+.arm64') {
    throw "expected ARM64 winget id, got $($target.WingetId)"
  }
  if ($target.DownloadUrl -ne 'https://aka.ms/vc14/vc_redist.arm64.exe') {
    throw "expected ARM64 download URL, got $($target.DownloadUrl)"
  }

  if (-not (Test-CodexNativeRuntimeRepairable -1073741515)) {
    throw 'signed STATUS_DLL_NOT_FOUND must be repairable'
  }
  if (-not (Test-CodexNativeRuntimeRepairable 3221225781)) {
    throw 'unsigned STATUS_DLL_NOT_FOUND must be repairable'
  }
  if (-not (Test-CodexNativeRuntimeRepairable 3221225785)) {
    throw 'unsigned STATUS_ENTRYPOINT_NOT_FOUND must be repairable'
  }

  $script:codexProbeNativeStartupStatus = 3221225781
  $script:wingetRepairCalled = $false
  function Confirm-CodexVCRedistInstall { return $true }
  function Install-CodexVCRedistWithWinget {
    $script:wingetRepairCalled = $true
    return $true
  }
  function Install-CodexVCRedistFromMicrosoft {
    throw 'fallback installer should not run after winget repair succeeds'
  }
  if (-not (Install-CodexVCRedistIfNeeded)) {
    throw 'expected repairable native status to trigger VC++ repair'
  }
  if (-not $script:wingetRepairCalled) {
    throw 'expected winget repair path to run'
  }

  $script:codexProbeNativeStartupStatus = 1
  $script:wingetRepairCalled = $false
  if (Install-CodexVCRedistIfNeeded) {
    throw 'generic failure should not trigger VC++ repair'
  }
  if ($script:wingetRepairCalled) {
    throw 'generic failure must not call winget repair'
  }
} finally {
  Remove-Item -Recurse -Force $npmPrefix -ErrorAction SilentlyContinue
}
`
	runWindowsPowerShellScript(t, script)
}

func windowsVCRedistFunctionBlock(t *testing.T) string {
	t.Helper()
	start := strings.Index(codexInstallBootstrapWindows, "function Get-CommonWindowsRuntimeDllStatus")
	if start < 0 {
		t.Fatal("missing VC++ runtime function block start")
	}
	end := strings.Index(codexInstallBootstrapWindows[start:], "function Set-CodexManagedNodeShims")
	if end < 0 {
		t.Fatal("missing VC++ runtime function block end")
	}
	return codexInstallBootstrapWindows[start : start+end]
}

func runWindowsPowerShellScript(t *testing.T, script string) {
	t.Helper()
	powershell, err := exec.LookPath("powershell.exe")
	if err != nil {
		powershell, err = exec.LookPath("pwsh.exe")
	}
	if err != nil {
		t.Skip("PowerShell not available")
	}
	scriptPath := filepath.Join(t.TempDir(), "codex-vcredist-test.ps1")
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf("write PowerShell script: %v", err)
	}
	cmd := exec.Command(powershell, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", scriptPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("PowerShell script failed: %v\n%s", err, out)
	}
}
