param([Parameter(Mandatory = $true)][string]$WorkingDirectory)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "codex_app_smoke_process.ps1")
$root = Join-Path $WorkingDirectory ("smoke process test " + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $root | Out-Null
try {
  $fixture = Join-Path $root "child script.ps1"
  'param([string]$Value, [int]$Result); [Console]::OutputEncoding = [Text.UTF8Encoding]::new($false); [Console]::Out.Write($Value); [Console]::Error.Write("stderr"); exit $Result' | Set-Content -LiteralPath $fixture -Encoding UTF8
  $value = "two words 中文 'quoted'"
  $arguments = @("-NoLogo", "-NoProfile", "-File", $fixture, "-Value", $value, "-Result", "0")
  $outputPath = Join-Path $root "success.out"
  $output = Invoke-SmokeProcess -FilePath (Join-Path $PSHOME "pwsh.exe") -Arguments $arguments -OutputPath $outputPath -WorkingDirectory $root
  if ($output -ne ($value + "stderr") -or (Get-Content -Raw -LiteralPath $outputPath) -ne $output) {
    $expectedBytes = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($value + "stderr"))
    $actualBytes = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes([string]$output))
    throw "the process runner lost UTF-8 output, argument boundaries, or stderr: expected=$expectedBytes actual=$actualBytes"
  }
  $arguments[-1] = "7"
  $outputPath = Join-Path $root "failure.out"
  $failure = ""
  try { Invoke-SmokeProcess -FilePath (Join-Path $PSHOME "pwsh.exe") -Arguments $arguments -OutputPath $outputPath -WorkingDirectory $root | Out-Null } catch { $failure = $_.Exception.Message }
  if ($failure -notmatch "exited with code 7" -or (Get-Content -Raw -LiteralPath $outputPath) -ne ($value + "stderr")) {
    throw "the process runner did not preserve a failing child's exit code and output"
  }
  $missing = Join-Path $root "missing.exe"
  $failure = ""
  try { Invoke-SmokeProcess -FilePath $missing -OutputPath (Join-Path $root "missing.out") | Out-Null } catch { $failure = $_.Exception.Message }
  if (!$failure.Contains($missing)) { throw "the process runner did not identify the executable that failed to start" }
  $diagnostics = Join-Path $root "diagnostics"
  $retained = Join-Path $root "retained"
  New-Item -ItemType Directory -Path $diagnostics, $retained | Out-Null
  Save-SmokeWingetDiagnostics -SourceDirectory (Join-Path $root "absent") -OutputDirectory $retained
  foreach ($index in 1..4) {
    $log = Join-Path $diagnostics ("test-$index.log")
    [IO.File]::WriteAllText($log, "diagnostic $index")
    [IO.File]::SetLastWriteTimeUtc($log, [DateTime]::UtcNow.AddMinutes($index))
  }
  [IO.File]::WriteAllText((Join-Path $diagnostics "private.json"), "not a diagnostic")
  Save-SmokeWingetDiagnostics -SourceDirectory $diagnostics -OutputDirectory $retained
  $files = @(Get-ChildItem -LiteralPath $retained -File)
  if ($files.Count -ne 3 -or (Test-Path -LiteralPath (Join-Path $retained "winget-test-1.log")) -or
      (Get-Content -Raw -LiteralPath (Join-Path $retained "winget-test-4.log")) -ne "diagnostic 4") {
    throw "WinGet diagnostics did not retain only the newest three logs with their original content"
  }
  $oversized = Join-Path $diagnostics "oversized.log"
  $stream = [IO.File]::Create($oversized)
  try { $stream.SetLength(4MB + 1) } finally { $stream.Dispose() }
  [IO.File]::SetLastWriteTimeUtc($oversized, [DateTime]::UtcNow.AddMinutes(10))
  Save-SmokeWingetDiagnostics -SourceDirectory $diagnostics -OutputDirectory $retained
  if (Test-Path -LiteralPath (Join-Path $retained "winget-oversized.log")) { throw "oversized WinGet diagnostic was copied" }
  $failure = ""
  try {
    try { throw "original Store failure" } finally {
      Save-SmokeWingetDiagnostics -SourceDirectory $diagnostics -OutputDirectory $fixture
    }
  } catch { $failure = $_.Exception.Message }
  if ($failure -ne "original Store failure") { throw "diagnostic collection hid the original Store failure" }
  Write-Host "Desktop smoke process runner behavior tests passed"
} finally {
  Remove-Item -LiteralPath $root -Recurse -Force
}
