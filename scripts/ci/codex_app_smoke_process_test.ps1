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
  Write-Host "Desktop smoke process runner behavior tests passed"
} finally {
  Remove-Item -LiteralPath $root -Recurse -Force
}
