function Save-SmokeWingetDiagnostics {
  param(
    [Parameter(Mandatory = $true)]
    [string]$SourceDirectory,
    [Parameter(Mandatory = $true)]
    [string]$OutputDirectory
  )

  try {
    if (!(Test-Path -LiteralPath $SourceDirectory -PathType Container)) { return }
    $logs = @(Get-ChildItem -LiteralPath $SourceDirectory -Filter "*.log" -File |
      Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 3)
    foreach ($log in $logs) {
      if ($log.Length -gt 4MB) { Write-Warning "Skipping oversized WinGet diagnostic: $($log.Name)"; continue }
      Copy-Item -LiteralPath $log.FullName -Destination (Join-Path $OutputDirectory ("winget-" + $log.Name))
    }
  } catch {
    Write-Warning "Could not retain WinGet diagnostics: $($_.Exception.Message)"
  }
}

function Invoke-SmokeProcess {
  param(
    [Parameter(Mandatory = $true)]
    [string]$FilePath,
    [string[]]$Arguments = @(),
    [Parameter(Mandatory = $true)]
    [string]$OutputPath,
    [string]$WorkingDirectory = (Get-Location).Path
  )

  $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
  $startInfo.FileName = [IO.Path]::GetFullPath($FilePath)
  $startInfo.WorkingDirectory = $WorkingDirectory
  $startInfo.UseShellExecute = $false
  $startInfo.CreateNoWindow = $true
  $startInfo.RedirectStandardInput = $true
  $startInfo.RedirectStandardOutput = $true
  $startInfo.RedirectStandardError = $true
  $startInfo.StandardOutputEncoding = [System.Text.UTF8Encoding]::new($false)
  $startInfo.StandardErrorEncoding = [System.Text.UTF8Encoding]::new($false)
  foreach ($argument in $Arguments) {
    [void]$startInfo.ArgumentList.Add([string]$argument)
  }

  $process = [System.Diagnostics.Process]::new()
  try {
    $process.StartInfo = $startInfo
    try {
      $started = $process.Start()
    } catch {
      throw "failed to start process $($startInfo.FileName): $($_.Exception.Message)"
    }
    if (-not $started) {
      throw "failed to start process: $FilePath"
    }
    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $process.StandardInput.Close()
    $process.WaitForExit()
    $output = $stdoutTask.GetAwaiter().GetResult() + $stderrTask.GetAwaiter().GetResult()
    [IO.File]::WriteAllText($OutputPath, $output, [System.Text.UTF8Encoding]::new($false))
    if ($process.ExitCode -ne 0) {
      throw "process exited with code $($process.ExitCode): $FilePath`n$output"
    }
    return $output
  } finally {
    $process.Dispose()
  }
}
