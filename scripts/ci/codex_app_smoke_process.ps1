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
