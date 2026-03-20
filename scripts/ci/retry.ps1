param(
  [Parameter(Mandatory = $true, Position = 0)]
  [int]$Attempts,
  [Parameter(Mandatory = $true, Position = 1)]
  [int]$SleepSeconds,
  [Parameter(Mandatory = $true, Position = 2, ValueFromRemainingArguments = $true)]
  [string[]]$Command
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

if ($Attempts -lt 1) {
  throw "Attempts must be >= 1"
}

for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
  try {
    $commandName = $Command[0]
    $commandArgs = @()
    if ($Command.Length -gt 1) {
      $commandArgs = $Command[1..($Command.Length - 1)]
    }
    & $commandName @commandArgs
    return
  } catch {
    if ($attempt -eq $Attempts) {
      break
    }
    Write-Warning ("command failed (attempt {0}/{1}), retrying in {2}s: {3}" -f $attempt, $Attempts, $SleepSeconds, ($Command -join " "))
    Start-Sleep -Seconds $SleepSeconds
  }
}

throw ("command failed after {0} attempts: {1}" -f $Attempts, ($Command -join " "))
