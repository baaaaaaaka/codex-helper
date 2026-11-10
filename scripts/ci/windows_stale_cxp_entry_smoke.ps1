# Native bootstrap is immutable across updates. Test an older protocol-two
# entry selecting a newer runtime, including a locked Windows entry handle.
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
if ($env:OS -ne "Windows_NT") { throw "Windows required" }
Push-Location (Join-Path $PSScriptRoot "..\..")
try {
  go test -tags cxp_boot_e2e ./internal/candidateupdate -run '^TestCandidateNativeInstallUpdateAndRecovery$' -count=1 -timeout=5m -v
  if ($LASTEXITCODE -ne 0) { throw "native candidate/locked-entry contract failed" }
} finally { Pop-Location }
