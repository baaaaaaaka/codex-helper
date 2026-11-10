#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
unset CXP_RUNTIME CXP_RUNTIME_ROOT CXP_RUNTIME_VERSION CXP_ENTRY_PATH CXP_RUNTIME_DISABLE CXP_RUNTIME_FORCE
selected="$(go test -tags cxp_boot_e2e ./internal/candidateupdate -list '^TestCandidateNativeInstallUpdateAndRecovery$')"
grep -Fxq 'TestCandidateNativeInstallUpdateAndRecovery' <<<"$selected" || { echo 'native candidate test not selected' >&2; exit 1; }
go test -tags cxp_boot_e2e ./internal/candidateupdate -run '^TestCandidateNativeInstallUpdateAndRecovery$' -count=1 -timeout=5m -v
