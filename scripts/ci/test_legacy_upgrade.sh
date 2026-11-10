#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
scratch="$(mktemp -d "${CXP_TUI_TEST_TMPDIR:-$PWD}/.cxp-legacy-e2e.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT
export TMPDIR="$scratch"
if [[ "${OS:-}" == Windows_NT ]]; then
  export TMPDIR="$(cygpath -w "$scratch")" TMP="$(cygpath -w "$scratch")" TEMP="$(cygpath -w "$scratch")"
fi
python3 scripts/ci/prepare_legacy_upgrade.py --scratch "$TMPDIR"
python3 scripts/ci/prepare_legacy_upgrade.py --baseline native --scratch "$TMPDIR"
export CXP_LEGACY_FIXTURE="$TMPDIR/rc96"
export CXP_NATIVE_FIXTURE="$TMPDIR/rc104"
[[ "${OS:-}" != Windows_NT ]] || CXP_LEGACY_FIXTURE="$TMPDIR/rc96.exe"
[[ "${OS:-}" != Windows_NT ]] || CXP_NATIVE_FIXTURE="$TMPDIR/rc104.exe"
selected="$(go test -tags cxp_boot_e2e ./internal/candidateupdate -list '^TestCandidateLegacy')"
[[ "$(grep -c '^TestCandidateLegacy' <<<"$selected")" -ge 2 ]] || { echo 'legacy upgrade tests not selected' >&2; exit 1; }
go test -tags cxp_boot_e2e ./internal/candidateupdate -run '^TestCandidateLegacy' -count=1 -timeout=5m -v
