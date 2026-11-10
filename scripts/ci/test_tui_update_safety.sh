#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
mode="${1:-all}"
case "$mode" in unit|e2e|all) ;; *) echo "usage: $0 [unit|e2e|all]" >&2; exit 2 ;; esac

if [[ "$mode" != e2e ]]; then
  go test ./internal/helperruntime ./internal/update ./internal/candidateupdate ./internal/tui ./internal/cli ./cmd/codex-proxy \
    -run '^Test(Legacy|Historical|SQLiteBootstrap|SafeBoot|TUIAuto|TUIBoot|SelectSessionReadiness|ApplyVerification|RunMainHandlesBootProtocol)' -count=1
fi
[[ "$mode" != unit ]] || exit 0

# The harness may mount /tmp noexec. Validate a private, executable scratch
# directory instead of assuming /tmp or an external cache is suitable.
scratch="$(mktemp -d "${CXP_TUI_TEST_TMPDIR:-$PWD}/.cxp-tui-e2e.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT
export TMPDIR="$scratch"
export CXP_RUNTIME_DISABLE=1
suffix=""
[[ "${OS:-}" != Windows_NT ]] || suffix=.exe
if [[ "${OS:-}" == Windows_NT ]]; then
  export TMPDIR="$(cygpath -w "$scratch")"
  export TMP="$TMPDIR" TEMP="$TMPDIR"
fi

if [[ -z "${CXP_BOOT_FIXTURE:-}" ]]; then
  export CXP_BOOT_FIXTURE="$scratch/fixture$suffix"
  [[ "${OS:-}" != Windows_NT ]] || CXP_BOOT_FIXTURE="$(cygpath -w "$CXP_BOOT_FIXTURE")"
  CGO_ENABLED=0 go build -o "$CXP_BOOT_FIXTURE" ./internal/helperruntime/testdata/bootfixture
fi
"$CXP_BOOT_FIXTURE" --version
if [[ -z "${CXP_TUI_SUT_BINARY:-}" ]]; then
  export CXP_TUI_SUT_BINARY="$scratch/cxp$suffix"
  [[ "${OS:-}" != Windows_NT ]] || CXP_TUI_SUT_BINARY="$(cygpath -w "$CXP_TUI_SUT_BINARY")"
  CGO_ENABLED=0 go build -trimpath -o "$CXP_TUI_SUT_BINARY" ./cmd/codex-proxy
fi
if [[ "${OS:-}" == Windows_NT ]]; then
  export CXP_BOOT_FIXTURE="$(cygpath -w "$CXP_BOOT_FIXTURE")"
  export CXP_TUI_SUT_BINARY="$(cygpath -w "$CXP_TUI_SUT_BINARY")"
fi
"$CXP_TUI_SUT_BINARY" --version

if [[ -n "${CXP_BOOT_RUNNER:-}" ]]; then
  "$CXP_BOOT_RUNNER" -test.run='^TestBootE2E' -test.v -test.timeout=180s
  "$CXP_BOOT_UPDATE_RUNNER" -test.run='^TestTUIAutoNative' -test.v -test.timeout=90s
else
  selected="$(go test -tags cxp_boot_e2e ./internal/helperruntime -list '^TestBootE2E')"
  if [[ "$(printf '%s\n' "$selected" | grep -c '^TestBootE2E')" -lt 9 ]]; then
    echo "critical native boot tests were not selected" >&2
    exit 1
  fi
  update_tests="$(go test -tags cxp_boot_e2e ./internal/update -list '^TestTUIAutoNativeDownloadThenRealNextInvocation$')"
  if ! grep -Fxq 'TestTUIAutoNativeDownloadThenRealNextInvocation' <<<"$update_tests"; then
    echo "critical native TUI update test was not selected" >&2
    exit 1
  fi
  go test -tags cxp_boot_e2e ./internal/helperruntime ./internal/update \
    -run '^Test(BootE2E|TUIAutoNative)' -count=1 -timeout=180s -v
fi

if [[ "${OS:-}" != Windows_NT ]]; then
  python3 scripts/ci/tui_update_pty.py --binary "$CXP_TUI_SUT_BINARY" --fault-fixture "$CXP_BOOT_FIXTURE"
fi

# This must run before publication, against a frozen historical caller rather
# than a current source build with an old version string.
bash scripts/ci/test_legacy_upgrade.sh
