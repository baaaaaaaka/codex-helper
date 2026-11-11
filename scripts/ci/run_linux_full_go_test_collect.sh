#!/usr/bin/env bash

# Run the Linux full Go-test coverage suites to completion. The package-wide
# suite and the deliberately isolated suites are separate failure domains: a
# failure in one must not prevent the remaining suites from producing their
# logs, coverage, and diagnostics for the same CI attempt.

set -u -o pipefail

go_bin="${CXP_CI_GO_BIN:-go}"
runner_temp="${RUNNER_TEMP:-${TMPDIR:-}}"
if [[ -z "$runner_temp" ]]; then
  echo "RUNNER_TEMP or TMPDIR must identify a writable CI scratch directory" >&2
  exit 2
fi
coverage_file="${PWD}/coverage.out"
if ! mkdir -p "$runner_temp"; then
  echo "cannot create CI scratch directory: $runner_temp" >&2
  exit 2
fi

migration_process_pattern='^TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup$'
isolated_skip_pattern='^(?:TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup)$'

failures=()

# The end-to-end scenarios run in their own CI job (one SUT build per package
# and a wall-clock budget of their own); every other package is covered here.
if ! packages_text="$("$go_bin" list ./... 2>"$runner_temp/full-go-test-list.err")"; then
  cat "$runner_temp/full-go-test-list.err" >&2
  echo "::error title=Full Go test failure::go list ./... failed"
  exit 1
fi
packages=()
while IFS= read -r pkg; do
  case "$pkg" in
    */internal/e2e/scenarios/*) ;;
    "") ;;
    *) packages+=("$pkg") ;;
  esac
done <<< "$packages_text"

failure_summary() {
  local log=$1
  local summary
  summary="$(grep -E -A8 '^--- FAIL:|^FAIL[[:space:]]|^panic:|^fatal error:' "$log" | tail -80 | tr '\n' ' ' || true)"
  if ! grep -Eq '^--- FAIL:|^panic:|^fatal error:' "$log"; then
    summary="$(tail -40 "$log" | tr '\n' ' ' || true)"
  fi
  printf '%s' "${summary:-go test failed without a recognized summary; inspect the retained step log}"
}

append_coverage_profile() {
  local profile=$1
  if [[ "$profile" == "$coverage_file" ]]; then
    return 0
  fi
  if [[ ! -s "$profile" ]]; then
    echo "::warning title=Missing coverage profile::${profile} was not produced"
    return 0
  fi
  if [[ ! -s "$coverage_file" ]]; then
    cp "$profile" "$coverage_file"
    return 0
  fi
  tail -n +2 "$profile" >> "$coverage_file"
}

run_suite() {
  local label=$1
  local log=$2
  local profile=$3
  shift 3

  echo "::group::${label}"
  echo "running: ${go_bin} $*"
  "$go_bin" "$@" 2>&1 | tee "$log"
  local status=${PIPESTATUS[0]}
  echo "::endgroup::"

  if (( status != 0 )); then
    failures+=("$label")
    echo "::error title=${label} failure::$(failure_summary "$log")"
  fi
  append_coverage_profile "$profile"
  return 0
}

run_suite \
  "Full Go test" \
  "$runner_temp/full-go-test.log" \
  "$coverage_file" \
  test -count=1 -timeout=20m -parallel=16 -skip "$isolated_skip_pattern" -coverprofile="$coverage_file" "${packages[@]}"

run_suite \
  "Isolated migration process" \
  "$runner_temp/migration-process-isolated.log" \
  "$runner_temp/migration-process-coverage.out" \
  test ./internal/cli -timeout=2m -parallel=16 -count=1 -run "$migration_process_pattern" -coverprofile="$runner_temp/migration-process-coverage.out" -v

if (( ${#failures[@]} != 0 )); then
  printf 'full Linux Go test suites failed after collection: %s\n' "${failures[*]}" >&2
  exit 1
fi

echo "full Linux Go test suites passed: 2 suite(s)"
