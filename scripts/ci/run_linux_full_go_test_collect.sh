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

frontier_recovery_pattern='^TestTeamsListenFalsePollFrontierSurvivesStoreReopenAndOwnerTakeover$'
migration_process_pattern='^TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup$'
external_perf_pattern='^(?:TestCXPPerfModelExternalScenariosCoverCommonPaths|TestCXPPerfModelSQLiteExternalScenariosCoverCommonPaths)$'
teams_durable_pattern='^(?:TestBridgeSyncLinkedTranscriptPersistsHistoryQuarantineWithLegacyGeneration|TestBridgeSyncLinkedTranscriptReleasesPendingRootAcrossSQLiteStoreReopen|TestTeamsListenFalseMalformedActiveSQLitePollDoesNotBaseline|TestTeamsListenFalsePolledTurnOutboxSurvivesReopen|TestTeamsOwnershipStressSQLiteHeartbeatSurvivesSaturatedGraphWorkersCI)$'
store_durable_pattern='^(?:TestGlobalInboundSQLiteWriterDefersPruneUntilClose|TestSQLiteHotPollAdmissionAdmitsMalformedPollWithoutStarvingHealthyChat|TestSQLiteHotPollReadyScheduleSkipsMalformedOperationalPrefix|TestSQLiteLegacyEmptyRuntimeProjectionBootstrapsOnceForHotPoll|TestSQLiteMalformedNumericCompatibilityColumnsDoNotAbortRecovery|TestSQLiteHotPollTrustedWorkCandidatesContinuesPastInvalidPage|TestSQLiteHotPollAdmissionUsesJSONFrontierHonorsBlockedUntilAndReservesControl|TestStoreOwnerBoundOutboxAdmissionRejectsStaleOwnerAcrossBackends|TestSQLiteActiveJSONSessionSurvivesStaleSQLStatus)$'
isolated_skip_pattern='^(?:TestTeamsListenFalsePollFrontierSurvivesStoreReopenAndOwnerTakeover|TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup|TestCXPPerfModelExternalScenariosCoverCommonPaths|TestCXPPerfModelSQLiteExternalScenariosCoverCommonPaths|TestBridgeSyncLinkedTranscriptPersistsHistoryQuarantineWithLegacyGeneration|TestBridgeSyncLinkedTranscriptReleasesPendingRootAcrossSQLiteStoreReopen|TestTeamsListenFalseMalformedActiveSQLitePollDoesNotBaseline|TestTeamsListenFalsePolledTurnOutboxSurvivesReopen|TestTeamsOwnershipStressSQLiteHeartbeatSurvivesSaturatedGraphWorkersCI|TestGlobalInboundSQLiteWriterDefersPruneUntilClose|TestSQLiteHotPollAdmissionAdmitsMalformedPollWithoutStarvingHealthyChat|TestSQLiteHotPollReadyScheduleSkipsMalformedOperationalPrefix|TestSQLiteLegacyEmptyRuntimeProjectionBootstrapsOnceForHotPoll|TestSQLiteMalformedNumericCompatibilityColumnsDoNotAbortRecovery|TestSQLiteHotPollTrustedWorkCandidatesContinuesPastInvalidPage|TestSQLiteHotPollAdmissionUsesJSONFrontierHonorsBlockedUntilAndReservesControl|TestStoreOwnerBoundOutboxAdmissionRejectsStaleOwnerAcrossBackends|TestSQLiteActiveJSONSessionSurvivesStaleSQLStatus)$'

failures=()

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
  test -timeout=20m -parallel=16 -skip "$isolated_skip_pattern" -coverprofile="$coverage_file" ./...

run_suite \
  "Isolated frontier recovery" \
  "$runner_temp/frontier-recovery-isolated.log" \
  "$runner_temp/frontier-recovery-coverage.out" \
  test ./internal/teams -timeout=2m -parallel=16 -count=1 -run "$frontier_recovery_pattern" -coverprofile="$runner_temp/frontier-recovery-coverage.out" -v

run_suite \
  "Isolated migration process" \
  "$runner_temp/migration-process-isolated.log" \
  "$runner_temp/migration-process-coverage.out" \
  test ./internal/cli -timeout=2m -parallel=16 -count=1 -run "$migration_process_pattern" -coverprofile="$runner_temp/migration-process-coverage.out" -v

run_suite \
  "Isolated external perf" \
  "$runner_temp/external-perf-isolated.log" \
  "$runner_temp/external-perf-coverage.out" \
  test ./internal/teams -timeout=12m -parallel=1 -count=1 -run "$external_perf_pattern" -coverprofile="$runner_temp/external-perf-coverage.out" -v

run_suite \
  "Isolated Teams durable" \
  "$runner_temp/teams-durable-isolated.log" \
  "$runner_temp/teams-durable-coverage.out" \
  test ./internal/teams -timeout=12m -parallel=1 -count=1 -run "$teams_durable_pattern" -coverprofile="$runner_temp/teams-durable-coverage.out" -v

run_suite \
  "Isolated store durable" \
  "$runner_temp/store-durable-isolated.log" \
  "$runner_temp/store-durable-coverage.out" \
  test ./internal/teams/store -timeout=12m -parallel=1 -count=1 -run "$store_durable_pattern" -coverprofile="$runner_temp/store-durable-coverage.out" -v

if (( ${#failures[@]} != 0 )); then
  printf 'full Linux Go test suites failed after collection: %s\n' "${failures[*]}" >&2
  exit 1
fi

echo "full Linux Go test suites passed: 6 suite(s)"
