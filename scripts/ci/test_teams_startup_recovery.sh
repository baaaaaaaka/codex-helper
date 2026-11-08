#!/usr/bin/env bash
# Keep the startup ownership contract visible beside the full-suite shards.
# Run from the repository root; no credentials or installed service is needed.
set -euo pipefail

go_bin="${CXP_CI_GO_BIN:-go}"
mode="${1:-unit}"

run_tests() {
  local package=$1 pattern=$2 required=$3 count=$4
  local listed
  listed="$("$go_bin" test "$package" -list "$pattern")"
  local expected
  local -a names
  IFS=',' read -r -a names <<< "$required"
  for expected in "${names[@]}"; do
    if ! printf '%s\n' "$listed" | grep -Fxq "$expected"; then
      echo "required startup recovery test is missing: $package/$expected" >&2
      exit 1
    fi
  done
  "$go_bin" test "$package" -run "$pattern" -count="$count" -timeout=5m -v
}

case "$mode" in
  unit)
    run_tests ./internal/core/migrate '^TestConversion' 'TestConversionOwnershipAtEveryCheckpoint,TestConversionCanceledBeforeStartCreatesNothing' 1
    run_tests ./internal/cli '^Test(TeamsCoreStore|FirstStart|TeamsServiceWatchdog|RunTeamsServiceWatchdog)' 'TestTeamsCoreStoreDoesNotWriteAfterLeaseLoss,TestTeamsCoreStoreLossAfterPreparationStarts,TestTeamsCoreStoreResumesMigrationAfterAbortedStart,TestTeamsCoreStorePreparationAfterMigrationBeforeActivation,TestTeamsServiceWatchdogRefreshesRuntimeBeforeHealthCheck,TestTeamsServiceWatchdogRuntimeProcessUpgradeAndRollback' 1
    ;;
  e2e)
    run_tests ./internal/e2e/scenarios/entry '^Test(EntryMigration|EntryWatchdogRuntime)' 'TestEntryMigrationResumesAfterInterruptedStart,TestEntryMigrationResumesAfterCrashDuringImport,TestEntryMigrationTakeoverFencesWaitingOwner,TestEntryMigrationTakeoverFencesImporter,TestEntryWatchdogRuntimeUpgradeAndRollback,TestEntryMigrationRefusesMissingPublishedDatabase' 3
    ;;
  *)
    echo "usage: $0 [unit|e2e]" >&2
    exit 2
    ;;
esac
