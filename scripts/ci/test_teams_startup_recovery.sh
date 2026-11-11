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
  # The migration-recovery scenario is a critical real-process acceptance
  # gate. Existence alone is insufficient: a runtime skip must fail closed.
  local critical=TestEntryMigrationResumesAfterInterruptedStart
  if [[ ",$required," != *",$critical,"* ]]; then
    "$go_bin" test "$package" -run "$pattern" -count="$count" -timeout=5m -v
    return
  fi
  local output
  output="$(mktemp "${TMPDIR:-/tmp}/cxp-recovery-gate.XXXXXX")"
  if ! "$go_bin" test "$package" -run "$pattern" -count="$count" -timeout=5m -v | tee "$output"; then
    rm -f "$output"
    return 1
  fi
  local passed
  passed="$(awk -v name="$critical" '$1 == "---" && $2 == "PASS:" && $3 == name { n++ } END { print n+0 }' "$output")"
  rm -f "$output"
  if [[ "$passed" -ne "$count" ]]; then
    echo "required recovery test passed $passed times, expected $count: $critical" >&2
    return 1
  fi
}

case "$mode" in
  unit)
    run_tests ./internal/core/store '^TestPublishedCoreStore' 'TestPublishedCoreStoreNeverInitializesLostSchema' 1
    run_tests ./internal/helperruntime '^TestSQLiteBootstrap' 'TestSQLiteBootstrapPublishedLossIsNotFreshInstall,TestSQLiteBootstrapExplicitMigrationDropsIncompatibleRollback,TestSQLiteBootstrapMigrationFailurePreservesLegacy,TestSQLiteBootstrapAutomaticUpdateRequiresExplicitMigration,TestSQLiteBootstrapRetryAfterCommittedBeforeArchive' 1
    run_tests ./internal/cli '^TestTeamsExchangeSQLite' 'TestTeamsExchangeSQLiteAtomicCompletionAndConditionalAck,TestTeamsExchangeSQLiteLegacyAtomicMigration' 1
    run_tests ./internal/sqlitestate '^TestSQLiteOnly' 'TestSQLiteOnlyPersistenceBoundary' 1
    run_tests ./internal/sqlitestate '^TestSQLiteSnapshot' 'TestSQLiteSnapshotAtomicFailureAndReadOnly' 1
    run_tests ./internal/core/lease '^TestLeaseNativeSlot' 'TestLeaseNativeSlotAndLegacyTakeoverEvidence' 1
    run_tests ./internal/core/tuilease '^Test(TUILease|Descriptor)' 'TestTUILeaseLifetimeAndAuthentication,TestTUILeaseFallbackOnlyForUnmanagedThread,TestDescriptorRejectsRemoteAndCredentialURLs' 1
    run_tests ./internal/cli '^Test(TeamsCommandsReadThePublished|TeamsRemoteStoreState|TeamsStatusPublisher)' 'TestTeamsCommandsReadThePublishedStateWhileAHelperHoldsTheStore,TestTeamsRemoteStoreStateUsesThePublishedFile,TestTeamsStatusPublisherFollowsCommits' 1
    run_tests ./internal/updatestate '^TestUpdateSQLite' 'TestUpdateSQLiteMigrationAndQuiet,TestUpdateSQLiteMigrationRollsBackAndReadOnly,TestUpdateSQLiteInvocationAndActivationEntry,TestUpdateSQLiteInvocationCrashRetention,TestUpdateSQLiteMissingPublishedDatabase' 1
    run_tests ./internal/sqlitestate '^TestPublication' 'TestPublicationMissingDatabaseFailsClosed,TestPublicationRejectsInvalidMarkerAndEmptyDatabase' 1
    run_tests ./internal/core/codex '^Test(PIDSQLite|ConcurrentPIDFileWrites)' 'TestPIDSQLiteMigrationAndQuietWrites,TestPIDSQLiteMigrationRollsBack,TestPIDSQLiteKeyTransactionRollsBack,TestConcurrentPIDFileWritesStayValid,TestPIDSQLiteIndependentOpeners' 1
    run_tests ./internal/cli '^TestWindowsAppSQLite' 'TestWindowsAppSQLiteMigrationAndQuiet,TestWindowsAppSQLiteMigrationRollback' 1
    run_tests ./internal/asr '^TestASRSQLite' 'TestASRSQLiteServerRecords,TestASRSQLiteServerMigrationRollback,TestASRSQLiteInstallManifest,TestASRSQLiteInstallMigration' 1
    run_tests ./internal/managedinstall '^TestInstallSQLite' 'TestInstallSQLiteMigrationAndQuiet,TestInstallSQLiteRejectsBadSources,TestInstallSQLiteInstallerEntry' 1
    run_tests ./internal/channel/teams '^TestFontSQLite' 'TestFontSQLiteMigrationAndReadOnly,TestFontSQLiteMigrationRejectsBadInputs' 1
    run_tests ./internal/sqlitestate '^TestOpen' 'TestOpenConcurrentAndReadOnly,TestOpenRejectsNonFilesAndWAL' 1
    run_tests ./internal/config '^TestConfigSQLite' 'TestConfigSQLiteReadOnlyRequiresMigration,TestConfigSQLiteMigrationAndQuietWrites,TestConfigSQLiteMigrationRollsBack,TestConfigSQLitePointQueryUsesIndex,TestConfigSQLiteMissingPublishedDatabase,TestConfigSQLitePointEditsPreserveUnrelatedCollections,TestConfigSQLitePrunePreservesConcurrentProgress,TestConfigSQLitePruneBatchRollsBack' 1
    run_tests ./internal/cli '^TestServiceSQLite' 'TestServiceSQLiteLegacyMigrationAndQuietWrites,TestServiceSQLiteMigrationRollbackAndCorruption,TestServiceSQLiteReadonlyDoesNotCreateState' 1
    run_tests ./internal/cli '^TestSupervisorSQLite' 'TestSupervisorSQLiteLegacyIdentityAndConfig,TestSupervisorSQLiteQuietAndChildRollback,TestSupervisorSQLiteConfigChildRollbackAndQuiet' 1
    run_tests ./internal/channel/teams '^Test(AuthSQLite|DelegatedAuth)' 'TestAuthSQLiteMigrationAtomicAndQuiet,TestAuthSQLiteMigrationRollsBack,TestAuthSQLiteFutureCorruptAndMissingFailClosed,TestAuthSQLiteCustomLegacyPathsKeepDistinctCredentials,TestDelegatedAuthRefreshIsSerialisedAcrossInstances' 1
    run_tests ./internal/appgateway '^TestGatewaySQLite' 'TestGatewaySQLiteMigrationAndQuietWrites,TestGatewaySQLiteMigrationRollsBackAndRejectsCorruption,TestGatewaySQLiteConcurrentInitialization' 1
    run_tests ./internal/skills '^TestSkillsSQLite' 'TestSkillsSQLiteMigrationAndQuietWrites,TestSkillsSQLiteMigrationRollsBack,TestSkillsSQLiteSubscriptionRollbackAndReviewIsolation,TestSkillsSQLiteManifestMigrationAndPublication' 1
    run_tests ./internal/core/delegate/machineregistry '^TestRegistryCacheSQLite' 'TestRegistryCacheSQLiteMigrationAndQuietReads,TestRegistryCacheSQLiteRejectsBadInputs' 1
    run_tests ./internal/core/delegate/delegation '^Test(WorkerSQLite|ReserveOutbox|SQLiteWorker)' 'TestWorkerSQLiteMigrationPreservesSafetyEvidence,TestWorkerSQLiteMigrationRollsBack,TestWorkerSQLiteLegacyJSONFailureIsAtomic,TestWorkerSQLiteUnchangedReadAndWriteAreQuiet,TestWorkerSQLiteRetentionMatchesExistingPolicy,TestWorkerSQLiteRetentionSeeksPastMissingTimestamps,TestWorkerSQLiteArchiveIsAfterAuthoritySwitch,TestWorkerSQLiteConcurrentStartAndGenerationRollback,TestWorkerSQLiteReservationAcrossProcesses,TestReserveOutboxAllowsOnlyOneConcurrentNonIdempotentOwner' 1
    run_tests ./internal/core/store '^Test(EngineStateMigration|EngineManagementMigration|EngineImportMigration|EngineUploadedReferenceMigration|EngineBadUploadedReference|EngineTypedStateUnchanged|EngineMeta|EngineWAL|Notification)' 'TestEngineStateMigrationFrozenV5,TestEngineStateMigrationRollsBack,TestEngineWALRecoversAfterRetainedReader,TestEngineManagementMigration,TestEngineTypedStateUnchangedWritesNothing,TestEngineImportMigrationPreservesArrayOrder,TestEngineUploadedReferenceMigration,TestEngineBadUploadedReferenceRollsBack,TestNotificationEventQueriesUsePointIndexes,TestEngineMetaRejectsFormerObjects,TestEngineMetaScalarBoundaryAndServiceEvidence,TestEngineMetaUnmappedLegacyStateRollsBack,TestNotificationUnknownBacklogDoesNotHideReady,TestNotificationCardAttemptIsDurable' 1
    run_tests ./internal/helperruntime '^TestVersionPointer' 'TestVersionPointerReplacementWhileReaderOpen,TestVersionPointerReadsLongPath' 1
    run_tests ./internal/candidateupdate '^TestHistoricalVersionProbeIsSuccessfulAndInert$' 'TestHistoricalVersionProbeIsSuccessfulAndInert' 1
    run_tests ./internal/core/migrate '^TestConversion' 'TestConversionOwnershipAtEveryCheckpoint,TestConversionCanceledBeforeStartCreatesNothing' 1
    run_tests ./internal/cli '^Test(LegacyTeamsWatchdogHandoff|TeamsCoreStore|FirstStart|TeamsServiceWatchdog|RunTeamsServiceWatchdog)' 'TestLegacyTeamsWatchdogHandoff,TestTeamsServiceWatchdogObservesUnpublishedMigrationClaim,TestTeamsCoreStoreDoesNotWriteAfterLeaseLoss,TestTeamsCoreStoreLossAfterPreparationStarts,TestTeamsCoreStoreResumesMigrationAfterAbortedStart,TestTeamsCoreStorePreparationAfterMigrationBeforeActivation,TestTeamsServiceWatchdogRefreshesRuntimeBeforeHealthCheck,TestTeamsServiceWatchdogRuntimeRefreshErrorsDoNotStopLoop,TestTeamsServiceWatchdogRuntimeRestartErrorsDoNotStopLoop,TestTeamsServiceWatchdogRuntimeProcessUpgradeAndRollback' 1
    ;;
  e2e)
    run_tests ./internal/e2e/scenarios/notify '^TestNotifySQLite' 'TestNotifySQLiteUnknownBacklogDoesNotHideNewFinal,TestNotifySQLiteAcceptedCardCrashDoesNotRetry' 3
    run_tests ./internal/e2e/scenarios/entry '^TestStatusRunningInstalledAndLastPoll$' 'TestStatusRunningInstalledAndLastPoll' 3
    run_tests ./internal/e2e/scenarios/commands '^TestSkills' 'TestSkillsPushFromTeams,TestSkillsSQLiteLegacyUpgradeFromCLI' 3
    run_tests ./internal/e2e/scenarios/delegation '^TestDelegationSQLiteMigration' 'TestDelegationSQLiteMigrationPreservesUnknownPOST' 3
    run_tests ./internal/e2e/scenarios/entry '^Test(EntryMigration|EntryWatchdogRuntime|EntryTypedSQLite)' 'TestEntryTypedSQLiteUpgradePreservesAttemptedNotice,TestEntryMigrationPreservesChatWithStoppedWatchdog,TestEntryMigrationResumesAfterInterruptedStart,TestEntryMigrationResumesAfterCrashDuringImport,TestEntryMigrationTakeoverFencesWaitingOwner,TestEntryMigrationTakeoverFencesImporter,TestEntryWatchdogRuntimeUpgradeAndRollback,TestEntryWatchdogRuntimeLegacyLauncher,TestEntryMigrationRefusesMissingPublishedDatabase,TestEntryMigrationRefusesEmptyPublishedDatabase' 3
    ;;
  *)
    echo "usage: $0 [unit|e2e]" >&2
    exit 2
    ;;
esac
