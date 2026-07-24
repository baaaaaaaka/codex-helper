#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"

run_runtime_tests() {
  sqlite_perf_pattern='TestCXPPerfModelSQLite(ProfilesCoverUpgradeOperations|ExternalScenariosCoverCommonPaths)$'
  go test ./internal/teams -count=1 -run "$sqlite_perf_pattern" -v
  go test ./internal/teams -count=1 -run 'TestWorkflowNotificationFlushInvalidPendingDoesNotLoadHotSQLiteTables$' -v
  go test ./internal/teams -count=1 -run 'TestWorkflowNotificationFlushEnabledNoPendingSkipsWebhookSecretRead$' -v
  go test ./internal/teams -count=1 -run 'TestBridge(HelperAutoUpdate(NotDueSkipsHotSQLiteTablesAndCheck|BackoffSkipsHotSQLiteTablesAndCheck|NotDueCachesMainLoopProbe)|ManualHelperUpdateClearsCachedAutoUpdateProbe)$' -v
  go test ./internal/teams -count=1 -run 'TestBridge(PendingCodexUpgradeNoUpgradeSkipsHotSQLiteTablesAndUpgrade|SaveRepairsMissingRegistryDuringThrottleWindow|CodexVersionFailureSchedulesUpgradeAfterActiveWork)$' -v
  go test ./internal/teams -count=1 -run 'TestCXPPerfActiveParkedFixtureHasNoPendingWorkflowNotifications$' -v
  go test ./internal/teams -count=1 -run 'Test(BridgeSyncLinkedTranscriptLegacyBackfillPreservesDedupeWindow|BridgeSyncLinkedTranscriptDeliveryLedgerPreventsReplayAfterOutboxPruneAndCheckpointRegressionSQLite|BridgeSyncLinkedTranscriptsBackfillsLegacyCheckpoint(Metadata|ThenImportsTail)|FindTranscriptCheckpointPositionSupportsFallbackCheckpoint)$' -v
  go test ./internal/teams -run '^$' -bench 'BenchmarkCXPPerfModelSQLiteLegacyLinkedTranscriptBackfilledIdleProfiles/light-user$' -benchtime=1x -count=1 -benchmem
  go test ./internal/teams -run '^$' -bench 'BenchmarkCXPPerfModelSQLiteInvalidWorkflowNotificationIdleTickProfiles/light-user$' -benchtime=1x -count=1 -benchmem
  go test ./internal/teams -run '^$' -bench 'BenchmarkCXPPerfModelSQLiteHelperAutoUpdateNotDueProfiles/light-user/(cold-state-refresh|cached-main-loop)$' -benchtime=1x -count=1 -benchmem
  go test ./internal/teams -run '^$' -bench 'BenchmarkCXPPerfModelSQLiteCodexUpgradeNoPendingProfiles/light-user/(cold-state-refresh|cached-main-loop)$' -benchtime=1x -count=1 -benchmem
  go test ./internal/teams -run '^$' -bench 'BenchmarkCXPPerfModelSQLiteSelectedSnapshotLargeColdStateProfiles/light-user/pending-workflow-notifications$' -benchtime=1x -count=1 -benchmem
}

run_mixed_write_benchmarks() {
  go test ./internal/teams -run '^$' -bench 'BenchmarkCXPPerfModelSQLiteRealisticMixedUser(ChatPollError|OutboxSendError|OutboxDriveItem|ArtifactUpsert|TranscriptDelivery|TranscriptCheckpoint|TranscriptQueue)StageWrites$' -benchtime=1x -count=1 -benchmem
  go test ./internal/teams -run '^$' -bench 'BenchmarkCXPPerfModelSQLiteRealisticMixedUserWALSpikeBreakdown$' -benchtime=1x -count=1 -benchmem
}

run_ledger_benchmarks() {
  go test ./internal/teams -run '^$' -bench 'BenchmarkCXPPerfModelSQLiteRegistryProjectionRetentionChurn$' -benchtime=1x -count=1 -benchmem
  go test ./internal/teams -run '^$' -bench 'Benchmark(GlobalOutboundLedgerRecord|GlobalInboundLedgerClaim|ControlChatHistoryAppend)' -benchtime=1x -count=1 -benchmem
}

case "$mode" in
  all)
    run_runtime_tests
    run_mixed_write_benchmarks
    run_ledger_benchmarks
    ;;
  tests)
    run_runtime_tests
    ;;
  mixed-bench)
    run_mixed_write_benchmarks
    ;;
  ledger-bench)
    run_ledger_benchmarks
    ;;
  *)
    echo "usage: $0 {all|tests|mixed-bench|ledger-bench}" >&2
    exit 2
    ;;
esac
