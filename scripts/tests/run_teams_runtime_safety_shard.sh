#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  isolation)
    CXP_TEAMS_TEST_PRESERVE_USER_DIRS=1 go test ./internal/cli -count=1 \
      -run '^TestTeamsRuntimeSafetyPackageTestMainIsolatesEveryUserDirectoryCI$' -v
    ;;
  service-update)
    go test ./internal/cli -count=1 \
      -run '^TestTeamsRuntimeSafety(ServiceSpec|AutoUpdate|DoesNotRestore|ExplicitStable|ServiceEnvironment|BridgeHelperAutoUpdate|HelperUpdateStatus)' -v
    ;;
  wsl-process)
    go test ./internal/cli -count=1 \
      -run '^TestTeamsRuntimeSafety(WSL|Doctor(Distinguishes|Classifies)|StableEntry|ManagedRuntime|Watchdog)' -v
    ;;
  diagnostics)
    go test ./internal/cli -count=1 \
      -run '^TestTeamsRuntimeSafety(LocalStart|LocalStatus|SupervisorLogs|Recoverable|ActiveMigrationLease|LegacyMigrationMarker|BlockedMigration|Status|DoctorDeepReads|UnavailableModel)' -v
    ;;
  windows)
    go test ./internal/cli -count=1 \
      -run '^TestTeamsRuntimeSafety(PackageTestMain|ServiceSpec|DoesNotRestore|ExplicitStable|ServiceEnvironment|StableEntry|ManagedRuntime|ActiveMigrationLease|LegacyMigrationMarker|LocalStatus|StatusReportsAuthoritative)' -v
    ;;
  *)
    echo "usage: $0 {isolation|service-update|wsl-process|diagnostics|windows}" >&2
    exit 2
    ;;
esac
