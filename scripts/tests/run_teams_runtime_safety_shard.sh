#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  isolation)
    go test ./internal/cli -count=1 \
      -run '^TestTeamsRuntimeSafetyPackageTestMainIsolatesEveryUserDirectoryCI$' -v
    ;;
  store)
    go test ./internal/teams -count=1 \
      -run '^TestTeamsRuntimeSafety(RuntimeResolver|Legacy|CanonicalFastPath|Discovery|SuccessfulMigration|Migration|AutomaticTakeover)' -v
    ;;
  service-update)
    go test ./internal/cli ./internal/teams -count=1 \
      -run '^TestTeamsRuntimeSafety(ServiceSpec|AutoUpdate|DoesNotRestore|ExplicitStable|ServiceEnvironment|BridgeHelperAutoUpdate|HelperUpdateStatus)' -v
    ;;
  wsl-process)
    go test ./internal/cli -count=1 \
      -run '^TestTeamsRuntimeSafety(WSL|Doctor(Distinguishes|Classifies)|StableEntry|ManagedRuntime|Watchdog)' -v
    ;;
  diagnostics)
    go test ./internal/cli -count=1 \
      -run '^TestTeamsRuntimeSafety(LocalStart|LocalStatus|SupervisorLogs|Recoverable|Status|DoctorDeepReads|UnavailableModel)' -v
    ;;
  *)
    echo "usage: $0 {isolation|store|service-update|wsl-process|diagnostics}" >&2
    exit 2
    ;;
esac
