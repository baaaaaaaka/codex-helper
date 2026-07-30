#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  isolation)
    go test ./internal/cli -count=1 \
      -run '^TestTeamsRuntimeSafetyPackageTestMainIsolatesEveryUserDirectoryCI$' -v
    ;;
  store)
    go test ./internal/teams -count=1 \
      -run '^TestTeamsRuntimeSafety(RuntimeResolver|Legacy|Canonical|Discovery|SuccessfulMigration|Migration|AutomaticTakeover)' -v
    ;;
  store-io)
    bash scripts/ci/teams_runtime_resolver_io_smoke.sh
    ;;
  store-process)
    bash scripts/ci/teams_runtime_takeover_process_smoke.sh
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
  windows)
    go test ./internal/cli -count=1 \
      -run '^TestTeamsRuntimeSafety(PackageTestMain|ServiceSpec|DoesNotRestore|ExplicitStable|ServiceEnvironment|StableEntry|ManagedRuntime|LocalStatus|StatusReportsAuthoritative)' -v
    ;;
  *)
    echo "usage: $0 {isolation|store|store-io|store-process|service-update|wsl-process|diagnostics|windows}" >&2
    exit 2
    ;;
esac
