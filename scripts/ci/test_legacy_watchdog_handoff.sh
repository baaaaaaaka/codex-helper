#!/usr/bin/env bash
set -euo pipefail

# A fixed historical executable is essential: rebuilding current source with
# an old version string already contains the refresh logic under test.
scratch=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:?set TMPDIR to a writable executable directory}}/cxp-legacy-watchdog.XXXXXX")
asset=codex-proxy_0.1.20_linux_amd64
curl --fail --location --retry 3 --connect-timeout 15 --max-time 180 \
  "https://github.com/baaaaaaaka/codex-helper/releases/download/v0.1.20/$asset" \
  --output "$scratch/$asset"
printf '%s  %s\n' fe71a0c790b55ece0d5567cef05dcd6ce911f6f2a0c72b6a3c2ac2e4e81693b7 "$scratch/$asset" | sha256sum --check --strict
chmod 700 "$scratch/$asset"
export CXP_LEGACY_WATCHDOG_BINARY="$scratch/$asset"
go test ./internal/e2e/scenarios/entry -list '^TestEntryLegacyWatchdogHandoff$' | grep -Fx 'TestEntryLegacyWatchdogHandoff'
go test ./internal/e2e/scenarios/entry -run '^TestEntryLegacyWatchdogHandoff$' -count=3 -timeout=5m -v

if [[ ${CXP_TEST_REAL_SYSTEMD:-0} == 1 ]]; then
  bash scripts/ci/test_watchdog_systemd.sh
fi
