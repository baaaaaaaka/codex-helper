#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
command -v strace >/dev/null || { echo 'strace is required' >&2; exit 1; }
scratch="$(mktemp -d "${CXP_STATE_PERF_TMPDIR:-$PWD}/.cxp-state-perf.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT
go test -c -o "$scratch/config-perf.test" ./internal/config
for rows in 1 1000 100000; do
  CXP_SQLITE_PERF_PROBE=1 CXP_SQLITE_PERF_ROWS="$rows" \
    strace -f -qq -yy -s 256 -o "$scratch/trace-$rows.log" \
    -e trace=pread64,read,pwrite64,write,writev,fdatasync,fsync,ftruncate,unlink,unlinkat,rename,renameat,mkdir,mkdirat,chmod,fchmodat \
    "$scratch/config-perf.test" -test.run '^TestConfigSQLitePerformanceProbe$' -test.count=1 -test.v
  python3 scripts/ci/verify_sqlite_state_io.py "$scratch/trace-$rows.log"
done

# Actual fresh CLI processes with native bootstrap, outside compile/migration
# costs. This runs only on the disposable CI runner or isolated source harness.
go build -o "$scratch/cxp" ./cmd/codex-proxy
python3 scripts/ci/sqlite_cli_cold_probe.py "$scratch/cxp"

# Legacy fixture construction is outside a fresh measurement process, so RSS
# reflects conversion rather than the old format's fixture-building buffers.
for package in internal/core/store internal/core/delegate/delegation; do
  go test -c -o "$scratch/migration.test" "./$package"
  for rows in 1 1000 100000; do
    (
      cd "$package"
      CXP_SQLITE_PERF_PROBE=1 CXP_SQLITE_PERF_ROWS="$rows" \
        "$scratch/migration.test" -test.run '^Test((Engine|Worker)SQLiteLargeMigrationProbe|EngineSQLiteSendChainProbe)$' -test.count=1 -test.v
    )
  done
done
