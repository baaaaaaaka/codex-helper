#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

if ! command -v strace >/dev/null 2>&1; then
  echo "strace is required for the Teams runtime resolver I/O smoke test" >&2
  exit 1
fi

cd "$repo_root"
go test -c -o "$tmp_dir/teams-runtime-resolver-io.test" ./internal/teams
env \
  CXP_TEAMS_RESOLVER_IO_PROBE=1 \
  CXP_TEAMS_RESOLVER_IO_ROOT_FILE="$tmp_dir/probe-root.txt" \
  strace -f -yy -s 512 -o "$tmp_dir/trace.log" \
    -e trace=openat,openat2,newfstatat,statx,getdents64,write,pwrite64,writev,fdatasync,fsync,ftruncate,unlink,unlinkat,rename,renameat,renameat2,mkdir,mkdirat,chmod,fchmodat,msync \
    "$tmp_dir/teams-runtime-resolver-io.test" \
      -test.run '^TestTeamsRuntimeSafetyCanonicalResolverSyscallProbeCI$' \
      -test.count=1 -test.v

python3 \
  "$repo_root/scripts/ci/verify_teams_runtime_resolver_io.py" \
  "$tmp_dir/trace.log" \
  "$tmp_dir/probe-root.txt"
