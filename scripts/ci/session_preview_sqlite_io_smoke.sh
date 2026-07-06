#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

if ! command -v strace >/dev/null 2>&1; then
  echo "strace is required for the SQLite preview-cache I/O smoke test" >&2
  exit 1
fi

cd "$repo_root"
go test -c -o "$tmp_dir/codexhistory-preview-io.test" ./internal/codexhistory
env CXP_SESSION_PREVIEW_IO_PROBE=1 \
  strace -f -yy -s 256 -o "$tmp_dir/trace.log" \
    -e trace=pwrite64,write,writev,fdatasync,fsync,ftruncate,unlink,unlinkat,rename,renameat,mkdir,mkdirat,chmod,fchmodat,msync \
    "$tmp_dir/codexhistory-preview-io.test" \
      -test.run '^TestSessionPreviewSQLiteSyscallProbe$' -test.count=1 -test.v

python3 "$repo_root/scripts/ci/verify_session_preview_sqlite_io.py" "$tmp_dir/trace.log"
