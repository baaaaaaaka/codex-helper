#!/usr/bin/env bash
set -euo pipefail

attempts="${1:?attempts required}"
shift
sleep_seconds="${1:?sleep_seconds required}"
shift

if [[ "$attempts" -lt 1 ]]; then
  echo "attempts must be >= 1" >&2
  exit 1
fi

for ((attempt = 1; attempt <= attempts; attempt++)); do
  if "$@"; then
    exit 0
  fi

  if [[ "$attempt" -eq "$attempts" ]]; then
    break
  fi

  echo "command failed (attempt ${attempt}/${attempts}), retrying in ${sleep_seconds}s: $*" >&2
  sleep "$sleep_seconds"
done

echo "command failed after ${attempts} attempts: $*" >&2
exit 1
