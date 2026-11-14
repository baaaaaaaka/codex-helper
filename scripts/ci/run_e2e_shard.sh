#!/usr/bin/env bash
# CI's package-sharded E2E suite; invoke only in an isolated test environment.
set -euo pipefail
if [[ $# != 1 || ! $1 =~ ^[0-2]$ ]]; then
  echo "usage: $0 {0|1|2}" >&2
  exit 2
fi
python3 scripts/ci/e2e_shards.py --count 3 --plan
packages_text=$(python3 scripts/ci/e2e_shards.py --count 3 --index "$1")
if [[ -z "$packages_text" ]]; then
  echo "E2E shard $1 selected no scenario packages" >&2
  exit 1
fi
packages=()
while IFS= read -r package; do
  packages+=("$package")
done <<< "$packages_text"
export E2E_SUT=cxp
exec go test -v -count=1 -parallel=8 -timeout=600s "${packages[@]}"
