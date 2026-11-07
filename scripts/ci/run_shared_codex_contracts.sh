#!/usr/bin/env bash
# Build inert test code on the host; execute the real binary only in an
# offline container with no host home, helper state, sockets or credentials.
set -euo pipefail

if [ "$#" -ne 1 ] || [ ! -f "$1" ] || [ ! -x "$1" ]; then
  echo "usage: bash scripts/ci/run_shared_codex_contracts.sh /absolute/path/to/native/codex" >&2
  exit 2
fi
case "$1" in
  /*) contract_codex="$1" ;;
  *) echo "Codex path must be absolute" >&2; exit 2 ;;
esac
contract_repo="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
contract_scratch="$(mktemp -d "${TMPDIR:-/tmp}/cxp-shared-contract.XXXXXX")"
trap 'rm -rf -- "$contract_scratch"' EXIT
cd -- "$contract_repo"
CGO_ENABLED=0 go test -c -o "$contract_scratch/probe" ./internal/e2e/realcodex
CGO_ENABLED=0 go test -c -o "$contract_scratch/client" ./internal/core/codex
for contract_suite in probe client; do
  docker run --rm --network none \
  -v "$contract_scratch:/contracts:ro" \
  -v "$contract_codex:/codex:ro" \
  -e E2E_REAL_CODEX=/codex \
  -w /work \
  "${CXP_CONTRACT_IMAGE:-golang:1.25.11-bookworm}" \
  "/contracts/$contract_suite" -test.v -test.count=3 -test.timeout=180s \
  -test.run 'TestRealCodex(Shared|NativeQueue)'
done
