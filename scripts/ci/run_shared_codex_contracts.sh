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
CGO_ENABLED=0 go test -c -o "$contract_scratch/agent" ./internal/core/codexagent
CGO_ENABLED=0 go build -o "$contract_scratch/cxp" ./cmd/codex-proxy
# A renamed or platform-excluded acceptance test must not become a green
# "no tests to run" result. Listing tests does not start any native processes.
"$contract_scratch/probe" -test.list '^TestRealCodexNewChatTUI$' | grep -qx TestRealCodexNewChatTUI
"$contract_scratch/agent" -test.list '^TestRealCodexSharedTUIThreadBinding$' | grep -qx TestRealCodexSharedTUIThreadBinding
for contract_suite in probe client agent; do
  docker run --rm --network none \
  -v "$contract_scratch:/contracts:ro" \
  -v "$contract_codex:/codex:ro" \
  -e E2E_REAL_CODEX=/codex \
  -w /work \
  "${CXP_CONTRACT_IMAGE:-golang:1.25.11-bookworm}" \
  "/contracts/$contract_suite" -test.v -test.count=3 -test.timeout=180s \
  -test.run 'TestRealCodex(Shared|NativeQueue|Title)'
done
# The selector-to-first-response path is a product acceptance test. Keep it
# separate from observations about native zero-input resume behavior.
docker run --rm --network none \
  -v "$contract_scratch:/contracts:ro" \
  -v "$contract_codex:/codex:ro" \
  -e E2E_REAL_CODEX=/codex -e E2E_CXP_BIN=/contracts/cxp \
  -w /work "${CXP_CONTRACT_IMAGE:-golang:1.25.11-bookworm}" \
  /contracts/probe -test.v -test.count=1 -test.timeout=180s \
  -test.run '^TestRealCodexNewChatTUI$' | tee "$contract_scratch/new-chat.log"
# A listed test may still skip at runtime; the acceptance gate must execute it.
if ! grep -Eq '^--- PASS: TestRealCodexNewChatTUI( |$)' "$contract_scratch/new-chat.log"; then
  echo 'New chat acceptance did not pass or was skipped' >&2
  exit 1
fi
