#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
image="cxp-teams-takeover-process-test:${GITHUB_RUN_ID:-local}-${$}"
trap 'docker image rm --force "$image" >/dev/null 2>&1 || true; rm -rf "$tmp_dir"' EXIT

command -v docker >/dev/null 2>&1 || {
  echo "docker is required for the Teams takeover real-process smoke test" >&2
  exit 1
}
docker info >/dev/null 2>&1 || {
  echo "docker daemon is unavailable for the Teams takeover real-process smoke test" >&2
  exit 1
}

cd "$repo_root"
CGO_ENABLED=0 go test -c -o "$tmp_dir/teams-runtime-safety.test" ./internal/teams
test_selector='^(TestTeamsRuntimeSafetyOfflineTakeoverWaitsForRealWriterExitDockerCI|TestTeamsRuntimeSafetyOfflineTakeoverAfterSIGKILLDockerCI|TestTeamsRuntimeSafetySQLiteFullFilesystemDockerCI|TestTeamsRuntimeSafetyBridgeTranscriptGraphAcceptedThenSQLiteFullDockerCI)$'
available_tests="$("$tmp_dir/teams-runtime-safety.test" -test.list "$test_selector")"
for test_name in \
  TestTeamsRuntimeSafetyOfflineTakeoverWaitsForRealWriterExitDockerCI \
  TestTeamsRuntimeSafetyOfflineTakeoverAfterSIGKILLDockerCI \
  TestTeamsRuntimeSafetySQLiteFullFilesystemDockerCI \
  TestTeamsRuntimeSafetyBridgeTranscriptGraphAcceptedThenSQLiteFullDockerCI; do
  if ! grep -Fxq -- "$test_name" <<<"$available_tests"; then
    echo "runtime takeover smoke selector did not find exact test: $test_name" >&2
    printf 'available tests:\n%s\n' "$available_tests" >&2
    exit 1
  fi
done
docker build \
  --file scripts/ci/Dockerfile.teams-runtime-takeover \
  --tag "$image" \
  "$tmp_dir" >/dev/null

run_log="$tmp_dir/runtime-takeover.log"
if ! timeout --foreground 180s docker run --rm \
  --network none \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --pids-limit 64 \
  --read-only \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m \
  --tmpfs /state:rw,nosuid,nodev,size=32m \
  --env CXP_TEAMS_TAKEOVER_REAL_PROCESS_DOCKER=1 \
  --env CXP_TEAMS_BOUNDARY_DOCKER=1 \
  --env CXP_TEAMS_BOUNDARY_FS_ROOT=/state \
  "$image" \
  -test.run "$test_selector" \
  -test.count=1 \
  -test.timeout=120s \
  -test.v >"$run_log" 2>&1; then
  cat "$run_log" >&2
  exit 1
fi
cat "$run_log"

for test_name in \
  TestTeamsRuntimeSafetyOfflineTakeoverWaitsForRealWriterExitDockerCI \
  TestTeamsRuntimeSafetyOfflineTakeoverAfterSIGKILLDockerCI \
  TestTeamsRuntimeSafetySQLiteFullFilesystemDockerCI \
  TestTeamsRuntimeSafetyBridgeTranscriptGraphAcceptedThenSQLiteFullDockerCI; do
  if ! grep -Eq -- "^--- PASS: ${test_name}([[:space:]]|$)" "$run_log"; then
    echo "runtime takeover smoke test did not pass (or was skipped): $test_name" >&2
    exit 1
  fi
  if grep -Eq -- "^--- SKIP: ${test_name}([[:space:]]|$)" "$run_log"; then
    echo "runtime takeover smoke test was skipped: $test_name" >&2
    exit 1
  fi
done
