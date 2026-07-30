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
CGO_ENABLED=0 go test -c -o "$tmp_dir/teams-runtime-safety-cli.test" ./internal/cli
docker build \
  --file scripts/ci/Dockerfile.teams-runtime-takeover \
  --tag "$image" \
  "$tmp_dir" >/dev/null
docker run --rm \
  --network none \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --pids-limit 64 \
  --read-only \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m \
  --env CXP_TEAMS_TAKEOVER_REAL_PROCESS_DOCKER=1 \
  "$image" \
  -test.run '^TestTeamsRuntimeSafetyAutomaticTakeoverRealWriterProcessDockerCI$' \
  -test.count=1 \
  -test.v
docker run --rm \
  --network none \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --pids-limit 64 \
  --read-only \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m \
  --env CXP_TEAMS_TAKEOVER_REAL_PROCESS_DOCKER=1 \
  --entrypoint /teams-runtime-safety-cli.test \
  "$image" \
  -test.run '^TestTeamsRuntimeSafetyExactLegacyWriterFenceRealProcessDockerCI$' \
  -test.count=1 \
  -test.v
docker run --rm \
  --network none \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --pids-limit 64 \
  --read-only \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m \
  --env CXP_TEAMS_TAKEOVER_REAL_PROCESS_DOCKER=1 \
  --entrypoint /teams-runtime-safety-cli.test \
  "$image" \
  -test.run '^TestTeamsRuntimeSafetySupervisorFencePreventsChildRestartRealProcessDockerCI$' \
  -test.count=1 \
  -test.v
