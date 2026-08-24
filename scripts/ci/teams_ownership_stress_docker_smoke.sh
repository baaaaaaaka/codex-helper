#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
image="cxp-teams-ownership-stress:${GITHUB_RUN_ID:-local}-$$"
trap 'docker image rm --force "$image" >/dev/null 2>&1 || true; rm -rf "$tmp_dir"' EXIT

command -v docker >/dev/null 2>&1 || {
	echo "docker is required for the Teams ownership stress smoke test" >&2
	exit 1
}
docker info >/dev/null 2>&1 || {
	echo "docker daemon is unavailable for the Teams ownership stress smoke test" >&2
	exit 1
}

cd "$repo_root"
CGO_ENABLED=0 go test -c -o "$tmp_dir/teams-ownership.test" ./internal/teams
cp scripts/ci/Dockerfile.teams-ownership-stress "$tmp_dir/Dockerfile"
docker build \
	--file "$tmp_dir/Dockerfile" \
	--tag "$image" \
	"$tmp_dir" >/dev/null

docker run --rm \
	--network none \
	--cap-drop ALL \
	--security-opt no-new-privileges \
	--pids-limit 128 \
	--read-only \
	--tmpfs /tmp:rw,nosuid,nodev,size=128m \
	--env CXP_RUNTIME_DISABLE=1 \
	--env CODEX_HELPER_TEAMS_OWNERSHIP_STRESS_STRICT=1 \
	"$image" \
	-test.run '^TestTeamsOwnershipStress' \
	-test.count=1 \
	-test.v
