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

test_selector='^TestTeamsOwnershipStress'
test_names="$tmp_dir/ownership-test-names"
"$tmp_dir/teams-ownership.test" -test.list "$test_selector" >"$test_names"
if ! grep -q '^TestTeamsOwnershipStress' "$test_names"; then
	echo "ownership stress smoke selector matched no tests" >&2
	exit 1
fi

run_log="$tmp_dir/ownership-stress.log"
if ! timeout --foreground 240s docker run --rm \
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
	-test.timeout=180s \
	-test.v >"$run_log" 2>&1; then
	cat "$run_log" >&2
	exit 1
fi
cat "$run_log"

while IFS= read -r test_name; do
	[ -n "$test_name" ] || continue
	if grep -Eq -- "^--- SKIP: ${test_name}([[:space:]]|$)" "$run_log"; then
		echo "ownership stress smoke test was skipped: $test_name" >&2
		exit 1
	fi
	if ! grep -Eq -- "^--- PASS: ${test_name}([[:space:]]|$)" "$run_log"; then
		echo "ownership stress smoke test did not pass: $test_name" >&2
		exit 1
	fi
done <"$test_names"
