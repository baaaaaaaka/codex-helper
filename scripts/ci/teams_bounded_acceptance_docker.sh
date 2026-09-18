#!/usr/bin/env bash
set -euo pipefail

# Compile the bounded fake-Graph acceptance test into a scratch image and run
# only that test with no network, no fixture mounts, and no credential
# environment. The test itself uses httptest on loopback; Docker isolation
# proves that an accidental real Graph route cannot succeed.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$repo_root"

: "${CXP_TEAMS_BOUNDED_ACCEPTANCE_TEST_TIMEOUT:=2m}"
: "${CXP_TEAMS_BOUNDED_ACCEPTANCE_DOCKER_TIMEOUT:=3m}"

build_dir="$(mktemp -d /tmp/cxp-teams-bounded-acceptance-XXXXXX)"
image="cxp-teams-bounded-acceptance:${GITHUB_RUN_ID:-local}-$$"
binary="$build_dir/teams-bounded-acceptance.test"

cleanup() {
	docker image rm --force "$image" >/dev/null 2>&1 || true
	rm -rf -- "$build_dir"
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || {
	echo "docker is required for the bounded Teams acceptance scenario" >&2
	exit 1
}
command -v timeout >/dev/null 2>&1 || {
	echo "timeout is required for the bounded Teams acceptance scenario" >&2
	exit 1
}
docker info >/dev/null 2>&1 || {
	echo "docker daemon is unavailable for the bounded Teams acceptance scenario" >&2
	exit 1
}

selector='^TestDockerBoundedAcceptanceScenario$'
listed_tests="$(CXP_RUNTIME_DISABLE=1 go test ./internal/teams -list "$selector")"
grep -Fxq -- "TestDockerBoundedAcceptanceScenario" <<<"$listed_tests" || {
	echo "bounded acceptance selector did not list TestDockerBoundedAcceptanceScenario" >&2
	exit 1
}

CGO_ENABLED=0 CXP_RUNTIME_DISABLE=1 go test -c -o "$binary" ./internal/teams
docker build --file scripts/ci/Dockerfile.teams-bounded-acceptance --tag "$image" "$build_dir"

timeout "$CXP_TEAMS_BOUNDED_ACCEPTANCE_DOCKER_TIMEOUT" docker run --rm \
	--network=none \
	--read-only \
	--tmpfs /tmp:rw,nosuid,nodev \
	--cap-drop=ALL \
	--security-opt no-new-privileges:true \
	--pids-limit 128 \
	--user "$(id -u):$(id -g)" \
	--env CXP_RUNTIME_DISABLE=1 \
	--env HOME=/tmp \
	--env TMPDIR=/tmp \
	"$image" \
	/teams-bounded-acceptance.test \
	-test.run "$selector" \
	-test.count=1 \
	-test.timeout "$CXP_TEAMS_BOUNDED_ACCEPTANCE_TEST_TIMEOUT" \
	-test.v
