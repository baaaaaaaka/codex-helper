#!/usr/bin/env bash
set -euo pipefail

# Run the copied production outbox in a network-isolated container.  The
# fixture is assembled before this script is invoked; the container receives
# only a read-only bind of that fixture and a separate writable runtime.  The
# test's Graph client points at an in-process fake server, so no Teams token is
# needed and no request can reach Microsoft.
if [[ $# -ne 1 ]]; then
	echo "usage: $0 FIXTURE_DIR" >&2
	exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
fixture_dir="$(cd "$1" && pwd -P)"
runtime_dir="$(mktemp -d /tmp/cxp-teams-real-data-outbox-runtime-XXXXXX)"
build_dir="$(mktemp -d /tmp/cxp-teams-real-data-outbox-build-XXXXXX)"
image="cxp-teams-real-data-outbox:${GITHUB_RUN_ID:-local}-$$"
host_uid="$(id -u)"
host_gid="$(id -g)"
duration="${CXP_TEAMS_DOCKER_REAL_DATA_OUTBOX_DURATION:-60s}"
test_timeout="${CXP_TEAMS_DOCKER_TEST_TIMEOUT:-3m}"
watchdog_timeout="${CXP_TEAMS_DOCKER_WATCHDOG_TIMEOUT:-4m}"

cleanup() {
	docker image rm --force "$image" >/dev/null 2>&1 || true
	rm -rf -- "$runtime_dir" "$build_dir"
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || {
	echo "docker is required for the real-data Teams outbox experiment" >&2
	exit 1
}
command -v sha256sum >/dev/null 2>&1 || {
	echo "sha256sum is required for the fixture immutability check" >&2
	exit 1
}
command -v sqlite3 >/dev/null 2>&1 || {
	echo "sqlite3 is required to validate the copied outbox fixture" >&2
	exit 1
}
command -v timeout >/dev/null 2>&1 || {
	echo "timeout is required for the Docker watchdog" >&2
	exit 1
}
docker info >/dev/null 2>&1 || {
	echo "docker daemon is unavailable for the real-data Teams outbox experiment" >&2
	exit 1
}

for required in \
	"$fixture_dir/teams/state.json" \
	"$fixture_dir/teams/store.sqlite" \
	"$fixture_dir/teams/registry.json" \
	"$fixture_dir/codex/sessions" \
	"$fixture_dir/source-proof-manifest.tsv"; do
	[[ -f "$required" && ! -L "$required" ]] || {
		if [[ -d "$required" && "$required" == "$fixture_dir/codex/sessions" ]]; then
			continue
		fi
		echo "fixture input is missing or not a regular file: $required" >&2
		exit 1
	}
done

if [[ -n "$(find "$fixture_dir/codex" -type l -print -quit)" ]]; then
	echo "fixture Codex tree contains a symlink" >&2
	exit 1
fi

write_fixture_manifest() {
	local output="$1"
	: > "$output"
	for input in \
		"$fixture_dir/teams/state.json" \
		"$fixture_dir/teams/store.sqlite" \
		"$fixture_dir/teams/registry.json" \
		"$fixture_dir/source-proof-manifest.tsv"; do
		sha256sum -- "$input" >> "$output"
	done
}

if ! sqlite3 "file:$fixture_dir/teams/store.sqlite?mode=ro" "PRAGMA quick_check;" | grep -Fxq ok; then
	echo "copied Teams outbox SQLite fixture failed PRAGMA quick_check" >&2
	exit 1
fi

fixture_manifest_before="$build_dir/fixture-before.sha256"
fixture_manifest_after="$build_dir/fixture-after.sha256"
write_fixture_manifest "$fixture_manifest_before"

cd "$repo_root"
CGO_ENABLED=0 go test -c -o "$build_dir/teams-sqlite-history-fixture.test" ./internal/teams
mkdir -p "$build_dir/docker-mountpoints/sessions"
touch -- "$build_dir/docker-mountpoints/sessions/.keep"
docker build \
	--file scripts/ci/Dockerfile.teams-real-data-experiment \
	--tag "$image" \
	"$build_dir" >/dev/null

echo "starting network-isolated real-data outbox Docker experiment: fixture=$fixture_dir duration=$duration"
set +e
timeout --foreground --kill-after=30s "$watchdog_timeout" docker run --rm --stop-timeout 30 \
	--network none \
	--user "$host_uid:$host_gid" \
	--cap-drop ALL \
	--security-opt no-new-privileges \
	--pids-limit 256 \
	--read-only \
	--tmpfs /tmp:rw,nosuid,nodev,size=256m \
	--env HOME=/runtime/home \
	--env TMPDIR=/tmp \
	--env CXP_RUNTIME_DISABLE=1 \
	--env CXP_TEAMS_DOCKER_FIXTURE_DIR=/fixture \
	--env CXP_TEAMS_DOCKER_RUNTIME_DIR=/runtime \
	--env CXP_TEAMS_DOCKER_REAL_DATA_OUTBOX_EXPERIMENT=1 \
	--env CXP_TEAMS_DOCKER_REAL_DATA_OUTBOX_DURATION="$duration" \
	--env CXP_TEAMS_DOCKER_CODEX_MOUNTED=1 \
	--env CXP_TEAMS_DOCKER_CODEX_SOURCE_PREFIX=/home/baka/.codex/ \
	--env CXP_TEAMS_DOCKER_SOURCE_PROOF_MANIFEST=/fixture/source-proof-manifest.tsv \
	--mount "type=bind,src=$fixture_dir,dst=/fixture,readonly" \
	--mount "type=bind,src=$fixture_dir/codex,dst=/home/baka/.codex,readonly" \
	--mount "type=bind,src=$runtime_dir,dst=/runtime" \
	"$image" \
	/teams-sqlite-history-fixture.test \
	-test.run '^TestDockerRealDataOutboxDrain$' \
	-test.count=1 \
	-test.timeout "$test_timeout" \
	-test.v
docker_status=$?
set -e

write_fixture_manifest "$fixture_manifest_after"
if ! cmp -s "$fixture_manifest_before" "$fixture_manifest_after"; then
	echo "fixture integrity check failed: the read-only source fixture changed during Docker run" >&2
	diff -u "$fixture_manifest_before" "$fixture_manifest_after" >&2 || true
	docker_status=1
else
	echo "fixture integrity check: source SQLite/state/registry unchanged"
fi

exit "$docker_status"
