#!/usr/bin/env bash
set -euo pipefail

# Run the real current Teams SQLite state through the production listener loop
# in a disposable, network-isolated container. This is intentionally an
# experiment, not a smoke test: the SQLite snapshot and registry are copied,
# while the current Codex session tree is mounted read-only because it is
# large (the current linked tree is tens of GB). Only a deterministic in-process
# Graph server and executor are used; no Teams token is read or mounted.
if [[ $# -lt 1 || $# -gt 2 ]]; then
	echo "usage: $0 TEAMS_SCOPE_DIR [CODEX_HOME]" >&2
	exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
scope_dir="$(cd "$1" && pwd -P)"
codex_home="${2:-/home/baka/.codex}"
teams_root="$(cd "$scope_dir/../.." && pwd -P)"
fixture_dir="$(mktemp -d /tmp/cxp-teams-real-data-XXXXXX)"
build_dir="$(mktemp -d /tmp/cxp-teams-real-build-XXXXXX)"
image="cxp-teams-real-data-experiment:${GITHUB_RUN_ID:-local}-$$"
runtime_volume="cxp-teams-real-data-runtime-${GITHUB_RUN_ID:-local}-$$"

cleanup() {
	docker image rm --force "$image" >/dev/null 2>&1 || true
	docker volume rm "$runtime_volume" >/dev/null 2>&1 || true
	rm -rf -- "$fixture_dir" "$build_dir"
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || {
	echo "docker is required for the real-data Teams experiment" >&2
	exit 1
}
command -v sqlite3 >/dev/null 2>&1 || {
	echo "sqlite3 is required to create a consistent read-only database snapshot" >&2
	exit 1
}
docker info >/dev/null 2>&1 || {
	echo "docker daemon is unavailable for the real-data Teams experiment" >&2
	exit 1
}

for required in \
	"$scope_dir/state.json" \
	"$scope_dir/store.sqlite" \
	"$scope_dir/registry.json" \
	"$teams_root/global-inbound-ledger.json" \
	"$teams_root/global-inbound-ledger.sqlite" \
	"$teams_root/global-outbound-ledger.json" \
	"$teams_root/global-outbound-ledger.sqlite" \
	"$codex_home/history.jsonl" \
	"$codex_home/session_index.jsonl" \
	"$codex_home/sessions"; do
	[[ -e "$required" ]] || {
		echo "missing real-data input: $required" >&2
		exit 1
	}
done

# Reject a scope whose history projection points outside the session tree. The
# experiment must never need to mount the Codex auth/config area.
bad_history_path="$(sqlite3 "file:$scope_dir/store.sqlite?mode=ro" "SELECT json_extract(j.value,'\$.path') FROM state_meta s,json_each(json_extract(s.value,'\$.history_watch')) j WHERE s.key='history_watch_projection' AND json_extract(j.value,'\$.path') IS NOT NULL;" | awk -v prefix="$codex_home/sessions/" 'index($0, prefix) != 1 { print; exit }')"
if [[ -n "$bad_history_path" ]]; then
	echo "refusing to mount a Codex path outside the read-only sessions tree: $bad_history_path" >&2
	exit 1
fi

mkdir -p "$fixture_dir/teams" "$fixture_dir/codex/sessions"
cp -- "$scope_dir/state.json" "$fixture_dir/teams/state.json"
cp -- "$scope_dir/registry.json" "$fixture_dir/teams/registry.json"
cp -- "$teams_root/global-inbound-ledger.json" "$fixture_dir/teams/global-inbound-ledger.json"
cp -- "$teams_root/global-outbound-ledger.json" "$fixture_dir/teams/global-outbound-ledger.json"
sqlite3 "file:$teams_root/global-inbound-ledger.sqlite?mode=ro" ".backup '$fixture_dir/teams/global-inbound-ledger.sqlite'"
sqlite3 "file:$teams_root/global-outbound-ledger.sqlite?mode=ro" ".backup '$fixture_dir/teams/global-outbound-ledger.sqlite'"
cp -- "$codex_home/history.jsonl" "$fixture_dir/codex/history.jsonl"
cp -- "$codex_home/session_index.jsonl" "$fixture_dir/codex/session_index.jsonl"

# sqlite3 .backup reads the live source, including its WAL, without replacing
# or checkpointing the source files. The container receives only this copy.
sqlite3 "file:$scope_dir/store.sqlite?mode=ro" ".backup '$fixture_dir/teams/store.sqlite'"
sqlite3 "file:$fixture_dir/teams/store.sqlite?mode=ro" "PRAGMA quick_check;" | grep -Fx ok >/dev/null

source_db_sha="$(sha256sum "$scope_dir/store.sqlite" | awk '{print $1}')"
source_state_sha="$(sha256sum "$scope_dir/state.json" | awk '{print $1}')"
echo "real-data snapshot: scope=$scope_dir codex_sessions=$codex_home/sessions"
sqlite3 "file:$fixture_dir/teams/store.sqlite?mode=ro" "SELECT 'rows', (SELECT count(*) FROM sessions),(SELECT count(*) FROM inbound_events),(SELECT count(*) FROM turns),(SELECT count(*) FROM chat_polls); SELECT 'queued_inbound',count(*) FROM inbound_events WHERE status='queued'; SELECT 'frontiers',sum(CASE WHEN json_extract(json,'\$.continuation_path') IS NOT NULL AND json_extract(json,'\$.continuation_path')<>'' THEN 1 ELSE 0 END),sum(CASE WHEN json_extract(json,'\$.gap') IS NOT NULL THEN 1 ELSE 0 END) FROM chat_polls;"

cd "$repo_root"
CGO_ENABLED=0 go test -c -o "$build_dir/teams-sqlite-history-fixture.test" ./internal/teams
mkdir -p "$build_dir/docker-mountpoints/sessions"
touch -- "$build_dir/docker-mountpoints/sessions/.keep"
docker build \
	--file scripts/ci/Dockerfile.teams-real-data-experiment \
	--tag "$image" \
	"$build_dir" >/dev/null
docker volume create "$runtime_volume" >/dev/null

set +e
docker run --rm \
	--network none \
	--cap-drop ALL \
	--cap-add DAC_READ_SEARCH \
	--security-opt no-new-privileges \
	--pids-limit 256 \
	--read-only \
	--tmpfs /tmp:rw,nosuid,nodev,size=256m \
	--env HOME=/runtime/home \
	--env TMPDIR=/tmp \
	--env CXP_TEAMS_DOCKER_FIXTURE_DIR=/fixture \
	--env CXP_TEAMS_DOCKER_RUNTIME_DIR=/runtime \
	--env CXP_TEAMS_DOCKER_CODEX_MOUNTED=1 \
	--env CXP_TEAMS_DOCKER_REAL_DATA_EXPERIMENT=1 \
	--env CXP_TEAMS_DOCKER_REAL_DATA_DURATION="${CXP_TEAMS_DOCKER_REAL_DATA_DURATION:-90s}" \
	--mount "type=bind,src=$fixture_dir,dst=/fixture,readonly" \
	--mount "type=bind,src=$fixture_dir/codex,dst=/home/baka/.codex,readonly" \
	--mount "type=bind,src=$codex_home/sessions,dst=/home/baka/.codex/sessions,readonly" \
	--mount "type=volume,src=$runtime_volume,dst=/runtime" \
	"$image" \
	/teams-sqlite-history-fixture.test \
	-test.run '^TestDockerRealDataTeamsProgressThroughput$' \
	-test.count=1 \
	-test.v
run_status=$?
set -e

after_db_sha="$(sha256sum "$scope_dir/store.sqlite" | awk '{print $1}')"
after_state_sha="$(sha256sum "$scope_dir/state.json" | awk '{print $1}')"
if [[ "$source_db_sha" != "$after_db_sha" || "$source_state_sha" != "$after_state_sha" ]]; then
	echo "WARNING: source pointer/database changed while the experiment was running; the experiment itself only read the source and wrote its disposable copy" >&2
else
	echo "source integrity check: state.json and store.sqlite unchanged"
fi

exit "$run_status"
