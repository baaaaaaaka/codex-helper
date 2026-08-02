#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
image="cxp-teams-codex-fork-compose:${GITHUB_RUN_ID:-local}-$$"
compose_file="$repo_root/scripts/ci/teams_codex_fork_compose.yml"
keep_failed="${CXP_FORK_COMPOSE_KEEP_FAILED:-0}"

cleanup() {
	if [[ -n "${active_project:-}" && "$keep_failed" != "1" ]]; then
		docker compose -p "$active_project" -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
	fi
	if [[ "$keep_failed" != "1" ]]; then
		docker image rm --force "$image" >/dev/null 2>&1 || true
	fi
	rm -rf "$tmp_dir"
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || {
	echo "docker is required for the Teams Codex fork Compose smoke test" >&2
	exit 1
}
docker info >/dev/null 2>&1 || {
	echo "docker daemon is unavailable for the Teams Codex fork Compose smoke test" >&2
	exit 1
}
docker compose version >/dev/null 2>&1 || {
	echo "docker compose is required for the Teams Codex fork Compose smoke test" >&2
	exit 1
}

cd "$repo_root"
CGO_ENABLED=0 go test -c -o "$tmp_dir/teams-fork.test" ./internal/teams
CGO_ENABLED=0 go build -o "$tmp_dir/fake-codex" ./scripts/tests/teams_fork_compose/fake-codex
CGO_ENABLED=0 go build -o "$tmp_dir/fake-graph" ./scripts/tests/teams_fork_compose/fake-graph
CGO_ENABLED=0 go build -o "$tmp_dir/controller" ./scripts/tests/teams_fork_compose/controller
CGO_ENABLED=0 go build -o "$tmp_dir/observer" ./scripts/tests/teams_fork_compose/observer
cp "$repo_root/scripts/ci/Dockerfile.teams-codex-fork-compose" "$tmp_dir/Dockerfile"

docker build --file "$tmp_dir/Dockerfile" --tag "$image" "$tmp_dir" >/dev/null

scenarios=(
	happy-path
	native-response-lost
	graph-response-lost
	activated-restart
	owner-takeover
)
if [[ -n "${CXP_FORK_COMPOSE_SCENARIO_ONLY:-}" ]]; then
	scenarios=("$CXP_FORK_COMPOSE_SCENARIO_ONLY")
fi

for store_kind in json sqlite; do
	for scenario in "${scenarios[@]}"; do
		active_project="cxp-fork-${store_kind}-${scenario}-$$"
		export CXP_FORK_COMPOSE_IMAGE="$image"
		export CXP_FORK_COMPOSE_SCENARIO="$scenario"
		export CXP_FORK_COMPOSE_STORE_KIND="$store_kind"
		echo "=== Compose fork scenario: store=$store_kind scenario=$scenario ==="
		docker compose -p "$active_project" -f "$compose_file" config >/dev/null
		docker compose -p "$active_project" -f "$compose_file" up -d

		observer_id=""
		deadline=$((SECONDS + 600))
		while (( SECONDS < deadline )); do
			observer_id="$(docker compose -p "$active_project" -f "$compose_file" ps -aq observer 2>/dev/null || true)"
			if [[ -n "$observer_id" ]]; then
				status="$(docker inspect -f '{{.State.Status}}' "$observer_id" 2>/dev/null || true)"
				if [[ "$status" == "exited" || "$status" == "dead" ]]; then
					break
				fi
			fi
			sleep 1
		done
		if [[ -z "$observer_id" ]] || [[ "$(docker inspect -f '{{.State.Status}}' "$observer_id" 2>/dev/null || true)" != "exited" ]]; then
			docker compose -p "$active_project" -f "$compose_file" ps || true
			docker compose -p "$active_project" -f "$compose_file" logs || true
			echo "Compose observer timed out for $store_kind/$scenario" >&2
			exit 1
		fi
		observer_code="$(docker inspect -f '{{.State.ExitCode}}' "$observer_id")"
		if [[ "$observer_code" != "0" ]]; then
			docker compose -p "$active_project" -f "$compose_file" ps || true
			docker compose -p "$active_project" -f "$compose_file" logs || true
			echo "Compose observer failed for $store_kind/$scenario" >&2
			exit 1
		fi
		docker compose -p "$active_project" -f "$compose_file" down -v --remove-orphans >/dev/null
		active_project=""
	done
done

echo "Compose fork fault matrix passed: ${#scenarios[@]} scenarios x 2 store backends"
