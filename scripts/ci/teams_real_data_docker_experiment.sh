#!/usr/bin/env bash
set -euo pipefail

# Run the real current Teams SQLite state through the production listener loop
# in a disposable, network-isolated container. This is intentionally an
# experiment, not a smoke test: the SQLite snapshot, registry, shared ledgers,
# and the complete current Codex session tree are copied into the disposable
# fixture. Only a deterministic in-process Graph server and executor are used;
# no Teams token is read or mounted, and Docker never reads the live session
# tree after fixture creation.
if [[ $# -lt 1 || $# -gt 2 ]]; then
	echo "usage: $0 TEAMS_SCOPE_DIR [CODEX_HOME]" >&2
	exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
scope_dir="$(cd "$1" && pwd -P)"
if ! codex_home="$(cd "${2:-/home/baka/.codex}" && pwd -P)"; then
	echo "unable to resolve CODEX_HOME as an existing directory: ${2:-/home/baka/.codex}" >&2
	exit 1
fi
teams_root="$(cd "$scope_dir/../.." && pwd -P)"
fixture_root="$(mktemp -d /tmp/cxp-teams-real-data-XXXXXX)"
fixture_dir=""
build_dir="$(mktemp -d /tmp/cxp-teams-real-build-XXXXXX)"
image="cxp-teams-real-data-experiment:${GITHUB_RUN_ID:-local}-$$"
runtime_volume="cxp-teams-real-data-runtime-${GITHUB_RUN_ID:-local}-$$"
docker_test_timeout="${CXP_TEAMS_DOCKER_TEST_TIMEOUT:-30m}"
docker_watchdog_timeout="${CXP_TEAMS_DOCKER_WATCHDOG_TIMEOUT:-35m}"
allow_source_drift="${CXP_TEAMS_DOCKER_ALLOW_SOURCE_DRIFT:-0}"
experiment_mode="${CXP_TEAMS_DOCKER_REAL_DATA_MODE:-throughput}"
experiment_duration="${CXP_TEAMS_DOCKER_REAL_DATA_DURATION:-5m}"
if [[ -n "${CXP_TEAMS_DOCKER_PROCESS_RESTART+x}" ]]; then
	process_restart="$CXP_TEAMS_DOCKER_PROCESS_RESTART"
elif [[ "$experiment_mode" == "complete" ]]; then
	# Complete mode may finish the entire corpus in its first process, so there
	# is no remaining work with which to prove a second-process progress delta.
	process_restart=0
else
	process_restart=1
fi
source_drift_diagnostic=0

case "$allow_source_drift" in
	0|1) ;;
	*)
		echo "CXP_TEAMS_DOCKER_ALLOW_SOURCE_DRIFT must be 0 or 1" >&2
		exit 2
		;;
esac

case "$process_restart" in
	0|1) ;;
	*)
		echo "CXP_TEAMS_DOCKER_PROCESS_RESTART must be 0 or 1" >&2
		exit 2
		;;
esac

cleanup() {
	docker image rm --force "$image" >/dev/null 2>&1 || true
	docker volume rm "$runtime_volume" >/dev/null 2>&1 || true
	rm -rf -- "$fixture_root" "$build_dir"
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
command -v timeout >/dev/null 2>&1 || {
	echo "timeout is required to put a hard outer watchdog around the Docker experiment" >&2
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
	if [[ -L "$required" ]]; then
		echo "refusing symlink real-data input: $required" >&2
		exit 1
	fi
done

for required_file in \
	"$scope_dir/state.json" \
	"$scope_dir/store.sqlite" \
	"$scope_dir/registry.json" \
	"$teams_root/global-inbound-ledger.json" \
	"$teams_root/global-inbound-ledger.sqlite" \
	"$teams_root/global-outbound-ledger.json" \
	"$teams_root/global-outbound-ledger.sqlite" \
	"$codex_home/history.jsonl" \
	"$codex_home/session_index.jsonl"; do
	[[ -f "$required_file" ]] || {
		echo "required real-data input is not a regular file: $required_file" >&2
		exit 1
	}
done
[[ -d "$codex_home/sessions" ]] || {
	echo "required Codex sessions input is not a directory: $codex_home/sessions" >&2
	exit 1
}

source_core_inputs=(
	"$scope_dir/state.json"
	"$scope_dir/store.sqlite"
	"$scope_dir/registry.json"
	"$scope_dir/control-chat-history.jsonl"
	"$scope_dir/control-chat-history.sqlite"
	"$scope_dir/helper-restart-pending.json"
	"$scope_dir/workflow-notifications.json"
	"$teams_root/global-inbound-ledger.json"
	"$teams_root/global-inbound-ledger.sqlite"
	"$teams_root/global-outbound-ledger.json"
	"$teams_root/global-outbound-ledger.sqlite"
	"$codex_home/history.jsonl"
	"$codex_home/session_index.jsonl"
)

write_source_core_manifest() {
	local output="$1"
	: > "$output" || return 1
	for input in "${source_core_inputs[@]}"; do
		if [[ -e "$input" ]]; then
			sha256sum -- "$input" >> "$output" || return 1
		else
			printf 'missing  %s\n' "$input" >> "$output" || return 1
		fi
	done
	# SQLite .backup calls below take a transaction-consistent snapshot, so the
	# live -wal/-shm sidecars are intentionally not manifest inputs: they are not
	# copied or mounted into the container and can change on every helper tick.
	# Session files are immutable/atomically replaced by the Codex writer. The
	# manifest records path, inode, size, mtime, and ctime so an append, replace,
	# or same-size rewrite with restored mtime during the copy is rejected
	# without hashing tens of gigabytes of history bodies.
	if ! find "$codex_home/sessions" -type f -printf '%P %i %s %T@ %C@\n' | sort | sha256sum >> "$output"; then
		return 1
	fi
	# Thread-link journals are small but operational: omitting them would make
	# linked-transcript recovery appear healthy only because the copied fixture
	# had no source-side ownership history. Hash their paths and contents too.
	if [[ -d "$scope_dir/thread-links" ]]; then
		if ! find "$scope_dir/thread-links" -type f -name '*.jsonl' -exec sha256sum {} + | sort | sha256sum >> "$output"; then
			return 1
		fi
	else
		if ! printf 'missing-thread-links\n' | sha256sum >> "$output"; then
			return 1
		fi
	fi
}

copy_optional_regular() {
	local source_path="$1"
	local destination_path="$2"
	if [[ ! -e "$source_path" ]]; then
		if [[ -L "$source_path" ]]; then
			echo "refusing dangling symlink optional fixture input: $source_path" >&2
			return 1
		fi
		return 0
	fi
	if [[ -L "$source_path" || ! -f "$source_path" ]]; then
		echo "refusing to copy non-regular optional fixture input: $source_path" >&2
		return 1
	fi
	mkdir -p "$(dirname "$destination_path")"
	if ! cp --no-dereference -- "$source_path" "$destination_path"; then
		return 1
	fi
	if [[ -L "$destination_path" || ! -f "$destination_path" ]]; then
		echo "refusing copied optional fixture that is not a regular file: $destination_path" >&2
		return 1
	fi
}

copy_optional_sqlite_backup() {
	local source_path="$1"
	local destination_path="$2"
	if [[ ! -e "$source_path" ]]; then
		if [[ -L "$source_path" ]]; then
			echo "refusing dangling symlink optional SQLite input: $source_path" >&2
			return 1
		fi
		return 0
	fi
	if [[ -L "$source_path" || ! -f "$source_path" ]]; then
		echo "refusing to copy non-regular optional SQLite input: $source_path" >&2
		return 1
	fi
	mkdir -p "$(dirname "$destination_path")"
	if ! sqlite3 "file:$source_path?mode=ro" ".backup '$destination_path'"; then
		return 1
	fi
	if ! sqlite3 "file:$destination_path?mode=ro" "PRAGMA quick_check;" | grep -Fx ok >/dev/null; then
		return 1
	fi
}

# Reject a scope whose history projection points outside the session tree. The
# experiment must never need to mount the Codex auth/config area. Lexical
# prefix checks are insufficient because a path such as sessions-evil/ also
# starts with the sessions/ string; resolve each existing path and require
# canonical containment instead. This check is repeated for every snapshot
# attempt because the live SQLite projection may change while the helper is
# running.
validate_history_paths() {
	local sessions_root bad_history_path history_rows
	if ! sessions_root="$(realpath -e -- "$codex_home/sessions")"; then return 1; fi
	if ! history_rows="$(sqlite3 "file:$scope_dir/store.sqlite?mode=ro" "SELECT json_extract(j.value,'\$.path') FROM state_meta s,json_each(json_extract(s.value,'\$.history_watch')) j WHERE s.key='history_watch_projection' AND json_extract(j.value,'\$.path') IS NOT NULL;")"; then
		echo "unable to read history-watch paths from the source SQLite database" >&2
		return 1
	fi
	bad_history_path="$(while IFS= read -r candidate; do
		[[ -z "$candidate" ]] && continue
		canonical_candidate="$(realpath -e -- "$candidate" 2>/dev/null || true)"
		case "$canonical_candidate" in
			"$sessions_root"/*) ;;
			*) printf '%s\n' "$candidate"; break ;;
		esac
	done <<< "$history_rows")"
	if [[ -n "$bad_history_path" ]]; then
		echo "refusing to mount a Codex path outside the read-only sessions tree: $bad_history_path" >&2
		return 1
	fi
}

assemble_fixture_snapshot() {
	local candidate="$1"
	local source_core_manifest_before="$2"
	local source_core_manifest_after="$3"
	if ! write_source_core_manifest "$source_core_manifest_before"; then return 1; fi
	if ! validate_history_paths; then return 1; fi

	if ! mkdir -p "$candidate/teams" "$candidate/codex/sessions"; then return 1; fi
	if ! cp --no-dereference -- "$scope_dir/state.json" "$candidate/teams/state.json"; then return 1; fi
	if ! cp --no-dereference -- "$scope_dir/registry.json" "$candidate/teams/registry.json"; then return 1; fi
	if ! cp --no-dereference -- "$teams_root/global-inbound-ledger.json" "$candidate/teams/global-inbound-ledger.json"; then return 1; fi
	if ! cp --no-dereference -- "$teams_root/global-outbound-ledger.json" "$candidate/teams/global-outbound-ledger.json"; then return 1; fi
	if ! sqlite3 "file:$teams_root/global-inbound-ledger.sqlite?mode=ro" ".backup '$candidate/teams/global-inbound-ledger.sqlite'"; then return 1; fi
	if ! sqlite3 "file:$teams_root/global-outbound-ledger.sqlite?mode=ro" ".backup '$candidate/teams/global-outbound-ledger.sqlite'"; then return 1; fi
	if ! copy_optional_regular "$scope_dir/control-chat-history.jsonl" "$candidate/teams/control-chat-history.jsonl"; then return 1; fi
	if ! copy_optional_sqlite_backup "$scope_dir/control-chat-history.sqlite" "$candidate/teams/control-chat-history.sqlite"; then return 1; fi
	if ! copy_optional_regular "$scope_dir/helper-restart-pending.json" "$candidate/teams/helper-restart-pending.json"; then return 1; fi
	if ! copy_optional_regular "$scope_dir/workflow-notifications.json" "$candidate/teams/workflow-notifications.json"; then return 1; fi
	if [[ -L "$scope_dir/thread-links" ]]; then
		echo "refusing symlink thread-links tree: $scope_dir/thread-links" >&2
		return 1
	fi
	if [[ -d "$scope_dir/thread-links" ]]; then
		if ! thread_link_symlinks="$(find "$scope_dir/thread-links" -type l -print -quit)"; then return 1; fi
		if [[ -n "$thread_link_symlinks" ]]; then
			echo "refusing to copy a thread-links tree containing symlinks" >&2
			return 1
		fi
		thread_link_inventory="$candidate/.thread-links.inventory"
		if ! find "$scope_dir/thread-links" -type f -name '*.jsonl' -print0 > "$thread_link_inventory"; then return 1; fi
		while IFS= read -r -d '' journal; do
			relative="${journal#"$scope_dir/thread-links/"}"
			if ! copy_optional_regular "$journal" "$candidate/teams/thread-links/$relative"; then return 1; fi
		done < "$thread_link_inventory"
		rm -f -- "$thread_link_inventory"
	fi
	if ! cp --no-dereference -- "$codex_home/history.jsonl" "$candidate/codex/history.jsonl"; then return 1; fi
	if ! cp --no-dereference -- "$codex_home/session_index.jsonl" "$candidate/codex/session_index.jsonl"; then return 1; fi
	# Do not use find -L here: dereferencing first could traverse an external
	# symlink before the rejection is observed and copy data outside the intended
	# point-in-time session tree.
	if ! session_symlinks="$(find "$codex_home/sessions" -type l -print -quit)"; then return 1; fi
	if [[ -n "$session_symlinks" ]]; then
		echo "refusing to copy a Codex sessions tree containing symlinks" >&2
		return 1
	fi
	if ! cp -a --no-dereference --reflink=auto "$codex_home/sessions/." "$candidate/codex/sessions/"; then return 1; fi
	if ! session_symlinks="$(find "$candidate/codex/sessions" -type l -print -quit)"; then return 1; fi
	if [[ -n "$session_symlinks" ]]; then
		echo "refusing copied Codex sessions tree containing symlinks: $session_symlinks" >&2
		return 1
	fi
	if ! copied_symlinks="$(find "$candidate" -type l -print -quit)"; then return 1; fi
	if [[ -n "$copied_symlinks" ]]; then
		echo "refusing copied fixture containing symlink: $copied_symlinks" >&2
		return 1
	fi

	# sqlite3 .backup reads the live source, including its WAL, without replacing
	# or checkpointing the source files. The container receives only this copy.
	if ! sqlite3 "file:$scope_dir/store.sqlite?mode=ro" ".backup '$candidate/teams/store.sqlite'"; then return 1; fi
	if ! sqlite3 "file:$candidate/teams/store.sqlite?mode=ro" "PRAGMA quick_check;" | grep -Fx ok >/dev/null; then return 1; fi

	if ! write_source_core_manifest "$source_core_manifest_after"; then return 1; fi
	if ! cmp -s "$source_core_manifest_before" "$source_core_manifest_after"; then
		echo "source inputs changed while the point-in-time fixture was being assembled; this attempt is not cross-file stable" >&2
		diff -u "$source_core_manifest_before" "$source_core_manifest_after" >&2 || true
		echo "SQLite .backup snapshots include committed WAL content; volatile SQLite -wal/-shm sidecars are not copied or used as fixture inputs" >&2
		# Return a distinct status so the caller can retry. A candidate whose
		# source manifest moved is never silently treated as a validated fixture.
		return 2
	fi
	return 0
}

# A live Codex writer may atomically add/replace a session while the fixture is
# being copied. Retry only after the complete before/after manifest rejects
# that attempt; never combine files from different observations, and never
# accept an attempt whose source manifest still differs unless the caller has
# explicitly requested a diagnostic run with source drift allowed.
snapshot_attempts=3
if [[ "$allow_source_drift" == "1" ]]; then
	# The caller explicitly requested a diagnostic run on a live, mutating
	# source. One internally consistent SQLite/session snapshot is enough; three
	# identical rejected copies only add minutes without improving isolation.
	snapshot_attempts=1
fi
fallback_fixture_dir=""
accepted_source_manifest=""
fallback_source_manifest=""
for attempt in $(seq 1 "$snapshot_attempts"); do
	candidate="$(mktemp -d "$fixture_root/attempt-XXXXXX")"
	before_manifest="$build_dir/source-core.before.$attempt.sha256"
	after_manifest="$build_dir/source-core.after.$attempt.sha256"
	if assemble_fixture_snapshot "$candidate" "$before_manifest" "$after_manifest"; then
		fixture_dir="$candidate"
		accepted_source_manifest="$before_manifest"
		break
	else
		assemble_status=$?
	fi
	if [[ "$assemble_status" -eq 2 && -s "$candidate/teams/store.sqlite" ]]; then
		if [[ -n "$fallback_fixture_dir" ]]; then
			rm -rf -- "$fallback_fixture_dir"
		fi
		fallback_fixture_dir="$candidate"
		fallback_source_manifest="$before_manifest"
	else
		rm -rf -- "$candidate"
	fi
	if [[ "$attempt" -lt "$snapshot_attempts" ]]; then
		sleep 1
	fi
done
if [[ -z "$fixture_dir" ]]; then
	if [[ "$allow_source_drift" == "1" && -n "$fallback_fixture_dir" ]]; then
		fixture_dir="$fallback_fixture_dir"
		accepted_source_manifest="$fallback_source_manifest"
		source_drift_diagnostic=1
		echo "WARNING: source drift explicitly allowed; using the last isolated SQLite/session fixture after $snapshot_attempts rejected attempts; result is diagnostic, not a point-in-time acceptance" >&2
	else
		echo "refusing to run Docker acceptance: unable to assemble a cross-file-stable fixture after $snapshot_attempts attempts (set CXP_TEAMS_DOCKER_ALLOW_SOURCE_DRIFT=1 only for explicitly diagnostic runs)" >&2
		exit 1
	fi
fi

# Docker may run rootless, so UID 0 inside the container is not necessarily
# the host user that created mktemp's 0700 directories/files. The fixture is a
# disposable read-only bind mount with no auth token; make only this temporary
# snapshot traversable/readable. Never broaden permissions on the live source
# tree or on the runtime volume.
chmod a+rx "$fixture_root" "$fixture_dir"
chmod -R a+rX "$fixture_dir"

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

run_experiment_process() {
	local resume="$1"
	local reuse_runtime="$2"
	timeout --foreground --kill-after=30s "$docker_watchdog_timeout" docker run --rm --stop-timeout 30 \
		--network none \
		--cap-drop ALL \
		--security-opt no-new-privileges \
		--pids-limit 256 \
		--read-only \
		--tmpfs /tmp:rw,nosuid,nodev,size=256m \
		--env HOME=/runtime/home \
		--env TMPDIR=/tmp \
		--env CXP_TEAMS_DOCKER_FIXTURE_DIR=/fixture \
		--env CXP_TEAMS_DOCKER_RUNTIME_DIR=/runtime \
		--env CXP_TEAMS_DOCKER_RUNTIME_REUSE="$reuse_runtime" \
		--env CXP_TEAMS_DOCKER_REAL_DATA_EXPERIMENT=1 \
		--env CXP_TEAMS_DOCKER_REAL_DATA_RESUME="$resume" \
		--env CXP_TEAMS_DOCKER_REAL_DATA_MODE="$experiment_mode" \
		--env CXP_TEAMS_DOCKER_REAL_DATA_DURATION="$experiment_duration" \
		--env CXP_TEAMS_DOCKER_CODEX_SOURCE_PREFIX="${codex_home%/}/" \
		--mount "type=bind,src=$fixture_dir,dst=/fixture,readonly" \
		--mount "type=bind,src=$fixture_dir/codex,dst=/home/baka/.codex,readonly" \
		--mount "type=volume,src=$runtime_volume,dst=/runtime" \
		"$image" \
		/teams-sqlite-history-fixture.test \
		-test.run '^TestDockerRealDataTeamsProgressThroughput$' \
		-test.count=1 \
		-test.timeout "$docker_test_timeout" \
		-test.v
}

set +e
run_experiment_process 0 0
first_process_status=$?
run_status=$first_process_status
if [[ "$process_restart" == "1" && "$first_process_status" -eq 0 ]]; then
	echo "real-data Docker experiment: starting a second container process against the same disposable runtime" >&2
	run_experiment_process 1 1
	run_status=$?
fi
set -e

run_source_manifest="$build_dir/source-core.after-run.sha256"
write_source_core_manifest "$run_source_manifest"
if [[ -n "$accepted_source_manifest" ]] && ! cmp -s "$accepted_source_manifest" "$run_source_manifest"; then
	if [[ "$allow_source_drift" == "1" ]]; then
		echo "WARNING: source inputs changed while the diagnostic experiment was running; only the disposable fixture was written, and this result is not point-in-time acceptance" >&2
		source_drift_diagnostic=1
	else
		echo "ERROR: source inputs changed while the Docker acceptance experiment was running; refusing to report a point-in-time result" >&2
		run_status=1
	fi
else
	echo "source integrity check: all manifest inputs unchanged"
fi

# A drift-tolerant run is useful for diagnosis on a busy workstation, but it
# must never be reported as a successful acceptance result.  The fixture and
# live database remain isolated in either case; this status only prevents a
# mixed point-in-time observation from becoming a green gate.
if [[ "$source_drift_diagnostic" -ne 0 ]]; then
	run_status=1
fi

exit "$run_status"
