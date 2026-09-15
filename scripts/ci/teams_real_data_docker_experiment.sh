#!/usr/bin/env bash
set -euo pipefail

# Run the real current Teams SQLite state through the production listener loop
# in a disposable, network-isolated container. This is intentionally an
# experiment, not a smoke test: the SQLite snapshot, registry, shared ledgers,
# and the Codex files needed by the copied history projection are copied into
# the disposable fixture. Only a deterministic in-process Graph server and
# executor are used; no Teams token is read or mounted, and Docker never reads
# the live session tree after fixture creation. The session corpus is sparse:
# copying every historical JSONL body would turn fixture creation into a 30+ GB
# disk experiment instead of a Teams experiment.
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
runtime_dir="$(mktemp -d "$build_dir/runtime-XXXXXX")"
image="cxp-teams-real-data-experiment:${GITHUB_RUN_ID:-local}-$$"
host_uid="$(id -u)"
host_gid="$(id -g)"
fixture_min_free_bytes="${CXP_TEAMS_DOCKER_FIXTURE_MIN_FREE_BYTES:-4294967296}"
docker_test_timeout="${CXP_TEAMS_DOCKER_TEST_TIMEOUT:-30m}"
docker_watchdog_timeout="${CXP_TEAMS_DOCKER_WATCHDOG_TIMEOUT:-35m}"
fixture_codex_dir="/home/baka/.codex"
allow_source_drift="${CXP_TEAMS_DOCKER_ALLOW_SOURCE_DRIFT:-0}"
experiment_mode="${CXP_TEAMS_DOCKER_REAL_DATA_MODE:-throughput}"
experiment_duration="${CXP_TEAMS_DOCKER_REAL_DATA_DURATION:-5m}"
rate_limit_experiment="${CXP_TEAMS_DOCKER_REAL_DATA_429:-0}"
rate_limit_scope="${CXP_TEAMS_DOCKER_REAL_DATA_429_SCOPE:-chat}"
poll_interval="${CXP_TEAMS_DOCKER_REAL_DATA_POLL_INTERVAL:-}"
recent_session_hours="${CXP_TEAMS_DOCKER_RECENT_SESSION_HOURS:-24}"
if [[ -n "${CXP_TEAMS_DOCKER_PROCESS_RESTART+x}" ]]; then
	process_restart="$CXP_TEAMS_DOCKER_PROCESS_RESTART"
elif [[ "$rate_limit_experiment" == "1" ]]; then
	# The bounded 429 experiment intentionally isolates read-gate recovery. It
	# does not create the ordinary experiment's ambiguous POST witness, so do
	# not launch a resume process that would require that unrelated artifact.
	process_restart=0
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

case "$rate_limit_experiment" in
	0|1) ;;
	*)
		echo "CXP_TEAMS_DOCKER_REAL_DATA_429 must be 0 or 1" >&2
		exit 2
		;;
esac

case "$rate_limit_scope" in
	chat|account|global) ;;
	*)
		echo "CXP_TEAMS_DOCKER_REAL_DATA_429_SCOPE must be chat, account, or global" >&2
		exit 2
		;;
esac

if [[ "$rate_limit_experiment" == "1" && "$process_restart" == "1" ]]; then
	echo "CXP_TEAMS_DOCKER_PROCESS_RESTART=1 cannot be combined with the isolated 429 experiment; run the unknown-POST restart experiment separately" >&2
	exit 2
fi

if [[ ! "$recent_session_hours" =~ ^[0-9]+$ ]] || [[ "$recent_session_hours" -lt 1 ]]; then
	echo "CXP_TEAMS_DOCKER_RECENT_SESSION_HOURS must be a positive integer" >&2
	exit 2
fi

if [[ ! "$fixture_min_free_bytes" =~ ^[1-9][0-9]*$ ]]; then
	echo "CXP_TEAMS_DOCKER_FIXTURE_MIN_FREE_BYTES must be a positive integer" >&2
	exit 2
fi

cleanup() {
	docker image rm --force "$image" >/dev/null 2>&1 || true
	rm -rf -- "$fixture_root" "$build_dir" "$runtime_dir"
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

require_fixture_free_space() {
	local requested_bytes="$1"
	local description="$2"
	local available_bytes
	if [[ ! "$requested_bytes" =~ ^[0-9]+$ ]]; then
		echo "internal fixture space check received an invalid byte count: $requested_bytes" >&2
		return 1
	fi
	available_bytes="$(df -P -B1 -- "$fixture_root" | awk 'NR == 2 {print $4}')"
	if [[ ! "$available_bytes" =~ ^[0-9]+$ ]]; then
		echo "unable to determine free space for Docker fixture: $fixture_root" >&2
		return 1
	fi
	if (( available_bytes < requested_bytes || available_bytes - requested_bytes < fixture_min_free_bytes )); then
		echo "refusing fixture copy of $description: need=${requested_bytes}B plus reserved=${fixture_min_free_bytes}B, available=${available_bytes}B" >&2
		return 1
	fi
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
	local input suffix sidecar
	: > "$output" || return 1
	for input in "${source_core_inputs[@]}"; do
		if [[ -e "$input" ]]; then
			sha256sum -- "$input" >> "$output" || return 1
		else
			printf 'missing  %s\n' "$input" >> "$output" || return 1
		fi
	done
	# SQLite .backup calls below take a transaction-consistent snapshot. Include
	# the source -wal/-shm bytes in the pre/post manifest even though they are not
	# copied or mounted into the container: a WAL-only commit or checkpoint must
	# invalidate this cross-file snapshot attempt rather than look unchanged just
	# because the main database file did not move. Session files are
	# immutable/atomically replaced by the Codex writer. The
	# manifest records path, inode, size, mtime, and ctime so an append, replace,
	# or same-size rewrite with restored mtime during the copy is rejected
	# without hashing tens of gigabytes of history bodies.
	if ! find "$codex_home/sessions" -type f -printf '%P %i %s %T@ %C@\n' | sort | sha256sum >> "$output"; then
		return 1
	fi
	for input in "${source_core_inputs[@]}"; do
		case "$input" in
			*.sqlite)
				for suffix in -wal -shm; do
					sidecar="$input$suffix"
					if [[ -L "$sidecar" ]]; then
						echo "refusing symlink SQLite sidecar: $sidecar" >&2
						return 1
					elif [[ -e "$sidecar" ]]; then
						if [[ ! -f "$sidecar" ]] || ! sha256sum -- "$sidecar" >> "$output"; then
							echo "refusing unreadable SQLite sidecar: $sidecar" >&2
							return 1
						fi
					else
						printf 'missing  %s\n' "$sidecar" >> "$output" || return 1
					fi
				 done
				;;
		esac
	done
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

sqlite_backup_readonly() {
	local source_path="$1"
	local destination_path="$2"
	local source_journal_mode
	local source_size
	source_size="$(stat -c '%s' -- "$source_path")"
	if [[ ! "$source_size" =~ ^[0-9]+$ ]]; then
		echo "unable to determine source SQLite size: $source_path" >&2
		return 1
	fi
	if ! require_fixture_free_space "$source_size" "SQLite backup $source_path"; then
		return 1
	fi
	source_journal_mode="$(sqlite3 "file:$source_path?mode=ro" 'PRAGMA journal_mode;' | tail -n 1 | tr '[:upper:]' '[:lower:]')"
	if [[ -z "$source_journal_mode" ]]; then
		echo "unable to determine source SQLite journal mode: $source_path" >&2
		return 1
	fi
	# The live helper can hold a short SQLite write/schema lock while this
	# disposable snapshot is assembled.  The sqlite3 CLI default timeout is
	# effectively zero for .backup, which made a transient source lock abort an
	# otherwise safe experiment before Docker even started.  Wait on the source
	# lock, but never open the source for writing or copy its WAL/SHM files.
	if ! sqlite3 -cmd ".timeout 30000" "file:$source_path?mode=ro" ".backup '$destination_path'"; then
		return 1
	fi
	# The SQLite backup API does not reliably carry the source journal mode to
	# the destination. A WAL source otherwise becomes a DELETE-journal fixture,
	# where the long read-only projection audit blocks every durable writer and
	# makes the Docker result look like a Teams/listener deadlock. Recreate the
	# source's journal mode on the disposable destination only; never issue this
	# pragma against the live source.
	case "$source_journal_mode" in
	wal)
		if [[ "$(sqlite3 "$destination_path" 'PRAGMA journal_mode=WAL;' | tail -n 1 | tr '[:upper:]' '[:lower:]')" != "wal" ]]; then
			echo "failed to preserve WAL journal mode in SQLite fixture: $destination_path" >&2
			return 1
		fi
		;;
	delete|truncate|persist|memory|off)
		# These modes are already the backup destination default or are not
		# safe to force without changing the source's locking semantics.
		;;
	*)
		echo "unsupported source SQLite journal mode $source_journal_mode: $source_path" >&2
		return 1
		;;
	esac
	sqlite3 "file:$destination_path?mode=ro" "PRAGMA quick_check;" | grep -Fx ok >/dev/null
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
	sqlite_backup_readonly "$source_path" "$destination_path"
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

# Copy only the bytes a history checkpoint can prove it needs. A checkpoint
# whose cursor is at offset O still authenticates the bounded 8 KiB window
# immediately before O; an unfinished cursor also needs the bytes from O to
# the current end of the source. The destination retains the source's logical
# size, so sparse holes are never mistaken for valid transcript bytes. This
# keeps the fixture faithful for history-tail replay while avoiding a second
# copy of tens of gigabytes of already-consumed JSONL.
copy_sparse_history_file() {
	local source_path="$1"
	local destination_path="$2"
	local checkpoint_offset="$3"
	local projected_size="$4"
	local source_size copy_start aligned_start copy_length block_size skip_blocks
	if [[ -L "$source_path" || ! -f "$source_path" ]]; then
		echo "refusing non-regular history source: $source_path" >&2
		return 1
	fi
	if [[ ! "$checkpoint_offset" =~ ^[0-9]+$ ]]; then
		echo "history checkpoint offset is not a non-negative integer for $source_path: $checkpoint_offset" >&2
		return 1
	fi
	if [[ ! "$projected_size" =~ ^[0-9]+$ ]]; then
		projected_size=0
	fi
	source_size="$(stat -c '%s' -- "$source_path")"
	if [[ ! "$source_size" =~ ^[0-9]+$ ]]; then
		echo "unable to read history source size: $source_path" >&2
		return 1
	fi
	if [[ "$source_size" -lt "$projected_size" || "$checkpoint_offset" -gt "$source_size" ]]; then
		echo "history source is behind its durable checkpoint: path=$source_path source_size=$source_size checkpoint_size=$projected_size offset=$checkpoint_offset" >&2
		return 1
	fi
	mkdir -p "$(dirname "$destination_path")"
	if ! truncate -s "$source_size" -- "$destination_path"; then return 1; fi
	# transcriptCheckpointFingerprintBytes is 8 KiB in the production scanner.
	copy_start=$((checkpoint_offset > 8192 ? checkpoint_offset - 8192 : 0))
	block_size=$((1024 * 1024))
	aligned_start=$((copy_start / block_size * block_size))
	copy_length=$((source_size - aligned_start))
	if [[ "$copy_length" -gt 0 ]]; then
		if ! require_fixture_free_space "$copy_length" "history proof tail $source_path"; then
			return 1
		fi
		skip_blocks=$((aligned_start / block_size))
		if ! dd if="$source_path" of="$destination_path" iflag=count_bytes,fullblock skip="$skip_blocks" seek="$skip_blocks" count="$copy_length" bs="$block_size" conv=notrunc status=none; then
			return 1
		fi
	fi
	if ! chmod --reference="$source_path" -- "$destination_path"; then return 1; fi
	if ! touch -r "$source_path" -- "$destination_path"; then return 1; fi
	if [[ -L "$destination_path" || ! -f "$destination_path" ]]; then
		echo "refusing copied sparse history that is not a regular file: $destination_path" >&2
		return 1
	fi
}

copy_referenced_history_files() {
	local candidate="$1"
	local snapshot_store="$2"
	local sessions_root history_inventory source_path checkpoint_offset projected_size
	local canonical_source relative destination
	sessions_root="$(realpath -e -- "$codex_home/sessions")"
	history_inventory="$candidate/.history-selection.tsv"
	if [[ -z "$snapshot_store" || ! -f "$snapshot_store" ]]; then
		echo "history inventory requires the transaction-consistent scope SQLite snapshot" >&2
		return 1
	fi
	if ! sqlite3 -separator $'\t' "file:$snapshot_store?mode=ro" > "$history_inventory" <<'SQL'
WITH refs(path, object, projected_size) AS (
	SELECT json_extract(j.value, '$.path'),
	       CASE WHEN json_valid(j.value) THEN j.value ELSE '{}' END,
	       CAST(CASE WHEN json_valid(j.value) THEN COALESCE(json_extract(j.value, '$.size'), 0) ELSE 0 END AS INTEGER)
	FROM state_meta AS s,
	     json_each(CASE WHEN json_valid(s.value) THEN json_extract(s.value, '$.history_watch') ELSE '[]' END) AS j
	WHERE s.key = 'history_watch_projection'
	  AND json_extract(j.value, '$.path') IS NOT NULL
	UNION ALL
	SELECT json_extract(j.value, '$.pending_history_range.source_path'),
	       CASE WHEN json_valid(j.value) THEN j.value ELSE '{}' END,
	       CAST(CASE WHEN json_valid(j.value) THEN COALESCE(json_extract(j.value, '$.size'), 0) ELSE 0 END AS INTEGER)
	FROM state_meta AS s,
	     json_each(CASE WHEN json_valid(s.value) THEN json_extract(s.value, '$.history_watch') ELSE '[]' END) AS j
	WHERE s.key = 'history_watch_projection'
	  AND json_extract(j.value, '$.pending_history_range.source_path') IS NOT NULL
	UNION ALL
	SELECT json_extract(json, '$.source_path'),
	       json,
	       CAST(COALESCE(json_extract(json, '$.source_size'), 0) AS INTEGER)
	FROM import_checkpoints
	WHERE json_valid(json)
	  AND json_extract(json, '$.source_path') IS NOT NULL
),
outbox_needed(path, projected_size, required_start) AS (
	SELECT json_extract(json, '$.transcript_source_path'),
	       0,
	       min(
			CASE WHEN json_type(json, '$.transcript_source_offset') IN ('integer', 'real')
			     THEN max(0, CAST(json_extract(json, '$.transcript_source_offset') AS INTEGER) - 8192)
			     ELSE 9223372036854775807 END,
			CASE WHEN json_type(json, '$.transcript_source_proof_offset') IN ('integer', 'real')
			     THEN max(0, CAST(json_extract(json, '$.transcript_source_proof_offset') AS INTEGER) - 8192)
			     ELSE 9223372036854775807 END,
			CASE WHEN json_type(json, '$.transcript_source_read_proof_start_offset') IN ('integer', 'real')
			     THEN max(0, CAST(json_extract(json, '$.transcript_source_read_proof_start_offset') AS INTEGER))
			     ELSE 9223372036854775807 END
	       )
	FROM outbox_messages
	WHERE json_valid(json)
	  AND json_extract(json, '$.transcript_source_path') IS NOT NULL
),
needed(path, projected_size, required_start) AS (
	SELECT path, projected_size, 0
	FROM refs
	UNION ALL
	SELECT path, projected_size, required_start
	FROM outbox_needed
	WHERE required_start < 9223372036854775807
	UNION ALL
	SELECT r.path, r.projected_size,
	       CASE
	       WHEN tree.key = 'offset' THEN max(0, CAST(tree.value AS INTEGER) - 8192)
	       ELSE max(0, CAST(tree.value AS INTEGER))
	       END
	FROM refs AS r, json_tree(r.object) AS tree
	WHERE tree.type IN ('integer', 'real')
	  AND tree.key IN (
		'offset',
		'partial_line_start_offset',
		'partial_replay_offset',
		'pending_opaque_record_start_offset',
		'source_rewrite_recovery_scan_offset',
		'last_final_start_offset',
		'unresolved_continuation_offset',
		'start_offset',
		'frontier_offset'
	  )
)
SELECT path, min(required_start), max(projected_size)
FROM needed
WHERE trim(COALESCE(path, '')) <> ''
GROUP BY path
ORDER BY path;
SQL
	then
		echo "unable to read history-watch projection rows from the scope SQLite snapshot" >&2
		return 1
	fi
	if [[ ! -s "$history_inventory" ]]; then
		echo "history-watch projection contains no source files" >&2
		return 1
	fi
	while IFS=$'\t' read -r source_path checkpoint_offset projected_size; do
		[[ -z "$source_path" ]] && continue
		if ! canonical_source="$(realpath -e -- "$source_path")"; then
			echo "history source disappeared while assembling fixture: $source_path" >&2
			return 1
		fi
		if [[ "$canonical_source" != "$sessions_root"/* ]]; then
			echo "history source escaped the validated sessions tree: $source_path" >&2
			return 1
		fi
		relative="${canonical_source#"$sessions_root/"}"
		destination="$candidate/codex/sessions/$relative"
		if ! copy_sparse_history_file "$canonical_source" "$destination" "$checkpoint_offset" "$projected_size"; then return 1; fi
	done < "$history_inventory"
	rm -f -- "$history_inventory"
}

copy_recent_history_files() {
	local candidate="$1"
	local sessions_root recent_inventory recent_minutes source_path canonical_source relative destination session_size
	sessions_root="$(realpath -e -- "$codex_home/sessions")"
	recent_minutes=$((recent_session_hours * 60))
	recent_inventory="$candidate/.recent-session-selection"
	if ! find "$sessions_root" -type f -name '*.jsonl' -mmin "-$recent_minutes" -print0 > "$recent_inventory"; then
		return 1
	fi
	while IFS= read -r -d '' source_path; do
		if ! canonical_source="$(realpath -e -- "$source_path")"; then
			echo "recent session disappeared while assembling fixture: $source_path" >&2
			return 1
		fi
		if [[ "$canonical_source" != "$sessions_root"/* ]]; then
			echo "recent session escaped the validated sessions tree: $source_path" >&2
			return 1
		fi
		relative="${canonical_source#"$sessions_root/"}"
		destination="$candidate/codex/sessions/$relative"
		session_size="$(stat -c '%s' -- "$canonical_source")"
		if [[ ! "$session_size" =~ ^[0-9]+$ ]]; then
			echo "unable to determine recent session size: $canonical_source" >&2
			return 1
		fi
		if ! require_fixture_free_space "$session_size" "recent session $canonical_source"; then
			return 1
		fi
		mkdir -p "$(dirname "$destination")"
		if ! cp -p --no-dereference --reflink=auto -- "$canonical_source" "$destination"; then return 1; fi
		if [[ -L "$destination" || ! -f "$destination" ]]; then
			echo "refusing copied recent session that is not a regular file: $destination" >&2
			return 1
		fi
	done < "$recent_inventory"
	rm -f -- "$recent_inventory"
}

# Build an immutable content-only witness for every bounded transcript proof
# that the copied Store may rebind. Production fingerprints intentionally bind
# an inode and pathname, which cannot survive copying into a disposable
# container. The manifest carries only the exact byte ranges needed by those
# proofs; it never grants access to the live source after assembly and never
# weakens the production fingerprint or source-containment checks.
write_source_proof_manifest() {
	local candidate="$1"
	local snapshot_store="$2"
	local sessions_root inventory source_path start end canonical_source relative logical_path source_size length digest
	sessions_root="$(realpath -e -- "$codex_home/sessions")"
	inventory="$candidate/.source-proof-ranges.tsv"
	: > "$candidate/source-proof-manifest.tsv" || return 1
	printf '# cxp-teams-source-proof-v1\n' >> "$candidate/source-proof-manifest.tsv" || return 1
	if ! sqlite3 -separator $'\t' "file:$snapshot_store?mode=ro" > "$inventory" <<'SQL'
WITH history_rows AS (
  SELECT json_extract(j.value, '$.path') AS root_path, j.value AS object
  FROM state_meta AS s,
       json_each(CASE WHEN json_valid(s.value) THEN json_extract(s.value, '$.history_watch') ELSE '{}' END) AS j
  WHERE s.key = 'history_watch_projection'
),
import_rows AS (
  SELECT json AS object
  FROM import_checkpoints
  WHERE json_valid(json)
),
ranges(path, start_offset, end_offset) AS (
  SELECT root_path,
         max(0, CAST(json_extract(object, '$.offset') AS INTEGER) - 8192),
         CAST(json_extract(object, '$.offset') AS INTEGER)
  FROM history_rows
  WHERE trim(COALESCE(root_path, '')) <> ''
    AND json_type(object, '$.offset') IN ('integer', 'real')
    AND CAST(json_extract(object, '$.offset') AS INTEGER) >= 0
  UNION ALL
  SELECT COALESCE(json_extract(object, '$.pending_history_range.source_path'), root_path),
         CAST(json_extract(object, '$.pending_history_range.start_offset') AS INTEGER),
         CAST(json_extract(object, '$.pending_history_range.exclusive_end_offset') AS INTEGER)
  FROM history_rows
  WHERE json_type(object, '$.pending_history_range.start_offset') IN ('integer', 'real')
    AND json_type(object, '$.pending_history_range.exclusive_end_offset') IN ('integer', 'real')
  UNION ALL
  SELECT root_path,
         CAST(json_extract(object, '$.context_gap.start_offset') AS INTEGER),
         CAST(json_extract(object, '$.context_gap.exclusive_end_offset') AS INTEGER)
  FROM history_rows
  WHERE json_type(object, '$.context_gap.start_offset') IN ('integer', 'real')
    AND json_type(object, '$.context_gap.exclusive_end_offset') IN ('integer', 'real')
  UNION ALL
  SELECT COALESCE(json_extract(object, '$.transcript_quarantine.source_path'), root_path),
         CAST(json_extract(object, '$.transcript_quarantine.frontier_offset') AS INTEGER),
         CAST(json_extract(object, '$.transcript_quarantine.exclusive_end_offset') AS INTEGER)
  FROM history_rows
  WHERE json_type(object, '$.transcript_quarantine.frontier_offset') IN ('integer', 'real')
    AND json_type(object, '$.transcript_quarantine.exclusive_end_offset') IN ('integer', 'real')
  UNION ALL
  SELECT root_path,
         CAST(json_extract(object, '$.terminal_boundary.start_offset') AS INTEGER),
         CAST(json_extract(object, '$.terminal_boundary.exclusive_end_offset') AS INTEGER)
  FROM history_rows
  WHERE json_type(object, '$.terminal_boundary.start_offset') IN ('integer', 'real')
    AND json_type(object, '$.terminal_boundary.exclusive_end_offset') IN ('integer', 'real')
  UNION ALL
  SELECT json_extract(object, '$.source_path'),
         max(0, CAST(json_extract(object, '$.last_offset') AS INTEGER) - 8192),
         CAST(json_extract(object, '$.last_offset') AS INTEGER)
  FROM import_rows
  WHERE trim(COALESCE(json_extract(object, '$.source_path'), '')) <> ''
    AND json_type(object, '$.last_offset') IN ('integer', 'real')
    AND CAST(json_extract(object, '$.last_offset') AS INTEGER) >= 0
  UNION ALL
  SELECT COALESCE(json_extract(object, '$.pending_history_range.source_path'), json_extract(object, '$.source_path')),
         CAST(json_extract(object, '$.pending_history_range.start_offset') AS INTEGER),
         CAST(json_extract(object, '$.pending_history_range.exclusive_end_offset') AS INTEGER)
  FROM import_rows
  WHERE json_type(object, '$.pending_history_range.start_offset') IN ('integer', 'real')
    AND json_type(object, '$.pending_history_range.exclusive_end_offset') IN ('integer', 'real')
  UNION ALL
  SELECT json_extract(object, '$.source_path'),
         CAST(json_extract(object, '$.context_gap.start_offset') AS INTEGER),
         CAST(json_extract(object, '$.context_gap.exclusive_end_offset') AS INTEGER)
  FROM import_rows
  WHERE json_type(object, '$.context_gap.start_offset') IN ('integer', 'real')
    AND json_type(object, '$.context_gap.exclusive_end_offset') IN ('integer', 'real')
  UNION ALL
  SELECT COALESCE(json_extract(object, '$.transcript_quarantine.source_path'), json_extract(object, '$.source_path')),
         CAST(json_extract(object, '$.transcript_quarantine.frontier_offset') AS INTEGER),
         CAST(json_extract(object, '$.transcript_quarantine.exclusive_end_offset') AS INTEGER)
  FROM import_rows
  WHERE json_type(object, '$.transcript_quarantine.frontier_offset') IN ('integer', 'real')
    AND json_type(object, '$.transcript_quarantine.exclusive_end_offset') IN ('integer', 'real')
  UNION ALL
  SELECT json_extract(object, '$.source_path'),
         CAST(json_extract(object, '$.terminal_boundary.start_offset') AS INTEGER),
         CAST(json_extract(object, '$.terminal_boundary.exclusive_end_offset') AS INTEGER)
  FROM import_rows
  WHERE json_type(object, '$.terminal_boundary.start_offset') IN ('integer', 'real')
    AND json_type(object, '$.terminal_boundary.exclusive_end_offset') IN ('integer', 'real')
  UNION ALL
  SELECT json_extract(object, '$.unresolved_execution.source_path'),
         max(0, CAST(json_extract(object, '$.unresolved_execution.cutoff_offset') AS INTEGER) - 8192),
         CAST(json_extract(object, '$.unresolved_execution.cutoff_offset') AS INTEGER)
  FROM import_rows
  WHERE trim(COALESCE(json_extract(object, '$.unresolved_execution.source_path'), '')) <> ''
    AND json_type(object, '$.unresolved_execution.cutoff_offset') IN ('integer', 'real')
    AND CAST(json_extract(object, '$.unresolved_execution.cutoff_offset') AS INTEGER) >= 0
  UNION ALL
  SELECT json_extract(json, '$.transcript_source_path'),
         max(0, CAST(json_extract(json, '$.transcript_source_proof_offset') AS INTEGER) - 8192),
         CAST(json_extract(json, '$.transcript_source_proof_offset') AS INTEGER)
  FROM outbox_messages
  WHERE json_valid(json)
    AND trim(COALESCE(json_extract(json, '$.transcript_source_path'), '')) <> ''
    AND json_type(json, '$.transcript_source_proof_offset') IN ('integer', 'real')
    AND CAST(json_extract(json, '$.transcript_source_proof_offset') AS INTEGER) >= 0
  UNION ALL
  SELECT json_extract(json, '$.transcript_source_path'),
         CAST(json_extract(json, '$.transcript_source_read_proof_start_offset') AS INTEGER),
         CAST(json_extract(json, '$.transcript_source_read_proof_end_offset') AS INTEGER)
  FROM outbox_messages
  WHERE json_valid(json)
    AND trim(COALESCE(json_extract(json, '$.transcript_source_path'), '')) <> ''
    AND json_type(json, '$.transcript_source_read_proof_start_offset') IN ('integer', 'real')
    AND json_type(json, '$.transcript_source_read_proof_end_offset') IN ('integer', 'real')
)
SELECT path, start_offset, end_offset
FROM ranges
WHERE trim(COALESCE(path, '')) <> ''
  AND start_offset >= 0
  AND end_offset >= start_offset
ORDER BY path, start_offset, end_offset;
SQL
	then
		echo "unable to derive copied transcript source-proof ranges" >&2
		return 1
	fi
	while IFS=$'\t' read -r source_path start end; do
		[[ -z "$source_path" ]] && continue
		if ! [[ "$start" =~ ^[0-9]+$ && "$end" =~ ^[0-9]+$ && "$end" -ge "$start" ]]; then
			echo "invalid transcript source-proof range: path=$source_path start=$start end=$end" >&2
			return 1
		fi
		if ! canonical_source="$(realpath -e -- "$source_path")"; then
			echo "transcript source disappeared while building proof manifest: $source_path" >&2
			return 1
		fi
		if [[ "$canonical_source" != "$sessions_root"/* ]]; then
			echo "transcript source proof escaped the sessions tree: $source_path" >&2
			return 1
		fi
		source_size="$(stat -c '%s' -- "$canonical_source")"
		if [[ "$end" -gt "$source_size" ]]; then
			echo "transcript source proof exceeds source size: path=$source_path end=$end size=$source_size" >&2
			return 1
		fi
		length=$((end - start))
		digest="$(dd if="$canonical_source" iflag=skip_bytes,count_bytes skip="$start" count="$length" bs=1M status=none | sha256sum)"
		digest="${digest%% *}"
		if [[ ! "$digest" =~ ^[0-9a-f]{64}$ ]]; then
			echo "unable to hash transcript source proof range: path=$source_path start=$start end=$end" >&2
			return 1
		fi
		relative="${canonical_source#"$sessions_root/"}"
		logical_path="${fixture_codex_dir%/}/sessions/$relative"
		printf '%s\t%s\t%s\t%s\n' "${logical_path}" "$start" "$end" "$digest" >> "$candidate/source-proof-manifest.tsv" || return 1
	done < "$inventory"
	rm -f -- "$inventory"
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
	if ! sqlite_backup_readonly "$teams_root/global-inbound-ledger.sqlite" "$candidate/teams/global-inbound-ledger.sqlite"; then return 1; fi
	if ! sqlite_backup_readonly "$teams_root/global-outbound-ledger.sqlite" "$candidate/teams/global-outbound-ledger.sqlite"; then return 1; fi
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
	# Build the scope SQLite backup before selecting history paths. This keeps
	# the history-watch projection and the copied database at one SQLite commit
	# boundary; the final source manifest still rejects cross-file drift while
	# Codex files are being copied.
	if ! sqlite_backup_readonly "$scope_dir/store.sqlite" "$candidate/teams/store.sqlite"; then return 1; fi
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
	if ! copy_referenced_history_files "$candidate" "$candidate/teams/store.sqlite"; then return 1; fi
	if ! copy_recent_history_files "$candidate"; then return 1; fi
	if ! write_source_proof_manifest "$candidate" "$candidate/teams/store.sqlite"; then return 1; fi
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

	if ! write_source_core_manifest "$source_core_manifest_after"; then return 1; fi
	if ! cmp -s "$source_core_manifest_before" "$source_core_manifest_after"; then
		echo "source inputs changed while the point-in-time fixture was being assembled; this attempt is not cross-file stable" >&2
		diff -u "$source_core_manifest_before" "$source_core_manifest_after" >&2 || true
		echo "SQLite .backup snapshots include committed WAL content; source SQLite -wal/-shm drift invalidated this cross-file snapshot attempt" >&2
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

# Run as the invoking host UID/GID so the disposable private fixture and the
# private runtime bind mount remain readable only by that user. In particular,
# do not chmod a real-data snapshot world-readable just to accommodate a root
# container: the fixture contains copied local history and must keep its normal
# 0700/0600 boundary.
chmod u+rwx,go-rwx "$fixture_root" "$fixture_dir" "$runtime_dir"

echo "real-data snapshot: scope=$scope_dir codex_sessions=$codex_home/sessions"
sqlite3 "file:$fixture_dir/teams/store.sqlite?mode=ro" "SELECT 'rows', (SELECT count(*) FROM sessions),(SELECT count(*) FROM inbound_events),(SELECT count(*) FROM turns),(SELECT count(*) FROM chat_polls); SELECT 'queued_inbound',count(*) FROM inbound_events WHERE status='queued'; SELECT 'frontiers',sum(CASE WHEN json_extract(json,'\$.continuation_path') IS NOT NULL AND json_extract(json,'\$.continuation_path')<>'' THEN 1 ELSE 0 END),sum(CASE WHEN json_extract(json,'\$.gap') IS NOT NULL THEN 1 ELSE 0 END) FROM chat_polls;"

cd "$repo_root"
CGO_ENABLED=0 go test -c -o "$build_dir/teams-sqlite-history-fixture.test" ./internal/teams
docker_test_selector='^TestDockerRealDataTeamsProgressThroughput$'
docker_listed_tests="$($build_dir/teams-sqlite-history-fixture.test -test.list "$docker_test_selector")"
if ! grep -Fxq -- 'TestDockerRealDataTeamsProgressThroughput' <<<"$docker_listed_tests"; then
	echo "Docker real-data selector did not list TestDockerRealDataTeamsProgressThroughput" >&2
	exit 1
fi
mkdir -p "$build_dir/docker-mountpoints/sessions"
touch -- "$build_dir/docker-mountpoints/sessions/.keep"
docker build \
	--file scripts/ci/Dockerfile.teams-real-data-experiment \
	--tag "$image" \
	"$build_dir" >/dev/null

run_experiment_process() {
	local resume="$1"
	local reuse_runtime="$2"
	timeout --foreground --kill-after=30s "$docker_watchdog_timeout" docker run --rm --stop-timeout 30 \
		--network none \
		--user "$host_uid:$host_gid" \
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
		--env CXP_TEAMS_DOCKER_PROCESS_RESTART="$process_restart" \
		--env CXP_TEAMS_DOCKER_REAL_DATA_MODE="$experiment_mode" \
		--env CXP_TEAMS_DOCKER_REAL_DATA_DURATION="$experiment_duration" \
		--env CXP_TEAMS_DOCKER_STARTUP_DEADLINE="${CXP_TEAMS_DOCKER_STARTUP_DEADLINE:-}" \
		--env CXP_TEAMS_DOCKER_REAL_DATA_429="$rate_limit_experiment" \
		--env CXP_TEAMS_DOCKER_REAL_DATA_429_SCOPE="$rate_limit_scope" \
		--env CXP_TEAMS_DOCKER_REAL_DATA_429_POLL_ONLY="${CXP_TEAMS_DOCKER_REAL_DATA_429_POLL_ONLY:-0}" \
		--env CXP_TEAMS_DOCKER_REAL_DATA_POLL_INTERVAL="$poll_interval" \
		--env CXP_TEAMS_DOCKER_CODEX_SOURCE_PREFIX="${codex_home%/}/" \
		--env CXP_TEAMS_DOCKER_CODEX_MOUNTED=1 \
		--env CXP_TEAMS_DOCKER_SOURCE_PROOF_MANIFEST=/fixture/source-proof-manifest.tsv \
		--mount "type=bind,src=$fixture_dir,dst=/fixture,readonly" \
		--mount "type=bind,src=$fixture_dir/codex,dst=/home/baka/.codex,readonly" \
		--mount "type=bind,src=$runtime_dir,dst=/runtime" \
		"$image" \
		/teams-sqlite-history-fixture.test \
		-test.run "$docker_test_selector" \
		-test.count=1 \
		-test.timeout "$docker_test_timeout" \
		-test.v
}

set +e
run_experiment_process 0 0
first_process_status=$?
run_status=$first_process_status

# A restart-mode acceptance run is a two-process protocol.  Always launch the
# resume process after the first process exits, including when the first
# process failed: otherwise a failure before the durable witness is written
# can be mistaken for a missing restart check, and a broken first boundary is
# never exercised by the recovery process. Preserve the first failure while
# still reporting the second process's diagnostics.
if [[ "$process_restart" == "1" ]]; then
	echo "real-data Docker experiment: starting a second container process against the same disposable runtime" >&2
	run_experiment_process 1 1
	second_process_status=$?
	if [[ "$run_status" -eq 0 ]]; then
		run_status=$second_process_status
	fi
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
