#!/usr/bin/env bash
set -euo pipefail

# This is a deliberately small mutation gate, not a general-purpose mutation
# framework.  It copies the current checkout (including uncommitted files) to
# an explicit temporary directory, applies one exact source mutation at a
# time, and requires the corresponding semantic regression test to fail.  The
# live worktree, Teams state, credentials, and Graph are never touched.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/cxp-teams-mutation.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

base="$tmp_dir/base"
mkdir -p "$base"
rsync -a --exclude '.git' --exclude '.codex' --exclude '.agents' --exclude 'dist' --exclude 'bin' \
	"$repo_root/" "$base/"

run_mutation() {
	local name="$1"
	local package="$2"
	local test_name="$3"
	local mutant_dir="$tmp_dir/$name"
	local output="$tmp_dir/$name.log"

	rsync -a --delete "$base/" "$mutant_dir/"
	python3 - "$mutant_dir" "$name" <<'PY'
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
name = sys.argv[2]
mutations = {
    "owner-cas": (
        "internal/teams/store/chat_poll_frontier.go",
        "if !storeOwnerCapabilityMatchesActiveLease(state, capabilityOwner(capability), capabilityLeaseGeneration(capability)) {",
        "if false { // mutation: stale owner is incorrectly admitted",
    ),
    "continuation-budget": (
        "internal/teams/poll_frontier.go",
        "if poll.ContinuationFailureCount >= continuationFailureBudget {",
        "if poll.ContinuationFailureCount >= 0 { // mutation: quarantine on the first failure",
    ),
    "chat-error-global": (
        "internal/teams/bridge.go",
        '''\t\tif err := runPhase("linked-transcript", func(phaseCtx context.Context) error {
\t\t\treturn b.syncLinkedTranscriptsIfDue(phaseCtx, time.Now())
\t\t}); err != nil && b.out != nil {
\t\t\t_, _ = fmt.Fprintf(b.out, "Teams transcript sync error: %v\\n", err)
\t\t}''',
        '''\t\tif err := runPhase("linked-transcript", func(phaseCtx context.Context) error {
\t\t\treturn b.syncLinkedTranscriptsIfDue(phaseCtx, time.Now())
\t\t}); err != nil {
\t\t\treturn err // mutation: a chat-local error stops the listener
\t\t}''',
    ),
}

relative, old, new = mutations[name]
path = root / relative
source = path.read_text(encoding="utf-8")
count = source.count(old)
if count != 1:
    raise SystemExit(f"{name}: expected one mutation target in {path}, found {count}")
path.write_text(source.replace(old, new), encoding="utf-8")
PY

	set +e
	(
		cd "$mutant_dir" &&
		go test "$package" -count=1 -run "^${test_name}$" -timeout=90s -v
	) >"$output" 2>&1
	local status=$?
	set -e
	if [ "$status" -eq 0 ]; then
		echo "mutation $name survived: $package/$test_name" >&2
		cat "$output" >&2
		return 1
	fi
	if ! grep -Eq -- "--- FAIL: ${test_name}([[:space:](/]|$)" "$output"; then
		echo "mutation $name failed for a non-semantic reason; expected the target test to fail" >&2
		cat "$output" >&2
		return 1
	fi
	echo "mutation $name killed by $package/$test_name"
}

run_baseline() {
	local package="$1"
	local test_name="$2"
	local output="$tmp_dir/baseline-${test_name}.log"

	if ! (
		cd "$base" &&
		go test "$package" -count=1 -run "^${test_name}$" -timeout=90s -v
	) >"$output" 2>&1; then
		echo "unmutated baseline failed: $package/$test_name" >&2
		cat "$output" >&2
		return 1
	fi
	if ! grep -Eq -- "--- PASS: ${test_name}([[:space:](/]|$)" "$output"; then
		echo "unmutated baseline did not execute the required test: $package/$test_name" >&2
		cat "$output" >&2
		return 1
	fi
	echo "unmutated baseline passed: $package/$test_name"
}

run_baseline ./internal/teams/store TestOwnerBoundChatPollMutationRejectsStaleOwnerAcrossBackends
run_baseline ./internal/teams TestPollFrontierGenericContinuationFailureUsesBoundedGap
run_baseline ./internal/teams TestTeamsListenFalseLinkedTranscriptSessionErrorDoesNotStarveHealthyTail

run_mutation owner-cas ./internal/teams/store TestOwnerBoundChatPollMutationRejectsStaleOwnerAcrossBackends
run_mutation continuation-budget ./internal/teams TestPollFrontierGenericContinuationFailureUsesBoundedGap
run_mutation chat-error-global ./internal/teams TestTeamsListenFalseLinkedTranscriptSessionErrorDoesNotStarveHealthyTail
