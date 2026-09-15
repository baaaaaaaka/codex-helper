#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

: "${CODEX_HELPER_TEAMS_GRAPH_429_STRESS_CHATS:=32}"
: "${CODEX_HELPER_TEAMS_GRAPH_429_STRESS_MESSAGES:=6}"
: "${CODEX_HELPER_TEAMS_GRAPH_429_STRESS_ROUNDS:=8}"
: "${CODEX_HELPER_TEAMS_GRAPH_429_STRESS_COUNT:=1}"
: "${CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_CHATS:=12}"
: "${CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_MESSAGES:=3}"
: "${CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_ROUNDS:=3}"

validate_positive_int() {
  local name="$1"
  local value="$2"
  local minimum="$3"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]] || (( value < minimum )); then
    echo "${name} must be an integer >= ${minimum}; got ${value@Q}" >&2
    exit 2
  fi
}

# A zero count makes `go test` do no work and still exit zero.  A one-chat
# fixture also cannot prove that an account/global gate preserves a healthy
# sibling. Reject both configurations before any test command starts.
validate_positive_int CODEX_HELPER_TEAMS_GRAPH_429_STRESS_CHATS "$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_CHATS" 2
validate_positive_int CODEX_HELPER_TEAMS_GRAPH_429_STRESS_MESSAGES "$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_MESSAGES" 1
validate_positive_int CODEX_HELPER_TEAMS_GRAPH_429_STRESS_ROUNDS "$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_ROUNDS" 1
validate_positive_int CODEX_HELPER_TEAMS_GRAPH_429_STRESS_COUNT "$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_COUNT" 1
validate_positive_int CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_CHATS "$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_CHATS" 2
validate_positive_int CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_MESSAGES "$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_MESSAGES" 1
validate_positive_int CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_ROUNDS "$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_ROUNDS" 1

echo "Teams Graph 429 stress scale: chats=${CODEX_HELPER_TEAMS_GRAPH_429_STRESS_CHATS} messages=${CODEX_HELPER_TEAMS_GRAPH_429_STRESS_MESSAGES} rounds=${CODEX_HELPER_TEAMS_GRAPH_429_STRESS_ROUNDS} count=${CODEX_HELPER_TEAMS_GRAPH_429_STRESS_COUNT}"

stress_selector='TestTeamsGraph429(Stress(Outbox|Poll)MaintainsAvailabilityAndSuppressesLoopsCI|PollAutomaticallyRecoversWithoutManualUnblock|SQLiteAdmissionDoesNotLoseTheRetry)$'
stress_tests=(
  TestTeamsGraph429StressOutboxMaintainsAvailabilityAndSuppressesLoopsCI
  TestTeamsGraph429StressPollMaintainsAvailabilityAndSuppressesLoopsCI
  TestTeamsGraph429PollAutomaticallyRecoversWithoutManualUnblock
  TestTeamsGraph429SQLiteAdmissionDoesNotLoseTheRetry
)

# `go test` exits successfully when -run matches no tests. Verify the exact
# top-level names before the stress loop so a rename or regex drift cannot make
# this CI job false-green.
listed_tests="$(go test -race ./internal/teams -list "$stress_selector")"
for test_name in "${stress_tests[@]}"; do
  if ! grep -Fxq -- "$test_name" <<<"$listed_tests"; then
    echo "429 stress selector did not list required test: $test_name" >&2
    exit 1
  fi
done

for ((i = 1; i <= CODEX_HELPER_TEAMS_GRAPH_429_STRESS_COUNT; i++)); do
  # Race instrumentation is valuable for the ownership/429 invariants, but
  # the 32-chat x 8-round fixture makes SQLite's deliberately serialized
  # durable tail take several minutes and can time out without finding a race.
  # Keep that proof at a bounded medium scale, then run the requested larger
  # availability load without -race. The listener account/global matrix is
  # exercised separately by the recovery manifest, so it does not lengthen
  # this high-scale stress lane.
  CODEX_HELPER_TEAMS_GRAPH_429_STRESS_CHATS="$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_CHATS" \
  CODEX_HELPER_TEAMS_GRAPH_429_STRESS_MESSAGES="$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_MESSAGES" \
  CODEX_HELPER_TEAMS_GRAPH_429_STRESS_ROUNDS="$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_ROUNDS" \
    go test -race ./internal/teams -count=1 -run "$stress_selector" -v
  CODEX_HELPER_TEAMS_GRAPH_429_STRESS=1 \
  CODEX_HELPER_TEAMS_GRAPH_429_STRESS_CHATS="$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_CHATS" \
  CODEX_HELPER_TEAMS_GRAPH_429_STRESS_MESSAGES="$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_MESSAGES" \
  CODEX_HELPER_TEAMS_GRAPH_429_STRESS_ROUNDS="$CODEX_HELPER_TEAMS_GRAPH_429_STRESS_ROUNDS" \
    go test ./internal/teams -count=1 -run "$stress_selector" -v
done
