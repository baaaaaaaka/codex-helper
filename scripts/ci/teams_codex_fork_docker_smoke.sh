#!/usr/bin/env bash
set -euo pipefail

# This harness deliberately uses only deterministic in-process fakes. It does
# not copy Codex or Teams credentials into the image and the containers have
# no network, capabilities, or writable filesystem beyond an ephemeral /tmp.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
image="cxp-teams-codex-fork-docker-smoke:${GITHUB_RUN_ID:-local}-$$"
trap 'docker image rm --force "$image" >/dev/null 2>&1 || true; rm -rf "$tmp_dir"' EXIT

command -v docker >/dev/null 2>&1 || {
	echo "docker is required for the Teams Codex fork Docker smoke test" >&2
	exit 1
}
docker info >/dev/null 2>&1 || {
	echo "docker daemon is unavailable for the Teams Codex fork Docker smoke test" >&2
	exit 1
}

cd "$repo_root"
CGO_ENABLED=0 go test -c -o "$tmp_dir/teams-fork.test" ./internal/teams
CGO_ENABLED=0 go test -c -o "$tmp_dir/store-fork.test" ./internal/teams/store
CGO_ENABLED=0 go test -c -o "$tmp_dir/codexrunner-fork.test" ./internal/codexrunner
CGO_ENABLED=0 go test -c -o "$tmp_dir/cli-fork.test" ./internal/cli

docker build \
	--file scripts/ci/Dockerfile.teams-codex-fork \
	--tag "$image" \
	"$tmp_dir" >/dev/null

run_test() {
	local binary="$1"
	local pattern="$2"
	docker run --rm \
		--network none \
		--cap-drop ALL \
		--security-opt no-new-privileges \
		--pids-limit 128 \
		--read-only \
		--tmpfs /tmp:rw,nosuid,nodev,size=128m \
		"$image" "/$binary" \
		-test.run "$pattern" \
		-test.count=1 \
		-test.v
}

run_test teams-fork.test '^Test(Fork(WorkSessionDeterministicGraphEndToEnd|ActivatedRecoverySendsParentLinkAfterRestart|WorkCommandMutatesParentGate|ParentAndCutoffLoadsDurableSessionAndTurn|ReconcileAmbiguousNativeResponseAdoptsOnlyUniqueChild)|BridgeFlushRecoversAcceptedOutboxFromGraphAfterRestart)$'
run_test store-fork.test '^Test(OwnedForkMutationStopsAfterLeaseTakeoverAndActivatedStaysFenced|ForkParentFenceBlocksNewInputsAndClaims|ForkHistoryRequiresSentProofBeforeActivation)$'
run_test codexrunner-fork.test '^TestAppServerRunnerForkThread'
run_test cli-fork.test '^TestTeamsCodexExecutorReconcileForkThread'
