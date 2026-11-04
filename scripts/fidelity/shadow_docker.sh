#!/usr/bin/env bash
# Real-data shadow run (TestShadowRealData) in offline Docker.
#
#   shadow_docker.sh <src-dir> <scope-dir> <seed.jsonl.gz> <out-dir> <mode> [idle]
#
# The old scope is mounted read-only (the scenario copies it into tmpfs),
# the owner's Codex sessions read-only under a tmpfs Codex home, and only
# <out-dir> is writable. No network. SHADOW_SCOPE_NAME names the scope
# directory as the old helper had it (default: the basename of <scope-dir>).
set -euo pipefail
src=$(readlink -f "$1"); scope=$(readlink -f "$2"); seed=$(readlink -f "$3"); out=$(readlink -f "$4"); mode=$5; idle=${6:-10m}
sessions=${SHADOW_SESSIONS:-$HOME/.codex/sessions}
codex_home=$(dirname "$sessions")
gomod=${GOMODCACHE_HOST:-$HOME/go/pkg/mod}
gocache=${GOCACHE_HOST:?set GOCACHE_HOST to a writable Go build cache}
mkdir -p "$out"
scope_name=${SHADOW_SCOPE_NAME:-$(basename "$scope")}
name="cxp-shadow-$$-$RANDOM"
start=$(date +%s.%N)
set +e
docker run --rm --name "$name" --network=none --user "$(id -u):$(id -g)" \
  --cpus=24 --memory=40g --pids-limit=8192 \
  -v "$src":/src:ro -v "$gomod":/gomod:ro -v "$gocache":/gocache \
  -v "$scope":/real/"$scope_name":ro -v "$seed":/seed/seed.jsonl.gz:ro -v "$out":/out \
  --tmpfs "$codex_home":rw,size=256m,uid="$(id -u)",gid="$(id -g)",mode=0700 -v "$sessions":"$sessions":ro \
  --tmpfs /tmp:rw,exec,size=24g --tmpfs /home/t:rw,size=2g \
  -e HOME=/home/t -e GOMODCACHE=/gomod -e GOCACHE=/gocache -e GOFLAGS=-mod=readonly \
  -e GOPROXY=off -e GOTOOLCHAIN=local -e GOTELEMETRY=off -e CXP_RUNTIME_DISABLE=1 -e TMPDIR=/tmp \
  -e E2E_SUT=cxp -e E2E_SHADOW_STORE=/real/"$scope_name" -e E2E_SHADOW_SEED=/seed/seed.jsonl.gz \
  -e E2E_SHADOW_CODEX_HOME="$codex_home" -e E2E_SHADOW_OUT=/out -e E2E_SHADOW_MODE="$mode" -e E2E_SHADOW_IDLE="$idle" \
  -e E2E_SHADOW_KILL_AFTER="${E2E_SHADOW_KILL_AFTER:-800}" \
  -w /src golang:1.25.11-bookworm go test -count=1 -json -timeout 5h -run '^TestShadowRealData$' ./internal/e2e/scenarios/entry/ > "$out/test.json" 2> "$out/test.err"
rc=$?
set -e
end=$(date +%s.%N)
echo "rc=$rc wall=$(echo "$end - $start" | bc)s" | tee "$out/rc"
exit $rc
