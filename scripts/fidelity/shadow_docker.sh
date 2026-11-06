#!/usr/bin/env bash
# Real-data shadow run (TestShadowRealData) in offline Docker.
#
#   shadow_docker.sh <src-dir> <scope-dir> <seed.jsonl.gz> <out-dir> <mode> [idle]
#
# The old scope is mounted read-only (the scenario copies it into tmpfs),
# the owner's Codex sessions read-only under a tmpfs Codex home, and only
# <out-dir> is writable. No network. SHADOW_SCOPE_NAME names the scope
# directory as the old helper had it (default: the basename of <scope-dir>).
# E2E_SHADOW_CPUPROFILE=<name> writes the helper's one-minute CPU profiles
# to <out-dir>/<name>.
set -euo pipefail
src=$(readlink -f "$1"); scope=$(readlink -f "$2"); seed=$(readlink -f "$3"); out=$(readlink -f "$4"); mode=$5; idle=${6:-10m}
sessions=${SHADOW_SESSIONS:-$HOME/.codex/sessions}
codex_home=$(dirname "$sessions")
gomod=${GOMODCACHE_HOST:-$HOME/go/pkg/mod}
gocache=${GOCACHE_HOST:?set GOCACHE_HOST to a writable Go build cache}
mkdir -p "$out"
scope_name=${SHADOW_SCOPE_NAME:-$(basename "$scope")}
# The sessions tree, read-only. In live mode the test writes one rollout of
# its own, so sessions/ is then part of the tmpfs Codex home and the real
# tree is mounted read-only below it: each month directory, each day of the
# current UTC month, and each rollout of today's UTC day directory (which
# stays writable). Nothing else of ~/.codex is mounted.
sess_mounts=(-v "$sessions":"$sessions":ro)
if [ "$mode" = live ]; then
  sess_mounts=()
  month=$(date -u +%Y/%m); today=$(date -u +%Y/%m/%d)
  while IFS= read -r d; do
    rel=${d#"$sessions"/}
    if [ "$rel" = "$month" ]; then
      while IFS= read -r day; do
        if [ "${day#"$sessions"/}" = "$today" ]; then
          while IFS= read -r f; do sess_mounts+=(-v "$f":"$f":ro); done < <(find "$day" -maxdepth 1 -type f)
        else
          sess_mounts+=(-v "$day":"$day":ro)
        fi
      done < <(find "$d" -mindepth 1 -maxdepth 1 -type d | sort)
    else
      sess_mounts+=(-v "$d":"$d":ro)
    fi
  done < <(find "$sessions" -mindepth 2 -maxdepth 2 -type d | sort)
  if find "$sessions" -mindepth 1 -maxdepth 2 -type f | grep -q .; then
    echo "unexpected files above the month directories of $sessions" >&2; exit 2
  fi
fi
uid=$(id -u); gid=$(id -g)
user=(--user "$uid:$gid")
cmd=(go test -count=1 -json -timeout 5h -run '^TestShadowRealData$' ./internal/e2e/scenarios/entry/)
if [ "$mode" = live ]; then
  # Docker creates the mount points' parents in the tmpfs as root: hand
  # those few directories (never the mounts) to the user, then drop root.
  user=(--user 0:0)
  dirs=("$sessions" "$sessions/$(date -u +%Y)" "$sessions/$(date -u +%Y/%m)" "$sessions/$(date -u +%Y/%m/%d)")
  cmd=(sh -c 'for d in "$@"; do [ -d "$d" ] && [ ! -L "$d" ] && chown '"$uid:$gid"' "$d"; done; exec setpriv --reuid='"$uid"' --regid='"$gid"' --clear-groups '"$(printf '%q ' "${cmd[@]}")" sh "${dirs[@]}")
fi
name="cxp-shadow-$$-$RANDOM"
start=$(date +%s.%N)
set +e
docker run --rm --name "$name" --network=none "${user[@]}" \
  --cpus=24 --memory=40g --pids-limit=8192 \
  -v "$src":/src:ro -v "$gomod":/gomod:ro -v "$gocache":/gocache \
  -v "$scope":/real/"$scope_name":ro -v "$seed":/seed/seed.jsonl.gz:ro -v "$out":/out \
  --tmpfs "$codex_home":rw,size=256m,uid="$(id -u)",gid="$(id -g)",mode=0700 "${sess_mounts[@]}" \
  --tmpfs /tmp:rw,exec,size=24g --tmpfs /home/t:rw,size=2g \
  -e HOME=/home/t -e GOMODCACHE=/gomod -e GOCACHE=/gocache -e GOFLAGS=-mod=readonly \
  -e GOPROXY=off -e GOTOOLCHAIN=local -e GOTELEMETRY=off -e CXP_RUNTIME_DISABLE=1 -e TMPDIR=/tmp \
  -e E2E_SUT=cxp -e E2E_SHADOW_STORE=/real/"$scope_name" -e E2E_SHADOW_SEED=/seed/seed.jsonl.gz \
  -e E2E_SHADOW_CODEX_HOME="$codex_home" -e E2E_SHADOW_OUT=/out -e E2E_SHADOW_MODE="$mode" -e E2E_SHADOW_IDLE="$idle" \
  -e E2E_SHADOW_CPUPROFILE="${E2E_SHADOW_CPUPROFILE:+/out/$E2E_SHADOW_CPUPROFILE}" \
  -e E2E_SHADOW_KILL_AFTER="${E2E_SHADOW_KILL_AFTER:-800}" -e E2E_SHADOW_LIVE_AFTER="${E2E_SHADOW_LIVE_AFTER:-1500}" \
  -w /src golang:1.25.11-bookworm "${cmd[@]}" > "$out/test.json" 2> "$out/test.err"
rc=$?
set -e
end=$(date +%s.%N)
echo "rc=$rc wall=$(echo "$end - $start" | bc)s" | tee "$out/rc"
exit $rc
