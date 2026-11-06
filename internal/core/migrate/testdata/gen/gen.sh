#!/usr/bin/env bash
# Regenerate the migrate fixtures by running the OLD store code.
# Usage: gen.sh <old-worktree> <tag> <out-dir> <seed.json>[,<seed2.json>...]
# Runs offline in Docker: the old worktree is mounted read-only except for the
# one generator file copied in beforehand; only <out-dir> is writable.
set -euo pipefail
old=$(readlink -f "$1"); tag=$2; out=$(readlink -f "$3"); seeds=$4
here=$(cd "$(dirname "$0")" && pwd)
cp "$here/fixture_gen_test.go.txt" "$old/internal/teams/store/zz_cxp_fixture_gen_test.go"
mkdir -p "$out"
cseeds=$(echo "$seeds" | tr ',' '\n' | sed 's#^#/seeds/#' | paste -sd, -)
docker run --rm --network=none --user "$(id -u):$(id -g)" \
  -v "$old":/src:ro -v "$here":/seeds:ro -v "$out":/out -v "${GOMODCACHE:-$HOME/go/pkg/mod}":/gomod:ro \
  --tmpfs /tmp:rw,exec,size=4g --tmpfs /home/t:rw,size=1g -e HOME=/home/t -e GOCACHE=/tmp/gocache \
  -e GOMODCACHE=/gomod -e GOFLAGS=-mod=readonly -e GOPROXY=off -e GOTOOLCHAIN=local -e GOTELEMETRY=off \
  -e CXP_FIXTURE_OUT=/out -e CXP_FIXTURE_TAG="$tag" -e CXP_FIXTURE_SEEDS="$cseeds" -e CXP_FIXTURE_WAL_SEED=/seeds/seed-wal.json \
  -w /src golang:1.25.11-bookworm go test -count=1 -run TestCXPFixtureGen -v ./internal/teams/store/
rm -f "$old/internal/teams/store/zz_cxp_fixture_gen_test.go"
