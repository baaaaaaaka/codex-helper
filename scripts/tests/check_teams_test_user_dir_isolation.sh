#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
probe_root="$(mktemp -d)"
trap 'rm -rf "$probe_root"' EXIT

mkdir -p \
  "$probe_root/home" \
  "$probe_root/config" \
  "$probe_root/state" \
  "$probe_root/cache" \
  "$probe_root/data" \
  "$probe_root/appdata" \
  "$probe_root/localappdata" \
  "$probe_root/codex-home" \
  "$probe_root/codex-config"
go_mod_cache="$(go env GOMODCACHE)"
go_build_cache="$(go env GOCACHE)"
service_config="$probe_root/config/codex-helper/teams/local-supervisor.json"
mkdir -p "$(dirname "$service_config")"
mkdir -p "$probe_root/state/codex-helper"
printf '%s\n' \
  '{"version":1,"enabled":true,"spec":{"Executable":"/opt/cxp/bin/cxp","WorkingDir":"/opt/cxp"}}' \
  >"$service_config"
for root in \
  "$probe_root/state" \
  "$probe_root/cache" \
  "$probe_root/data" \
  "$probe_root/appdata" \
  "$probe_root/localappdata" \
  "$probe_root/codex-home" \
  "$probe_root/codex-config"
do
  mkdir -p "$root/codex-helper"
  printf 'caller-owned sentinel\n' >"$root/codex-helper/sentinel.txt"
done

snapshot_probe() {
  find \
    "$probe_root/config" \
    "$probe_root/state" \
    "$probe_root/cache" \
    "$probe_root/data" \
    "$probe_root/appdata" \
    "$probe_root/localappdata" \
    "$probe_root/codex-home" \
    "$probe_root/codex-config" \
    -path "$probe_root/config/go" -prune -o \
    -printf '%y %m %p\n' |
    LC_ALL=C sort
  find \
    "$probe_root/config" \
    "$probe_root/state" \
    "$probe_root/cache" \
    "$probe_root/data" \
    "$probe_root/appdata" \
    "$probe_root/localappdata" \
    "$probe_root/codex-home" \
    "$probe_root/codex-config" \
    -path "$probe_root/config/go" -prune -o \
    -type f -print0 |
    LC_ALL=C sort -z |
    xargs -0 sha256sum
}

before_snapshot="$(snapshot_probe)"

test_status=0
(
  cd "$repo_root"
  HOME="$probe_root/home" \
  USERPROFILE="$probe_root/home" \
  APPDATA="$probe_root/appdata" \
  LOCALAPPDATA="$probe_root/localappdata" \
  XDG_CONFIG_HOME="$probe_root/config" \
  XDG_STATE_HOME="$probe_root/state" \
  XDG_CACHE_HOME="$probe_root/cache" \
  XDG_DATA_HOME="$probe_root/data" \
  CODEX_HOME="$probe_root/codex-home" \
  CODEX_DIR="$probe_root/codex-home" \
  CODEX_CONFIG_DIR="$probe_root/codex-config" \
  GOMODCACHE="$go_mod_cache" \
  GOCACHE="$go_build_cache" \
  GOTELEMETRY=off \
  go test ./internal/cli -count=1 -run '^TestTeamsService'
) || test_status=$?

after_snapshot="$(snapshot_probe)"
if [[ "$before_snapshot" != "$after_snapshot" ]]; then
  echo "Teams CLI tests escaped package isolation and wrote to caller-owned config/state directories:" >&2
  diff -u <(printf '%s\n' "$before_snapshot") <(printf '%s\n' "$after_snapshot") >&2 || true
  exit 1
fi
if [[ "$test_status" -ne 0 ]]; then
  echo "Teams service/bootstrap tests did not complete cleanly under caller-owned config/state sentinels." >&2
  exit "$test_status"
fi
