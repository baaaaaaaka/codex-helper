#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
probe_root="$(mktemp -d)"
trap 'rm -rf "$probe_root"' EXIT

mkdir -p "$probe_root/home" "$probe_root/config" "$probe_root/state" "$probe_root/cache"
go_mod_cache="$(go env GOMODCACHE)"
go_build_cache="$(go env GOCACHE)"
service_config="$probe_root/config/codex-helper/teams/local-supervisor.json"
mkdir -p "$(dirname "$service_config")"
mkdir -p "$probe_root/state/codex-helper"
printf '%s\n' \
  '{"version":1,"enabled":true,"spec":{"Executable":"/opt/cxp/bin/cxp","WorkingDir":"/opt/cxp"}}' \
  >"$service_config"
before_hash="$(sha256sum "$service_config")"

(
  cd "$repo_root"
  HOME="$probe_root/home" \
  USERPROFILE="$probe_root/home" \
  XDG_CONFIG_HOME="$probe_root/config" \
  XDG_STATE_HOME="$probe_root/state" \
  XDG_CACHE_HOME="$probe_root/cache" \
  GOMODCACHE="$go_mod_cache" \
  GOCACHE="$go_build_cache" \
  go test ./internal/cli -count=1 -run '^TestTeamsServiceBootstrapFailsWhenDuplicateProcessCannotBeRetired$'
)

after_hash="$(sha256sum "$service_config")"
unexpected_files="$(
  find "$probe_root/config/codex-helper" "$probe_root/state/codex-helper" -type f \
    ! -path "$service_config" \
    -print
)"
if [[ "$before_hash" != "$after_hash" || -n "$unexpected_files" ]]; then
  echo "Teams CLI tests escaped package isolation and wrote to caller-owned config/state directories:" >&2
  if [[ "$before_hash" != "$after_hash" ]]; then
    echo "modified: $service_config" >&2
  fi
  if [[ -n "$unexpected_files" ]]; then
    printf '%s\n' "$unexpected_files" >&2
  fi
  exit 1
fi
