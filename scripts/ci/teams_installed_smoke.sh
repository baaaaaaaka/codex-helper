#!/usr/bin/env bash
set -euo pipefail

base="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/codex-helper-teams-installed-smoke"
rm -rf "$base"
mkdir -p "$base/bin" "$base/codex-home" "$base/config" "$base/cache" "$base/state"

bin="$base/bin/codex-proxy"
go build -trimpath -o "$bin" ./cmd/codex-proxy

export CODEX_HOME="$base/codex-home"
export XDG_CONFIG_HOME="$base/config"
export XDG_CACHE_HOME="$base/cache"
export XDG_STATE_HOME="$base/state"
export NO_COLOR=1
export CODEX_HELPER_TEAMS_PROFILE=ci-installed-smoke
export CODEX_HELPER_TEAMS_TENANT_ID=ci-tenant
export CODEX_HELPER_TEAMS_CLIENT_ID=ci-client
export CODEX_HELPER_TEAMS_READ_CLIENT_ID=ci-read-client
export CODEX_HELPER_TEAMS_FULL_CLIENT_ID=ci-full-client
export CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID=ci-file-client
export CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND=systemd
export CODEX_HELPER_TEAMS_TOKEN_CACHE="$base/cache/teams-token.json"
export CODEX_HELPER_TEAMS_READ_TOKEN_CACHE="$base/cache/teams-read-token.json"
export CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE="$base/cache/teams-file-token.json"

run_smoke() {
  local name="$1"
  shift
  local out="$base/${name}.txt"
  "$bin" "$@" 2>&1 | tee "$out"
}

assert_contains() {
  local file="$1"
  local text="$2"
  if ! grep -Fq "$text" "$file"; then
    echo "expected $file to contain: $text" >&2
    echo "---- $file ----" >&2
    cat "$file" >&2
    exit 1
  fi
}

"$bin" --version >/dev/null

run_smoke setup teams setup
run_smoke status teams status
run_smoke control-print teams control --print
run_smoke doctor teams doctor
run_smoke service-doctor teams service doctor

assert_contains "$base/setup.txt" "Teams setup checklist"
assert_contains "$base/status.txt" "Teams status"
assert_contains "$base/status.txt" "Control chat: unavailable"
assert_contains "$base/control-print.txt" "Teams control chat: unavailable"
assert_contains "$base/doctor.txt" "Teams doctor"
assert_contains "$base/doctor.txt" "Graph: not checked"
assert_contains "$base/service-doctor.txt" "Teams service backend:"
