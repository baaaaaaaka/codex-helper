#!/usr/bin/env bash
set -euo pipefail

repo="${REPO:-${GITHUB_REPOSITORY:-}}"
old_tag="${OLD_TAG:-}"
target_tag="${TARGET_TAG:-${TAG:-${GITHUB_REF_NAME:-}}}"
fetcher="${FETCHER:-curl}"

if [[ -z "$repo" || -z "$old_tag" || -z "$target_tag" ]]; then
  echo "REPO, OLD_TAG, and TARGET_TAG are required" >&2
  exit 2
fi

if [[ "$old_tag" == "$target_tag" ]]; then
  echo "old tag equals target tag ($old_tag); nothing to upgrade"
  exit 0
fi

case "$(uname -s)" in
  Linux) os="linux" ;;
  *)
    echo "helper upgrade compatibility smoke currently supports Linux only" >&2
    exit 2
    ;;
esac

case "$(uname -m)" in
  x86_64|amd64) arch="amd64" ;;
  aarch64|arm64) arch="arm64" ;;
  *)
    echo "unsupported architecture: $(uname -m)" >&2
    exit 2
    ;;
esac

version_no_v() {
  local tag="$1"
  printf '%s' "${tag#v}"
}

asset_name() {
  local tag="$1"
  printf 'codex-proxy_%s_%s_%s' "$(version_no_v "$tag")" "$os" "$arch"
}

retry() {
  local attempts="$1"
  local delay="$2"
  shift 2
  local n=1
  while true; do
    if "$@"; then
      return 0
    fi
    if (( n >= attempts )); then
      return 1
    fi
    echo "command failed; retrying in ${delay}s ($n/$attempts): $*" >&2
    sleep "$delay"
    n=$((n + 1))
  done
}

fetch_url() {
  local url="$1"
  local out="$2"
  case "$fetcher" in
    curl)
      retry 5 5 curl --connect-timeout 30 -fsSL -o "$out" "$url"
      ;;
    wget)
      retry 5 5 wget --tries=1 --timeout=30 -q -O "$out" "$url"
      ;;
    *)
      echo "unsupported FETCHER=$fetcher (expected curl or wget)" >&2
      exit 2
      ;;
  esac
}

download_binary() {
  local tag="$1"
  local dst="$2"
  local url="https://github.com/${repo}/releases/download/${tag}/$(asset_name "$tag")"
  local tmp
  tmp="$(mktemp)"
  fetch_url "$url" "$tmp"
  mkdir -p "$(dirname "$dst")"
  install -m 0755 "$tmp" "$dst"
  rm -f "$tmp"
}

assert_version() {
  local path="$1"
  local tag="$2"
  local output
  output="$("$path" --version)"
  printf '%s\n' "$output"
  grep -Fq "$(version_no_v "$tag")" <<<"$output"
}

run_upgrade_convergence_scenario() {
  local scenario="$1"
  local seed_managed="$2"
  local scenario_base="$base_root/$scenario"
  rm -rf "$scenario_base"
  mkdir -p "$scenario_base"

  export HOME="$scenario_base/home"
  export XDG_CONFIG_HOME="$HOME/.config"
  export XDG_CACHE_HOME="$HOME/.cache"
  export CODEX_HOME="$HOME/.codex"
  export CODEX_PROXY_SKIP_BUILTIN_SKILLS=1
  export CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND=local-supervisor
  export CODEX_HELPER_TEAMS_TENANT_ID=ci-tenant
  export CODEX_HELPER_TEAMS_CLIENT_ID=ci-client
  export CODEX_HELPER_TEAMS_READ_CLIENT_ID=ci-read-client
  export CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID=ci-file-client
  export CODEX_HELPER_TEAMS_TOKEN_CACHE="$XDG_CACHE_HOME/teams-token.json"
  export CODEX_HELPER_TEAMS_READ_TOKEN_CACHE="$XDG_CACHE_HOME/teams-read-token.json"
  export CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE="$XDG_CACHE_HOME/teams-file-token.json"

  local managed="$HOME/.local/bin/codex-proxy"
  local managed_cxp="$HOME/.local/bin/cxp"
  local go_bin="$HOME/go/bin/codex-proxy"

  echo "helper upgrade compatibility smoke: scenario=$scenario repo=$repo old=$old_tag target=$target_tag"
  download_binary "$old_tag" "$go_bin"
  if [[ "$seed_managed" == "1" ]]; then
    mkdir -p "$(dirname "$managed")"
    cp -f "$go_bin" "$managed"
    cp -f "$go_bin" "$managed_cxp"
    chmod 0755 "$managed" "$managed_cxp"
    assert_version "$managed" "$old_tag"
    assert_version "$managed_cxp" "$old_tag"
  else
    [[ ! -e "$managed" ]]
    [[ ! -e "$managed_cxp" ]]
  fi
  assert_version "$go_bin" "$old_tag"

  retry 5 10 "$go_bin" upgrade --repo "$repo" --version "$target_tag" --install-path "$go_bin"

  assert_version "$go_bin" "$target_tag"
  if [[ "$seed_managed" == "1" ]]; then
    assert_version "$managed" "$old_tag"
    assert_version "$managed_cxp" "$old_tag"
  else
    [[ ! -e "$managed" ]]
  fi

  "$go_bin" teams service install

  assert_version "$managed" "$target_tag"
  if [[ -e "$managed_cxp" ]]; then
    assert_version "$managed_cxp" "$target_tag"
  fi

  export MANAGED_TARGET="$managed"
  export MANAGED_CXP="$managed_cxp"
  export EXPECT_MANAGED_CXP="$seed_managed"
  export TARGET_VERSION="$(version_no_v "$target_tag")"
  python3 - <<'PY'
import json
import os
from pathlib import Path

managed = os.environ["MANAGED_TARGET"]
cxp = os.environ["MANAGED_CXP"]
expect_cxp = os.environ["EXPECT_MANAGED_CXP"] == "1"
target_version = os.environ["TARGET_VERSION"]
config_home = Path(os.environ["XDG_CONFIG_HOME"])

record_path = config_home / "codex-helper" / "install.json"
record = json.loads(record_path.read_text())
assert record["schema_version"] == 1, record
assert record["target_path"] == managed, record
assert record["target_state"] == "managed", record
assert record["version"].lstrip("v") == target_version, record
if expect_cxp:
    assert cxp in record.get("shims", []), record
else:
    assert cxp not in record.get("shims", []), record

supervisor_path = config_home / "codex-helper" / "teams" / "local-supervisor.json"
supervisor = json.loads(supervisor_path.read_text())
spec = supervisor["spec"]
assert spec["Executable"] == managed, supervisor
env = spec.get("Environment") or {}
assert env.get("CODEX_PROXY_INSTALL_PATH") == managed, supervisor
assert "CODEX_PROXY_INSTALL_DIR" not in env, supervisor
PY
}

safe_old="${old_tag//[^A-Za-z0-9._-]/_}"
safe_target="${target_tag//[^A-Za-z0-9._-]/_}"
base_root="${RUNNER_TEMP:-/tmp}/codex-helper-upgrade-compat-${safe_old}-to-${safe_target}"
rm -rf "$base_root"
mkdir -p "$base_root"

run_upgrade_convergence_scenario "existing-managed" "1"
run_upgrade_convergence_scenario "missing-managed" "0"

echo "helper upgrade compatibility smoke passed"
