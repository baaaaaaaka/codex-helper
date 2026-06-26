#!/usr/bin/env bash
set -euo pipefail

repo="${REPO:-${GITHUB_REPOSITORY:-}}"
old_tag="${OLD_TAG:-}"
target_tag="${TARGET_TAG:-${TAG:-${GITHUB_REF_NAME:-}}}"
fetcher="${FETCHER:-curl}"
service_backend="${SERVICE_BACKEND:-local-supervisor}"
original_path="$PATH"

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
  Darwin) os="darwin" ;;
  *)
    echo "helper upgrade compatibility smoke currently supports Linux and macOS only" >&2
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
      retry 5 5 curl --connect-timeout 30 --max-time 180 -fsSL -o "$out" "$url"
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

configure_managed_storage_layout() {
  local scenario_base="$1"
  local layout="$2"
  local home="$3"

  case "$layout" in
    normal)
      ;;
    local-dir-symlink)
      local physical_local="$scenario_base/local-overflow"
      mkdir -p "$home" "$physical_local"
      rm -rf "$home/.local"
      ln -s "$physical_local" "$home/.local"
      ;;
    local-bin-symlink)
      local physical_bin="$scenario_base/local-bin-overflow"
      mkdir -p "$home/.local" "$physical_bin"
      rm -rf "$home/.local/bin"
      ln -s "$physical_bin" "$home/.local/bin"
      ;;
    *)
      echo "unknown helper upgrade compatibility storage layout: $layout" >&2
      exit 2
      ;;
  esac
}

run_upgrade_convergence_scenario() {
  local scenario="$1"
  local seed_mode="$2"
  local storage_layout="${3:-normal}"
  local scenario_base="$base_root/$scenario"
  rm -rf "$scenario_base"
  mkdir -p "$scenario_base"

  export HOME="$scenario_base/home"
  export XDG_CONFIG_HOME="$HOME/.config"
  export XDG_CACHE_HOME="$HOME/.cache"
  export CODEX_HOME="$HOME/.codex"
  export CODEX_PROXY_SKIP_BUILTIN_SKILLS=1
  export PATH="$original_path"
  export CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND="$service_backend"
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

  configure_managed_storage_layout "$scenario_base" "$storage_layout" "$HOME"

  case "$os:$service_backend" in
    linux:local-supervisor)
      ;;
    linux:systemd|linux:systemd-user)
      local fake_bin="$scenario_base/fake-bin"
      mkdir -p "$fake_bin"
      cat > "$fake_bin/systemctl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${CODEX_HELPER_FAKE_SYSTEMCTL_LOG:?}"
exit 0
SH
      chmod +x "$fake_bin/systemctl"
      export CODEX_HELPER_FAKE_SYSTEMCTL_LOG="$scenario_base/systemctl.log"
      : > "$CODEX_HELPER_FAKE_SYSTEMCTL_LOG"
      export PATH="$fake_bin:$PATH"
      ;;
    darwin:*)
      ;;
    linux:*)
      echo "unsupported Linux service backend for helper upgrade compatibility smoke: $service_backend" >&2
      exit 2
      ;;
  esac

  echo "helper upgrade compatibility smoke: scenario=$scenario mode=$seed_mode storage=$storage_layout os=$os service_backend=$service_backend repo=$repo old=$old_tag target=$target_tag"
  download_binary "$old_tag" "$go_bin"
  case "$seed_mode" in
    copy)
      mkdir -p "$(dirname "$managed")"
      cp -f "$go_bin" "$managed"
      cp -f "$go_bin" "$managed_cxp"
      chmod 0755 "$managed" "$managed_cxp"
      assert_version "$managed" "$old_tag"
      assert_version "$managed_cxp" "$old_tag"
      ;;
    symlink)
      mkdir -p "$(dirname "$managed")"
      cp -f "$go_bin" "$managed"
      chmod 0755 "$managed"
      ln -s "$managed" "$managed_cxp"
      assert_version "$managed" "$old_tag"
      assert_version "$managed_cxp" "$old_tag"
      ;;
    stale-symlink)
      mkdir -p "$(dirname "$managed")"
      cp -f "$go_bin" "$managed"
      chmod 0755 "$managed"
      ln -s "$go_bin" "$managed_cxp"
      assert_version "$managed" "$old_tag"
      assert_version "$managed_cxp" "$old_tag"
      ;;
    missing|current-missing-cxp)
      [[ ! -e "$managed" ]]
      [[ ! -e "$managed_cxp" ]]
      ;;
    *)
      echo "unknown helper upgrade compatibility seed mode: $seed_mode" >&2
      exit 2
      ;;
  esac
  assert_version "$go_bin" "$old_tag"

  retry 5 10 "$go_bin" upgrade --repo "$repo" --version "$target_tag" --install-path "$go_bin"

  assert_version "$go_bin" "$target_tag"
  # This first hop is executed by the old release binary, so only the invoked
  # install path is guaranteed to be current before the new helper runs.
  case "$seed_mode" in
    copy|symlink)
      assert_version "$managed" "$old_tag"
      assert_version "$managed_cxp" "$old_tag"
      ;;
    stale-symlink)
      assert_version "$managed" "$old_tag"
      assert_version "$managed_cxp" "$target_tag"
      ;;
    missing)
      [[ ! -e "$managed" ]]
      [[ ! -e "$managed_cxp" ]]
      ;;
    current-missing-cxp)
      mkdir -p "$(dirname "$managed")"
      cp -f "$go_bin" "$managed"
      chmod 0755 "$managed"
      rm -f "$managed_cxp"
      assert_version "$managed" "$target_tag"
      [[ ! -e "$managed_cxp" ]]
      ;;
  esac

  "$go_bin" teams service install

  assert_version "$managed" "$target_tag"
  assert_version "$managed_cxp" "$target_tag"
  case "$seed_mode" in
    symlink|stale-symlink|missing|current-missing-cxp)
      if [[ ! -L "$managed_cxp" ]]; then
        echo "managed cxp should be a symlink for seed mode $seed_mode" >&2
        ls -l "$managed_cxp" >&2 || true
        exit 1
      fi
      if [[ "$(readlink "$managed_cxp")" != "$managed" ]]; then
        echo "managed cxp symlink should point to $managed for seed mode $seed_mode" >&2
        ls -l "$managed_cxp" >&2 || true
        exit 1
      fi
      ;;
  esac

  echo "helper upgrade compatibility smoke: scenario=$scenario second-hop via managed cxp"
  retry 5 10 "$managed_cxp" upgrade --repo "$repo" --version "$target_tag"
  assert_version "$managed" "$target_tag"
  assert_version "$managed_cxp" "$target_tag"

  export MANAGED_TARGET="$managed"
  export MANAGED_CXP="$managed_cxp"
  export TARGET_VERSION="$(version_no_v "$target_tag")"
  case "$os" in
    darwin)
      export CODEX_HELPER_CONFIG_HOME="$HOME/Library/Application Support"
      ;;
    *)
      export CODEX_HELPER_CONFIG_HOME="$XDG_CONFIG_HOME"
      ;;
  esac
  python3 - <<'PY'
import json
import os
from pathlib import Path

managed = os.environ["MANAGED_TARGET"]
cxp = os.environ["MANAGED_CXP"]
target_version = os.environ["TARGET_VERSION"]
config_home = Path(os.environ["CODEX_HELPER_CONFIG_HOME"])

record_path = config_home / "codex-helper" / "install.json"
record = json.loads(record_path.read_text())
assert record["schema_version"] == 1, record
assert record["target_path"] == managed, record
assert record["target_state"] == "managed", record
assert record["version"].lstrip("v") == target_version, record
assert cxp in record.get("shims", []), record
PY

  case "$os" in
    linux)
      export MANAGED_TARGET="$managed"
      case "$service_backend" in
        local-supervisor)
          python3 - <<'PY'
import json
import os
from pathlib import Path

managed = os.environ["MANAGED_TARGET"]
config_home = Path(os.environ["XDG_CONFIG_HOME"])
supervisor_path = config_home / "codex-helper" / "teams" / "local-supervisor.json"
supervisor = json.loads(supervisor_path.read_text())
spec = supervisor["spec"]
assert spec["Executable"] == managed, supervisor
env = spec.get("Environment") or {}
assert env.get("CODEX_PROXY_INSTALL_PATH") == managed, supervisor
assert "CODEX_PROXY_INSTALL_DIR" not in env, supervisor
PY
          ;;
        systemd|systemd-user)
          unit="$XDG_CONFIG_HOME/systemd/user/codex-helper-teams.service"
          watchdog_unit="$XDG_CONFIG_HOME/systemd/user/codex-helper-teams-watchdog.service"
          watchdog_timer="$XDG_CONFIG_HOME/systemd/user/codex-helper-teams-watchdog.timer"
          test -f "$unit"
          test -f "$watchdog_unit"
          test -f "$watchdog_timer"
          grep -Fq "ExecStart=$managed teams run --owner-stale-after 1m30s --auto-service=false" "$unit"
          grep -Fq "Environment=CODEX_PROXY_INSTALL_PATH=$managed" "$unit"
          if grep -Fq "CODEX_PROXY_INSTALL_DIR" "$unit"; then
            echo "systemd unit should not preserve CODEX_PROXY_INSTALL_DIR" >&2
            cat "$unit" >&2
            exit 1
          fi
          grep -Fxq -- "--user daemon-reload" "$CODEX_HELPER_FAKE_SYSTEMCTL_LOG"
          ;;
      esac
      ;;
    darwin)
      local plist="$HOME/Library/LaunchAgents/com.codex-helper.teams.plist"
      test -f "$plist"
      grep -Fq "<string>$managed</string>" "$plist"
      grep -Fq "<key>CODEX_PROXY_INSTALL_PATH</key>" "$plist"
      if grep -Fq "CODEX_PROXY_INSTALL_DIR" "$plist"; then
        echo "LaunchAgent plist should not preserve CODEX_PROXY_INSTALL_DIR" >&2
        cat "$plist" >&2
        exit 1
      fi
      ;;
  esac
}

safe_old="${old_tag//[^A-Za-z0-9._-]/_}"
safe_target="${target_tag//[^A-Za-z0-9._-]/_}"
base_root="${RUNNER_TEMP:-/tmp}/codex-helper-upgrade-compat-${safe_old}-to-${safe_target}"
rm -rf "$base_root"
mkdir -p "$base_root"

run_upgrade_convergence_scenario "existing-managed-copy" "copy"
run_upgrade_convergence_scenario "existing-managed-symlink" "symlink"
run_upgrade_convergence_scenario "stale-managed-symlink" "stale-symlink"
run_upgrade_convergence_scenario "missing-managed" "missing"
run_upgrade_convergence_scenario "current-managed-missing-cxp" "current-missing-cxp"
run_upgrade_convergence_scenario "symlinked-local-dir-managed-symlink" "symlink" "local-dir-symlink"
run_upgrade_convergence_scenario "symlinked-local-bin-managed-symlink" "symlink" "local-bin-symlink"

echo "helper upgrade compatibility smoke passed"
