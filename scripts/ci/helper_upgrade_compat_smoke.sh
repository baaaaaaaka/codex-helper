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

assert_version_any() {
  local path="$1"
  shift
  local output
  output="$("$path" --version)"
  printf '%s\n' "$output"
  local tag
  for tag in "$@"; do
    if grep -Fq "$(version_no_v "$tag")" <<<"$output"; then
      return 0
    fi
  done
  echo "$path version did not match any expected tag: $*" >&2
  return 1
}

assert_missing_or_version() {
  local path="$1"
  local tag="$2"
  if [[ ! -e "$path" ]]; then
    return 0
  fi
  assert_version "$path" "$tag"
}

assert_cxp_entrypoint_healthy() {
  local managed="$1"
  local cxp="$2"
  local tag="$3"
  assert_version "$managed" "$tag"
  assert_version "$cxp" "$tag"
  export CXP_HEALTH_MANAGED="$managed"
  export CXP_HEALTH_CXP="$cxp"
  python3 - <<'PY'
import os
from pathlib import Path

managed = Path(os.environ["CXP_HEALTH_MANAGED"])
cxp = Path(os.environ["CXP_HEALTH_CXP"])
if not managed.exists():
    raise SystemExit(f"managed target is missing: {managed}")
if not cxp.exists() and not cxp.is_symlink():
    raise SystemExit(f"cxp entrypoint is missing: {cxp}")
if cxp.is_symlink():
    seen = set()
    current = cxp
    for _ in range(64):
        key = str(current)
        if key in seen:
            raise SystemExit(f"cxp symlink chain loops at {current}")
        seen.add(key)
        if not current.is_symlink():
            break
        target = os.readlink(current)
        current = Path(target) if os.path.isabs(target) else current.parent / target
    else:
        raise SystemExit(f"cxp symlink chain is too deep: {cxp}")
    if not current.exists():
        raise SystemExit(f"cxp symlink chain is broken: {cxp} -> {current}")
    if os.path.realpath(current) != os.path.realpath(managed):
        raise SystemExit(f"cxp symlink resolves to {current}, want {managed}")
PY
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
    local-dir-multihop)
      local physical_local="$scenario_base/storage with spaces/physical-local"
      local first_hop="$scenario_base/link-hop-one"
      mkdir -p "$home" "$physical_local"
      rm -rf "$home/.local" "$first_hop"
      ln -s "$physical_local" "$first_hop"
      ln -s "$first_hop" "$home/.local"
      ;;
    local-bin-multihop)
      local physical_bin="$scenario_base/storage with spaces/physical-bin"
      local first_hop="$scenario_base/link-hop-one-bin"
      mkdir -p "$home/.local" "$physical_bin"
      rm -rf "$home/.local/bin" "$first_hop"
      ln -s "$physical_bin" "$first_hop"
      ln -s "$first_hop" "$home/.local/bin"
      ;;
    *)
      echo "unknown helper upgrade compatibility storage layout: $layout" >&2
      exit 2
      ;;
  esac
}

managed_storage_layout_snapshot() {
  local scenario_base="$1"
  local layout="$2"
  local home="$3"
  case "$layout" in
    normal)
      ;;
    local-dir-symlink)
      printf '.local=%s\n' "$(readlink "$home/.local")"
      ;;
    local-bin-symlink)
      printf '.local/bin=%s\n' "$(readlink "$home/.local/bin")"
      ;;
    local-dir-multihop)
      printf '.local=%s\n' "$(readlink "$home/.local")"
      printf 'hop=%s\n' "$(readlink "$scenario_base/link-hop-one")"
      ;;
    local-bin-multihop)
      printf '.local/bin=%s\n' "$(readlink "$home/.local/bin")"
      printf 'hop=%s\n' "$(readlink "$scenario_base/link-hop-one-bin")"
      ;;
  esac
}

helper_config_home() {
  case "$os" in
    darwin)
      printf '%s\n' "$HOME/Library/Application Support"
      ;;
    *)
      printf '%s\n' "$XDG_CONFIG_HOME"
      ;;
  esac
}

helper_install_record_path() {
  printf '%s\n' "$(helper_config_home)/codex-helper/install.json"
}

write_poisoned_cxp_install_record() {
  local managed_cxp="$1"
  local version="$2"
  local record_path
  record_path="$(helper_install_record_path)"
  mkdir -p "$(dirname "$record_path")"
  export HELPER_RECORD_PATH="$record_path"
  export MANAGED_CXP="$managed_cxp"
  export RECORD_VERSION="$(version_no_v "$version")"
  export HELPER_GOOS="$os"
  python3 - <<'PY'
import json
import os
from pathlib import Path

record_path = Path(os.environ["HELPER_RECORD_PATH"])
record_path.write_text(json.dumps({
    "schema_version": 1,
    "target_path": os.environ["MANAGED_CXP"],
    "target_source": "record",
    "target_state": "managed",
    "version": os.environ["RECORD_VERSION"],
    "goos": os.environ["HELPER_GOOS"],
    "shims": [os.environ["MANAGED_CXP"]],
}, indent=2) + "\n")
PY
}

write_physical_target_install_record_without_shims() {
  local managed_target="$1"
  local version="$2"
  local record_path
  record_path="$(helper_install_record_path)"
  mkdir -p "$(dirname "$record_path")"
  export HELPER_RECORD_PATH="$record_path"
  export MANAGED_TARGET="$managed_target"
  export RECORD_VERSION="$(version_no_v "$version")"
  export HELPER_GOOS="$os"
  python3 - <<'PY'
import json
import os
from pathlib import Path

record_path = Path(os.environ["HELPER_RECORD_PATH"])
record_path.write_text(json.dumps({
    "schema_version": 1,
    "target_path": os.environ["MANAGED_TARGET"],
    "target_source": "current_executable",
    "target_state": "managed",
    "version": os.environ["RECORD_VERSION"],
    "goos": os.environ["HELPER_GOOS"],
    "shims": None,
}, indent=2) + "\n")
PY
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
  local storage_snapshot_before
  storage_snapshot_before="$(managed_storage_layout_snapshot "$scenario_base" "$storage_layout" "$HOME")"

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
  # install path is guaranteed to be current before the new helper runs. Some
  # newer old releases also repair managed aliases during this hop.
  case "$seed_mode" in
    copy|symlink)
      assert_version_any "$managed" "$old_tag" "$target_tag"
      assert_version_any "$managed_cxp" "$old_tag" "$target_tag"
      ;;
    stale-symlink)
      assert_version_any "$managed" "$old_tag" "$target_tag"
      assert_version "$managed_cxp" "$target_tag"
      ;;
    missing)
      assert_missing_or_version "$managed" "$target_tag"
      assert_missing_or_version "$managed_cxp" "$target_tag"
      ;;
    current-missing-cxp)
      mkdir -p "$(dirname "$managed")"
      rm -f "$managed"
      cp -f "$go_bin" "$managed"
      chmod 0755 "$managed"
      rm -f "$managed_cxp"
      assert_version "$managed" "$target_tag"
      [[ ! -e "$managed_cxp" ]]
      ;;
  esac

  "$go_bin" teams service install

  assert_cxp_entrypoint_healthy "$managed" "$managed_cxp" "$target_tag"
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
  assert_cxp_entrypoint_healthy "$managed" "$managed_cxp" "$target_tag"

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

managed = os.environ["MANAGED_CXP"]
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
          grep -Fq "ExecStart=$managed_cxp teams run --owner-stale-after 1m30s --auto-service=false" "$unit"
          grep -Fq "Environment=CODEX_PROXY_INSTALL_PATH=$managed_cxp" "$unit"
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
      grep -Fq "<string>$managed_cxp</string>" "$plist"
      grep -Fq "<key>CODEX_PROXY_INSTALL_PATH</key>" "$plist"
      if grep -Fq "CODEX_PROXY_INSTALL_DIR" "$plist"; then
        echo "LaunchAgent plist should not preserve CODEX_PROXY_INSTALL_DIR" >&2
        cat "$plist" >&2
        exit 1
      fi
      ;;
  esac

  local storage_snapshot_after
  storage_snapshot_after="$(managed_storage_layout_snapshot "$scenario_base" "$storage_layout" "$HOME")"
  if [[ "$storage_snapshot_after" != "$storage_snapshot_before" ]]; then
    echo "managed storage link topology changed during upgrade: layout=$storage_layout" >&2
    printf 'before:\n%s\nafter:\n%s\n' "$storage_snapshot_before" "$storage_snapshot_after" >&2
    exit 1
  fi
}

run_legacy_recorded_cxp_upgrade_scenario() {
  local scenario="$1"
  local storage_layout="$2"
  local scenario_base="$base_root/$scenario"
  rm -rf "$scenario_base"
  mkdir -p "$scenario_base"

  export HOME="$scenario_base/home"
  export XDG_CONFIG_HOME="$HOME/.config"
  export XDG_CACHE_HOME="$HOME/.cache"
  export CODEX_HOME="$HOME/.codex"
  export CODEX_PROXY_SKIP_BUILTIN_SKILLS=1
  export PATH="$original_path"

  local managed="$HOME/.local/bin/codex-proxy"
  local managed_cxp="$HOME/.local/bin/cxp"
  configure_managed_storage_layout "$scenario_base" "$storage_layout" "$HOME"
  mkdir -p "$(dirname "$managed")"
  download_binary "$old_tag" "$managed"
  ln -s "$managed" "$managed_cxp"
  assert_cxp_entrypoint_healthy "$managed" "$managed_cxp" "$old_tag"

  write_poisoned_cxp_install_record "$managed_cxp" "$old_tag"
  echo "helper upgrade compatibility smoke: legacy recorded-cxp upgrade scenario=$scenario storage=$storage_layout"
  retry 5 10 "$managed" upgrade --repo "$repo" --version "$target_tag"
  assert_cxp_entrypoint_healthy "$managed" "$managed_cxp" "$target_tag"

  # A second no-op run exercises the newly installed helper's finalizer and
  # verifies that old records pointing at cxp are migrated back to codex-proxy.
  retry 5 10 "$managed" upgrade --repo "$repo" --version "$target_tag"
  assert_cxp_entrypoint_healthy "$managed" "$managed_cxp" "$target_tag"

  export MANAGED_TARGET="$managed"
  export MANAGED_CXP="$managed_cxp"
  export TARGET_VERSION="$(version_no_v "$target_tag")"
  export HELPER_RECORD_PATH="$(helper_install_record_path)"
  python3 - <<'PY'
import json
import os
from pathlib import Path

record = json.loads(Path(os.environ["HELPER_RECORD_PATH"]).read_text())
assert record["target_path"] == os.environ["MANAGED_TARGET"], record
assert os.environ["MANAGED_CXP"] in record.get("shims", []), record
assert record["version"].lstrip("v") == os.environ["TARGET_VERSION"], record
PY
}

prepare_legacy_physical_record_fixture() {
  local scenario="$1"
  local storage_layout="$2"
  local scenario_base="$base_root/$scenario"
  rm -rf "$scenario_base"
  mkdir -p "$scenario_base"

  export HOME="$scenario_base/home"
  export XDG_CONFIG_HOME="$HOME/.config"
  export XDG_CACHE_HOME="$HOME/.cache"
  export CODEX_HOME="$HOME/.codex"
  export CODEX_PROXY_SKIP_BUILTIN_SKILLS=1
  export PATH="$original_path"
  unset CODEX_PROXY_INSTALL_PATH CODEX_PROXY_INSTALL_DIR
  unset CODEX_HELPER_TEAMS_CHILD CODEX_HELPER_TEAMS_PARENT_PID

  configure_managed_storage_layout "$scenario_base" "$storage_layout" "$HOME"
  local logical_target="$HOME/.local/bin/codex-proxy"
  local physical_bin
  mkdir -p "$(dirname "$logical_target")"
  physical_bin="$(cd "$(dirname "$logical_target")" && pwd -P)"
  LEGACY_FIXTURE_TARGET="$physical_bin/codex-proxy"
  LEGACY_FIXTURE_CXP="$physical_bin/cxp"
  LEGACY_FIXTURE_LOGICAL_TARGET="$logical_target"
  LEGACY_FIXTURE_RECORD="$(helper_install_record_path)"

  download_binary "$old_tag" "$LEGACY_FIXTURE_TARGET"
  ln -s codex-proxy "$LEGACY_FIXTURE_CXP"
  write_physical_target_install_record_without_shims "$LEGACY_FIXTURE_TARGET" "$old_tag"

  if [[ "$LEGACY_FIXTURE_LOGICAL_TARGET" == "$LEGACY_FIXTURE_TARGET" ]]; then
    echo "legacy bridge fixture did not preserve distinct logical and physical target paths" >&2
    exit 1
  fi
  if [[ "$(realpath "$LEGACY_FIXTURE_LOGICAL_TARGET")" != "$LEGACY_FIXTURE_TARGET" ]]; then
    echo "legacy bridge fixture logical target does not resolve to physical target" >&2
    exit 1
  fi
  if [[ -L "$LEGACY_FIXTURE_TARGET" ]]; then
    echo "legacy bridge fixture target unexpectedly starts as a symlink" >&2
    exit 1
  fi
  assert_cxp_entrypoint_healthy "$LEGACY_FIXTURE_TARGET" "$LEGACY_FIXTURE_CXP" "$old_tag"
}

run_legacy_physical_record_failure_control() {
  local vulnerable_target="v0.1.13-rc.16"
  prepare_legacy_physical_record_fixture "legacy-physical-record-failure-control" "local-dir-symlink"
  local output="$base_root/legacy-physical-record-failure-control.log"
  set +e
  "$LEGACY_FIXTURE_TARGET" upgrade --repo "$repo" --version "$vulnerable_target" >"$output" 2>&1
  local status=$?
  set -e
  printf 'legacy physical-record failure control exit status: %s\n' "$status"
  cat "$output"

  if [[ ! -L "$LEGACY_FIXTURE_TARGET" ]]; then
    echo "v0.1.12 -> $vulnerable_target did not reproduce the managed target self-loop" >&2
    ls -l "$LEGACY_FIXTURE_TARGET" "$LEGACY_FIXTURE_TARGET.prev" >&2 || true
    exit 1
  fi
  if [[ "$(readlink "$LEGACY_FIXTURE_TARGET")" != "$LEGACY_FIXTURE_TARGET" ]]; then
    echo "failure control target is a symlink but not the expected self-loop" >&2
    ls -l "$LEGACY_FIXTURE_TARGET" >&2
    exit 1
  fi
  assert_version "$LEGACY_FIXTURE_TARGET.prev" "$old_tag"
}

run_legacy_existing_self_loop_vulnerable_parent_recovery() {
  run_legacy_physical_record_failure_control

  echo "helper upgrade compatibility smoke: recover generated self-loop through vulnerable $old_tag parent"
  retry 5 10 "$LEGACY_FIXTURE_TARGET.prev" upgrade \
    --repo "$repo" \
    --version "$target_tag" \
    --install-path "$LEGACY_FIXTURE_TARGET"
  if [[ -L "$LEGACY_FIXTURE_TARGET" ]]; then
    echo "vulnerable-parent recovery left managed target as a symlink" >&2
    ls -l "$LEGACY_FIXTURE_TARGET" "$LEGACY_FIXTURE_TARGET.prev" >&2 || true
    exit 1
  fi
  assert_cxp_entrypoint_healthy "$LEGACY_FIXTURE_TARGET" "$LEGACY_FIXTURE_CXP" "$target_tag"
}

run_legacy_existing_self_loop_safe_parent_recovery() {
  local safe_parent_tag="v0.1.13-rc.16"
  prepare_legacy_physical_record_fixture "legacy-existing-self-loop-safe-parent" "local-dir-symlink"
  local safe_parent="$base_root/legacy-existing-self-loop-safe-parent/running-codex-proxy"
  download_binary "$safe_parent_tag" "$safe_parent"
  install -m 0755 "$safe_parent" "$LEGACY_FIXTURE_TARGET.prev"
  rm -f "$LEGACY_FIXTURE_TARGET"
  ln -s "$LEGACY_FIXTURE_TARGET" "$LEGACY_FIXTURE_TARGET"
  write_physical_target_install_record_without_shims "$LEGACY_FIXTURE_TARGET" "$safe_parent_tag"

  if [[ ! -L "$LEGACY_FIXTURE_TARGET" || "$(readlink "$LEGACY_FIXTURE_TARGET")" != "$LEGACY_FIXTURE_TARGET" ]]; then
    echo "safe-parent recovery fixture is not the expected self-loop" >&2
    ls -l "$LEGACY_FIXTURE_TARGET" "$LEGACY_FIXTURE_TARGET.prev" >&2 || true
    exit 1
  fi
  test "$(sha256sum "$safe_parent" | awk '{print $1}')" = "$(sha256sum "$LEGACY_FIXTURE_TARGET.prev" | awk '{print $1}')"

  echo "helper upgrade compatibility smoke: recover existing self-loop through safe $safe_parent_tag parent"
  retry 5 10 "$safe_parent" upgrade \
    --repo "$repo" \
    --version "$target_tag" \
    --install-path "$LEGACY_FIXTURE_TARGET"
  if [[ -L "$LEGACY_FIXTURE_TARGET" ]]; then
    echo "safe-parent recovery left managed target as a symlink" >&2
    ls -l "$LEGACY_FIXTURE_TARGET" "$LEGACY_FIXTURE_TARGET.prev" >&2 || true
    exit 1
  fi
  assert_cxp_entrypoint_healthy "$LEGACY_FIXTURE_TARGET" "$LEGACY_FIXTURE_CXP" "$target_tag"
}

run_legacy_linked_managed_target_upgrade() {
  local scenario_base="$base_root/legacy-linked-managed-target"
  rm -rf "$scenario_base"
  mkdir -p "$scenario_base"

  export HOME="$scenario_base/home"
  export XDG_CONFIG_HOME="$HOME/.config"
  export XDG_CACHE_HOME="$HOME/.cache"
  export CODEX_HOME="$HOME/.codex"
  export CODEX_PROXY_SKIP_BUILTIN_SKILLS=1
  export PATH="$original_path"
  unset CODEX_PROXY_INSTALL_PATH CODEX_PROXY_INSTALL_DIR
  unset CODEX_HELPER_TEAMS_CHILD CODEX_HELPER_TEAMS_PARENT_PID

  local parent="$scenario_base/physical/codex-proxy"
  local managed="$HOME/.local/bin/codex-proxy"
  local managed_cxp="$HOME/.local/bin/cxp"
  download_binary "$old_tag" "$parent"
  mkdir -p "$(dirname "$managed")"
  ln -s "$parent" "$managed"
  ln -s codex-proxy "$managed_cxp"
  # Legacy alias convergence recorded the physical running helper while
  # materializing the managed default as a symlink to that helper.
  write_physical_target_install_record_without_shims "$parent" "$old_tag"

  if [[ ! -L "$managed" || "$(readlink "$managed")" != "$parent" ]]; then
    echo "legacy linked-target fixture is not the expected managed symlink" >&2
    ls -l "$managed" "$parent" >&2 || true
    exit 1
  fi
  assert_cxp_entrypoint_healthy "$managed" "$managed_cxp" "$old_tag"

  echo "helper upgrade compatibility smoke: upgrade verified legacy managed-target symlink"
  retry 5 10 "$parent" upgrade \
    --repo "$repo" \
    --version "$target_tag" \
    --install-path "$managed"
  if [[ -L "$managed" ]]; then
    echo "legacy linked-target upgrade did not replace the managed symlink with the downloaded binary" >&2
    ls -l "$managed" "$parent" >&2 || true
    exit 1
  fi
  assert_cxp_entrypoint_healthy "$managed" "$managed_cxp" "$target_tag"
  assert_version "$parent" "$target_tag"

  export MANAGED_TARGET="$managed"
  export MANAGED_CXP="$managed_cxp"
  export TARGET_VERSION="$(version_no_v "$target_tag")"
  export HELPER_RECORD_PATH="$(helper_install_record_path)"
  python3 - <<'PY'
import json
import os
from pathlib import Path

record = json.loads(Path(os.environ["HELPER_RECORD_PATH"]).read_text())
assert record["target_path"] == os.environ["MANAGED_TARGET"], record
assert record["version"].lstrip("v") == os.environ["TARGET_VERSION"], record
assert os.environ["MANAGED_CXP"] in record.get("shims", []), record
PY
}

run_legacy_physical_record_bridge_success() {
  local storage_layout="$1"
  local scenario="legacy-physical-record-bridge-${storage_layout}"
  prepare_legacy_physical_record_fixture "$scenario" "$storage_layout"

  retry 5 10 "$LEGACY_FIXTURE_TARGET" upgrade --repo "$repo" --version "$target_tag"
  if [[ -L "$LEGACY_FIXTURE_TARGET" ]]; then
    echo "bridge upgrade left managed target as a symlink" >&2
    ls -l "$LEGACY_FIXTURE_TARGET" >&2
    exit 1
  fi
  assert_cxp_entrypoint_healthy "$LEGACY_FIXTURE_TARGET" "$LEGACY_FIXTURE_CXP" "$target_tag"

  export MANAGED_TARGET="$LEGACY_FIXTURE_TARGET"
  export MANAGED_CXP="$LEGACY_FIXTURE_CXP"
  export TARGET_VERSION="$(version_no_v "$target_tag")"
  export HELPER_RECORD_PATH="$LEGACY_FIXTURE_RECORD"
  python3 - <<'PY'
import json
import os
from pathlib import Path

record = json.loads(Path(os.environ["HELPER_RECORD_PATH"]).read_text())
assert record["target_path"] == os.environ["MANAGED_TARGET"], record
assert record["version"].lstrip("v") == os.environ["TARGET_VERSION"], record
assert record.get("shims", [None])[0] == os.environ["MANAGED_CXP"], record
PY

  local record_checksum_before record_mtime_before shim_before
  record_checksum_before="$(sha256sum "$LEGACY_FIXTURE_RECORD")"
  record_mtime_before="$(stat -c '%y' "$LEGACY_FIXTURE_RECORD")"
  shim_before="$(readlink "$LEGACY_FIXTURE_CXP")"
  assert_version "$LEGACY_FIXTURE_TARGET" "$target_tag"
  test "$(sha256sum "$LEGACY_FIXTURE_RECORD")" = "$record_checksum_before"
  test "$(stat -c '%y' "$LEGACY_FIXTURE_RECORD")" = "$record_mtime_before"
  test "$(readlink "$LEGACY_FIXTURE_CXP")" = "$shim_before"
}

run_legacy_physical_record_unsafe_env_rejection() {
  prepare_legacy_physical_record_fixture "legacy-physical-record-unsafe-env" "local-dir-symlink"
  local record_checksum_before output
  record_checksum_before="$(sha256sum "$LEGACY_FIXTURE_RECORD")"
  output="$base_root/legacy-physical-record-unsafe-env.log"
  export CODEX_PROXY_INSTALL_PATH="$LEGACY_FIXTURE_LOGICAL_TARGET"
  set +e
  "$LEGACY_FIXTURE_TARGET" upgrade --repo "$repo" --version "$target_tag" >"$output" 2>&1
  local status=$?
  set -e
  unset CODEX_PROXY_INSTALL_PATH
  cat "$output"

  if (( status == 0 )); then
    echo "legacy bridge unexpectedly accepted an unsafe logical install environment alias" >&2
    exit 1
  fi
  grep -Fq "unsafe environment alias" "$output"
  if [[ -L "$LEGACY_FIXTURE_TARGET" ]]; then
    echo "rejected unsafe-env upgrade changed the managed target into a symlink" >&2
    ls -l "$LEGACY_FIXTURE_TARGET" >&2
    exit 1
  fi
  test "$(sha256sum "$LEGACY_FIXTURE_RECORD")" = "$record_checksum_before"
  assert_cxp_entrypoint_healthy "$LEGACY_FIXTURE_TARGET" "$LEGACY_FIXTURE_CXP" "$old_tag"
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
run_upgrade_convergence_scenario "multihop-local-dir-managed-symlink" "symlink" "local-dir-multihop"
run_upgrade_convergence_scenario "multihop-local-bin-managed-symlink" "symlink" "local-bin-multihop"
run_legacy_recorded_cxp_upgrade_scenario "legacy-recorded-cxp-symlinked-local-dir" "local-dir-symlink"
run_legacy_recorded_cxp_upgrade_scenario "legacy-recorded-cxp-symlinked-local-bin" "local-bin-symlink"

if [[ "$os" == "linux" && "$old_tag" == "v0.1.12" && "$service_backend" == "local-supervisor" ]]; then
  run_legacy_existing_self_loop_vulnerable_parent_recovery
  run_legacy_existing_self_loop_safe_parent_recovery
  run_legacy_linked_managed_target_upgrade
  run_legacy_physical_record_bridge_success "local-dir-symlink"
  run_legacy_physical_record_bridge_success "local-bin-symlink"
  run_legacy_physical_record_unsafe_env_rejection
fi

echo "helper upgrade compatibility smoke passed"
