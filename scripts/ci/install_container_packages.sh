#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -eq 0 ]]; then
  echo "usage: $0 <package> [package...]" >&2
  exit 1
fi

retry_attempts="${RETRY_ATTEMPTS:-5}"
retry_sleep_seconds="${RETRY_SLEEP_SECONDS:-5}"
apt_retry_attempts="${APT_RETRY_ATTEMPTS:-6}"
apt_retry_sleep_seconds="${APT_RETRY_SLEEP_SECONDS:-5}"
apt_retry_max_sleep_seconds="${APT_RETRY_MAX_SLEEP_SECONDS:-20}"
ci_script_dir="${CI_SCRIPT_DIR:-/ci}"
packages=("$@")

retry_cmd() {
  bash "$ci_script_dir/retry.sh" "$retry_attempts" "$retry_sleep_seconds" "$@"
}

apt_retry_delay_seconds() {
  local attempt="$1"
  local delay=$((apt_retry_sleep_seconds * attempt))
  if (( delay > apt_retry_max_sleep_seconds )); then
    delay="$apt_retry_max_sleep_seconds"
  fi
  printf '%s' "$delay"
}

apt_reset_state() {
  apt-get clean >/dev/null 2>&1 || true
  rm -rf /var/lib/apt/lists/partial/* /var/cache/apt/archives/partial/* 2>/dev/null || true
}

apt_retry_run() {
  local label="$1"
  shift

  local attempt=1
  while (( attempt <= apt_retry_attempts )); do
    if "$@"; then
      return 0
    fi

    if (( attempt >= apt_retry_attempts )); then
      break
    fi

    local delay
    delay="$(apt_retry_delay_seconds "$attempt")"
    echo "command failed (attempt ${attempt}/${apt_retry_attempts}), retrying in ${delay}s: ${label}" >&2
    apt_reset_state
    sleep "$delay"
    ((attempt += 1))
  done

  echo "command failed after ${apt_retry_attempts} attempts: ${label}" >&2
  return 1
}

strict_apt_update_once() {
  local log_path
  local status=0
  log_path="$(mktemp)"

  set +e
  apt-get update 2>&1 | tee "$log_path"
  status=${PIPESTATUS[0]}
  set -e

  if [[ "$status" -eq 0 ]] && ! grep -Eq '^W: Failed to fetch|^W: Some index files failed to download' "$log_path"; then
    rm -f "$log_path"
    return 0
  fi

  if [[ "$status" -eq 0 ]]; then
    echo "apt-get update completed with incomplete package indexes; treating as failure" >&2
  fi
  rm -f "$log_path"
  return 1
}

strict_apt_update() {
  apt_retry_run "apt-get update" strict_apt_update_once
}

apt_install_packages() {
  apt-get install -y --no-install-recommends "${packages[@]}"
}

apt_install() {
  export DEBIAN_FRONTEND=noninteractive
  mkdir -p /etc/apt/apt.conf.d
  cat > /etc/apt/apt.conf.d/80codex-ci-retries <<EOF
Acquire::Retries "${apt_retry_attempts}";
Acquire::http::Timeout "30";
Acquire::https::Timeout "30";
EOF
  if ! strict_apt_update; then
    if is_ubuntu; then
      bash "$ci_script_dir/configure_container_repos.sh" ubuntu-azure-archive
      run_strategy ubuntu-azure-archive strict_apt_update
    else
      return 1
    fi
  fi
  apt_retry_run "apt-get install -y --no-install-recommends ${packages[*]}" apt_install_packages
}

dnf_install() {
  retry_cmd dnf -y clean all
  retry_cmd dnf -y makecache
  retry_cmd dnf -y install "${packages[@]}"
}

yum_install() {
  retry_cmd yum -y clean all
  retry_cmd yum -y makecache
  retry_cmd yum -y install "${packages[@]}"
}

run_strategy() {
  local name="$1"
  shift
  echo "Using package source strategy: $name" >&2
  "$@"
}

is_rocky() {
  local distro_id=""
  if [[ -r /etc/os-release ]]; then
    distro_id="$(. /etc/os-release && printf '%s' "${ID:-}")"
  fi
  [[ "$distro_id" == "rocky" ]] || ls /etc/yum.repos.d/Rocky-*.repo >/dev/null 2>&1
}

is_ubuntu() {
  local distro_id=""
  if [[ -r /etc/os-release ]]; then
    distro_id="$(. /etc/os-release && printf '%s' "${ID:-}")"
  fi
  [[ "$distro_id" == "ubuntu" ]]
}

if command -v apt-get >/dev/null 2>&1; then
  apt_install
  exit 0
fi

if command -v dnf >/dev/null 2>&1; then
  if is_rocky; then
    if run_strategy rocky-default dnf_install; then
      exit 0
    fi
    bash "$ci_script_dir/configure_container_repos.sh" rocky-official
    run_strategy rocky-official-baseurl dnf_install
    exit 0
  fi

  dnf_install
  exit 0
fi

if command -v yum >/dev/null 2>&1; then
  if [[ -f /etc/yum.repos.d/CentOS-Base.repo ]]; then
    bash "$ci_script_dir/configure_container_repos.sh" centos-vault
  fi
  yum_install
  exit 0
fi

echo "No supported package manager found" >&2
exit 1
