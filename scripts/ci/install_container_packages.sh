#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -eq 0 ]]; then
  echo "usage: $0 <package> [package...]" >&2
  exit 1
fi

retry_attempts="${RETRY_ATTEMPTS:-5}"
retry_sleep_seconds="${RETRY_SLEEP_SECONDS:-5}"
packages=("$@")

retry_cmd() {
  bash /ci/retry.sh "$retry_attempts" "$retry_sleep_seconds" "$@"
}

apt_install() {
  export DEBIAN_FRONTEND=noninteractive
  mkdir -p /etc/apt/apt.conf.d
  cat > /etc/apt/apt.conf.d/80codex-ci-retries <<EOF
Acquire::Retries "${retry_attempts}";
Acquire::http::Timeout "30";
Acquire::https::Timeout "30";
EOF
  retry_cmd apt-get update
  retry_cmd apt-get install -y --no-install-recommends "${packages[@]}"
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

if command -v apt-get >/dev/null 2>&1; then
  apt_install
  exit 0
fi

if command -v dnf >/dev/null 2>&1; then
  if is_rocky; then
    if run_strategy rocky-default dnf_install; then
      exit 0
    fi
    bash /ci/configure_container_repos.sh rocky-official
    run_strategy rocky-official-baseurl dnf_install
    exit 0
  fi

  dnf_install
  exit 0
fi

if command -v yum >/dev/null 2>&1; then
  if [[ -f /etc/yum.repos.d/CentOS-Base.repo ]]; then
    bash /ci/configure_container_repos.sh centos-vault
  fi
  yum_install
  exit 0
fi

echo "No supported package manager found" >&2
exit 1
