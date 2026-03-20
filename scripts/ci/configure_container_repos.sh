#!/usr/bin/env bash
set -euo pipefail

strategy="${1:-auto}"

configure_centos_vault() {
  if [[ -f /etc/yum.repos.d/CentOS-Base.repo ]]; then
    sed -i 's/^mirrorlist=/#mirrorlist=/g' /etc/yum.repos.d/CentOS-Base.repo || true
    sed -i 's|^#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-Base.repo || true
  fi
}

configure_rocky_official() {
  if ls /etc/yum.repos.d/Rocky-*.repo >/dev/null 2>&1; then
    sed -i 's|^mirrorlist=|#mirrorlist=|g' /etc/yum.repos.d/Rocky-*.repo || true
    sed -i 's|^#baseurl=http://dl\.rockylinux\.org/|baseurl=https://dl.rockylinux.org/|g' /etc/yum.repos.d/Rocky-*.repo || true
    dnf clean all >/dev/null 2>&1 || true
  fi
}

case "$strategy" in
  auto)
    configure_centos_vault
    configure_rocky_official
    ;;
  centos-vault)
    configure_centos_vault
    ;;
  rocky-official)
    configure_rocky_official
    ;;
  none)
    ;;
  *)
    echo "unsupported repo strategy: $strategy" >&2
    exit 1
    ;;
esac
