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

rewrite_ubuntu_archive_mirror() {
  local replacement_host="$1"
  local sources=()
  if [[ -f /etc/apt/sources.list ]]; then
    sources+=(/etc/apt/sources.list)
  fi
  if compgen -G "/etc/apt/sources.list.d/*.list" >/dev/null 2>&1; then
    sources+=(/etc/apt/sources.list.d/*.list)
  fi
  if compgen -G "/etc/apt/sources.list.d/*.sources" >/dev/null 2>&1; then
    sources+=(/etc/apt/sources.list.d/*.sources)
  fi

  if [[ "${#sources[@]}" -eq 0 ]]; then
    return
  fi

  for source_file in "${sources[@]}"; do
    sed -i -E "s|https?://archive\.ubuntu\.com/ubuntu/?|http://${replacement_host}/ubuntu/|g" "$source_file" || true
  done

  rm -rf /var/lib/apt/lists/*
}

configure_ubuntu_azure_archive() {
  rewrite_ubuntu_archive_mirror "azure.archive.ubuntu.com"
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
  ubuntu-azure-archive)
    configure_ubuntu_azure_archive
    ;;
  none)
    ;;
  *)
    echo "unsupported repo strategy: $strategy" >&2
    exit 1
    ;;
esac
