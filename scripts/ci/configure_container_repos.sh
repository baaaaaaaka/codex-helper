#!/usr/bin/env bash
set -euo pipefail

strategy="${1:-auto}"
container_root="${CI_CONTAINER_ROOT:-}"

root_path() {
  printf '%s%s' "$container_root" "$1"
}

configure_centos_vault() {
  local repo_file
  repo_file="$(root_path /etc/yum.repos.d/CentOS-Base.repo)"
  if [[ -f "$repo_file" ]]; then
    sed -i 's/^mirrorlist=/#mirrorlist=/g' "$repo_file" || true
    sed -i 's|^#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' "$repo_file" || true
  fi
}

configure_rocky_official() {
  local repo_dir
  repo_dir="$(root_path /etc/yum.repos.d)"
  local repos=("$repo_dir"/Rocky-*.repo)
  if [[ -e "${repos[0]}" ]]; then
    sed -i 's|^mirrorlist=|#mirrorlist=|g' "${repos[@]}" || true
    sed -i 's|^#baseurl=http://dl\.rockylinux\.org/|baseurl=https://dl.rockylinux.org/|g' "${repos[@]}" || true
    if [[ -z "$container_root" ]]; then
      dnf clean all >/dev/null 2>&1 || true
    fi
  fi
}

rewrite_ubuntu_archive_mirror() {
  local replacement_host="$1"
  local sources=()
  local sources_list
  sources_list="$(root_path /etc/apt/sources.list)"
  if [[ -f "$sources_list" ]]; then
    sources+=("$sources_list")
  fi
  local sources_dir
  sources_dir="$(root_path /etc/apt/sources.list.d)"
  local list_sources=("$sources_dir"/*.list)
  if [[ -e "${list_sources[0]}" ]]; then
    sources+=("${list_sources[@]}")
  fi
  local deb822_sources=("$sources_dir"/*.sources)
  if [[ -e "${deb822_sources[0]}" ]]; then
    sources+=("${deb822_sources[@]}")
  fi

  if [[ "${#sources[@]}" -eq 0 ]]; then
    return
  fi

  for source_file in "${sources[@]}"; do
    sed -i -E "s|https?://archive\.ubuntu\.com/ubuntu/?|http://${replacement_host}/ubuntu/|g" "$source_file" || true
  done

  local apt_lists
  apt_lists="$(root_path /var/lib/apt/lists)"
  rm -rf "$apt_lists"/*
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
