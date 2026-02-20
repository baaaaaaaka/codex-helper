#!/usr/bin/env bash

set -euo pipefail

node_min_major="${NODE_MIN_MAJOR:-16}"
node_preferred_major="${NODE_MAJOR:-22}"
allow_system_node="${NODE_ALLOW_SYSTEM_NODE:-1}"

case "$(uname -s)" in
  Linux) node_os="linux" ;;
  *)
    echo "unsupported OS: $(uname -s)" >&2
    exit 1
    ;;
esac

case "$(uname -m)" in
  x86_64 | amd64) node_arch="x64" ;;
  aarch64 | arm64) node_arch="arm64" ;;
  *)
    echo "unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

parse_major() {
  local raw="$1"
  raw="${raw#v}"
  raw="${raw%%.*}"
  if [ -z "$raw" ]; then
    echo ""
    return
  fi
  case "$raw" in
    '' | *[!0-9]*) echo "" ;;
    *) echo "$raw" ;;
  esac
}

resolve_node_major() {
  local node_path="$1"
  local out=""
  if ! out="$("$node_path" -v 2>/dev/null)"; then
    echo ""
    return
  fi
  parse_major "$out"
}

if [ "$allow_system_node" = "1" ] && command -v node >/dev/null 2>&1; then
  system_node="$(command -v node)"
  system_major="$(resolve_node_major "$system_node")"
  if [ -n "$system_major" ] && [ "$system_major" -ge "$node_min_major" ]; then
    dirname "$system_node"
    exit 0
  fi
fi

default_root=""
if [ -n "${HOME:-}" ] && mkdir -p "$HOME/.cache/codex-node" 2>/dev/null; then
  default_root="$HOME/.cache/codex-node"
else
  uid_part="$(id -u 2>/dev/null || echo 0)"
  default_root="/tmp/codex-node-${uid_part}"
  mkdir -p "$default_root"
fi

install_root="${NODE_INSTALL_ROOT:-$default_root}"
install_dir="${NODE_INSTALL_DIR:-$install_root/v${node_preferred_major}-${node_os}-${node_arch}}"

if [ -x "$install_dir/bin/node" ]; then
  local_major="$(resolve_node_major "$install_dir/bin/node")"
  if [ -n "$local_major" ] && [ "$local_major" -ge "$node_min_major" ]; then
    echo "${install_dir}/bin"
    exit 0
  fi
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

base_url="https://nodejs.org/dist/latest-v${node_preferred_major}.x"
shasums_file="${tmp_dir}/SHASUMS256.txt"
curl -fsSL "${base_url}/SHASUMS256.txt" -o "$shasums_file"

tarball="$(
  awk -v major="${node_preferred_major}" -v os="${node_os}" -v arch="${node_arch}" '
    $2 ~ ("^node-v" major "\\.[0-9]+\\.[0-9]+-" os "-" arch "\\.tar\\.xz$") {
      print $2
      exit
    }
  ' "$shasums_file"
)"
if [ -z "$tarball" ]; then
  echo "failed to resolve Node.js tarball for ${node_os}-${node_arch}" >&2
  exit 1
fi

expected_sha="$(awk -v target="$tarball" '$2 == target { print $1; exit }' "$shasums_file")"
if [ -z "$expected_sha" ]; then
  echo "missing checksum for ${tarball}" >&2
  exit 1
fi

archive_path="${tmp_dir}/${tarball}"
curl -fsSL "${base_url}/${tarball}" -o "$archive_path"

if command -v sha256sum >/dev/null 2>&1; then
  actual_sha="$(sha256sum "$archive_path" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  actual_sha="$(shasum -a 256 "$archive_path" | awk '{print $1}')"
else
  echo "missing checksum tool: need sha256sum or shasum" >&2
  exit 1
fi
if [ "$actual_sha" != "$expected_sha" ]; then
  echo "Node.js checksum mismatch for ${tarball}" >&2
  echo "expected=${expected_sha}" >&2
  echo "actual=${actual_sha}" >&2
  exit 1
fi

rm -rf "$install_dir"
mkdir -p "$install_dir"
tar -xJf "$archive_path" --strip-components=1 -C "$install_dir"

installed_major="$(resolve_node_major "$install_dir/bin/node")"
if [ -z "$installed_major" ] || [ "$installed_major" -lt "$node_min_major" ]; then
  echo "installed Node.js version is below minimum ${node_min_major}" >&2
  exit 1
fi

echo "${install_dir}/bin"
