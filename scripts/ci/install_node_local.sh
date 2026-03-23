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

parse_minor() {
  local raw="$1"
  raw="${raw#v}"
  case "$raw" in
    *.*) raw="${raw#*.}" ;;
    *) echo ""; return ;;
  esac
  raw="${raw%%.*}"
  case "$raw" in
    '' | *[!0-9]*) echo "" ;;
    *) echo "$raw" ;;
  esac
}

resolve_glibc_version() {
  local raw=""
  if command -v getconf >/dev/null 2>&1; then
    raw="$(getconf GNU_LIBC_VERSION 2>/dev/null || true)"
    case "$raw" in
      glibc\ *) printf '%s\n' "${raw#glibc }"; return ;;
    esac
  fi
  if command -v ldd >/dev/null 2>&1; then
    raw="$(ldd --version 2>/dev/null | head -n 1 || true)"
    printf '%s\n' "$raw" | sed -n 's/.* \([0-9][0-9]*\.[0-9][0-9]*\).*/\1/p' | head -n 1
    return
  fi
  printf '\n'
}

version_lt() {
  local left="$1"
  local right="$2"
  local left_major left_minor right_major right_minor
  left_major="$(parse_major "$left")"
  left_minor="$(parse_minor "$left")"
  right_major="$(parse_major "$right")"
  right_minor="$(parse_minor "$right")"
  if [ -z "$left_major" ] || [ -z "$left_minor" ] || [ -z "$right_major" ] || [ -z "$right_minor" ]; then
    return 1
  fi
  if [ "$left_major" -lt "$right_major" ]; then
    return 0
  fi
  if [ "$left_major" -gt "$right_major" ]; then
    return 1
  fi
  [ "$left_minor" -lt "$right_minor" ]
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

download_file() {
  local src="$1"
  local dest="$2"
  curl --retry 5 --retry-delay 5 --connect-timeout 30 -fsSL "$src" -o "$dest"
}

resolve_official_node_version() {
  local latest_shasums latest_tarball latest_version
  latest_shasums="${tmp_dir}/latest-SHASUMS256.txt"
  download_file "https://nodejs.org/dist/latest-v${node_preferred_major}.x/SHASUMS256.txt" "$latest_shasums"

  latest_tarball="$(
    awk -v major="${node_preferred_major}" -v os="${node_os}" -v arch="${node_arch}" '
      $2 ~ ("^node-v" major "\\.[0-9]+\\.[0-9]+-" os "-" arch "\\.tar\\.(gz|xz)$") {
        print $2
        exit
      }
    ' "$latest_shasums"
  )"
  if [ -z "$latest_tarball" ]; then
    echo "failed to resolve latest Node.js tarball for ${node_os}-${node_arch}" >&2
    exit 1
  fi

  latest_version="${latest_tarball#node-v}"
  latest_version="${latest_version%-${node_os}-${node_arch}.tar.gz}"
  latest_version="${latest_version%-${node_os}-${node_arch}.tar.xz}"
  if [ -z "$latest_version" ]; then
    echo "failed to parse latest Node.js version from ${latest_tarball}" >&2
    exit 1
  fi
  printf '%s\n' "$latest_version"
}

if [ "$node_os" = "linux" ] && [ "$node_arch" = "x64" ]; then
  glibc_version="$(resolve_glibc_version)"
  if [ -n "$glibc_version" ] && version_lt "$glibc_version" "2.28"; then
    unofficial_index="${tmp_dir}/unofficial-index.tab"
    download_file "https://unofficial-builds.nodejs.org/download/release/index.tab" "$unofficial_index"
    latest_version="$(
      awk -F '\t' -v major="${node_preferred_major}" '
        $1 ~ ("^v" major "\\.") && $3 ~ /(^|,)linux-x64-glibc-217(,|$)/ {
          version=$1
          sub(/^v/, "", version)
          print version
          exit
        }
      ' "$unofficial_index"
    )"
    if [ -z "$latest_version" ]; then
      echo "failed to resolve legacy glibc Node.js version for major ${node_preferred_major}" >&2
      exit 1
    fi
    base_url="https://unofficial-builds.nodejs.org/download/release/v${latest_version}"
    tarball_pattern="node-v${latest_version}-${node_os}-${node_arch}-glibc-217"
  else
    latest_version="$(resolve_official_node_version)"
    base_url="https://nodejs.org/dist/v${latest_version}"
    tarball_pattern="node-v${latest_version}-${node_os}-${node_arch}"
  fi
else
  latest_version="$(resolve_official_node_version)"
  base_url="https://nodejs.org/dist/v${latest_version}"
  tarball_pattern="node-v${latest_version}-${node_os}-${node_arch}"
fi

shasums_file="${tmp_dir}/SHASUMS256.txt"
download_file "${base_url}/SHASUMS256.txt" "$shasums_file"

tarball=""
for ext in tar.gz tar.xz; do
  candidate="${tarball_pattern}.${ext}"
  if awk -v target="$candidate" '$2 == target { found=1; exit } END { exit found ? 0 : 1 }' "$shasums_file"; then
    tarball="$candidate"
    break
  fi
done
if [ -z "$tarball" ]; then
  echo "failed to resolve Node.js tarball for ${tarball_pattern}" >&2
  exit 1
fi

expected_sha="$(awk -v target="$tarball" '$2 == target { print $1; exit }' "$shasums_file")"
if [ -z "$expected_sha" ]; then
  echo "missing checksum for ${tarball}" >&2
  exit 1
fi

archive_path="${tmp_dir}/${tarball}"
download_file "${base_url}/${tarball}" "$archive_path"

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
case "$tarball" in
  *.tar.gz) tar -xzf "$archive_path" --strip-components=1 -C "$install_dir" ;;
  *.tar.xz) tar -xJf "$archive_path" --strip-components=1 -C "$install_dir" ;;
  *)
    echo "unsupported Node.js archive format: ${tarball}" >&2
    exit 1
    ;;
esac

installed_major="$(resolve_node_major "$install_dir/bin/node")"
if [ -z "$installed_major" ] || [ "$installed_major" -lt "$node_min_major" ]; then
  echo "installed Node.js version is below minimum ${node_min_major}" >&2
  exit 1
fi

echo "${install_dir}/bin"
