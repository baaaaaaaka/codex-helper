package cli

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	codexPathCacheFile      = "codex_path"
	codexInstallLockName    = "codex_install.lock"
	codexProbeTimeout       = 5 * time.Second
	codexInstallDiskExit    = 75
	codexInstallFailureExit = 76
)

var (
	codexInstallLockPollDelay  = 200 * time.Millisecond
	codexInstallLockStaleAfter = 30 * time.Minute
	codexInstallLockMaxWait    = 30 * time.Second
	codexRemoveAll             = os.RemoveAll
	errCodexBinaryNotFound     = errors.New("codex binary not found")
)

type codexInstallOptions struct {
	installerEnv     []string
	withInstallerEnv func(context.Context, func([]string) error) error
	upgradeCodex     bool
}

type codexInstallCmd struct {
	path string
	args []string
}

type codexInstallOrigin string

const (
	codexInstallOriginUnknown codexInstallOrigin = "unknown"
	codexInstallOriginSystem  codexInstallOrigin = "system-npm"
	codexInstallOriginManaged codexInstallOrigin = "managed-npm"
)

type codexUpgradeSource struct {
	origin      codexInstallOrigin
	codexPath   string
	npmPrefix   string
	displayName string
}

const codexInstallBootstrap = `set -eu

min_major="${CODEX_NODE_MIN_MAJOR:-16}"
target_major="${CODEX_NODE_MAJOR:-22}"
home_dir="${HOME:-}"
if [ -z "$home_dir" ]; then
  home_dir="$(cd ~ 2>/dev/null && pwd || true)"
fi
if [ -z "$home_dir" ]; then
  home_dir="$(pwd)"
fi
min_free_kb="${CODEX_PROXY_CODEX_INSTALL_MIN_FREE_KB:-524288}"

is_positive_integer() {
  case "${1:-}" in
    ''|*[!0-9]*) return 1 ;;
    *) [ "$1" -gt 0 ] ;;
  esac
}

print_codex_install_failure() {
  reason="$1"
  printf '\n%s\n' "============================================================" >&2
  printf '  %s\n' "CODEX CLI INSTALL FAILED" >&2
  printf '%s\n' "============================================================" >&2
  printf 'Reason: %s\n' "$reason" >&2
}

fail_codex_install() {
  reason="$1"
  code="${2:-1}"
  print_codex_install_failure "$reason"
  exit "$code"
}

existing_path_for_space_check() {
  path="$1"
  if [ -z "${path:-}" ]; then
    return 1
  fi
  while [ ! -e "$path" ]; do
    parent="$(dirname "$path")"
    if [ "$parent" = "$path" ]; then
      return 1
    fi
    path="$parent"
  done
  printf '%s\n' "$path"
}

available_kb_for_path() {
  path="$1"
  if ! command -v df >/dev/null 2>&1 || ! command -v awk >/dev/null 2>&1; then
    return 1
  fi
  existing="$(existing_path_for_space_check "$path" 2>/dev/null || true)"
  if [ -z "${existing:-}" ]; then
    return 1
  fi
  df -Pk "$existing" 2>/dev/null | awk 'NR==2 { print $4 }'
}

check_disk_space() {
  label="$1"
  path="$2"
  if ! is_positive_integer "$min_free_kb"; then
    return 0
  fi
  available_kb="$(available_kb_for_path "$path" 2>/dev/null || true)"
  if ! is_positive_integer "$available_kb"; then
    echo "warning: could not reliably check free disk space for $label ($path); continuing." >&2
    return 0
  fi
  if [ "$available_kb" -lt "$min_free_kb" ]; then
    need_mb=$(( (min_free_kb + 1023) / 1024 ))
    have_mb=$(( available_kb / 1024 ))
    fail_codex_install "Not enough disk space for $label ($path): ${have_mb} MiB available, need at least ${need_mb} MiB." 75
  fi
}

disk_space_failure_reason() {
  label="$1"
  path="$2"
  if ! is_positive_integer "$min_free_kb"; then
    return 1
  fi
  available_kb="$(available_kb_for_path "$path" 2>/dev/null || true)"
  if ! is_positive_integer "$available_kb"; then
    return 1
  fi
  if [ "$available_kb" -lt "$min_free_kb" ]; then
    need_mb=$(( (min_free_kb + 1023) / 1024 ))
    have_mb=$(( available_kb / 1024 ))
    printf 'Not enough disk space for %s (%s): %s MiB available, need at least %s MiB.' "$label" "$path" "$have_mb" "$need_mb"
    return 0
  fi
  return 1
}

fail_if_disk_space_low() {
  reason="$(disk_space_failure_reason "$1" "$2" 2>/dev/null || true)"
  if [ -n "${reason:-}" ]; then
    fail_codex_install "$reason" 75
  fi
}

fail_write_or_disk() {
  label="$1"
  path="$2"
  fallback="$3"
  fail_if_disk_space_low "$label" "$path"
  fail_codex_install "$fallback"
}

parse_major() {
  raw="$1"
  raw="${raw#v}"
  raw="${raw%%.*}"
  case "$raw" in
    ''|*[!0-9]*) echo ""; return ;;
    *) echo "$raw"; return ;;
  esac
}

parse_minor() {
  raw="$1"
  raw="${raw#v}"
  case "$raw" in
    *.*) raw="${raw#*.}" ;;
    *) echo ""; return ;;
  esac
  raw="${raw%%.*}"
  case "$raw" in
    ''|*[!0-9]*) echo ""; return ;;
    *) echo "$raw"; return ;;
  esac
}

resolve_glibc_version() {
  if command -v getconf >/dev/null 2>&1; then
    glibc_raw="$(getconf GNU_LIBC_VERSION 2>/dev/null || true)"
    case "$glibc_raw" in
      glibc\ *) printf '%s\n' "${glibc_raw#glibc }"; return ;;
    esac
  fi

  if command -v ldd >/dev/null 2>&1; then
    ldd_line="$(ldd --version 2>/dev/null | head -n 1 || true)"
    printf '%s\n' "$ldd_line" | sed -n 's/.* \([0-9][0-9]*\.[0-9][0-9]*\).*/\1/p' | head -n 1
    return
  fi

  printf '\n'
}

version_lt() {
  left="$1"
  right="$2"
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

is_wsl=false
case "$(uname -r 2>/dev/null || true)" in
  *[Mm]icrosoft*|*WSL*) is_wsl=true ;;
esac

node_root="${CODEX_NODE_INSTALL_ROOT:-$home_dir/.cache/codex-proxy/node}"
prefix="${CODEX_NPM_PREFIX:-$home_dir/.local/share/codex-proxy/npm-global}"
npm_cache_dir="${npm_config_cache:-${NPM_CONFIG_CACHE:-$home_dir/.npm}}"
check_disk_space "temporary directory" "${TMPDIR:-/tmp}"
check_disk_space "managed npm prefix" "$prefix"
check_disk_space "managed Node.js install root" "$node_root"
check_disk_space "npm cache" "$npm_cache_dir"

npm_usable_with_path() {
  npm_path="$1"
  npm_node_bin="$2"
  if [ ! -x "$npm_path" ]; then
    return 1
  fi
  if [ -n "${npm_node_bin:-}" ]; then
    PATH="$npm_node_bin:$PATH" "$npm_path" --version >/dev/null 2>&1 || return 1
    PATH="$npm_node_bin:$PATH" "$npm_path" prefix -g >/dev/null 2>&1 || return 1
  else
    "$npm_path" --version >/dev/null 2>&1 || return 1
    "$npm_path" prefix -g >/dev/null 2>&1 || return 1
  fi
}

download_local_node() {
  os_name="$(uname -s | tr '[:upper:]' '[:lower:]')"
  case "$os_name" in
    linux|darwin) ;;
    *) echo "unsupported OS: $os_name" >&2; exit 1 ;;
  esac

  arch_raw="$(uname -m)"
  case "$arch_raw" in
    x86_64|amd64) node_arch="x64" ;;
    aarch64|arm64) node_arch="arm64" ;;
    *) echo "unsupported architecture: $arch_raw" >&2; exit 1 ;;
  esac

  install_dir="$node_root/v${target_major}-${os_name}-${node_arch}"
  node_path="$install_dir/bin/node"
  npm_path="$install_dir/bin/npm"
  node_bin_candidate="$install_dir/bin"
  installed_major=""
  if [ -x "$node_path" ]; then
    installed_major="$(parse_major "$("$node_path" -v 2>/dev/null || true)")"
  fi
  needs_install=false
  if [ -z "$installed_major" ] || [ "$installed_major" -lt "$min_major" ]; then
    needs_install=true
  elif ! npm_usable_with_path "$npm_path" "$node_bin_candidate"; then
    echo "managed Node.js/npm install is missing or broken; reinstalling: $install_dir" >&2
    needs_install=true
  fi
  if $needs_install; then
    check_disk_space "temporary directory" "${TMPDIR:-/tmp}"
    tmp_dir="$(mktemp -d 2>/dev/null || true)"
    if [ -z "${tmp_dir:-}" ]; then
      fail_write_or_disk "temporary directory" "${TMPDIR:-/tmp}" "failed to create temporary directory for Node.js download"
    fi
    cleanup() {
      rm -rf "$tmp_dir"
    }
    trap cleanup EXIT INT TERM

    download_file() {
      src="$1"
      dest="$2"
      check_disk_space "download destination" "$dest"
      if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$src" -o "$dest" || fail_write_or_disk "download destination" "$dest" "failed to download $src"
      elif command -v wget >/dev/null 2>&1; then
        wget -qO "$dest" "$src" || fail_write_or_disk "download destination" "$dest" "failed to download $src"
      else
        echo "need curl or wget to download Node.js" >&2
        exit 1
      fi
    }

    resolve_official_node_version() {
      latest_shasums="$tmp_dir/latest-SHASUMS256.txt"
      download_file "https://nodejs.org/dist/latest-v${target_major}.x/SHASUMS256.txt" "$latest_shasums"
      latest_tarball="$(
        awk -v major="$target_major" -v os="$os_name" -v arch="$node_arch" '
          $2 ~ ("^node-v" major "\\.[0-9]+\\.[0-9]+-" os "-" arch "\\.tar\\.(gz|xz)$") {
            print $2
            exit
          }
        ' "$latest_shasums"
      )"
      if [ -z "$latest_tarball" ]; then
        echo "failed to resolve latest Node.js version for ${os_name}-${node_arch}" >&2
        exit 1
      fi
      latest_version="${latest_tarball#node-v}"
      latest_version="${latest_version%-${os_name}-${node_arch}.tar.gz}"
      latest_version="${latest_version%-${os_name}-${node_arch}.tar.xz}"
      if [ -z "$latest_version" ]; then
        echo "failed to parse latest Node.js version from ${latest_tarball}" >&2
        exit 1
      fi
      printf '%s\n' "$latest_version"
    }

    legacy_glibc=false
    if [ "$os_name" = "linux" ] && [ "$node_arch" = "x64" ]; then
      glibc_version="$(resolve_glibc_version)"
      if [ -n "$glibc_version" ] && version_lt "$glibc_version" "2.28"; then
        legacy_glibc=true
      fi
    fi

    if $legacy_glibc; then
      unofficial_index="$tmp_dir/unofficial-index.tab"
      download_file "https://unofficial-builds.nodejs.org/download/release/index.tab" "$unofficial_index"
      latest_version="$(
        awk -F '\t' -v major="$target_major" '
          $1 ~ ("^v" major "\\.") && $3 ~ /(^|,)linux-x64-glibc-217(,|$)/ {
            version=$1
            sub(/^v/, "", version)
            print version
            exit
          }
        ' "$unofficial_index"
      )"
      if [ -z "$latest_version" ]; then
        echo "failed to resolve legacy glibc Node.js version for major ${target_major}" >&2
        exit 1
      fi
      base_url="https://unofficial-builds.nodejs.org/download/release/v${latest_version}"
      tarball_pattern="node-v${latest_version}-${os_name}-${node_arch}-glibc-217"
    else
      latest_version="$(resolve_official_node_version)"
      base_url="https://nodejs.org/dist/v${latest_version}"
      tarball_pattern="node-v${latest_version}-${os_name}-${node_arch}"
    fi

    shasums_file="$tmp_dir/SHASUMS256.txt"
    download_file "$base_url/SHASUMS256.txt" "$shasums_file"

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

    archive_path="$tmp_dir/$tarball"
    download_file "$base_url/$tarball" "$archive_path"

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

    check_disk_space "managed Node.js install directory" "$install_dir"
    rm -rf "$install_dir" || fail_codex_install "failed to remove incomplete managed Node.js/npm install: $install_dir"
    check_disk_space "managed Node.js install directory" "$install_dir"
    mkdir -p "$install_dir" || fail_write_or_disk "managed Node.js install directory" "$install_dir" "failed to create managed Node.js/npm install directory: $install_dir"
    check_disk_space "managed Node.js extraction" "$install_dir"
    case "$tarball" in
      *.tar.gz) tar -xzf "$archive_path" --strip-components=1 -C "$install_dir" || fail_write_or_disk "managed Node.js extraction" "$install_dir" "failed to extract Node.js archive into $install_dir" ;;
      *.tar.xz) tar -xJf "$archive_path" --strip-components=1 -C "$install_dir" || fail_write_or_disk "managed Node.js extraction" "$install_dir" "failed to extract Node.js archive into $install_dir" ;;
      *)
        echo "unsupported Node.js archive format: $tarball" >&2
        exit 1
        ;;
    esac
    rm -rf "$tmp_dir"
  fi

  if ! npm_usable_with_path "$npm_path" "$node_bin_candidate"; then
    fail_codex_install "npm is not usable in managed Node.js install: $npm_path"
  fi
  node_bin_dir="$install_dir/bin"
  npm_cmd="$npm_path"
}

node_bin_dir=""
npm_cmd=""
used_system_node=false
if command -v node >/dev/null 2>&1 && command -v npm >/dev/null 2>&1; then
  node_found="$(command -v node)"
  skip_system=false
  # On WSL, Windows binaries under /mnt/ are visible via PATH interop but
  # npm would install wrong platform-specific optional dependencies.
  if $is_wsl; then
    case "$node_found" in
      /mnt/*) skip_system=true ;;
    esac
  fi
  if ! $skip_system; then
    node_major="$(parse_major "$(node -v 2>/dev/null || true)")"
    if [ -n "$node_major" ] && [ "$node_major" -ge "$min_major" ]; then
      node_bin_dir="$(dirname "$node_found")"
      npm_found="$(command -v npm)"
      if npm_usable_with_path "$npm_found" "$node_bin_dir"; then
        npm_cmd="$npm_found"
        used_system_node=true
      else
        echo "system npm is not usable; installing managed Node.js/npm..." >&2
      fi
    fi
  fi
fi

if [ -z "$npm_cmd" ]; then
  download_local_node
fi

check_disk_space "managed npm prefix" "$prefix"
mkdir -p "$prefix" || fail_write_or_disk "managed npm prefix" "$prefix" "failed to create managed npm prefix: $prefix"
check_disk_space "managed npm prefix" "$prefix"
check_disk_space "npm cache" "$npm_cache_dir"
check_disk_space "temporary directory" "${TMPDIR:-/tmp}"
if ! PATH="$node_bin_dir:$prefix/bin:$PATH" "$npm_cmd" install -g --prefix "$prefix" --include=optional @openai/codex; then
  fail_if_disk_space_low "managed npm prefix" "$prefix"
  fail_if_disk_space_low "npm cache" "$npm_cache_dir"
  fail_if_disk_space_low "temporary directory" "${TMPDIR:-/tmp}"
  fail_codex_install "npm install -g @openai/codex failed"
fi

if ! "$prefix/bin/codex" --version >/dev/null 2>&1 && $used_system_node; then
  echo "codex installed with system node is not functional; retrying with local node..." >&2
  download_local_node
  check_disk_space "managed npm prefix" "$prefix"
  check_disk_space "npm cache" "$npm_cache_dir"
  check_disk_space "temporary directory" "${TMPDIR:-/tmp}"
  if ! PATH="$node_bin_dir:$prefix/bin:$PATH" "$npm_cmd" install -g --prefix "$prefix" --include=optional @openai/codex; then
    fail_if_disk_space_low "managed npm prefix" "$prefix"
    fail_if_disk_space_low "npm cache" "$npm_cache_dir"
    fail_if_disk_space_low "temporary directory" "${TMPDIR:-/tmp}"
    fail_codex_install "npm install -g @openai/codex failed after switching to managed Node.js/npm"
  fi
fi
`

const codexInstallBootstrapWindows = `$ErrorActionPreference = 'Stop'

$minMajorRaw = [Environment]::GetEnvironmentVariable('CODEX_NODE_MIN_MAJOR')
if ([string]::IsNullOrWhiteSpace($minMajorRaw)) { $minMajorRaw = '16' }
$targetMajorRaw = [Environment]::GetEnvironmentVariable('CODEX_NODE_MAJOR')
if ([string]::IsNullOrWhiteSpace($targetMajorRaw)) { $targetMajorRaw = '22' }
$minMajor = [int]$minMajorRaw
$targetMajor = [int]$targetMajorRaw
$minFreeKBRaw = [Environment]::GetEnvironmentVariable('CODEX_PROXY_CODEX_INSTALL_MIN_FREE_KB')
if ([string]::IsNullOrWhiteSpace($minFreeKBRaw)) { $minFreeKBRaw = '524288' }
$minFreeKB = 524288L
$parsedMinFreeKB = 0L
if ([int64]::TryParse($minFreeKBRaw, [ref]$parsedMinFreeKB)) {
  $minFreeKB = $parsedMinFreeKB
}

function Write-CodexInstallFailure([string]$reason) {
  Write-Host ""
  Write-Host "============================================================" -ForegroundColor Red
  Write-Host "  CODEX CLI INSTALL FAILED" -ForegroundColor Red
  Write-Host "============================================================" -ForegroundColor Red
  Write-Host ("Reason: " + $reason) -ForegroundColor Red
}

function Fail-CodexInstall([string]$reason, [int]$code = 76) {
  Write-CodexInstallFailure $reason
  exit $code
}

function Get-NodeMajor([string]$nodeExe) {
  try {
    $out = & $nodeExe -v 2>$null
    if (-not $out) { return -1 }
    $v = $out.Trim().TrimStart('v')
    $major = $v.Split('.')[0]
    return [int]$major
  } catch {
    return -1
  }
}

$baseDir = [Environment]::GetFolderPath('LocalApplicationData')
if ([string]::IsNullOrWhiteSpace($baseDir)) {
  $baseDir = [IO.Path]::GetTempPath()
}

$nodeRoot = [Environment]::GetEnvironmentVariable('CODEX_NODE_INSTALL_ROOT')
if ([string]::IsNullOrWhiteSpace($nodeRoot)) {
  $nodeRoot = Join-Path $baseDir 'codex-proxy\node'
}
$npmPrefix = [Environment]::GetEnvironmentVariable('CODEX_NPM_PREFIX')
if ([string]::IsNullOrWhiteSpace($npmPrefix)) {
  $npmPrefix = Join-Path $baseDir 'codex-proxy\npm-global'
}
$npmCacheDir = [Environment]::GetEnvironmentVariable('npm_config_cache')
if ([string]::IsNullOrWhiteSpace($npmCacheDir)) {
  $npmCacheDir = [Environment]::GetEnvironmentVariable('NPM_CONFIG_CACHE')
}
if ([string]::IsNullOrWhiteSpace($npmCacheDir)) {
  $npmCacheDir = Join-Path $baseDir 'npm-cache'
}

function Get-ExistingPathForSpaceCheck([string]$pathValue) {
  if ([string]::IsNullOrWhiteSpace($pathValue)) { return $null }
  try {
    $candidate = [IO.Path]::GetFullPath($pathValue)
  } catch {
    return $null
  }
  while (-not [string]::IsNullOrWhiteSpace($candidate) -and -not (Test-Path -LiteralPath $candidate)) {
    $parent = Split-Path -Parent $candidate
    if ([string]::IsNullOrWhiteSpace($parent) -or $parent -eq $candidate) { return $null }
    $candidate = $parent
  }
  return $candidate
}

function Get-FreeBytesForPath([string]$pathValue) {
  $existing = Get-ExistingPathForSpaceCheck $pathValue
  if ([string]::IsNullOrWhiteSpace($existing)) { return $null }
  try {
    $root = [IO.Path]::GetPathRoot([IO.Path]::GetFullPath($existing))
    if ([string]::IsNullOrWhiteSpace($root)) { return $null }
    $drive = [System.IO.DriveInfo]::new($root)
    if (-not $drive.IsReady) { return $null }
    return [int64]$drive.AvailableFreeSpace
  } catch {
    return $null
  }
}

function Get-DiskSpaceFailureReason([string]$label, [string]$pathValue) {
  if ($minFreeKB -le 0) { return }
  $minBytes = $minFreeKB * 1024L
  $freeBytes = Get-FreeBytesForPath $pathValue
  if ($null -eq $freeBytes) { return $null }
  if ($freeBytes -lt $minBytes) {
    $haveMiB = [math]::Floor($freeBytes / 1MB)
    $needMiB = [math]::Ceiling($minBytes / 1MB)
    return "Not enough disk space for $label ($pathValue): $haveMiB MiB available, need at least $needMiB MiB."
  }
  return $null
}

function Test-DiskSpaceError([object]$errorRecord) {
  $text = ""
  if ($null -ne $errorRecord) {
    $text = $errorRecord.ToString()
    if ($errorRecord.Exception -and $errorRecord.Exception.Message) {
      $text = $text + [Environment]::NewLine + $errorRecord.Exception.Message
    }
  }
  return ($text -match '(?i)(no space left|not enough space|disk full|insufficient disk|quota)')
}

function Assert-DiskSpace([string]$label, [string]$pathValue) {
  if ($minFreeKB -le 0) { return }
  $reason = Get-DiskSpaceFailureReason $label $pathValue
  if (-not [string]::IsNullOrWhiteSpace($reason)) {
    Fail-CodexInstall $reason 75
  }
  if ($null -eq (Get-FreeBytesForPath $pathValue)) {
    Write-Warning "Could not reliably check free disk space for $label ($pathValue); continuing."
  }
}

function Fail-IfDiskSpaceLow([string]$label, [string]$pathValue) {
  $reason = Get-DiskSpaceFailureReason $label $pathValue
  if (-not [string]::IsNullOrWhiteSpace($reason)) {
    Fail-CodexInstall $reason 75
  }
}

function Invoke-DiskWrite([string]$label, [string]$pathValue, [string]$defaultReason, [scriptblock]$action) {
  Assert-DiskSpace $label $pathValue
  try {
    & $action
  } catch {
    $reason = Get-DiskSpaceFailureReason $label $pathValue
    if ([string]::IsNullOrWhiteSpace($reason) -and (Test-DiskSpaceError $_)) {
      $reason = "Not enough disk space for $label ($pathValue)."
    }
    if (-not [string]::IsNullOrWhiteSpace($reason)) {
      Fail-CodexInstall $reason 75
    }
    if (-not [string]::IsNullOrWhiteSpace($defaultReason)) {
      Fail-CodexInstall ($defaultReason + ": " + $_.Exception.Message)
    }
    throw
  }
}

function Test-NpmUsable([string]$npmPath, [string]$nodePathDir) {
  if ([string]::IsNullOrWhiteSpace($npmPath) -or -not (Test-Path $npmPath)) { return $false }
  $oldPath = $env:PATH
  try {
    if (-not [string]::IsNullOrWhiteSpace($nodePathDir)) {
      $env:PATH = "$nodePathDir;$oldPath"
    }
    & $npmPath --version 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { return $false }
    & $npmPath prefix -g 2>$null | Out-Null
    return ($LASTEXITCODE -eq 0)
  } catch {
    return $false
  } finally {
    $env:PATH = $oldPath
  }
}

function Get-CommonWindowsRuntimeDllStatus {
  $systemRoot = $env:SystemRoot
  if ([string]::IsNullOrWhiteSpace($systemRoot)) { return "" }

  $missing = @()
  foreach ($dll in @('VCRUNTIME140.dll', 'VCRUNTIME140_1.dll', 'api-ms-win-crt-runtime-l1-1-0.dll')) {
    $path = Join-Path (Join-Path $systemRoot 'System32') $dll
    if (-not (Test-Path -LiteralPath $path)) {
      $missing += $dll
    }
  }
  if ($missing.Count -eq 0) { return "" }
  return " Missing common runtime DLLs: $($missing -join ', ')."
}

function Resolve-CodexNativeRuntimeArch {
  $preferredArch = ''
  if (-not [string]::IsNullOrWhiteSpace($nodeDir)) {
    $leaf = (Split-Path -Leaf $nodeDir)
    if ($leaf -match '(?i)win-arm64') { $preferredArch = 'arm64' }
    if ($leaf -match '(?i)win-x64') { $preferredArch = 'x64' }
  }
  if ([string]::IsNullOrWhiteSpace($preferredArch)) {
    if ($env:PROCESSOR_ARCHITECTURE -match 'ARM64' -or $env:PROCESSOR_ARCHITEW6432 -match 'ARM64') {
      $preferredArch = 'arm64'
    } else {
      $preferredArch = 'x64'
    }
  }

  if (-not [string]::IsNullOrWhiteSpace($npmPrefix)) {
    $openaiRoot = Join-Path $npmPrefix 'node_modules\@openai'
    $arm64Checks = @(
      (Join-Path $openaiRoot 'codex-win32-arm64\vendor\aarch64-pc-windows-msvc\codex\codex.exe')
      (Join-Path $openaiRoot 'codex\node_modules\@openai\codex-win32-arm64\vendor\aarch64-pc-windows-msvc\codex\codex.exe')
      (Join-Path $openaiRoot 'codex\vendor\aarch64-pc-windows-msvc\codex\codex.exe')
    )
    $x64Checks = @(
      (Join-Path $openaiRoot 'codex-win32-x64\vendor\x86_64-pc-windows-msvc\codex\codex.exe')
      (Join-Path $openaiRoot 'codex\node_modules\@openai\codex-win32-x64\vendor\x86_64-pc-windows-msvc\codex\codex.exe')
      (Join-Path $openaiRoot 'codex\vendor\x86_64-pc-windows-msvc\codex\codex.exe')
    )
    $orderedArchs = @($preferredArch)
    if ($preferredArch -ne 'arm64') { $orderedArchs += 'arm64' }
    if ($preferredArch -ne 'x64') { $orderedArchs += 'x64' }
    foreach ($arch in $orderedArchs) {
      $checks = if ($arch -eq 'arm64') { $arm64Checks } else { $x64Checks }
      foreach ($checkPath in $checks) {
        if (Test-Path -LiteralPath $checkPath) {
          return $arch
        }
      }
    }
  }

  return $preferredArch
}

function Get-CodexVCRedistTarget {
  $arch = Resolve-CodexNativeRuntimeArch
  if ($arch -eq 'arm64') {
    return [pscustomobject]@{
      Arch = 'arm64'
      Display = 'ARM64'
      WingetId = 'Microsoft.VCRedist.2015+.arm64'
      DownloadUrl = 'https://aka.ms/vc14/vc_redist.arm64.exe'
      FileName = 'vc_redist.arm64.exe'
    }
  }
  return [pscustomobject]@{
    Arch = 'x64'
    Display = 'x64'
    WingetId = 'Microsoft.VCRedist.2015+.x64'
    DownloadUrl = 'https://aka.ms/vc14/vc_redist.x64.exe'
    FileName = 'vc_redist.x64.exe'
  }
}

function Get-NativeExitStatus([int64]$code) {
  if ($code -lt 0) {
    return [uint32]($code + 4294967296L)
  }
  return [uint32]$code
}

function Test-CodexNativeRuntimeRepairable([int64]$code) {
  $status = Get-NativeExitStatus $code
  return ($status -eq 0xC0000135 -or $status -eq 0xC0000139)
}

function Get-CodexNativeStartupFailureHint([int64]$code) {
  $status = Get-NativeExitStatus $code

  $redistTarget = Get-CodexVCRedistTarget
  $installHint = "run: winget install --id $($redistTarget.WingetId) -e"
  switch ($status) {
    0xC0000135 {
      return "Windows native Codex exited with STATUS_DLL_NOT_FOUND (0xC0000135). This usually means the Microsoft Visual C++ 2015-2022 Redistributable ($($redistTarget.Display)) or Universal CRT is missing; $installHint.$(Get-CommonWindowsRuntimeDllStatus)"
    }
    0xC0000139 {
      return "Windows native Codex exited with STATUS_ENTRYPOINT_NOT_FOUND (0xC0000139). This usually means a runtime DLL or Windows component is too old; install/update the Microsoft Visual C++ 2015-2022 Redistributable ($($redistTarget.Display)) and run Windows Update."
    }
    0xC000007B {
      return "Windows native Codex exited with STATUS_INVALID_IMAGE_FORMAT (0xC000007B). This usually means a wrong-architecture or corrupt native package; clear the managed Codex install and reinstall, and verify the Windows architecture."
    }
    0xC000001D {
      return "Windows native Codex exited with STATUS_ILLEGAL_INSTRUCTION (0xC000001D). This usually means the current native Codex build is not compatible with this CPU or Windows runtime; update Windows or use a compatible Codex version."
    }
  }
  return ""
}

function Confirm-CodexVCRedistInstall {
  $mode = [Environment]::GetEnvironmentVariable('CODEX_PROXY_VCREDIST_INSTALL')
  if ([string]::IsNullOrWhiteSpace($mode)) { $mode = 'auto' }
  $mode = $mode.Trim().ToLowerInvariant()
  if ($mode -in @('0', 'false', 'no', 'never', 'off', 'skip')) {
    return $false
  }
  if ($mode -in @('1', 'true', 'yes', 'always', 'auto')) {
    return $true
  }
  try {
    if ([Console]::IsInputRedirected) { return $false }
  } catch {
    return $false
  }

  Write-Host ""
  Write-Host "Codex needs the Microsoft Visual C++ 2015-2022 Redistributable." -ForegroundColor Yellow
  Write-Host "A Windows administrator permission prompt may appear."
  $answer = Read-Host "Install it now? [y/N]"
  return ($answer -match '^(?i:y|yes)$')
}

function Test-CodexVCRedistInstallerExitCode([int]$code) {
  return ($code -eq 0 -or $code -eq 3010 -or $code -eq 1638)
}

function Install-CodexVCRedistWithWinget {
  $redistTarget = Get-CodexVCRedistTarget
  $winget = Get-Command winget -ErrorAction SilentlyContinue
  if (-not $winget) { return $false }

  Write-Host "Installing Microsoft Visual C++ Redistributable ($($redistTarget.Display)) with winget..." -ForegroundColor Yellow
  try {
    $args = @(
      'install',
      '--id', $redistTarget.WingetId,
      '-e',
      '--accept-package-agreements',
      '--accept-source-agreements'
    )
    $proc = Start-Process -FilePath $winget.Source -ArgumentList $args -Verb RunAs -Wait -PassThru
    if ($proc -and (Test-CodexVCRedistInstallerExitCode $proc.ExitCode)) {
      return $true
    }
    if ($proc) {
      Write-Warning "winget VC++ runtime install exited with code $($proc.ExitCode)."
    }
  } catch {
    Write-Warning "winget VC++ runtime install failed: $($_.Exception.Message)"
  }
  return $false
}

function Install-CodexVCRedistFromMicrosoft {
  $redistTarget = Get-CodexVCRedistTarget
  $tmpDir = Join-Path ([IO.Path]::GetTempPath()) ("codex-vcredist-" + [Guid]::NewGuid().ToString('N'))
  $installer = Join-Path $tmpDir $redistTarget.FileName
  try {
    New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
    Write-Host "Downloading Microsoft Visual C++ Redistributable ($($redistTarget.Display)) from Microsoft..." -ForegroundColor Yellow
    Invoke-WebRequest -UseBasicParsing -Uri $redistTarget.DownloadUrl -OutFile $installer
    $sig = Get-AuthenticodeSignature -FilePath $installer
    if ($sig.Status -ne 'Valid' -or -not ($sig.SignerCertificate.Subject -match 'Microsoft')) {
      throw "downloaded VC++ runtime installer signature is not trusted"
    }
    $proc = Start-Process -FilePath $installer -ArgumentList @('/install', '/quiet', '/norestart') -Verb RunAs -Wait -PassThru
    if ($proc -and (Test-CodexVCRedistInstallerExitCode $proc.ExitCode)) {
      return $true
    }
    if ($proc) {
      Write-Warning "VC++ runtime installer exited with code $($proc.ExitCode)."
    }
  } catch {
    Write-Warning "Microsoft VC++ runtime install failed: $($_.Exception.Message)"
  } finally {
    Remove-Item -Recurse -Force $tmpDir -ErrorAction SilentlyContinue
  }
  return $false
}

function Install-CodexVCRedistIfNeeded {
  if (-not (Test-CodexNativeRuntimeRepairable $script:codexProbeNativeStartupStatus)) {
    return $false
  }
  if (-not (Confirm-CodexVCRedistInstall)) {
    return $false
  }

  if (Install-CodexVCRedistWithWinget) {
    return $true
  }
  return Install-CodexVCRedistFromMicrosoft
}

function Set-CodexManagedNodeShims {
  if ([string]::IsNullOrWhiteSpace($npmPrefix) -or [string]::IsNullOrWhiteSpace($nodeDir)) { return }
  $nodeExe = Join-Path $nodeDir 'node.exe'
  if (-not (Test-Path $nodeExe)) { return }

  $nodeLeaf = Split-Path -Leaf $nodeDir
  if ([string]::IsNullOrWhiteSpace($nodeLeaf)) { return }

  $codexCmd = Join-Path $npmPrefix 'codex.cmd'
  $codexJsRel = 'node_modules\@openai\codex\bin\codex.js'
  if (Test-Path $codexCmd) {
    try {
      $cmdText = Get-Content -LiteralPath $codexCmd -Raw
      $match = [regex]::Match($cmdText, '(?:%~?dp0%[\\/]|%~dp0)(?<rel>[^"]+\.js)')
      if ($match.Success -and -not [string]::IsNullOrWhiteSpace($match.Groups['rel'].Value)) {
        $codexJsRel = $match.Groups['rel'].Value.Replace('/', '\')
      }
    } catch {}
  }

  if (Test-Path $codexCmd) {
    $cmdShim = @(
      '@echo off',
      'setlocal',
      ('rem "%~dp0{0}"' -f $codexJsRel),
      'set "_codex_ps=%~dp0codex.ps1"',
      'set "_powershell=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"',
      'if not exist "%_powershell%" set "_powershell=powershell"',
      '"%_powershell%" -NoProfile -ExecutionPolicy Bypass -File "%_codex_ps%" %*',
      'exit /b %ERRORLEVEL%'
    )
    Invoke-DiskWrite "codex command shim" $codexCmd "failed to update codex command shim" {
      Set-Content -Path $codexCmd -Value $cmdShim -Encoding ASCII
    }
  }

  $codexPs1 = Join-Path $npmPrefix 'codex.ps1'
  if (Test-Path $codexPs1) {
    $nodeLeafLiteral = '"' + $nodeLeaf + '"'
    $codexJsRelLiteral = '"' + $codexJsRel + '"'
    $psShim = @(
      '$basedir = Split-Path $MyInvocation.MyCommand.Definition -Parent',
      ('$nodeLeaf = ' + $nodeLeafLiteral),
      '$nodeRoot = $env:CODEX_NODE_INSTALL_ROOT',
      'if ([string]::IsNullOrWhiteSpace($nodeRoot) -and -not [string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {',
      '  $nodeRoot = Join-Path $env:LOCALAPPDATA ''codex-proxy\node''',
      '}',
      '$nodePath = ''''',
      'if (-not [string]::IsNullOrWhiteSpace($nodeRoot)) {',
      '  $nodePath = Join-Path (Join-Path $nodeRoot $nodeLeaf) ''node.exe''',
      '}',
      'if ([string]::IsNullOrWhiteSpace($nodePath) -or -not (Test-Path $nodePath)) {',
      '  $nodePath = Join-Path $basedir (''..\node\'' + $nodeLeaf + ''\node.exe'')',
      '}',
      'if (-not (Test-Path $nodePath)) {',
      '  Write-Error "Managed Node.js not found: $nodePath"',
      '  exit 1',
      '}',
      ('$scriptPath = Join-Path $basedir ' + $codexJsRelLiteral),
      'if (-not (Test-Path $scriptPath)) {',
      '  Write-Error "Codex JS entrypoint not found: $scriptPath"',
      '  exit 1',
      '}',
      'if ($MyInvocation.ExpectingInput) {',
      '  $input | & $nodePath $scriptPath $args',
      '} else {',
      '  & $nodePath $scriptPath $args',
      '}',
      'exit $LASTEXITCODE'
    )
    Invoke-DiskWrite "codex PowerShell shim" $codexPs1 "failed to update codex PowerShell shim" {
      Set-Content -Path $codexPs1 -Value $psShim -Encoding UTF8
    }
  }
}

function Test-CodexCommand([string]$codexPath) {
  $script:codexProbeFailure = ""
  $script:codexProbeNativeStartupStatus = 0
  if ([string]::IsNullOrWhiteSpace($codexPath) -or -not (Test-Path $codexPath)) {
    $script:codexProbeFailure = "codex command not found: $codexPath"
    return $false
  }

  try {
    $probeOut = & $codexPath --version 2>&1
    $code = $LASTEXITCODE
    if ($code -eq 0) {
      return $true
    }
    $text = ($probeOut | Out-String).Trim()
    $hint = Get-CodexNativeStartupFailureHint $code
    if (-not [string]::IsNullOrWhiteSpace($hint)) {
      $script:codexProbeNativeStartupStatus = Get-NativeExitStatus $code
    }
    if (-not [string]::IsNullOrWhiteSpace($text)) {
      if (-not [string]::IsNullOrWhiteSpace($hint)) {
        $text = "$text; $hint"
      }
      $script:codexProbeFailure = "exit code ${code}: $text"
    } else {
      $script:codexProbeFailure = "exit code $code"
      if (-not [string]::IsNullOrWhiteSpace($hint)) {
        $script:codexProbeFailure = $script:codexProbeFailure + ": " + $hint
      }
    }
  } catch {
    $script:codexProbeFailure = $_.Exception.Message
  }
  return $false
}

function Get-CodexSHA256Hex([string]$path) {
  if (Get-Command Get-FileHash -ErrorAction SilentlyContinue) {
    return (Get-FileHash -Algorithm SHA256 -Path $path).Hash.ToLowerInvariant()
  }

  $stream = [System.IO.File]::OpenRead($path)
  try {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
      $hashBytes = $sha.ComputeHash($stream)
      return ([System.BitConverter]::ToString($hashBytes)).Replace('-', '').ToLowerInvariant()
    } finally {
      $sha.Dispose()
    }
  } finally {
    $stream.Dispose()
  }
}

Assert-DiskSpace "temporary directory" ([IO.Path]::GetTempPath())
Assert-DiskSpace "managed npm prefix" $npmPrefix
Assert-DiskSpace "managed Node.js install root" $nodeRoot
Assert-DiskSpace "npm cache" $npmCacheDir

function Install-LocalNode {
  $arch = 'x64'
  if ($env:PROCESSOR_ARCHITECTURE -match 'ARM64' -or $env:PROCESSOR_ARCHITEW6432 -match 'ARM64') {
    $arch = 'arm64'
  }
  $installDir = Join-Path $nodeRoot ("v{0}-win-{1}" -f $targetMajor, $arch)
  $nodeExe = Join-Path $installDir 'node.exe'
  $localNpmCmd = Join-Path $installDir 'npm.cmd'

  $needsInstall = $false
  if (-not (Test-Path $nodeExe) -or ((Get-NodeMajor $nodeExe) -lt $minMajor)) {
    $needsInstall = $true
  } elseif (-not (Test-NpmUsable $localNpmCmd $installDir)) {
    Write-Host "managed Node.js/npm install is missing or broken; reinstalling: $installDir" -ForegroundColor Yellow
    $needsInstall = $true
  }

  if ($needsInstall) {
    $tmpDir = Join-Path ([IO.Path]::GetTempPath()) ("codex-node-" + [Guid]::NewGuid().ToString('N'))
    Invoke-DiskWrite "temporary directory" $tmpDir "failed to create temporary directory for Node.js download" {
      New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
    }
    try {
      $baseUrl = "https://nodejs.org/dist/latest-v$targetMajor.x"
      $shasumsPath = Join-Path $tmpDir 'SHASUMS256.txt'
      Invoke-DiskWrite "Node.js checksum download" $shasumsPath "failed to download Node.js checksums" {
        Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/SHASUMS256.txt" -OutFile $shasumsPath
      }

      $pattern = " node-v$targetMajor\.\d+\.\d+-win-$arch\.zip$"
      $line = Get-Content $shasumsPath | Where-Object { $_ -match $pattern } | Select-Object -First 1
      if ([string]::IsNullOrWhiteSpace($line)) {
        throw "failed to resolve Node.js zip for win-$arch"
      }
      $parts = $line -split '\s+', 3
      $expected = $parts[0].ToLower()
      $zipName = $parts[1]
      $zipPath = Join-Path $tmpDir $zipName
      Invoke-DiskWrite "Node.js archive download" $zipPath "failed to download Node.js archive" {
        Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/$zipName" -OutFile $zipPath
      }
      $actual = Get-CodexSHA256Hex $zipPath
      if ($actual -ne $expected) {
        throw "Node.js checksum mismatch for $zipName"
      }

      $extractRoot = Join-Path $tmpDir 'extract'
      Invoke-DiskWrite "Node.js archive extraction" $extractRoot "failed to extract Node.js archive" {
        Expand-Archive -Path $zipPath -DestinationPath $extractRoot -Force
      }
      $expanded = Get-ChildItem -Path $extractRoot -Directory | Select-Object -First 1
      if (-not $expanded) {
        throw "failed to extract Node.js archive"
      }
      if (Test-Path $installDir) {
        Remove-Item -Recurse -Force $installDir
      }
      $installParent = Split-Path -Parent $installDir
      Invoke-DiskWrite "managed Node.js install root" $installParent "failed to create managed Node.js install root" {
        New-Item -ItemType Directory -Force -Path $installParent | Out-Null
      }
      Invoke-DiskWrite "managed Node.js install directory" $installDir "failed to install managed Node.js" {
        Move-Item -Path $expanded.FullName -Destination $installDir -Force
      }
    } finally {
      Remove-Item -Recurse -Force $tmpDir -ErrorAction SilentlyContinue
    }
  }

  $script:nodeDir = $installDir
  $script:npmCmd = $localNpmCmd
  if (-not (Test-NpmUsable $script:npmCmd $script:nodeDir)) {
    Fail-CodexInstall "npm is not usable in managed Node.js install: $script:npmCmd"
  }
}

$nodeDir = $null
$npmCmd = $null
$usedSystemNode = $false
$usingManagedNode = $false
$systemNode = Get-Command node -ErrorAction SilentlyContinue
$systemNpm = Get-Command npm -ErrorAction SilentlyContinue
if ($systemNode -and $systemNpm) {
  $major = Get-NodeMajor $systemNode.Source
  if ($major -ge $minMajor) {
    $nodeDir = Split-Path -Parent $systemNode.Source
    if (Test-NpmUsable $systemNpm.Source $nodeDir) {
      $npmCmd = $systemNpm.Source
      $usedSystemNode = $true
    } else {
      Write-Host "system npm is not usable; installing managed Node.js/npm..." -ForegroundColor Yellow
    }
  }
}

if (-not $npmCmd) {
  Install-LocalNode
  $usingManagedNode = $true
}

if (-not (Test-NpmUsable $npmCmd $nodeDir)) {
  Fail-CodexInstall "npm is not available for Codex install"
}

Invoke-DiskWrite "managed npm prefix" $npmPrefix "failed to create managed npm prefix" {
  New-Item -ItemType Directory -Force -Path $npmPrefix | Out-Null
}
$prefixBin = Join-Path $npmPrefix 'bin'
$env:PATH = "$nodeDir;$npmPrefix;$prefixBin;$env:PATH"
Assert-DiskSpace "managed npm prefix" $npmPrefix
Assert-DiskSpace "npm cache" $npmCacheDir
Assert-DiskSpace "temporary directory" ([IO.Path]::GetTempPath())
& $npmCmd install -g --prefix $npmPrefix --include=optional @openai/codex
if ($LASTEXITCODE -ne 0) {
  Fail-IfDiskSpaceLow "managed npm prefix" $npmPrefix
  Fail-IfDiskSpaceLow "npm cache" $npmCacheDir
  Fail-IfDiskSpaceLow "temporary directory" ([IO.Path]::GetTempPath())
  Fail-CodexInstall "npm install -g @openai/codex failed" $LASTEXITCODE
}
if ($usingManagedNode) {
  Set-CodexManagedNodeShims
}

$codexCmd = Join-Path $npmPrefix 'codex.cmd'
$probeOk = Test-CodexCommand $codexCmd
if (-not $probeOk -and $usedSystemNode) {
  Write-Host "codex installed with system node is not functional; retrying with local node..." -ForegroundColor Yellow
  Install-LocalNode
  $usingManagedNode = $true
  if (-not (Test-NpmUsable $npmCmd $nodeDir)) {
    Fail-CodexInstall "npm is not available for Codex install"
  }
  $env:PATH = "$nodeDir;$npmPrefix;$prefixBin;$env:PATH"
  Assert-DiskSpace "managed npm prefix" $npmPrefix
  Assert-DiskSpace "npm cache" $npmCacheDir
  Assert-DiskSpace "temporary directory" ([IO.Path]::GetTempPath())
  & $npmCmd install -g --prefix $npmPrefix --include=optional @openai/codex
  if ($LASTEXITCODE -ne 0) {
    Fail-IfDiskSpaceLow "managed npm prefix" $npmPrefix
    Fail-IfDiskSpaceLow "npm cache" $npmCacheDir
    Fail-IfDiskSpaceLow "temporary directory" ([IO.Path]::GetTempPath())
    Fail-CodexInstall "npm install -g @openai/codex failed after switching to managed Node.js/npm" $LASTEXITCODE
  }
  Set-CodexManagedNodeShims
  $probeOk = Test-CodexCommand $codexCmd
}
if (-not $probeOk -and (Install-CodexVCRedistIfNeeded)) {
  Write-Host "Rechecking Codex CLI after VC++ runtime install..." -ForegroundColor Yellow
  $probeOk = Test-CodexCommand $codexCmd
}
if (-not $probeOk) {
  if ([string]::IsNullOrWhiteSpace($script:codexProbeFailure)) {
    $script:codexProbeFailure = "unknown probe failure"
  }
  Fail-CodexInstall "codex installation finished but $codexCmd is not functional ($script:codexProbeFailure)"
}
`

// probeCodex runs a quick smoke test to verify the codex binary is functional.
// Returns true if `codex --version` exits 0 within 5 seconds.
func probeCodex(ctx context.Context, codexPath string) bool {
	return probeCodexVersion(ctx, codexPath) == nil
}

func probeCodexVersion(ctx context.Context, codexPath string) error {
	ctx, cancel := context.WithTimeout(ctx, codexProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, codexPath, "--version")
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return fmt.Errorf("--version timed out after %s", codexProbeTimeout)
	}
	if err != nil {
		output := summarizeProbeOutput(out)
		hint := ""
		if exitCode, ok := commandExitCode(err); ok {
			hint = codexProbeFailureHintForExitCode(exitCode)
		}
		if output != "" {
			if hint != "" {
				output += " (" + hint + ")"
			}
			return fmt.Errorf("--version failed: %w: %s", err, output)
		}
		if hint != "" {
			return fmt.Errorf("--version failed: %w: %s", err, hint)
		}
		return fmt.Errorf("--version failed: %w", err)
	}
	return nil
}

func summarizeProbeOutput(out []byte) string {
	text := strings.Join(strings.Fields(strings.TrimSpace(string(out))), " ")
	const maxLen = 500
	if len(text) > maxLen {
		text = text[:maxLen] + "..."
	}
	return text
}

func codexProbeFailureHintForExitCode(exitCode int) string {
	switch uint32(exitCode) {
	case 0xC0000135:
		return "Windows native Codex exited with STATUS_DLL_NOT_FOUND (0xC0000135); install the Microsoft Visual C++ 2015-2022 Redistributable that matches Codex architecture (x64: winget install --id Microsoft.VCRedist.2015+.x64 -e; ARM64: winget install --id Microsoft.VCRedist.2015+.arm64 -e), then rerun cxp"
	case 0xC0000139:
		return "Windows native Codex exited with STATUS_ENTRYPOINT_NOT_FOUND (0xC0000139); install/update Microsoft Visual C++ 2015-2022 Redistributable and run Windows Update, then rerun cxp"
	case 0xC000007B:
		return "Windows native Codex exited with STATUS_INVALID_IMAGE_FORMAT (0xC000007B); clear the managed Codex install, reinstall, and verify the Windows architecture"
	case 0xC000001D:
		return "Windows native Codex exited with STATUS_ILLEGAL_INSTRUCTION (0xC000001D); update Windows or use a CPU-compatible Codex version"
	default:
		return ""
	}
}

func codexPostInstallError(action string, err error) error {
	if err == nil || errors.Is(err, errCodexBinaryNotFound) {
		return fmt.Errorf("codex %s finished but binary not found in PATH", action)
	}
	return fmt.Errorf("codex %s finished but installed binary is not functional: %w", action, err)
}

func ensureCodexInstalled(ctx context.Context, codexPath string, out io.Writer) (string, error) {
	return ensureCodexInstalledWithOptions(ctx, codexPath, out, codexInstallOptions{})
}

func ensureCodexInstalledWithOptions(ctx context.Context, codexPath string, out io.Writer, opts codexInstallOptions) (string, error) {
	if opts.upgradeCodex {
		if strings.TrimSpace(codexPath) != "" {
			return "", fmt.Errorf("--upgrade-codex cannot be used with --codex-path")
		}
		return upgradeCodexInstalledWithOptions(ctx, out, opts)
	}

	ensureManagedNodeOnPath()

	if strings.TrimSpace(codexPath) != "" {
		resolvedPath := normalizeExecutablePath(codexPath)
		if executableExists(resolvedPath) {
			if err := probeCodexVersion(ctx, resolvedPath); err == nil {
				writeCachedCodexPath(resolvedPath)
				return resolvedPath, nil
			} else {
				return "", fmt.Errorf("codex at %s is not functional: %w", resolvedPath, err)
			}
		}
		return "", fmt.Errorf("codex not found at %s", codexPath)
	}

	if path, err := exec.LookPath("codex"); err == nil {
		path = normalizeExecutablePath(path)
		probeErr := probeCodexVersion(ctx, path)
		if probeErr == nil {
			writeCachedCodexPath(path)
			return path, nil
		}
		if out != nil {
			_, _ = fmt.Fprintf(out, "codex at %s is not functional (%v); installing a local copy...\n", path, probeErr)
		}
	}

	if cached := strings.TrimSpace(readCachedCodexPath()); cached != "" {
		if !filepath.IsAbs(cached) {
			clearCachedCodexPath()
		} else if executableExists(cached) && probeCodex(ctx, cached) {
			writeCachedCodexPath(cached)
			return cached, nil
		} else {
			clearCachedCodexPath()
		}
	}

	if path, err := findInstalledCodexInCandidates(ctx); err == nil {
		writeCachedCodexPath(path)
		return path, nil
	}

	if out != nil {
		_, _ = fmt.Fprintln(out, "codex not found; installing...")
	}

	var installedPath string
	if err := withCodexInstallLock(ctx, out, func() error {
		// Another process may have installed Codex while we waited for the lock.
		if path, err := findInstalledCodex(ctx); err == nil {
			installedPath = path
			return nil
		}

		runInstall := func(installerEnv []string) error {
			return runCodexInstaller(ctx, out, installerEnv)
		}
		if opts.withInstallerEnv != nil {
			if err := opts.withInstallerEnv(ctx, runInstall); err != nil {
				return err
			}
		} else {
			if err := runInstall(opts.installerEnv); err != nil {
				return err
			}
		}

		path, err := findInstalledCodex(ctx)
		if err != nil {
			return codexPostInstallError("installation", err)
		}
		installedPath = path
		return nil
	}); err != nil {
		return "", err
	}

	if installedPath != "" {
		writeCachedCodexPath(installedPath)
		return installedPath, nil
	}
	return "", codexPostInstallError("installation", nil)
}

func upgradeCodexInstalledWithOptions(ctx context.Context, out io.Writer, opts codexInstallOptions) (string, error) {
	ensureManagedNodeOnPath()

	var upgradedPath string
	if err := withCodexInstallLock(ctx, out, func() error {
		source, err := detectCodexUpgradeSource(ctx, opts.installerEnv)
		if err != nil {
			return err
		}
		if source.origin == codexInstallOriginUnknown {
			return fmt.Errorf("cannot determine codex installation origin; refusing automatic upgrade")
		}
		if out != nil {
			_, _ = fmt.Fprintf(out, "upgrading codex (%s)...\n", source.displayName)
		}
		if err := cleanupStaleCodexRetiredPathsForSource(out, source); err != nil {
			return err
		}

		runUpgrade := func(installerEnv []string) error {
			return runCodexUpgradeBySource(ctx, out, installerEnv, source)
		}
		if opts.withInstallerEnv != nil {
			if err := opts.withInstallerEnv(ctx, runUpgrade); err != nil {
				return err
			}
		} else {
			if err := runUpgrade(opts.installerEnv); err != nil {
				return err
			}
		}

		path, err := resolveUpgradedCodexPath(ctx, source.codexPath)
		if err != nil {
			return codexPostInstallError("upgrade", err)
		}
		upgradedPath = path
		return nil
	}); err != nil {
		return "", err
	}

	if upgradedPath == "" {
		return "", codexPostInstallError("upgrade", nil)
	}
	writeCachedCodexPath(upgradedPath)
	return upgradedPath, nil
}

func detectCodexUpgradeSource(ctx context.Context, installerEnv []string) (codexUpgradeSource, error) {
	return detectCodexUpgradeSourceForPath(ctx, "", installerEnv)
}

func detectCodexUpgradeSourceForPath(ctx context.Context, codexPath string, installerEnv []string) (codexUpgradeSource, error) {
	var err error
	if strings.TrimSpace(codexPath) == "" {
		codexPath, err = findInstalledCodexWithoutProbe()
		if err != nil {
			return codexUpgradeSource{}, fmt.Errorf("codex is not installed; cannot upgrade")
		}
	}
	codexPath = normalizeExecutablePath(codexPath)
	if codexPath == "" {
		return codexUpgradeSource{}, fmt.Errorf("codex is not installed; cannot upgrade")
	}

	if prefix, ok := managedCodexPrefixForPath(codexPath, installerEnv); ok {
		return codexUpgradeSource{
			origin:      codexInstallOriginManaged,
			codexPath:   codexPath,
			npmPrefix:   prefix,
			displayName: "managed npm",
		}, nil
	}

	systemPrefix, err := npmGlobalPrefix(ctx, installerEnv)
	if err == nil && pathWithinDir(codexPath, systemPrefix) {
		return codexUpgradeSource{
			origin:      codexInstallOriginSystem,
			codexPath:   codexPath,
			npmPrefix:   systemPrefix,
			displayName: "system npm",
		}, nil
	}

	return codexUpgradeSource{
		origin:      codexInstallOriginUnknown,
		codexPath:   codexPath,
		displayName: "unknown source",
	}, nil
}

func cleanupStaleCodexRetiredPathsForSource(out io.Writer, source codexUpgradeSource) error {
	targets := codexRetiredPathTargets(source)
	for _, target := range targets {
		retired := codexRetirePath(target)
		if retired == "" || retired == target {
			continue
		}
		if _, err := os.Lstat(retired); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("inspect stale npm retired path %s: %w", retired, err)
		}
		if err := codexRemoveAll(retired); err != nil {
			return fmt.Errorf("remove stale npm retired path %s: %w", retired, err)
		}
		if out != nil {
			_, _ = fmt.Fprintf(out, "removed stale npm update backup: %s\n", retired)
		}
	}
	return nil
}

func codexRetiredPathTargets(source codexUpgradeSource) []string {
	targets := make([]string, 0, 6)
	seen := map[string]struct{}{}
	add := func(path string) {
		path = normalizeExecutablePath(path)
		if path == "" {
			return
		}
		key := path
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		targets = append(targets, path)
	}

	if source.npmPrefix != "" {
		add(codexPackageDirForPrefix(source.npmPrefix))
		for _, path := range codexBinCandidatesForPrefix(source.npmPrefix) {
			add(path)
		}
	}
	add(source.codexPath)
	return targets
}

func codexPackageDirForPrefix(prefix string) string {
	return codexPackageDirForPrefixForOS(runtime.GOOS, prefix)
}

func codexPackageDirForPrefixForOS(goos, prefix string) string {
	prefix = normalizeExecutablePath(prefix)
	if prefix == "" {
		return ""
	}
	if strings.EqualFold(goos, "windows") {
		return filepath.Join(prefix, "node_modules", "@openai", "codex")
	}
	return filepath.Join(prefix, "lib", "node_modules", "@openai", "codex")
}

func codexBinCandidatesForPrefix(prefix string) []string {
	return codexBinCandidatesForPrefixForOS(runtime.GOOS, prefix)
}

func codexBinCandidatesForPrefixForOS(goos, prefix string) []string {
	prefix = normalizeExecutablePath(prefix)
	if prefix == "" {
		return nil
	}
	if strings.EqualFold(goos, "windows") {
		return []string{
			filepath.Join(prefix, "codex"),
			filepath.Join(prefix, "codex.cmd"),
			filepath.Join(prefix, "codex.ps1"),
			filepath.Join(prefix, "codex.exe"),
			filepath.Join(prefix, "bin", "codex"),
			filepath.Join(prefix, "bin", "codex.cmd"),
			filepath.Join(prefix, "bin", "codex.ps1"),
			filepath.Join(prefix, "bin", "codex.exe"),
		}
	}
	return []string{
		filepath.Join(prefix, "bin", "codex"),
	}
}

func codexRetirePath(path string) string {
	path = normalizeExecutablePath(path)
	if path == "" {
		return ""
	}

	sum := sha1.Sum([]byte(path))
	encoded := base64.StdEncoding.EncodeToString(sum[:])
	var slug strings.Builder
	slug.Grow(8)
	for _, ch := range encoded {
		if ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') || ('0' <= ch && ch <= '9') {
			slug.WriteRune(ch)
			if slug.Len() == 8 {
				break
			}
		}
	}
	if slug.Len() == 0 {
		return ""
	}
	return filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+"-"+slug.String())
}

func runCodexUpgradeBySource(ctx context.Context, out io.Writer, installerEnv []string, source codexUpgradeSource) error {
	switch source.origin {
	case codexInstallOriginManaged:
		if strings.TrimSpace(source.npmPrefix) == "" {
			return fmt.Errorf("managed codex install path is missing npm prefix")
		}
		envWithPrefix := setEnvValue(installerEnv, "CODEX_NPM_PREFIX", source.npmPrefix)
		return runCodexInstaller(ctx, out, envWithPrefix)
	case codexInstallOriginSystem:
		if err := ensureCodexInstallDiskSpaceForTargets(out, installerEnv, codexInstallDiskTargets(installerEnv, []codexInstallDiskTarget{
			{label: "system npm prefix", path: source.npmPrefix},
		}, false)); err != nil {
			return err
		}
		return runSystemNpmCodexUpgrade(ctx, out, installerEnv)
	default:
		return fmt.Errorf("cannot determine codex installation origin; refusing automatic upgrade")
	}
}

func runSystemNpmCodexUpgrade(ctx context.Context, out io.Writer, installerEnv []string) error {
	npmPath, err := exec.LookPath("npm")
	if err != nil {
		return fmt.Errorf("npm not found in PATH: %w", err)
	}
	var diskTargets []codexInstallDiskTarget
	if prefix, prefixErr := npmGlobalPrefix(ctx, installerEnv); prefixErr == nil {
		diskTargets = append(diskTargets, codexInstallDiskTarget{label: "system npm prefix", path: prefix})
	}
	if err := ensureCodexInstallDiskSpaceForTargets(out, installerEnv, codexInstallDiskTargets(installerEnv, diskTargets, false)); err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, npmPath, "install", "-g", "--include=optional", "@openai/codex")
	if len(installerEnv) > 0 {
		cmd.Env = installerEnv
	}
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		if diskErr := ensureCodexInstallDiskSpaceForTargets(out, installerEnv, codexInstallDiskTargets(installerEnv, diskTargets, false)); diskErr != nil {
			return diskErr
		}
		return fmt.Errorf("system npm codex upgrade failed: %w", err)
	}
	return nil
}

func resolveUpgradedCodexPath(ctx context.Context, preferred string) (string, error) {
	preferred = normalizeExecutablePath(preferred)
	if preferred != "" && executableExists(preferred) && probeCodex(ctx, preferred) {
		return preferred, nil
	}
	return findInstalledCodex(ctx)
}

func findInstalledCodexWithoutProbe() (string, error) {
	ensureManagedNodeOnPath()

	if path, err := exec.LookPath("codex"); err == nil {
		path = normalizeExecutablePath(path)
		if executableExists(path) {
			return path, nil
		}
	}

	if cached := strings.TrimSpace(readCachedCodexPath()); cached != "" {
		cached = normalizeExecutablePath(cached)
		if filepath.IsAbs(cached) && executableExists(cached) {
			return cached, nil
		}
	}

	for _, candidate := range codexBinaryCandidates() {
		if executableExists(candidate) {
			return normalizeExecutablePath(candidate), nil
		}
	}
	return "", fmt.Errorf("codex not installed")
}

func managedCodexPrefixForPath(codexPath string, installerEnv []string) (string, bool) {
	for _, prefix := range managedCodexPrefixCandidates(installerEnv) {
		if pathWithinDir(codexPath, prefix) {
			return filepath.Clean(prefix), true
		}
	}
	return inferManagedPrefixFromPath(codexPath)
}

func managedCodexPrefixCandidates(installerEnv []string) []string {
	out := make([]string, 0, 4)
	seen := map[string]struct{}{}
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		path = normalizeExecutablePath(path)
		if path == "" {
			return
		}
		key := path
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, path)
	}

	add(envValue(installerEnv, "CODEX_NPM_PREFIX"))
	if runtime.GOOS == "windows" {
		localAppData := strings.TrimSpace(envValue(installerEnv, "LOCALAPPDATA"))
		if localAppData == "" {
			localAppData = strings.TrimSpace(envTempDir(installerEnv))
		}
		if localAppData != "" {
			add(filepath.Join(localAppData, "codex-proxy", "npm-global"))
		}
	} else if home := strings.TrimSpace(envValue(installerEnv, "HOME")); home != "" {
		add(filepath.Join(home, ".local", "share", "codex-proxy", "npm-global"))
	} else {
		if home := preferredHomeDir(); home != "" {
			add(filepath.Join(home, ".local", "share", "codex-proxy", "npm-global"))
		}
	}

	return out
}

func inferManagedPrefixFromPath(path string) (string, bool) {
	path = normalizeExecutablePath(path)
	if path == "" {
		return "", false
	}

	asSlash := filepath.ToSlash(path)
	search := "/codex-proxy/npm-global"
	lookup := asSlash
	if runtime.GOOS == "windows" {
		lookup = strings.ToLower(asSlash)
	}
	idx := strings.Index(lookup, search)
	if idx < 0 {
		return "", false
	}
	prefix := asSlash[:idx+len(search)]
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "", false
	}
	return normalizeExecutablePath(filepath.FromSlash(prefix)), true
}

func npmGlobalPrefix(ctx context.Context, installerEnv []string) (string, error) {
	npmPath, err := exec.LookPath("npm")
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, npmPath, "prefix", "-g")
	if len(installerEnv) > 0 {
		cmd.Env = installerEnv
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("resolve npm global prefix: %w", err)
	}
	prefix := normalizeExecutablePath(strings.TrimSpace(string(out)))
	if prefix == "" {
		return "", fmt.Errorf("resolve npm global prefix: empty output")
	}
	return prefix, nil
}

func pathWithinDir(path string, dir string) bool {
	path = normalizeExecutablePath(path)
	dir = normalizeExecutablePath(dir)
	if path == "" || dir == "" {
		return false
	}

	p := path
	d := dir
	if runtime.GOOS == "windows" {
		p = strings.ToLower(p)
		d = strings.ToLower(d)
	}

	if p == d {
		return true
	}
	if !strings.HasSuffix(d, string(os.PathSeparator)) {
		d += string(os.PathSeparator)
	}
	return strings.HasPrefix(p, d)
}

func setEnvValue(base []string, key, value string) []string {
	env := make([]string, 0, len(base)+1)
	if len(base) == 0 {
		env = append(env, os.Environ()...)
	} else {
		env = append(env, base...)
	}

	out := make([]string, 0, len(env)+1)
	replaced := false
	for _, kv := range env {
		k, _, ok := strings.Cut(kv, "=")
		if !ok {
			out = append(out, kv)
			continue
		}
		if envKeyEqual(k, key) {
			if !replaced {
				out = append(out, key+"="+value)
				replaced = true
			}
			continue
		}
		out = append(out, kv)
	}
	if !replaced {
		out = append(out, key+"="+value)
	}
	return out
}

func envValue(env []string, key string) string {
	for i := len(env) - 1; i >= 0; i-- {
		k, v, ok := strings.Cut(env[i], "=")
		if !ok {
			continue
		}
		if envKeyEqual(k, key) {
			return v
		}
	}
	return os.Getenv(key)
}

func envKeyEqual(a, b string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}
	return a == b
}

func envTempDir(env []string) string {
	for _, key := range []string{"TMPDIR", "TEMP", "TMP"} {
		if v := strings.TrimSpace(envValue(env, key)); v != "" {
			return v
		}
	}
	return os.TempDir()
}

func withCodexInstallLock(ctx context.Context, out io.Writer, fn func() error) error {
	lockPath := codexInstallLockPath()
	if lockPath == "" {
		return fn()
	}
	lockDir := filepath.Dir(lockPath)
	if err := ensureCodexInstallDiskSpace(out, nil, []codexInstallDiskTarget{
		{label: "codex install lock directory", path: lockDir},
	}); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		if diskErr := ensureCodexInstallDiskSpace(out, nil, []codexInstallDiskTarget{
			{label: "codex install lock directory", path: lockDir},
		}); diskErr != nil {
			return diskErr
		}
		return fmt.Errorf("create codex install lock dir: %w", err)
	}
	waitStart := time.Time{}
	for {
		err := os.Mkdir(lockPath, 0o700)
		if err == nil {
			defer func() { _ = os.Remove(lockPath) }()
			return fn()
		}
		if !codexInstallLockIsContended(lockPath, err) {
			return fmt.Errorf("acquire codex install lock: %w", err)
		}
		stale, staleErr := codexInstallLockStale(lockPath)
		if staleErr == nil && stale {
			if rmErr := os.Remove(lockPath); rmErr == nil || os.IsNotExist(rmErr) {
				continue
			}
		}
		if waitStart.IsZero() {
			waitStart = time.Now()
			if out != nil {
				_, _ = fmt.Fprintf(out, "codex installation lock is held by another process; waiting up to %s...\n", codexInstallLockMaxWait)
			}
		}
		if time.Since(waitStart) >= codexInstallLockMaxWait {
			if out != nil {
				_, _ = fmt.Fprintf(out, "codex installation lock still held after %s; continuing without lock.\n", codexInstallLockMaxWait)
			}
			return fn()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(codexInstallLockPollDelay):
		}
	}
}

func codexInstallLockIsContended(lockPath string, err error) bool {
	if err == nil {
		return false
	}
	if os.IsExist(err) || os.IsPermission(err) {
		return true
	}
	_, statErr := os.Stat(lockPath)
	return statErr == nil
}

func codexInstallLockStale(lockPath string) (bool, error) {
	info, err := os.Stat(lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return time.Since(info.ModTime()) > codexInstallLockStaleAfter, nil
}

func codexInstallLockPath() string {
	cacheFile := cachedCodexPathFile()
	if cacheFile != "" {
		return filepath.Join(filepath.Dir(cacheFile), codexInstallLockName)
	}
	if tmp := strings.TrimSpace(os.TempDir()); tmp != "" {
		return filepath.Join(tmp, "codex-proxy", codexInstallLockName)
	}
	return ""
}

func normalizeExecutablePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if !filepath.IsAbs(path) {
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
	}
	return filepath.Clean(path)
}

func cachedCodexPathFile() string {
	base, err := os.UserCacheDir()
	if err != nil || strings.TrimSpace(base) == "" {
		home := preferredHomeDir()
		if home == "" {
			return ""
		}
		base = filepath.Join(home, ".cache")
	}
	return filepath.Join(base, "codex-proxy", codexPathCacheFile)
}

func readCachedCodexPath() string {
	cacheFile := cachedCodexPathFile()
	if cacheFile == "" {
		return ""
	}
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func writeCachedCodexPath(path string) {
	path = normalizeExecutablePath(path)
	if path == "" || !filepath.IsAbs(path) {
		return
	}
	cacheFile := cachedCodexPathFile()
	if cacheFile == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(cacheFile), 0o755); err != nil {
		return
	}
	_ = os.WriteFile(cacheFile, []byte(path+"\n"), 0o600)
}

func clearCachedCodexPath() {
	cacheFile := cachedCodexPathFile()
	if cacheFile == "" {
		return
	}
	_ = os.Remove(cacheFile)
}

func runCodexInstaller(ctx context.Context, out io.Writer, installerEnv []string) error {
	if err := ensureCodexInstallDiskSpace(out, installerEnv, nil); err != nil {
		return err
	}

	candidates := codexInstallerCandidates(runtime.GOOS)
	if len(candidates) == 0 {
		return fmt.Errorf("no supported installer available for %s", runtime.GOOS)
	}

	attemptErrors := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if _, err := exec.LookPath(candidate.path); err != nil {
			attemptErrors = append(attemptErrors, fmt.Sprintf("%s: not found in PATH", installerAttemptLabel(candidate)))
			continue
		}

		cmd := exec.CommandContext(ctx, candidate.path, candidate.args...)
		if len(installerEnv) > 0 {
			cmd.Env = installerEnv
		}
		cmd.Stdout = out
		cmd.Stderr = out
		cmd.Stdin = os.Stdin
		if err := cmd.Run(); err != nil {
			attemptError := fmt.Sprintf("%s: %v", installerAttemptLabel(candidate), err)
			if exitCode, ok := commandExitCode(err); ok {
				if exitCode == codexInstallDiskExit || exitCode == codexInstallFailureExit {
					return fmt.Errorf("failed to install codex CLI for %s (%s)", runtime.GOOS, attemptError)
				}
			}
			if diskErr := ensureCodexInstallDiskSpace(out, installerEnv, nil); diskErr != nil {
				return diskErr
			}
			attemptErrors = append(attemptErrors, attemptError)
			continue
		}
		return nil
	}

	if len(attemptErrors) == 0 {
		return fmt.Errorf("no supported installer available for %s", runtime.GOOS)
	}
	return fmt.Errorf("failed to install codex CLI for %s (%s)", runtime.GOOS, strings.Join(attemptErrors, "; "))
}

func commandExitCode(err error) (int, bool) {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return 0, false
	}
	return exitErr.ExitCode(), true
}

func codexInstallerCandidates(goos string) []codexInstallCmd {
	switch strings.ToLower(goos) {
	case "windows":
		out := make([]codexInstallCmd, 0, 3)
		seen := map[string]struct{}{}
		add := func(path string) {
			path = strings.TrimSpace(path)
			if path == "" {
				return
			}
			key := strings.ToLower(path)
			if _, ok := seen[key]; ok {
				return
			}
			seen[key] = struct{}{}
			out = append(out, codexInstallCmd{
				path: path,
				args: []string{"-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", codexInstallBootstrapWindows},
			})
		}
		if systemRoot := strings.TrimSpace(os.Getenv("SystemRoot")); systemRoot != "" {
			add(strings.TrimRight(systemRoot, `\/`) + `\System32\WindowsPowerShell\v1.0\powershell.exe`)
		}
		add("powershell")
		add("pwsh")
		return out
	case "darwin", "linux":
		return []codexInstallCmd{
			{path: "bash", args: []string{"-c", codexInstallBootstrap}},
			{path: "sh", args: []string{"-c", codexInstallBootstrap}},
		}
	default:
		return nil
	}
}

func installerAttemptLabel(cmd codexInstallCmd) string {
	if len(cmd.args) == 0 {
		return cmd.path
	}
	return fmt.Sprintf("%s %s", cmd.path, cmd.args[0])
}

func findInstalledCodex(ctx context.Context) (string, error) {
	ensureManagedNodeOnPath()

	var pathProbeFailure string
	if path, err := exec.LookPath("codex"); err == nil {
		path = normalizeExecutablePath(path)
		if err := probeCodexVersion(ctx, path); err == nil {
			return path, nil
		} else {
			pathProbeFailure = fmt.Sprintf("%s: %v", path, err)
		}
	}
	path, err := findInstalledCodexInCandidates(ctx)
	if err == nil {
		return path, nil
	}
	if pathProbeFailure != "" && errors.Is(err, errCodexBinaryNotFound) {
		return "", fmt.Errorf("codex binary found but not functional (%s)", pathProbeFailure)
	}
	return "", err
}

func findInstalledCodexInCandidates(ctx context.Context) (string, error) {
	probeFailures := make([]string, 0, 3)
	probeFailureCount := 0
	for _, candidate := range codexBinaryCandidates() {
		if !executableExists(candidate) {
			continue
		}
		if err := probeCodexVersion(ctx, candidate); err == nil {
			return normalizeExecutablePath(candidate), nil
		} else {
			probeFailureCount++
			if len(probeFailures) < 3 {
				probeFailures = append(probeFailures, fmt.Sprintf("%s: %v", filepath.Clean(candidate), err))
			}
		}
	}
	if len(probeFailures) > 0 {
		msg := strings.Join(probeFailures, "; ")
		if probeFailureCount > len(probeFailures) {
			msg += fmt.Sprintf("; and %d more", probeFailureCount-len(probeFailures))
		}
		return "", fmt.Errorf("codex binary found but not functional (%s)", msg)
	}
	return "", errCodexBinaryNotFound
}

func codexBinaryCandidates() []string {
	return codexBinaryCandidatesForEnv(
		runtime.GOOS,
		preferredHomeDir(),
		os.Getenv("CODEX_NPM_PREFIX"),
		os.Getenv("LOCALAPPDATA"),
		os.Getenv("APPDATA"),
		os.TempDir(),
	)
}

func ensureManagedNodeOnPath() {
	candidates := managedNodeBinCandidates()
	if len(candidates) == 0 {
		return
	}

	currentPath := os.Getenv("PATH")
	pathParts := filepath.SplitList(currentPath)
	seen := make(map[string]struct{}, len(pathParts)+len(candidates))
	normalize := func(path string) string {
		path = filepath.Clean(strings.TrimSpace(path))
		if runtime.GOOS == "windows" {
			path = strings.ToLower(path)
		}
		return path
	}
	for _, part := range pathParts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			seen[normalize(trimmed)] = struct{}{}
		}
	}

	prepend := make([]string, 0, len(candidates))
	for _, dir := range candidates {
		normalized := normalize(dir)
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		prepend = append(prepend, filepath.Clean(dir))
	}
	if len(prepend) == 0 {
		return
	}

	updated := append(prepend, pathParts...)
	_ = os.Setenv("PATH", strings.Join(updated, string(os.PathListSeparator)))
}

func managedNodeBinCandidates() []string {
	return managedNodeBinCandidatesForEnv(
		runtime.GOOS,
		runtime.GOARCH,
		preferredHomeDir(),
		os.Getenv("CODEX_NODE_INSTALL_ROOT"),
		os.Getenv("CODEX_NODE_MAJOR"),
		os.Getenv("LOCALAPPDATA"),
		os.TempDir(),
	)
}

func managedNodeBinCandidatesForEnv(goos, goarch, home, nodeRoot, nodeMajor, localAppData, tempDir string) []string {
	candidates := make([]string, 0, 8)
	seen := map[string]struct{}{}
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		path = filepath.Clean(path)
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		candidates = append(candidates, path)
	}

	nodeRoot = strings.TrimSpace(nodeRoot)
	isWindows := strings.EqualFold(goos, "windows")
	if nodeRoot == "" {
		if isWindows {
			baseDir := strings.TrimSpace(localAppData)
			if baseDir == "" {
				baseDir = strings.TrimSpace(tempDir)
			}
			if baseDir != "" {
				nodeRoot = filepath.Join(baseDir, "codex-proxy", "node")
			}
		} else {
			if home = strings.TrimSpace(home); home != "" {
				nodeRoot = filepath.Join(home, ".cache", "codex-proxy", "node")
			}
		}
	}
	if nodeRoot == "" {
		return candidates
	}

	major := strings.TrimPrefix(strings.TrimSpace(nodeMajor), "v")
	if major == "" {
		major = "22"
	}
	for _, ch := range major {
		if ch < '0' || ch > '9' {
			major = "22"
			break
		}
	}

	arch := nodeRuntimeArch(goarch)
	if isWindows {
		nodeFile := "node.exe"
		addInstallDir := func(installDir string) {
			installDir = strings.TrimSpace(installDir)
			if installDir == "" {
				return
			}
			if executableExists(filepath.Join(installDir, nodeFile)) {
				add(installDir)
			}
		}

		if arch != "" {
			addInstallDir(filepath.Join(nodeRoot, fmt.Sprintf("v%s-win-%s", major, arch)))
		}
		pattern := filepath.Join(nodeRoot, "v*-win-*")
		if arch != "" {
			pattern = filepath.Join(nodeRoot, fmt.Sprintf("v*-win-%s", arch))
		}
		if matches, err := filepath.Glob(pattern); err == nil {
			for _, match := range matches {
				addInstallDir(match)
			}
		}
		return candidates
	}

	osName := strings.ToLower(strings.TrimSpace(goos))
	if osName != "linux" && osName != "darwin" {
		return candidates
	}
	addInstallDir := func(installDir string) {
		installDir = strings.TrimSpace(installDir)
		if installDir == "" {
			return
		}
		binDir := filepath.Join(installDir, "bin")
		if executableExists(filepath.Join(binDir, "node")) {
			add(binDir)
		}
	}

	if arch != "" {
		addInstallDir(filepath.Join(nodeRoot, fmt.Sprintf("v%s-%s-%s", major, osName, arch)))
	}
	pattern := filepath.Join(nodeRoot, fmt.Sprintf("v*-%s-*", osName))
	if arch != "" {
		pattern = filepath.Join(nodeRoot, fmt.Sprintf("v*-%s-%s", osName, arch))
	}
	if matches, err := filepath.Glob(pattern); err == nil {
		for _, match := range matches {
			addInstallDir(match)
		}
	}
	return candidates
}

func nodeRuntimeArch(goarch string) string {
	switch strings.ToLower(strings.TrimSpace(goarch)) {
	case "amd64", "x86_64":
		return "x64"
	case "arm64", "aarch64":
		return "arm64"
	default:
		return ""
	}
}

func codexBinaryCandidatesForEnv(goos, home, npmPrefix, localAppData, appData, tempDir string) []string {
	candidates := make([]string, 0, 16)
	seen := map[string]struct{}{}
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		path = filepath.Clean(path)
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		candidates = append(candidates, path)
	}

	isWindows := strings.EqualFold(goos, "windows")

	if prefix := strings.TrimSpace(npmPrefix); prefix != "" {
		if isWindows {
			add(filepath.Join(prefix, "codex.cmd"))
			add(filepath.Join(prefix, "codex.exe"))
			add(filepath.Join(prefix, "bin", "codex.cmd"))
			add(filepath.Join(prefix, "bin", "codex.exe"))
		} else {
			add(filepath.Join(prefix, "bin", "codex"))
			add(filepath.Join(prefix, "codex"))
		}
	}

	if home = strings.TrimSpace(home); home != "" {
		add(filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex"))
		add(filepath.Join(home, ".npm-global", "bin", "codex"))
		add(filepath.Join(home, ".local", "bin", "codex"))
	}

	if isWindows {
		localAppData = strings.TrimSpace(localAppData)
		if localAppData == "" {
			localAppData = strings.TrimSpace(tempDir)
		}
		if localAppData != "" {
			add(filepath.Join(localAppData, "codex-proxy", "npm-global", "codex.cmd"))
			add(filepath.Join(localAppData, "codex-proxy", "npm-global", "bin", "codex.cmd"))
			add(filepath.Join(localAppData, "codex-proxy", "npm-global", "codex.exe"))
		}
		appData = strings.TrimSpace(appData)
		if appData != "" {
			add(filepath.Join(appData, "npm", "codex.cmd"))
			add(filepath.Join(appData, "npm", "codex.exe"))
		}
	}

	return candidates
}

func preferredHomeDir() string {
	if home := strings.TrimSpace(os.Getenv("HOME")); home != "" {
		return filepath.Clean(home)
	}
	if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
		return filepath.Clean(home)
	}
	if cwd, err := os.Getwd(); err == nil && strings.TrimSpace(cwd) != "" {
		return filepath.Clean(cwd)
	}
	return ""
}

func isCodexCommand(command string) bool {
	command = strings.TrimSpace(command)
	if command == "" {
		return false
	}
	command = strings.ReplaceAll(command, "\\", "/")
	base := strings.ToLower(filepath.Base(command))
	switch base {
	case "codex", "codex.exe", "codex.cmd", "codex.bat", "codex.ps1", "codex.js", "codex.mjs", "codex.cjs":
		return true
	default:
		return false
	}
}

func hasPathSeparator(command string) bool {
	return strings.Contains(command, "/") || strings.Contains(command, "\\")
}

func resolveRunCommand(ctx context.Context, cmdArgs []string, out io.Writer) ([]string, error) {
	return resolveRunCommandWithInstallOptions(ctx, cmdArgs, out, codexInstallOptions{})
}

func resolveRunCommandWithInstallOptions(ctx context.Context, cmdArgs []string, out io.Writer, installOpts codexInstallOptions) ([]string, error) {
	if len(cmdArgs) == 0 {
		return cmdArgs, nil
	}
	cmd := strings.TrimSpace(cmdArgs[0])
	if !isCodexCommand(cmd) {
		return cmdArgs, nil
	}

	var resolved string
	var err error
	if filepath.IsAbs(cmd) || hasPathSeparator(cmd) {
		resolved, err = ensureCodexInstalledWithOptions(ctx, cmd, out, installOpts)
	} else {
		if path, lookErr := exec.LookPath(cmd); lookErr == nil {
			path = normalizeExecutablePath(path)
			if probeCodex(ctx, path) {
				updated := make([]string, len(cmdArgs))
				copy(updated, cmdArgs)
				updated[0] = path
				return updated, nil
			}
		}
		resolved, err = ensureCodexInstalledWithOptions(ctx, "", out, installOpts)
	}
	if err != nil {
		return nil, err
	}

	updated := make([]string, len(cmdArgs))
	copy(updated, cmdArgs)
	updated[0] = resolved
	return updated, nil
}

func executableExists(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	path = filepath.Clean(path)
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	return true
}
