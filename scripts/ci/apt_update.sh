#!/usr/bin/env bash
set -euo pipefail

# Hosted Ubuntu images may carry a Google Chrome source that is not needed by
# this repository's package setup. Its CDN can publish a Packages index and
# matching Release metadata at different moments, producing an apt Hash Sum
# mismatch even while the Ubuntu archives are healthy. Keep the official
# sources enabled, temporarily remove only the known optional Chrome source,
# and restore it before returning to the caller.

source_dir="${APT_UPDATE_SOURCE_DIR:-/etc/apt/sources.list.d}"
attempts="${APT_UPDATE_ATTEMPTS:-5}"
sleep_seconds="${APT_UPDATE_SLEEP_SECONDS:-5}"
lists_dir="${APT_UPDATE_LISTS_DIR:-/var/lib/apt/lists}"

if ! command -v apt-get >/dev/null 2>&1; then
  echo "apt-get is required" >&2
  exit 1
fi
if [[ ! "$attempts" =~ ^[1-9][0-9]*$ ]]; then
  echo "APT_UPDATE_ATTEMPTS must be a positive integer" >&2
  exit 1
fi
if [[ ! "$sleep_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "APT_UPDATE_SLEEP_SECONDS must be a non-negative number" >&2
  exit 1
fi

disabled_dir="$(mktemp -d "${TMPDIR:-/tmp}/codex-apt-sources.XXXXXX")"
disabled_sources=()

restore_sources() {
  local source base
  for source in "${disabled_sources[@]}"; do
    base="$(basename "$source")"
    if [[ ! -e "$disabled_dir/$base" ]]; then
      echo "temporarily disabled apt source disappeared: $source" >&2
      continue
    fi
    if [[ -e "$source" ]]; then
      echo "cannot restore apt source because the destination is occupied: $source" >&2
      return 1
    fi
    if ! mv -- "$disabled_dir/$base" "$source"; then
      echo "failed to restore apt source: $source" >&2
      return 1
    fi
  done
  return 0
}

cleanup() {
  local rc="$?"
  trap - EXIT INT TERM
  if ! restore_sources; then
    rc=1
  fi
  rm -rf -- "$disabled_dir"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ -d "$source_dir" ]]; then
  while IFS= read -r -d '' source; do
    base="$(basename "$source")"
    if ! mv -- "$source" "$disabled_dir/$base"; then
      echo "failed to temporarily disable apt source: $source" >&2
      exit 1
    fi
    disabled_sources+=("$source")
    echo "temporarily disabled optional apt source: $source" >&2
  done < <(find "$source_dir" -maxdepth 1 -type f \( -iname 'google-chrome*.list' -o -iname 'google-chrome*.sources' \) -print0)
fi

for ((attempt = 1; attempt <= attempts; attempt++)); do
  log_path="$(mktemp "${TMPDIR:-/tmp}/codex-apt-update.XXXXXX")"
  set +e
  apt-get update 2>&1 | tee "$log_path"
  update_rc="${PIPESTATUS[0]}"
  set -e

  incomplete=0
  if [[ "$update_rc" -eq 0 ]] && grep -Eq '^W: (Failed to fetch|Some index files failed to download)' "$log_path"; then
    incomplete=1
    update_rc=1
    echo "apt-get update returned success with incomplete package indexes" >&2
  fi
  rm -f -- "$log_path"

  if [[ "$update_rc" -eq 0 && "$incomplete" -eq 0 ]]; then
    exit 0
  fi
  if [[ "$attempt" -eq "$attempts" ]]; then
    break
  fi

  # Do not carry a partial index from a failed mirror response into the next
  # attempt. Ignore cleanup errors so the original apt failure remains visible.
  apt-get clean >/dev/null 2>&1 || true
  rm -rf -- "$lists_dir/partial"/* 2>/dev/null || true
  echo "apt-get update failed (attempt ${attempt}/${attempts}), retrying in ${sleep_seconds}s" >&2
  sleep "$sleep_seconds"
done

echo "apt-get update failed after ${attempts} attempts" >&2
exit 1
