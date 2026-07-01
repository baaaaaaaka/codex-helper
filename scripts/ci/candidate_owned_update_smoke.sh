#!/usr/bin/env bash
set -euo pipefail

unset CXP_RUNTIME CXP_RUNTIME_ROOT CXP_RUNTIME_VERSION CXP_ENTRY_PATH CXP_RUNTIME_DISABLE CXP_RUNTIME_FORCE

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

build_candidate() {
  local version="$1"
  local output="$2"
  (
    cd "$repo_root"
    go build -trimpath \
      -ldflags "-X github.com/baaaaaaaka/codex-helper/internal/cli.version=$version" \
      -o "$output" \
      ./cmd/codex-proxy
  )
}

install_dir="$tmp/install"
runtime_root="$install_dir/.cxp-runtime"
entry="$install_dir/cxp"
record="$tmp/config/install.json"
mkdir -p "$install_dir" "$tmp/config" "$tmp/runtime-storage"
ln -s runtime-storage "$tmp/runtime-hop"
ln -s ../runtime-hop "$runtime_root"

candidate1="$tmp/candidate-rc36"
candidate2="$tmp/candidate-rc37"
build_candidate v0.1.13-rc.36 "$candidate1"
build_candidate v0.1.13-rc.37 "$candidate2"

apply_candidate() {
  local candidate="$1"
  local source_version="$2"
  local target_version="$3"
  local request_id="$4"
  local context="$tmp/context-$request_id.json"
  local hash
  hash="$(sha256_file "$candidate")"
  printf '{"schema":1,"candidate_path":"%s","candidate_sha256":"%s","source_version":"%s","target_version":"%s","runtime_root":"%s","entry_path":"%s","record_path":"%s","request_id":"%s"}\n' \
    "$candidate" "$hash" "$source_version" "$target_version" "$runtime_root" "$entry" "$record" "$request_id" >"$context"
  chmod 600 "$context"
  "$candidate" __internal-update-apply --protocol=1 --context-file="$context" >"$tmp/result-$request_id.json"
}

apply_candidate "$candidate1" "" "v0.1.13-rc.36" "first"
test "$(tr -d '\r\n' <"$runtime_root/active")" = "v0.1.13-rc.36"
test ! -e "$runtime_root/pending-update.json"
"$entry" --version | grep -F "0.1.13-rc.36" >/dev/null

apply_candidate "$candidate2" "0.1.13-rc.36" "v0.1.13-rc.37" "second"
test "$(tr -d '\r\n' <"$runtime_root/active")" = "v0.1.13-rc.37"
test "$(tr -d '\r\n' <"$runtime_root/previous")" = "v0.1.13-rc.36"
"$entry" --version | grep -F "0.1.13-rc.37" >/dev/null
test ! -e "$runtime_root/pending-update.json"

"$entry" --recover-previous | grep -F "v0.1.13-rc.36" >/dev/null
test "$(tr -d '\r\n' <"$runtime_root/active")" = "v0.1.13-rc.36"
test "$(tr -d '\r\n' <"$runtime_root/previous")" = "v0.1.13-rc.37"
"$entry" --version | grep -F "0.1.13-rc.36" >/dev/null

echo "candidate-owned update smoke passed"
