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
test -e "$runtime_root/pending-update.json"
"$entry" --version | grep -F "0.1.13-rc.36" >/dev/null
test ! -e "$runtime_root/pending-update.json"

apply_candidate "$candidate2" "0.1.13-rc.36" "v0.1.13-rc.37" "second"
test "$(tr -d '\r\n' <"$runtime_root/active")" = "v0.1.13-rc.37"
test "$(tr -d '\r\n' <"$runtime_root/previous")" = "v0.1.13-rc.36"
test -e "$runtime_root/pending-update.json"
"$entry" --version | grep -F "0.1.13-rc.37" >/dev/null
test ! -e "$runtime_root/pending-update.json"

"$entry" --recover-previous | grep -F "v0.1.13-rc.36" >/dev/null
test "$(tr -d '\r\n' <"$runtime_root/active")" = "v0.1.13-rc.36"
test "$(tr -d '\r\n' <"$runtime_root/previous")" = "v0.1.13-rc.37"
"$entry" --version | grep -F "0.1.13-rc.36" >/dev/null

split_install_dir="$tmp/split-install"
split_runtime_root="$split_install_dir/.cxp-runtime"
split_entry="$split_install_dir/cxp"
split_stable="$split_install_dir/codex-proxy"
split_record="$tmp/split-config/install.json"
split_active_source="$tmp/split-active-v0.1.15"
split_active_runtime="$split_runtime_root/versions/v0.1.15/cxp"
split_candidate="$tmp/split-candidate-v0.1.16"
split_context="$tmp/context-split-stable.json"
split_result="$tmp/result-split-stable.json"
split_stderr="$tmp/stderr-split-stable.log"

mkdir -p "$(dirname "$split_active_runtime")" "$(dirname "$split_record")"
build_candidate v0.1.13 "$split_stable"
build_candidate v0.1.15 "$split_active_source"
build_candidate v0.1.16 "$split_candidate"
install -m 0755 "$split_active_source" "$split_active_runtime"
ln -s codex-proxy "$split_entry"
printf '%s\n' "v0.1.15" >"$split_runtime_root/active"
printf '{"schema_version":1,"target_path":"%s","target_source":"installer","target_state":"managed","repo":"baaaaaaaka/codex-helper","version":"0.1.15","shims":["%s"]}\n' \
  "$split_stable" "$split_entry" >"$split_record"

split_hash="$(sha256_file "$split_candidate")"
printf '{"schema":1,"candidate_path":"%s","candidate_sha256":"%s","source_version":"v0.1.15","target_version":"v0.1.16","runtime_root":"%s","entry_path":"%s","record_path":"%s","request_id":"split-stable-v0.1.13"}\n' \
  "$split_candidate" "$split_hash" "$split_runtime_root" "$split_entry" "$split_record" >"$split_context"
chmod 600 "$split_context"

if ! "$split_candidate" __internal-update-apply --protocol=1 --context-file="$split_context" >"$split_result" 2>"$split_stderr"; then
  echo "candidate-owned update failed for a verified managed split entry: stable=v0.1.13 active=v0.1.15 target=v0.1.16" >&2
  cat "$split_stderr" >&2
  exit 1
fi

test "$(tr -d '\r\n' <"$split_runtime_root/active")" = "v0.1.16"
test "$(tr -d '\r\n' <"$split_runtime_root/previous")" = "v0.1.15"
test -e "$split_runtime_root/pending-update.json"
"$split_entry" --version | grep -F "0.1.16" >/dev/null
test ! -e "$split_runtime_root/pending-update.json"

echo "candidate-owned update smoke passed"
