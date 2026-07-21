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
split_stable_source="$tmp/split-stable-v0.1.13"
split_record="$tmp/split-config/install.json"
split_active_source="$tmp/split-active-v0.1.15"
split_active_runtime="$split_runtime_root/versions/v0.1.15/cxp"
split_candidate="$tmp/split-candidate-v0.1.16"
split_context="$tmp/context-split-stable.json"
split_result="$tmp/result-split-stable.json"
split_stderr="$tmp/stderr-split-stable.log"

mkdir -p "$(dirname "$split_active_runtime")" "$(dirname "$split_record")"
build_candidate v0.1.13 "$split_stable_source"
build_candidate v0.1.15 "$split_active_source"
build_candidate v0.1.16 "$split_candidate"
install -m 0755 "$split_active_source" "$split_active_runtime"
install -m 0755 "$split_stable_source" "$split_stable"
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

expect_managed_update_failure() {
  local name="$1"
  local root="$2"
  local entry="$3"
  local record="$4"
  local stable="$5"
  local expected_error="$6"
  local context="$tmp/context-negative-$name.json"
  local result="$tmp/result-negative-$name.json"
  local stderr="$tmp/stderr-negative-$name.log"
  local stable_hash
  local record_hash
  local active_before
  stable_hash="$(sha256_file "$stable")"
  record_hash="$(sha256_file "$record")"
  active_before="$(tr -d '\r\n' <"$root/active")"
  printf '{"schema":1,"candidate_path":"%s","candidate_sha256":"%s","source_version":"v0.1.15","target_version":"v0.1.16","runtime_root":"%s","entry_path":"%s","record_path":"%s","request_id":"negative-%s"}\n' \
    "$split_candidate" "$split_hash" "$root" "$entry" "$record" "$name" >"$context"
  chmod 600 "$context"
  if "$split_candidate" __internal-update-apply --protocol=1 --context-file="$context" >"$result" 2>"$stderr"; then
    echo "negative managed update $name unexpectedly succeeded" >&2
    exit 1
  fi
  grep -F "$expected_error" "$stderr" >/dev/null
  test "$(tr -d '\r\n' <"$root/active")" = "$active_before"
  test "$(sha256_file "$stable")" = "$stable_hash"
  test "$(sha256_file "$record")" = "$record_hash"
  test ! -e "$root/pending-update.json"
}

negative_install_dir="$tmp/negative-mismatch"
negative_runtime_root="$negative_install_dir/.cxp-runtime"
negative_entry="$negative_install_dir/cxp"
negative_stable="$negative_install_dir/codex-proxy"
negative_record="$tmp/negative-mismatch-config/install.json"
mkdir -p "$negative_runtime_root/versions/v0.1.15" "$(dirname "$negative_record")"
install -m 0755 "$split_active_source" "$negative_runtime_root/versions/v0.1.15/cxp"
printf '%s\n' "v0.1.15" >"$negative_runtime_root/active"
install -m 0755 "$split_stable_source" "$negative_stable"
ln -s codex-proxy "$negative_entry"
printf '{"schema_version":1,"target_path":"%s","target_state":"managed","version":"0.1.15"}\n' \
  "$negative_install_dir/another-install/codex-proxy" >"$negative_record"
expect_managed_update_failure "record-mismatch" "$negative_runtime_root" "$negative_entry" "$negative_record" "$negative_stable" "not a recorded managed helper"

negative_custom_install_dir="$tmp/negative-custom"
negative_custom_runtime_root="$negative_custom_install_dir/.cxp-runtime"
negative_custom_entry="$negative_custom_install_dir/cxp"
negative_custom_stable="$negative_custom_install_dir/codex-proxy"
negative_custom_record="$tmp/negative-custom-config/install.json"
mkdir -p "$negative_custom_runtime_root/versions/v0.1.15" "$(dirname "$negative_custom_record")"
install -m 0755 "$split_active_source" "$negative_custom_runtime_root/versions/v0.1.15/cxp"
printf '%s\n' "v0.1.15" >"$negative_custom_runtime_root/active"
printf '#!/bin/sh\nexit 0\n' >"$negative_custom_stable"
chmod 0755 "$negative_custom_stable"
ln -s codex-proxy "$negative_custom_entry"
printf '{"schema_version":1,"target_path":"%s","target_state":"managed","version":"0.1.15"}\n' \
  "$negative_custom_stable" >"$negative_custom_record"
expect_managed_update_failure "custom-entry" "$negative_custom_runtime_root" "$negative_custom_entry" "$negative_custom_record" "$negative_custom_stable" "not a verified helper"

echo "candidate-owned update smoke passed"
