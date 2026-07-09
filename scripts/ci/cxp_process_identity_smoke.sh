#!/usr/bin/env bash
set -euo pipefail

unset CXP_RUNTIME CXP_RUNTIME_ROOT CXP_RUNTIME_VERSION CXP_ENTRY_PATH CXP_RUNTIME_DISABLE CXP_RUNTIME_FORCE

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
root="$(mktemp -d "${TMPDIR:-/tmp}/cxp-process.XXXXXX")"
pid=""

cleanup() {
  if [[ -n "$pid" ]]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf "$root"
}
trap cleanup EXIT

assert_process_clean() {
  local current_pid="$1"
  local observed="$root/observed.txt"
  case "$(uname -s)" in
    Linux)
      {
        ps -p "$current_pid" -o comm=,args=
        readlink "/proc/$current_pid/exe"
        awk '{ if (NF >= 6 && $NF ~ /^\//) print $NF }' "/proc/$current_pid/maps"
      } >"$observed"
      ;;
    Darwin)
      {
        ps -p "$current_pid" -o comm=,command=
        lsof -a -p "$current_pid" -d txt -Fn
      } >"$observed"
      ;;
    *)
      echo "unsupported process identity platform: $(uname -s)" >&2
      exit 2
      ;;
  esac
  cat "$observed"
  if grep -Eqi 'codex' "$observed"; then
    echo "CXP-owned process metadata contains the forbidden compatibility keyword" >&2
    exit 1
  fi
}

assert_fresh_launcher_dispatches_active_runtime() {
  local fixture="$root/fresh-launch-restart"
  local entry="$fixture/cxp"
  local runtime_root="$fixture/.cxp-runtime"
  local old_version="0.1.13-rc.9001"
  local target_version="0.1.13-rc.9002"
  local old_runtime="$runtime_root/versions/v$old_version/cxp"
  local target_runtime="$runtime_root/versions/v$target_version/cxp"

  mkdir -p "$(dirname "$old_runtime")" "$(dirname "$target_runtime")"
  (
    cd "$repo_root"
    go build -trimpath \
      -ldflags "-X github.com/baaaaaaaka/codex-helper/internal/cli.version=$old_version" \
      -o "$entry" \
      ./cmd/codex-proxy
    install -m 0755 "$entry" "$old_runtime"
    go build -trimpath \
      -ldflags "-X github.com/baaaaaaaka/codex-helper/internal/cli.version=$target_version" \
      -o "$target_runtime" \
      ./cmd/codex-proxy
  )
  printf 'v%s\n' "$target_version" >"$runtime_root/active"

  local stale_output
  stale_output="$(
    CXP_RUNTIME=1 \
      CXP_RUNTIME_ROOT="$runtime_root" \
      CXP_RUNTIME_VERSION="v$old_version" \
      CXP_ENTRY_PATH="$entry" \
      "$entry" --version
  )"
  printf '%s\n' "$stale_output"
  if [[ "$stale_output" != *"$old_version"* ]]; then
    echo "runtime-marked launcher did not stay pinned to $old_version" >&2
    exit 1
  fi

  local fresh_output
  fresh_output="$(
    env \
      -u CXP_RUNTIME \
      -u CXP_RUNTIME_ROOT \
      -u CXP_RUNTIME_VERSION \
      -u CXP_ENTRY_PATH \
      -u CXP_RUNTIME_DISABLE \
      -u CXP_RUNTIME_FORCE \
      "$entry" --version
  )"
  printf '%s\n' "$fresh_output"
  if [[ "$fresh_output" != *"$target_version"* ]]; then
    echo "fresh stable launcher did not dispatch active runtime $target_version" >&2
    exit 1
  fi
  test "$(tr -d '\r\n' <"$runtime_root/active")" = "v$target_version"
}

wait_for_runtime() {
  local current_pid="$1"
  local active="$root/.cxp-runtime/active"
  for _ in $(seq 1 200); do
    if ! kill -0 "$current_pid" 2>/dev/null; then
      cat "$root/stderr.log" >&2 || true
      echo "CXP process exited before runtime activation" >&2
      exit 1
    fi
    if [[ -s "$active" ]]; then
      case "$(uname -s)" in
        Linux)
          if [[ "$(readlink "/proc/$current_pid/exe" 2>/dev/null || true)" == *"/.cxp-runtime/versions/"* ]]; then
            return 0
          fi
          ;;
        Darwin)
          if ps -p "$current_pid" -o command= 2>/dev/null | grep -Fq '/.cxp-runtime/versions/'; then
            return 0
          fi
          ;;
      esac
    fi
    sleep 0.05
  done
  cat "$root/stderr.log" >&2 || true
  echo "timed out waiting for CXP runtime activation" >&2
  exit 1
}

cd "$repo_root"
go build -o "$root/cxp" ./cmd/codex-proxy
cp "$root/cxp" "$root/codex-proxy"
chmod 0755 "$root/cxp" "$root/codex-proxy"

assert_fresh_launcher_dispatches_active_runtime

for entry in cxp codex-proxy; do
  : >"$root/stdout.log"
  : >"$root/stderr.log"
  HOME="$root/home" XDG_STATE_HOME="$root/state" \
    "$root/$entry" responses serve \
      --listen 127.0.0.1:0 \
      --base-url http://127.0.0.1:9/v1 \
      --api-key process-identity-smoke \
      --model process-identity-smoke \
      --store-path "$root/responses.sqlite" \
      >"$root/stdout.log" 2>"$root/stderr.log" &
  pid=$!
  wait_for_runtime "$pid"
  assert_process_clean "$pid"
  kill "$pid"
  wait "$pid" 2>/dev/null || true
  pid=""
done

echo "actual CXP process identity smoke passed"
