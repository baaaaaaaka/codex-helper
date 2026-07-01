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
