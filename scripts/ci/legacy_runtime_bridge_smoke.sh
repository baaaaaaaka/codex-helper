#!/usr/bin/env bash
set -euo pipefail

unset CXP_RUNTIME CXP_RUNTIME_ROOT CXP_RUNTIME_VERSION CXP_ENTRY_PATH CXP_RUNTIME_DISABLE CXP_RUNTIME_FORCE

goos="$(go env GOOS)"
goarch="$(go env GOARCH)"
if [ "$goos" = "windows" ]; then
  echo "legacy runtime bridge smoke uses the separate Windows handoff path"
  exit 0
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

root="$tmp/install/.cxp-runtime"
entry="$tmp/install/cxp"
old_runtime="$root/versions/v0.1.13-rc.31/cxp"
candidate_dir="$root/versions/v0.1.13-rc.36"
candidate="$candidate_dir/.codex-proxy_0.1.13-rc.36_${goos}_${goarch}.12345"
mkdir -p "$tmp/install" "$tmp/runtime-storage"
ln -s runtime-storage "$tmp/runtime-hop"
ln -s ../runtime-hop "$root"
mkdir -p "$(dirname "$old_runtime")" "$candidate_dir"

(
  cd "$repo_root"
  go build -trimpath -o "$old_runtime" ./scripts/tests/legacy_bridge_parent
  go build -trimpath \
    -ldflags '-X github.com/baaaaaaaka/codex-helper/internal/cli.version=v0.1.13-rc.36' \
    -o "$candidate" \
    ./cmd/codex-proxy
)
cp "$old_runtime" "$entry"
chmod 700 "$old_runtime" "$entry" "$candidate"
printf 'v0.1.13-rc.31\n' >"$root/active"

# The candidate completes managed-install record reconciliation during its
# version probe. Keep that real side effect inside the fixture so this smoke
# cannot leave a dangling install record for later jobs on the same runner.
export HOME="$tmp/home"
export XDG_CONFIG_HOME="$HOME/.config"
export XDG_CACHE_HOME="$HOME/.cache"
export XDG_STATE_HOME="$HOME/.local/state"
mkdir -p "$XDG_CONFIG_HOME" "$XDG_CACHE_HOME" "$XDG_STATE_HOME"

env \
  CXP_RUNTIME=1 \
  CXP_RUNTIME_ROOT="$root" \
  CXP_RUNTIME_VERSION=v0.1.13-rc.31 \
  CXP_ENTRY_PATH="$entry" \
  CXP_RUNTIME_DISABLE=1 \
  "$old_runtime" "$candidate" | grep -F '0.1.13-rc.36' >/dev/null

test "$(tr -d '\r\n' <"$root/active")" = "v0.1.13-rc.36"
test "$(tr -d '\r\n' <"$root/previous")" = "v0.1.13-rc.31"
cmp -s "$entry" "$root/versions/v0.1.13-rc.36/cxp"

env \
  CXP_RUNTIME=1 \
  CXP_RUNTIME_ROOT="$root" \
  CXP_RUNTIME_VERSION=v0.1.13-rc.36 \
  CXP_ENTRY_PATH="$entry" \
  CXP_RUNTIME_DISABLE=1 \
  "$entry" --version | grep -F '0.1.13-rc.36' >/dev/null

echo "legacy runtime bridge smoke passed"
