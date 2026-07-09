#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "NFS cache smoke requires Linux" >&2
  exit 1
fi
if ! sudo -n true; then
  echo "NFS cache smoke requires non-interactive sudo" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
export_dir="$tmp_dir/export"
mount_dir="$tmp_dir/mount"
exports_file="/etc/exports.d/cxp-cache-v2-$$.exports"
mounted=0

cleanup() {
  if [[ "$mounted" == "1" ]]; then
    sudo umount "$mount_dir" || true
  fi
  sudo rm -f "$exports_file"
  sudo exportfs -ra >/dev/null 2>&1 || true
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

if ! command -v exportfs >/dev/null 2>&1 || ! command -v mount.nfs >/dev/null 2>&1; then
  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y nfs-kernel-server nfs-common
fi

mkdir -p "$export_dir" "$mount_dir"
sudo chown "$(id -u):$(id -g)" "$export_dir"
sudo mkdir -p /etc/exports.d
printf '%s 127.0.0.1(rw,sync,no_subtree_check,no_root_squash,insecure)\n' "$export_dir" | sudo tee "$exports_file" >/dev/null
sudo exportfs -ra

if command -v systemctl >/dev/null 2>&1; then
  sudo systemctl start rpcbind
  sudo systemctl restart nfs-kernel-server
else
  sudo service rpcbind start
  sudo service nfs-kernel-server restart
fi

sudo mount -t nfs -o vers=3,proto=tcp,lock "127.0.0.1:$export_dir" "$mount_dir"
mounted=1
if ! findmnt -n -o FSTYPE --target "$mount_dir" | grep -Eq '^nfs'; then
  echo "test mount is not NFS" >&2
  exit 1
fi
sudo chown "$(id -u):$(id -g)" "$mount_dir"

cd "$repo_root"
go test -c -o "$tmp_dir/codexhistory-nfs.test" ./internal/codexhistory
env CXP_CACHE_NFS_ROOT="$mount_dir" \
  "$tmp_dir/codexhistory-nfs.test" \
    -test.run '^TestCacheV2LiveNFS$' -test.count=1 -test.v
