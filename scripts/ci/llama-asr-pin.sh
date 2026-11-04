#!/usr/bin/env bash
# Write <archive>.pin.json (name, sha256, size, platform) next to a runtime
# archive; the llama-asr-assets workflow turns these into Go pins.
set -euo pipefail
archive=${1:?usage: llama-asr-pin.sh ARCHIVE PLATFORM}
platform=${2:?usage: llama-asr-pin.sh ARCHIVE PLATFORM}
if command -v sha256sum >/dev/null; then sha=$(sha256sum "$archive" | cut -d' ' -f1); else sha=$(shasum -a 256 "$archive" | cut -d' ' -f1); fi
size=$(wc -c < "$archive" | tr -d ' ')
name=$(basename "$archive")
printf '{"name":"%s","sha256":"%s","size":%s,"platform":"%s"}\n' "$name" "$sha" "$size" "$platform" | tee "${archive%.tar.gz}.pin.json"
