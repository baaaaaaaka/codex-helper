#!/usr/bin/env bash
# Download the pinned Qwen3-ASR GGUF model and projector into $1 under their
# published names and verify them against the pins in the workflow
# environment (MODEL_URL, MODEL_SHA256, MMPROJ_SHA256).
set -euo pipefail
dir=${1:?usage: llama-asr-fetch-model.sh DIR}
mkdir -p "$dir"
sha256() { if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1; }
fetch() {
  curl -sSfL --retry 5 --retry-all-errors -o "$dir/$1" "${MODEL_URL:?}/$1"
  got=$(sha256 "$dir/$1")
  [[ "$got" == "$2" ]] || { echo "$1: sha256 $got, want $2" >&2; exit 1; }
}
fetch Qwen3-ASR-0.6B-Q8_0.gguf "${MODEL_SHA256:?}"
fetch mmproj-Qwen3-ASR-0.6B-Q8_0.gguf "${MMPROJ_SHA256:?}"
