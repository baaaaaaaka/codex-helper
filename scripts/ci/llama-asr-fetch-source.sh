#!/usr/bin/env bash
# Shallow-clone llama.cpp at $LLAMA_TAG into $1 and verify it is $LLAMA_COMMIT.
set -euo pipefail
dest=${1:?usage: llama-asr-fetch-source.sh DEST}
git clone --quiet --depth 1 --branch "${LLAMA_TAG:?}" https://github.com/ggml-org/llama.cpp.git "$dest"
got=$(git -C "$dest" rev-parse HEAD)
if [[ "$got" != "${LLAMA_COMMIT:?}" ]]; then
  echo "llama.cpp $LLAMA_TAG resolved to $got, want $LLAMA_COMMIT" >&2
  exit 1
fi
echo "llama.cpp $LLAMA_TAG at $got"
