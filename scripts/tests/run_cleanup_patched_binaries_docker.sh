#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
python_bases=(
  "py38|python:3.8-slim-bullseye@sha256:e191a71397fd61fbddb6712cd43ef9a2c17df0b5e7ba67607128554cd6bff267"
  "py312|python:3.12-slim@sha256:423ed6ab25b1921a477529254bfeeabf5855151dc2c3141699a1bfc852199fbf"
)

for python_base in "${python_bases[@]}"; do
  label="${python_base%%|*}"
  base="${python_base#*|}"
  image="codex-helper-patched-cleanup-test:${label}"

  docker build \
    --build-arg "PYTHON_BASE=$base" \
    --file "$repo_root/scripts/tests/Dockerfile.cleanup-patched-binaries" \
    --tag "$image" \
    "$repo_root"

  # The normal run exercises current-user ownership as an unprivileged user.
  docker run --rm --network none "$image"

  # The root run additionally exercises refusal to remove a foreign-owned file.
  docker run --rm --network none --user 0:0 --env HOME=/root "$image"
done
