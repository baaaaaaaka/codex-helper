#!/usr/bin/env bash
# Upload exact release files, then verify the public copy against local pins.
# No rebuilding and no overwriting existing assets. A separate token must
# have access to the pre-existing mirror repository.
set -euo pipefail
release_dir=$1
mirror_repo=$2
release_tag=$3
[[ "$mirror_repo" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || { echo 'invalid mirror repository' >&2; exit 2; }
[[ "$release_tag" =~ ^llama-asr-[A-Za-z0-9_.-]+$ ]] || { echo 'invalid ASR release tag' >&2; exit 2; }
[[ -n "${GH_TOKEN:-}" ]] || { echo 'ASR_MIRROR_TOKEN is required' >&2; exit 2; }
[[ -n "${RUNNER_TEMP:-}" ]] || { echo 'RUNNER_TEMP must be a writable scratch directory' >&2; exit 2; }
release_dir=$(cd "$release_dir" && pwd)
(cd "$release_dir" && sha256sum --check SHA256SUMS)
if ! gh release view "$release_tag" --repo "$mirror_repo" >/dev/null 2>&1; then
  gh release create "$release_tag" --repo "$mirror_repo" --prerelease --latest=false \
    --title "Voice assets ($release_tag)" --notes 'Byte-identical mirror of the codex-helper voice assets; see SHA256SUMS.' "$release_dir"/*
fi
verify_dir=$(mktemp -d "$RUNNER_TEMP/asr-mirror-check.XXXXXXXX")
# Verification data stays in the CI runner scratch for failure inspection.
gh release download "$release_tag" --repo "$mirror_repo" --dir "$verify_dir"
cmp "$release_dir/SHA256SUMS" "$verify_dir/SHA256SUMS"
(cd "$verify_dir" && sha256sum --check "$release_dir/SHA256SUMS")
echo "Verified mirror base: https://github.com/$mirror_repo/releases/download/$release_tag/"
