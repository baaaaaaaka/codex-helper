#!/usr/bin/env bash
# Shallow-clone FFmpeg at $FFMPEG_TAG into $1 and verify it is $FFMPEG_COMMIT.
set -euo pipefail
dest=${1:?usage: ffmpeg-fetch-source.sh DEST}
git -c advice.detachedHead=false clone --quiet --depth 1 --branch "${FFMPEG_TAG:?}" https://github.com/FFmpeg/FFmpeg.git "$dest"
got=$(git -C "$dest" rev-parse HEAD)
if [[ "$got" != "${FFMPEG_COMMIT:?}" ]]; then
  echo "FFmpeg $FFMPEG_TAG resolved to $got, want $FFMPEG_COMMIT" >&2
  exit 1
fi
echo "FFmpeg $FFMPEG_TAG at $got"
