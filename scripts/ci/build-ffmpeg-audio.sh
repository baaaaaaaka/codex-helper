#!/usr/bin/env bash
# Build the minimal, audio-only, LGPL ffmpeg that the managed voice
# transcription backend (internal/asr) runs to turn a Teams voice message
# into 16 kHz mono WAV segments, and stage it with its license and build
# info. Runs on every platform we publish:
#
#   linux-x64, linux-arm64   inside quay.io/pypa/manylinux2014_* (glibc 2.17)
#   macos-arm64, macos-x64   natively (deployment target 13.3)
#   win-x64                  MSYS2 UCRT64 (gcc), static
#   win-arm64                MSYS2 CLANGARM64 (clang), static
#
# Usage: build-ffmpeg-audio.sh PLATFORM SRC STAGE_PARENT
#   SRC           FFmpeg checkout at $FFMPEG_TAG (read-only is fine)
#   STAGE_PARENT  the staged directory ffmpeg-audio-<tag>-<rev>-<platform>/
#                 is created here (packaged by scripts/ci/asr-package.py)
#
# Environment: FFMPEG_TAG, FFMPEG_COMMIT (verified when SRC is a git
# checkout), ASSET_REV (default r1), JOBS.
#
# Only what internal/asr needs is enabled: the containers and codecs Teams
# and phones produce (mp4/m4a/aac, ogg/webm/opus/vorbis, mp3, wav, flac,
# amr, caf), the resample/tempo filters, and the WAV segment output. Nothing
# GPL or nonfree, no network, no hardware or system libraries.
set -euo pipefail

platform=${1:?usage: build-ffmpeg-audio.sh PLATFORM SRC STAGE_PARENT}
src=$(cd "${2:?}" && pwd)
stage_parent=${3:?}
FFMPEG_TAG=${FFMPEG_TAG:-n7.1.5}
FFMPEG_COMMIT=${FFMPEG_COMMIT:-3a0867c2bfda4a4d4309ca1a8cbdc6175e67f587}
ASSET_REV=${ASSET_REV:-r1}
JOBS=${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}
name="ffmpeg-audio-${FFMPEG_TAG}-${ASSET_REV}-${platform}"

if git -C "$src" rev-parse HEAD >/dev/null 2>&1; then
  got=$(git -C "$src" rev-parse HEAD)
  if [[ "$got" != "$FFMPEG_COMMIT" ]]; then
    echo "FFmpeg $FFMPEG_TAG resolved to $got, want $FFMPEG_COMMIT" >&2
    exit 1
  fi
fi

exe=ffmpeg
extra_cflags=()
extra_ldflags=()
case "$platform" in
  linux-x64|linux-arm64)
    extra_ldflags+=(-static-libgcc) ;;
  macos-arm64|macos-x64)
    extra_cflags+=(-mmacosx-version-min=13.3)
    extra_ldflags+=(-mmacosx-version-min=13.3) ;;
  win-x64|win-arm64)
    exe=ffmpeg.exe
    # No libgcc/libwinpthread/libc++ DLLs next to ffmpeg.exe: only Windows
    # system DLLs (UCRT is part of Windows 10 and later).
    extra_ldflags+=(-static) ;;
  *) echo "unknown platform $platform" >&2; exit 2 ;;
esac

configure_flags=(
  --disable-everything --disable-autodetect --disable-network
  --disable-doc --disable-debug --disable-shared --enable-static
  --disable-programs --enable-ffmpeg
  --disable-avdevice --disable-swscale
  --enable-avcodec --enable-avformat --enable-avfilter --enable-swresample
  --disable-x86asm
  --enable-protocol=file,pipe
  --enable-demuxer=mov,matroska,ogg,wav,w64,mp3,aac,flac,amr,caf,aiff
  --enable-decoder=aac,aac_latm,opus,vorbis,mp3,mp3float,flac,alac,amrnb,amrwb,pcm_s16le,pcm_s16be,pcm_s24le,pcm_s32le,pcm_f32le,pcm_f64le,pcm_u8,pcm_alaw,pcm_mulaw,adpcm_ima_wav,adpcm_ms,gsm_ms
  --enable-parser=aac,aac_latm,opus,vorbis,mpegaudio,flac
  --enable-filter=aresample,atempo,aformat,anull,abuffer,abuffersink,atrim,asetpts
  --enable-encoder=pcm_s16le
  --enable-muxer=wav,segment
  --extra-version=cxp-audio
)
if [[ "$platform" == win-arm64 ]]; then
  configure_flags+=(--cc=clang --cxx=clang++ --ar=llvm-ar --nm=llvm-nm --ranlib=llvm-ranlib)
fi
[[ ${#extra_cflags[@]} -gt 0 ]] && configure_flags+=("--extra-cflags=${extra_cflags[*]}")
[[ ${#extra_ldflags[@]} -gt 0 ]] && configure_flags+=("--extra-ldflags=${extra_ldflags[*]}")

# MSYS2 inherits a Windows-style TMPDIR (C:\...), which make cannot use.
tmp=${TMPDIR:-/tmp}
[[ "$tmp" == /* ]] || tmp=/tmp
work=$(mktemp -d "$tmp/ffmpeg-audio.XXXXXX")
trap 'rm -rf "$work"' EXIT
echo "== configuring $name"
(cd "$work" && "$src/configure" "${configure_flags[@]}") > "$work/configure.log" 2>&1 || {
  tail -60 "$work/configure.log" >&2; tail -60 "$work/ffbuild/config.log" >&2 || true; exit 1; }
# The license is decided by configure; refuse anything but LGPL.
grep -q '^License: LGPL version 2.1 or later' "$work/configure.log" || {
  grep '^License' "$work/configure.log" >&2; echo "ffmpeg is not LGPL-only" >&2; exit 1; }

echo "== building with $(command -v make) ($(make --version | head -1))"
make -C "$work" -j "$JOBS" ffmpeg > "$work/build.log" 2>&1 || {
  grep -E -B2 -A10 'error|Error [0-9]' "$work/build.log" | head -120 >&2
  echo "== last lines of the build log" >&2; tail -40 "$work/build.log" >&2; exit 1; }

out="$stage_parent/$name"
rm -rf "$out"
mkdir -p "$out"
cp "$work/$exe" "$out/$exe"
strip "$out/$exe" 2>/dev/null || llvm-strip "$out/$exe"
if [[ "$platform" == macos-* ]]; then
  codesign --force --sign - "$out/$exe" # stripping drops the linker's ad-hoc signature
fi
cp "$src/LICENSE.md" "$out/LICENSE.md"
cp "$src/COPYING.LGPLv2.1" "$out/COPYING.LGPLv2.1"

# What internal/asr runs must be there; what it never needs must not be.
"$out/$exe" -hide_banner -version > "$work/version.txt"
"$out/$exe" -hide_banner -filters > "$work/filters.txt" 2>&1
for f in atempo aresample; do
  grep -qw "$f" "$work/filters.txt" || { echo "ffmpeg lacks the $f filter" >&2; exit 1; }
done
"$out/$exe" -hide_banner -L > "$work/license.txt"
grep -q 'GNU Lesser General Public' "$work/license.txt" || { cat "$work/license.txt" >&2; exit 1; }
if grep -Eq -- '--enable-(gpl|nonfree|version3)' "$work/version.txt"; then
  echo "ffmpeg was configured with a non-LGPL-2.1 option" >&2; exit 1
fi

# Shared-library dependencies: only the OS's own.
case "$platform" in
  linux-*)
    deps=$(readelf -d "$out/$exe" | sed -n 's/.*(NEEDED).*\[\(.*\)\]/\1/p' | tr '\n' ' ')
    for d in $deps; do
      case "$d" in libc.so.6|libm.so.6|libpthread.so.0|libdl.so.2|librt.so.1|ld-linux*) ;;
        *) echo "ffmpeg needs $d" >&2; exit 1 ;; esac
    done
    glibc=$(objdump -T "$out/$exe" | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -V | tail -1)
    if [[ "$(printf '%s\n2.17\n' "$glibc" | sort -V | tail -1)" != 2.17 ]]; then
      echo "ffmpeg needs GLIBC_$glibc (> 2.17)" >&2; exit 1
    fi
    deps="$deps(glibc <= $glibc)" ;;
  macos-*)
    deps=$(otool -L "$out/$exe" | tail -n +2 | awk '{print $1}' | tr '\n' ' ')
    for d in $deps; do
      case "$d" in /usr/lib/*|/System/Library/*) ;; *) echo "ffmpeg needs $d" >&2; exit 1 ;; esac
    done ;;
  win-*)
    od=$(command -v objdump || command -v llvm-objdump)
    deps=$("$od" -p "$out/$exe" | sed -n 's/.*DLL Name: *//p' | tr '\n' ' ')
    for d in $deps; do
      case "$(tr '[:upper:]' '[:lower:]' <<<"$d")" in
        kernel32.dll|user32.dll|advapi32.dll|bcrypt.dll|ole32.dll|shell32.dll|shlwapi.dll|ws2_32.dll|msvcrt.dll|ucrtbase.dll|api-ms-win-*) ;;
        *) echo "ffmpeg.exe imports $d" >&2; exit 1 ;;
      esac
    done ;;
esac

{
  echo "Audio-only build of FFmpeg for codex-helper voice transcription."
  echo "FFmpeg is licensed under the GNU LGPL version 2.1 or later (LICENSE.md, COPYING.LGPLv2.1)."
  echo "This build is unmodified FFmpeg source; the complete source is published next to this"
  echo "archive as ffmpeg-audio-${FFMPEG_TAG}-source.tar.gz in the same release, and at"
  echo "https://github.com/FFmpeg/FFmpeg/tree/${FFMPEG_TAG}."
  echo
  echo "FFmpeg tag:     $FFMPEG_TAG"
  echo "FFmpeg commit:  $FFMPEG_COMMIT"
  echo "asset revision: $ASSET_REV"
  echo "platform:       $platform"
  echo "compiler:       $(grep -m1 -E '^C compiler' "$work/configure.log" || true)"
  echo "dependencies:   $deps"
  echo "configure:"
  printf '  %s\n' "${configure_flags[@]}"
  echo "ffmpeg -version:"
  sed 's/^/  /' "$work/version.txt"
} > "$out/BUILDINFO.txt"
cat "$out/BUILDINFO.txt"
