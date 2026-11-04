#!/usr/bin/env bash
# Build the managed Teams voice-transcription runtime (llama.cpp llama-server
# plus its shared libraries) against an old glibc, inside a manylinux
# container, and package it as one release asset.
#
# Usage (from the repository root):
#   docker run --rm -v "$PWD":/work:ro -v "$OUT":/out \
#     quay.io/pypa/manylinux2014_x86_64 /work/scripts/ci/build-llama-asr.sh
#
# Environment:
#   LLAMA_TAG       llama.cpp release tag (default b9437)
#   LLAMA_COMMIT    expected commit of LLAMA_TAG (verified after checkout)
#   LLAMA_SRC       existing checkout to build instead of cloning (read-only ok)
#   ASSET_REV       our packaging revision (default r1)
#   ASSET_PLATFORM  linux-x64 | linux-arm64 (default from uname -m)
#   OUT_DIR         where the tarball and pins are written (default /out)
#   MAX_GLIBC       highest GLIBC_ symbol version allowed (default 2.17)
#   MAX_GLIBCXX     highest GLIBCXX_ symbol version allowed (default 3.4.19)
#   EXTRA_CMAKE     extra cmake flags (e.g. -DGGML_VULKAN=ON for a GPU build)
#   JOBS            parallel build jobs (default nproc)
#
# The script fails if any shipped ELF needs a newer glibc/libstdc++ than the
# limits, links libgomp, or lacks an $ORIGIN runpath.
set -euo pipefail

LLAMA_TAG=${LLAMA_TAG:-b9437}
LLAMA_COMMIT=${LLAMA_COMMIT:-aa46bda89b9a8378ae76bb15fc2ce2f571f0983c}
ASSET_REV=${ASSET_REV:-r1}
OUT_DIR=${OUT_DIR:-/out}
MAX_GLIBC=${MAX_GLIBC:-2.17}
MAX_GLIBCXX=${MAX_GLIBCXX:-3.4.19}
EXTRA_CMAKE=${EXTRA_CMAKE:-}
JOBS=${JOBS:-$(nproc)}

case "${ASSET_PLATFORM:-$(uname -m)}" in
  x86_64|linux-x64) ASSET_PLATFORM=linux-x64 ;;
  aarch64|linux-arm64) ASSET_PLATFORM=linux-arm64 ;;
  *) echo "unsupported platform ${ASSET_PLATFORM:-$(uname -m)}" >&2; exit 2 ;;
esac

NAME="llama-asr-${LLAMA_TAG}-${ASSET_REV}-${ASSET_PLATFORM}"
WORK=$(mktemp -d /tmp/llama-asr.XXXXXX)
trap 'rm -rf "$WORK"' EXIT

log() { printf '== %s\n' "$*"; }

# 1. Source at the pinned tag.
if [[ -n "${LLAMA_SRC:-}" ]]; then
  log "copying source from $LLAMA_SRC"
  cp -a "$LLAMA_SRC" "$WORK/src"
else
  log "cloning llama.cpp $LLAMA_TAG"
  git clone --quiet --depth 1 --branch "$LLAMA_TAG" https://github.com/ggml-org/llama.cpp.git "$WORK/src"
fi
got_commit=$(git -C "$WORK/src" rev-parse HEAD 2>/dev/null || echo unknown)
if [[ "$got_commit" != "$LLAMA_COMMIT" ]]; then
  echo "llama.cpp $LLAMA_TAG resolved to $got_commit, want $LLAMA_COMMIT" >&2
  exit 1
fi

# gcc 10 (the newest compiler manylinux2014 ships) cannot emit AVX-VNNI or AMX
# code. Drop only those two CPU variants; such CPUs then use the haswell /
# zen4 variant instead. Record what was dropped in BUILDINFO.
dropped_variants=""
drop_variant_unless() { # <variant> <compiler flag>
  if ! echo 'int main(void){return 0;}' | ${CC:-cc} -x c "$2" -c -o /dev/null - 2>/dev/null; then
    sed -i "/ggml_add_cpu_backend_variant($1 /d" "$WORK/src/ggml/src/CMakeLists.txt"
    dropped_variants="$dropped_variants $1"
  fi
}
drop_variant_unless alderlake -mavxvnni
drop_variant_unless sapphirerapids -mamx-tile

# 2. Configure. Notes:
#  - GGML_BACKEND_DL + GGML_CPU_ALL_VARIANTS: one tarball serves every x86-64
#    CPU level (SSE4.2 .. AVX-512); ggml picks the best variant at runtime.
#  - GGML_OPENMP=OFF: no libgomp dependency (old hosts often lack it).
#  - LLAMA_OPENSSL=OFF, LLAMA_BUILD_UI=OFF: no TLS, no web UI, no npm.
#  - $ORIGIN runpath: the binaries find their own libraries without
#    LD_LIBRARY_PATH.
log "configuring (jobs=$JOBS, extra='$EXTRA_CMAKE')"
cmake_flags=(
  -DCMAKE_BUILD_TYPE=Release
  -DBUILD_SHARED_LIBS=ON
  -DCMAKE_INSTALL_RPATH='$ORIGIN'
  -DCMAKE_BUILD_WITH_INSTALL_RPATH=ON
  -DGGML_BACKEND_DL=ON
  -DGGML_NATIVE=OFF
  -DGGML_OPENMP=OFF
  -DLLAMA_OPENSSL=OFF
  -DLLAMA_BUILD_UI=OFF
  -DLLAMA_BUILD_TESTS=OFF
  -DLLAMA_BUILD_EXAMPLES=OFF
  -DLLAMA_BUILD_APP=OFF
  -DLLAMA_BUILD_TOOLS=ON
  -DLLAMA_BUILD_SERVER=ON
)
if [[ "$ASSET_PLATFORM" == linux-x64 ]]; then
  cmake_flags+=(-DGGML_CPU_ALL_VARIANTS=ON)
fi
# shellcheck disable=SC2206
cmake_flags+=($EXTRA_CMAKE)
cmake -S "$WORK/src" -B "$WORK/build" "${cmake_flags[@]}" > "$WORK/configure.log" 2>&1 || {
  tail -50 "$WORK/configure.log" >&2; exit 1; }

log "building"
cmake --build "$WORK/build" --config Release -j "$JOBS" > "$WORK/build.log" 2>&1 || {
  grep -E -A12 'error:|Error [0-9]|undefined reference' "$WORK/build.log" | head -150 >&2; exit 1; }

# 3. Stage only what the managed runtime needs.
stage="$WORK/stage/$NAME"
mkdir -p "$stage"
# Only llama-server: internal/asr never runs llama-mtmd-cli or other tools.
cp "$WORK/build/bin/llama-server" "$stage/"
# Shared libraries are stored under their SONAME as plain files (no symlinks),
# so the archive extracts the same way everywhere. Tool-only "-impl" libraries
# other than the server's are left out.
for f in "$WORK"/build/bin/*.so*; do
  [[ -L "$f" ]] && continue
  base=$(basename "$f")
  case "$base" in libllama-*-impl.so) [[ "$base" == libllama-server-impl.so ]] || continue ;; esac
  soname=$(readelf -d "$f" | sed -n 's/.*(SONAME).*\[\(.*\)\]/\1/p')
  cp "$f" "$stage/${soname:-$base}"
done
cp "$WORK/src/LICENSE" "$stage/LICENSE-llama.cpp"
find "$stage" -type f \( -name 'llama-*' -o -name '*.so*' \) -exec strip --strip-unneeded {} \;

# 4. Verify the ABI floor of every ELF we ship.
log "verifying symbol versions (GLIBC <= $MAX_GLIBC, GLIBCXX <= $MAX_GLIBCXX)"
vmax() { sort -V | tail -1; }
le() { [[ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -1)" == "$2" ]]; }
report="$WORK/abi.txt"
: > "$report"
fail=0
for f in "$stage"/*; do
  [[ -f "$f" && ! -L "$f" && "$(head -c4 "$f" | tr -d '\0')" == $'\x7fELF' ]] || continue
  base=$(basename "$f")
  g=$(objdump -T "$f" | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | vmax || true)
  gx=$(objdump -T "$f" | grep -o 'GLIBCXX_[0-9.]*' | sed 's/GLIBCXX_//' | vmax || true)
  needed=$(readelf -d "$f" | sed -n 's/.*(NEEDED).*\[\(.*\)\]/\1/p' | tr '\n' ' ')
  runpath=$(readelf -d "$f" | sed -n 's/.*(\(RUNPATH\|RPATH\)).*\[\(.*\)\]/\2/p')
  printf '%-34s GLIBC_%-6s GLIBCXX_%-8s runpath=%-8s needs: %s\n' "$base" "${g:--}" "${gx:--}" "${runpath:--}" "$needed" | tee -a "$report"
  if [[ -n "$g" ]] && ! le "$g" "$MAX_GLIBC"; then echo "  !! $base needs GLIBC_$g" >&2; fail=1; fi
  if [[ -n "$gx" ]] && ! le "$gx" "$MAX_GLIBCXX"; then echo "  !! $base needs GLIBCXX_$gx" >&2; fail=1; fi
  if grep -q 'libgomp' <<<"$needed"; then echo "  !! $base links libgomp" >&2; fail=1; fi
  if [[ "$runpath" != *'$ORIGIN'* ]] && grep -q -E 'libllama|libggml|libmtmd' <<<"$needed"; then
    echo "  !! $base lacks an \$ORIGIN runpath" >&2; fail=1
  fi
done
[[ $fail == 0 ]] || { echo "ABI verification failed" >&2; exit 1; }

"$stage/llama-server" --version > "$WORK/version.txt" 2>&1 || {
  cat "$WORK/version.txt" >&2; echo "llama-server --version failed" >&2; exit 1; }

{
  echo "llama.cpp tag:    $LLAMA_TAG"
  echo "llama.cpp commit: $got_commit"
  echo "asset revision:   $ASSET_REV"
  echo "platform:         $ASSET_PLATFORM"
  echo "built in:         $(cat /etc/redhat-release 2>/dev/null || uname -a)"
  echo "compiler:         $(${CXX:-c++} --version | head -1)"
  echo "build glibc:      $(ldd --version | head -1)"
  echo "cmake flags:      ${cmake_flags[*]}"
  echo "dropped CPU variants (compiler too old):${dropped_variants:- none}"
  echo "llama-server --version:"
  sed 's/^/  /' "$WORK/version.txt"
  echo "ABI report:"
  sed 's/^/  /' "$report"
} > "$stage/BUILDINFO.txt"

# 5. Package with a stable file order and mtime so rebuilds of the same
#    inputs differ only where the compiler output differs.
log "packaging $NAME.tar.gz"
mkdir -p "$OUT_DIR"
(cd "$WORK/stage" && find "$NAME" -print | LC_ALL=C sort |
  tar --no-recursion --mtime='2026-01-01 00:00:00' --owner=0 --group=0 --numeric-owner -cf - -T - |
  gzip -n -9) > "$OUT_DIR/$NAME.tar.gz"
sha=$(sha256sum "$OUT_DIR/$NAME.tar.gz" | cut -d' ' -f1)
size=$(stat -c %s "$OUT_DIR/$NAME.tar.gz")
cp "$stage/BUILDINFO.txt" "$OUT_DIR/$NAME.BUILDINFO.txt"
printf '{"name":"%s.tar.gz","sha256":"%s","size":%s,"platform":"%s","binary":"%s/llama-server"}\n' \
  "$NAME" "$sha" "$size" "$ASSET_PLATFORM" "$NAME" > "$OUT_DIR/$NAME.pin.json"
cat "$OUT_DIR/$NAME.pin.json"
log "done"
