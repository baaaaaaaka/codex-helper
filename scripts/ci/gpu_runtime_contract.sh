#!/usr/bin/env bash
set -euo pipefail

mode="${1:-all}"
image="${CXP_GPU_CONTRACT_IMAGE:-nvidia/cuda:12.8.1-base-ubuntu22.04}"

case "$mode" in
  host|docker|all) ;;
  *) echo "usage: $0 [host|docker|all]" >&2; exit 2 ;;
esac

command -v nvidia-smi >/dev/null
command -v nvcc >/dev/null
nvidia-smi --query-gpu=name --format=csv,noheader

work="$(mktemp -d "${TMPDIR:-/tmp}/cxp-gpu-contract-XXXXXX")"
trap 'rm -rf "$work"' EXIT

cat > "$work/probe.cu" <<'CUDA'
#include <cuda_runtime.h>
#include <cstdio>
__global__ void add_one(int *value) { *value += 1; }
int main() {
  int initial = 41;
  int result = 0;
  int *device = nullptr;
  if (cudaMalloc(&device, sizeof(int)) != cudaSuccess) return 2;
  if (cudaMemcpy(device, &initial, sizeof(int), cudaMemcpyHostToDevice) != cudaSuccess) return 3;
  add_one<<<1, 1>>>(device);
  if (cudaDeviceSynchronize() != cudaSuccess) return 4;
  if (cudaMemcpy(&result, device, sizeof(int), cudaMemcpyDeviceToHost) != cudaSuccess) return 5;
  cudaFree(device);
  if (result != 42) return 6;
  std::printf("cxp-cuda-ok:%d\n", result);
  return 0;
}
CUDA

nvcc -O2 "$work/probe.cu" -o "$work/probe"

if [[ "$mode" == "host" || "$mode" == "all" ]]; then
  "$work/probe" | tee "$work/host.out"
  grep -qx 'cxp-cuda-ok:42' "$work/host.out"
fi

if [[ "$mode" == "docker" || "$mode" == "all" ]]; then
  command -v docker >/dev/null
  printf 'bind-mount-ok\n' > "$work/input.txt"
  docker run --rm --gpus all --ipc=host \
    -v "$work:/contract" -w /contract "$image" \
    bash -lc 'test "$(cat input.txt)" = bind-mount-ok; ./probe | tee docker.out; grep -qx "cxp-cuda-ok:42" docker.out; test -e /dev/nvidiactl || test -e /dev/dxg'
  grep -qx 'cxp-cuda-ok:42' "$work/docker.out"
fi

echo "GPU runtime contract passed ($mode)"
