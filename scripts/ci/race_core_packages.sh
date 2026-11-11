#!/usr/bin/env bash
# One package-level race group; --plan lists the exact packages without running tests.
set -euo pipefail
if [[ $# -lt 1 || $# -gt 2 ]] || [[ "$1" != migrate && "$1" != store && "$1" != engine ]]; then
  echo "usage: $0 {migrate|store|engine} [--plan]" >&2
  exit 2
fi
group=$1
if [[ $# == 2 && "$2" != --plan ]]; then echo "unknown option: $2" >&2; exit 2; fi
packages_text=$(go list ./internal/core/... ./internal/channel/... ./internal/asr)
packages=()
while IFS= read -r pkg; do
  [[ -n "$pkg" ]] || continue
  case "$pkg" in
    */internal/core/migrate|*/internal/core/migrate/*) selected=migrate ;;
    */internal/core/engine|*/internal/core/engine/*|*/internal/asr|*/internal/asr/*) selected=engine ;;
    *) selected=store ;;
  esac
  [[ "$selected" != "$group" ]] || packages+=("$pkg")
done <<< "$packages_text"
if (( ${#packages[@]} == 0 )); then echo "empty race group: $group" >&2; exit 1; fi
if [[ ${2:-} == --plan ]]; then printf '%s\n' "${packages[@]}"; exit 0; fi
exec go test -race -p=1 -count=1 -timeout=15m -parallel=8 "${packages[@]}"
