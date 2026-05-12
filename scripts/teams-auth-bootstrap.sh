#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: teams-auth-bootstrap.sh [options]

Interactively configure Teams Graph auth, run full Teams auth, and bootstrap
the Teams helper service.

Options:
  --codex-proxy PATH    codex-proxy/cxp executable to run
  --tenant-id ID        Microsoft Entra tenant ID
  --client-id ID        Teams Graph public client ID
  --no-open-control     pass --no-open-control to teams service bootstrap
  -h, --help            show this help

Environment defaults:
  CODEX_HELPER_TEAMS_SETUP_CXP
  CODEX_HELPER_TEAMS_SETUP_TENANT_ID or CODEX_HELPER_TEAMS_TENANT_ID
  CODEX_HELPER_TEAMS_SETUP_CLIENT_ID or CODEX_HELPER_TEAMS_CLIENT_ID
EOF
}

codex_proxy="${CODEX_HELPER_TEAMS_SETUP_CXP:-}"
tenant_id="${CODEX_HELPER_TEAMS_SETUP_TENANT_ID:-${CODEX_HELPER_TEAMS_TENANT_ID:-}}"
client_id="${CODEX_HELPER_TEAMS_SETUP_CLIENT_ID:-${CODEX_HELPER_TEAMS_CLIENT_ID:-}}"
no_open_control=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --codex-proxy)
      [ "$#" -ge 2 ] || { echo "missing value for --codex-proxy" >&2; exit 2; }
      codex_proxy="$2"
      shift 2
      ;;
    --tenant-id)
      [ "$#" -ge 2 ] || { echo "missing value for --tenant-id" >&2; exit 2; }
      tenant_id="$2"
      shift 2
      ;;
    --client-id)
      [ "$#" -ge 2 ] || { echo "missing value for --client-id" >&2; exit 2; }
      client_id="$2"
      shift 2
      ;;
    --no-open-control)
      no_open_control=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

section() {
  local title="$1"
  printf '\n============================================================\n'
  printf '%s\n' "$title"
  printf '============================================================\n\n'
}

fail() {
  printf 'Error: %s\n' "$*" >&2
  exit 1
}

resolve_codex_proxy() {
  if [ -n "$codex_proxy" ]; then
    return
  fi
  if command -v codex-proxy >/dev/null 2>&1; then
    codex_proxy="$(command -v codex-proxy)"
    return
  fi
  if command -v cxp >/dev/null 2>&1; then
    codex_proxy="$(command -v cxp)"
    return
  fi
  fail "could not find codex-proxy or cxp in PATH; rerun with --codex-proxy PATH"
}

prompt_required() {
  local name="$1"
  local prompt="$2"
  local value="$3"
  while [ -z "${value//[[:space:]]/}" ]; do
    printf '%s: ' "$prompt" >&2
    if ! IFS= read -r value; then
      fail "$name is required"
    fi
  done
  printf '%s' "$value"
}

run_cxp() {
  "$codex_proxy" "$@"
}

section "STEP 1/4: Configure Teams Graph auth"
resolve_codex_proxy
tenant_id="$(prompt_required "tenant id" "Microsoft Entra tenant ID" "$tenant_id")"
client_id="$(prompt_required "client id" "Teams Graph public client ID" "$client_id")"

printf 'Using: %s\n' "$codex_proxy"
printf 'This writes local auth metadata only. The client ID is not a secret.\n'
run_cxp teams auth config \
  --tenant-id "$tenant_id" \
  --read-client-id "$client_id" \
  --client-id "$client_id" \
  --file-write-client-id "$client_id" \
  --full-client-id "$client_id"

section "STEP 2/4: Sign in with Microsoft device login"
printf 'A device login code may appear next. Open the shown URL, enter the code, and finish SSO/MFA.\n'
run_cxp teams auth full

section "STEP 3/4: Verify local Teams auth cache"
run_cxp teams auth full-status

section "STEP 4/4: Bootstrap the Teams helper service"
printf 'If Windows or WSL asks for permission, follow the prompt. When bootstrap asks for confirmation, type yes and press Enter.\n'
bootstrap_args=(teams service bootstrap)
if [ "$no_open_control" -eq 1 ]; then
  bootstrap_args+=(--no-open-control)
fi
run_cxp "${bootstrap_args[@]}"

section "DONE"
printf 'Teams auth and service bootstrap completed.\n'
printf 'Next: open the Teams control chat shown by bootstrap and send help.\n'
