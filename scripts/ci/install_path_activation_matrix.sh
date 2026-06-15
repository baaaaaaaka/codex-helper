#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
require_all="${CODEX_PROXY_REQUIRE_SHELL_MATRIX:-0}"
matrix_cases="${CODEX_PROXY_SHELL_MATRIX_CASES:-sh bash zsh fish csh tcsh}"
tmp_root="$(mktemp -d)"
trap 'rm -rf "$tmp_root"' EXIT

goos="$(uname -s)"
case "$goos" in
  Linux) goos="linux" ;;
  Darwin) goos="darwin" ;;
  *)
    echo "unsupported OS for install PATH activation matrix: $goos" >&2
    exit 1
    ;;
esac

goarch="$(uname -m)"
case "$goarch" in
  x86_64|amd64) goarch="amd64" ;;
  aarch64|arm64) goarch="arm64" ;;
  armv7l) goarch="arm" ;;
esac

version="v1.2.3"
version_no_v="${version#v}"
asset="codex-proxy_${version_no_v}_${goos}_${goarch}"
asset_data='#!/bin/sh
if [ "$1" = "--version" ]; then
  echo codex-proxy 1.2.3
  exit 0
fi
if [ "$1" = "skills" ]; then
  exit 0
fi
exit 0
'

checksum() {
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' "$asset_data" | sha256sum | awk '{print $1}'
    return
  fi
  printf '%s' "$asset_data" | shasum -a 256 | awk '{print $1}'
}

csh_quote() {
  printf "'"
  printf '%s' "$1" | sed "s/'/'\\\\''/g"
  printf "'"
}

fake_bin="$tmp_root/fake-bin"
mkdir -p "$fake_bin"
cat > "$fake_bin/curl" <<'SH'
#!/usr/bin/env sh
set -e

out=""
write_effective=""
url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o)
      out="$2"
      shift 2
      ;;
    -w)
      write_effective="$2"
      shift 2
      ;;
    -*)
      shift
      ;;
    *)
      url="$1"
      shift
      ;;
  esac
done

if [ -n "$write_effective" ]; then
  printf '%s' "${CODEX_PROXY_TEST_LATEST_URL:-}"
  exit 0
fi

if [ -z "$out" ]; then
  exit 1
fi

case "$url" in
  *"/repos/"*"/releases/latest")
    printf '%s' "${CODEX_PROXY_TEST_API_JSON:-}" > "$out"
    ;;
  *"/checksums.txt")
    printf '%s' "${CODEX_PROXY_TEST_CHECKSUMS:-}" > "$out"
    ;;
  *"/${CODEX_PROXY_TEST_ASSET}")
    printf '%s' "${CODEX_PROXY_TEST_ASSET_DATA:-}" > "$out"
    ;;
  *)
    echo "unexpected curl URL: $url" >&2
    exit 22
    ;;
esac
SH
chmod +x "$fake_bin/curl"

run_case() {
  local name="$1"
  local shell_bin="$2"
  local kind="$3"
  local shell_path
  shell_path="$(command -v "$shell_bin" || true)"
  if [ -z "$shell_path" ]; then
    if [ "$require_all" = "1" ]; then
      echo "required shell missing: $name ($shell_bin)" >&2
      exit 1
    fi
    echo "skipping missing shell: $name ($shell_bin)"
    return 0
  fi

  local path_suffix=' [brackets] (paren) dollar$ dq" tick`'
  local home_suffix="$path_suffix"
  # Ubuntu csh fails before reading any script when HOME itself contains
  # brackets or a backtick. Keep the install and managed-bin paths maximally
  # adversarial, but use a csh-startable HOME so the matrix tests installer
  # behavior instead of csh startup parsing.
  if [ "$name" = "csh" ]; then
    home_suffix=' space (paren) dollar$ dq" semi;colon tick'
  fi
  local home="$tmp_root/home dir-$name$home_suffix"
  local install_dir="$tmp_root/install dir-$name$path_suffix"
  local managed_prefix="$tmp_root/managed prefix-$name$path_suffix"
  local install_log="$tmp_root/install-$name.log"
  mkdir -p "$home"
  if [ "$name" = "tcsh" ]; then
    printf '# existing tcshrc\n' > "$home/.tcshrc"
  fi

  env \
    HOME="$home" \
    XDG_CONFIG_HOME="$home/.config" \
    SHELL="$shell_path" \
    PATH="$fake_bin:$PATH" \
    CODEX_PROXY_REPO="owner/name" \
    CODEX_PROXY_VERSION="latest" \
    CODEX_PROXY_INSTALL_DIR="$install_dir" \
    CODEX_NPM_PREFIX="$managed_prefix" \
    CODEX_PROXY_SKIP_BUILTIN_SKILLS=1 \
    CODEX_PROXY_TEST_LATEST_URL="https://github.com/owner/name/releases/tag/$version" \
    CODEX_PROXY_TEST_API_JSON="{\"tag_name\":\"$version\"}" \
    CODEX_PROXY_TEST_ASSET="$asset" \
    CODEX_PROXY_TEST_ASSET_DATA="$asset_data" \
    CODEX_PROXY_TEST_CHECKSUMS="$(checksum)  $asset" \
    sh "$repo_root/install.sh" >"$install_log" 2>&1

  if grep -Fq "reload attempted" "$install_log"; then
    echo "installer claimed runtime reload for $name" >&2
    cat "$install_log" >&2
    exit 1
  fi

  echo "==> explicit PATH activation: $name"
  case "$kind" in
    posix)
      env -i HOME="$home" PATH="/usr/bin:/bin" "$shell_path" -c '. "$HOME/.config/codex-proxy/shell/path.sh"; cxp --version'
      ;;
    zsh)
      env -i HOME="$home" PATH="/usr/bin:/bin" "$shell_path" -c 'source "$HOME/.config/codex-proxy/shell/path.sh"; cxp --version'
      ;;
    fish)
      env -i HOME="$home" PATH="/usr/bin:/bin" "$shell_path" -c 'source "$HOME/.config/fish/conf.d/codex-proxy-path.fish"; cxp --version'
      ;;
    csh)
      csh_script="$tmp_root/activate-$name.csh"
      {
        printf 'source %s\n' "$(csh_quote "$home/.config/codex-proxy/shell/path.csh")"
        printf '%s\n' 'cxp --version'
      } >"$csh_script"
      env -i HOME="$home" PATH="/usr/bin:/bin" "$shell_path" -f "$csh_script"
      ;;
    *)
      echo "unknown activation kind: $kind" >&2
      exit 1
      ;;
  esac

  echo "==> future shell startup: $name"
  case "$name:$kind" in
    sh:posix)
      echo "skipping future startup check for plain sh; startup file semantics are not portable"
      ;;
    bash:posix)
      env -i HOME="$home" PATH="/usr/bin:/bin" "$shell_path" -lc 'cxp --version'
      env -i HOME="$home" PATH="/usr/bin:/bin" "$shell_path" -ic 'cxp --version'
      ;;
    zsh:zsh)
      env -i HOME="$home" PATH="/usr/bin:/bin" "$shell_path" -lc 'cxp --version'
      env -i HOME="$home" PATH="/usr/bin:/bin" "$shell_path" -ic 'cxp --version'
      ;;
    fish:fish)
      env -i HOME="$home" PATH="/usr/bin:/bin" "$shell_path" -lc 'cxp --version'
      env -i HOME="$home" PATH="/usr/bin:/bin" "$shell_path" -ic 'cxp --version'
      ;;
    csh:csh|tcsh:csh)
      env -i HOME="$home" PATH="/usr/bin:/bin" "$shell_path" -c 'cxp --version'
      ;;
  esac

  echo "==> stale command cache refresh: $name"
  case "$kind" in
    posix)
      env -i HOME="$home" PATH="/usr/bin:/bin" INSTALL_DIR="$install_dir" "$shell_path" -c '
        mv "$INSTALL_DIR/cxp" "$INSTALL_DIR/cxp.hidden"
        PATH="$INSTALL_DIR:$PATH"
        cxp >/dev/null 2>&1 || true
        mv "$INSTALL_DIR/cxp.hidden" "$INSTALL_DIR/cxp"
        . "$HOME/.config/codex-proxy/shell/path.sh"
        cxp --version
      '
      ;;
    zsh)
      env -i HOME="$home" PATH="/usr/bin:/bin" INSTALL_DIR="$install_dir" "$shell_path" -c '
        mv "$INSTALL_DIR/cxp" "$INSTALL_DIR/cxp.hidden"
        PATH="$INSTALL_DIR:$PATH"
        cxp >/dev/null 2>&1 || true
        mv "$INSTALL_DIR/cxp.hidden" "$INSTALL_DIR/cxp"
        source "$HOME/.config/codex-proxy/shell/path.sh"
        cxp --version
      '
      ;;
    fish)
      env -i HOME="$home" PATH="/usr/bin:/bin" INSTALL_DIR="$install_dir" "$shell_path" -c '
        mv "$INSTALL_DIR/cxp" "$INSTALL_DIR/cxp.hidden"
        set -gx PATH "$INSTALL_DIR" $PATH
        cxp >/dev/null 2>&1; or true
        mv "$INSTALL_DIR/cxp.hidden" "$INSTALL_DIR/cxp"
        source "$HOME/.config/fish/conf.d/codex-proxy-path.fish"
        cxp --version
      '
      ;;
    csh)
      csh_script="$tmp_root/stale-cache-$name.csh"
      {
        printf 'mv %s %s\n' "$(csh_quote "$install_dir/cxp")" "$(csh_quote "$install_dir/cxp.hidden")"
        printf 'set _path_entry = %s\n' "$(csh_quote "$install_dir")"
        printf '%s\n' 'if ( $?path ) then'
        printf '%s\n' '  set path = ( "$_path_entry" $path:q )'
        printf '%s\n' 'else'
        printf '%s\n' '  set path = ( "$_path_entry" )'
        printf '%s\n' 'endif'
        printf '%s\n' 'unset _path_entry'
        printf '%s\n' 'cxp >& /dev/null'
        printf 'mv %s %s\n' "$(csh_quote "$install_dir/cxp.hidden")" "$(csh_quote "$install_dir/cxp")"
        printf 'source %s\n' "$(csh_quote "$home/.config/codex-proxy/shell/path.csh")"
        printf '%s\n' 'cxp --version'
      } >"$csh_script"
      env -i HOME="$home" PATH="/usr/bin:/bin" "$shell_path" -f "$csh_script"
      ;;
  esac
}

for matrix_case in $matrix_cases; do
  case "$matrix_case" in
    sh) run_case "sh" "sh" "posix" ;;
    bash) run_case "bash" "bash" "posix" ;;
    zsh) run_case "zsh" "zsh" "zsh" ;;
    fish) run_case "fish" "fish" "fish" ;;
    csh) run_case "csh" "csh" "csh" ;;
    tcsh) run_case "tcsh" "tcsh" "csh" ;;
    *)
      echo "unknown shell matrix case: $matrix_case" >&2
      exit 1
      ;;
  esac
done
