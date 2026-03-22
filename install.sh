#!/usr/bin/env sh
set -eu

usage() {
  cat <<'EOF'
codex-proxy installer (no root required)

Usage:
  ./install.sh [--repo owner/name] [--version vX.Y.Z|X.Y.Z|latest] [--dir <install-dir>]

Defaults:
  --repo    baaaaaaaka/codex-helper
  --version latest
  --dir     $HOME/.local/bin

Examples:
  ./install.sh
  ./install.sh --version v0.0.28
  ./install.sh --dir "$HOME/.local/bin"
  ./install.sh --repo baaaaaaaka/codex-helper --version v0.0.28
EOF
}

repo="${CODEX_PROXY_REPO:-baaaaaaaka/codex-helper}"
version="${CODEX_PROXY_VERSION:-latest}"
install_dir="${CODEX_PROXY_INSTALL_DIR:-${HOME:-}/.local/bin}"
api_base="${CODEX_PROXY_API_BASE:-https://api.github.com}"
release_base="${CODEX_PROXY_RELEASE_BASE:-https://github.com}"
api_base="${api_base%/}"
release_base="${release_base%/}"

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --repo)
      repo="$2"
      shift 2
      ;;
    --version)
      version="$2"
      shift 2
      ;;
    --dir)
      install_dir="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

os="$(uname -s 2>/dev/null || echo unknown)"
arch="$(uname -m 2>/dev/null || echo unknown)"

case "$os" in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *)
    echo "Unsupported OS: $os" >&2
    exit 1
    ;;
esac

case "$arch" in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *)
    echo "Unsupported architecture: $arch" >&2
    exit 1
    ;;
esac

shell_name="$(basename "${SHELL:-}")"
if [ -z "${shell_name:-}" ]; then
  shell_name="sh"
fi

have_cmd() { command -v "$1" >/dev/null 2>&1; }

file_contains_text() {
  file="$1"
  text="$2"
  [ -f "$file" ] || return 1
  LC_ALL=C grep -aF "$text" "$file" >/dev/null 2>&1
}

is_codex_owned_legacy_file() {
  file="$1"
  file_contains_text "$file" "github.com/baaaaaaaka/codex-helper" && return 0
  file_contains_text "$file" "codex-proxy" && return 0
  return 1
}

resolve_legacy_target_path() {
  base_dir="$1"
  target="$2"
  case "$target" in
    /*) printf '%s\n' "$target" ;;
    *) printf '%s/%s\n' "$base_dir" "$target" ;;
  esac
}

curl_run() {
  curl --retry 5 --retry-delay 5 --connect-timeout 30 -fsSL "$@"
}

wget_run() {
  if wget --help 2>/dev/null | grep -q -- '--waitretry'; then
    wget --tries=5 --waitretry=5 --timeout=30 "$@"
    return 0
  fi

  attempt=1
  while [ "$attempt" -le 5 ]; do
    if wget "$@"; then
      return 0
    fi
    if [ "$attempt" -eq 5 ]; then
      break
    fi
    sleep 5
    attempt=$((attempt + 1))
  done

  return 1
}

http_get() {
  url="$1"
  out="$2"
  if have_cmd curl; then
    curl_run -o "$out" "$url"
    return 0
  fi
  if have_cmd wget; then
    wget_run -q -O "$out" "$url"
    return 0
  fi
  echo "Missing downloader: need curl or wget" >&2
  return 1
}

CONFIG_UPDATED=0
SOURCE_FILES=""

add_source_file() {
  file="$1"
  case "
$SOURCE_FILES
" in
    *"
$file
"*) ;;
    *)
      if [ -n "$SOURCE_FILES" ]; then
        SOURCE_FILES="$SOURCE_FILES
$file"
      else
        SOURCE_FILES="$file"
      fi
      ;;
  esac
}

remove_line() {
  file="$1"
  line="$2"
  if [ -z "${file:-}" ] || [ -z "${line:-}" ] || [ ! -f "$file" ]; then
    return 0
  fi

  tmp="$file.codex-proxy.$$"
  grep -Fvx "$line" "$file" >"$tmp" 2>/dev/null || true
  if ! cmp -s "$file" "$tmp" 2>/dev/null; then
    cat "$tmp" >"$file"
    CONFIG_UPDATED=1
    add_source_file "$file"
  fi
  rm -f "$tmp"
}

ensure_line() {
  file="$1"
  line="$2"
  if [ -z "${file:-}" ] || [ -z "${line:-}" ]; then
    return 0
  fi
  dir="$(dirname "$file")"
  if [ -n "${dir:-}" ]; then
    mkdir -p "$dir" 2>/dev/null || true
  fi
  if [ ! -f "$file" ]; then
    : > "$file"
  fi
  if ! grep -Fqx "$line" "$file" 2>/dev/null; then
    printf "\n%s\n" "$line" >> "$file"
    CONFIG_UPDATED=1
    add_source_file "$file"
  fi
}

escape_double_quotes() {
  if have_cmd sed; then
    printf '%s' "$1" | sed 's/[\\\"`$]/\\&/g'
    return 0
  fi
  printf '%s' "$1"
}

resolve_dir_path() {
  dir="$1"
  if [ -z "${dir:-}" ]; then
    return 0
  fi
  mkdir -p "$dir" 2>/dev/null || true
  resolved="$(cd "$dir" 2>/dev/null && pwd -P || true)"
  if [ -n "${resolved:-}" ]; then
    printf '%s' "$resolved"
    return 0
  fi
  printf '%s' "$dir"
}

write_posix_path_snippet() {
  file="$1"
  install_path="$2"
  managed_path="$3"

  dir="$(dirname "$file")"
  mkdir -p "$dir" 2>/dev/null || true

  tmp="$file.codex-proxy.$$"
  install_escaped="$(escape_double_quotes "$install_path")"
  managed_escaped="$(escape_double_quotes "$managed_path")"
  {
    printf '%s\n' "# added by codex-proxy installer"
    printf '_path_entry="%s"\n' "$install_escaped"
    cat <<'EOF'
case ":$PATH:" in
  *":$_path_entry:"*) ;;
  *) PATH="$_path_entry${PATH:+:$PATH}" ;;
esac
EOF
    if [ -n "${managed_path:-}" ]; then
      printf '\n'
      printf '_path_entry="%s"\n' "$managed_escaped"
      cat <<'EOF'
case ":$PATH:" in
  *":$_path_entry:"*) ;;
  *) PATH="$_path_entry${PATH:+:$PATH}" ;;
esac
EOF
    fi
    printf '\n%s\n' 'export PATH'
    printf '%s\n' 'unset _path_entry'
  } >"$tmp"

  if [ ! -f "$file" ] || ! cmp -s "$file" "$tmp" 2>/dev/null; then
    cat "$tmp" >"$file"
    CONFIG_UPDATED=1
    add_source_file "$file"
  fi
  rm -f "$tmp"
}

write_fish_path_snippet() {
  file="$1"
  install_path="$2"
  managed_path="$3"

  dir="$(dirname "$file")"
  mkdir -p "$dir" 2>/dev/null || true

  tmp="$file.codex-proxy.$$"
  install_escaped="$(escape_double_quotes "$install_path")"
  managed_escaped="$(escape_double_quotes "$managed_path")"
  {
    printf '%s\n' "# added by codex-proxy installer"
    printf 'if not contains -- "%s" $PATH\n' "$install_escaped"
    printf '  set -gx PATH "%s" $PATH\n' "$install_escaped"
    printf '%s\n' 'end'
    if [ -n "${managed_path:-}" ]; then
      printf '\n'
      printf 'if not contains -- "%s" $PATH\n' "$managed_escaped"
      printf '  set -gx PATH "%s" $PATH\n' "$managed_escaped"
      printf '%s\n' 'end'
    fi
  } >"$tmp"

  if [ ! -f "$file" ] || ! cmp -s "$file" "$tmp" 2>/dev/null; then
    cat "$tmp" >"$file"
    CONFIG_UPDATED=1
    add_source_file "$file"
  fi
  rm -f "$tmp"
}

write_csh_path_snippet() {
  file="$1"
  install_path="$2"
  managed_path="$3"

  dir="$(dirname "$file")"
  mkdir -p "$dir" 2>/dev/null || true

  tmp="$file.codex-proxy.$$"
  install_escaped="$(escape_double_quotes "$install_path")"
  managed_escaped="$(escape_double_quotes "$managed_path")"
  {
    printf '%s\n' "# added by codex-proxy installer"
    printf 'set _path_entry = "%s"\n' "$install_escaped"
    cat <<'EOF'
if ( $?PATH ) then
  if ( ":$PATH:" !~ *":$_path_entry:"* ) then
    setenv PATH "$_path_entry:$PATH"
  endif
else
  setenv PATH "$_path_entry"
endif
EOF
    if [ -n "${managed_path:-}" ]; then
      printf '\n'
      printf 'set _path_entry = "%s"\n' "$managed_escaped"
      cat <<'EOF'
if ( $?PATH ) then
  if ( ":$PATH:" !~ *":$_path_entry:"* ) then
    setenv PATH "$_path_entry:$PATH"
  endif
else
  setenv PATH "$_path_entry"
endif
EOF
    fi
    printf '\n%s\n' 'unset _path_entry'
  } >"$tmp"

  if [ ! -f "$file" ] || ! cmp -s "$file" "$tmp" 2>/dev/null; then
    cat "$tmp" >"$file"
    CONFIG_UPDATED=1
    add_source_file "$file"
  fi
  rm -f "$tmp"
}

source_config_file() {
  file="$1"
  if [ -z "${file:-}" ] || [ ! -f "$file" ]; then
    return 0
  fi
  case "$shell_name" in
    bash)
      if have_cmd bash; then
        bash -c ". \"$file\"" >/dev/null 2>&1 || true
      fi
      ;;
    zsh)
      if have_cmd zsh; then
        zsh -c "source \"$file\"" >/dev/null 2>&1 || true
      fi
      ;;
    fish)
      if have_cmd fish; then
        fish -c "source \"$file\"" >/dev/null 2>&1 || true
      fi
      ;;
    csh)
      if have_cmd csh; then
        csh -c "source \"$file\"" >/dev/null 2>&1 || true
      fi
      ;;
    tcsh)
      if have_cmd tcsh; then
        tcsh -c "source \"$file\"" >/dev/null 2>&1 || true
      elif have_cmd csh; then
        csh -c "source \"$file\"" >/dev/null 2>&1 || true
      fi
      ;;
    *)
      . "$file" >/dev/null 2>&1 || true
      ;;
  esac
}

update_shell_config() {
  if [ -z "${HOME:-}" ]; then
    return 0
  fi

  install_dir_resolved="$(resolve_dir_path "$install_dir")"
  config_root="${XDG_CONFIG_HOME:-$HOME/.config}"
  managed_prefix="${CODEX_NPM_PREFIX:-$HOME/.local/share/codex-proxy/npm-global}"
  managed_prefix_resolved="$(resolve_dir_path "$managed_prefix")"
  managed_bin_dir="$managed_prefix_resolved/bin"
  posix_path_snippet="$config_root/codex-proxy/shell/path.sh"
  csh_path_snippet="$config_root/codex-proxy/shell/path.csh"
  fish_path_snippet="$config_root/fish/conf.d/codex-proxy-path.fish"
  posix_source_line="[ -f \"$posix_path_snippet\" ] && . \"$posix_path_snippet\""
  csh_source_line="source \"$csh_path_snippet\""
  legacy_path_line="export PATH=\"$install_dir:\$PATH\""
  legacy_path_line_resolved="export PATH=\"$install_dir_resolved:\$PATH\""
  legacy_fish_path_line="set -gx PATH \"$install_dir\" \$PATH"
  legacy_fish_path_line_resolved="set -gx PATH \"$install_dir_resolved\" \$PATH"
  alias_line="alias cxp='codex-proxy'"
  alias_file=""
  login_file=""
  interactive_file=""

  case "$shell_name" in
    bash)
      if [ -f "$HOME/.bash_profile" ]; then
        login_file="$HOME/.bash_profile"
      elif [ -f "$HOME/.bash_login" ]; then
        login_file="$HOME/.bash_login"
      elif [ -f "$HOME/.profile" ]; then
        login_file="$HOME/.profile"
      else
        login_file="$HOME/.profile"
      fi
      interactive_file="$HOME/.bashrc"
      write_posix_path_snippet "$posix_path_snippet" "$install_dir_resolved" "$managed_bin_dir"
      remove_line "$login_file" "$legacy_path_line"
      remove_line "$login_file" "$legacy_path_line_resolved"
      remove_line "$interactive_file" "$legacy_path_line"
      remove_line "$interactive_file" "$legacy_path_line_resolved"
      ensure_line "$login_file" "$posix_source_line"
      ensure_line "$interactive_file" "$posix_source_line"
      ensure_line "$login_file" "$alias_line"
      alias_file="$interactive_file"
      ;;
    zsh)
      login_file="$HOME/.zprofile"
      interactive_file="$HOME/.zshrc"
      write_posix_path_snippet "$posix_path_snippet" "$install_dir_resolved" "$managed_bin_dir"
      remove_line "$login_file" "$legacy_path_line"
      remove_line "$login_file" "$legacy_path_line_resolved"
      remove_line "$interactive_file" "$legacy_path_line"
      remove_line "$interactive_file" "$legacy_path_line_resolved"
      ensure_line "$login_file" "$posix_source_line"
      ensure_line "$interactive_file" "$posix_source_line"
      alias_file="$interactive_file"
      ;;
    fish)
      write_fish_path_snippet "$fish_path_snippet" "$install_dir_resolved" "$managed_bin_dir"
      alias_file="$HOME/.config/fish/config.fish"
      alias_line="alias cxp \"codex-proxy\""
      remove_line "$alias_file" "$legacy_fish_path_line"
      remove_line "$alias_file" "$legacy_fish_path_line_resolved"
      ;;
    csh|tcsh)
      cshrc_file="$HOME/.cshrc"
      tcshrc_file="$HOME/.tcshrc"
      write_csh_path_snippet "$csh_path_snippet" "$install_dir_resolved" "$managed_bin_dir"
      ensure_line "$cshrc_file" "$csh_source_line"
      if [ "$shell_name" = "tcsh" ] && [ -f "$tcshrc_file" ]; then
        ensure_line "$tcshrc_file" "$csh_source_line"
        ensure_line "$tcshrc_file" "alias cxp codex-proxy"
      else
        ensure_line "$cshrc_file" "alias cxp codex-proxy"
      fi
      alias_file=""
      alias_line=""
      ;;
    *)
      login_file="$HOME/.profile"
      write_posix_path_snippet "$posix_path_snippet" "$install_dir_resolved" "$managed_bin_dir"
      remove_line "$login_file" "$legacy_path_line"
      remove_line "$login_file" "$legacy_path_line_resolved"
      ensure_line "$login_file" "$posix_source_line"
      alias_file="$login_file"
      ;;
  esac

  ensure_line "$alias_file" "$alias_line"

  if [ "$CONFIG_UPDATED" -eq 1 ]; then
    old_ifs="$IFS"
    IFS='
'
    for file in $SOURCE_FILES; do
      [ -n "${file:-}" ] || continue
      source_config_file "$file"
    done
    IFS="$old_ifs"
  fi
}

get_latest_tag_from_redirect() {
  url="$release_base/$repo/releases/latest"
  tag=""

  if have_cmd curl; then
    if final="$(curl_run -o /dev/null -w '%{url_effective}' "$url")"; then
      tag="${final##*/}"
      if [ -n "${tag:-}" ] && [ "$tag" != "latest" ]; then
        printf "%s" "$tag"
        return 0
      fi
    fi
  fi

  if have_cmd wget; then
    headers="$(wget_run -qO /dev/null --max-redirect=0 --server-response "$url" 2>&1 || true)"
    if [ -n "${headers:-}" ]; then
      if have_cmd awk; then
        location="$(printf "%s" "$headers" | awk '/^  Location: /{print $2}' | head -n 1)"
      elif have_cmd sed; then
        location="$(printf "%s" "$headers" | sed -n 's/^  Location: //p' | head -n 1)"
      else
        location=""
      fi
      location="$(printf "%s" "$location" | tr -d '\r')"
      case "$location" in
        http*) final="$location" ;;
        /*) final="https://github.com$location" ;;
        *) final="" ;;
      esac
      tag="${final##*/}"
      if [ -n "${tag:-}" ] && [ "$tag" != "latest" ]; then
        printf "%s" "$tag"
        return 0
      fi
    fi
  fi

  return 1
}

get_latest_tag() {
  tmp="$1"
  tag=""
  if http_get "$api_base/repos/$repo/releases/latest" "$tmp"; then
    if have_cmd sed; then
      tag="$(sed -n 's/.*\"tag_name\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p' "$tmp" | head -n 1 || true)"
    fi
  fi
  if [ -n "${tag:-}" ]; then
    printf "%s" "$tag"
    return 0
  fi
  if tag="$(get_latest_tag_from_redirect)"; then
    if [ -n "${tag:-}" ]; then
      printf "%s" "$tag"
      return 0
    fi
  fi
  echo "Failed to determine latest version automatically; pass --version vX.Y.Z" >&2
  return 1
}

tmpdir="$(mktemp -d 2>/dev/null || mktemp -d -t codex-proxy)"
trap 'rm -rf "$tmpdir"' EXIT INT TERM

if [ "$version" = "latest" ] || [ -z "${version:-}" ]; then
  version="$(get_latest_tag "$tmpdir/latest.json")"
fi

ver_nov="${version#v}"
asset="codex-proxy_${ver_nov}_${os}_${arch}"
url="https://github.com/$repo/releases/download/$version/$asset"
url="$release_base/$repo/releases/download/$version/$asset"
checksums_url="$release_base/$repo/releases/download/$version/checksums.txt"

bin_tmp="$tmpdir/$asset"
http_get "$url" "$bin_tmp"

# Optional checksum verification.
if have_cmd sha256sum || have_cmd shasum; then
  http_get "$checksums_url" "$tmpdir/checksums.txt" || true
  if [ -s "$tmpdir/checksums.txt" ] && have_cmd awk; then
    expected="$(awk -v a="$asset" '$2==a {print $1}' "$tmpdir/checksums.txt" | head -n 1 || true)"
    if [ -n "${expected:-}" ]; then
      if have_cmd sha256sum; then
        actual="$(sha256sum "$bin_tmp" | awk '{print $1}')"
      else
        actual="$(shasum -a 256 "$bin_tmp" | awk '{print $1}')"
      fi
      if [ "$expected" != "$actual" ]; then
        echo "Checksum mismatch for $asset" >&2
        echo "Expected: $expected" >&2
        echo "Actual:   $actual" >&2
        exit 1
      fi
    fi
  fi
fi

mkdir -p "$install_dir"
chmod 0755 "$bin_tmp" 2>/dev/null || true

dst="$install_dir/codex-proxy"
mv -f "$bin_tmp" "$dst"

cxp_dst="$install_dir/cxp"
if have_cmd ln; then
  ln -sf "$dst" "$cxp_dst" 2>/dev/null || true
fi
if [ ! -f "$cxp_dst" ]; then
  cp -f "$dst" "$cxp_dst" 2>/dev/null || true
fi
chmod 0755 "$cxp_dst" 2>/dev/null || true

legacy_claude_proxy_owned=0
legacy_claude_proxy_path="$install_dir/claude-proxy"
if [ -f "$legacy_claude_proxy_path" ] && is_codex_owned_legacy_file "$legacy_claude_proxy_path"; then
  legacy_claude_proxy_owned=1
fi

# Clean up legacy command names when they can be positively identified as
# codex-proxy-owned leftovers from earlier installs.
for legacy_name in claude-proxy clp; do
  legacy_path="$install_dir/$legacy_name"
  if [ -f "$legacy_path" ] || [ -L "$legacy_path" ]; then
    # Only remove if it points to codex-proxy or is itself a codex-proxy build.
    should_remove=0
    if [ -L "$legacy_path" ]; then
      link_target="$(readlink "$legacy_path" 2>/dev/null || true)"
      resolved_target="$(resolve_legacy_target_path "$install_dir" "$link_target")"
      case "$legacy_name:$link_target" in
        claude-proxy:*codex-proxy*) should_remove=1 ;;
        clp:*codex-proxy*) should_remove=1 ;;
        claude-proxy:*claude-proxy*|clp:*claude-proxy*)
          if is_codex_owned_legacy_file "$resolved_target" || [ "$legacy_claude_proxy_owned" -eq 1 ]; then
            should_remove=1
          fi
          ;;
      esac
    elif [ -f "$legacy_path" ]; then
      legacy_version="$("$legacy_path" --version 2>/dev/null || true)"
      case "$legacy_name:$legacy_version" in
        claude-proxy:*codex-proxy*) should_remove=1 ;;
        clp:*codex-proxy*) should_remove=1 ;;
        claude-proxy:*claude-proxy*|clp:*claude-proxy*)
          if is_codex_owned_legacy_file "$legacy_path" || [ "$legacy_claude_proxy_owned" -eq 1 ]; then
            should_remove=1
          fi
          ;;
      esac
      if [ "$should_remove" -eq 0 ] && is_codex_owned_legacy_file "$legacy_path"; then
        should_remove=1
      fi
    fi
    if [ "$should_remove" -eq 1 ]; then
      rm -f "$legacy_path"
      echo "Removed legacy: $legacy_path"
    fi
  fi
done

echo "Installed: $dst"
update_shell_config
echo "Run: $dst proxy doctor"
echo "Shell config checked for install/managed CLI PATH and alias 'cxp' (reload attempted)"
