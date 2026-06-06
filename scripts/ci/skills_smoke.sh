#!/usr/bin/env bash
set -euo pipefail

root="$(mktemp -d)"
trap 'chmod -R u+w "$root" 2>/dev/null || true; rm -rf "$root"' EXIT

repo="$root/repo"
config="$root/config.json"
codex_dir="$root/codex"
home_dir="$root/home"
go_bin="${GO:-go}"
if [[ -n "${CODEX_HELPER_BIN:-}" ]]; then
  helper_cmd=("$CODEX_HELPER_BIN")
else
  default_gomodcache="$("$go_bin" env GOMODCACHE 2>/dev/null || true)"
  default_gocache="$("$go_bin" env GOCACHE 2>/dev/null || true)"
  helper_cmd=("$go_bin" run ./cmd/codex-proxy)
fi

git_repo() {
  (cd "$repo" && git "$@")
}

export HOME="$home_dir"
export XDG_CONFIG_HOME="$home_dir/.config"
export XDG_CACHE_HOME="$home_dir/.cache"
if [[ -n "${default_gomodcache:-}" && -z "${GOMODCACHE:-}" ]]; then
  export GOMODCACHE="$default_gomodcache"
fi
if [[ -n "${default_gocache:-}" && -z "${GOCACHE:-}" ]]; then
  export GOCACHE="$default_gocache"
fi

mkdir -p "$repo/skills/review/scripts" "$codex_dir" "$home_dir"
agents_dir="$home_dir/.agents/skills"

"${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" install-builtin --yes
test -f "$agents_dir/cxp/SKILL.md"
test -f "$agents_dir/cxp/references/commands.md"
grep -q -- "--after-current-turn" "$agents_dir/cxp/references/commands.md"
list_output="$("${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" list)"
grep -q "No skill subscriptions." <<<"$list_output"

git_repo init
git_repo config user.name "Skill Smoke"
git_repo config user.email "skill-smoke@example.invalid"
cat > "$repo/skills/review/SKILL.md" <<'SKILL'
---
name: review
description: Review code
---
initial body
SKILL
cat > "$repo/skills/review/scripts/check.sh" <<'SCRIPT'
#!/bin/sh
echo ok
SCRIPT
chmod +x "$repo/skills/review/scripts/check.sh"
git_repo add -A
git_repo commit -m "initial skill"

"${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" add "$repo" --name acme --ref HEAD --path skills/review --yes
installed="$agents_dir/acme__review"
test -f "$installed/SKILL.md"
test -f "$installed/scripts/check.sh"
test -x "$installed/scripts/check.sh"

cat > "$repo/skills/review/SKILL.md" <<'SKILL'
---
name: review
description: Review code
---
remote update
SKILL
rm "$repo/skills/review/scripts/check.sh"
mkdir -p "$repo/skills/review/agents"
cat > "$repo/skills/review/agents/openai.yaml" <<'AGENT'
version: 1
AGENT
git_repo add -A
git_repo commit -m "remote update"

"${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" sync
grep -q "remote update" "$installed/SKILL.md"
test -f "$installed/agents/openai.yaml"
test ! -e "$installed/scripts/check.sh"

cat > "$installed/SKILL.md" <<'SKILL'
---
name: review
description: Local smoke edit
---
local smoke edit
SKILL
cat > "$repo/skills/review/SKILL.md" <<'SKILL'
---
name: review
description: Remote smoke edit
---
remote smoke edit
SKILL
git_repo add -A
git_repo commit -m "remote edit while local is dirty"

set +e
sync_output="$("${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" sync 2>&1)"
sync_status=$?
set -e
if [[ "$sync_status" == "0" ]]; then
  echo "skills sync unexpectedly succeeded with local edits" >&2
  exit 1
fi
grep -q "local modifications" <<<"$sync_output"
grep -q "local smoke edit" "$installed/SKILL.md"

printf 'y\ny\ny\n' | "${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" push
branch="$(git_repo for-each-ref --format='%(refname:short)' refs/heads/skill)"
if [[ -z "$branch" ]]; then
  echo "skills push did not create a review branch" >&2
  exit 1
fi
pushed_skill="$(git_repo show "$branch:skills/review/SKILL.md")"
grep -q "local smoke edit" <<<"$pushed_skill"
