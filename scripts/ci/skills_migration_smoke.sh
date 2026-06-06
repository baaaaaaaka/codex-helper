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

export HOME="$home_dir"
export XDG_CONFIG_HOME="$home_dir/.config"
export XDG_CACHE_HOME="$home_dir/.cache"
if [[ -n "${default_gomodcache:-}" && -z "${GOMODCACHE:-}" ]]; then
  export GOMODCACHE="$default_gomodcache"
fi
if [[ -n "${default_gocache:-}" && -z "${GOCACHE:-}" ]]; then
  export GOCACHE="$default_gocache"
fi

git_repo() {
  (cd "$repo" && git "$@")
}

mkdir -p "$repo/skills/review" "$codex_dir" "$home_dir"
cat > "$repo/skills/review/SKILL.md" <<'SKILL'
---
name: review
description: Review code
---
legacy body
SKILL
git_repo init
git_repo config user.name "Skill Migration Smoke"
git_repo config user.email "skill-migration-smoke@example.invalid"
git_repo add -A
git_repo commit -m "initial legacy skill"

"${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" add "$repo" --name acme --ref HEAD --path skills/review --target codex-home --yes
legacy="$codex_dir/skills/acme__review"
agents="$home_dir/.agents/skills/acme__review"
test -f "$legacy/SKILL.md"
test ! -e "$agents"

"${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" migrate --dry-run | grep -q "Migration: dry_run"
test -f "$legacy/SKILL.md"
test ! -e "$agents"

"${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" list | grep -q "target agents"
test -f "$agents/SKILL.md"
test ! -e "$legacy"
find "$codex_dir/skills/.cxp-migrated-backups" -name SKILL.md -print -quit | grep -q .

cat > "$repo/skills/review/SKILL.md" <<'SKILL'
---
name: review
description: Review code
---
post-migration sync
SKILL
git_repo add -A
git_repo commit -m "post migration sync"

"${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" sync acme
grep -q "post-migration sync" "$agents/SKILL.md"
"${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" doctor acme | grep -q "$home_dir/.agents/skills"

cat > "$repo/skills/review/SKILL.md" <<'SKILL'
---
name: review
description: Review code
---
dirty legacy body
SKILL
git_repo add -A
git_repo commit -m "dirty legacy skill"

"${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" add "$repo" --name dirty --ref HEAD --path skills/review --target codex-home --yes
dirty_legacy="$codex_dir/skills/dirty__review"
dirty_agents="$home_dir/.agents/skills/dirty__review"
test -f "$dirty_legacy/SKILL.md"
cat > "$dirty_legacy/SKILL.md" <<'SKILL'
---
name: review
description: Local dirty edit
---
local dirty edit
SKILL

migrate_output="$("${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" migrate --yes)"
grep -q "local_modified" <<<"$migrate_output"
test -f "$dirty_legacy/SKILL.md"
test ! -e "$dirty_agents"
grep -q "local dirty edit" "$dirty_legacy/SKILL.md"

fixture_config_dir="$root/old-shape-config"
fixture_config="$fixture_config_dir/proxy.json"
fixture_skill_config="$fixture_config_dir/skill-subscriptions.json"
fixture_codex="$root/old-shape-codex"
fixture_home="$root/old-shape-home"
mkdir -p "$fixture_config_dir" "$fixture_codex" "$fixture_home"
HOME="$fixture_home" XDG_CONFIG_HOME="$fixture_home/.config" XDG_CACHE_HOME="$fixture_home/.cache" \
  "${helper_cmd[@]}" --config "$fixture_config" skills --codex-dir "$fixture_codex" add "$repo" --name oldshape --ref HEAD --path skills/review --target codex-home --yes
awk '!/"target_kind":/ && !/"target_root":/' "$fixture_skill_config" > "$fixture_skill_config.tmp"
mv "$fixture_skill_config.tmp" "$fixture_skill_config"
oldshape_legacy="$fixture_codex/skills/oldshape__review"
oldshape_agents="$fixture_home/.agents/skills/oldshape__review"
test -f "$oldshape_legacy/SKILL.md"
HOME="$fixture_home" XDG_CONFIG_HOME="$fixture_home/.config" XDG_CACHE_HOME="$fixture_home/.cache" \
  "${helper_cmd[@]}" --config "$fixture_config" skills --codex-dir "$fixture_codex" list | grep -q "target agents"
test -f "$oldshape_agents/SKILL.md"
test ! -e "$oldshape_legacy"
