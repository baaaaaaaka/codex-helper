#!/usr/bin/env bash
set -euo pipefail

root="$(mktemp -d)"
trap 'rm -rf "$root"' EXIT

repo="$root/repo"
config="$root/config.json"
codex_dir="$root/codex"
go_bin="${GO:-go}"
if [[ -n "${CODEX_HELPER_BIN:-}" ]]; then
  helper_cmd=("$CODEX_HELPER_BIN")
else
  helper_cmd=("$go_bin" run ./cmd/codex-proxy)
fi

git_repo() {
  (cd "$repo" && git "$@")
}

mkdir -p "$repo/skills/review/scripts" "$codex_dir"

"${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" install-builtin --yes
test -f "$codex_dir/skills/cxp/SKILL.md"
test -f "$codex_dir/skills/cxp/references/commands.md"
grep -q -- "--after-current-turn" "$codex_dir/skills/cxp/references/commands.md"
"${helper_cmd[@]}" --config "$config" skills --codex-dir "$codex_dir" list | grep -q "No skill subscriptions."

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
installed="$codex_dir/skills/acme__review"
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
git_repo show "$branch:skills/review/SKILL.md" | grep -q "local smoke edit"
