---
name: cxp
description: "Operate installed CXP/codex-proxy/codex-helper: SSH/proxy profiles, approvals, Teams, delegation, upgrades, operational Codex history, and CXP-managed skills. Exclude conceptual history, skill authoring, source changes, and unrelated scheduler/GPU work."
---

# cxp

Use `cxp` (or `codex-proxy`); in Teams turns prefer `$CODEX_HELPER_CLI_PATH` because service PATH may differ.

Trigger for installed CXP operations, even if they do not name CXP:
standard approvals, Teams helper/control/work chats, cross-machine delegation, SSH/proxy profiles,
upgrades, operational Codex history through the CXP CLI/store, or
CXP-managed skill operations (install-builtin, list, add, sync,
doctor, push, or remove). Do not trigger for conceptual history, generic skill
authoring, source changes, or unrelated GPU/scheduler work.

Read live help: `cxp --help`, `cxp <command> --help`, and `cxp <command> <subcommand> --help`. For the command map and workflows, load `references/commands.md`.

For natural-language cross-machine requests, load `references/delegation.md`. Use `cxp delegate resolve/start/status/wait/cancel --json`; do not scan raw Teams registry messages.

## Disruptive Actions

Some operations replace the helper process. Do not run these inline from an active Codex turn.

For Teams helper lifecycle work, do not restart, update, kill, replace, or background the helper from a child Codex turn. Tell the user to send `helper restart now` after upgrades or `helper update now` / `helper update prerelease` for release updates.

For auth prompts, destructive confirmations, and skill pushes, direct the user to run the local `cxp ...` command in their terminal unless the helper explicitly provides a safe Teams command.
