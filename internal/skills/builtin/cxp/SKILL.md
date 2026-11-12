---
name: cxp
description: "Operate installed CXP/codex-proxy/codex-helper: SSH/proxy profiles, approvals, Teams, delegation, upgrades, operational Codex history, and CXP-managed skills. Exclude conceptual history, skill authoring, source changes, and unrelated scheduler/GPU work."
---

# cxp

Use `cxp` (or `codex-proxy`). In Teams turns use the supplied installation entry:
POSIX shells: `"$CODEX_HELPER_CLI_PATH" teams read --help`;
PowerShell: `& $env:CODEX_HELPER_CLI_PATH teams read --help`.
Quote the path because it may contain spaces. A managed helper supplies its
own stable entry; each invocation selects that installation's current confirmed
runtime, which can be newer than the running helper. Service PATH may select
another installation. If the supplied entry is missing or cannot run, report the
path and error; do not guess a version-directory binary or another PATH entry.

Trigger for installed CXP operations, even if they do not name CXP:
standard approvals, Teams helper/control/work chats, cross-machine delegation, SSH/proxy profiles,
upgrades, operational Codex history through the CXP CLI/store, or
CXP-managed skill operations (install-builtin, list, add, sync,
doctor, push, or remove). Do not trigger for conceptual history, generic skill
authoring, source changes, or unrelated GPU/scheduler work.

For a Teams conversation/message link, reading discussion context, finding a
message, or downloading a Teams attachment, load `references/teams-read.md`.
Use `cxp teams read`, `search`, and `fetch` with the current read identity.
Choose history/replies explicitly for the user's objective; these commands do
not require a running helper and do not send messages or change session bindings.

Read live help: `cxp --help`, `cxp <command> --help`, and `cxp <command> <subcommand> --help`. For the command map and workflows, load `references/commands.md`.

For natural-language cross-machine requests, load `references/delegation.md`. Use `cxp delegate resolve/start/status/wait/cancel --json`; do not scan raw Teams registry messages.

## Disruptive Actions

Some operations replace the helper process. Do not run these inline from an active Codex turn.

For Teams helper lifecycle work, do not restart, update, kill, replace, or background the helper from a child Codex turn. Tell the user to send `helper restart now` after upgrades or `helper update now` / `helper update prerelease` for release updates.

For auth prompts, destructive confirmations, and skill pushes, direct the user to run the local `cxp ...` command in their terminal unless the helper explicitly provides a safe Teams command.
