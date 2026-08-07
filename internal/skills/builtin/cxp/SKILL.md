---
name: cxp
description: "Operate installed CXP/codex-proxy/codex-helper: model and SSH/proxy profiles, Responses adapter, approvals, Teams, beacon targets, delegation, upgrades, operational Codex history, and CXP-managed skills. Exclude conceptual history, skill authoring, source changes, and unrelated scheduler/GPU work."
---

# cxp

Use `cxp` (or `codex-proxy`); in Teams turns prefer `$CODEX_HELPER_CLI_PATH` because service PATH may differ.

Trigger for installed CXP operations, even if they do not name CXP:
model/profile routing, Responses adapter, standard approvals, beacon
targets, Teams helper/control/work chats, cross-machine delegation, SSH/proxy
profiles, upgrades, operational Codex history through the CXP CLI/store, or
CXP-managed skill operations (install-builtin, list, add, sync,
doctor, push, or remove). Do not trigger for conceptual history, generic skill
authoring, source changes, or unrelated GPU/scheduler work.

Read live help: `cxp --help`, `cxp <command> --help`, and `cxp <command> <subcommand> --help`. For the command map and workflows, load `references/commands.md`.

For natural-language cross-machine requests, load `references/delegation.md`. Use `cxp delegate resolve/start/status/wait/cancel --json`; do not scan raw Teams registry messages.

For third-party model JSON subscriptions, load `references/commands.md`: Git, file, and
schema-v2 sources keep candidates hidden until `bind` stores a key in the local secret
store and a bounded probe succeeds. Keep credentials/private data/runtime evidence out
of JSON; reject unknown fields, unsafe paths, dangling or ambiguous refs, and unsupported routes.

## Disruptive Actions

Some operations replace the helper process or move future Codex work. Do not run these inline from an active Codex turn.

For beacon switches from inside a Codex turn, use the deferred switch form so the current turn can finish cleanly:

```bash
cxp beacon switch-profile <profile> --session <session-id> --after-current-turn
```

If the current session id is unknown, inspect `cxp beacon status --session <id>` or ask for the session/work chat. If the command reports an incompatible execution signature, ask whether to fork before using `--fork`.

For Teams helper lifecycle work, do not restart, reload, update, kill, replace, or background the helper from a child Codex turn. For normal installed helpers, tell the user to send `helper restart now` after upgrades or `helper update now` / `helper update prerelease` for release updates. Tell the user to send `helper reload now` only for source-checkout development reloads.

For auth prompts, destructive confirmations, and skill pushes, direct the user to run the local `cxp ...` command in their terminal unless the helper explicitly provides a safe Teams command.
