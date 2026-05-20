---
name: cxp
description: "Use when a user asks about cxp, codex-proxy, or codex-helper operations: proxy/SSH profiles, Teams helper/control/work chats, Codex history or skills, upgrades, beacon/execution target/profile switching, Slurm/LSF/GPU/local execution, and safe handoffs for operations that can interrupt Codex."
---

# cxp

Use the installed `cxp` command, falling back to `codex-proxy` if `cxp` is not on PATH. In Teams-launched Codex turns, prefer `$CODEX_HELPER_CLI_PATH` when it is set because the helper service may not inherit the user's interactive shell startup files.

Trigger on related requests even when the user does not say `cxp`: beacon mode, execution target/profile switching, GPU/Slurm/LSF/local execution, Teams helper/control/work chats, proxy/SSH profiles, history, skills, and upgrades.

Read live help before changing behavior: `cxp --help`, `cxp <command> --help`, and `cxp <command> <subcommand> --help`. For the command map and workflows, load `references/commands.md`.

## Disruptive Actions

Some operations can replace the helper process or move future Codex work to another target. Do not run these inline from an active Codex turn unless the user explicitly asked for immediate execution and the surrounding environment allows it.

For beacon switches from inside a Codex turn, use the deferred switch form so the current turn can finish cleanly:

```bash
cxp beacon switch-profile <profile> --session <session-id> --after-current-turn
```

If the current session id is unknown, inspect `cxp beacon status --session <id>` when the id is available, or ask the user for the session/work chat to switch. If the command reports an incompatible execution signature, ask whether to fork before using `--fork`.

For Teams helper reloads or restarts, finish the answer and tell the user to send `helper reload now` or `helper restart now` in the Teams control chat. Do not restart the helper from a child Codex turn.

For auth prompts, destructive confirmations, and skill pushes, direct the user to run the local `cxp ...` command in their terminal unless the helper explicitly provides a safe Teams command.
