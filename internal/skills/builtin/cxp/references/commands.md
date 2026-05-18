# cxp Command Reference

`cxp` is the short command installed next to `codex-proxy`. Both invoke the same binary. Prefer `cxp` in user-facing examples, but use `codex-proxy` or `$CODEX_HELPER_CLI_PATH` when `cxp` is not visible in the current process PATH.

## General Checks

- `cxp --version`: print the installed helper version.
- `cxp --help`: show top-level commands.
- `cxp <command> --help`: verify current flags before changing configuration.
- `type cxp`, `command -v cxp`, and `echo $PATH`: diagnose shell alias/PATH differences.

## Codex Launching

- `cxp`: open the local Codex history TUI.
- `cxp tui`: open the local Codex history TUI explicitly.
- `cxp run [profile] -- <cmd args...>`: run a command through the selected proxy profile and helper runtime handling.
- `cxp init`: create or repair local proxy configuration interactively.

## Proxy Profiles

Proxy profiles are SSH/network profiles for reaching another machine. They are not beacon execution profiles.

- `cxp proxy`: manage local proxy profile configuration.
- `cxp proxy list`: list proxy profiles and running instances.
- `cxp proxy start <profile>`: start or reuse a proxy instance.
- `cxp proxy stop <instance>`: stop a proxy instance.
- `cxp proxy prune`: clean stale instances.
- `cxp proxy doctor`: diagnose local proxy dependencies and configuration.

Use `cxp proxy` only when the user is asking about SSH/network routing. If the user asks for beacon mode or beacon profiles, use `cxp beacon ...`.

## Beacon Execution Profiles

Beacon profiles describe where future Codex work executes. Create and confirm a profile before switching a conversation to it.

- `cxp beacon profile list`: list beacon profiles.
- `cxp beacon profile create <name> --provider slurm --partition <partition> --image <image> --nodes <n> --gpu <n> --duration <duration>`: create a Slurm draft profile.
- `cxp beacon profile create <name> --provider lsf --queue <queue>`: create an LSF draft profile.
- `cxp beacon profile create <name> --provider local`: create a local draft profile.
- `cxp beacon profile create <name> ... --proxy ssh_profile --proxy-profile <existing-proxy>`: attach an existing SSH proxy profile when the beacon job needs that network route.
- `cxp beacon profile create <name> ... --isolation shared|exclusive`: set the default lease sharing mode.
- `cxp beacon profile doctor <name>`: mark the profile's local doctor check successful.
- `cxp beacon profile confirm <name>`: confirm a reviewed profile so it can be used.
- `cxp beacon profile status <name>`: inspect one profile.
- `cxp beacon status --session <session-id>`: inspect a conversation's current, pending, and queued target state.
- `cxp beacon switch-profile <name> --session <session-id>`: switch immediately when no Codex work is queued or running.
- `cxp beacon switch-profile <name> --session <session-id> --after-current-turn`: defer the switch so the current Codex turn stays on its existing target and future turns use the new profile.
- `cxp beacon switch-profile <name> --session <session-id> --fork`: fork when the target execution signature is incompatible and the user accepts the fork.
- `cxp beacon machine list`: list beacon machines.
- `cxp beacon machine status <machine-or-lease>`: inspect a machine/lease and get confirmation tokens.
- `cxp beacon machine release <machine-or-lease>`: drain or release a machine.
- `cxp beacon machine kill <machine-or-lease-or-job> --confirm <token>`: hard kill only after using the exact token from status.

From an active Codex turn, prefer `--after-current-turn` for profile switches. This writes a pending target and avoids interrupting the running answer.

## Teams Helper

- `cxp teams setup`: guided setup checklist.
- `cxp teams run`: run the Teams listener in the foreground.
- `cxp teams run --once`: poll once for diagnosis.
- `cxp teams status`: show registry, owner, listener, poll, and session status.
- `cxp teams doctor`: local diagnostics.
- `cxp teams doctor --live`: opt in to live Microsoft Graph checks.
- `cxp teams control --print`: print the bound Teams control chat.
- `cxp teams service bootstrap`: install or repair the background service/watchdog.
- `cxp teams service status`: inspect OS service/task state.
- `cxp teams service doctor`: diagnose service backend readiness.
- `cxp teams auth full`: refresh full Teams auth locally.
- `cxp teams auth full-status`: inspect auth cache expiry without printing tokens.

From a Teams-launched Codex child turn, do not restart or reload the running helper directly. Tell the user to send `helper reload now` or `helper restart now` in the control chat.

## Teams Chat Commands

Control chat commands:

- `projects` or `p`: list workspaces.
- `project <number>` or `p <number>`: open a workspace.
- `sessions`, `s`, or `history`: list local Codex sessions.
- `new <directory>` or `n <directory>`: create a Work chat for a directory.
- `new` or `n`: create a Work chat for the currently selected workspace.
- `continue <number-or-session-id>` or `c <...>`: create or open a Work chat for an existing session.
- `open <number>`: show an existing linked Work chat.
- `status` or `st`: list active Work chats.
- `helper update now`: update to the latest stable helper release.
- `helper update prerelease`: update to the newest eligible release or pre-release.
- `helper reload now`: load the latest helper code after update.
- `helper restart now`: restart the Teams helper.

Work chat commands:

- Regular text: send a task to Codex.
- `helper status`: check current request state.
- `helper retry last`: retry the last failed or interrupted request.
- `helper cancel last` or `helper cancel all`: cancel or drop queued/running work.
- `helper file <relative-path>`: upload a generated file from the Teams outbound folder.
- `helper close`: close the Work chat binding.

## Skills

- `cxp skills install-builtin`: install or repair bundled skills such as this `cxp` skill.
- `cxp skills list`: list git-backed skill subscriptions.
- `cxp skills add <github/gitlab/git-url>`: install skills from a git source and keep them updated.
- `cxp skills sync [name]`: sync one source, or all sources when no name is given.
- `cxp skills doctor [name]`: inspect local skill subscription state.
- `cxp skills push [name]`: review and push local skill edits with per-change confirmation.
- `cxp skills remove <name>`: remove a git-backed subscription and its managed installed skills.

Built-in skills are installed into the Codex skills directory, but they are not git subscriptions and do not appear as remote sources in `cxp skills list`.

## History

- `cxp history`: inspect local Codex history.
- `cxp history tui`: open the history browser directly.

## Upgrades

- `cxp upgrade`: upgrade codex-helper from GitHub releases.
- `cxp upgrade --version <tag>`: install a specific release.
- `cxp upgrade --include-prerelease`: allow latest resolution to include prereleases.

From a Teams child turn, do not self-manage the running helper process. Use control chat update/reload/restart commands as directed by the helper safety rules.
