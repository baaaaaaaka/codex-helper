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
- `cxp app [profile]`: install the Codex desktop app if needed, use or configure proxy mode, and launch the desktop app on macOS, Windows, or WSL. Linux outside WSL has no official Codex desktop app. If a proxy profile is literally named `app` or `auth`, use `cxp tui app`, `cxp app --profile auth`, or `cxp app auth --profile auth` to avoid command-name ambiguity.
- `cxp app auth [profile]`: complete ChatGPT auth for the Codex desktop app through a temporary Codex app-server using the same CODEX_HOME and proxy setup as `cxp app`.
- `cxp run [profile] -- <cmd args...>`: run a command through the selected proxy profile and helper runtime handling.
- `cxp init`: create or repair local proxy configuration interactively.

## Proxy Profiles

Proxy profiles are SSH/network profiles for reaching another machine. They are not beacon execution profiles.

- `cxp proxy`: manage local proxy profile configuration.
- `cxp proxy list`: list proxy profiles and running instances.
- `cxp proxy start <profile>`: start or reuse a proxy instance.
- `cxp proxy stop <instance>`: stop a proxy instance.
- `cxp proxy prune`: clean stale instances.
- `cxp proxy reset` or `cxp proxy clear`: clear saved proxy profiles, preference, and known instances, then attempt to stop known proxy daemons so the next launch asks whether to configure proxy mode again.
- `cxp proxy doctor`: diagnose local proxy dependencies and configuration.

Use `cxp proxy` only when the user is asking about SSH/network routing. If the user asks for beacon mode or beacon profiles, use `cxp beacon ...`.

## Beacon Execution Profiles

Beacon profiles describe where future Codex work executes. Create and confirm a profile before switching a conversation to it.

- `cxp beacon profile list`: list beacon profiles.
- `cxp beacon profile create <name> --provider slurm --partition <partition> --image <image> --nodes <n> --gpu <n> --duration <duration> --shared-path <path>`: create a Slurm draft profile. The shared path must be absolute and visible to the control machine and allocated workers.
- `cxp beacon profile create <name> ... --query-command <script> --submit-command <script> --cancel-command <script> --renew-command <script>`: store Slurm/LSF adapter commands on the profile so future Teams turns can use them without a helper reload.
- Managed Slurm/LSF adapters use `--adapter-shell user` by default, so scheduler setup from modules, `submit_job`, NSS, or `SUBMIT_ACCOUNT` is available unless the profile explicitly sets `--adapter-shell direct` because user-shell capture is incompatible or the adapter needs the clean helper service environment.
- `cxp beacon profile update <name> ...`: create a new profile revision without breaking Work chats already bound to the old revision.
- `cxp beacon profile history <name>`: list current and historical profile revisions.
- `cxp beacon profile rollback <name> <revision>`: publish a historical revision as a new latest revision.
- `cxp beacon profile gc <name>`: prune unreferenced historical revisions only.
- `cxp beacon profile create <name> --provider lsf --queue <queue> --shared-path <path>`: create an LSF draft profile.
- `cxp beacon profile create <name> --provider local`: create a local draft profile.
- `cxp beacon profile create <name> ... --proxy ssh_profile --proxy-profile <existing-proxy>`: attach an existing SSH proxy profile when the beacon job needs that network route.
- `cxp beacon profile create <name> ... --isolation shared|exclusive`: set the default lease sharing mode.
- `cxp beacon profile doctor <name>`: validate profile fields, shared-path access, and the query/submit/cancel/renew adapter commands visible to cxp without touching the scheduler.
- `cxp beacon profile doctor <name> --smoke`: submit, query, and cancel one real scheduler allocation to verify adapter output and cleanup.
- `cxp beacon profile confirm <name>`: confirm a reviewed profile so it can be used.
- `cxp beacon profile status <name>`: inspect one profile.
- `cxp beacon status --session <session-id>`: inspect a conversation's current, pending, and queued target state.
- `cxp beacon switch-profile <name> --session <session-id>`: switch immediately when no Codex work is queued or running.
- `cxp beacon switch-profile <name> --session <session-id> --after-current-turn`: defer the switch so the current Codex turn stays on its existing target and future turns use the new profile.
- `cxp beacon switch-profile <name> --session <session-id> --fork`: fork when the target execution signature is incompatible and the user accepts the fork.
- `cxp beacon release <profile|allocation|provider-job|machine> [--force] [--confirm <token>]`: preview and release a beacon resource. In Teams Work chat, `beacon release` releases the current chat's resource when safe; if a worker is shared, it detaches only the current chat and leaves other chats running unless the user confirms a control-chat shared/forced release.
- `cxp beacon allocation list`: list managed allocation requests.
- `cxp beacon allocation status <allocation-or-provider-job>`: inspect one allocation request.
- `cxp beacon allocation cancel <allocation-or-provider-job>`: cancel one managed allocation through the configured provider adapter.
- `cxp beacon allocation reconcile <allocation>`: query/adopt/submit through the configured provider adapter.
- `cxp beacon allocation reconcile-all`: reconcile all allocations, drain stale workers, and recover stale claims.
- `cxp beacon provider template slurm|lsf`: print a starter scheduler adapter script that can be edited for the site.
- `cxp beacon worker run-once --machine <machine-id>`: run inside an allocated worker, claim one queued job, execute Codex, and publish the fenced terminal result.
- `cxp beacon worker run-once --allocation <request-id> --wait 30m`: register the current Slurm/LSF worker for a managed allocation, wait for the Teams job, execute Codex, and publish the terminal result.
- `cxp beacon worker serve --allocation <request-id>`: register a long-lived worker, store bootstrap diagnostics, send heartbeats, serve jobs until idle or stopped, then drain on exit.
- `cxp beacon machine list`: list beacon machines.
- `cxp beacon machine status <machine-or-lease>`: inspect a machine/lease and get confirmation tokens.
- `cxp beacon machine release <machine-or-lease>`: drain or release a machine.
- `cxp beacon machine kill <machine-or-lease-or-job> --confirm <token>`: hard kill only after using the exact token from status.

From an active Codex turn, prefer `--after-current-turn` for profile switches. This writes a pending target and avoids interrupting the running answer.

Teams Work chat turns targeting beacon snapshot their target and record a managed allocation before Codex can start. Explicit beacon requests must not be answered by running local Codex. If no accepting worker or lease is available, inspect `cxp beacon status --session <session-id>` for `allocation`, `allocation_state`, `provider_job`, `provider_state`, and `provider_reason`.

Managed provider submission uses explicit site adapters instead of guessing scheduler commands. Prefer storing executable adapter paths on each profile with `--query-command`, `--submit-command`, `--cancel-command`, and `--renew-command`; this is visible in `beacon profile status` and applies to future Teams turns without a helper reload. Managed Slurm/LSF adapters default to `--adapter-shell user`: cxp resolves the environment through `$SHELL -lic`, then direct execs the adapter with that environment instead of asking you to copy shell-initialized scheduler variables into the helper service. Set `--adapter-shell direct` when user-shell capture is incompatible (for example tcsh/csh) or an adapter needs the clean helper service environment. As a global fallback, configure executable paths with `CODEX_HELPER_BEACON_SLURM_QUERY`, `CODEX_HELPER_BEACON_SLURM_SUBMIT`, `CODEX_HELPER_BEACON_SLURM_CANCEL`, `CODEX_HELPER_BEACON_SLURM_RENEW`, `CODEX_HELPER_BEACON_LSF_QUERY`, `CODEX_HELPER_BEACON_LSF_SUBMIT`, `CODEX_HELPER_BEACON_LSF_CANCEL`, `CODEX_HELPER_BEACON_LSF_RENEW`, and optional shell mode `CODEX_HELPER_BEACON_PROVIDER_SHELL_MODE`. Environment fallback changes require the Teams helper service to reload.

For managed Slurm/LSF adapters, the default `user` shell mode invokes `$SHELL -lic` only to capture a framed environment snapshot, then executes the adapter directly so shell startup output cannot pollute the adapter protocol. Use `--adapter-shell shell-command` only for rare sites where scheduler submission is a shell function or alias that cannot be captured as environment. The adapter should print JSON fields such as `provider_job_id`, `raw_state`, `reason`, and optional `provider_deadline`, or equivalent `key=value` pairs. The generated Slurm/LSF templates include `query`, `submit`, `cancel`, and a site-policy `renew` stub that exits non-zero until edited. Query and renew should return `provider_deadline` when the scheduler exposes a walltime/deadline so Teams can renew before expiry.

Beacon adapter troubleshooting:

- `exit 127` from the scheduler job often means the submitted command was malformed or PATH is different inside the allocation. Keep exactly one `exec` in the final worker command; if a site wrapper already prepends `exec`, remove the extra one from the adapter.
- `allocation request ... not found` inside the worker usually means the worker is using the wrong state file. Custom Slurm/LSF submit adapters must accept `--shared-store` and pass it to the worker as `cxp beacon --store <shared-store> worker ...`.
- If `$SHELL -lic` fails under tcsh/csh, update the profile with `--adapter-shell direct`; profile revisions apply to future turns.
- Beacon workers launch Codex in yolo mode by default so scheduler/container devices and mounts stay visible; pass worker `--no-yolo` only when the worker must keep Codex sandboxing.
- If worker doctor reports `missing codex`, set scheduler PATH or pass worker `--codex-path <codex-or-wrapper>` from the adapter. A wrapper is still useful for path resolution or extra Codex exec flags such as `--skip-git-repo-check`; Teams service `--codex-arg` settings do not automatically reach remote beacon workers. The generated templates honor `CXP_BEACON_CODEX_BIN` by passing it as worker `--codex-path`.

The active Teams helper owner periodically queries existing provider jobs and renews due allocations through the configured `*_RENEW` adapter. Renewal is epoch-fenced and never updates a newer cancel, replacement, or provider job. During helper drain, renewal only protects allocations whose job may already have started; pre-start replacement is conservative and only resets a lost allocation when all jobs are still queued.

For real remote execution, start from `cxp beacon provider template slurm` or `cxp beacon provider template lsf`, edit it for the site, and set the matching `CODEX_HELPER_BEACON_*_QUERY` / `*_SUBMIT` / `*_CANCEL` / `*_RENEW` environment variables. The scheduler job should run `cxp beacon worker serve --allocation <request-id>` for reusable workers, or `cxp beacon worker run-once --allocation <request-id> --wait 30m` for one-shot jobs. The provider template passes the explicit beacon store to the worker when configured, otherwise it passes the profile-derived shared store under `--shared-path`. The worker derives `SLURM_JOB_ID` or `LSB_JOBID` when `--provider-job` is omitted, runs a doctor check, sends heartbeats, waits for the Teams turn to enqueue work, and publishes the terminal result. Teams waits through this path instead of falling back to local Codex.

Scheduler-capable CI can opt in to the real adapter test with `CODEX_HELPER_BEACON_LIVE=1`, `CODEX_HELPER_BEACON_LIVE_PROVIDER=slurm|lsf`, and the matching query/submit/cancel commands. The live test submits through a profile-stored adapter snapshot and cancels the provider job during cleanup.

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
- `cxp teams service restart --force`: recover active local Teams state, mark ambiguous turns interrupted, then restart or activate the pending helper. Use only from a local terminal when you accept interrupting active work.
- `cxp teams service doctor`: diagnose service backend readiness.
- On Linux, service auto mode prefers `systemd --user`; if no user manager is usable, it falls back to `local-supervisor`, which survives terminal close and helper crashes but not machine/container reboot. Enabled or active local-supervisor installs stay sticky to avoid backend flapping; in WSL, the Windows Task backend remains preferred unless local-supervisor is sticky or explicitly selected.
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
- `helper cancel last`, `helper cancel queued`, `helper cancel running`, or `helper cancel all`: cancel or drop queued/running control-chat Codex question(s).
- `helper update now`: update to the latest stable helper release.
- `helper update prerelease`: update to the newest eligible release or pre-release.
- `helper reload now`: load the latest helper code after update.
- `helper restart now`: restart the Teams helper.

Work chat commands:

- Regular text: send a task to Codex.
- `helper status`: check current request state.
- `helper retry last`: retry the last failed or interrupted request.
- `helper cancel last`, `helper cancel queued`, `helper cancel running`, or `helper cancel all`: cancel or drop queued/running work.
- `helper file <relative-path>`: upload a generated file from the Teams outbound folder.
- `helper close`: close the Work chat binding.

## Skills

- `cxp skills install-builtin`: install or repair bundled skills such as this `cxp` skill in `$HOME/.agents/skills`.
- `cxp skills list`: list git-backed skill subscriptions.
- `cxp skills add <github/gitlab/git-url>`: install skills from a git source and keep them updated in the user agents skills directory.
- `cxp skills migrate`: migrate managed skills from the legacy Codex skills directory to `$HOME/.agents/skills`.
- `cxp skills sync [name]`: sync one source, or all sources when no name is given.
- `cxp skills doctor [name]`: inspect local skill subscription state.
- `cxp skills push [name]`: review and push local skill edits with per-change confirmation.
- `cxp skills remove <name>`: remove a git-backed subscription and its managed installed skills.

Built-in skills are installed into `$HOME/.agents/skills`, but they are not git subscriptions and do not appear as remote sources in `cxp skills list`.

## History

- `cxp history`: inspect local Codex history.
- `cxp history tui`: open the history browser directly.

## Upgrades

- `cxp upgrade`: upgrade codex-helper from GitHub releases.
- `cxp upgrade --version <tag>`: install a specific release.
- `cxp upgrade --include-prerelease`: allow latest resolution to include prereleases.

From a Teams child turn, do not self-manage the running helper process. Use control chat update/reload/restart commands as directed by the helper safety rules.
