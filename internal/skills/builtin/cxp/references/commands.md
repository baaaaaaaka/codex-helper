# cxp Command Reference

`cxp` is the short command installed next to `codex-proxy`. Both invoke the same binary. Prefer `cxp` in user-facing examples, but use `codex-proxy` or `$CODEX_HELPER_CLI_PATH` when `cxp` is not visible in the current process PATH.

## General Checks

- `cxp --version`: print the installed helper version.
- `cxp --help`: show top-level commands.
- `cxp <command> --help`: verify current flags before changing configuration.
- `type cxp`, `command -v cxp`, and `echo $PATH`: diagnose shell alias/PATH differences.

## Codex Launching

- `cxp`: open the local Codex history TUI.
- `cxp tui`: open the local Codex history TUI explicitly. Press `Ctrl+A` to toggle Agent Auto Approve (AAA) and `Ctrl+K` to open the skills menu. AAA defaults off.
- `cxp app [profile]`: install the Codex desktop app if needed, use or configure proxy mode, and launch the desktop app on macOS, Windows, or WSL. Linux outside WSL has no official Codex desktop app. If a proxy profile is literally named `app` or `auth`, use `cxp tui app`, `cxp app --profile auth`, or `cxp app auth --profile auth` to avoid command-name ambiguity.
- `cxp app auth [profile]`: complete ChatGPT auth for the Codex desktop app through a temporary Codex app-server using the same CODEX_HOME and proxy setup as `cxp app`.
- `cxp --upgrade-codex-app [profile]`: on native Windows or WSL, download and verify the official signed x64 ChatGPT MSIX, publish it beside the current CXP-managed runtime, and select it for future `cxp app` launches without modifying the Microsoft Store/AppX installation.
- `cxp run [profile] -- <cmd args...>`: run a command through the selected proxy profile and helper runtime handling.
- `cxp run -- codex`: launch the original Codex TUI through CXP's standard approval broker with manual approvals.
- `cxp run --aaa -- codex`: enable Agent Auto Approve for this run without changing the saved TUI preference.
- `cxp run --aaa -- codex exec <prompt>`: enable Agent Auto Approve for a non-interactive Codex turn; without AAA, approval-required operations fail closed.
- `cxp init`: create or repair local proxy configuration interactively; when `~/.ssh/config` contains concrete `Host` entries, it can reuse and validate one before falling back to manual SSH host details.

## Models

CXP launches the official Codex CLI and uses the models Codex offers for the signed-in account. Choose a model the way Codex does (`/model` in the TUI, `-m <model>` for `codex exec`). In Teams, send `model` in the control chat to list the models and `helper model <model> [effort]` in a work chat to switch that session. Third-party model profiles, catalogs and the local Responses adapter were removed; configs that still contain them load normally and keep the settings untouched.

## Proxy Profiles

Proxy profiles are SSH/network profiles for reaching another machine.

- `cxp proxy`: manage local proxy profile configuration.
- `cxp proxy list`: read-only listing of saved instances and app gateways for profiles in the selected config; checks local health and shows failure details. Private session stacks are not registered. `LAST_SEEN` is the saved record timestamp.
- `cxp proxy start <profile>`: start or reuse a proxy instance.
- `cxp proxy stop <instance>`: stop a saved proxy instance; app-gateway rows in `proxy list` are informational and are not managed by this command.
- `cxp proxy prune`: remove dead records, preserving live processes (including recovering backends), startup leases and durable recovery blocks.
- `cxp proxy reset` or `cxp proxy clear`: clear saved proxy profiles, preference, and known instances, then attempt to stop known proxy daemons so the next launch asks whether to configure proxy mode again.
- `cxp proxy doctor`: diagnose local proxy dependencies and configuration.

Use `cxp proxy` only when the user is asking about SSH/network routing.

## Teams Helper

- `cxp teams setup`: local setup checklist (no network).
- `cxp teams auth config --tenant-id <tenant-id> --client-id <client-id>`: save the Teams Graph tenant and public client ID.
- `cxp teams auth` (or `cxp teams auth login`): sign in with a Microsoft device code in a foreground terminal; one delegated token covers reading and sending chat messages, meeting chats and file uploads. Add `--force` for a fresh login.
- `cxp teams auth status`: show the local sign-in state and token expiry without printing tokens.
- `cxp teams auth logout`: remove the local Teams token cache.
- `cxp teams run`: start the helper. Where supported it installs and starts the background service, which keeps the helper running after terminal close; the helper creates this machine's control chat when it first starts. `--once` polls once in the foreground, finishes the work that brings in (and any kept command-line request) and exits; `--interval` sets the polling interval.
- `cxp teams status`: show the helper store, owner, control chat, sessions and queues. When the helper is stopped on all machines, the first line says by whom and how to start it again.
- `cxp teams stop`: stop the helper on all machines that share this state directory until `cxp teams start` (it creates the kill-switch file `DISABLED` next to the store; the running helper stops at its next heartbeat and standbys and services stay idle). `shutdown` in the Teams control chat does the same.
- `cxp teams start`: start it again after `cxp teams stop`; it starts this machine's installed service when that is not running, otherwise it says which helper picks up.
- `cxp teams doctor`: local diagnostics. `--live` also checks Microsoft Graph sign-in and read access; `--appserver-probe` checks Codex app-server compatibility without starting a model turn.
- `cxp teams control`: print this machine's control chat from the local store.
- `cxp teams control --recreate --yes`: create a fresh control chat and move the helper to it; the old chat gets an @mentioned link to the new one and is kept. Like the other commands below, the running helper carries it out; when no helper runs the request is kept until it starts (`cxp teams run --once` runs it now).
- `cxp teams chat recreate <session|codex-thread-id|teams-chat-id> --yes [--history full] [--activate]`: create a fresh work chat for an existing session and move the session there; `--history full` publishes the whole local transcript into the new chat first, `--activate` also reactivates a held session. The old chat gets a link and is kept.
- `cxp teams chat publish-history <session> --full --yes [--force]`: publish what the session's Teams chat is missing of its local Codex transcript; `--force` replays the whole transcript once more.
- `cxp teams send-file <path> --session <session>` (or `--chat-id <chat> --yes`): upload a file and send it as an attachment in a work chat. Relative paths are read from the Teams outbound root; other paths need `--allow-local-path`. `--message` sends a note before it.
- `cxp teams migrate`: convert a stopped v0.1.20 or v0.1.22-rc.59 helper store explicitly (`--dry-run --out <path>` previews). The helper converts automatically at its first start and never modifies the old store.
- `cxp teams path status [--show-path]`: resolve the effective Teams Codex PATH; the full value stays redacted unless explicitly requested.
- `cxp teams path mode account-default|explicit`: select the PATH source for future cold starts; `cxp teams path explicit <PATH>` saves an explicit PATH, and `cxp teams path shell <absolute-path|default>` sets or clears an account-shell override.
- `cxp teams service bootstrap`: install or repair the background service/watchdog and start it.
- `cxp teams service status`: inspect OS service/task state.
- `cxp teams service restart`: restart the helper service from a local terminal; interrupted work is recovered at start and each interrupted request gets one notice. While the helper is stopped on all machines, `service start` and `service restart` refuse and point to `cxp teams start`.
- `cxp teams service stop`: stop this machine's service only; a helper on another machine sharing the home may take over. Use `cxp teams stop` to stop it everywhere.
- `cxp teams service doctor`: diagnose service backend readiness.
- On Linux, service auto mode prefers `systemd --user`; if no user manager is usable, it falls back to `local-supervisor`, which survives terminal close and helper crashes but not machine/container reboot. Enabled or active local-supervisor installs stay sticky to avoid backend flapping; in WSL, the Windows Task backend remains preferred unless local-supervisor is sticky or explicitly selected.
- `cxp teams workflow status|enable|disable|test`: configure optional Teams Workflow notification cards; store the webhook URL in a private local file and pass that file path.

From a Teams-launched Codex child turn, do not restart, update, kill, replace, or background the running helper directly. Tell the user to send `helper restart now` after local upgrades or `helper update now` / `helper update prerelease` for release updates.

## Cross-Machine Delegation

Use these from a Codex turn when the user naturally asks another signed-in Teams machine or remote agent to inspect part of the task. Load `references/delegation.md` for the full workflow.

- `cxp delegate resolve --query <text> --json`: return a compact action plus the visible registry candidates from the inspected window; add `--source-session <codex-thread> --workspace-fingerprint <key>` when available so the result can include a `new_thread_token` and reusable `thread_candidates`. Registry internals stay out of the model prompt.
- `cxp delegate start --candidate-token <token> --task-file <path> --json`: create an idempotent delegation request for the selected machine. Add `--new-thread-token <token>` for a fresh remote thread or `--thread-token <token>` to continue a reusable remote thread when `resolve` returned one.
- `cxp delegate status --id <delegation-id> --json`: inspect reduced state.
- `cxp delegate wait --id <delegation-id> --timeout <duration> --json`: wait for a terminal result or a target-side question without tight polling. Add `--until terminal` to ignore intermediate questions.
- `cxp delegate cancel --id <delegation-id> --json`: tombstone an open request.
- `cxp delegate claim --id <delegation-id> --machine-id <machine> --worker-instance <worker> --json`: claim an open request from the target machine side; execute only when JSON returns `winning=true` and `should_execute=true`.
- `cxp delegate progress --id <delegation-id> --claim-id <claim> --claim-epoch <n> --machine-id <machine> --worker-instance <worker> --body <text> --json`: publish sparse intermediate progress for the winning claim.
- `cxp delegate question --id <delegation-id> --claim-id <claim> --claim-epoch <n> --machine-id <machine> --worker-instance <worker> --body <text> --json`: publish one sparse blocker question for the source agent/user.
- `cxp delegate result --id <delegation-id> --claim-id <claim> --claim-epoch <n> --machine-id <machine> --worker-instance <worker> --status complete|blocked|reuse_rejected --body <text> --json`: publish the terminal result for the winning claim. Normal target helpers publish `reuse_rejected` automatically when Agent B replies with `CXP_REUSE_REJECTED: <reason>` for a bad reused thread.
- `cxp delegate machine publish-once --machine-id <machine> --capability <cap> --json`: publish one machine capability heartbeat to the Teams registry for diagnostics; active `cxp teams run` helpers publish and patch their heartbeat automatically.

## Teams Chat Commands

Control chat commands (a leading `helper` is accepted too):

- `new <directory>` or `n <directory>`: start a Codex session in a new work chat.
- `list`: list sessions with their labels (for example `s003`).
- `r <session>`: wake a parked session.
- `park <session>`: stop reading a session's chat until it is woken.
- `fork <session>`: copy a session into a new work chat.
- `model`: list the models Codex offers.
- `stats`: requests and token usage of all sessions.
- `skills`: list skill subscriptions (`skills add <url>`, `skills sync`).
- `status`: helper status.
- `restart`: show the safe-restart confirmation; `helper restart now` restarts when the running requests finish (messages sent meanwhile are saved and run once after the restart); `helper restart force` restarts at once (running requests are interrupted and reported once).
- `update now` / `update prerelease`: update the helper (`update status` shows a pending update).
- `codex update now`: update the local Codex CLI using its detected install source; this does not update the helper.
- `help`: the command list.

Work chat commands:

- Regular text, files and voice messages: start a task when idle, or add instructions to the active task immediately (steer). Voice is transcribed locally first.
- `helper queue <prompt>`: run a separate task after the current one. Regular messages can still steer while queued tasks wait.
- With the local helper running, create or resume through `cxp tui` to share the native backend with Teams. Closing the TUI leaves its task running; updates wait for attached windows to close. Existing independent Codex windows must be closed and reopened to join that backend.
- `helper cancel` / `helper cancel last`: cancel the newest queued request (or the running one when none is queued); `helper cancel queued`, `helper cancel running`, `helper cancel all`, `helper cancel <turn-id>` (`!cancel <turn-id>`). The reply names the cancelled prompt and what is still queued.
- `helper retry`: run the last ignored duplicate request again; `helper retry last` runs the last failed, interrupted, not-started or cancelled request again; `helper retry <turn-id>` (`!retry <turn-id>`) runs that one again. A request is never retried automatically.
- `helper unpause`: resume auto-start after a pause (held requests are not run).
- `helper park`: stop reading this chat until woken.
- `helper status`: session status.
- `helper fork`: copy this session into a new work chat (also while a request runs; the copy stops at the last finished request), then post the new chat's link.
- `helper model`, `helper model list`, `helper model <model> [effort]`, `helper model reset`: show or switch this session's model.
- `helper effort <effort>`: switch this session's reasoning effort.
- `helper stats`: requests and token usage of this session.
- `helper skills`: skill subscriptions.
- `helper file <relative-path>` / `!file <relative-path>`: send one file from the Teams outbound root (the bare command shows the root).
- `helper restart`: restart the helper (see `restart` above).
- `helper help`: the command list.

Codex returns files to a work chat by writing them under the per-turn directory named in its prompt and listing them in a `codex-helper-artifacts` block of the final answer.

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

From a Teams child turn, do not self-manage the running helper process. Use control chat update or restart commands for installed helpers.
