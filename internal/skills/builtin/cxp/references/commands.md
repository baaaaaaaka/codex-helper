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
- `cxp app --model-profile <name>`: launch the desktop app with a saved model profile through an isolated CODEX_HOME; quit an already-running app first so the setting takes effect.
- `cxp --upgrade-codex-app [profile]`: on native Windows or WSL, download and verify the official signed x64 ChatGPT MSIX, publish it beside the current CXP-managed runtime, and select it for future `cxp app` launches without modifying the Microsoft Store/AppX installation.
- `cxp run [profile] -- <cmd args...>`: run a command through the selected proxy profile and helper runtime handling.
- `cxp run -- codex`: launch the original Codex TUI through CXP's standard approval broker with manual approvals.
- `cxp run --aaa -- codex`: enable Agent Auto Approve for this run without changing the saved TUI preference.
- `cxp run --aaa -- codex exec <prompt>`: enable Agent Auto Approve for a non-interactive Codex turn; without AAA, approval-required operations fail closed.
- `cxp run --model-profile <name> -- codex`: launch Codex with a saved model profile for this run.
- `cxp init`: create or repair local proxy configuration interactively; when `~/.ssh/config` contains concrete `Host` entries, it can reuse and validate one before falling back to manual SSH host details.

## Model Profiles And Responses Adapter

- `cxp model list`: list built-in model choices and setup status.
- `cxp model setup <model> --api-key-stdin`: configure a built-in third-party model choice using an API key read from stdin.
- `cxp model setup <model> --api-key-env <ENV>`: configure a built-in model choice using an API key environment variable.
- `cxp model use <model>`: make a configured model the default for future Codex launches.
- `cxp model doctor [model]`: validate the profile backing a built-in model choice.
- `cxp model-profile setup [name] --provider <built-in-or-compatibility-provider> --model <provider-model> --api-key-stdin --set-default`: create or update a named profile. DeepSeek V4 and MiMo V2.5 are external-catalog-only; add or sync a catalog and activate its provider with `cxp model provider setup <provider>`. Generic providers also require `--base-url <url>` or `--base-url-env <ENV>` and can use `--supported-reasoning-efforts low,medium,high,xhigh` plus `--reasoning-effort-map xhigh=max`. When any third-party profile is configured, CXP exposes official and third-party models through one credential-isolating gateway; official-only configurations remain on Codex's native path.
- `cxp model-profile setup [name] ... --ssh-proxy <proxy-profile>`: route a model profile through an existing SSH proxy profile.
- `cxp model-profile list`: list saved model profiles.
- `cxp model-profile doctor [name]`: validate a saved model profile.
- `cxp model-profile set-default <name>`: set the default model profile.
- `cxp model-profile delete <name>`: delete a non-default model profile.
- `cxp model-source sync <repository-or-source> [--kind git|file|directory] [--name <name>] [--ref <ref>] [--file <path>]`: shallow-sync candidates without asking for a key. The source can be a Git URL, a single legacy JSON file, or a local directory containing a schema-v2 `manifest.json` with `providers/` and `models/` documents. The kind is inferred when omitted; a `manifest.json` path denotes its containing directory.
- `cxp model-source list`: list synced sources, candidates, and verification state.
- `cxp model-source bind <source> <profile> --api-key-stdin|--api-key-env <ENV>`: store a key only in the local secret store and run a timeout-bounded minimal real inference probe; only a successful, fingerprint-current profile enters CLI, App, and Teams model catalogs. Never put a key in the subscription JSON.
- Schema-v2 model identity is `provider/model`, so the same model id may exist behind multiple providers. A provider credential reference may be shared by its models, while each provider/interface can use its own auth shape and route policy.
- Schema-v2 parsing is fail-closed and atomic: unknown fields, trailing JSON, raw secrets, path escapes or symlinks, dangling manifest references, alias collisions, missing capability declarations, and unsupported adapter/protocol routes are rejected.
- Model-source refreshes preserve verification across Git-only changes. Effective model/config changes reuse the stored key for automatic verification, and a removed or failed default safely falls back to Codex Official.
- A failed refresh of a previously activated source records its last-known-good JSON as an active backup. `model-source list` and every accepted Teams turn using that source show a warning until sync recovers.
- CXP checks official login with `codex login status`. Logged-in launches merge the current Codex binary's complete bundled GPT catalog first; logged-out launches remain third-party-only and do not require OpenAI auth. Catalog preparation is local and cached: it never runs the old synchronous `model/list` prewarm or blocks third-party inference on official catalog readiness.
- Teams `model list` is verified-only. When Codex login succeeds, it reads the current account's app-server `model/list`, preserves official order, and prints the concrete `isDefault` model. Use `model setup` to discover candidates; a third-party model appears in the selectable list only after its model-specific authentication probe succeeds and remains fingerprint-current.
- `cxp responses serve --base-url <openai-compatible-url> --api-key-env <ENV> --model <model>`: run a local `/v1/responses` adapter backed by an OpenAI-compatible chat upstream.

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

- `cxp teams setup`: guided setup checklist.
- `cxp teams run`: run the Teams listener in the foreground.
- `cxp teams run --once`: poll once for diagnosis.
- `cxp teams status`: show registry, owner, listener, poll, and session status.
- `cxp teams doctor`: local diagnostics.
- `cxp teams doctor --live`: opt in to live Microsoft Graph checks.
- `cxp teams control --print`: print the bound Teams control chat.
- `cxp teams path status [--show-path]`: resolve the effective Teams Codex PATH; the full value stays redacted unless explicitly requested.
- `cxp teams path mode account-default|captured-terminal|explicit|service`: select the PATH source for future cold starts.
- `cxp teams path capture-terminal`: save the current terminal PATH; use `cxp teams path shell <absolute-path|default>` to set or clear an account-shell override.
- `cxp teams service bootstrap`: install or repair the background service/watchdog.
- `cxp teams service status`: inspect OS service/task state.
- `cxp teams service restart --force`: recover active local Teams state, mark ambiguous turns interrupted, then restart or activate the pending helper. Use only from a local terminal when you accept interrupting active work.
- `cxp teams service doctor`: diagnose service backend readiness.
- On Linux, service auto mode prefers `systemd --user`; if no user manager is usable, it falls back to `local-supervisor`, which survives terminal close and helper crashes but not machine/container reboot. Enabled or active local-supervisor installs stay sticky to avoid backend flapping; in WSL, the Windows Task backend remains preferred unless local-supervisor is sticky or explicitly selected.
- `cxp teams auth full`: refresh full Teams auth locally.
- `cxp teams auth full-status`: inspect auth cache expiry without printing tokens.
- `cxp teams workflow status|enable|disable|test`: configure optional Teams Workflow notification cards; store the webhook URL in a private local file and pass that file path.
- `cxp teams send-file <path> --session <session-id>`: upload a local outbound file and send it as a Teams attachment.
- `cxp teams probe-chat --chat <teams-chat-id-or-link>`: read-only probe of an external Teams chat without binding helper state unless `--send-test` is used.
- `cxp teams pause`, `cxp teams resume`, `cxp teams drain`, and `cxp teams recover`: local helper lifecycle controls for diagnosis and recovery.
- `cxp teams chat recreate <session-id> --history full --yes`: create and atomically bind a fresh Work chat for an existing helper session, pinning the store that owns the session and publishing full history when requested. Add `--activate` only to reactivate a quarantined session, or `--store <state.json>` to resolve an inspected retained-store conflict.
- `cxp teams chat publish-history <session-id> --full --yes`: idempotently publish complete local Codex history to the session's current Work chat. Add `--force` only for an intentional replay; the command briefly drains the listener and releases its maintenance fence before returning.
- `cxp teams chat quarantine <session-or-chat> --dry-run|--yes`: resolve exactly one durable Work-chat session and atomically stop its polling, turns, pending inbound, and unsent outbox after unsafe helper behavior. Mutation refuses a live helper owner.
- `cxp teams chat unquarantine <session-or-chat> --dry-run|--yes`: resume polling only for a session whose durable status is `quarantined`; interrupted turns and skipped messages are not replayed.

From a Teams-launched Codex child turn, do not restart, reload, update, kill, replace, or background the running helper directly. For normal installed helpers, tell the user to send `helper restart now` after local upgrades or `helper update now` / `helper update prerelease` for release updates. Use `helper reload now` only for source-checkout development reloads when the helper has access to a local `codex-helper` source tree.

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

Control chat commands:

- `projects` or `p`: list workspaces.
- `project <number>` or `p <number>`: open a workspace.
- `sessions`, `s`, or `history`: list local Codex sessions.
- `new <directory>` or `n <directory>`: create a Work chat for a directory.
- `new <directory> --model <profile>` or `new <directory> --model-profile <profile>`: create a Work chat pinned to a model profile.
- `new` or `n`: create a Work chat for the currently selected workspace.
- `continue <number-or-session-id>` or `c <...>`: create or open a Work chat for an existing session.
- `open <number>`: show an existing linked Work chat.
- `status` or `st`: list active Work chats.
- `model status`, `model list`, `model switch <model>`, and `model reset`: inspect or change only the Control chat model. `model use <model>` is an alias for `model switch <model>`.
- `model setup <model>` and `model doctor <model>`: configure or validate model availability without selecting it or changing a default.
- `effort status`, `effort list`, `effort set <value>`, and `effort reset`: inspect or change only the Control chat reasoning effort.
- `default status`, `default model status|list|set|reset`, and `default effort status|list|set|reset`: inspect or change global defaults for future launches and newly created chats. These commands are Control-chat-only and do not rewrite an existing chat.
- `helper cancel last`, `helper cancel queued`, `helper cancel running`, or `helper cancel all`: cancel or drop queued/running control-chat Codex question(s).
- `helper update now`: update to the latest stable helper release.
- `helper update prerelease`: update to the newest eligible release or pre-release.
- `codex update now`: wait for active work, then update the local Codex CLI using its detected install source; this does not update the helper.
- `helper restart now`: restart the installed Teams helper, including after a release update.
- `helper reload now`: development/source-checkout only; rebuild and reload from a local `codex-helper` source checkout.

Work chat commands:

- Regular text: send a task to Codex.
- `helper status`: check current request state.
- `helper retry last`: retry the last failed or interrupted request.
- `helper cancel last`, `helper cancel queued`, `helper cancel running`, or `helper cancel all`: cancel or drop queued/running work.
- `helper file <relative-path>`: upload a generated file from the Teams outbound folder.
- `model status`, `model switch <profile>`, `model reset`, and `model fork <profile>`: inspect, switch when compatible, reset to the effective global model, or fork the Work chat with another model profile. Official `gpt-*` slugs take precedence on name collisions; use `official:<slug>` or `profile:<name>` to select explicitly.
- `effort status`, `effort list`, `effort set <value>`, and `effort reset`: inspect or change this Work chat's reasoning effort.
- `default ...`: rejected in a Work chat because global defaults are Control-chat-only.
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

From a Teams child turn, do not self-manage the running helper process. Use control chat update or restart commands for installed helpers; use reload only for source-checkout development as directed by the helper safety rules.
