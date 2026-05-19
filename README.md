# codex-proxy

Run `codex` (or any command) through an SSH-backed local proxy stack:

- **Upstream**: `ssh -D 127.0.0.1:<port>` SOCKS5 tunnel
- **Downstream**: local **HTTP CONNECT** proxy (Go) that dials via SOCKS5
- **Run supervision**: if the proxy becomes unhealthy and cannot be healed, the target process is terminated to avoid direct connections

This project is designed to ship as a **single binary** per OS/arch.

## Quick start

### 1) **Install**

Linux / macOS:

```bash
sh -c 'url="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.sh"; if command -v curl >/dev/null 2>&1; then curl -fsSL "$url" | sh; elif command -v wget >/dev/null 2>&1; then wget -qO- "$url" | sh; else echo "need curl or wget" >&2; exit 1; fi'
```

Windows (PowerShell):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command "iwr -useb https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1 | iex"
```

The installer drops a `cxp` shim alongside `codex-proxy`, tries to add the
install directory plus the managed CLI directory to PATH, and also adds a `cxp`
shell alias where applicable. Open a new shell if the command is not found.

### 2) **Run**

```bash
codex-proxy
# or
cxp
```

On first run, if no proxy preference or profile has been saved yet, you'll be
asked whether to use the SSH proxy. Choose **no** for direct connections.
Choose **yes** to enter SSH host/port/user and let the tool create a dedicated
key if needed. You can toggle proxy mode later with `Ctrl+P` in the TUI.

### 3) Optional: initialize Teams helper

Teams helper is currently available from **pre-release builds only**. After the
normal install, switch `codex-proxy` to the newest pre-release:

```bash
codex-proxy upgrade --include-prerelease
```

If your installed stable version does not recognize `--include-prerelease`,
install the newest `v0.1.0-rc.*` tag from GitHub Releases explicitly with the
installer `--version` option once. After that, future pre-release updates can
use `codex-proxy upgrade --include-prerelease`. The detailed install section
below shows the exact `--version` installer commands.

Then run the interactive Teams setup script. It asks for the required Teams
auth metadata, starts Microsoft device login, verifies the local auth cache,
and bootstraps the background helper service.

Linux / macOS / WSL:

```bash
sh -c 'set -e; url="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/scripts/teams-auth-bootstrap.sh"; tmp="${TMPDIR:-/tmp}/teams-auth-bootstrap.sh"; if command -v curl >/dev/null 2>&1; then curl -fsSL "$url" -o "$tmp"; elif command -v wget >/dev/null 2>&1; then wget -qO "$tmp" "$url"; else echo "need curl or wget" >&2; exit 1; fi; bash "$tmp"; rm -f "$tmp"'
```

Windows (PowerShell):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command '$u="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/scripts/teams-auth-bootstrap.ps1"; $p=Join-Path $env:TEMP "teams-auth-bootstrap.ps1"; iwr -useb $u -OutFile $p; & $p; Remove-Item -Force $p'
```

When setup finishes, open the Teams control chat shown by bootstrap and send
`help`.

### 4) Next steps

- Press Enter to open the selected Codex session.
- If there is no history yet, Enter starts a new session in the current directory.
- If you have multiple profiles, select one with `codex-proxy <profile>`.
- Run any command using the current direct/proxy mode with
  `codex-proxy run -- <cmd> [args...]`.
- Force proxy mode for one command with
  `codex-proxy run [profile] -- <cmd> [args...]`.
- If no command is given after `--`, `run` launches `codex`.
- If no proxy profile exists yet, `run` will guide you through creating one.
- Example: `codex-proxy run pdx -- curl https://example.com`.

### Optional: preconfigure a proxy profile

```bash
codex-proxy init
```

Config is stored under your OS user config directory (Linux typically
`~/.config/codex-proxy/config.json`).

## Requirements

- Proxy mode requires `ssh` (OpenSSH client)
- `ssh-keygen` is optional (only needed when proxy mode creates a dedicated key)
- Direct mode does not require SSH tools
- If `codex` is missing or unusable, `codex-proxy` can install `@openai/codex`
  in a user-local location and bootstrap a managed Node.js runtime when needed

Check your environment (`proxy doctor` is informational and may also report
missing `node`, `npm`, or `codex` even though `codex-proxy` can install managed
copies later):

```bash
codex-proxy proxy doctor
```

## Commands

| Command | Description |
|---------|-------------|
| `codex-proxy [profile]` | Open the TUI (default) |
| `codex-proxy --upgrade-codex` | Reinstall Codex CLI using detected install source |
| `codex-proxy completion <shell>` | Generate shell completion |
| `codex-proxy init` | Create an SSH profile |
| `codex-proxy run [profile] -- <cmd> [args...]` | Run a command using the current mode, or force proxy when a profile is given (`codex` by default) |
| `codex-proxy tui` | Browse Codex history in a terminal UI |
| `codex-proxy history tui` | Browse Codex history in a terminal UI |
| `codex-proxy history list [--pretty]` | List discovered projects/sessions as JSON |
| `codex-proxy history show <session-id>` | Print full history for a session |
| `codex-proxy history open <session-id>` | Open a session in Codex |
| `codex-proxy skills install-builtin` | Install or repair bundled skills, including the built-in `cxp` usage skill |
| `codex-proxy skills add <git-url>` | Install skills from a git source and keep them updated |
| `codex-proxy skills list` | List Codex skill subscriptions |
| `codex-proxy skills sync [name]` | Sync one skill source, or all sources when no name is given |
| `codex-proxy skills push [name]` | Push local skill edits with per-change confirmation |
| `codex-proxy skills doctor` | Check local skill subscription state |
| `codex-proxy skills remove <name>` | Remove a skill subscription and managed installed skills |
| `codex-proxy beacon profile list` | List beacon execution profiles |
| `codex-proxy beacon profile create <name>` | Create a draft beacon execution profile |
| `codex-proxy beacon profile update <name>` | Create a new profile revision without breaking chats already bound to the old revision |
| `codex-proxy beacon profile history <name>` | List current and historical revisions for a beacon profile |
| `codex-proxy beacon profile rollback <name> <revision>` | Publish a historical profile revision as a new latest revision |
| `codex-proxy beacon profile gc <name>` | Prune historical revisions no active target/allocation still references |
| `codex-proxy beacon profile doctor <name>` | Validate profile fields and provider adapter commands without touching the scheduler |
| `codex-proxy beacon profile doctor <name> --smoke` | Submit, query, and cancel one scheduler allocation to verify adapters |
| `codex-proxy beacon profile confirm <name>` | Confirm a beacon profile after review |
| `codex-proxy beacon status [--session <id>]` | Show beacon target state |
| `codex-proxy beacon release <profile\|allocation\|provider-job\|machine>` | Preview and release a beacon resource by profile, allocation id, provider job id, or machine id |
| `codex-proxy beacon switch-profile <name> --session <id>` | Switch a conversation to a ready beacon profile |
| `codex-proxy beacon switch-profile <name> --session <id> --after-current-turn` | Defer a beacon switch so the current Codex turn can finish |
| `codex-proxy beacon allocation list` | List managed beacon allocation requests |
| `codex-proxy beacon allocation status <allocation-or-provider-job>` | Show one managed allocation request |
| `codex-proxy beacon allocation cancel <allocation-or-provider-job>` | Cancel one managed allocation through the configured provider adapter |
| `codex-proxy beacon allocation reconcile <allocation>` | Query/adopt/submit through the configured provider adapter |
| `codex-proxy beacon allocation reconcile-all` | Reconcile all allocations, drain stale workers, and recover stale claims |
| `codex-proxy beacon provider template slurm` | Print a starter Slurm adapter script |
| `codex-proxy beacon provider template lsf` | Print a starter LSF adapter script |
| `codex-proxy beacon worker run-once --machine <id>` | Claim one queued beacon job on an allocated worker and publish its terminal result |
| `codex-proxy beacon worker run-once --allocation <id> --wait 30m` | Register the current scheduler worker, wait for its Teams job, and publish the terminal result |
| `codex-proxy beacon worker serve --allocation <id>` | Register a long-lived worker with bootstrap diagnostics and serve jobs until idle or stopped |
| `codex-proxy proxy start [profile]` | Start a long-lived proxy daemon |
| `codex-proxy proxy list` | List known proxy instances |
| `codex-proxy proxy stop <instance-id>` | Stop a proxy instance |
| `codex-proxy proxy prune` | Remove dead/unhealthy instances |
| `codex-proxy proxy doctor` | Report environment issues and installation hints |
| `codex-proxy teams status` | Show Teams helper state, control chat, service, owner, and queue status |
| `codex-proxy teams doctor` | Check local Teams helper auth and service readiness |
| `codex-proxy teams service bootstrap` | Install or repair the background Teams helper service |
| `codex-proxy teams control --print` | Print the configured Teams control chat link |
| `codex-proxy upgrade` | Self-update from GitHub Releases |

Common flags:

- `--config /path/to/config.json` overrides the config file location
- `tui` / `history tui` support `--codex-dir`, `--codex-path`, `--profile`, and `--refresh-interval` (default `5s`, use `0` to disable)
- `history open` supports `--codex-dir`, `--codex-path`, and `--profile`
- `history list` / `history show` support `--codex-dir`
- `skills` supports `--codex-dir`
- `beacon` supports `--store /path/to/beacon.json` to override the beacon state file

## Beacon execution profiles

Beacon profiles describe where Codex work should execute. They are separate
from SSH proxy profiles: `codex-proxy proxy` controls network routing, while
`codex-proxy beacon profile ...` controls scheduler or worker placement.

Create a Slurm draft profile with the scheduler fields your site requires:

```bash
codex-proxy beacon profile create gpu \
  --provider slurm \
  --partition interactive \
  --image image.sqsh \
  --nodes 1 \
  --gpu 1 \
  --duration 4 \
  --query-command /path/to/query-slurm-allocation \
  --submit-command /path/to/submit-slurm-allocation \
  --cancel-command /path/to/cancel-slurm-allocation \
  --renew-command /path/to/renew-slurm-allocation
```

LSF and local drafts use smaller inputs:

```bash
codex-proxy beacon profile create batch --provider lsf --queue normal
codex-proxy beacon profile create local --provider local
```

If the beacon job should use an existing SSH proxy profile for network access,
add `--proxy ssh_profile --proxy-profile <existing-profile>`. Add
`--isolation shared` or `--isolation exclusive` to choose the default lease
sharing mode.

Profiles stay draft until checked and confirmed. `profile doctor` validates the
profile fields and the `query`/`submit`/`cancel`/`renew` adapter commands that
future Teams turns will need:

```bash
codex-proxy beacon profile doctor gpu
codex-proxy beacon profile doctor gpu --smoke
codex-proxy beacon profile confirm gpu
codex-proxy beacon profile status gpu
```

Plain `doctor` is non-destructive and checks profile fields plus adapter
presence/executability. Add `--smoke` only when you want cxp to submit, query,
and cancel one real scheduler allocation to verify the adapter output contract
from the same profile/environment future Teams turns will use.

To change a profile, update it in place. The helper records a new revision, while
queued or running turns keep their existing target snapshot:

```bash
codex-proxy beacon profile update gpu \
  --provider slurm \
  --partition interactive \
  --image new-image.sqsh \
  --nodes 1 \
  --gpu 1 \
  --duration 4
```

Use `profile history` to inspect published revisions. If a new revision is bad,
`profile rollback <name> <revision>` republishes the historical config as a new
latest revision, and `profile gc <name>` removes only history entries that no
conversation, queued turn, or allocation still references.

After a profile is ready, inspect target state or switch an existing
conversation explicitly:

```bash
codex-proxy beacon status --session <session-id>
codex-proxy beacon switch-profile gpu --session <session-id>
```

In Teams, use `beacon switch <profile>` from the Work chat. The helper then
submits, queries, waits for the worker, renews, and cleans up the managed
allocation automatically. If you need to manually free the current resource,
send `beacon release` in the Work chat; the profile binding stays unchanged, so
future turns may acquire a fresh worker for the same profile. If the worker is
shared, Work-chat release detaches only the current chat's demand and leaves the
shared worker available to other chats; control-chat release is still required
for a shared or forced release that would affect everyone. `beacon local`
switches future turns back to local execution and asks the helper to drain or
release this chat's old beacon resource when that is safe.

From the CLI or Teams control chat,
`codex-proxy beacon release <profile|allocation|provider-job|machine>` accepts
the resource identifier you have and resolves the internal object type. Release
commands show a preview with affected chats, queued/running turns, allocation
ids, provider job ids, and the planned action. Shared or forced releases require
the shown `--confirm <token>` value before they can affect other chats.

When a Teams Work chat targets a beacon profile, each turn snapshots the target
and records a managed allocation request before Codex can start. Explicit beacon
turns do not fall back to local execution. If no accepting beacon worker/lease is
available yet, `beacon status` shows the allocation id, allocation state,
provider job id, provider state, and provider reason that need attention.

Provider submission is adapter-based. The least surprising Teams workflow is to
store adapter commands on the beacon profile with `--query-command`,
`--submit-command`, `--cancel-command`, and `--renew-command`; those profile
changes apply to future turns without reloading the Teams helper. Managed
Slurm/LSF adapters use your default user shell environment by default, so site
setup from modules, `submit_job`, NSS, or `SUBMIT_ACCOUNT` is available without
copying those variables into the helper service. Add `--adapter-shell direct`
only when an adapter needs the older clean service environment. To start from a
site-editable Slurm or LSF wrapper, print a template:

```bash
codex-proxy beacon provider template slurm > ~/bin/cxp-beacon-slurm-adapter
chmod +x ~/bin/cxp-beacon-slurm-adapter
```

If you prefer one global adapter per provider, point managed beacon allocations
at executable adapters through the helper service environment instead:

```bash
export CODEX_HELPER_BEACON_SLURM_QUERY=/path/to/query-slurm-allocation
export CODEX_HELPER_BEACON_SLURM_SUBMIT=/path/to/submit-slurm-allocation
export CODEX_HELPER_BEACON_SLURM_CANCEL=/path/to/cancel-slurm-allocation
export CODEX_HELPER_BEACON_SLURM_RENEW=/path/to/renew-slurm-allocation
export CODEX_HELPER_BEACON_LSF_QUERY=/path/to/query-lsf-allocation
export CODEX_HELPER_BEACON_LSF_SUBMIT=/path/to/submit-lsf-allocation
export CODEX_HELPER_BEACON_LSF_CANCEL=/path/to/cancel-lsf-allocation
export CODEX_HELPER_BEACON_LSF_RENEW=/path/to/renew-lsf-allocation
export CODEX_HELPER_BEACON_PROVIDER_SHELL_MODE=user
```

When no adapter shell mode is set, managed Slurm/LSF adapters behave as
`--adapter-shell user`. cxp uses `$SHELL -lic` only to capture a framed
environment snapshot, then runs the adapter directly with that environment so
shell startup output cannot pollute the adapter protocol. Use
`--adapter-shell shell-command` only for rare sites where scheduler submission is
a shell function or alias that cannot be captured as environment. The adapter
receives flags such as `--request-id`, `--name`, `--profile`, `--provider`,
`--partition`, `--image`, `--queue`, and
`--operation query|submit|cancel|renew`. It should print JSON like
`{"provider_job_id":"123","raw_state":"PD","reason":"Resources","provider_deadline":"2026-05-18T10:30:00Z"}`
or key-value output such as
`provider_job_id=123 raw_state=PD reason=Resources provider_deadline=1779090600`.
The generated Slurm/LSF templates include `query`, `submit`, `cancel`, and a
site-policy `renew` stub that exits non-zero until edited; implement the renew
case when the scheduler exposes walltime extension.

The active Teams helper owner periodically queries existing provider jobs,
projects scheduler state back into beacon machines/jobs, and renews allocations
whose provider deadline is near. Provider calls are never made while holding the
beacon state lock, and renewal results are applied only when the provider job id
and renew epoch still match. During helper drain, the renewal controller only
protects allocations whose job may already have started; it does not create new
scheduler work for pre-start turns. Release/cancel intents are durable, so the
active owner retries them during reconcile if an earlier path recorded the
intent before the provider adapter became available. The same reconcile pass
drains idle workers with no chat/job demand so shared and exclusive workers do
not stay accepting forever after demand disappears.

Inside an allocated worker, the scheduler job should register itself against the
managed allocation and wait for the Teams turn to enqueue work:

```bash
codex-proxy beacon worker run-once --allocation <request-id> --wait 30m
```

The worker derives the scheduler job id from `SLURM_JOB_ID` or `LSB_JOBID` when
`--provider-job` is not provided, registers an accepting machine, claims one
queued job, runs Codex with the snapshotted prompt and workspace, then writes a
fenced terminal result back to the shared beacon state. Teams waits through the
allocation and worker terminal path instead of failing early or running local
Codex.

For reusable allocations, run a long-lived worker instead:

```bash
codex-proxy beacon worker serve --allocation <request-id> --idle-timeout 30m
```

`serve` records worker heartbeats, runs the worker doctor before accepting jobs,
stores bootstrap diagnostics such as node list, stdout/stderr paths, shared
beacon store path, and `codex`/`cxp` paths, then drains the machine on exit. A
controller or cron job can reconcile stale state with:

```bash
codex-proxy beacon allocation reconcile-all
```

Scheduler-capable CI can opt in to the real adapter path with
`CODEX_HELPER_BEACON_LIVE=1`, `CODEX_HELPER_BEACON_LIVE_PROVIDER=slurm|lsf`,
and the matching `CODEX_HELPER_BEACON_*_QUERY/SUBMIT/CANCEL` commands. The live
test submits through the profile-stored adapter snapshot and cancels the
provider job during cleanup.

When issuing a beacon switch from inside an active Codex turn, prefer the
deferred form so the current answer stays on its existing target and later
turns use the new profile:

```bash
codex-proxy beacon switch-profile gpu --session <session-id> --after-current-turn
```

## Built-in cxp skill

The installer best-effort installs a bundled `cxp` Codex skill into
`~/.codex/skills/cxp`. The skill contains the local command map and the safe
handoff rules for disruptive operations such as beacon profile switching and
Teams helper restarts. It is managed as a built-in skill, not as a git-backed
subscription, so it does not appear as a remote source in `codex-proxy skills
list`.

To repair or install it manually:

```bash
codex-proxy skills install-builtin
```

## Codex history

Browse Codex history in an interactive terminal UI:

```bash
codex-proxy tui
# or
codex-proxy history tui
```

This opens the TUI. Press Enter to open the selected session in Codex
using the current proxy mode (direct or SSH proxy). Toggle proxy mode with
`Ctrl+P`; if proxy is enabled but not configured, you will be prompted to
enter SSH host/port/user. If no history exists yet, Enter starts a new
session in the current directory.

The Projects column always includes your current working directory and marks
it as `[current]`. The Sessions column always includes a `(New Agent)` entry.

If you have multiple proxy profiles:

```bash
codex-proxy tui --profile <profile>
```

Default data dir is `~/.codex`. You can override it with:

```bash
codex-proxy tui --codex-dir /path/to/.codex
```

Controls:

- Navigation: Up/Down
- Preview scroll: PageUp/PageDown, Home/End
- Switch pane: Tab / Left / Right (also `h`/`l`)
- Search: `/` then type, Enter apply, Esc cancel (`n`/`N` next/prev in preview)
- Open: Enter (opens in Codex and sets cwd)
- New session: `(New Agent)` entry or `Ctrl+N` (in selected project or current dir)
- Expand/collapse subagents: `Ctrl+O`
- Proxy mode: `Ctrl+P` toggle (status shows `Proxy mode (Ctrl+P): on/off`)
- Refresh: `r` (or `Ctrl+R`)
- Quit: `q`, `Esc`, `Ctrl+C`
- In-app update: `Ctrl+U` (when an update is available)

If the update check fails, the status bar shows the error.

List sessions as JSON:

```bash
codex-proxy history list --pretty
```

Print a full session by id:

```bash
codex-proxy history show <session-id>
```

Open a session directly in Codex:

```bash
codex-proxy history open <session-id>
```

This uses the current proxy mode (direct or SSH proxy). If proxy mode is
enabled but no profile exists, you will be prompted to configure SSH.

If `codex` is missing or unusable, `codex-proxy` will automatically install
`@openai/codex` in a user-local location (and bootstrap a private Node.js
runtime when the system Node.js is missing or too old).

To use a specific Codex binary:

```bash
codex-proxy tui --codex-path /path/to/codex
```

## Teams helper

Teams helper lets you drive Codex from Microsoft Teams while keeping execution
on your own machine. A local `codex-proxy teams run` listener reads Teams
messages, starts or resumes Codex sessions in the selected project directory,
and sends status, answer, artifact, and notification updates back to Teams.

Teams helper is currently a **pre-release-only** feature. Install the normal
binary first, then update to the newest pre-release before setup:

```bash
codex-proxy upgrade --include-prerelease
```

If the installed stable binary is too old to support `--include-prerelease`,
install the newest `v0.1.0-rc.*` tag explicitly with the installer `--version`
option once, then use `codex-proxy upgrade --include-prerelease` for later
pre-release updates.

From an existing Teams control chat, you can ask the helper itself to move to
the newest release or pre-release:

```text
helper update prerelease
```

### Fast setup

The recommended setup path is the interactive bootstrap script from the quick
start section. It performs the complete local setup flow:

1. Configure Teams auth metadata.
2. Run Microsoft device login.
3. Verify the local Teams auth cache.
4. Install or repair the background Teams helper service.
5. Open or print the Teams control chat link.

The script stores auth metadata and tokens locally under the current user. It
does not require a project checkout and does not hard-code Teams auth values;
it asks for the required values interactively unless you provide them with
environment variables or script flags.

### How it works

The helper creates a **control chat** for machine-level commands and separate
work chats for Codex sessions. Use the control chat to list projects, choose a
project/session, start new Codex work, check status, recover stuck state,
restart or reload the helper, and update the helper.

When a Teams work chat message is routed to Codex, the helper queues it,
starts Codex with the selected session working directory, streams progress as
status updates, and posts the final answer back to the work chat. If a Codex
answer is detected from another local entry point, the helper can create or
link the matching Teams chat and notify you there as well.

Teams file and image attachments can be passed through to Codex when available.
Generated files listed in a Codex artifact manifest can be uploaded back to
Teams when file-write auth is configured.

The background service keeps the listener alive after terminal close, SSH
disconnect, WSL session exit, sleep/wake, or helper upgrade. Service bootstrap
chooses the native per-user service mechanism for the platform where possible
and repairs old helper service definitions when it can do so safely.

### Common Teams commands

Run these locally when diagnosing setup:

```bash
codex-proxy teams status
codex-proxy teams doctor --live
codex-proxy teams control --print
codex-proxy teams service doctor
codex-proxy teams service status
codex-proxy teams service bootstrap
```

In the Teams control chat, start with:

```text
help
projects
status
```

Beacon profile setup is a local `codex-proxy beacon ...` CLI workflow. You can
ask questions about it in the Teams control chat, but profile mutation commands
should be run locally unless the helper prints a specific supported Teams
command.

Control-chat Codex fallback runs under the Teams helper service environment,
not necessarily your interactive shell. If it needs to inspect the local helper
binary, the child process receives `CODEX_HELPER_CLI_PATH`; a missing `cxp`
alias in that environment does not mean the installed helper is missing.

If you update the helper while Teams work is active, the helper drains current
work first, restarts the service when needed, and then sends a completion or
failure notice back to Teams.

For a deeper deployment and troubleshooting guide, see
[`docs/teams_source_deployment_guide.md`](docs/teams_source_deployment_guide.md).

## Upgrade

Upgrade from GitHub Releases:

```bash
codex-proxy upgrade
```

Optional flags:

- `--repo owner/name` (override GitHub repo)
- `--version vX.Y.Z` (install a specific version)
- `--include-prerelease` (allow latest to resolve to the newest pre-release)
- `--install-path /path/to/codex-proxy` (override install path; file or directory)

Teams background mode also checks `codex-helper` GitHub Releases every 30
minutes and silently applies eligible helper updates after current Teams/Codex
work drains:

- `p0`: update as soon as the release is detected.
- `p1`: update after the release has been published for 48 hours.
- `p2`: never auto-update.

Releases default to `p2` unless the release notes include this machine-readable
marker:

```md
<!-- codex-helper-release: {"auto_update_priority":"p1"} -->
```

Use `p0`, `p1`, or `p2` in the marker. A release asset named
`codex-helper-auto-update-p0`, `codex-helper-auto-update-p1`, or
`codex-helper-auto-update-p2` is also accepted; conflicting markers fail closed
to `p2`. Release publishing updates a static `auto-update-index` branch, so
Teams helper checks normally fetch one small JSON file instead of listing GitHub
Releases. If the index is unavailable, the helper falls back to the GitHub
Release API. Teams mode ignores draft releases, prereleases, older versions,
and releases without a matching platform asset.

Upgrade Codex CLI itself (reinstall-style):

```bash
codex-proxy --upgrade-codex
```

Behavior:

- Uses current proxy preference: proxy on -> upgrade through proxy; proxy off -> direct.
- Requires Codex to already be installed; it will not install from scratch in this mode.
- Keeps install source when recognized:
  - system npm global install -> `npm install -g @openai/codex`
  - managed/local npm install (`codex-proxy` prefix) -> managed reinstall path
- Fails fast when source cannot be determined (to avoid changing install topology unexpectedly).

## Long-lived instances (optional)

Start a reusable daemon instance:

```bash
codex-proxy proxy start [profile]
codex-proxy proxy list
```

Normal `run`, `history open`, and TUI-launched sessions use private proxy stacks.
Only instances started with `proxy start` are shared/reused across sessions.

Use `--foreground` to keep the daemon attached to the current terminal.

Stop an instance:

```bash
codex-proxy proxy stop <instance-id>
```

## Install (detailed)

### Linux / macOS (one-liner, auto-detects curl/wget)

```bash
sh -c 'url="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.sh"; if command -v curl >/dev/null 2>&1; then curl -fsSL "$url" | sh; elif command -v wget >/dev/null 2>&1; then wget -qO- "$url" | sh; else echo "need curl or wget" >&2; exit 1; fi'
```

By default it installs to `~/.local/bin/codex-proxy`.

The installer drops a `cxp` shim alongside `codex-proxy` and tries to add
`~/.local/bin` plus the managed CLI directory to PATH (plus a `cxp` alias).
Open a new shell if the command is not found.
If you need to update PATH manually:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

Install a specific version (example):

```bash
curl -fsSL https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.sh | sh -s -- --version vX.Y.Z
```

### Windows (PowerShell one-liner)

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command "iwr -useb https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1 | iex"
```

By default it installs to `%USERPROFILE%\.local\bin\codex-proxy.exe`.
The installer also writes `cxp.cmd` there and updates PATH for that directory
plus the managed CLI directory.
The managed Codex CLI uses a native Windows binary; if it exits with
`0xC0000135` or mentions `VCRUNTIME140*.dll`, install the Microsoft Visual C++
2015-2022 Redistributable that matches the Codex architecture (x64:
`Microsoft.VCRedist.2015+.x64`, ARM64: `Microsoft.VCRedist.2015+.arm64`).
When this exact runtime failure is detected during managed Codex install,
`cxp` automatically attempts to install the redistributable and trigger a
Windows UAC prompt. Set `CODEX_PROXY_VCREDIST_INSTALL=never` to disable that,
or `prompt` to ask in the terminal before showing the UAC prompt.

Install a specific version:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command "$u='https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1'; $p=Join-Path $env:TEMP 'codex-proxy-install.ps1'; iwr -useb $u -OutFile $p; & $p -Version vX.Y.Z; Remove-Item -Force $p"
```

### Environment variables

These variables are used by the installer scripts. `codex-proxy upgrade` also
honors `CODEX_PROXY_REPO`, `CODEX_PROXY_VERSION`, and
`CODEX_PROXY_INSTALL_DIR`.

| Variable | Description |
|----------|-------------|
| `CODEX_PROXY_REPO` | Override GitHub repo (default: `baaaaaaaka/codex-helper`) |
| `CODEX_PROXY_VERSION` | Override version (default: `latest`) |
| `CODEX_PROXY_INSTALL_DIR` | Override install directory (Unix default: `~/.local/bin`; Windows default: `%USERPROFILE%\.local\bin`) |
| `CODEX_NPM_PREFIX` | Override the managed CLI npm prefix whose executable directory is added to PATH |
| `CODEX_PROXY_API_BASE` | Override GitHub API base URL |
| `CODEX_PROXY_RELEASE_BASE` | Override GitHub release base URL |
| `CODEX_PROXY_SKIP_PATH_UPDATE` | Windows installer only: skip persistent PATH updates when set to `1` |
| `CODEX_PROXY_PROFILE_PATH` | Windows installer only: override which PowerShell profile is updated |
