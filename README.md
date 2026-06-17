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
$ErrorActionPreference="Stop"; $u="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1"; $p=Join-Path $env:TEMP "codex-proxy-install.ps1"; try { Invoke-WebRequest -UseBasicParsing $u -OutFile $p; Unblock-File -LiteralPath $p; & powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File $p; if ($LASTEXITCODE -ne 0) { throw "installer exited with code $LASTEXITCODE" } } finally { Remove-Item -Force $p -ErrorAction SilentlyContinue }
```

The Windows command downloads the installer to a temporary file before running
it. It avoids piping remote script text into PowerShell while using a
process-scoped `RemoteSigned` policy so default script restrictions can still
run the local temporary file. Group Policy script restrictions can still block
execution.

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
To discard saved proxy setup, attempt to stop known proxy daemons, and see this prompt again, run
`codex-proxy proxy reset`.

### 3) Optional: initialize Teams helper

Teams helper is available in stable releases. A fresh install uses the current
stable helper. If this machine has an older `codex-proxy`, update to the latest
stable release first:

```bash
codex-proxy upgrade
```

Use `codex-proxy upgrade --include-prerelease` only when you intentionally want
to test the newest pre-release.

Then run the interactive Teams setup script. It asks for the required Teams
auth metadata, starts Microsoft device login, verifies the local auth cache,
and bootstraps the background helper service.

Linux / macOS / WSL:

```bash
sh -c 'set -e; url="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/scripts/teams-auth-bootstrap.sh"; tmp="${TMPDIR:-/tmp}/teams-auth-bootstrap.sh"; if command -v curl >/dev/null 2>&1; then curl -fsSL "$url" -o "$tmp"; elif command -v wget >/dev/null 2>&1; then wget -qO "$tmp" "$url"; else echo "need curl or wget" >&2; exit 1; fi; bash "$tmp"; rm -f "$tmp"'
```

Windows (PowerShell):

```powershell
$ErrorActionPreference="Stop"; $u="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/scripts/teams-auth-bootstrap.ps1"; $p=Join-Path $env:TEMP "teams-auth-bootstrap.ps1"; try { Invoke-WebRequest -UseBasicParsing $u -OutFile $p; Unblock-File -LiteralPath $p; & powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File $p; if ($LASTEXITCODE -ne 0) { throw "teams auth bootstrap exited with code $LASTEXITCODE" } } finally { Remove-Item -Force $p -ErrorAction SilentlyContinue }
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

### Usage levels

Most users only need the quick start above plus the common commands below:

- **Basic local use**: install, run `cxp`, choose direct or SSH proxy mode, and
  open sessions from the history TUI.
- **Daily options**: run commands through the selected proxy, choose a model
  profile, open the Codex desktop app, or drive Codex from Teams.
- **Advanced operations**: skills maintenance, beacon scheduler execution,
  Teams service recovery, workflow webhooks, and source-checkout deployment.

The rest of this README is grouped with that split in mind: common commands
come first, then optional model/desktop/Teams flows, then advanced execution
and maintenance references. For a map of all docs by audience and depth, see
[`docs/README.md`](docs/README.md).

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

## Common commands

These are the commands a normal install most often needs:

| Command | Description |
|---------|-------------|
| `codex-proxy` or `cxp` | Open the local Codex history TUI |
| `codex-proxy run -- <cmd> [args...]` | Run a command using the current direct/proxy mode |
| `codex-proxy run --yolo -- codex` | Launch Codex with YOLO mode enabled for this run |
| `codex-proxy run --model-profile <name> -- codex` | Launch Codex with a saved model profile for this run |
| `codex-proxy model list` | Show built-in and configured model choices |
| `codex-proxy model setup <model>` | Configure a built-in model choice such as `deepseek`, `mimo`, `kimi`, `glm`, `minimax`, or `qwen` |
| `codex-proxy proxy doctor` | Check local proxy/Codex prerequisites |
| `codex-proxy proxy reset` | Clear saved proxy setup and ask again on next launch |
| `codex-proxy app` | Launch the Codex desktop app on macOS, Windows, or WSL |
| `codex-proxy teams status` | Check Teams helper status after setup |
| `codex-proxy upgrade` | Update `codex-proxy` / `cxp` from GitHub Releases |

## Command reference

<details>
<summary>Full command reference (advanced)</summary>

This table includes advanced and maintenance commands as well as daily ones.
Skip it on first read if you are still setting up; the guided sections below
walk through the normal flows in order.

| Command | Description |
|---------|-------------|
| `codex-proxy [profile]` | Open the TUI (default) |
| `codex-proxy app [profile]` | Install if needed, use or configure proxy mode, and launch the Codex desktop app on macOS, Windows, or WSL |
| `codex-proxy app auth [profile]` | Complete ChatGPT auth for the Codex desktop app using the same `CODEX_HOME` and proxy setup |
| `codex-proxy app --model-profile <name>` | Launch the Codex desktop app with a saved model profile through an isolated `CODEX_HOME` |
| `codex-proxy --upgrade-codex` | Reinstall Codex CLI using detected install source |
| `codex-proxy completion <shell>` | Generate shell completion |
| `codex-proxy init` | Create an SSH profile |
| `codex-proxy run [profile] -- <cmd> [args...]` | Run a command using the current mode, or force proxy when a profile is given (`codex` by default) |
| `codex-proxy run --yolo -- codex` | Launch Codex with YOLO mode enabled for this run |
| `codex-proxy run --model-profile <name> -- codex` | Launch Codex with a saved model profile for this run |
| `codex-proxy tui` | Browse Codex history in a terminal UI |
| `codex-proxy history tui` | Browse Codex history in a terminal UI |
| `codex-proxy history list [--pretty]` | List discovered projects/sessions as JSON |
| `codex-proxy history show <session-id>` | Print full history for a session |
| `codex-proxy history open <session-id>` | Open a session in Codex |
| `codex-proxy model list` | List built-in model choices and setup status |
| `codex-proxy model setup <model>` | Set up a built-in model choice and optionally make it the default |
| `codex-proxy model use <model>` | Make an already configured model the default for future Codex launches |
| `codex-proxy model doctor [model]` | Validate the model profile backing a built-in model choice |
| `codex-proxy model-profile setup [name]` | Create or update a named model profile |
| `codex-proxy model-profile list` | List saved model profiles |
| `codex-proxy model-profile doctor [name]` | Validate a saved model profile |
| `codex-proxy model-profile set-default <name>` | Set the default model profile |
| `codex-proxy model-profile delete <name>` | Delete a non-default model profile |
| `codex-proxy responses serve` | Run a local `/v1/responses` adapter backed by an OpenAI-compatible chat upstream |
| `codex-proxy skills install-builtin` | Install or repair bundled skills in `$HOME/.agents/skills`, including the built-in `cxp` usage skill |
| `codex-proxy skills add <git-url>` | Install skills from a git source and keep them updated |
| `codex-proxy skills migrate` | Migrate managed legacy skills from `~/.codex/skills` to `$HOME/.agents/skills` |
| `codex-proxy skills list` | List Codex skill subscriptions |
| `codex-proxy skills sync [name]` | Sync one skill source, or all sources when no name is given |
| `codex-proxy skills push [name]` | Push local skill edits with per-change confirmation |
| `codex-proxy skills doctor` | Check local skill subscription state |
| `codex-proxy skills remove <name>` | Remove a skill subscription and managed installed skills |
| `codex-proxy proxy start [profile]` | Start a long-lived proxy daemon |
| `codex-proxy proxy list` | List known proxy instances |
| `codex-proxy proxy stop <instance-id>` | Stop a proxy instance |
| `codex-proxy proxy prune` | Remove dead/unhealthy instances |
| `codex-proxy proxy reset` | Clear saved proxy setup and attempt to stop known daemons so the next launch asks again |
| `codex-proxy proxy doctor` | Report environment issues and installation hints |
| `codex-proxy teams status` | Show Teams helper state, control chat, service, owner, and queue status |
| `codex-proxy teams doctor` | Check local Teams helper auth and service readiness |
| `codex-proxy teams workflow status` | Show optional Teams Workflow notification configuration |
| `codex-proxy teams workflow enable --webhook-url-file <path>` | Enable Workflow cards using a private local webhook URL file |
| `codex-proxy teams send-file <path> --session <id>` | Upload a local outbound file and send it as a Teams attachment |
| `codex-proxy teams probe-chat --chat <chat-id-or-link>` | Read-only probe of an external Teams chat without binding helper state |
| `codex-proxy teams pause` / `resume` / `drain` / `recover` | Pause, resume, drain, or recover Teams helper state from a terminal |
| `codex-proxy teams chat recreate <session-id> --yes` | Create and bind a fresh Teams Work chat for an existing helper session |
| `codex-proxy teams service bootstrap` | Install or repair the background Teams helper service |
| `codex-proxy teams service restart --force` | Recover local active Teams state, then force a service restart from a terminal |
| `codex-proxy teams control --print` | Print the configured Teams control chat link |
| `codex-proxy beacon profile list` | List beacon execution profiles |
| `codex-proxy beacon profile create <name>` | Create a draft beacon execution profile |
| `codex-proxy beacon profile update <name>` | Create a new profile revision without breaking chats already bound to the old revision |
| `codex-proxy beacon profile history <name>` | List current and historical revisions for a beacon profile |
| `codex-proxy beacon profile rollback <name> <revision>` | Publish a historical profile revision as a new latest revision |
| `codex-proxy beacon profile gc <name>` | Prune historical revisions no active target/allocation still references |
| `codex-proxy beacon profile doctor <name>` | Validate profile fields and provider adapter commands without touching the scheduler |
| `codex-proxy beacon profile doctor <name> --smoke` | Submit, query, and cancel one scheduler allocation to verify adapters |
| `codex-proxy beacon profile confirm <name>` | Confirm a beacon profile after review; incomplete profiles remain draft |
| `codex-proxy beacon profile status <name>` | Inspect one beacon profile |
| `codex-proxy beacon profile delete <name>` | Archive a beacon profile when it is not in active use |
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
| `codex-proxy beacon machine list` | List beacon machines |
| `codex-proxy beacon machine status <machine-or-lease>` | Inspect a beacon machine or lease and get confirmation tokens |
| `codex-proxy beacon machine release <machine-or-lease>` | Drain or release a beacon machine |
| `codex-proxy beacon machine kill <machine-or-lease-or-job> --confirm <token>` | Hard-kill a beacon machine only with the exact token from status |
| `codex-proxy upgrade` | Self-update from GitHub Releases |

Common flags:

- `--config /path/to/config.json` overrides the config file location
- `run` supports `--yolo` for per-launch YOLO mode and `--model-profile <name>`
  for per-launch model selection when the command is Codex
- `app` supports `--model-profile <name>` for desktop-app launches that should
  use a saved model profile
- `tui` / `history tui` support `--codex-dir`, `--codex-path`, `--profile`, and `--refresh-interval` (default `5s`, use `0` to disable)
- `history open` supports `--codex-dir`, `--codex-path`, and `--profile`
- `history list` / `history show` support `--codex-dir`
- `skills` supports `--codex-dir`
- `beacon` supports `--store /path/to/beacon.json` to override the beacon state file

</details>

## Model selection and YOLO mode

### YOLO mode

YOLO mode can be enabled per Codex launch:

```bash
codex-proxy run --yolo -- codex
```

If you are using the history TUI, press `Ctrl+Y` before opening or starting a
session. The status bar shows whether YOLO mode is on; press `Ctrl+Y` again to
turn it off for the next launch.

For local use, this flag and the TUI toggle are the only YOLO controls most
users need.

### Built-in model choices

Use `model` when you want to choose from the built-in model/provider presets:

```bash
codex-proxy model list
printf '%s' "$DEEPSEEK_API_KEY" | codex-proxy model setup deepseek --api-key-stdin
codex-proxy model use deepseek
codex-proxy model doctor deepseek
```

The built-in choices include `default` plus third-party choices such as
`deepseek`, `mimo`, `kimi`, `glm`, `minimax`, and `qwen`. Third-party choices
store a model profile locally and run Codex through the local Responses adapter.

Use a saved model profile for one launch without changing the default:

```bash
codex-proxy run --model-profile deepseek -- codex
codex-proxy app --model-profile deepseek
```

Teams users can pick model profiles when creating or switching Work chats; the
Teams commands are listed in the Teams helper section below.

### Custom model profiles and Responses adapter

Use `model-profile` when you need a named profile with explicit provider,
model, API-key source, or SSH proxy route:

```bash
codex-proxy model-profile setup work-deepseek \
  --provider deepseek \
  --model deepseek/deepseek-v4-pro \
  --api-key-env DEEPSEEK_API_KEY \
  --set-default

codex-proxy model-profile list
codex-proxy model-profile doctor work-deepseek
codex-proxy model-profile set-default work-deepseek
```

For lower-level experiments or integrations, `responses serve` exposes a local
`/v1/responses` facade backed by an OpenAI-compatible chat upstream:

```bash
codex-proxy responses serve \
  --base-url https://api.example.com/v1 \
  --api-key-env PROVIDER_API_KEY \
  --model provider-model
```

## Local Codex history and TUI

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
- YOLO mode: `Ctrl+Y` toggle before opening or starting a Codex session
- Skills menu: `Ctrl+K`
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

## Codex desktop app

Launch the Codex desktop app directly:

```bash
codex-proxy app
```

On macOS and Windows this installs the desktop app if needed, uses the saved
direct/proxy preference, asks on first setup, and launches the desktop app. WSL
launches the Windows desktop app. Linux outside WSL has no official Codex
desktop app, so the command exits with an unsupported-platform message there.
Use `codex-proxy app <profile>` to force a proxy profile. If proxy mode is
enabled, the command passes proxy environment variables to the desktop app and
prints a warning for WSL/AppX cases where the desktop app may not inherit or
reach that environment directly.

Use `codex-proxy app --model-profile <name>` when the desktop app should launch
with a saved model profile. If the desktop app is already running, quit it first
so the model profile configuration takes effect.

Authenticate the Codex desktop app without relying on the desktop UI to show a
device code:

```bash
codex-proxy app auth
```

This starts a temporary Codex app-server, requests ChatGPT device-code login,
opens the verification URL when possible, waits for completion, and writes auth
through Codex's own login flow into the same `CODEX_HOME` that `codex-proxy app`
uses. Use `codex-proxy app auth <profile>` to force a proxy profile. The Codex
token polling and exchange use the selected proxy; when a supported Chromium
browser is available, the verification page is opened in an isolated per-run
profile with that proxy as well. In proxy mode, `app auth` does not silently
fall back to an unmanaged default browser; if a proxy-managed browser cannot be
opened, it prints the URL and one-time code and asks you to open them manually in
a browser configured for the selected proxy. On WSL, Windows browser access to a
WSL loopback proxy is checked before auto-open because it is
environment-specific.

`app` is a root command, and `auth` is an `app` subcommand. If you have proxy
profiles literally named `app` or `auth`, use `codex-proxy tui app`,
`codex-proxy app --profile auth`, or `codex-proxy app auth --profile auth` to
remove the ambiguity.

To use a specific Codex binary:

```bash
codex-proxy tui --codex-path /path/to/codex
```

## Teams helper

Teams helper lets you drive Codex from Microsoft Teams while keeping execution
on your own machine. A local `codex-proxy teams run` listener reads Teams
messages, starts or resumes Codex sessions in the selected project directory,
and sends status, answer, artifact, and notification updates back to Teams.

Teams helper is available in stable releases. Install the normal binary first;
if this machine has an older helper, update to the latest stable release before
setup:

```bash
codex-proxy upgrade
```

From an existing Teams control chat, you can ask the helper itself to move to
the latest stable release:

```text
helper update now
```

Use `helper update prerelease` only when you intentionally want the newest
pre-release.

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
restart or update the installed helper, and reload only a source-checkout
development helper.

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

On non-WSL Linux, bootstrap uses `systemd --user` when the user manager is
available. If `systemd --user` is not usable, it falls back to a local
supervisor that detaches with `setsid`, uses a file lock to prevent duplicate
instances, and restarts the Teams runner when it exits or local owner/poll
evidence becomes stale. This fallback survives terminal close and helper
crashes, but it does not provide restart after a machine or container reboot.
Set `CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND=systemd` or
`CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND=local-supervisor` to force a backend;
leave it unset for auto selection. In auto mode, an enabled or currently active
local-supervisor install remains on local-supervisor to avoid backend flapping.
In WSL, the default backend remains the Windows Scheduled Task, but an enabled
or active local-supervisor install is also sticky unless
`CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND=windows-task` is set explicitly.

### Common Teams commands

Daily local checks:

```bash
codex-proxy teams status
codex-proxy teams doctor --live
codex-proxy teams control --print
```

Daily control-chat commands:

```text
help
projects
new /absolute/path
new <directory> --model <profile>
sessions
continue 1
status
model list
model use deepseek
```

Daily Work chat commands:

```text
helper status
helper retry last
helper cancel running
helper file relative/path.ext
model status
model switch deepseek
model fork deepseek
```

Advanced local checks and maintenance:

```bash
codex-proxy teams service doctor
codex-proxy teams service status
codex-proxy teams service bootstrap
codex-proxy teams workflow status
codex-proxy teams send-file relative/path.ext --session <session-id>
codex-proxy teams probe-chat --chat <teams-chat-id-or-link>
codex-proxy teams pause
codex-proxy teams resume
codex-proxy teams drain
codex-proxy teams recover
codex-proxy teams chat recreate <session-id> --yes
```

In the Teams control chat, `helper reload now` is for source-checkout
development reloads only. Normal installed helpers should use `helper restart
now` after a local repair or `helper update now` for a release update. Use
`helper update prerelease` only when you intentionally want the newest
pre-release.

If a quick Codex question sent in the control chat gets stuck, send
`helper cancel running` in that same control chat. Work chat requests are still
canceled inside their own Work chat.

Beacon profile setup is a local `codex-proxy beacon ...` CLI workflow. You can
ask questions about it in the Teams control chat, but profile mutation commands
should be run locally unless the helper prints a specific supported Teams
command.

Control-chat Codex fallback runs under the Teams helper service environment,
not necessarily your interactive shell. If it needs to inspect the local helper
binary, the child process receives `CODEX_HELPER_CLI_PATH`; a missing `cxp`
alias in that environment does not mean the installed helper is missing.

If you update the helper from the Teams control chat, the helper drains current
work first and sends a completion or failure notice back to Teams. A local
`codex-proxy upgrade` only installs the helper binary and matching `cxp` entry
points; it does not repair, stop, or restart the Teams service by default. After
a local helper upgrade, send `helper restart now` in the Teams control chat to
run the installed helper update.

For a deeper deployment and troubleshooting guide, see
[`docs/teams_source_deployment_guide.md`](docs/teams_source_deployment_guide.md).

## Beacon execution profiles

Beacon is an advanced Teams execution mode. You can skip this section unless
you already use Teams helper and want future Teams Work chat turns to run on a
specific local worker, Slurm allocation, or LSF allocation instead of the
helper service machine.

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
  --shared-path /shared/cxp-beacon \
  --query-command /path/to/query-slurm-allocation \
  --submit-command /path/to/submit-slurm-allocation \
  --cancel-command /path/to/cancel-slurm-allocation \
  --renew-command /path/to/renew-slurm-allocation
```

LSF and local drafts use smaller inputs:

```bash
codex-proxy beacon profile create batch --provider lsf --queue normal --shared-path /shared/cxp-beacon
codex-proxy beacon profile create local --provider local
```

If the beacon job should use an existing SSH proxy profile for network access,
add `--proxy ssh_profile --proxy-profile <existing-profile>`. Add
`--isolation shared` or `--isolation exclusive` to choose the default lease
sharing mode.

For managed Slurm/LSF profiles, `--shared-path` must point at an absolute
directory that both the control machine and allocated workers can read and
write. If `CODEX_HELPER_BEACON_STORE` is explicitly configured, cxp preserves
that store path for workers; otherwise it derives a store under `shared_path`.

Profiles stay draft until checked and confirmed. `profile doctor` validates the
profile fields, shared-path write/rename access, and the
`query`/`submit`/`cancel`/`renew` adapter commands that future Teams turns will
need:

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

To change a profile, update it in place. The helper records a new revision,
while queued or running turns keep their existing target snapshot:

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
and records a managed allocation request before Codex can start. Explicit
beacon turns do not fall back to local execution. If no accepting beacon
worker/lease is available yet, `beacon status` shows the allocation id,
allocation state, provider job id, provider state, and provider reason that
need attention.

Provider submission is adapter-based. The least surprising Teams workflow is to
store adapter commands on the beacon profile with `--query-command`,
`--submit-command`, `--cancel-command`, and `--renew-command`; those profile
changes apply to future turns without restarting the Teams helper. Managed
Slurm/LSF adapters use your default user shell environment by default, so site
setup from modules, `submit_job`, NSS, or `SUBMIT_ACCOUNT` is available without
copying those variables into the helper service. Add `--adapter-shell direct`
when `$SHELL -lic` is incompatible (for example tcsh/csh) or an adapter needs
the clean helper service environment. To start from a site-editable Slurm or
LSF wrapper, print a template:

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

Profile-stored adapter commands are usually easier because they apply to future
turns without a service change. If you change helper service environment
variables instead, restart the installed helper from the Teams control chat with
`helper restart now`.

When no adapter shell mode is set, managed Slurm/LSF adapters behave as
`--adapter-shell user`. cxp uses `$SHELL -lic` only to capture a framed
environment snapshot, then runs the adapter directly with that environment so
shell startup output cannot pollute the adapter protocol. Use
`--adapter-shell shell-command` only for rare sites where scheduler submission
is a shell function or alias that cannot be captured as environment. The
adapter receives flags such as `--request-id`, `--name`, `--profile`,
`--provider`, `--partition`, `--image`, `--queue`, and
`--operation query|submit|cancel|renew`. It should print JSON like
`{"provider_job_id":"123","raw_state":"PD","reason":"Resources","provider_deadline":"2026-05-18T10:30:00Z"}`
or key-value output such as
`provider_job_id=123 raw_state=PD reason=Resources provider_deadline=1779090600`.
The generated Slurm/LSF templates include `query`, `submit`, `cancel`, and a
site-policy `renew` stub that exits non-zero until edited; implement the renew
case when the scheduler exposes walltime extension.

Beacon adapter troubleshooting:

- `exit 127` from the scheduler job often means the submitted command was
  malformed or PATH is different inside the allocation. Keep exactly one `exec`
  in the final worker command; if a site wrapper already prepends `exec`, remove
  the extra one from the adapter.
- `allocation request ... not found` inside the worker usually means the worker
  is using the wrong state file. Custom Slurm/LSF submit adapters must accept
  `--shared-store` and pass it to the worker as
  `cxp beacon --store <shared-store> worker ...`.
- If `$SHELL -lic` fails under tcsh/csh, update the profile with
  `--adapter-shell direct`; profile revisions apply to future turns.
- Beacon workers launch Codex in yolo mode by default so scheduler/container
  devices and mounts stay visible; pass worker `--no-yolo` only when the worker
  must keep Codex sandboxing.
- If worker doctor reports `missing codex`, set scheduler PATH or pass worker
  `--codex-path <codex-or-wrapper>` from the adapter. A wrapper is still useful
  for path resolution or extra Codex exec flags such as
  `--skip-git-repo-check`; Teams service `--codex-arg` settings do not
  automatically reach remote beacon workers. The generated templates honor
  `CXP_BEACON_CODEX_BIN` by passing it as worker `--codex-path`.

The active Teams helper owner periodically queries existing provider jobs,
projects scheduler state back into beacon machines/jobs, and renews allocations
whose provider deadline is near. Provider calls are never made while holding
the beacon state lock, and renewal results are applied only when the provider
job id and renew epoch still match. During helper drain, the renewal controller
only protects allocations whose job may already have started; it does not
create new scheduler work for pre-start turns. Release/cancel intents are
durable, so the active owner retries them during reconcile if an earlier path
recorded the intent before the provider adapter became available. The same
reconcile pass drains idle workers with no chat/job demand so shared and
exclusive workers do not stay accepting forever after demand disappears.

Inside an allocated worker, the scheduler job should register itself against
the managed allocation and wait for the Teams turn to enqueue work:

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

`serve` records worker heartbeats, runs the worker doctor before accepting
jobs, stores bootstrap diagnostics such as node list, stdout/stderr paths,
shared beacon store path, and `codex`/`cxp` paths, then drains the machine on
exit. A controller or cron job can reconcile stale state with:

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

This is optional maintenance. Normal first-time users do not need to run these
commands.

The installer best-effort installs a bundled `cxp` Codex skill into
`$HOME/.agents/skills/cxp`. Managed skills previously installed under
`~/.codex/skills` are migrated to the agents skills directory when the skills
commands run. The skill contains the local command map and the safe handoff
rules for disruptive operations such as beacon profile switching and Teams
helper restarts. It is managed as a built-in skill, not as a git-backed
subscription, so it does not appear as a remote source in `codex-proxy skills
list`.

To repair or install it manually:

```bash
codex-proxy skills install-builtin
codex-proxy skills migrate --yes
```

To inspect a migration without changing files, run:

```bash
codex-proxy skills migrate --dry-run
```

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
- `--restart-teams-service` (opt in to the legacy drain/stop/refresh/restart
  path; on WSL this may request Windows permission if service repair is needed)

By default, `codex-proxy upgrade` downloads, validates, installs, and unifies
known helper entry points without touching the Teams service. If older installs
left `cxp` or `codex-proxy` in multiple known locations, the upgrade keeps the
currently invoked helper as the canonical target when it is runnable, ignores
broken stale install-path hints, and repairs known managed/env/default entries
to point at the canonical helper. This keeps a network timeout or Windows
service repair problem from interrupting a running helper. Use `helper restart
now` in the Teams control chat to run the installed helper update.

Teams background mode also checks `codex-helper` GitHub Releases every 30
minutes and silently applies eligible helper updates after current Teams/Codex
work drains. Background helper auto-update uses the running stable helper path
when it is a runnable helper binary, then repairs known managed/env/default
entry points to the installed helper before restarting the bridge:

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

Clear saved proxy setup, attempt to stop known proxy daemons, and trigger the first-run proxy prompt again:

```bash
codex-proxy proxy reset
```

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

### Windows (PowerShell)

```powershell
$ErrorActionPreference="Stop"; $u="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1"; $p=Join-Path $env:TEMP "codex-proxy-install.ps1"; try { Invoke-WebRequest -UseBasicParsing $u -OutFile $p; Unblock-File -LiteralPath $p; & powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File $p; if ($LASTEXITCODE -ne 0) { throw "installer exited with code $LASTEXITCODE" } } finally { Remove-Item -Force $p -ErrorAction SilentlyContinue }
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
$ErrorActionPreference="Stop"; $u="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1"; $p=Join-Path $env:TEMP "codex-proxy-install.ps1"; try { Invoke-WebRequest -UseBasicParsing $u -OutFile $p; Unblock-File -LiteralPath $p; & powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File $p -Version vX.Y.Z; if ($LASTEXITCODE -ne 0) { throw "installer exited with code $LASTEXITCODE" } } finally { Remove-Item -Force $p -ErrorAction SilentlyContinue }
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
