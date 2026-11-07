# codex-proxy

Languages: English | [中文](README.zh-CN.md)

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

The installer puts a stable `cxp` executable alongside the legacy
`codex-proxy` compatibility entry and adds the install directory plus the
managed CLI directory to PATH. Exact legacy aliases that redirected `cxp` back
to `codex-proxy` are removed. Existing user-managed directory and multi-hop
file symlinks are preserved while their final payload is updated. Open a new
shell if the command is not found.

Released builds run through an immutable, versioned `cxp` runtime. Updates
publish the new runtime before atomically switching the active version, so an
already-running session keeps its old executable and new CXP-owned long-lived
processes use `cxp` paths and process names.

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
- **Daily options**: run commands through the selected proxy, open the Codex
  desktop app, or drive Codex from Teams.
- **Advanced operations**: skills maintenance, the Teams background service,
  workflow webhooks, and source-checkout deployment.

The rest of this README is grouped with that split in mind: common commands
come first, then optional desktop/Teams flows, then advanced execution
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
| `codex-proxy run -- codex` | Launch the original Codex TUI through CXP's standard approval broker |
| `codex-proxy run --aaa -- codex` | Enable agent-auto-approve for this Codex run |
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
| `codex-proxy --upgrade-codex-app [profile]` | Update the CXP-managed Windows/WSL or current-user macOS desktop app |
| `codex-proxy --upgrade-codex` | Install or upgrade the CXP-managed Codex CLI |
| `codex-proxy --upgrade-codex --upgrade-codex-path <absolute-path>` | Upgrade one explicitly selected external Codex installation |
| `codex-proxy completion <shell>` | Generate shell completion |
| `codex-proxy init` | Create an SSH profile |
| `codex-proxy run [profile] -- <cmd> [args...]` | Run a command using the current mode, or force proxy when a profile is given (`codex` by default) |
| `codex-proxy run -- codex` | Launch the original Codex TUI through CXP's standard approval broker |
| `codex-proxy run --aaa -- codex` | Enable agent-auto-approve for this Codex run |
| `codex-proxy tui` | Browse Codex history in a terminal UI |
| `codex-proxy history tui` | Browse Codex history in a terminal UI |
| `codex-proxy history list [--pretty]` | List discovered projects/sessions as JSON |
| `codex-proxy history show <session-id>` | Print full history for a session |
| `codex-proxy history open <session-id>` | Open a session in Codex |
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
| `codex-proxy proxy prune` | Remove dead records; preserve live owners, startup leases and recovery blocks |
| `codex-proxy proxy reset` | Clear saved proxy setup and attempt to stop known daemons so the next launch asks again |
| `codex-proxy proxy doctor` | Report environment issues and installation hints |
| `codex-proxy teams status` | Show Teams helper state, control chat, service, owner, and queue status |
| `codex-proxy teams doctor` | Check local Teams helper auth and service readiness |
| `codex-proxy teams path status` | Resolve the account PATH used by Teams Codex; add `--show-path` to print it |
| `codex-proxy teams path mode <mode>` | Select the `account-default` or `explicit` PATH mode (`teams path explicit <PATH>` saves the PATH) |
| `codex-proxy teams workflow status` | Show optional Teams Workflow notification configuration |
| `codex-proxy teams workflow enable --webhook-url-file <path>` | Enable Workflow cards using a private local webhook URL file |
| `codex-proxy teams service bootstrap` | Install or repair the background Teams helper service |
| `codex-proxy teams service restart` | Restart the Teams helper service; interrupted requests continue after the restart |
| `codex-proxy teams control` | Print this machine's Teams control chat link |
| `codex-proxy teams auth config --tenant-id <id> --client-id <id>` | Save the Teams Graph tenant and client ID |
| `codex-proxy teams auth` / `auth status` / `auth logout` | Sign in with a device code, show the sign-in state, or remove the token cache |
| `codex-proxy teams run` | Start the Teams helper (installs and starts the background service where supported) |
| `codex-proxy teams migrate` | Convert a stopped v0.1.20 or v0.1.22-rc.59 helper store explicitly (the helper does it at first start) |
| `codex-proxy delegate resolve --query <text> --json` | Resolve cross-machine delegation candidates for Codex or diagnostics |
| `codex-proxy delegate start --candidate-token <token> --task-file <path> --json` | Create an idempotent delegation request for a selected machine |
| `codex-proxy delegate status` / `wait` / `cancel` | Inspect, wait for, or cancel a delegation using the route saved by `start` |
| `codex-proxy upgrade` | Self-update from GitHub Releases |

Common flags:

- `--config /path/to/config.json` overrides the config file location
- `tui` / `history tui` support `--codex-dir`, `--codex-path`, `--profile`, and `--refresh-interval` (default `5s`, use `0` to disable)
- `history open` supports `--codex-dir`, `--codex-path`, and `--profile`
- `history list` / `history show` support `--codex-dir`
- `skills` supports `--codex-dir`

</details>

## Standard approvals

### Standard approval runtime

CXP launches the original Codex binary with ordinary on-request approvals.
Local interactive launches default to manual review: the broker forwards each
approval request byte-for-byte to the official Codex TUI. Press `Ctrl+A` in the
CXP history TUI to persistently toggle Agent Auto Approve (AAA), or enable it for
one command without changing the saved preference:

```bash
codex-proxy run --aaa -- codex
codex-proxy run --aaa -- codex exec "..."
```

AAA answers supported approval requests after a fixed 500 ms delay. Without
AAA, non-interactive `codex exec` facade requests fail closed because no human
reviewer is attached. Teams is an unattended surface and therefore enables
the same automatic handler explicitly; they do not inherit the local TUI
preference. The `--aaa` flag is consumed by CXP and is never passed to Codex.

This runtime requires Codex CLI 0.131.0 or newer; older managed/PATH installs
are upgraded automatically before the first brokered turn. The release compatibility
sweep verifies the app-server handshake, the remote TUI capability, and the
production broker's root WebSocket plus bearer-token attachment contract.

The Codex inner sandbox remains restricted until an approved operation needs
the execution target's assigned hardware or mounts. Approval cannot grant
devices that the outer host, container, cgroup, Slurm job, or LSF job did not
already provide. Telemetry remains enabled and is forwarded without payload
mutation.

The brokered surfaces are the Codex TUI/history launch paths, the `codex exec`
facade, and Teams turns. Manual and AAA modes use the same
original binary, on-request policy, Responses gateway, and proxy routing; only
the reviewer changes. Contract CI uses the original Codex
binary with analytics enabled and requires the emitted review to remain an
ordinary `reviewer=user`, `status=approved`, `user_approved` event. CXP does not
hide its app-server client identity or try to prevent server-side inference from
timing and other normal telemetry.

CXP's local policy gateway is intentionally limited to Responses traffic. It
does not rewrite `chatgpt_base_url`: workspace configuration, MCP discovery,
analytics, and other ChatGPT control-plane requests keep the official HTTPS
origin and use the selected profile through standard `HTTP_PROXY` / `HTTPS_PROXY`
environment variables. This preserves the official client's secure cookie and
Cloudflare behavior while keeping the existing SSH/direct proxy choice intact.

Normal CI and the Codex release monitor assert this routing boundary without
external account credentials. Runtime-contract jobs install and launch the
original Codex binary, but provide a fresh synthetic `CODEX_HOME`, synthetic
ChatGPT auth metadata, and a loopback-only scripted Responses service. The
contract fails if per-turn model or effort is lost, a model switch changes the
thread, portable context disappears, or Codex attempts to rely on a real model
account.
Repository CI must not store or inject OpenAI, NVIDIA, Teams, proxy, or other
external-account tokens.

`codex-proxy app` still launches the official Desktop App directly. The Desktop
App does not currently expose a stable external app-server attachment contract,
so Desktop automatic approval remains a final-release blocker for any claim
that every CXP surface is brokered; CXP does not silently fall back to a retired
execution mode for that surface.

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

Session JSONL files remain authoritative. CXP keeps only rebuildable catalog and
preview acceleration under the selected Codex data root at
`CODEX_DIR/.codex-proxy/codexhistory/v3/<user-identity>/`. There is no separate
cache-directory setting: when a cluster places `CODEX_DIR` on larger or shared
storage, the cache follows it automatically. Both SQLite databases use rollback
journals instead of WAL, update only changed rows, and fall back to source JSONL
when the cache directory is unavailable. The v3 namespace rebuilds the
disposable history index, metadata, and preview caches from JSONL after the fork
classification change. Set
`CODEX_HELPER_PREVIEW_CACHE_BACKEND=off` only for diagnosis; the former `json`
rollback value now uses SQLite and cannot re-enable whole-file JSON rewrites.

Controls:

- Navigation: Up/Down
- Preview scroll: PageUp/PageDown, Home/End
- Switch pane: Tab / Left / Right (also `h`/`l`)
- Search: `/` then type, Enter apply, Esc cancel (`n`/`N` next/prev in preview)
- Open: Enter (opens in Codex and sets cwd)
- New session: `(New Agent)` entry or `Ctrl+N` (in selected project or current dir)
- Expand/collapse subagents: `Ctrl+O`
- Proxy mode: `Ctrl+P` toggle (status shows `Proxy mode (Ctrl+P): on/off`)
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
direct/proxy preference, asks on first setup, and launches the desktop app. On
Windows, proxy launches use a cxp-managed unpacked copy of the
official signed x64 ChatGPT MSIX under `%LOCALAPPDATA%\cxp\apps\chatgpt`,
so `CODEX_HOME`, proxy environment variables, and Chromium's
`--proxy-server` argument reach the process without changing the system proxy.
Ordinary direct Windows launches keep the existing Store/AppX path. WSL
launches the Windows desktop app. Linux outside WSL has no official Codex
desktop app, so the command exits with an unsupported-platform message there.
OpenAI now exposes Codex from the unified ChatGPT desktop app. Current macOS
packages use `ChatGPT.app` with a `ChatGPT` executable, while older packages use
`Codex.app` with a `Codex` executable; `codex-proxy app` supports both and
prefers the current ChatGPT entry when both are installed. Windows keeps the
Codex Store product/package identity for update compatibility, while direct
launch accepts either `ChatGPT.exe` or the legacy `Codex.exe`. The ordinary
Windows Store path keeps the Codex product/package identity for update
compatibility. The managed
Windows copy is deliberately not registered as an AppX package and does not
take over ChatGPT's updater; a later `cxp app` run reuses the verified cache.

To manually refresh that CXP-managed Windows copy from the current official
signed x64 ChatGPT MSIX, run:

```bash
codex-proxy --upgrade-codex-app
```

On Windows and WSL, this command optionally accepts a
proxy profile (`codex-proxy --upgrade-codex-app <profile>`), installs the
package if the managed copy is missing, and switches new `cxp app` launches to
the newly verified runtime. It stages the new runtime beside the old one, so a
currently running app is not overwritten; quit and relaunch the app to use the
new version. It does not register the unpacked copy as an AppX package or
change the Microsoft Store installation.

On macOS, `--upgrade-codex-app` uses only a verified OpenAI app in the current
user's `~/Applications` (or installs one there); it never updates `/Applications`.
It preserves that app's path, stages and verifies the new bundle before
replacement, refuses to update while any ChatGPT or Codex process is running,
and restores a verifiable previous bundle after an interrupted replacement.
To avoid macOS forwarding a same-bundle launch to another copy, `cxp app` also
refuses to launch while ChatGPT or Codex is already running. Quit every
ChatGPT/Codex instance, including system-installed copies, and do not reopen one
until the update finishes. CXP serializes its own app launches and updates, but
cannot lock Finder launches or OpenAI's updater; the updater's background file
activity is not verified, so CXP is not guaranteed to be the only updater.
On macOS the
launcher also requires bundle identifier `com.openai.codex`, so a separately
installed classic `ChatGPT.app` is never mistaken for Codex or overwritten; a
name conflict is installed safely as `Codex.app` instead.
Use `codex-proxy app <profile>` to force a proxy profile. If the managed
Windows runtime cannot be downloaded, verified, or started before creating a
process, `cxp` makes one legacy direct-launch attempt; it never falls back to
AppX activation when launch-time environment would be lost. Set
`CXP_WINDOWS_APP_BACKEND=managed-only` or `legacy` only for diagnostics and CI.

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
on your own machine. A local `codex-proxy teams run` helper reads Teams
messages, runs Codex sessions in the selected project directory, and sends
progress, answers, files, and notifications back to Teams.

Teams helper is available in stable releases. Install the normal binary first;
if this machine has an older helper, update to the latest stable release before
setup:

```bash
codex-proxy upgrade
```

From an existing Teams control chat, you can ask the helper itself to move to
the latest stable release:

```text
update now
```

Use `update prerelease` only when you intentionally want the newest
pre-release.

### Fast setup

The recommended setup path is the interactive bootstrap script from the quick
start section. It performs the complete local setup flow:

1. Configure the Teams Graph tenant and client ID.
2. Run Microsoft device login.
3. Verify the local Teams auth cache.
4. Install or repair the background Teams helper service and start it.

The script stores auth metadata and tokens locally under the current user. It
does not require a project checkout and does not hard-code Teams auth values;
it asks for the required values interactively unless you provide them with
environment variables or script flags. One delegated sign-in covers reading and
sending chat messages, meeting chats, and file uploads.

The same steps by hand:

```bash
codex-proxy teams auth config --tenant-id <tenant-id> --client-id <client-id>
codex-proxy teams auth
codex-proxy teams run
```

### How it works

The helper creates a **control chat** for machine-level commands and one
**work chat** per Codex session. Use the control chat to start sessions, list,
park and wake them, check status, update the local Codex CLI, and restart or
update the helper.

A work chat message is acknowledged and sent to its Codex thread. If a task is
running, regular messages add instructions to that task immediately (steer).
Use `helper queue <prompt>` to run a separate task after the current one.
Queued tasks run in order. Progress appears while Codex works, and each task's
final answer is posted once. The owner receives a control-chat mention or a
configured Teams Workflow card when an answer arrives.

Teams text, quoted messages, files, pasted images, and supported inline media
are passed through to Codex. Teams voice or video clips are transcribed locally
before the turn starts, and Codex receives the transcript with a note that it
came from automatic speech recognition. Files that Codex lists in a
`codex-helper-artifacts` manifest are uploaded back to the work chat.

If you also use Codex from a local terminal or IDE on a session that has a work
chat, the helper posts those prompts and answers to the work chat too, so Teams
stays the shared conversation surface.

When the local Teams helper is running, `cxp tui` creates and resumes local
sessions through the helper's native app-server. Both interfaces can add input and see progress
in the same thread. Closing the TUI leaves its task running. Helper updates wait
for attached TUI windows to close and native tasks to finish. Independent Codex
windows already open during an update keep their existing backend; close and
reopen them through `cxp tui` to join the shared backend.

The helper keeps its state in one local SQLite store. A store from v0.1.20 or
v0.1.22-rc.59 is converted automatically at the first start; the old store is
kept unchanged. A request that was running when the helper stopped (a crash, an
update, a restart) continues in the same Codex thread with one interruption
notice. Under Microsoft Graph throttling the helper paces its reads and writes
and catches up on its own.

When the same Teams user runs helpers on multiple machines, Codex can use
cross-machine delegation for tasks that need another machine's repo, hardware,
or local context. The user-facing trigger is natural language in a work chat,
for example "ask the other machine to inspect this repo" or "have the GPU box
check this failure"; Codex decides whether to use the `cxp delegate` workflow
from its installed skill. Each active helper publishes a compact machine card
and heartbeat to a hidden per-user Teams registry chat, then receives work
through its own hidden machine inbox chat. The registry is only for discovery
and liveness; delegation requests, claims, progress, questions, and results stay
in the target machine inbox. This currently converges only for machines signed
in as the same Teams user, and hidden Teams chats remain subject to tenant
retention, eDiscovery, and audit policy.

The background service keeps the helper alive after terminal close, SSH
disconnect, WSL session exit, sleep/wake, or helper upgrade. Service bootstrap
chooses the native per-user service mechanism for the platform where possible
and repairs old helper service definitions when it can do so safely.

Codex app-servers launched for Teams use the target account's default PATH
instead of copying the background service PATH. On Unix and WSL, cxp resolves
the account shell from the account database, starts it under the same UID,
GID, groups, and HOME as Codex, and receives only PATH through a framed private
Unix socket with a per-probe nonce. On Windows, cxp reads the environment block
for the current Scheduled Task user token. Managed Node remains private to
Codex installation and wrapper discovery; when the packaged native Codex binary
is available, it is launched directly so `node`, `npm`, and other tools still
resolve from the user PATH.

`account-default` intentionally does not reproduce terminal-only activation
such as venv, direnv, or a project-specific shell. Use `teams path explicit
<PATH>` for a maintained explicit value. Unknown account shells fail with a
diagnostic instead of silently being treated as bash. `teams path status`
reports identity, shell, source, entry count, and a fingerprint without
printing the complete PATH unless `--show-path` is supplied. Use `teams path
shell default` to clear a previous shell override and return to the account
database value. Saved `captured-terminal` and `service` modes of earlier
releases resolve as `explicit` and `account-default`.

On non-WSL Linux, bootstrap uses `systemd --user` when the user manager is
available. If `systemd --user` is not usable, it falls back to a local
supervisor that detaches with `setsid`, uses a file lock to prevent duplicate
instances, and restarts the Teams helper when it exits. This fallback survives
terminal close and helper crashes, but it does not provide restart after a
machine or container reboot.
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
codex-proxy teams control
```

Control chat commands (send `help` for the list):

```text
new /absolute/path
list
r s003
park s003
fork s003
model
stats
skills
status
restart
update now
codex update now
```

Work chat commands (send `helper help` for the list; any other message goes to
Codex):

```text
helper status
helper cancel
helper cancel all
helper retry
helper retry last
helper unpause
helper park
helper fork
helper model
helper model <model> [effort]
helper effort <effort>
helper stats
helper restart
```

`model` lists the models Codex offers for the signed-in account, and
`helper model <model> [effort]` switches one session. Third-party model
profiles and catalogs are no longer supported; configs that still contain them
load normally and keep those settings untouched.

Advanced local checks and maintenance:

```bash
codex-proxy teams service doctor
codex-proxy teams service status
codex-proxy teams service bootstrap
codex-proxy teams service restart
codex-proxy teams workflow status
codex-proxy teams migrate --dry-run --out /tmp/core-preview.sqlite
```

Send `codex update now` in the control chat to update the CXP-managed Codex CLI.
The helper waits for active Codex work first and posts the result back to the
control chat. This command does not update cxp/codex-helper and does not retry
earlier failed requests; `helper retry last` in a work chat does.

Codex turns launched by the helper receive the absolute
`CODEX_HELPER_CLI_PATH`; cxp is not added to the user PATH, so a missing `cxp`
alias does not mean the installed helper is missing.

If you update the helper from the Teams control chat, the helper sends a
completion or failure notice back to Teams. A local `codex-proxy upgrade` only
installs the helper binary and matching `cxp` entry points; it does not restart
the Teams service. After a local helper upgrade, send `restart` in the control
chat (or run `codex-proxy teams service restart`).

For a deeper deployment and troubleshooting guide, see
[`docs/teams_source_deployment_guide.md`](docs/teams_source_deployment_guide.md).

## Git-backed skill subscriptions

Use git-backed skill subscriptions when you want your own Codex skills to live
in a repo and stay synchronized across machines. A subscription installs the
skills from that git source into `$HOME/.agents/skills`; later `sync` updates
the local managed copy, and `push` lets you publish confirmed local edits back
to the source.

```bash
codex-proxy skills add <git-url>
codex-proxy skills list
codex-proxy skills sync
codex-proxy skills doctor
codex-proxy skills push <name>
codex-proxy skills remove <name>
```

This is the recommended path for personal or team-maintained skills. The
built-in `cxp` skill below is maintained by the helper itself and is separate
from these git-backed subscriptions.

## Built-in cxp skill

This is optional maintenance. Normal first-time users do not need to run these
commands.

The installer best-effort installs a bundled `cxp` Codex skill into
`$HOME/.agents/skills/cxp`. Managed skills previously installed under
`~/.codex/skills` are migrated to the agents skills directory when the skills
commands run. The skill contains the local command map and the safe handoff
rules for disruptive operations such as Teams helper restarts. It is managed as
a built-in skill, not as a git-backed subscription, so it does not appear as a
remote source in `codex-proxy skills list`.

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
service repair problem from interrupting a running helper. Send `restart`
in the Teams control chat to run the installed helper update.

Teams background mode also checks `codex-helper` GitHub Releases every 30
minutes and silently applies eligible helper updates after current Teams/Codex
work drains. Background helper auto-update uses the running stable helper path
when it is a runnable helper binary, then repairs known managed/env/default
entry points to the installed helper before restarting the helper:

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

This targets the same CXP-managed Codex installation that normal CXP launches
use. To upgrade a separate system or user-global Codex installation, select its
executable explicitly:

```bash
codex-proxy --upgrade-codex --upgrade-codex-path /absolute/path/to/codex
```

Behavior:

- Uses current proxy preference: proxy on -> upgrade through proxy; proxy off -> direct.
- Installs the CXP-managed Codex CLI when it is missing and upgrades it when present.
- Ignores unrelated `codex` executables earlier in `PATH` for the default managed operation.
- `--upgrade-codex-path` requires an absolute executable path and keeps that
  installation's recognized npm source. The command may repair a present but
  nonfunctional executable; it verifies the replacement after npm completes.
- Explicit external upgrades fail fast when the source cannot be determined.

Manually refresh the CXP-managed Codex desktop app copy on native Windows or
WSL:

```bash
codex-proxy --upgrade-codex-app
```

The optional positional argument selects a configured proxy profile. The
command downloads and verifies the official signed x64 ChatGPT MSIX, installs
it beside the current managed runtime, and atomically selects it for future
`codex-proxy app` launches. An already-running desktop process continues on
its old runtime until it is quit and relaunched; the Microsoft Store/AppX
installation is not modified.

### Audit or clean legacy patched binaries (POSIX)

A source checkout includes a fail-closed cleanup tool for old patched copies
left by earlier `codex-helper` or `claude-helper` releases. Audit is the default
and does not modify files:

```bash
python3 scripts/cleanup_patched_binaries.py --audit
```

After reviewing the report, remove only artifacts with proven helper
provenance on a system with readable Linux-style procfs:

```bash
python3 scripts/cleanup_patched_binaries.py --purge --json cleanup-report.json
```

Audit works on POSIX systems; purge deliberately requires procfs so an active
executable cannot be mistaken for an inactive artifact. The cleaner is not run
automatically. It stays inside validated current-user
home/XDG roots and current-user-owned helper namespaces under `/tmp`; refuses
symlinks, hard links, foreign ownership, changed inodes, nested mounts, and
ambiguous histories; and restores an in-place Claude patch instead of deleting
the executable. A confirmed in-place patch without a hash-verified original is
reported as `repair_required`. Running processes are preserved unless
`--terminate-active` is explicitly combined with `--purge`.

## Long-lived instances (optional)

Start a reusable daemon instance:

```bash
codex-proxy proxy start [profile]
codex-proxy proxy list
```

`proxy list` reads saved instances and app-gateway registrations for profiles in
the selected config without changing state. It checks local listener health,
shows failure details, and does not probe external destinations. `LAST_SEEN` is
the saved heartbeat/update timestamp, not the time of this check. Historical dead
records remain visible. `proxy prune` preserves live processes even when their
backends are recovering, as well as startup leases and durable recovery blocks.
App gateway rows are informational; `proxy stop` and `proxy prune` operate on
saved instances, not app gateway registrations.

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
