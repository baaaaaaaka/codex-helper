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

### 3) Next steps

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
| `codex-proxy proxy start [profile]` | Start a long-lived proxy daemon |
| `codex-proxy proxy list` | List known proxy instances |
| `codex-proxy proxy stop <instance-id>` | Stop a proxy instance |
| `codex-proxy proxy prune` | Remove dead/unhealthy instances |
| `codex-proxy proxy doctor` | Report environment issues and installation hints |
| `codex-proxy upgrade` | Self-update from GitHub Releases |

Common flags:

- `--config /path/to/config.json` overrides the config file location
- `tui` / `history tui` support `--codex-dir`, `--codex-path`, `--profile`, and `--refresh-interval` (default `5s`, use `0` to disable)
- `history open` supports `--codex-dir`, `--codex-path`, and `--profile`
- `history list` / `history show` support `--codex-dir`

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

## Upgrade

Upgrade from GitHub Releases:

```bash
codex-proxy upgrade
```

Optional flags:

- `--repo owner/name` (override GitHub repo)
- `--version vX.Y.Z` (install a specific version)
- `--install-path /path/to/codex-proxy` (override install path; file or directory)

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
