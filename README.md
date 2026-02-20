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

The installer drops a `cxp` shim alongside `codex-proxy` and tries to add the
install directory to PATH (plus a `cxp` alias). Open a new shell if the
command is not found.

### 2) **Run**

```bash
codex-proxy
# or
cxp
```

On first run you'll be asked whether to use the SSH proxy. Choose **no** for
direct connections. Choose **yes** to enter SSH host/port/user and let the
tool create a dedicated key if needed. You can toggle proxy mode later with
`Ctrl+P` in the TUI.

### 3) Next steps

- Press Enter to open a Codex session.
- If there is no history yet, Enter starts a new session in the current directory.
- If you have multiple profiles, select one with `codex-proxy <profile>`.
- Run any command through the proxy (requires a profile):
  `codex-proxy run <profile> -- <cmd> [args...]`.
- Example: `codex-proxy run pdx -- curl https://example.com`.

### Optional: preconfigure a proxy profile

```bash
codex-proxy init
```

Config is stored under your OS user config directory (Linux typically
`~/.config/codex-proxy/config.json`).

## Requirements (runtime)

- `ssh` (OpenSSH client) is required
- `ssh-keygen` is optional (only needed when proxy mode creates a dedicated key)

Check your environment:

```bash
codex-proxy proxy doctor
```

## Commands

| Command | Description |
|---------|-------------|
| `codex-proxy` | Open the TUI (default) |
| `codex-proxy init` | Create an SSH profile |
| `codex-proxy run [profile] -- <cmd> [args...]` | Run a command through the proxy |
| `codex-proxy tui` | Browse Codex history in a terminal UI |
| `codex-proxy history list [--pretty]` | List discovered projects/sessions as JSON |
| `codex-proxy history show <session-id>` | Print full history for a session |
| `codex-proxy history open <session-id>` | Open a session in Codex |
| `codex-proxy proxy start <profile>` | Start a long-lived proxy daemon |
| `codex-proxy proxy list` | List known proxy instances |
| `codex-proxy proxy stop <instance-id>` | Stop a proxy instance |
| `codex-proxy proxy prune` | Remove dead/unhealthy instances |
| `codex-proxy proxy doctor` | Check required tools and configuration |
| `codex-proxy upgrade` | Self-update from GitHub Releases |

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
codex-proxy history --codex-dir /path/to/.codex tui
```

Controls:

- Navigation: Up/Down, PageUp/PageDown (also `j`/`k`)
- Switch pane: Tab / Left / Right (also `h`/`l`)
- Search: `/` then type, Enter apply, Esc cancel (`n`/`N` next/prev in preview)
- Open: Enter (opens in Codex and sets cwd)
- New session: `(New Agent)` entry or `Ctrl+N` (in selected project or current dir)
- Expand/collapse subagents: `Ctrl+O`
- Proxy mode: `Ctrl+P` toggle (status shows `Proxy mode (Ctrl+P): on/off`)
- YOLO mode: `Ctrl+Y` toggle (`--permission-mode bypassPermissions`, status shows warning)
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

If `codex` is not in PATH, `codex-proxy` will automatically install
`@openai/codex` in a user-local location (and bootstrap a private Node.js
runtime when the system Node.js is missing or too old).

To use a specific Codex binary:

```bash
codex-proxy history --codex-path /path/to/codex tui
```

## Upgrade

Upgrade from GitHub Releases:

```bash
codex-proxy upgrade
```

Optional flags:

- `--repo owner/name` (override GitHub repo)
- `--version vX.Y.Z` (install a specific version)
- `--install-path /path/to/codex-proxy` (override install path)

## Long-lived instances (optional)

Start a reusable daemon instance:

```bash
codex-proxy proxy start <profile>
codex-proxy proxy list
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
`~/.local/bin` to PATH (plus a `cxp` alias). Open a new shell if the command is
not found.
If you need to update PATH manually:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

Install a specific version (example):

```bash
curl -fsSL https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.sh | sh -s -- --version v0.0.5
```

### Windows (PowerShell one-liner)

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command "iwr -useb https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1 | iex"
```

Install a specific version:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command "$u='https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1'; $p=Join-Path $env:TEMP 'codex-proxy-install.ps1'; iwr -useb $u -OutFile $p; & $p -Version v0.0.5; Remove-Item -Force $p"
```

### Environment variables

| Variable | Description |
|----------|-------------|
| `CODEX_PROXY_REPO` | Override GitHub repo (default: `baaaaaaaka/codex-helper`) |
| `CODEX_PROXY_VERSION` | Override version (default: `latest`) |
| `CODEX_PROXY_INSTALL_DIR` | Override install directory (default: `~/.local/bin`) |
| `CODEX_PROXY_API_BASE` | Override GitHub API base URL |
| `CODEX_PROXY_RELEASE_BASE` | Override GitHub release base URL |
