# Teams Helper Source Deployment Guide

This guide installs the Teams helper from source on a machine and brings it to
the normal operating model: one Teams control chat per machine, one Teams work
chat per Codex session, background service supervision, and file and voice
message support.

The guide intentionally does not include tenant IDs, client IDs, access tokens,
refresh tokens, or webhook URLs. Keep those values in local config or a secret
store only.

## 1. What You Need

- A machine where you can run user-level commands. Root/admin is not required for
  the helper service path.
- Git.
- Go 1.25 or newer. The repository pins Go 1.25.11 so local source builds use
  the same toolchain as CI and the bundled pure-Go SQLite runtime.
- Network access to GitHub, Microsoft login, Microsoft Graph, and the Codex API.
- The Microsoft Entra tenant ID and the public client ID of a Teams Graph app
  approved for your environment. The app must allow the delegated scopes for
  reading and sending chat messages, creating meeting chats, and uploading
  files.
- A Teams account that is allowed to use those delegated Graph scopes.
- A working Codex login on that machine, or enough access for `codex-helper` to
  install and use Codex the same way it does for normal local usage.

Do not copy token cache files from another machine. Sign in on each machine
separately.

## 2. Build From Source

Clone and build. The public HTTPS URL works without GitHub SSH keys; use the
`git@github.com:...` form only when SSH auth is already configured.

```sh
mkdir -p "$HOME/project"
cd "$HOME/project"
git clone https://github.com/baaaaaaaka/codex-helper.git
cd codex-helper
git checkout <branch-or-commit>
go test ./internal/core/... ./internal/channel/... ./internal/cli
go build -o "$HOME/.local/bin/codex-proxy" ./cmd/codex-proxy
```

These source-build commands are written for Linux, macOS, or WSL. On Windows,
prefer the release installer from the README, or adapt the source-build path to
PowerShell and build `codex-proxy.exe`.

Make sure the binary is on `PATH`:

```sh
mkdir -p "$HOME/.local/bin"
case ":$PATH:" in
  *":$HOME/.local/bin:"*) ;;
  *) echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.profile" ;;
esac
export PATH="$HOME/.local/bin:$PATH"
codex-proxy --help
```

Use a stable binary path for service mode. Do not install the service from a
temporary `go run` binary.

## 3. Configure And Sign In

The interactive setup script asks for the tenant ID and write client ID. You
can also pass a separate read client ID with `--read-client-id` (or
`-ReadClientId` on PowerShell). It signs in to both clients only when their IDs
differ, verifies the caches, and bootstraps the background service:

Linux, macOS, or WSL:

```sh
scripts/teams-auth-bootstrap.sh
```

Windows PowerShell:

```powershell
.\scripts\teams-auth-bootstrap.ps1
```

The same scripts are attached to GitHub releases for source-free setup.

To do the same steps by hand:

```sh
codex-proxy teams auth config \
  --tenant-id "<tenant-id>" \
  --read-client-id "<read-client-id>" \
  --client-id "<teams-graph-public-client-id>"
codex-proxy teams auth read
codex-proxy teams auth full
codex-proxy teams auth read-status
codex-proxy teams auth full-status
```

`teams auth read` and `teams auth full` print Microsoft device login URLs and
codes. Open each URL, enter the code, and complete SSO and MFA. The read token
handles idempotent Graph reads. The full token handles Graph writes and all
operations that need write scopes. If both client IDs match, configure that ID
for both and run only `codex-proxy teams auth full`. `--read-scopes` and
`--scopes` override the respective delegated scopes when your tenant needs a
different set.

The helper creates standalone online meetings only as chat carriers; it does
not add calendar entries for normal use.

Token caches are local secrets. They are stored under the user cache directory
with private file permissions. Do not copy them to another machine and do not
commit them. `codex-proxy teams auth logout` removes the full cache;
`codex-proxy teams auth read-logout` removes the separate read cache.

## 4. Verify

```sh
codex-proxy teams doctor --live
codex-proxy teams status
```

Expected result:

- `doctor --live` signs in to Graph and reads the account (it sends nothing).
- `status` shows the helper store and, once the helper has started, the
  control chat, owner, and sessions.

If your environment requires an HTTP proxy, configure `codex-helper` proxy
settings as you normally would. Teams Graph calls respect the helper proxy
configuration; if proxy is disabled, Teams Graph calls use direct networking.

## 5. Start The Helper

```sh
codex-proxy teams run
```

Where a background service is supported, `teams run` installs (or repairs) and
starts it, so the helper keeps running after the terminal closes. On first
start the helper creates this machine's control chat, titled like
`🏠 Codex Control - <machine>`, and mentions you in it.

Print the control chat link at any time without sending anything:

```sh
codex-proxy teams control
```

If the chat does not appear on a mobile client, open the printed link once.
Some Teams clients hide standalone meeting chats until they are opened or
mentioned.

The control chat is single-user from the helper's perspective. Only use this
mode with your own Teams account.

### Upgrading from an earlier helper

A helper store written by v0.1.20 or v0.1.22-rc.59 is converted automatically
at the first start of this version. The old store is never modified or
deleted; a marker file next to it records the conversion. Sessions keep their
work chats, and a request that was running during the upgrade continues in the
same Codex thread with one interruption notice. To preview a conversion
without starting the helper:

```sh
codex-proxy teams migrate --dry-run --out /tmp/core-preview.sqlite
```

## 6. Background Service

Check which no-root backend the helper will use:

```sh
codex-proxy teams service doctor
```

Typical backends:

- Linux: `systemd --user`, or `local-supervisor` when no user systemd manager is
  usable; an enabled or active local-supervisor install stays sticky in auto mode
- WSL: per-user Windows Scheduled Task that launches `wsl.exe`, unless an
  enabled or active local-supervisor install is sticky or Windows Task mode is
  explicitly forced
- macOS: LaunchAgent
- Windows: per-user Scheduled Task

`teams run` and `teams service bootstrap` install, enable, and start the
service. The individual steps are also available:

```sh
codex-proxy teams service install
codex-proxy teams service enable
codex-proxy teams service start
codex-proxy teams service status
```

Platform notes:

- Linux with `systemd --user`: the service survives terminal close while the
  user session exists. A real logout may stop `systemd --user` unless the
  environment enables linger or keeps a user session alive.
- Linux without a usable user systemd manager: `service bootstrap` falls back to
  `local-supervisor`. It detaches with `setsid`, uses a config-root lock/status
  file to prevent duplicate supervisors, and restarts the Teams helper after
  crashes, but it cannot guarantee restart after a machine or container reboot.
  Auto mode keeps an enabled or active local-supervisor install on that backend
  to avoid backend flapping.
- WSL: the helper installs a Windows Scheduled Task for the current Windows user
  and target distro/user. This is the preferred path for surviving terminal close
  and WSL shell exit. Explicit WSL `systemd` mode fails as systemd when no user
  manager is usable; explicit WSL `local-supervisor` mode is available for
  no-Windows-task debugging, but it has the same no-reboot-autostart limit as
  non-WSL local-supervisor. If local-supervisor is already enabled or active,
  WSL auto mode keeps it sticky unless `CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND`
  explicitly requests `windows-task`.
- macOS: LaunchAgent starts when the user logs in. Sleep/wake restarts the
  process if needed.
- Windows: Scheduled Task runs as the current user. Enterprise policy can block
  scheduled tasks; `service doctor` and `service status` show the failure.

The service restarts the helper after a crash or an update (exit code 75 asks
for a restart on the same state). Requests that were running are continued in
the same Codex thread after the restart, with one interruption notice each.

## 7. Using It From Teams

Control chat (send `help` for the list):

```text
new /absolute/path/to/workspace
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
update prerelease
codex update now
```

`new <directory>` creates a `💬` work chat for a new Codex session. Inside a
work chat every message that does not start with `helper` goes to Codex,
including attached files and voice messages (voice is transcribed locally).

Work chat commands (send `helper help` for the list):

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
helper skills
helper restart
```

Expected behaviour:

- A prompt gets a short ACK; Codex progress appears while it works; the final
  answer is posted once.
- The owner is mentioned in the control chat when a final answer arrives.
  Finals that arrive within a few seconds of each other share one mention.
- Several prompts sent while Codex is busy are queued and run one after
  another; the queue is announced once when it starts.

`model` lists the models Codex offers for the signed-in account;
`helper model <model> [effort]` switches one session. Third-party model
profiles are no longer supported.

## 8. Files And Images

Each Codex turn is given its own output directory in its prompt. To return
files, Codex writes them there and lists them in its final answer:

````text
```codex-helper-artifacts
{"version":1,"files":[{"path":"relative/path.ext","name":"display-name.ext"}]}
```
````

The helper accepts only relative paths under that turn's directory, uploads
each file, and posts it as an attachment under the given name after the final
answer. Files and images sent to a work chat are downloaded and handed to
Codex with the prompt.

## 9. Keep Multiple Machines Separate

Each machine gets its own `🏠 Codex Control - <machine>` chat and creates its own
work chats. The helper store on each machine holds that machine's identity and
chat bindings.

- Only one helper process owns a store at a time; a second `teams run` on the
  same store waits instead of polling the same chats.
- Multiple OS users on the same server use separate user config/cache paths and
  do not interfere with each other.
- Signed-in machines publish a small capability card to a hidden registry chat
  so a Codex turn on one machine can delegate work to another with
  `cxp delegate`.

## 10. Upgrade From Source

On the target machine:

```sh
cd "$HOME/project/codex-helper"
git fetch origin
git switch <branch>
git pull --ff-only
go test ./internal/core/... ./internal/channel/... ./internal/cli
go build -o "$HOME/.local/bin/codex-proxy.new" ./cmd/codex-proxy
mv "$HOME/.local/bin/codex-proxy.new" "$HOME/.local/bin/codex-proxy"
codex-proxy teams service restart
codex-proxy teams status
```

For a pinned commit instead of a branch, use `git checkout <commit>` instead of
`git switch` and `git pull`, then run the same test, build and restart steps.

Installed release helpers update themselves from the control chat with
`update now` (or `update prerelease`); `restart` restarts the helper after a
local binary repair.

## 11. Troubleshooting

No control chat in Teams:

```sh
codex-proxy teams control
codex-proxy teams doctor --live
```

If `teams control` reports no control chat yet, the helper has not started on
this store; run `codex-proxy teams run`. Otherwise open the printed link once.

Messages are slow or a request seems stuck:

```sh
codex-proxy teams status
```

In the work chat, `helper status` shows the running and queued requests;
`helper cancel` cancels the running one. Under Graph throttling the helper
paces its reads and writes and catches up on its own.

Auth expired:

```sh
codex-proxy teams auth
```

Service not running:

```sh
codex-proxy teams service doctor
codex-proxy teams service status
codex-proxy teams service restart
```

Inspect local state without sending Teams messages:

```sh
codex-proxy teams status
codex-proxy teams control
codex-proxy teams doctor
```
