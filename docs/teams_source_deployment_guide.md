# Teams Helper Source Deployment Guide

This guide installs the Teams helper from source on a second machine and brings
it to the same operating model as the original machine: a Teams control chat, one
Teams work chat per Codex session, background service supervision, local Codex
history sync, and optional file upload support.

The guide intentionally does not include tenant IDs, client IDs, access tokens,
refresh tokens, webhook URLs, or API keys. Keep those values in local config or a
secret store only.

## 1. What You Need

- A machine where you can run user-level commands. Root/admin is not required for
  the helper service path.
- Git.
- Go 1.22 or newer.
- Network access to GitHub, Microsoft login, Microsoft Graph, and the Codex API.
- The Microsoft Teams Graph tenant/client IDs approved for your environment:
  - tenant ID
  - read client ID
  - chat/write client ID
  - optional file-write client ID
- A Teams account that is allowed to use those delegated Graph scopes.
- A working Codex login on that machine, or enough access for `codex-helper` to
  install/use Codex in the same way it does for normal local usage.

Do not copy token cache files from another machine. Run Microsoft login on each
machine separately.

## 2. Build From Source

Clone and build:

```sh
mkdir -p "$HOME/project"
cd "$HOME/project"
git clone git@github.com:baaaaaaaka/codex-helper.git
cd codex-helper
git checkout <branch-or-commit>
go test ./internal/teams ./internal/cli
go build -o "$HOME/.local/bin/codex-proxy" ./cmd/codex-proxy
```

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

## 3. Configure Teams Graph App IDs

Configure the local Teams auth metadata in one command:

```sh
codex-proxy teams auth config \
  --tenant-id "<tenant-id>" \
  --read-client-id "<read-client-id>" \
  --chat-client-id "<chat-write-client-id>" \
  --file-write-client-id "<file-write-client-id>"
```

If file upload is not needed yet, omit `--file-write-client-id`; the helper will
fall back to the chat/write client for file-write auth when possible.

To keep scopes explicit, you may also configure them in the same command:

```sh
codex-proxy teams auth config \
  --tenant-id "<tenant-id>" \
  --read-client-id "<read-client-id>" \
  --chat-client-id "<chat-write-client-id>" \
  --read-scopes "openid profile offline_access User.Read Chat.Read" \
  --chat-scopes "openid profile offline_access User.Read Chat.ReadWrite OnlineMeetings.ReadWrite"
```

The default chat scopes are designed for meeting-based Teams chats. The helper
creates standalone online meetings only as a chat carrier; it does not rely on
calendar pollution for normal use.

## 4. Authenticate Teams

Run read auth first. This token is used for lower-latency message polling:

```sh
codex-proxy teams auth read
```

Open the device login URL shown by the command, enter the code, and complete SSO
and MFA.

Run chat/write auth next. This token is used to create meeting chats and send
messages:

```sh
codex-proxy teams auth
```

Optional file upload support:

```sh
codex-proxy teams auth file-write
```

Token caches are local secrets. They are stored under the user cache directory
with private file permissions. Do not copy them to another machine and do not
commit them.

## 5. Verify Auth And Connectivity

Run local checks:

```sh
codex-proxy teams auth read-status
codex-proxy teams auth status
codex-proxy teams doctor --live
codex-proxy teams status
```

Expected result:

- auth status says the token cache is present and not expired
- doctor can call Graph `/me`
- status shows the state path and no active error

If your environment requires an HTTP proxy, configure `codex-helper` proxy
settings as you normally would. Teams Graph calls respect the helper proxy
configuration; if proxy is disabled, Teams Graph calls use direct networking.

## 6. Create The Control Chat

Create or show the machine control chat:

```sh
codex-proxy teams control
```

Expected Teams result:

- A meeting-based chat appears with a title like `🏠 Codex Control - <machine>`.
- The helper sends an owner mention and a ready/help message.
- The control chat is single-user from the helper's perspective. Only use this
  mode with your own Teams account.

If the chat does not appear automatically on a mobile client, open the printed
Teams URL once. Some Teams clients hide standalone meeting chats until they are
opened or mentioned.

Print the current local binding without creating or sending anything:

```sh
codex-proxy teams control --print
```

Recreate the control chat during development:

```sh
codex-proxy teams control --recreate --yes
```

The old control chat is not deleted. The helper sends a link to the new control
chat before rebinding local state.

## 7. Run Foreground For A First Test

Start the bridge in a foreground terminal:

```sh
codex-proxy teams run
```

In the Teams control chat, try:

```text
help
p
```

To create a new work chat for a directory:

```text
n /absolute/path/to/workspace
```

Inside the new `💬 Codex Work ...` chat, send a normal task, for example:

```text
Summarize this repository and list the most important test commands.
```

Expected behavior:

- The work chat sends a short ACK.
- Codex status messages appear as `🤖 ⏳ Codex status`.
- The final reply appears as `🤖 ✅ Codex answer`.
- The helper mentions the owner after a new final answer so Teams can notify.

Stop foreground mode with `Ctrl-C`. Foreground mode stops when the terminal or
SSH session closes, so use service mode for daily use.

## 8. Install The Background Service

Check which no-root backend the helper will use:

```sh
codex-proxy teams service doctor
```

Typical backends:

- Linux: `systemd --user`
- WSL: per-user Windows Scheduled Task that launches `wsl.exe`
- macOS: LaunchAgent
- Windows: per-user Scheduled Task

Install, enable, and start:

```sh
codex-proxy teams service install
codex-proxy teams service enable
codex-proxy teams service start
codex-proxy teams service status
codex-proxy teams status
```

The install step writes only user-level service configuration. It does not start
automatically until you run `enable` and `start`.

Platform notes:

- Linux without user lingering: the service should survive terminal close while
  the user session exists. A real logout may stop `systemd --user` unless the
  environment enables linger or keeps a user session alive.
- WSL: the helper installs a Windows Scheduled Task for the current Windows user
  and target distro/user. This is the preferred path for surviving terminal close
  and WSL shell exit.
- macOS: LaunchAgent starts when the user logs in. Sleep/wake should restart the
  process if needed.
- Windows: Scheduled Task runs as the current user. Enterprise policy can block
  scheduled tasks; `service doctor` and `service status` show the failure.

## 9. Daily Teams Commands

Control chat:

```text
help
p
projects
n /absolute/path
new /absolute/path
s
sessions
c 1
continue 1
st
status
```

Work chat:

```text
helper help
helper status
helper retry last
helper close
helper file relative/path.ext
```

Normal work chat messages are sent to Codex. Helper commands start with
`helper` or the supported short command form shown by `helper help`.

## 10. Files And Images

Codex can ask the helper to upload generated files by writing them under:

```text
~/.cache/codex-helper/teams-outbound
```

A final answer can include:

````text
```codex-helper-artifacts
{"version":1,"files":[{"path":"relative/path.ext","name":"display-name.ext"}]}
```
````

The helper only accepts relative manifest paths under the outbound root. File
upload requires successful `codex-proxy teams auth file-write`.

To send an existing local file manually from a work chat, put it under the
outbound root and send:

```text
helper file relative/path.ext
```

## 11. Keep Multiple Machines Separate

Each machine gets its own `🏠 Codex Control - <machine>` chat and creates its own
work chats. Durable local state includes the machine identity, scope, and Teams
chat bindings.

If multiple machines share storage:

- Long-lived machines should be treated as primary.
- Temporary machines can start Teams mode, but the lease/avoidance mechanism
  keeps only one active owner per helper scope.
- Multiple OS users on the same server use separate user config/cache paths and
  should not interfere with each other.

## 12. Upgrade From Source

On the target machine:

```sh
cd "$HOME/project/codex-helper"
git fetch origin
git checkout <branch-or-commit>
git pull --ff-only
go test ./internal/teams ./internal/cli
go build -o "$HOME/.local/bin/codex-proxy.new" ./cmd/codex-proxy
mv "$HOME/.local/bin/codex-proxy.new" "$HOME/.local/bin/codex-proxy"
codex-proxy teams service restart
codex-proxy teams status
```

When the helper is already running from a stable source checkout and stable
binary path, you can also ask from the Teams control chat:

```text
helper reload now
```

The Teams reload path runs its safety tests, builds a replacement binary,
swaps it into the current install path, and restarts the helper. Use
`helper reload force` only for development when you have checked that no Codex
work is active.

If service restart reports a stale owner:

```sh
codex-proxy teams status
codex-proxy teams recover --stale-after 2m
codex-proxy teams service start
```

Use `recover --force` only when you are sure the old helper process is gone.

## 13. Troubleshooting

No control chat in Teams:

```sh
codex-proxy teams control --print
codex-proxy teams doctor --live
```

Open the printed URL once. Some mobile clients do not show standalone meeting
chats until opened or mentioned.

Messages are slow:

```sh
codex-proxy teams status
```

Check poll summary, blocked chats, and Graph window warnings. Hot chats should
poll faster after a user message or final answer. Parked chats do not poll until
resumed from the control chat.

Queued or interrupted turn:

```text
helper status
helper retry last
```

Check recent messages and local file changes before retrying. Retry can repeat
file edits or terminal commands.

Auth expired:

```sh
codex-proxy teams auth read
codex-proxy teams auth
codex-proxy teams auth file-write
```

Service not running:

```sh
codex-proxy teams service doctor
codex-proxy teams service status
codex-proxy teams service restart
```

Need a clean development chat:

```sh
codex-proxy teams control --recreate --yes
```

Need to inspect local state without sending Teams messages:

```sh
codex-proxy teams status
codex-proxy teams control --print
codex-proxy teams doctor
```
