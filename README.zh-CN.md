# codex-proxy

Languages: [English](README.md) | 中文

通过基于 SSH 的本地代理栈运行 `codex`（或任何命令）：

- **上游**：`ssh -D 127.0.0.1:<port>` SOCKS5 隧道
- **下游**：本地 **HTTP CONNECT** 代理（Go），通过 SOCKS5 拨号
- **运行监督**：如果代理变得不健康且无法修复，目标进程会被终止，避免直接连接外网

本项目设计为按 OS/arch 发布的 **单二进制文件**。

## 快速开始

### 1) **安装**

Linux / macOS:

```bash
sh -c 'url="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.sh"; if command -v curl >/dev/null 2>&1; then curl -fsSL "$url" | sh; elif command -v wget >/dev/null 2>&1; then wget -qO- "$url" | sh; else echo "need curl or wget" >&2; exit 1; fi'
```

Windows (PowerShell):

```powershell
$ErrorActionPreference="Stop"; $u="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1"; $p=Join-Path $env:TEMP "codex-proxy-install.ps1"; try { Invoke-WebRequest -UseBasicParsing $u -OutFile $p; Unblock-File -LiteralPath $p; & powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File $p; if ($LASTEXITCODE -ne 0) { throw "installer exited with code $LASTEXITCODE" } } finally { Remove-Item -Force $p -ErrorAction SilentlyContinue }
```

Windows 命令会先把安装器下载到临时文件再执行。它避免把远程脚本文本
pipe 进 PowerShell，同时使用仅作用于当前进程的 `RemoteSigned` 策略，
让默认脚本限制仍能运行本地临时文件。Group Policy 脚本限制仍然可能阻止执行。

安装器会在旧的 `codex-proxy` 兼容入口旁放置稳定的 `cxp` executable，并尝试
把安装目录和托管 CLI 目录加入 PATH。旧版中把 `cxp` 重定向回
`codex-proxy` 的精确 shell alias 会被移除；用户自己创建的目录软链接和多跳
文件软链接会保持原样，只更新最终 payload。如果找不到命令，请打开一个新
shell。

正式构建会通过不可变、按版本保存的 `cxp` runtime 运行。更新先发布新 runtime，
再原子切换 active version；已经运行的 session 继续使用旧 executable，新的
CXP 常驻进程则使用 `cxp` 的路径和进程名。

### 2) **运行**

```bash
codex-proxy
# or
cxp
```

首次运行时，如果还没有保存代理偏好或 profile，程序会询问是否使用 SSH
代理。选择 **no** 表示直接连接；选择 **yes** 会继续输入 SSH host/port/user，
并在需要时让工具创建专用密钥。之后可以在 TUI 里用 `Ctrl+P` 切换代理模式。
如果要丢弃已保存的代理设置、尝试停止已知代理 daemon，并再次看到这个提示，
运行 `codex-proxy proxy reset`。

### 3) 可选：初始化 Teams helper

Teams helper 已在稳定版发布中可用。全新安装会使用当前稳定版 helper。如果这台
机器上有较旧的 `codex-proxy`，请先更新到最新稳定版：

```bash
codex-proxy upgrade
```

只有在明确想测试最新 pre-release 时，才使用
`codex-proxy upgrade --include-prerelease`。

然后运行交互式 Teams 设置脚本。它会询问所需的 Teams auth metadata，启动
Microsoft device login，验证本地 auth cache，并 bootstrap 后台 helper service。

Linux / macOS / WSL:

```bash
sh -c 'set -e; url="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/scripts/teams-auth-bootstrap.sh"; tmp="${TMPDIR:-/tmp}/teams-auth-bootstrap.sh"; if command -v curl >/dev/null 2>&1; then curl -fsSL "$url" -o "$tmp"; elif command -v wget >/dev/null 2>&1; then wget -qO "$tmp" "$url"; else echo "need curl or wget" >&2; exit 1; fi; bash "$tmp"; rm -f "$tmp"'
```

Windows (PowerShell):

```powershell
$ErrorActionPreference="Stop"; $u="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/scripts/teams-auth-bootstrap.ps1"; $p=Join-Path $env:TEMP "teams-auth-bootstrap.ps1"; try { Invoke-WebRequest -UseBasicParsing $u -OutFile $p; Unblock-File -LiteralPath $p; & powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File $p; if ($LASTEXITCODE -ne 0) { throw "teams auth bootstrap exited with code $LASTEXITCODE" } } finally { Remove-Item -Force $p -ErrorAction SilentlyContinue }
```

设置完成后，打开 bootstrap 显示的 Teams control chat，并发送 `help`。

### 4) 下一步

- 按 Enter 打开选中的 Codex session。
- 如果还没有历史，Enter 会在当前目录启动一个新 session。
- 如果有多个 profile，用 `codex-proxy <profile>` 选择。
- 使用当前直接/代理模式运行任意命令：
  `codex-proxy run -- <cmd> [args...]`。
- 对某个命令强制使用代理模式：
  `codex-proxy run [profile] -- <cmd> [args...]`。
- 如果 `--` 后没有给命令，`run` 会启动 `codex`。
- 如果还没有代理 profile，`run` 会引导你创建一个。
- 示例：`codex-proxy run pdx -- curl https://example.com`。

### 使用层级

大多数用户只需要上面的快速开始和下面的常用命令：

- **基础本地使用**：安装，运行 `cxp`，选择直接或 SSH 代理模式，并从历史 TUI
  打开 session。
- **日常选项**：通过选中的代理运行命令，打开 Codex 桌面 App，
  或从 Teams 驱动 Codex。
- **高级操作**：skills 维护、Teams service 恢复、workflow webhook，以及
  source-checkout 部署。

本 README 的其余部分按这个分层组织：先是常用命令，再是可选的
desktop/Teams 流程，然后是高级执行和维护参考。按受众和深度组织的完整
文档地图见 [`docs/README.md`](docs/README.md)。

### 可选：预配置代理 profile

```bash
codex-proxy init
```

配置保存在 OS 用户配置目录下（Linux 通常是
`~/.config/codex-proxy/config.json`）。

## 要求

- 代理模式需要 `ssh`（OpenSSH client）
- `ssh-keygen` 是可选的（只有在代理模式创建专用密钥时需要）
- 直接模式不需要 SSH 工具
- 如果 `codex` 缺失或不可用，`codex-proxy` 可以在用户本地位置安装
  `@openai/codex`，并在需要时 bootstrap 一个托管 Node.js runtime

检查环境（`proxy doctor` 仅提供信息；即使 `codex-proxy` 后续可以安装托管副本，
它也可能报告缺少 `node`、`npm` 或 `codex`）：

```bash
codex-proxy proxy doctor
```

## 常用命令

这些是普通安装最常用的命令：

| 命令 | 说明 |
|------|------|
| `codex-proxy` 或 `cxp` | 打开本地 Codex 历史 TUI |
| `codex-proxy run -- <cmd> [args...]` | 使用当前直接/代理模式运行命令 |
| `codex-proxy run -- codex` | 通过 CXP 标准审批 broker 启动原版 Codex TUI |
| `codex-proxy run --aaa -- codex` | 为本次 Codex 运行开启 Agent Auto Approve |
| `codex-proxy proxy doctor` | 检查本地 proxy/Codex 前置条件 |
| `codex-proxy proxy reset` | 清除已保存的代理设置，下次启动时重新询问 |
| `codex-proxy app` | 在 macOS、Windows 或 WSL 上启动 Codex 桌面 App |
| `codex-proxy teams status` | 设置后检查 Teams helper 状态 |
| `codex-proxy upgrade` | 从 GitHub Releases 更新 `codex-proxy` / `cxp` |

## 命令参考

<details>
<summary>完整命令参考（高级）</summary>

这个表同时包含日常命令以及高级/维护命令。如果你还在首次设置，可以先跳过它；
下面的引导章节会按顺序说明正常流程。

| 命令 | 说明 |
|------|------|
| `codex-proxy [profile]` | 打开 TUI（默认） |
| `codex-proxy app [profile]` | 需要时安装、使用或配置代理模式，并在 macOS、Windows 或 WSL 上启动 Codex 桌面 App |
| `codex-proxy app auth [profile]` | 使用相同的 `CODEX_HOME` 和代理设置完成 Codex 桌面 App 的 ChatGPT auth |
| `codex-proxy --upgrade-codex-app [profile]` | 更新 Windows/WSL 的 CXP 托管桌面 App 或 macOS 当前用户的桌面 App |
| `codex-proxy --upgrade-codex` | 使用检测到的安装来源重新安装 Codex CLI |
| `codex-proxy completion <shell>` | 生成 shell completion |
| `codex-proxy init` | 创建 SSH profile |
| `codex-proxy run [profile] -- <cmd> [args...]` | 使用当前模式运行命令；给出 profile 时强制使用代理（默认命令是 `codex`） |
| `codex-proxy run -- codex` | 通过 CXP 标准审批 broker 启动原版 Codex TUI |
| `codex-proxy run --aaa -- codex` | 为本次 Codex 运行开启 Agent Auto Approve |
| `codex-proxy tui` | 在终端 UI 中浏览 Codex 历史 |
| `codex-proxy history tui` | 在终端 UI 中浏览 Codex 历史 |
| `codex-proxy history list [--pretty]` | 以 JSON 列出发现的 projects/sessions |
| `codex-proxy history show <session-id>` | 打印某个 session 的完整历史 |
| `codex-proxy history open <session-id>` | 在 Codex 中打开某个 session |
| `codex-proxy skills install-builtin` | 在 `$HOME/.agents/skills` 中安装或修复 bundled skills，包括内置 `cxp` 使用 skill |
| `codex-proxy skills add <git-url>` | 从 git source 安装 skills 并保持更新 |
| `codex-proxy skills migrate` | 把托管的 legacy skills 从 `~/.codex/skills` 迁移到 `$HOME/.agents/skills` |
| `codex-proxy skills list` | 列出 Codex skill subscriptions |
| `codex-proxy skills sync [name]` | 同步一个 skill source；不指定 name 时同步全部 |
| `codex-proxy skills push [name]` | 逐项确认后推送本地 skill edits |
| `codex-proxy skills doctor` | 检查本地 skill subscription 状态 |
| `codex-proxy skills remove <name>` | 移除 skill subscription 和对应的托管已安装 skills |
| `codex-proxy proxy start [profile]` | 启动长期运行的 proxy daemon |
| `codex-proxy proxy list` | 列出已知 proxy instances |
| `codex-proxy proxy stop <instance-id>` | 停止一个 proxy instance |
| `codex-proxy proxy prune` | 清理失效记录，保留活进程、启动租约和恢复熔断记录 |
| `codex-proxy proxy reset` | 清除已保存的代理设置，并尝试停止已知 daemons，让下次启动重新询问 |
| `codex-proxy proxy doctor` | 报告环境问题和安装提示 |
| `codex-proxy teams status` | 显示 Teams helper state、control chat、service、owner 和 queue 状态 |
| `codex-proxy teams doctor` | 检查本地 Teams helper auth 和 service readiness |
| `codex-proxy teams path status` | 解析 Teams Codex 使用的账号 PATH；加 `--show-path` 才打印完整值 |
| `codex-proxy teams path mode <mode>` | 选择 `account-default` 或 `explicit` PATH 模式（`teams path explicit <PATH>` 保存 PATH） |
| `codex-proxy teams workflow status` | 显示可选 Teams Workflow notification 配置 |
| `codex-proxy teams workflow enable --webhook-url-file <path>` | 用私有本地 webhook URL 文件启用 Workflow cards |
| `codex-proxy teams service bootstrap` | 安装或修复后台 Teams helper service |
| `codex-proxy teams service restart` | 重启 Teams helper service；被中断的请求会在重启后继续 |
| `codex-proxy teams control` | 打印本机的 Teams control chat link |
| `codex-proxy teams auth config --tenant-id <id> --client-id <id>` | 保存 Teams Graph tenant 和 client ID |
| `codex-proxy teams auth` / `auth status` / `auth logout` | 用 device code 登录、显示登录状态，或删除 token cache |
| `codex-proxy teams run` | 启动 Teams helper（在支持的平台上安装并启动后台 service） |
| `codex-proxy teams migrate` | 显式转换已停止的 v0.1.20 或 v0.1.22-rc.59 helper store（helper 首次启动时会自动转换） |
| `codex-proxy delegate resolve --query <text> --json` | 为 Codex 或诊断解析跨机器 delegation 候选机器 |
| `codex-proxy delegate start --candidate-token <token> --task-file <path> --json` | 为选中的机器创建 idempotent delegation request |
| `codex-proxy delegate status` / `wait` / `cancel` | 使用 `start` 保存的 route 检查、等待或取消 delegation |
| `codex-proxy upgrade` | 从 GitHub Releases self-update |

常用 flags:

- `--config /path/to/config.json` 覆盖 config file 路径
- `tui` / `history tui` 支持 `--codex-dir`、`--codex-path`、`--profile` 和 `--refresh-interval`（默认 `5s`，用 `0` 禁用）
- `history open` 支持 `--codex-dir`、`--codex-path` 和 `--profile`
- `history list` / `history show` 支持 `--codex-dir`
- `skills` 支持 `--codex-dir`

</details>

## 标准审批

### 标准审批 runtime

CXP 使用普通的 on-request 审批启动原版 Codex binary。本地交互式入口默认由用户审批：
broker 会把 approval 请求原样交给官方 Codex TUI。可以在 CXP 历史 TUI 中按 `Ctrl+A`
持久切换 Agent Auto Approve（AAA），也可以只为一次命令开启而不修改保存的偏好：

```bash
codex-proxy run --aaa -- codex
codex-proxy run --aaa -- codex exec "..."
```

AAA 收到支持的 approval 请求后固定等待 500 ms 再批准。未开启 AAA 时，非交互式
`codex exec` facade 因没有人工 reviewer 而 fail-closed。Teams 是无人值守入口，
因此会在代码中显式启用相同的自动批准 handler，并且不会继承本地 TUI 偏好。
`--aaa` 只由 CXP 消费，不会传给 Codex。

这套 runtime 要求 Codex CLI 0.131.0 或更高版本；较旧的 managed/PATH 安装会在
第一次 broker turn 前自动升级。release compatibility sweep
会同时验证 app-server handshake、remote TUI 能力，以及生产 broker 的根 WebSocket
地址和 bearer-token attachment contract。

Codex 内层 sandbox 在操作获批前仍保持受限。获批操作只能继承外层 host、container、
cgroup、Slurm job 或 LSF job 已经授予的硬件和 mounts，不能突破外层隔离边界。Telemetry
保持开启，并且 payload 不经过 CXP 修改。

已接入 broker 的入口包括 Codex TUI/history、`codex exec` facade 和 Teams turn。手动模式和 AAA 使用相同的原版 binary、on-request policy、Responses gateway 与
proxy 路由，唯一差别是 reviewer。Contract CI 使用开启 analytics 的原版 Codex binary，并要求审批 telemetry 仍表现为
普通的 `reviewer=user`、`status=approved`、`user_approved` 事件。CXP 不隐藏自己的
app-server client identity，也不承诺服务端无法根据时序或其他正常 telemetry 推断自动化。

普通 CI 和 Codex release monitor 不使用外部账户凭据。Runtime contract job 会安装并
启动原版 Codex binary，但只使用全新的合成 `CODEX_HOME`、合成 ChatGPT auth metadata，
以及 runner 内仅 loopback 的脚本化 Responses service。如果 per-turn model 或 effort
丢失、切换模型改变了 thread、可移植上下文消失，或 Codex 试图依赖真实模型账户，
contract 就会失败。
仓库 CI 禁止存储或注入 OpenAI、NVIDIA、Teams、proxy 等外部 token。

`codex-proxy app` 仍然直接启动官方 Desktop App。Desktop App 目前没有稳定的外部
app-server attachment contract，因此，在声称“所有 CXP surface 都已接入 broker”之前，
Desktop 自动审批仍是 final release blocker；CXP 不会为这个入口静默回退到已经退役的执行机制。

## 本地 Codex 历史和 TUI

在交互式终端 UI 中浏览 Codex 历史：

```bash
codex-proxy tui
# or
codex-proxy history tui
```

这会打开 TUI。按 Enter 会用当前代理模式（直接或 SSH 代理）在 Codex 中打开选中
session。用 `Ctrl+P` 切换代理模式；如果代理已启用但尚未配置，会提示输入 SSH
host/port/user。如果还没有历史，Enter 会在当前目录启动新 session。

Projects 列始终包含当前工作目录，并标为 `[current]`。Sessions 列始终包含
`(New Agent)` 条目。

如果有多个代理 profiles:

```bash
codex-proxy tui --profile <profile>
```

默认数据目录是 `~/.codex`。可以这样覆盖：

```bash
codex-proxy tui --codex-dir /path/to/.codex
```

快捷键：

- Navigation: Up/Down
- Preview scroll: PageUp/PageDown, Home/End
- Switch pane: Tab / Left / Right（也支持 `h`/`l`）
- Search: `/` 后输入，Enter 应用，Esc 取消（preview 中 `n`/`N` 下一个/上一个）
- Open: Enter（在 Codex 中打开并设置 cwd）
- New session: `(New Agent)` 条目或 `Ctrl+N`（在选中 project 或当前目录）
- Expand/collapse subagents: `Ctrl+O`
- Proxy mode: `Ctrl+P` toggle（状态显示 `Proxy mode (Ctrl+P): on/off`）
- Skills menu: `Ctrl+K`
- Refresh: `r`（或 `Ctrl+R`）
- Quit: `q`、`Esc`、`Ctrl+C`
- In-app update: `Ctrl+U`（有更新时）

如果 update check 失败，状态栏会显示错误。

以 JSON 列出 sessions:

```bash
codex-proxy history list --pretty
```

按 id 打印完整 session:

```bash
codex-proxy history show <session-id>
```

直接在 Codex 中打开 session:

```bash
codex-proxy history open <session-id>
```

这会使用当前代理模式（直接或 SSH 代理）。如果代理模式已启用但没有 profile，
会提示配置 SSH。

如果 `codex` 缺失或不可用，`codex-proxy` 会自动在用户本地位置安装
`@openai/codex`（并在系统 Node.js 缺失或过旧时 bootstrap 一个私有 Node.js runtime）。

## Codex 桌面 App

直接启动 Codex 桌面 App:

```bash
codex-proxy app
```

在 macOS 和 Windows 上，这会在需要时安装桌面 App，使用保存的直接/代理偏好，
首次设置时询问，并启动桌面 App。WSL 会启动 Windows 桌面 App。WSL 之外的
Linux 没有官方 Codex 桌面 App，所以该命令会显示 unsupported-platform message
后退出。OpenAI 现在把 Codex 放在统一的 ChatGPT 桌面 App 中。当前 macOS 包使用
`ChatGPT.app` 和 `ChatGPT` executable，旧包使用 `Codex.app` 和 `Codex`
executable；`codex-proxy app` 同时支持两者，并在两者都存在时优先使用新的
ChatGPT 入口。Windows 为了兼容更新仍保留 Codex Store product/package identity，
direct launch 则同时接受 `ChatGPT.exe` 和旧的 `Codex.exe`。macOS 启动时还会
强制校验 bundle identifier 为 `com.openai.codex`，因此不会把单独安装的经典
`ChatGPT.app` 误当成 Codex，也不会覆盖它；遇到同名冲突时会安全安装为
`Codex.app`。用
`codex-proxy app <profile>` 强制使用某个代理 profile。如果代理模式
已启用，该命令会把代理环境变量传给桌面 App，并对 WSL/AppX 这种桌面 App 可能
无法继承或访问该环境的情况打印 warning。

不依赖桌面 UI 显示 device code，完成 Codex 桌面 App auth:

```bash
codex-proxy app auth
```

这会启动一个临时 Codex app-server，请求 ChatGPT device-code login，在可能时打开
verification URL，等待完成，并通过 Codex 自己的 login flow 把 auth 写入
`codex-proxy app` 使用的同一个 `CODEX_HOME`。用
`codex-proxy app auth <profile>` 强制使用某个代理 profile。Codex token polling
和 exchange 会使用选中的代理；当有受支持的 Chromium browser 时，verification
page 会在该代理下的隔离 per-run profile 中打开。在代理模式下，`app auth` 不会
静默 fallback 到未托管的默认浏览器；如果无法打开 proxy-managed browser，它会
打印 URL 和 one-time code，并要求你在为所选代理配置的浏览器中手动打开。在 WSL
上，自动打开前会检查 Windows browser 是否能访问 WSL loopback proxy，因为这取决
于环境。

在 Windows 上，代理模式启动会使用官方签名 x64 ChatGPT MSIX 的
CXP-managed 解包副本，默认位于 `%LOCALAPPDATA%\cxp\apps\chatgpt`。这个副本不注册为
AppX，也不会接管 ChatGPT 自带的 updater；如果需要手动刷新它，使用：

```bash
codex-proxy --upgrade-codex-app
```

该命令只支持原生 Windows 和 WSL，可选地接受 proxy profile
（`codex-proxy --upgrade-codex-app <profile>`），在副本缺失时安装，并把通过校验的
新 runtime 原子切换为后续 `codex-proxy app` 使用的版本。它会把新 runtime 放在旧版本旁边，
不会覆盖正在运行的 App；退出并重新启动桌面 App 后新版本才会生效，也不会修改 Microsoft
Store/AppX 安装。

`app` 是 root command，`auth` 是 `app` subcommand。如果你的代理 profiles 字面上
叫 `app` 或 `auth`，使用 `codex-proxy tui app`、`codex-proxy app --profile auth`
或 `codex-proxy app auth --profile auth` 来消除歧义。

使用指定 Codex binary:

```bash
codex-proxy tui --codex-path /path/to/codex
```

## Teams helper

Teams helper 让你从 Microsoft Teams 驱动 Codex，同时执行仍在你自己的机器上。
本地 `codex-proxy teams run` helper 会读取 Teams messages，在选中的 project
directory 中运行 Codex sessions，并把进度、答案、文件和通知发回 Teams。

Teams helper 已在稳定版发布中可用。先安装普通 binary；如果这台机器上有旧 helper，
请在 setup 前更新到最新稳定版：

```bash
codex-proxy upgrade
```

如果已有 Teams control chat，可以让 helper 自己升级到最新稳定版：

```text
update now
```

只有在明确想使用最新 pre-release 时，才用 `update prerelease`。

### 快速设置

推荐设置路径是快速开始章节里的交互式 bootstrap 脚本。它会完成完整本地设置流程：

1. 配置 Teams Graph tenant 和 client ID。
2. 运行 Microsoft device login。
3. 验证本地 Teams auth cache。
4. 安装或修复后台 Teams helper service 并启动它。

脚本把 auth metadata 和 tokens 存在当前用户本地。不需要 project checkout，也不会
hard-code Teams auth values；除非用环境变量或脚本 flags 提供，否则它会交互式询问
所需值。一次 delegated 登录即可覆盖读写 chat messages、meeting chats 和文件上传。

手动执行相同步骤：

```bash
codex-proxy teams auth config --tenant-id <tenant-id> --client-id <client-id>
codex-proxy teams auth
codex-proxy teams run
```

### 工作方式

helper 会创建一个用于机器级命令的 **control chat**，并为每个 Codex session 创建一个
**work chat**。用 control chat 启动 session、列出、park 和唤醒 session、检查状态、
更新本地 Codex CLI，以及重启或更新 helper。

Work chat 中的消息会先收到 ACK，然后排队并在该 session 的 Codex thread 中运行；
Codex 工作时会显示进度，最终答案只发送一次。答案到达时会在 control chat 中提醒
owner（几秒内到达的多个答案共用一次提醒），配置了 Teams Workflow card 时则通过
card 通知。Codex 忙碌时发送的多个 prompt 会依次运行，队列开始时只通知一次。

Teams 文本、引用消息、文件、粘贴图片和支持的 inline media 会传给 Codex。Teams 语音
或视频 clips 会先在本地转写，Codex 收到的是带有自动语音识别说明的 transcript。
Codex 在 `codex-helper-artifacts` manifest 中列出的文件会上传回 work chat。

如果你也在本地 terminal 或 IDE 中使用一个已有 work chat 的 session，helper 会把
这些 prompt 和答案同样发到该 work chat，让 Teams 仍然作为共享 conversation surface。

helper 把状态保存在一个本地 SQLite store 中。v0.1.20 或 v0.1.22-rc.59 的 store
会在首次启动时自动转换；旧 store 保持不变。helper 停止时（crash、更新、重启）正在
运行的请求会在同一个 Codex thread 中继续，并只发送一次中断通知。遇到 Microsoft
Graph throttling 时，helper 会控制读写速率并自行追上进度。

当同一个 Teams 用户在多台机器上运行 helper 时，Codex 可以为需要另一台机器上的
repo、硬件或本地上下文的任务使用跨机器 delegation。用户侧触发仍然是 Work chat
中的自然语言，例如“让另一台机器看一下这个 repo”或“让 GPU 机器检查这个失败”；
Codex 会根据已安装的 `cxp` skill 判断是否调用 `cxp delegate` workflow。每个 active
helper 会把紧凑的 machine card 和 heartbeat 发布到隐藏的每用户 Teams registry
chat，并通过自己的隐藏 machine inbox chat 接收任务。registry 只用于 discovery 和
liveness；delegation request、claim、progress、question 和 result 都保存在目标机器
的 inbox 中。当前这个机制只在同一个 signed-in Teams user 的机器之间汇合；隐藏
Teams chats 仍然受 tenant retention、eDiscovery 和 audit policy 约束。

后台 service 会在 terminal close、SSH disconnect、WSL session exit、sleep/wake 或
helper upgrade 后保持 helper 存活。Service bootstrap 会尽可能选择平台原生 per-user
service 机制，并在能安全操作时修复旧 helper service definitions。

Teams 启动的 Codex app-server 使用目标账号的默认 PATH，而不是复制后台 service PATH。
Unix 和 WSL 会从账号数据库解析 shell，并以 Codex 相同的 UID、GID、supplementary
groups 和 HOME 运行带逐次随机 nonce 的私有 framed Unix socket 探测，只取回 PATH。
Windows 会读取当前 Scheduled Task 用户 token 的环境块。Managed Node 只用于 Codex
安装和 wrapper discovery；存在 packaged native Codex binary 时会直接启动它，`node`、
`npm` 等命令仍从用户 PATH 解析。

`account-default` 不会复制 venv、direnv 或项目 shell 等仅属于当前终端的激活状态。
维护固定值时使用 `teams path explicit <PATH>`。未知 shell 会明确报错，不会伪装成
bash。`teams path status` 默认只显示身份、shell、来源、entry 数和 fingerprint；只有
`--show-path` 才显示完整 PATH。此前设置过 shell override 时，可用 `teams path shell
default` 恢复账号数据库中的默认 shell。早期版本保存的 `captured-terminal` 和
`service` 模式分别按 `explicit` 和 `account-default` 解析。

在非 WSL Linux 上，当 user manager 可用时，bootstrap 使用 `systemd --user`。如果
`systemd --user` 不可用，它会 fallback 到 local supervisor：用 `setsid` detach，用
file lock 防止重复实例，并在 Teams helper 退出时重启它。该 fallback 能承受 terminal
close 和 helper crashes，但不能在机器或 container reboot 后自动重启。设置
`CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND=systemd` 或
`CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND=local-supervisor` 可强制 backend；不设置则自动
选择。在 auto mode 中，已 enabled 或当前 active 的 local-supervisor install 会继续使用
local-supervisor，避免 backend flapping。在 WSL 中，默认 backend 仍是 Windows Scheduled
Task，但除非显式设置 `CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND=windows-task`，已 enabled 或
active 的 local-supervisor install 也会保持 sticky。

### 常用 Teams 命令

日常本地检查：

```bash
codex-proxy teams status
codex-proxy teams doctor --live
codex-proxy teams control
```

Control chat 命令（发送 `help` 查看列表）：

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

Work chat 命令（发送 `helper help` 查看列表；其他消息都会发给 Codex）：

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

`model` 列出 Codex 为当前登录账号提供的模型，`helper model <model> [effort]` 切换
单个 session 的模型。第三方 model profiles 和 catalogs 已不再支持；仍包含这些设置的
配置可以正常加载，并保持这些设置不变。

高级本地检查和维护：

```bash
codex-proxy teams service doctor
codex-proxy teams service status
codex-proxy teams service bootstrap
codex-proxy teams service restart
codex-proxy teams workflow status
codex-proxy teams migrate --dry-run --out /tmp/core-preview.sqlite
```

在 control chat 中发送 `codex update now` 可更新 CXP 托管的 Codex CLI。helper 会先
等待正在进行的 Codex 工作，再把结果发回 control chat。这个命令不会更新
cxp/codex-helper，也不会重试之前失败的请求；在 work chat 中使用 `helper retry last`
重试。

helper 启动的 Codex turn 会收到绝对路径 `CODEX_HELPER_CLI_PATH`；helper 不会为了暴露
`cxp` 而修改用户 PATH，因此缺少 `cxp` alias 并不意味着已安装 helper 缺失。

如果从 Teams control chat 更新 helper，helper 会把完成或失败通知发回 Teams。本地
`codex-proxy upgrade` 只安装 helper binary 和匹配的 `cxp` entry points；不会重启 Teams
service。本地 helper upgrade 后，在 control chat 中发送 `restart`（或运行
`codex-proxy teams service restart`）。

更深入的部署和排障指南见
[`docs/teams_source_deployment_guide.md`](docs/teams_source_deployment_guide.md)。

## Git-backed skill subscriptions

当你希望自己的 Codex skills 放在 Git repo 里，并在多台机器之间保持同步时，使用
git-backed skill subscriptions。一个 subscription 会把对应 git source 中的 skills
安装到 `$HOME/.agents/skills`；之后可以用 `sync` 更新本地 managed copy，用 `push`
在逐项确认后把本地 edits 发布回 source。

```bash
codex-proxy skills add <git-url>
codex-proxy skills list
codex-proxy skills sync
codex-proxy skills doctor
codex-proxy skills push <name>
codex-proxy skills remove <name>
```

这是维护个人或团队 skills 的推荐路径。下面的内置 `cxp` skill 由 helper 自己维护，
和这些 git-backed subscriptions 分开管理。

## 内置 cxp skill

这是可选维护。普通首次用户不需要运行这些命令。

安装器会 best-effort 把 bundled `cxp` Codex skill 安装到 `$HOME/.agents/skills/cxp`。
之前安装在 `~/.codex/skills` 下的 managed skills 会在运行 skills commands 时迁移到 agents
skills 目录。该 skill 包含本地命令地图，以及 Teams helper restarts 等 disruptive
operations 的 safe handoff rules。它作为 built-in skill 管理，不是
git-backed subscription，因此不会作为 remote source 出现在 `codex-proxy skills list` 中。

手动修复或安装：

```bash
codex-proxy skills install-builtin
codex-proxy skills migrate --yes
```

只查看迁移计划而不改文件：

```bash
codex-proxy skills migrate --dry-run
```

## 升级

已安装的本地 TUI 会在首屏显示后静默准备符合条件的 CXP 更新，并在 Codex 会话运行期间
继续检查。新窗口可以试用已准备的版本，已有窗口继续运行。网络或下载失败不会延迟启动。
候选启动失败时回退到已确认的旧版，同一候选不再自动重试；稳定启动器始终保留。

设置 `CXP_AUTO_UPDATE=0` 可关闭 TUI 检查和待处理候选的自动试启；把
`CODEX_PROXY_VERSION` 固定为某个版本也会关闭此路径。TUI 自动更新只接受通过原生安装包
测试的稳定版 `p0`/`p1` 发布，不更新 Codex 本身，也不重启 Teams 服务。旧的物理启动器
需要先进行一次明确的安装或升级。恢复规则、存储上限、信任边界和测试命令见
[TUI 更新安全设计](docs/tui_auto_update.md)。

从 GitHub Releases 升级：

```bash
codex-proxy upgrade
```

可选 flags:

- `--repo owner/name`（覆盖 GitHub repo）
- `--version vX.Y.Z`（安装指定版本）
- `--include-prerelease`（允许 latest 解析到最新 pre-release）
- `--install-path /path/to/codex-proxy`（覆盖安装路径；可以是文件或目录）
- `--restart-teams-service`（opt in 到 legacy drain/stop/refresh/restart 路径；在 WSL 上，
  如果需要 service repair，可能请求 Windows permission）

默认情况下，`codex-proxy upgrade` 会 download、validate、install，并统一已知 helper
entry points，而不会触碰 Teams service。如果旧安装在多个已知位置留下了 `cxp` 或
`codex-proxy`，upgrade 会在当前调用的 helper 可运行时把它作为 canonical target，忽略 broken
stale install-path hints，并修复已知 managed/env/default entries 让它们指向 canonical helper。
这样 network timeout 或 Windows service repair 问题不会中断正在运行的 helper。在 Teams
control chat 中发送 `restart` 来运行已安装 helper update。

Teams background mode 也会每 30 分钟检查 `codex-helper` GitHub Releases，并在当前
Teams/Codex work drain 后静默应用 eligible helper updates。Background helper auto-update 使用
正在运行的 stable helper path（当它是 runnable helper binary 时），然后在重启 helper 前
修复已知 managed/env/default entry points 指向已安装 helper：

- `p0`: 一检测到 release 就更新。
- `p1`: release 发布 48 小时后更新。
- `p2`: 从不 auto-update。

除非 release notes 包含这个 machine-readable marker，release 默认是 `p2`:

```md
<!-- codex-helper-release: {"auto_update_priority":"p1"} -->
```

marker 中可使用 `p0`、`p1` 或 `p2`。名为 `codex-helper-auto-update-p0`、
`codex-helper-auto-update-p1` 或 `codex-helper-auto-update-p2` 的 release asset 也会被接受；
冲突 marker 会 fail closed 到 `p2`。Release publishing 会更新静态
`auto-update-index` branch，因此 Teams helper checks 通常只需获取一个小 JSON 文件，而不是列出
GitHub Releases。如果 index 不可用，helper 会 fallback 到 GitHub Release API。Teams mode
忽略 draft releases、prereleases、旧版本，以及没有匹配 platform asset 的 releases。

升级 Codex CLI 本身（reinstall-style）：

```bash
codex-proxy --upgrade-codex
```

行为：

- 使用当前代理偏好：proxy on -> 通过代理升级；proxy off -> 直接连接。
- 要求 Codex 已安装；该模式不会从零安装。
- 在可识别时保持安装来源：
  - system npm global install -> `npm install -g @openai/codex`
  - managed/local npm install（`codex-proxy` prefix）-> managed reinstall path
- 无法确定来源时 fail fast（避免意外改变 install topology）。

在原生 Windows 或 WSL 上手动刷新 CXP-managed 的 Codex 桌面 App 副本：

```bash
codex-proxy --upgrade-codex-app
```

可选的 positional argument 用来选择已配置的 proxy profile。该命令会下载并校验官方签名
x64 ChatGPT MSIX，把它安装到当前 managed runtime 旁边，并原子选择它供后续
`codex-proxy app` 使用。已经运行的桌面进程会继续使用旧 runtime，退出并重新启动后才会
使用新版本；Microsoft Store/AppX 安装不会被修改。

### 审计或清理旧版 patched binaries（POSIX）

源码 checkout 内提供了一个 fail-closed 清理工具，用于处理早期 `codex-helper` 或
`claude-helper` 遗留的 patched copies。默认 audit 不会修改文件：

```bash
python3 scripts/cleanup_patched_binaries.py --audit
```

检查报告后，可在具有可读 Linux-style procfs 的系统上只清理由 helper provenance 明确证明的
artifacts：

```bash
python3 scripts/cleanup_patched_binaries.py --purge --json cleanup-report.json
```

POSIX 系统都可执行 audit；purge 刻意要求 procfs，避免把 active executable 误判为 inactive
artifact。该工具不会自动运行。它只操作经过验证的当前用户 home/XDG roots 和 `/tmp` 下由当前用户
拥有的 helper namespace；遇到 symlink、hard link、其他用户 ownership、变化的 inode、nested mount 或
ambiguous history 时会拒绝操作；对 in-place Claude patch 只恢复 executable，不会删除它。
如果确认目标仍是 patched content、但没有 hash-verified original，则报告
`repair_required`。运行中的进程默认保留；只有显式同时使用 `--purge` 和
`--terminate-active` 才会尝试终止精确匹配的进程。

## 长期运行实例（可选）

启动可复用 daemon instance:

```bash
codex-proxy proxy start [profile]
codex-proxy proxy list
```

`proxy list` 只读查询已保存实例，以及当前配置中各 profile 的 app-gateway 登记，
检查本地监听器并展示故障原因，不探测外部目标。`LAST_SEEN` 是已保存的心跳或更新时间，
不是本次检查时间。历史 dead 记录仍会显示。`proxy prune` 会保留后端正在恢复的活进程、
启动租约，以及持久化的恢复熔断记录。
app-gateway 行用于诊断；`proxy stop` 和 `proxy prune` 仍操作已保存实例，
不管理 app-gateway 登记。

普通 `run`、`history open` 和从 TUI 启动的 sessions 使用 private proxy stacks。只有用
`proxy start` 启动的 instances 会跨 sessions shared/reused。

用 `--foreground` 让 daemon 保持 attached 到当前 terminal。

清除已保存代理设置、尝试停止已知 proxy daemons，并再次触发首次运行代理提示：

```bash
codex-proxy proxy reset
```

停止一个 instance:

```bash
codex-proxy proxy stop <instance-id>
```

## 安装（详细）

### Linux / macOS（一行命令，自动检测 curl/wget）

```bash
sh -c 'url="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.sh"; if command -v curl >/dev/null 2>&1; then curl -fsSL "$url" | sh; elif command -v wget >/dev/null 2>&1; then wget -qO- "$url" | sh; else echo "need curl or wget" >&2; exit 1; fi'
```

默认安装到 `~/.local/bin/codex-proxy`。

安装器会在 `codex-proxy` 旁放置 `cxp` shim，并尝试把 `~/.local/bin` 和托管 CLI 目录加入
PATH（以及添加 `cxp` alias）。如果找不到命令，请打开一个新 shell。
如果需要手动更新 PATH:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

安装指定版本（示例）：

```bash
curl -fsSL https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.sh | sh -s -- --version vX.Y.Z
```

### Windows (PowerShell)

```powershell
$ErrorActionPreference="Stop"; $u="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1"; $p=Join-Path $env:TEMP "codex-proxy-install.ps1"; try { Invoke-WebRequest -UseBasicParsing $u -OutFile $p; Unblock-File -LiteralPath $p; & powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File $p; if ($LASTEXITCODE -ne 0) { throw "installer exited with code $LASTEXITCODE" } } finally { Remove-Item -Force $p -ErrorAction SilentlyContinue }
```

默认安装到 `%USERPROFILE%\.local\bin\codex-proxy.exe`。
安装器也会在那里写入 `cxp.cmd`，并为该目录和托管 CLI 目录更新 PATH。
托管 Codex CLI 使用 native Windows binary；如果它以 `0xC0000135` 退出或提到
`VCRUNTIME140*.dll`，请安装与 Codex architecture 匹配的 Microsoft Visual C++ 2015-2022
Redistributable（x64: `Microsoft.VCRedist.2015+.x64`，ARM64:
`Microsoft.VCRedist.2015+.arm64`）。当 managed Codex install 期间检测到这个精确 runtime
failure 时，`cxp` 会自动尝试安装 redistributable 并触发 Windows UAC prompt。设置
`CODEX_PROXY_VCREDIST_INSTALL=never` 可禁用；设置为 `prompt` 则会在显示 UAC prompt 前先在
terminal 中询问。

安装指定版本：

```powershell
$ErrorActionPreference="Stop"; $u="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1"; $p=Join-Path $env:TEMP "codex-proxy-install.ps1"; try { Invoke-WebRequest -UseBasicParsing $u -OutFile $p; Unblock-File -LiteralPath $p; & powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File $p -Version vX.Y.Z; if ($LASTEXITCODE -ne 0) { throw "installer exited with code $LASTEXITCODE" } } finally { Remove-Item -Force $p -ErrorAction SilentlyContinue }
```

### 环境变量

这些变量由安装脚本使用。`codex-proxy upgrade` 也会 honor `CODEX_PROXY_REPO`、
`CODEX_PROXY_VERSION` 和 `CODEX_PROXY_INSTALL_DIR`。

| 变量 | 说明 |
|------|------|
| `CODEX_PROXY_REPO` | 覆盖 GitHub repo（默认：`baaaaaaaka/codex-helper`） |
| `CODEX_PROXY_VERSION` | 覆盖版本（默认：`latest`） |
| `CODEX_PROXY_INSTALL_DIR` | 覆盖安装目录（Unix 默认：`~/.local/bin`；Windows 默认：`%USERPROFILE%\.local\bin`） |
| `CODEX_NPM_PREFIX` | 覆盖托管 CLI npm prefix，其 executable directory 会加入 PATH |
| `CODEX_PROXY_API_BASE` | 覆盖 GitHub API base URL |
| `CODEX_PROXY_RELEASE_BASE` | 覆盖 GitHub release base URL |
| `CODEX_PROXY_SKIP_PATH_UPDATE` | 仅 Windows installer：设为 `1` 时跳过持久 PATH 更新 |
| `CODEX_PROXY_PROFILE_PATH` | 仅 Windows installer：覆盖要更新的 PowerShell profile |
