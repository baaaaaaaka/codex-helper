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
- **日常选项**：通过选中的代理运行命令，选择模型 profile，打开 Codex 桌面 App，
  或从 Teams 驱动 Codex。
- **高级操作**：skills 维护、beacon scheduler 执行、Teams service 恢复、
  workflow webhook，以及 source-checkout 部署。

本 README 的其余部分按这个分层组织：先是常用命令，再是可选的
model/desktop/Teams 流程，然后是高级执行和维护参考。按受众和深度组织的完整
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
| `codex-proxy run --model-profile <name> -- codex` | 使用保存的模型 profile 启动 Codex |
| `codex-proxy model list` | 显示内置和已配置的模型选择 |
| `codex-proxy model setup <model>` | 配置内置模型选择，例如 `deepseek`、`mimo`、`kimi`、`glm`、`minimax` 或 `qwen` |
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
| `codex-proxy app --model-profile <name>` | 通过隔离的 `CODEX_HOME` 使用保存的模型 profile 启动 Codex 桌面 App |
| `codex-proxy --upgrade-codex` | 使用检测到的安装来源重新安装 Codex CLI |
| `codex-proxy completion <shell>` | 生成 shell completion |
| `codex-proxy init` | 创建 SSH profile |
| `codex-proxy run [profile] -- <cmd> [args...]` | 使用当前模式运行命令；给出 profile 时强制使用代理（默认命令是 `codex`） |
| `codex-proxy run -- codex` | 通过 CXP 标准审批 broker 启动原版 Codex TUI |
| `codex-proxy run --aaa -- codex` | 为本次 Codex 运行开启 Agent Auto Approve |
| `codex-proxy run --model-profile <name> -- codex` | 使用保存的模型 profile 启动 Codex |
| `codex-proxy tui` | 在终端 UI 中浏览 Codex 历史 |
| `codex-proxy history tui` | 在终端 UI 中浏览 Codex 历史 |
| `codex-proxy history list [--pretty]` | 以 JSON 列出发现的 projects/sessions |
| `codex-proxy history show <session-id>` | 打印某个 session 的完整历史 |
| `codex-proxy history open <session-id>` | 在 Codex 中打开某个 session |
| `codex-proxy model list` | 列出内置模型选择和配置状态 |
| `codex-proxy model setup <model>` | 设置内置模型选择，并可选择设为默认 |
| `codex-proxy model use <model>` | 把已配置的模型设为后续 Codex 启动默认值 |
| `codex-proxy model doctor [model]` | 验证内置模型选择背后的模型 profile |
| `codex-proxy model-profile setup [name]` | 创建或更新命名模型 profile |
| `codex-proxy model-profile list` | 列出保存的模型 profiles |
| `codex-proxy model-profile doctor [name]` | 验证保存的模型 profile |
| `codex-proxy model-profile set-default <name>` | 设置默认模型 profile |
| `codex-proxy model-profile delete <name>` | 删除非默认模型 profile |
| `codex-proxy responses serve` | 运行本地 `/v1/responses` adapter，后端是 OpenAI-compatible chat upstream |
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
| `codex-proxy proxy prune` | 移除 dead/unhealthy instances |
| `codex-proxy proxy reset` | 清除已保存的代理设置，并尝试停止已知 daemons，让下次启动重新询问 |
| `codex-proxy proxy doctor` | 报告环境问题和安装提示 |
| `codex-proxy teams status` | 显示 Teams helper state、control chat、service、owner 和 queue 状态 |
| `codex-proxy teams doctor` | 检查本地 Teams helper auth 和 service readiness |
| `codex-proxy teams workflow status` | 显示可选 Teams Workflow notification 配置 |
| `codex-proxy teams workflow enable --webhook-url-file <path>` | 用私有本地 webhook URL 文件启用 Workflow cards |
| `codex-proxy teams send-file <path> --session <id>` | 上传本地 outbound 文件并作为 Teams attachment 发送 |
| `codex-proxy teams probe-chat --chat <chat-id-or-link>` | 对外部 Teams chat 做只读探测，不绑定 helper state |
| `codex-proxy teams pause` / `resume` / `drain` / `recover` | 从终端 pause、resume、drain 或 recover Teams helper state |
| `codex-proxy teams chat recreate <session-id> --yes` | 为现有 helper session 创建并绑定新的 Teams Work chat |
| `codex-proxy teams service bootstrap` | 安装或修复后台 Teams helper service |
| `codex-proxy teams service restart --force` | 恢复本地 active Teams state，然后从终端强制重启 service |
| `codex-proxy teams control --print` | 打印配置好的 Teams control chat link |
| `codex-proxy delegate resolve --query <text> --json` | 为 Codex 或诊断解析跨机器 delegation 候选机器 |
| `codex-proxy delegate start --candidate-token <token> --task-file <path> --json` | 为选中的机器创建 idempotent delegation request |
| `codex-proxy delegate status` / `wait` / `cancel` | 使用 `start` 保存的 route 检查、等待或取消 delegation |
| `codex-proxy beacon profile list` | 列出 beacon execution profiles |
| `codex-proxy beacon profile create <name>` | 创建 draft beacon execution profile |
| `codex-proxy beacon profile update <name>` | 创建新的 profile revision，不破坏已绑定旧 revision 的 chats |
| `codex-proxy beacon profile history <name>` | 列出某个 beacon profile 的当前和历史 revisions |
| `codex-proxy beacon profile rollback <name> <revision>` | 把历史 profile revision 发布为新的 latest revision |
| `codex-proxy beacon profile gc <name>` | 仅清理没有 active target/allocation 引用的历史 revisions |
| `codex-proxy beacon profile doctor <name>` | 验证 profile fields 和 provider adapter commands，不触碰 scheduler |
| `codex-proxy beacon profile doctor <name> --smoke` | submit、query、cancel 一个 scheduler allocation 以验证 adapters |
| `codex-proxy beacon profile confirm <name>` | review 后确认 beacon profile；不完整 profiles 仍保持 draft |
| `codex-proxy beacon profile status <name>` | 检查一个 beacon profile |
| `codex-proxy beacon profile delete <name>` | 在未被 active 使用时 archive 一个 beacon profile |
| `codex-proxy beacon status [--session <id>]` | 显示 beacon target state |
| `codex-proxy beacon release <profile\|allocation\|provider-job\|machine>` | 按 profile、allocation id、provider job id 或 machine id 预览并释放 beacon resource |
| `codex-proxy beacon switch-profile <name> --session <id>` | 把 conversation 切换到 ready beacon profile |
| `codex-proxy beacon switch-profile <name> --session <id> --after-current-turn` | 延迟 beacon switch，让当前 Codex turn 完成 |
| `codex-proxy beacon allocation list` | 列出 managed beacon allocation requests |
| `codex-proxy beacon allocation status <allocation-or-provider-job>` | 显示一个 managed allocation request |
| `codex-proxy beacon allocation cancel <allocation-or-provider-job>` | 通过配置的 provider adapter cancel 一个 managed allocation |
| `codex-proxy beacon allocation reconcile <allocation>` | 通过配置的 provider adapter query/adopt/submit |
| `codex-proxy beacon allocation reconcile-all` | reconcile 全部 allocations、drain stale workers，并 recover stale claims |
| `codex-proxy beacon provider template slurm` | 打印 starter Slurm adapter script |
| `codex-proxy beacon provider template lsf` | 打印 starter LSF adapter script |
| `codex-proxy beacon worker run-once --machine <id>` | 在 allocated worker 上 claim 一个 queued beacon job，并发布 terminal result |
| `codex-proxy beacon worker run-once --allocation <id> --wait 30m` | 注册当前 scheduler worker，等待 Teams job，并发布 terminal result |
| `codex-proxy beacon worker serve --allocation <id>` | 注册带 bootstrap diagnostics 的 long-lived worker，serve jobs 直到 idle 或停止 |
| `codex-proxy beacon machine list` | 列出 beacon machines |
| `codex-proxy beacon machine status <machine-or-lease>` | 检查 beacon machine 或 lease，并获得 confirmation tokens |
| `codex-proxy beacon machine release <machine-or-lease>` | drain 或 release beacon machine |
| `codex-proxy beacon machine kill <machine-or-lease-or-job> --confirm <token>` | 只有带 status 中精确 token 时才 hard-kill beacon machine |
| `codex-proxy upgrade` | 从 GitHub Releases self-update |

常用 flags:

- `--config /path/to/config.json` 覆盖 config file 路径
- 当命令是 Codex 时，`run` 支持 `--model-profile <name>` 进行单次模型选择
- `app` 支持 `--model-profile <name>`，用于需要保存模型 profile 的桌面 App 启动
- `tui` / `history tui` 支持 `--codex-dir`、`--codex-path`、`--profile` 和 `--refresh-interval`（默认 `5s`，用 `0` 禁用）
- `history open` 支持 `--codex-dir`、`--codex-path` 和 `--profile`
- `history list` / `history show` 支持 `--codex-dir`
- `skills` 支持 `--codex-dir`
- `beacon` 支持 `--store /path/to/beacon.json` 覆盖 beacon state file

</details>

## 模型选择和标准审批

### 标准审批 runtime

CXP 使用普通的 on-request 审批启动原版 Codex binary。本地交互式入口默认由用户审批：
broker 会把 approval 请求原样交给官方 Codex TUI。可以在 CXP 历史 TUI 中按 `Ctrl+A`
持久切换 Agent Auto Approve（AAA），也可以只为一次命令开启而不修改保存的偏好：

```bash
codex-proxy run --aaa -- codex
codex-proxy run --aaa -- codex exec "..."
```

AAA 收到支持的 approval 请求后固定等待 500 ms 再批准。未开启 AAA 时，非交互式
`codex exec` facade 因没有人工 reviewer 而 fail-closed。Teams 和 Beacon 是无人值守入口，
因此会在代码中显式启用相同的自动批准 handler，并且不会继承本地 TUI 偏好。
`--aaa` 只由 CXP 消费，不会传给 Codex。

这套 runtime 要求 Codex CLI 0.131.0 或更高版本；较旧的 managed/PATH 安装会在
第一次 broker turn 前自动升级。release compatibility sweep
会同时验证 app-server handshake、remote TUI 能力，以及生产 broker 的根 WebSocket
地址和 bearer-token attachment contract。

Codex 内层 sandbox 在操作获批前仍保持受限。获批操作只能继承外层 host、container、
cgroup、Slurm job 或 LSF job 已经授予的硬件和 mounts，不能突破外层隔离边界。Telemetry
保持开启，并且 payload 不经过 CXP 修改。

已接入 broker 的入口包括 Codex TUI/history、`codex exec` facade、Teams turn 和 Beacon
worker。手动模式和 AAA 使用相同的原版 binary、on-request policy、Responses gateway 与
proxy 路由，唯一差别是 reviewer。Contract CI 使用开启 analytics 的原版 Codex binary，并要求审批 telemetry 仍表现为
普通的 `reviewer=user`、`status=approved`、`user_approved` 事件。CXP 不隐藏自己的
app-server client identity，也不承诺服务端无法根据时序或其他正常 telemetry 推断自动化。

`codex-proxy app` 仍然直接启动官方 Desktop App。Desktop App 目前没有稳定的外部
app-server attachment contract，因此，在声称“所有 CXP surface 都已接入 broker”之前，
Desktop 自动审批仍是 final release blocker；CXP 不会为这个入口静默回退到已经退役的执行机制。

### 内置模型选择

当你想从内置 model/provider presets 中选择时，使用 `model`:

```bash
codex-proxy model list
printf '%s' "$DEEPSEEK_API_KEY" | codex-proxy model setup deepseek --api-key-stdin
codex-proxy model use deepseek
codex-proxy model doctor deepseek
```

内置选择包括 `default`，以及 `deepseek`、`mimo`、`kimi`、`glm`、`minimax` 和
`qwen` 等第三方 choices。第三方 choices 会在本地保存 model profile，并通过本地
Responses adapter 运行 Codex。

用保存的 model profile 启动一次，而不改变默认值：

```bash
codex-proxy run --model-profile deepseek -- codex
codex-proxy app --model-profile deepseek
```

Teams 用户可以在创建或切换 Work chats 时选择 model profiles；Teams 命令列在
下面的 Teams helper 章节。

### 自定义 model profiles 和 Responses adapter

当你需要带明确 provider、model、API-key source 或 SSH proxy route 的命名
profile 时，使用 `model-profile`:

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

对于更底层的实验或集成，`responses serve` 会暴露一个本地 `/v1/responses`
facade，后端是 OpenAI-compatible chat upstream:

```bash
codex-proxy responses serve \
  --base-url https://api.example.com/v1 \
  --api-key-env PROVIDER_API_KEY \
  --model provider-model
```

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
后退出。用 `codex-proxy app <profile>` 强制使用某个代理 profile。如果代理模式
已启用，该命令会把代理环境变量传给桌面 App，并对 WSL/AppX 这种桌面 App 可能
无法继承或访问该环境的情况打印 warning。

当桌面 App 应使用保存的 model profile 启动时，使用
`codex-proxy app --model-profile <name>`。如果桌面 App 已经在运行，请先退出它，
让 model profile 配置生效。

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

`app` 是 root command，`auth` 是 `app` subcommand。如果你的代理 profiles 字面上
叫 `app` 或 `auth`，使用 `codex-proxy tui app`、`codex-proxy app --profile auth`
或 `codex-proxy app auth --profile auth` 来消除歧义。

使用指定 Codex binary:

```bash
codex-proxy tui --codex-path /path/to/codex
```

## Teams helper

Teams helper 让你从 Microsoft Teams 驱动 Codex，同时执行仍在你自己的机器上。
本地 `codex-proxy teams run` listener 会读取 Teams messages，在选中的 project
directory 中启动或恢复 Codex sessions，并把 status、answer、artifact 和
notification updates 发回 Teams。

Teams helper 已在稳定版发布中可用。先安装普通 binary；如果这台机器上有旧 helper，
请在 setup 前更新到最新稳定版：

```bash
codex-proxy upgrade
```

如果已有 Teams control chat，可以让 helper 自己升级到最新稳定版：

```text
helper update now
```

只有在明确想使用最新 pre-release 时，才用 `helper update prerelease`。

### 快速设置

推荐设置路径是快速开始章节里的交互式 bootstrap 脚本。它会完成完整本地设置流程：

1. 配置 Teams auth metadata。
2. 运行 Microsoft device login。
3. 验证本地 Teams auth cache。
4. 安装或修复后台 Teams helper service。
5. 打开或打印 Teams control chat link。

脚本把 auth metadata 和 tokens 存在当前用户本地。不需要 project checkout，也不会
hard-code Teams auth values；除非用环境变量或脚本 flags 提供，否则它会交互式询问
所需值。

### 工作方式

helper 会创建一个用于机器级命令的 **control chat**，并为 Codex sessions 创建单独的
work chats。用 control chat 列出 projects、选择 project/session、启动新的 Codex
work、检查 status、恢复 stuck state、restart 或 update 已安装 helper，并且只对
source-checkout development helper 做 reload。

当 Teams work chat message 被路由到 Codex 时，helper 会将其排队，在选中 session
working directory 中启动 Codex，把进度以 status updates 流式发送，并把最终答案发回
work chat。如果从另一个本地入口检测到 Codex answer，helper 也可以创建或链接对应
Teams chat，并在那里通知你。

Teams 文本、引用消息、文件、粘贴图片和支持的 inline media 可在可用时传给 Codex。
Teams 语音或视频 clips 会先在本地转写，Codex 收到的是带有自动语音识别说明的
transcript。Codex artifact manifest 中列出的 generated files 可在配置 file-write auth
后上传回 Teams。

如果你也在同一台机器的本地 terminal 里使用 Codex，Teams helper 可以检测匹配的本地
answer，创建或链接对应的 Teams chat，并把可见结果发到那里。这样 Teams 仍然可以作为
共享 conversation surface，而不要求每个 Codex turn 都必须从 Teams 发起。

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
helper upgrade 后保持 listener 存活。Service bootstrap 会尽可能选择平台原生 per-user
service 机制，并在能安全操作时修复旧 helper service definitions。

在非 WSL Linux 上，当 user manager 可用时，bootstrap 使用 `systemd --user`。如果
`systemd --user` 不可用，它会 fallback 到 local supervisor：用 `setsid` detach，用
file lock 防止重复实例，并在 Teams runner 退出或本地 owner/poll 证据 stale 时重启它。
该 fallback 能承受 terminal close 和 helper crashes，但不能在机器或 container reboot
后自动重启。设置 `CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND=systemd` 或
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
codex-proxy teams control --print
```

日常 control-chat 命令：

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

日常 Work chat 命令：

```text
helper status
helper retry last
helper cancel running
helper file relative/path.ext
model status
model switch deepseek
model fork deepseek
```

高级本地检查和维护：

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

在 Teams control chat 中，`helper reload now` 只用于 source-checkout development
reload。普通安装的 helper 在本地 repair 后应使用 `helper restart now`，release update
则使用 `helper update now`。只有在明确想使用最新 pre-release 时，才用
`helper update prerelease`。

如果发到 control chat 的快速 Codex 问题卡住了，在同一个 control chat 发送
`helper cancel running`。Work chat requests 仍在各自的 Work chat 内取消。

Beacon profile setup 是本地 `codex-proxy beacon ...` CLI workflow。可以在 Teams
control chat 里询问它，但 profile mutation commands 应在本地运行，除非 helper 打印了
明确支持的 Teams command。

Control-chat Codex fallback 在 Teams helper service environment 下运行，不一定是你的
interactive shell。如果它需要检查本地 helper binary，child process 会收到
`CODEX_HELPER_CLI_PATH`；该环境中缺少 `cxp` alias 并不意味着已安装 helper 缺失。

如果从 Teams control chat 更新 helper，helper 会先 drain 当前 work，并把完成或失败通知
发回 Teams。本地 `codex-proxy upgrade` 只安装 helper binary 和匹配的 `cxp` entry
points；默认不会 repair、stop 或 restart Teams service。本地 helper upgrade 后，在
Teams control chat 中发送 `helper restart now`，让已安装 helper update 生效。

更深入的部署和排障指南见
[`docs/teams_source_deployment_guide.md`](docs/teams_source_deployment_guide.md)。

## Beacon execution profiles

Beacon 是高级 Teams 执行模式。除非你已经使用 Teams helper，并希望未来 Teams Work
chat turns 在特定 local worker、Slurm allocation 或 LSF allocation 上运行，而不是在
helper service machine 上运行，否则可以跳过本节。

Beacon profiles 描述 Codex work 应在哪里执行。它们和 SSH proxy profiles 是不同的：
`codex-proxy proxy` 控制网络路由，而 `codex-proxy beacon profile ...` 控制 scheduler
或 worker placement。

用站点所需 scheduler fields 创建一个 Slurm draft profile:

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

LSF 和 local drafts 输入更少：

```bash
codex-proxy beacon profile create batch --provider lsf --queue normal --shared-path /shared/cxp-beacon
codex-proxy beacon profile create local --provider local
```

如果 beacon job 需要使用已有 SSH proxy profile 做网络访问，加上
`--proxy ssh_profile --proxy-profile <existing-profile>`。用 `--isolation shared` 或
`--isolation exclusive` 选择默认 lease sharing mode。

对于 managed Slurm/LSF profiles，`--shared-path` 必须指向 control machine 和 allocated
workers 都能读写的绝对目录。如果显式配置了 `CODEX_HELPER_BEACON_STORE`，cxp 会为 workers
保留该 store path；否则会在 `shared_path` 下派生 store。

Profiles 在检查并确认前保持 draft。`profile doctor` 会验证 profile fields、shared-path
write/rename access，以及未来 Teams turns 需要的 `query`/`submit`/`cancel`/`renew`
adapter commands:

```bash
codex-proxy beacon profile doctor gpu
codex-proxy beacon profile doctor gpu --smoke
codex-proxy beacon profile confirm gpu
codex-proxy beacon profile status gpu
```

普通 `doctor` 是非破坏性的，会检查 profile fields 和 adapter presence/executability。
只有当你希望 cxp submit、query、cancel 一个真实 scheduler allocation，从和未来 Teams
turns 相同的 profile/environment 验证 adapter output contract 时，才加 `--smoke`。

要修改 profile，原地 update。helper 会记录新的 revision，而 queued 或 running turns 会
继续使用已有 target snapshot:

```bash
codex-proxy beacon profile update gpu \
  --provider slurm \
  --partition interactive \
  --image new-image.sqsh \
  --nodes 1 \
  --gpu 1 \
  --duration 4
```

用 `profile history` 查看 published revisions。如果新 revision 有问题，
`profile rollback <name> <revision>` 会把历史 config 重新发布为新的 latest revision；
`profile gc <name>` 只移除没有任何 conversation、queued turn 或 allocation 仍引用的
history entries。

Profile ready 后，检查 target state 或显式切换已有 conversation:

```bash
codex-proxy beacon status --session <session-id>
codex-proxy beacon switch-profile gpu --session <session-id>
```

在 Teams 中，从 Work chat 使用 `beacon switch <profile>`。helper 会自动 submit、query、
等待 worker、renew，并清理 managed allocation。如果需要手动释放当前 resource，在 Work
chat 中发送 `beacon release`；profile binding 保持不变，未来 turns 可以为同一个 profile
获取 fresh worker。如果 worker 是 shared，Work-chat release 只 detach 当前 chat 的 demand，
并让 shared worker 可供其他 chats 使用；会影响所有人的 shared 或 forced release 仍然需要
control-chat release。`beacon local` 会把未来 turns 切回 local execution，并在安全时要求
helper drain 或 release 此 chat 的旧 beacon resource。

从 CLI 或 Teams control chat，
`codex-proxy beacon release <profile|allocation|provider-job|machine>` 接受你已有的
resource identifier，并解析内部对象类型。Release commands 会显示预览，包括受影响 chats、
queued/running turns、allocation ids、provider job ids 和计划动作。Shared 或 forced
releases 必须带上显示的 `--confirm <token>` 值，才能影响其他 chats。

当 Teams Work chat 目标是 beacon profile 时，每个 turn 都会 snapshot target，并在 Codex
开始前记录 managed allocation request。显式 beacon turns 不会 fallback 到 local execution。
如果尚无 accepting beacon worker/lease，`beacon status` 会显示需要关注的 allocation id、
allocation state、provider job id、provider state 和 provider reason。

Provider submission 基于 adapter。最不容易出错的 Teams workflow 是把 adapter commands
存到 beacon profile 上：`--query-command`、`--submit-command`、`--cancel-command` 和
`--renew-command`；这些 profile changes 会应用到未来 turns，而不需要 restart Teams helper。
Managed Slurm/LSF adapters 默认使用你的 default user shell environment，所以 modules、
`submit_job`、NSS 或 `SUBMIT_ACCOUNT` 的站点设置可用，不需要复制到 helper service。若
`$SHELL -lic` 不兼容（例如 tcsh/csh），或 adapter 需要干净的 helper service environment，
加 `--adapter-shell direct`。要从 site-editable Slurm 或 LSF wrapper 开始，打印 template:

```bash
codex-proxy beacon provider template slurm > ~/bin/cxp-beacon-slurm-adapter
chmod +x ~/bin/cxp-beacon-slurm-adapter
```

如果更喜欢每个 provider 一个全局 adapter，也可以通过 helper service environment 指向
可执行 adapters：

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

Profile-stored adapter commands 通常更简单，因为它们会应用到未来 turns，而不需要 service
change。如果改的是 helper service environment variables，则在 Teams control chat 中用
`helper restart now` 重启已安装 helper。

当未设置 adapter shell mode 时，managed Slurm/LSF adapters 的行为等同于
`--adapter-shell user`。cxp 只用 `$SHELL -lic` 捕获 framed environment snapshot，然后用
该 environment 直接运行 adapter，因此 shell startup output 不会污染 adapter protocol。
仅在少数站点 scheduler submission 是 shell function 或 alias、无法捕获为 environment
时，才使用 `--adapter-shell shell-command`。adapter 接收 `--request-id`、`--name`、
`--profile`、`--provider`、`--partition`、`--image`、`--queue` 和
`--operation query|submit|cancel|renew` 等 flags。它应打印 JSON，例如
`{"provider_job_id":"123","raw_state":"PD","reason":"Resources","provider_deadline":"2026-05-18T10:30:00Z"}`
或 key-value 输出，例如
`provider_job_id=123 raw_state=PD reason=Resources provider_deadline=1779090600`。
生成的 Slurm/LSF templates 包含 `query`、`submit`、`cancel`，以及一个 site-policy
`renew` stub；该 stub 在编辑前会非零退出。当 scheduler 暴露 walltime extension 时，实现
renew case。

Beacon adapter troubleshooting:

- scheduler job 返回 `exit 127` 通常意味着 submitted command malformed，或 allocation
  内 PATH 不同。最终 worker command 中保持恰好一个 `exec`；如果站点 wrapper 已经 prepends
  `exec`，从 adapter 中移除额外的一个。
- worker 内出现 `allocation request ... not found` 通常意味着 worker 使用了错误 state file。
  自定义 Slurm/LSF submit adapters 必须接受 `--shared-store`，并传给 worker：
  `cxp beacon --store <shared-store> worker ...`。
- 如果 `$SHELL -lic` 在 tcsh/csh 下失败，用 `--adapter-shell direct` 更新 profile；profile
  revisions 会应用到未来 turns。
- Beacon worker 会在 allocation 内启动标准审批 runtime，因此获批命令可以继承 scheduler
  或 container 已授予的 devices 和 mounts，同时保留外层隔离边界。
- 如果 worker doctor 报告 `missing codex`，设置 scheduler PATH 或从 adapter 传 worker
  `--codex-path <codex-or-wrapper>`。wrapper 对 path resolution 或额外 Codex exec flags
  仍有用，例如 `--skip-git-repo-check`；Teams service `--codex-arg` settings 不会自动到达
  remote beacon workers。生成的 templates 会通过 worker `--codex-path` 传递
  `CXP_BEACON_CODEX_BIN`。

Active Teams helper owner 会周期性 query existing provider jobs，把 scheduler state 投影回
beacon machines/jobs，并 renew provider deadline 接近的 allocations。Provider calls 永远不会
在持有 beacon state lock 时执行；renewal results 只有在 provider job id 和 renew epoch 仍然
匹配时才会应用。Helper drain 期间，renewal controller 只保护 job 可能已经 started 的
allocations；它不会为 pre-start turns 创建新的 scheduler work。Release/cancel intents 是
durable 的，因此如果早期路径在 provider adapter 可用前记录了 intent，active owner 会在
reconcile 时重试。同一个 reconcile pass 也会 drain 无 chat/job demand 的 idle workers，避免
shared 和 exclusive workers 在 demand 消失后永远保持 accepting。

在 allocated worker 内，scheduler job 应注册到 managed allocation，并等待 Teams turn 入队：

```bash
codex-proxy beacon worker run-once --allocation <request-id> --wait 30m
```

当未提供 `--provider-job` 时，worker 会从 `SLURM_JOB_ID` 或 `LSB_JOBID` 派生 scheduler
job id，注册 accepting machine，claim 一个 queued job，用 snapshotted prompt 和 workspace
运行 Codex，然后把 fenced terminal result 写回 shared beacon state。Teams 会等待 allocation
和 worker terminal path，而不是过早失败或运行 local Codex。

对于可复用 allocations，改运行 long-lived worker:

```bash
codex-proxy beacon worker serve --allocation <request-id> --idle-timeout 30m
```

`serve` 会记录 worker heartbeats，在 accepting jobs 前运行 worker doctor，存储 bootstrap
diagnostics，例如 node list、stdout/stderr paths、shared beacon store path，以及 `codex`/`cxp`
paths，然后在退出时 drain machine。Controller 或 cron job 可用以下命令 reconcile stale state:

```bash
codex-proxy beacon allocation reconcile-all
```

具备 scheduler 的 CI 可以通过 `CODEX_HELPER_BEACON_LIVE=1`、
`CODEX_HELPER_BEACON_LIVE_PROVIDER=slurm|lsf` 和匹配的
`CODEX_HELPER_BEACON_*_QUERY/SUBMIT/CANCEL` commands opt in 真实 adapter path。Live test
会通过 profile-stored adapter snapshot submit，并在 cleanup 中 cancel provider job。

从 active Codex turn 内发起 beacon switch 时，优先使用 deferred form，让当前 answer 留在
现有 target，后续 turns 使用新 profile:

```bash
codex-proxy beacon switch-profile gpu --session <session-id> --after-current-turn
```

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
skills 目录。该 skill 包含本地命令地图，以及 beacon profile switching、Teams helper
restarts 等 disruptive operations 的 safe handoff rules。它作为 built-in skill 管理，不是
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
这样 network timeout 或 Windows service repair 问题不会中断正在运行的 helper。使用 Teams
control chat 中的 `helper restart now` 来运行已安装 helper update。

Teams background mode 也会每 30 分钟检查 `codex-helper` GitHub Releases，并在当前
Teams/Codex work drain 后静默应用 eligible helper updates。Background helper auto-update 使用
正在运行的 stable helper path（当它是 runnable helper binary 时），然后在 restart bridge 前
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

## 长期运行实例（可选）

启动可复用 daemon instance:

```bash
codex-proxy proxy start [profile]
codex-proxy proxy list
```

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
