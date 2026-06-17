# codex-proxy / cxp 中文快速入口

> 英文 [README.md](README.md) 仍然是默认和完整文档。本页是中文快速入口，
> 重点覆盖首次安装、常用命令和容易混淆的 Teams/Beacon 关系。

`codex-proxy` 可以用直接连接或 SSH 代理方式运行 `codex`，也可以用 `cxp`
这个短命令打开本地 Codex 历史、启动桌面 App、配置模型、管理 Teams helper。

## 快速开始

### 1. 安装

Linux / macOS:

```bash
sh -c 'url="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.sh"; if command -v curl >/dev/null 2>&1; then curl -fsSL "$url" | sh; elif command -v wget >/dev/null 2>&1; then wget -qO- "$url" | sh; else echo "need curl or wget" >&2; exit 1; fi'
```

Windows PowerShell:

```powershell
$ErrorActionPreference="Stop"; $u="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1"; $p=Join-Path $env:TEMP "codex-proxy-install.ps1"; try { Invoke-WebRequest -UseBasicParsing $u -OutFile $p; Unblock-File -LiteralPath $p; & powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File $p; if ($LASTEXITCODE -ne 0) { throw "installer exited with code $LASTEXITCODE" } } finally { Remove-Item -Force $p -ErrorAction SilentlyContinue }
```

Windows 命令会先把安装脚本下载到临时文件，再用当前进程作用域的
`RemoteSigned` 策略执行本地临时文件。它不会使用 `iwr ... | iex` 或
`Invoke-Expression` 直接执行远程文本。

### 2. 运行

```bash
cxp
# 或
codex-proxy
```

首次运行时，如果还没有保存直接连接或 SSH 代理偏好，程序会询问是否配置
SSH 代理。选择 `no` 表示直接连接；选择 `yes` 会继续询问 SSH 主机、端口和用户。

## 常用命令

| 命令 | 用途 |
|------|------|
| `cxp` | 打开本地 Codex 历史 TUI |
| `cxp run -- <cmd> [args...]` | 用当前直接/代理模式运行命令 |
| `cxp run --yolo -- codex` | 本次启动 Codex 时开启 YOLO mode |
| `cxp run --model-profile <name> -- codex` | 用指定模型配置启动 Codex |
| `cxp model list` | 查看内置和已配置的模型选择 |
| `cxp model setup <model>` | 配置 `deepseek`、`kimi`、`qwen` 等模型 |
| `cxp proxy reset` | 清除代理设置，下次启动重新询问 |
| `cxp app` | 启动 Codex 桌面 App |
| `cxp teams status` | 查看 Teams helper 状态 |
| `cxp upgrade` | 从 GitHub Releases 更新 `codex-proxy` / `cxp` |

更多完整命令请看英文 [README.md](README.md#command-reference)。

## Teams helper

Teams helper 可以让你在 Microsoft Teams 里驱动本机上的 Codex。推荐先安装稳定版
`cxp`，再按照英文 README 的 [Teams helper](README.md#teams-helper) 章节运行交互式
bootstrap 脚本。

常用 Teams 控制聊天命令：

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
helper update now
helper restart now
```

常用 Work chat 命令：

```text
helper status
helper retry last
helper cancel running
helper file relative/path.ext
model status
model switch deepseek
model fork deepseek
```

普通安装用户更新或本地修复 helper 后，通常在 Teams 控制聊天里发送
`helper restart now`。`helper reload now` 只适用于 source-checkout 开发环境，
也就是 helper 能访问本地 `codex-helper` 源码树的情况。

## 进阶功能

- 模型配置和 YOLO mode: 见英文 [Model selection and YOLO mode](README.md#model-selection-and-yolo-mode)。
- 本地历史和 TUI: 见英文 [Local Codex history and TUI](README.md#local-codex-history-and-tui)。
- Codex 桌面 App: 见英文 [Codex desktop app](README.md#codex-desktop-app)。
- Beacon execution profiles: 这是 Teams helper 的进阶执行目标功能。先理解 Teams
  helper 和 Work chat，再看英文 [Beacon execution profiles](README.md#beacon-execution-profiles)。
- 内置 `cxp` skill: 见英文 [Built-in cxp skill](README.md#built-in-cxp-skill)。
- 升级: 见英文 [Upgrade](README.md#upgrade)。

## 更多文档

- [docs/README.md](docs/README.md): 按主题整理的文档入口。
- [docs/teams_source_deployment_guide.md](docs/teams_source_deployment_guide.md):
  source-checkout 部署和 Teams helper 排障。
- [docs/beacon_mode_plan.md](docs/beacon_mode_plan.md): Beacon 设计和命令语义。
