# Legacy Codex 会话启动前迁移 Checklist

## 目标

- [x] 只修改 `codex-helper`，不修改或推送 Codex 源码。
- [x] 解决 `cxp tui`/历史会话打开时，legacy rollout 通过 app-server stdout 返回完整历史而触发 CXP 16 MiB 单行限制的问题。
- [x] 保持用户操作和 TUI 参数不变；迁移隐藏在 broker 启动前。
- [x] 保持原 thread ID，不创建新的 Codex thread、turn 或 Teams 会话。
- [x] 迁移失败时不启动 remote broker，避免先进入会话再退化到危险的全量历史路径。

## 约束与边界

- [x] 只针对已经明确知道 canonical thread ID 的 broker-backed TUI resume 路径：历史选择器/`history open`，以及显式 `resume <canonical-thread-id>`。
- [x] 新会话、preview/list、fork、Teams runner、AppServerRunner/exec、`--last`、名称解析和无法确定唯一 thread ID 的路径不调用迁移。
- [x] 不改 remote broker 的 scanner、不实现 CXP 自己的 JSONL reducer/分页协议、不改 Teams service、不改 Codex。
- [x] 使用官方 Codex `migrate-rollouts --apply --thread <id> --json`，以官方锁、staging、journal、atomic publish 和 canonicalization 处理迁移。
- [x] 只有报告中目标 rollout 的状态为 `migrated`、`already_paginated` 或空文件的 `skipped_empty` 时才允许继续；缺少目标、busy、failed、未知状态或非法报告均阻断。
- [x] 迁移命令 stdout/stderr 只在内存中做有界捕获；不把迁移进度写入 TUI，也不增加新的用户交互。
- [x] 明确记录残余限制：单个历史 item 本身大于 broker 单行上限、损坏/非标准 rollout、过旧且不支持该官方子命令的 Codex 仍需升级或人工处理。

## 实现

- [x] 增加最小的 migration report 解析与目标状态校验；避免把“命令退出 0”误判为迁移成功。
- [x] 增加可取消、非 TTY、进程组受控的官方 migration 子进程执行；复用现有 CXP 环境、执行身份和 Codex runtime，不复用会混合 stdout/stderr 的 probe helper。
- [x] 在 broker 启动前、且仅在明确目标的 resume 路径调用 migration；保持 fork 的 `beforeBroker` 行为不变。
- [x] 让显式 broker-backed `resume <id>` 只在 ID 是唯一明确的 positional target 时触发；`--last`、名称/额外参数等路径保持原行为。
- [x] 将 migration error 作为启动门槛，确保失败时没有 broker、app-server 或 TUI 子进程遗留。

## 测试

- [x] report parser：成功、已分页、空文件、busy、failed、空 outcomes、未知 status/坏 JSON。
- [x] fake Codex process：验证 migration 参数精确为 `migrate-rollouts --apply --thread <id> --json`，使用与 app-server 一致的 Codex home/工作目录/执行身份，但不带 broker token 或 remote TUI SQLite 环境。
- [x] 历史 resume：验证 migration 发生在 broker/TUI 之前、TUI 参数完全不变、thread ID 不变。
- [x] new session 与 fork：验证不会误调用 migration；fork 仍先 fork 再 resume child。
- [x] migration 失败/报告不接受：验证 broker 和 TUI 均不会启动，并检查错误包含可诊断信息。
- [x] explicit `resume` 路径边界：只迁移明确 ID，不迁移 `--last`、名称解析或无 ID 的交互路径。
- [x] 进程取消/超时：验证 migration 子进程组被终止且不会留下子进程。
- [x] 运行仓库要求的聚焦测试、扩展测试、`go vet`/构建门禁（按环境可用性执行），并执行 `git diff --check`。

## 性能与数据安全审计

- [x] CXP 不读取或解析 rollout 文件本身，不做第二次 dry-run，不复制历史内容，不保留备份/缓存/新锁/新状态文件。
- [x] 大会话的额外工作仅是官方一次定向迁移；迁移吞吐和内存由 Codex 官方实现负责，CXP 只捕获有界诊断输出。
- [x] 检查错误路径、取消路径和 broker 启动前后的资源释放，确认不会造成重复 thread/turn、数据截断或锁死。
- [x] 检查最终 diff 只包含本 checklist、实现和必要测试，不混入主 worktree 或其他探索 worktree 的改动。

## 验收

- [ ] 使用支持 `migrate-rollouts` 的实际 Codex，在隔离 Codex home 中完成至少一个 legacy fixture 的端到端启动验证，并确认 app-server wire path 使用排除 turns/分页加载，而非 `thread/read(includeTurns=true)` 全量历史。
- [x] 记录测试结果、未能执行的环境门禁及残余限制。
- [x] 复核无用户可感知的新命令、新提示、新刷屏或操作步骤。

## 执行记录

- [x] `go test ./internal/cli -count=1`
- [x] `go test ./internal/codexhistory ./internal/codexrunner -count=1`
- [x] `go vet ./internal/cli ./internal/codexhistory ./internal/codexrunner`
- [x] `go test ./... -count=1`
- [x] `gofmt -l`（实现和测试文件无输出）以及 `git diff --check`
- [ ] 未执行实际 Codex 的 live TUI wire 验证：当前 worktree 只包含 CXP，且不能修改 Codex；fake Codex 测试已覆盖 CXP 的命令、门禁、顺序、环境隔离和取消行为。

## 已知限制

- [x] CXP 不读取 rollout 内容，也不自定义分页；大文件的迁移耗时、磁盘峰值和语义兼容性由官方 Codex migration 负责。
- [x] 单个 item 本身若仍超过 broker 单行限制、rollout 损坏/非标准、或 Codex 不支持 `migrate-rollouts`，会在 broker 启动前失败，而不是静默走旧的全量历史路径。
- [x] 迁移是官方语义 canonicalization，不承诺 legacy 文件字节级不变；目标 thread ID 保持不变，也不会创建新的 thread/turn/Teams 会话。
