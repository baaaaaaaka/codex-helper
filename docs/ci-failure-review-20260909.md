# CI 失败复盘与修订执行计划（2026-09-09）

结论：不能把本轮 CI 失败统一解释为 runner 争用。已复现一个真实的 poll attempt 清理竞态，并定位到另一个 Windows fixture 的非原子 readiness 发布问题。应先修这些具体失败机制，再根据测量调整资源调度。覆盖清单、mutation 和汇总整理作为质量护栏，不代替根因修复。

## 范围、基线和证据

- 审查 2026-09-08 的最近 13 个 CI run：最新状态为 5 success、5 failure、3 cancelled。跨多个 commit，不能把比例当成同一版本的 flake 概率。
- 使用分页 API 取得每个 run 的全部 62 个 job。当前安装的 `gh run view --json jobs` 检查遗漏了部分失败 job；不能据其不完整结果判断“只有一个失败”。
- 最新主线 run [34223907120](https://github.com/baaaaaaaka/codex-helper/actions/runs/34223907120) 的 success 来自 attempt 2。attempt 1 失败于 Windows `TestRuntimeProcessIdentityWindows`。按首次 attempt，10 个非取消 run 中实际是 6 failure、4 success。取消 run 也保留已产生的真实失败，不能当作成功或丢弃其证据。
- 本地已提交 HEAD 为 `1e781aba3e8b25c9c0fe84ba81634d0b515e14b9`，最新主线为 `58812b27ed2e2b01f6f53c558f4ca3ab43ea21ab`。两者 tree 均为 `7f81ae90edd435bcdc8fff7c34edac7a9c5daca9`。本地实验使用 `git archive HEAD` 的独立快照，不含用户未提交修改。
- 最新主线相对上一失败主线 `242152235cae2435f8fb689a602d638f5d584f3b`，仅修改 cooperative shutdown 的 admission 等待预算及对应 manifest 预算。其他失败没有对应代码修复。
- 原始 job JSON、失败日志、调度实验 patch 和测试输出保存在 `/home/baka/.local/state/ci-plan-audit.jYBeQA/`。仅使用假 Graph、假 executor 和临时状态；未操作正在运行的 Teams helper。

## 实际失败簇

| run | 底层失败 | 证据与解释 |
| --- | --- | --- |
| [34196926025](https://github.com/baaaaaaaka/codex-helper/actions/runs/34196926025) | Windows full：Graph429 自动恢复；Windows recovery/race：malformed poll、phase timeout | 前者以观察时刻判断短 Retry-After；后两者涉及有限阶段等待。已有后续预算/断言修订，不能再作为尚未修复的同一版本证据。 |
| [34201828173](https://github.com/baaaaaaaka/codex-helper/actions/runs/34201828173) | Windows recovery/race：owner loss | executor 未 dispatch，Graph reads=1；不是 stale-owner 断言失败。 |
| [34207730778](https://github.com/baaaaaaaka/codex-helper/actions/runs/34207730778) | Windows recovery/race：shutdown followup | executor 未 dispatch，Graph reads=0；未到达目标 shutdown 场景。 |
| [34213396710](https://github.com/baaaaaaaka/codex-helper/actions/runs/34213396710) | Ubuntu race：SQLite backlog optional maintenance | broad shard 中 admission 等待失败；状态已有 running turn，poll 阶段约 6.1 秒。资源争用是候选原因，不是日志证明的结论。 |
| [34218346037](https://github.com/baaaaaaaka/codex-helper/actions/runs/34218346037) | macOS full：paged backlog | 11/24 完成；round 10–25 不再推进，PendingPage 和 Attempt 仍存在。 |
| 同上 | Windows recovery/race：cooperative shutdown | admission 未完成，Graph reads=1；后续只对此增加了等待预算。 |
| 同上 | Windows full：unrequested canceled execution、completed history final recovery | 前者 durable turn 已 interrupted，但等待 idle queue 超时；后者缺 recovered final。应分别观察 durable terminal、worker 退出和 outbox 发送，不能归为同一个 admission 问题。 |
| [34223907120 attempt 1](https://github.com/baaaaaaaka/codex-helper/actions/runs/34223907120/attempts/1) | Windows full：runtime process identity | `strconv.Atoi: parsing "": invalid syntax`，第二次 attempt 才通过。 |

Windows recovery runner 已经配置一个 worker。继续降低该通道的测试间并发无法修复其失败。进程隔离也不等于没有测试内部竞争。

## 已完成的因果实验

目标：`TestTeamsOwnershipStressPagedBacklogAfterServiceOutageCI`。

1. 未修改快照，Linux / Go 1.26.3 / normal：20 次通过，测试阶段 61.164 秒。这只是初始本地观察。
2. 未修改快照，切换为与 CI 一致的 Go 1.25.11 / normal：10 次通过，测试阶段 45.158 秒。
3. 仅在临时副本中，在 `refreshChatPollAttemptRevision` 返回之后、terminal CAS 之前插入 `time.Sleep(100 * time.Millisecond)`。不修改断言、消息数量、worker 数、store 状态或 CAS 条件。Go 1.25.11 下 5/5 次语义失败，累计约 0.996 秒；每次仅 1/24 完成，后续轮次不再推进。不是测试 watchdog 超时。
4. 增加一次只读诊断后再执行一次，确认 cleanup 输入 `expectedRevision=4`，实际 row revision=5，attempt 的 ExpectedPollRevision=5；ID、owner 和 process incarnation 与本次 capability 相同，尚未过期。Abandon 返回未释放、无错误，attempt 留在状态中。
5. 保存调度 patch 后恢复副本原始源码，Go 1.25.11 再执行 3 次全部通过，10.734 秒。形成原版通过 → 调度窗口放大后失败 → 恢复原版通过的对照；并未把实验性 sleep 写入产品源码。

机制：异步完成更新了同一个 capability 允许更新的 revision；本轮在最后一次刷新后发生调度切换，terminal commit 使用过期版本被拒绝，defer 清理继续使用过期版本再次被拒绝。后续 Begin 被遗留 attempt 阻挡，直到其过期或另有恢复。不能把降低出现这种时序的概率视为修复。

这是实际可复现的竞态机制，与 macOS 现场的 pending page + attempt + 无后续进展一致；仍不能声称已从旧日志证明 macOS 那一次的精确交错。实验也未证明永久死锁，已观察的是 attempt 有效期内的进展中断。

100ms sleep 只用于放大调度窗口，不应进入正式回归。正式测试应使用 barrier 控制“读取 revision → 异步 schedule 更新 → terminal CAS”，保留竞争和 owner fencing。

## Windows readiness 的具体问题

`internal/helperruntime/process_identity_windows_test.go` 中，子进程直接 `os.WriteFile(ready, PID)`；父进程将第一次成功 ReadFile 视为完整发布并立即 Atoi。创建文件与写完内容并不原子，父进程可能读到空文件，正好对应 attempt 1 的错误。

应使用完整写入并关闭后的原子发布，或具有完整消息边界的 readiness 握手。错误诊断需区分未就绪、子进程退出、无效完整内容，并保留有限 deadline。原有真实进程 image/command-line 身份断言不能替换成 mock。此机制由源码和日志支持，本机尚未执行 Windows 复现实验；不能声称已经验证 Windows 修复。

## 两版计划的复盘

原计划优点：资源隔离、避免固定 sleep、保留 race/stress、覆盖守恒的方向正确。缺点：把争用过早视为主要根因；没有计入重跑 attempt；把“总结更清晰”和“底层失败减少”混在一起；准备将全面串行和测试去重提前实施。

改进版优点：先聚类、固定版本对照、按根因修复。仍需补充：必须分页收集所有 jobs 和历史 attempts；已取消 run 的既有失败也要保存；独立运行通过不能排除产品竞态；阶段失败必须区分 admission、durable terminal、worker completion、outbox effect。

本轮实证已否定“统一降并发足以解决”及“最新绿色代表修好了”的判断。

## 对“整个测试思路是否有问题”的检验

有系统性设计风险的证据，但不是所有测试都无效，也不是所有红灯都由测试造成。已看到的共同风险是：把异步系统的中间状态当作完成协议，以隐含时间关系替代明确前置条件，再通过放宽外层等待补偿。

- readiness 文件已出现，不等于 PID 消息写完；这是本轮 Windows 失败的具体候选机制。
- turn 已 terminal、async worker 已退出、outbox 已发布是不同状态。共用 `waitForBridgeAsyncTurns` 只等待其 WaitGroup，不应被调用者无条件理解为所有外部效果均完成。应逐一核对调用者真正需要的完成条件，不能仅凭 helper 名称推断错误。
- `startListenerRecovery` 返回只表示已启动 goroutine，不表示 listener 已完成初始化或 admission。相关测试必须明确等待目标阶段，及早报告 listener 退出。
- `listenerRecoveryBaseOptions` 当前使用 500ms PhaseBudget、100ms PollWorkerBudget、1s OwnerStaleAfter。扩大外层等待到 60/90 秒并不会扩大内部单次工作预算。若持久化路径超过内部预算，会反复失败；应审查这些时间比例是否服务于该测试的目标，而非一律沿用加速配置。这里是需测量验证的风险，尚未证明是每个 Windows 超时的原因。
- 固定轮数适合验证明确的“每轮至少推进多少”契约；若推进受真实时钟、backoff 或异步完成控制，仅重复调用若干次并不能代表等待了有效调度机会。但本轮 orphan attempt 不能靠增加轮数消除，应保留它抓到的产品缺陷。

在 P0 中加入共用 harness 审查：列出每个 helper 的启动、就绪、完成、退出语义及其负责等待的任务集合；从实际失败调用者开始修，避免直接重写整个测试体系。

逐步将测试职责分清：状态转换与不变量用受控输入/时钟；并发边界用 barrier 明确制造合法交错；真实 listener/磁盘/进程集成保留必要的端到端场景和有限 wall-clock watchdog。迁移前后保留行为与平台映射，不能以“已有单元测试”删除真实集成验证。

假设的判定标准：如果改正共用完成协议后，多组原本无关的 fixture 在原负载下同时稳定，支持系统性测试设计问题；如果 barrier 在合法交错下仍使生产状态失去进展，则属于产品问题，应修产品并保留测试。两种结果可能同时存在，本轮已经有这种迹象。

## 修订后的实施顺序与验收门槛

### P0：先修复已定位机制

1. **Poll attempt 竞态**：先添加 barrier 控制的回归，证明旧代码出现上述 orphan attempt。修复应在 store 原子操作内验证当前 capability，或执行有界、重新验证 identity/frontier/lease 的恢复；不得使用无条件清空 attempt、忽略 revision、无限重试，也不得重跑可能产生副作用的 handler。
   - 验收：原有 24 条 backlog 全部完成，每条恰好一次；异步 completion/schedule 与 terminal CAS 相交时仍能继续；JSON/SQLite、normal/race 都覆盖。
   - 反例：owner takeover、不同 process incarnation、replacement attempt、frontier/receipt 变化时，旧执行者不能提交或清除新 attempt。
2. **Windows readiness**：原子发布/有界握手；验证部分发布、延迟启动、提前退出及正常进程身份。必须在 Windows runner 上实际执行，Linux 编译通过不够。
3. **阶段诊断**：对 Windows listener/canceled execution/history final 三类 fixture，记录启动、Graph admission、durable turn terminal、worker exit、outbox publication 的单调时间线。针对卡住阶段检查锁、重试门槛、同步和真实产品逻辑；已经加过预算的测试不能仅凭再扩大预算结案。

### P1：按证据调整调度

- 首先只调整 broad full/race 的外部并发，测试内部负载、worker 数和断言不变。Windows recovery 已串行，不做无效重复改造。
- 对照原并发与低并发，记录 test/job 耗时和首次失败；完整 suite 中仍验证 P0 回归，防止局部修复只在独立运行有效。
- 需要更多 runner 时拆分整个测试，避免串行化后 job 总预算超限；不同时去重或迁移 gate。

### P2：质量护栏与报告

- 迁移清单单位为 test × OS × mode × backend × required env。预期选择与实际 run/pass/skip 事件对账；覆盖率补采并合并，不能仅比较测试数。
- 保留现有 required matrix 和 stress 参数；本轮不迁移到 nightly，不用自动 retry 改写首次失败。
- summary 合并及 PR 旧运行取消可随后做；切换 summary 需同步 required checks，并对必需 job 的 failure/cancel/意外 skip fail closed。
- 依赖获取的重试与产品断言分离。当前取样中的底层失败集中于测试行为，不支持优先把 ASR/npm/distro 移出门槛。

### 完成标准

- 每个活跃失败簇都有回归、对应修复和实际 CI 入口；未知失败不得归类为已解决。
- P0 的旧版本失败、新版本通过，并且安全性反例仍被拒绝。调度暂停实验只证明机制，不能代替正式回归和跨平台验证。
- 同一候选 commit 的原矩阵首次结果用于验收；所有 rerun 独立记录。观测样本量根据目标失败率预先约定，不采用“连续三次绿就完成”。即使独立同分布假设成立，30 次零失败的一侧 95% 上界仍约为 9.5%，不能据此宣称低于 1%；现实 runner 样本还可能相关。
- 无新增总 job timeout、无覆盖组合缺失、无断言/压力参数削弱。
- 当前并未达到“CI 全部修好”的完成标准：Windows 原机验证、正式竞态修复与跨平台完整 CI 尚待执行。

## 本轮实际变更和验证

- `.github/workflows/ci.yml`：删除 full-suite 失败后固定重跑无关 `internal/cli` 并推断环境争用的诊断分支。原测试执行和失败退出码不变；不减少正常 CI 覆盖。
- 已通过现有 `test_ci_targeted_shards.py` 的 15 项检查及该 workflow 的 `git diff --check`。
- 生产代码未修改，用户已有修改未覆盖，未提交或发布变更。
- API/本地实验没有遇到 sandbox 拒绝；安装的 gh 不支持 `--slurp`，改用已支持的分页 JSONL 收集方式后成功。这是工具版本兼容问题，不是 CI 根因。
