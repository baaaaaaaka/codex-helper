# CI 失败复盘与执行记录（2026-09-09）

本次修复基于远端最新 `main`（`db363dcdfa5317471891ca75b676a24cf589c0f4`）在独立工作区执行。结论是：红灯并非单个测试断言偶发失败，至少包含一个生产状态竞态、一个测试夹具发布协议错误，以及 CI 编排和失败诊断的问题。只降低并发或重复重跑不能保证解决。

## 已确认的根因

### 轮询终态 CAS 竞态

Teams poll 在 handler 返回后会刷新一次 `PollRevision`，然后用该修订号提交终态。异步 final-answer 投递可以在刷新之后、终态 CAS 之前更新调度；该更新保留同一个 attempt，并在同一行把 `Attempt.ExpectedPollRevision` 更新为新值。旧实现因此拒绝终态提交，随后 defer cleanup 也用旧修订号拒绝，pending page 和 attempt 会保留到 TTL，下一轮无法继续。

这不是通过放宽 watchdog 得出的推测：在独立快照中把这段窗口扩大后，分页 backlog 会稳定停在部分完成状态；恢复原代码后对照通过。修复在 store 的原子 mutation 内只允许同一未过期 capability 吸收这种自洽的修订推进，owner、process incarnation、lease、schedule、frontier 和 receipt 检查仍然严格执行。ID-only 旧接口不获得该能力。

### Windows 进程就绪夹具

Windows 子进程原先直接创建并写入 ready 文件，父进程一看到文件就读取 PID。文件可见与内容完整不是同一个事件，正好会产生 `strconv.Atoi` 解析空字符串。现在所有平台的 process-identity 子进程都先写同目录临时文件、同步并关闭，再用 rename 发布；父进程只会看到完整 marker。

## 已执行的修改

- `internal/teams/store/chat_poll_frontier.go`：为 capability-scoped commit/abandon 增加受约束的 retained-revision adoption；普通 stale writer 和接管后的旧 owner 仍返回 no-op。
- `internal/teams/store/store_test.go`：JSON/SQLite 回归覆盖“调度保留 capability、终态使用旧 revision”以及 replacement owner 可以继续获取。
- `internal/teams/bridge.go`、`internal/teams/listener_recovery_regression_test.go`：加入仅测试使用的终态边界 barrier，实际制造“handler 读完 revision → scheduler boost → terminal CAS”的合法交错；保留原有 head/continuation、重开和 owner takeover 断言。
- `internal/helperruntime/process_identity_ready_test.go` 及三个平台夹具：原子 ready marker 和跨平台单元覆盖。
- `.github/workflows/ci.yml`：按 PR/branch 取消过时 run；Linux 隔离 frontier 测试的 coverage profile 合并回总 profile；删除失败后无关地重跑 `internal/cli` 并据此推断争用的误导诊断；保留首次失败并上传 full/race 日志；汇总 job 从三个完全相同的 Ubuntu matrix job 合并为一个 fail-closed job。
- Windows lifecycle job 现在显式运行 `TestRuntimeProcessIdentityWindows`，让 ready marker 和 Windows 进程元数据回归在原生 Windows runner 上有直接的失败信号；非 Linux full-suite 分片仍保留，避免减少覆盖。
- `scripts/tests/test_ci_targeted_shards.py`：为上述编排、coverage、诊断和去重约束增加静态护栏。

## 验证结果

- Go 1.25.11：store lifecycle、retained-capability CAS、frontier mismatch 全部通过，JSON/SQLite 均覆盖。
- Go 1.25.11：`go test -timeout=20m -parallel=16 -count=1 ./...` 全量 normal suite 通过。
- Go 1.25.11：`go test -race ./internal/teams -count=1 -timeout=30m` 全量 Teams race suite 通过。
- Go 1.25.11：终态 barrier 恢复测试 normal 通过 3 次，`-race` 通过 3 次。
- process-identity 原子 marker 测试通过；Windows 和 Darwin amd64 test binary 交叉编译通过。
- `test_ci_targeted_shards.py`（16 项）和 `test_ci_helper_scripts.py`（9 项，2 项按环境跳过）通过。
- workflow YAML 可解析，`git diff --check` 通过。

Windows 原生 runner 和远端完整矩阵尚未在本地执行，因此不能把本地结果表述成跨平台 CI 已经全部通过。提交后应以同一 commit 的首次矩阵结果验收；若仍有红灯，按上传日志区分 admission、durable terminal、worker exit、outbox publication 和外部依赖，不用自动重跑覆盖首次证据。

## 提交后 CI 复盘与补强

首个远端矩阵继续暴露了两个此前没有被本地 Linux 测试覆盖的边界：Windows targeted shard 在发布真实固定 FFmpeg 目录时遇到 `Access is denied`，Linux race shard 的迁移进程组回归在普通包并发池中报告子进程仍存活。前者是 Windows `MoveFileEx` 在目录仍被短暂占用时的实际重命名竞态；后者的测试只保存 PID，且与大量其他子进程共享 runner，不能区分原始进程和同 PID 的后续进程，也让有限 liveness 观察受到无关进程压力影响。

后续修改保持生产和测试边界都严格：

- `internal/teams/asr_managed.go` 的默认目录发布改用现有 durable replacement（含 Windows 重试/替换语义），并保留可注入 seam；新增测试确认真实发布路径使用该替换。
- `internal/cli/process_group_unix.go` 在初次进程组 `SIGINT` 返回错误时仍执行进程组 `SIGKILL`，同时保留 leader fallback，避免首个信号的 ESRCH/EPERM 竞态留下 descendant。
- 迁移进程测试在 Linux 记录并核对 `/proc` 启动时间，失败时输出启动时间、PGID 和命令行，避免 PID 复用造成误报，同时不放宽真实存活断言。
- `scripts/ci/run_full_go_test_shards.go` 将该测试从普通包池拆成独立且 host-exclusive 的 test process；普通 `internal/cli` 测试用 `-skip` 排除它，再由独立 job 执行一次，分片仍保持 exact-once。
- Linux coverage job 也跳过后单独执行该进程树测试，并把独立 profile 合并回 `coverage.out`；独立日志纳入失败 artifact，首次失败证据不被重跑覆盖。

这些调整没有删除测试、降低 timeout 或把失败改成 skip；它们分别修复了真实 Windows 发布竞态、补足 Unix 清理兜底，并使测试只观察自己创建的进程且在可控的调度边界运行。

## 验收边界

测试覆盖没有删除：大套件的精确 test-name 分片和 race 参数保留，隔离 frontier 测试补回 coverage；汇总 job 只减少重复观察，不减少依赖的实际执行。修复保证的是已定位竞态和夹具协议不再把合法时序误判为失败；未知失败仍需按日志建立新的回归和修复。

## 本轮根因方案执行（2026-09-09）

上一轮修复之后，最新红灯仍落在 `TestStoreOwnerBoundOutboxAdmissionRejectsStaleOwnerAcrossBackends/sqlite` 的 10 秒 watchdog 内，栈顶是 Windows `FlushFileBuffers`。这说明 owner admission 语义被一个不相关的 legacy-to-SQLite 迁移夹具包住了；迁移的持久化边界和 owner CAS 边界必须分别验收。

本轮把该语义回归改为当前 schema 的原生 SQLite fixture，仍通过真实 `Store`、SQLite 连接、事务和 JSON/SQLite 两个 backend 子测试；迁移、指针发布和 durability 继续由专门的 migration/reopen 测试覆盖。SQLite schema fixture 的 DDL 也在一个事务内创建，减少 Windows 上无意义的逐条 flush。恢复 manifest 升级为 version 2，为每个条目声明 `pure_cpu`、`listener_async`、`sqlite_fsync` 或 `host_exclusive`，runner 按类别限制同一 hosted runner 上的并发；Windows 仍按独立 partition 串行执行。

runner 为每个 manifest test 生成独立 JSONL phase trace 和汇总报告，记录启动、fixture、owner admission、listener stop 等已埋点事件。诊断文件缺失或损坏会进入报告，但不会把语义通过改成重试或跳过；因此首个失败仍是权威结果，同时可以区分测试失败、夹具未就绪、子进程未退出和诊断路径本身异常。

本地验证包括完整 `go test ./... -count=1`、恢复 manifest selector、脚本和 workflow 静态检查，以及 owner-admission 回归 50 次重复；Linux 不能证明 Windows 的 `FlushFileBuffers` 行为，最终验收必须看同一提交的 Windows normal/race 首轮矩阵和保留的 phase artifact。若出现新的红灯，继续按报告建立对应回归，不通过增加重跑次数掩盖未知竞态。

PR #114 的首轮矩阵验证了这个验收边界：Windows Teams recovery normal 的两个 partition 都通过，包含此前失败的 owner-admission SQLite 条目；同一首轮的 Windows full-suite partition 0 却在 `TestRunAppGatewayDaemonBoundsBackendRecoveryBeforeCooldown` 和 `TestRunAppGatewayDaemonRestartReusesStablePort` 中失败。它们不是 Teams 业务断言，而是 `go test` 多包调度把短 registration/cooldown/restart 观察与大量 Teams/store 子进程放在同一 hosted runner 上，导致临时 registration 文件仍被占用、daemon stop 观察超时。

这次首轮红灯没有通过重跑掩盖。`run_full_go_test_shards.go` 现将全部六个 App Gateway daemon timing fixtures 从普通 `internal/cli` 包池拆成独立 test process，并标为 host-exclusive；普通 CLI 测试仍 exact-once 执行，六个 fixture 的所有断言和 timeout 保持不变。该补强把同一类“跨包 runner 压力污染有限 liveness 观察”的根因纳入通用调度边界，随后必须重新跑完整首轮矩阵确认没有新的资源类别遗漏。

## 第二轮根因修复与验收

后续 Windows race artifact 进一步定位到另一条未被隔离解决的路径：listener 在第一次轮询前同步执行 legacy JSON→SQLite 迁移，迁移中的 SQLite schema 初始化逐条执行几十个 DDL/索引语句。Windows modernc SQLite 会为这些语句反复进入 `FlushFileBuffers`；因此测试只能看到 control-chat 的一次读取，60 秒后 watchdog 才能取消 listener。单纯把该测试标成 host-exclusive 只能去掉同机并发，不能消除迁移协议本身的长串 durable 边界。

本轮把生产迁移链路改成可取消的 context-aware 路径：基础表/索引、兼容列和最终索引分别以原子事务批量提交，schema trigger 重建也使用单次事务；临时库写入、校验读取和 WAL checkpoint 不再偷偷切回 `context.Background()`。这样仍保留每个 DDL、backfill、校验和 durable replace 的语义，但把 Windows 的逐条 flush 放大器移除。listener phase trace 增加 migration start/finish 和 startup-ready 事件，用于证明首次轮询前是否仍卡在迁移。

测试边界同步修正为生产 phase budget，继续由外层有限 progress watchdog 约束；manifest 为两个此前漏掉的 `TaskStartedPromptRace` 和 `MainLoopOutbox` 语义族声明独立调度，完整 runner 根据语义族自动生成隔离/host-exclusive job，避免新增同类回归再次落入普通包池。没有删除测试、跳过失败或把失败转成重试。

本地验收：`go test ./... -count=1`、关键 listener normal/race 重复、Store 全套、Windows Store 与 Darwin Teams 交叉编译、manifest/runner exact-once 计划、workflow/JSON/Python 静态检查均通过。修复前 PR #114 首轮仍保留为失败基线；最终是否根治必须由包含该生产迁移修复的同一提交完成 Windows normal/race、macOS 和 Ubuntu full/race 首轮矩阵来确认。

第二轮首轮矩阵（commit `2a84f04`）验证了 Windows 迁移修复：Windows normal/race 的八个 recovery job 全部通过，phase trace 中 legacy migration 为约 0.15–0.52 秒；此前 60 秒卡死的 JSON listener 路径已恢复。与此同时，Ubuntu race 的 full-suite partition 1 暴露出同一调度根因的另一个成员：`TestTeamsListenFalseSQLiteOperationalFloodPreservesHealthyOrdinaryChat` 被放进包含 254 个测试名的普通 shard，在 28 秒 phase 后被 context cancel，Graph 只有 control-chat 一次读取。该失败不是业务断言回归，而是尚未纳入隔离族的 continuous listener liveness fixture，说明只补两个具体测试仍会继续“打地鼠”。

因此 runner 的语义族规则扩大为所有 `TestTeamsListenFalse*` 以及 `TestTeamsMainLoopOutbox*`：每个测试仍执行一次、保留原始 race/timeout/断言，但在独立 test process 中运行，并在 full runner 的 host-exclusive phase 中避开同机 shard 压力。这样新增同类 listener 回归会自动获得相同资源边界；未知类别仍不会被静默跳过，首轮失败继续阻断验收。该补强之后需要重新跑完整首轮矩阵，不能用第二轮中已通过的 Windows 结果替代第三轮验收。

第三轮的恢复矩阵和 Linux/macOS full/race 分片均通过；Windows full-suite partition 0 又暴露了同一调度类别的遗漏：`TestAppServerProcessCloseTerminatesWindowsDescendants` 在普通 `internal/codexrunner` 包进程中启动 PowerShell 和 `ping.exe`，与 Teams 分片并发时在 10 秒内没有读到第一个 descendant PID 行。日志没有显示产品断言或进程树清理失败，而是 fixture readiness 超时。这是一个跨平台进程树生命周期族，不能靠增加全局重试或延长断言隐藏。

当前补强让普通包也经过候选测试名发现和语义族映射；`TestAppServerProcessCloseTerminates*`（Unix wrapper 和 Windows PowerShell 两个 build-tag 变体）会各自执行一次独立且 host-exclusive 的 test process，普通 `internal/codexrunner` 测试以精确 `-skip` 执行。这样把有限的 PID/readiness 和 tasklist 清理观察从全量包池的无关进程压力中隔离，同时保留原始 10 秒读取边界、5 秒 descendant 清理边界和全部断言。Linux 本地重复与 Windows amd64 交叉编译通过；修复提交后的完整 Windows 矩阵仍是最终验收条件。

## 第四轮失败复盘与修复

提交 `b44dbfd` 的矩阵（run `34368088055`）没有出现上一轮的进程树失败，但首轮仍发现四类同一系统问题：Windows recovery normal/race 的 SQLite 或长 continuation 条目在短 watchdog 内卡在 `FlushFileBuffers`；Windows full-suite 的多聊天 async cache 压力测试在普通 Teams shard 中等待 worker 超时；macOS full-suite 的 audience budget 测试在 40ms Graph admission budget 到期前没有进入真实 loopback server。随后 Ubuntu race 的完整 runner 又在已经独立的 migration process-group fixture 中报告子进程未在 2 秒内发布 marker。它们分别来自生产 schema 初始化、测试 Graph 传输、异步持久化压力、runner 资源边界和 fixture 启动预算，不能合并成一个“偶发断言”。

本轮做了以下修复：

- `ensureGlobalOutboundSQLite` 用一个 context-aware SQLite 事务创建两张表和索引。原先三条独立 DDL 会在 Windows modernc SQLite 上产生三次 durable flush；现在仍以一个原子 schema 边界提交，失败时整体回滚。
- `TestTeamsOwnershipStressDueHotChatsRotateBeyondCycleCapCI` 和 `TestTeamsWorkChatAudienceLookupUsesPollBudget` 的 fake Graph 改用进程内 `RoundTripper`。这两个断言观察的是 scheduler/admission 语义，不需要操作系统 loopback；请求路径、方法、上下文取消和响应解析仍经过 `GraphClient`，但不会把 TCP listener 启动排队误判成产品失败。
- full runner 自动隔离 `TestTeamsThirdPartyCacheStress*`、audience budget 测试，并把 legacy owner cross-backend fixture 从普通 store shard 拆出且标为 host-exclusive。每个测试仍只执行一次，普通包通过精确 `-skip` 排除，隔离 job 使用原来的 race、断言和测试内部 timeout。
- recovery manifest 将 legacy owner fixture 的最大运行窗口设为 30 秒并置于 exclusive phase，将 long continuation 条目标为 `sqlite_fsync` 并使用 30 秒窗口。该窗口仍由单测试进程的 Go watchdog 和外层 runtime grace 共同限制；它反映 Windows 文件系统真实 durable commit 成本，不会跳过 65 页 continuation 或把失败转成重试。
- `TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup` 的 shell-child readiness 等待从 2 秒改为 5 秒，并明确说明这是 race instrumentation 下的有限启动预算；进程组取消、leader fallback、descendant 存活和最终清理断言保持不变。该改动针对实际的 marker 发布竞态，不是把清理断言改成宽松等待。

本地 Linux 重复验证：hot-chat 20 次、long continuation 5 次、legacy owner JSON/SQLite 10 次、audience budget 20 次、migration process-group race 20 次全部通过；完整 `internal/teams` 与 `internal/teams/store` normal suite 通过，脚本静态测试 32 项通过，Teams/store Windows amd64 交叉编译通过。Linux 不能证明 Windows `FlushFileBuffers`，因此这些改动只有在同一提交的 Windows normal/race、macOS 和 Linux full/race 首轮矩阵全部通过后，才可以确认本轮类别已经覆盖；若仍有红灯，继续根据首次 artifact 建立新的资源类别或生产边界，不增加无条件重跑。

## 第五轮失败复盘与修复

提交 `7c8f292` 的首轮矩阵（run `34373744549`）只剩一个恢复测试红灯：Windows race partition 0 的 `TestTeamsListenFalsePollFrontierSurvivesStoreReopenAndOwnerTakeover/json` 在 41 秒后报告“first generation drained Graph continuation”超时，随后 TempDir 清理还遇到 SQLite 文件锁。phase trace 显示 listener 已在约 1 秒完成 migration 和 startup-ready，失败发生在真实两消息轮询、outbox 发布和 durable frontier drain；SQLite backend 随后的子测试通过。因而这不是 migration 未就绪或 Graph admission 未执行，而是该跨 backend fixture 仍使用 20 秒的短进度预算，恰好把 Windows durable-I/O 尾延误判成语义失败，并让清理在未结束的 worker 上进行。

本轮将该 helper 的有限进度预算改为已有的 `listenerRecoveryDurableIOProgressTimeout`（60 秒），并把 manifest 外层窗口从 90 秒扩为 180 秒以覆盖首代执行、frontier drain 和 reopen owner takeover 三个独立等待；测试仍由 Go watchdog 和 manifest runtime grace 共同限制。条目同时进入 manifest 的 exclusive phase，避免在 Unix runner 或未来并行度调整下与其他 SQLite/listener fixture 共享同机压力。JSON、SQLite、Graph continuation、exactly-once final、store reopen 和 owner takeover 断言全部保留，未增加重试、skip 或宽松成功条件。

该修复针对已观测的 Windows durable-I/O 尾延，并把内部等待与外层 watchdog 对齐；它不能由 Linux 单独证明。提交后必须重新查看同一提交的 Windows normal/race、macOS 和 Ubuntu full/race 首轮结果及 phase artifact；若仍出现红灯，按新的首次 trace 判断是另一种资源类别、生产边界还是诊断问题。

本轮新增的 manifest 元数据护栏要求该测试继续声明 JSON/SQLite 双 backend、`sqlite_fsync`、exclusive phase 和至少 180 秒外层窗口；本地脚本检查共 33 项（2 项按环境跳过）通过。

## 第六轮失败复盘与修复

提交 `22e5ca0` 的首轮矩阵（run `34376359413`）中，Teams recovery、listener frontier 和其他平台的 full/race job 均通过；唯一实际失败来自 Windows targeted shard 的 `Codex upgrade integration (system npm, Windows)`。第二次 npm 安装报告成功后，CXP 在显式 `--upgrade-codex-path C:\npm\prefix\codex.cmd` 的单次 `--version` 前置探测处收到 `codex-cli 0.153.4` 和 exit status 1。此前同一 Windows 镜像和版本在 run `34339737261`、`34338051354` 也出现过相同错误，而 run `34325457353` 成功；因此这是安装后外部 CLI 状态/启动时序的间歇性失败，不是某个 Teams 断言或一个固定坏版本可以解释的。

这里的逻辑问题是把“升级”当成了“先证明旧文件健康再允许修复”。显式路径只要存在且能识别出 npm source，就足以安全选择更新目标；旧文件可能正是需要被 npm 替换的损坏或短暂不可用状态。现在 root explicit upgrade 不再以旧文件的 `--version` 作为前置闸门，仍会拒绝不存在或无法识别 source 的路径；npm 完成后 `upgradeCodexInstalledWithOptions` 继续通过 `resolveUpgradedCodexPath` 对替换结果做功能探测，结果不健康仍返回 `codex upgrade finished but installed binary is not functional`。这保留了最终测试质量，同时让升级真正具备修复损坏安装的语义。

候选发现、安装重检和 root upgrade 结果探测另外采用 5 秒总上下文内最多三次、每次间隔 250ms 的有限重试。任何一次成功才算通过，三次失败仍返回最后一次原始错误；新增跨平台真实子进程 fixture 覆盖“首次失败后恢复”和“永久失败仍失败”，并把这些测试加入 Windows targeted root regression。该重试只吸收外部 CLI 发布后的短暂启动尾延，不改变断言、跳过测试或无限重试策略。

本地验证包含 CLI root/探测回归、完整 `internal/cli`、Windows amd64 交叉编译、workflow 静态检查和 `git diff --check`。旧提交的失败 job 在同一 run 的第二次尝试（job `102564026305`）成功，确认该错误确实具有间歇性；这次 rerun 只用于建立根因证据，不能替代新修复提交的首次结果。新提交仍需观察 Windows system npm job 和完整 normal/race 矩阵。Linux 测试不能证明 Windows npm/批处理 shim 和文件系统行为，最终验收继续以 Windows artifact 为准。

## 第七轮失败复盘与修复

提交 `bcb188b9` 的首轮矩阵（run `34382692434`）把前一轮的 Codex 修复路径验证为通过，但又暴露了四个可复现的边界。Windows targeted/full 日志中的探针测试失败不是探针重试次数错误：`.cmd` 夹具用 `echo` 写入 CRLF，断言把字节数误当成调用次数，三次调用被错误算成九次。Windows recovery 的 `TestTeamsListenFalsePolledTurnOutboxSurvivesReopen` 则在一个进程内顺序执行 JSON、SQLite 两个声明后端，20 秒总 watchdog 在 SQLite 子测试启动前耗尽。Windows full suite 的 ownership/Graph-429 压力回归被放进 252 个测试名的普通 Teams shard，有限的 worker/readiness 观察受同机进程调度压力影响；Linux 重复运行并没有复现。Ubuntu 两个安装步骤的首次失败来自不相关的 Google Chrome CDN `Hash Sum mismatch`，重试同一个 apt 源不会改变这个外部状态。

本轮修复保持测试语义和失败门槛：

- Windows 探针计数按 marker 出现次数统计，仍要求恰好三次有限探测；生产探测重试和永久失败断言没有放宽。
- recovery runner 将 `max_seconds` 明确定义为每个声明后端的有限预算。一个顶层测试若顺序覆盖两个后端，Go watchdog 和外层 watchdog 都按后端数量保留预算；manifest 仍解析 JSONL 并要求每个后端实际 `pass`，没有把超时改成通过或跳过。
- full runner 自动将 `TestTeamsOwnershipStress*` 和 `TestTeamsGraph429Stress*` 语义族拆成独立、host-exclusive 进程。所有测试名仍 exact-once 执行，普通 shard 用精确 `-skip`，原始 `-race`、断言和测试内部预算不变；新增同族回归也会自动获得该边界。
- 新增 `scripts/ci/apt_update.sh`。Ubuntu 主机安装前只在有限更新期间暂时移开明确不需要的 `google-chrome*.list/.sources`，保留 Ubuntu/Microsoft 源，严格检查不完整索引并清理 partial lists 后有限重试，最后无论成功或失败都恢复源文件。NFS smoke 和 release/targeted 主机安装步骤统一使用该 helper，包安装仍是有限重试；Docker 内独立的 Ubuntu glibc smoke 源保持原样。

本轮本地先验证了 helper 的源文件恢复、重试和官方源保留，CLI 探针回归 20 次、manifest/runner 单元、脚本和 workflow 静态检查通过；随后必须以新提交的 Windows normal/race、Windows full、Ubuntu targeted 和完整矩阵首轮结果验收。Linux 只能证明语义回归和 runner 计划，不能替代 Windows `.cmd`、SQLite `FlushFileBuffers` 或 hosted scheduler 的远端证据。

## 第八轮失败复盘与修复

提交 `7bb4b32` 的首轮矩阵（run `34388874839`）已经通过此前四类关键路径：Windows full 两个 partition、Windows recovery normal/race、Ubuntu full/race（除最后一个仍运行的 partition）以及 macOS full；Ubuntu race partition 1 最终唯一的实际测试失败是 `TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup`。它在子进程已经退出、`/proc/<pid>/stat` 条目消失后，仍被测试的 `proc.IsAlive` 判为存活；随后诊断中的 `start_err=open /proc/...: no such file or directory` 和 `pgid=-1` 证明这是 liveness 观测窗口竞态，而不是进程组清理真的失败。汇总 `Test` job 只是随该失败而失败。

本轮修复 `internal/proc.IsAlive` 的 Linux 实现：先保留 `kill(pid, 0)` 的权限/存在性检查，再只在 Linux 读取 procfs；如果 stat 条目在非原子检查间隙消失，明确按已结束处理；Darwin 继续使用原有无 procfs 的行为，格式异常或其他读取错误仍保持保守的存活结果。新增缺失 proc 条目的回归测试，并在 Linux race 下将进程组测试重复 100 次；完整 `go test ./... -count=1`、`internal/proc` normal/race 均通过。

这个改动没有缩短清理等待、删除 descendant 断言或把失败变成重试；真实存活进程和 PID 复用仍会被启动时间/存活检查捕获。下一轮必须重新观察同一完整矩阵，确认 procfs 观测修复不会在其他 Unix 进程清理路径产生副作用。

## 第九轮失败复盘与修复

合并提交 `a4a3b40575a689ad32c654a1cbea8191886b992d` 的完整矩阵 run `34396041530` 暴露了一个新的调度层问题：59 个作业中 58 个作业本身通过，唯一失败是 Ubuntu race 的 `TestTeamsListenFalseRecoversExpiredAmbiguousOutboxWithoutPost`。失败诊断显示测试的 500ms phase 在 outbox 恢复期间超时，Graph GET 还没有开始，最终没有产生 POST；同一时间另一个 SQLite 压力测试进程正在同一 hosted runner 上运行。测试资源类别分别标记为 `listener_async` 和 `sqlite_fsync`，现有 manifest 限流器只分别限制每个类别，因此两个类别仍能并发，调度压力把一个有意较短的 phase budget 变成了环境相关的超时。

修复将 `listener_async` 与 `sqlite_fsync` 放入共享的 `host_io` lane：race 模式下两类测试共用一个 token，普通模式保留两个 token，Windows 仍由单 worker 串行执行。每个类别仍保留自己的吞吐上限，并以固定顺序取得多个 token，避免引入新的死锁。新增限流器测试明确验证 race 模式下 listener 持有 token 时 SQLite 不能启动。测试断言、Graph 精确 marker 校验、无重复 POST 约束和每个 manifest entry 的外部 watchdog 都保持不变。

本轮失败说明之前的“按资源类别分别限流”并没有真正隔离共享的 hosted runner 资源；修复后必须重新执行完整 Windows/macOS/Linux recovery 矩阵，并特别确认 Ubuntu race 两个分区和 Windows full/recovery 路径。

## 第十轮失败复盘与修复

提交 `287fe8a3` 的完整矩阵 run `34425115603` 验证了共享 `host_io` lane：上一轮失败的 Ubuntu race recovery partition 0、Windows recovery/full、macOS recovery 以及其他 58 个作业均通过。唯一实际失败转移到 Ubuntu race partition 1 的 `TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup`。失败日志再次给出 `kill(pid, 0)` 成功后，`/proc/<pid>/stat` 读取返回 `ENOENT`，并且 PGID/命令行也已经不存在。

这里剩下的是测试身份观察器自己的非原子窗口：`childIsOriginalProcess` 在 `proc.IsAlive` 与启动时间读取之间遇到进程退出时，把明确的 procfs `ENOENT` 仍按“保守存活”处理；这会在最终 deadline 检查中把已经结束的子进程误报为存活。现在只有明确的 `os.ErrNotExist` 被视为原始进程已结束，权限、格式或其他未知读取错误继续保守返回存活；进程组取消、leader fallback、descendant 清理和启动时间匹配断言都保持不变。

该修复不删除进程树断言，也不增加重试或放宽清理等待，而是把“PID 对应的 procfs 条目已消失”这个确定事实纳入身份检查。修复后必须重新执行完整矩阵并重点查看 Ubuntu race 两个分区，以确认本处与 `internal/proc.IsAlive` 的边界共同覆盖所有 liveness 观测窗口。

## 第十一轮失败复盘与修复

提交 `f2e324f` 的首轮矩阵（run `34427386261`）再次证明进程身份修复有效，但 Windows race partition 0 的 recovery job 出现了另一种宿主机尾延：`TestTeamsListenFalsePolledTurnOutboxSurvivesReopen/sqlite` 在 40 秒外层 watchdog 内卡在 modernc SQLite 的 `FlushFileBuffers`，没有到达 Graph poll/executor；同一作业随后报告的 recovery 断言是这个未结束进程的连带结果。失败堆栈位于 `updateChatPollSQLiteWithCapability` 的事务提交，不是测试把超时判成成功，也不是 listener 的语义断言失败。此前相同提交族的 Windows job 曾在 4 秒左右通过该 SQLite 子测试，说明 C: 用户临时目录的文件系统/扫描尾延会改变同一持久化提交的调度。

恢复步骤现在把 `TMPDIR`、`TMP` 和 `TEMP` 统一指向每个 GitHub job 自带的 `${{ runner.temp }}`。在 Windows 上，Go 默认会把 `t.TempDir` 和编译临时文件放在用户 profile 的 C: 临时目录；显式使用 runner 的隔离临时卷后，SQLite WAL、phase trace 和 test binary scratch 不再与 profile/antivirus 扫描共享路径。这个改动只改变 fixture 的文件位置，仍执行真实 file-backed SQLite、WAL/synchronous 配置、store reopen、Graph poll、Codex executor 和 exactly-once outbox 断言；外层 40 秒 watchdog、各测试内部预算和失败门槛保持不变。它针对的是已观察到的宿主机 I/O 根因，没有加入重试、skip 或降低 durability。

该提交还需要重新观察 Windows normal/race recovery、Windows full、Ubuntu/macOS full/race 的完整首轮结果。Linux 可以验证 workflow 语法和测试语义，但不能替代 Windows `FlushFileBuffers` 路径；只有同一修复提交在远端矩阵中稳定通过，才可继续合并到 main。
