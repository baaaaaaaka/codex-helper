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
