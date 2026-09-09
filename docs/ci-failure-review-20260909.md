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
