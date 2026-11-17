# Teams Codex goal：状态来源与停止恢复验证

日期：2026-11-17。本文记录隔离工作区的实现与验证，不代表已部署、已发布或云端 CI 已通过。

## 工作区与边界

- 上游基线：`ecd719f9`，从 `origin/main` fetch 后建立分支 `refactor/goal-authority-20261117`。
- 工作区：`/home/baka/.local/state/cxp-goal-mechanism-qiwpzlcw/worktree`。原工作区与正在运行的 Teams helper 保持独立。
- 原生 Codex 拥有 goal 的当前状态和续接调度。CXP 不新增执行调度器，也不直接修改原生 goal 数据库。
- 本地进程单元测试使用 fake agent/child。真实 CXP 与 Codex 协议测试均通过开发 skill 的临时 Docker harness，使用空白配置、模拟 Graph/Responses 和独立状态；未复制账户凭证。

## 实现契约

1. `goal.State` 保留原生状态、objective、token budget、usage 与时间字段。`goal.Snapshot.Known` 区分原生确认的无 goal 与尚未观察、请求失败或断线；不再用空字符串表示这两者。
2. 原生 RPC 响应在 reader 中更新当前快照，然后唤醒调用方。每个 thread 的 revision 使旧请求响应、超时和迟到查询不能覆盖更新的请求或 goal 通知。断线与协议损坏使观察失效，不把它们当作 goal 被清除。
3. JSONL 是历史证据。`turn_attribution.turn_trigger=goal` 只标记该 turn 的来源；旧 objective 只能显示为 unconfirmed，不能使 thread 变成 active。每个 live mirror message 使用同一原生快照，原生 clear 可以覆盖旧 rollout 中的 active。
4. 查询、暂停和取消中的状态核对不调用 `thread/resume`。只有显式激活 goal 才加载执行 thread；即使持久化状态已经 active，冷连接也会完成加载，并保留原有 budget/usage。模型切换也先读取当前原生 goal，避免未知快照绕过原有的 active-goal 限制。
5. `helper cancel` 的 ACK 与停止意图在同一个 SQLite 事务中提交。ACK 表示正在核对；原生确认之后才发送停止结果；该结果先入队，再中断选中的 helper execution，使取消消息顺序固定。请求失败时保留 pending，暂缓该会话的新启动和 steer，用户可重试 cancel、pause 或用更新的显式控制改变意图。
6. 每会话的 goal mutation 串行；紧急 cancel 仍能越过其他慢命令。持久化 inbound watermark 防止较旧的排队激活在取消后执行。恢复只重试 pending stop；不重放已经完成的执行请求。
7. 取消保存选中的 helper turn 和独立 native turn ID。恢复不把取消重新绑定到后来启动的 native turn。恢复 worker 不阻塞整个 helper 的启动，也不阻塞其他会话。
8. 明确的原生“不支持／goals 已关闭”错误允许普通 cancel 继续；超时、writer 冲突和数据库错误不走此分支。不支持也不会被缓存成“已确认无 goal”。

状态展示保留 `active / paused / blocked / usageLimited / budgetLimited / complete`。普通消息没有 goal 证据时不增加 unknown 注释；goal-triggered 输出缺少当前确认时明确标记 unconfirmed。`helper status` 明确显示当前观察状态。

## 持久化与性能

schema 从 10 升到 11，新增 `goal_controls`：每会话只有一行最新控制意图，以及只含 pending 的索引。普通启动与 steer 使用主键查询；启动恢复只查询 pending 索引。不扫描或重写 JSON/JSONL，不存储第二份原生 goal 当前状态。

`TestGoalControlsNoOpWritesAndBoundedRows` 验证重复读取、相同意图、重复完成不增加 SQLite `total_changes()`，1000 次新控制仍只有一行。既有 migration 测试覆盖旧 schema 升级后历史消息与未知投递证据不变。

1000 条 progress 的 `BenchmarkTrackerProgressHistory`，同机对照上游基线与工作区，`-benchmem -benchtime=500ms -count=3`：中位数约 3.186 ms/op 与 3.184 ms/op；两者均为 29908 allocs/op，约 1.879 MB/op。新增 turn 来源标记没有随历史规模增长的存储或读放大。

## Validation

测试对象为上述分支的未提交工作区。测试日志位于工作区同级的 `scratch/`，不会进入提交。

- Core 全量：`env -u http_proxy -u https_proxy -u all_proxy go test -race -p=2 ./internal/core/... -count=1 -timeout=8m` 全部通过。后续冷连接显式激活与取消消息顺序的修改，另跑 engine goal/cancel 与 codexagent 的定向 race；最终 engine 检查通过（23.528s）。store 查询计划、bounded rows/no-op writes、transcript attribution 与 fakecodex 的 race 也通过。
- 黑盒全量：在隔离 Docker 中依次执行 `bash scripts/ci/run_e2e_shard.sh 0`、`1`、`2`，实际执行 188 个场景。初次 179 通过，8 个消息快照与 1 个 schema 版本断言需要随契约更新；其余功能检查与全部 invariant 通过。逐项审阅后更新断言，最终在同一隔离 harness 中重新执行 11 个相关场景，11/11 通过（含原生激活的最终修改）。提交前在最终代码上重新运行全部三个 CI 分片，188/188 场景通过，全部 golden 与 invariant 通过。
- 消息快照变更对应明确的用户行为：历史 goal 不能标记当前 active；`helper status` 的未知状态明确展示；cancel 的检查 ACK 与确认结果分开发送。schema 黑盒断言更新到 11，并额外验证新表存在，原有恢复记录保留检查继续通过。effort 测试只放宽临时路径导致的 prompt footer 截断归一化，完整用户回复仍由独立精确断言核对。
- 最终黑盒选择器：`^Test(GoalCommands|EffortCommands|WorkChatCommandForms|EntryTypedSQLiteUpgradePreservesAttemptedNotice|R20_R33_CancelRunningTurn|CancelCommands|SilenceHintsAndNoStallKill|R05b_UnconfirmedFinalRetained|UserLabel|GroupChatMentionsAndCoworkerCommands|ModelSwitchWhileBusy)$`，覆盖 commands、entry、turns、faults、edits、chats 六个 package，`-v -count=1 -parallel=8 -timeout=300s`。取消测试在发送下一条请求前等待确认；engine 的可控阻塞测试另行证明确认前不能启动或 steer。
- 原生版本契约：隔离 Docker 中对 Codex `0.166.0`、`0.167.1`、`0.169.0` 各执行 `go test -v -count=1 -timeout=120s ./internal/core/codex ./internal/core/codexagent -run '^TestRealCodexGoal'`，共 9/9 通过，未 skip。真实 native app-server 使用本地模拟 Responses，未调用云端模型。读取／停止保持 unloaded，显式 active 则验证 thread 已加载。
- 静态和构建：最终 `go vet ./internal/core/... ./internal/e2e/fakecodex`、`git diff --check`、Linux `CGO_ENABLED=0` production build 与 `GOOS=windows GOARCH=amd64 CGO_ENABLED=0` production cross-build 均通过。此前 Linux `go build ./...` 也通过。Windows 的 `./...` 包含仅支持 Unix 的黑盒 harness，故 Windows 验证使用 CI 的 production entry `./cmd/codex-proxy`，不是原生 Windows 执行。

环境处理：宿主 Linux Docker socket 不可用，使用 Windows Docker daemon 运行同一临时 harness；仅在 scratch 中扩展其无凭证 Go 工具链／缓存，以执行项目测试。Docker `/tmp` 为 noexec，测试临时目录放在容器私有 workspace。原生版本下载时移除容器不可达的宿主 loopback proxy。Core 子进程测试清除继承的小写 proxy 变量，避免覆盖 fixture。上述均为测试环境设置，不改变产品网络或权限策略。

关键回归测试：

- engine：未知观察仍执行停止；确认前与失败后保留屏障；ACK 后退出恢复；重启不阻塞其他会话、不误停后来的 native turn；慢命令和在途激活下的受控取消顺序；未知状态不能授权模型切换；历史不覆盖原生状态；完整状态展示。
- codex：通知／响应、多个并发请求的顺序；断线；深复制 budget；损坏 payload 不当作无 goal；仅识别明确的 capability rejection。
- transcript：goal trigger 不泄漏到后一 turn；turn_context 与 task_started 的顺序；未知或损坏 attribution 沿用普通输出。
- native：冷 observer 的读和 pause 不加载执行；冷 Agent 的查询与停止走真实生产适配器；默认关闭 goals 的普通取消不被锁住；冷连接显式激活已有 active goal 仍加载执行，且不重置 budget。
- E2E：三个 CI 分片通过真实 source-built CXP、模拟 Teams Graph 与模拟 Codex，核对命令、消息、队列、取消、重启及共享 home 等场景。

提交前 CI 修正：首次 PR CI 揭示仓库的选择器检查与模拟门禁依赖旧选择器；同步更新这两项，并验证任一新增 native goal 契约被 skip 时门禁都会失败。定向 Go 与 Python 测试通过。

CI 可达性：core 测试由 `race-core-exec` 的 `store` 与 `engine` 分组收集；黑盒场景由三个 `e2e` 分片按 package 自动收集。原生 goal 契约加入 `run_shared_codex_contracts.sh` 的选择器，并检查测试名称及实际 PASS，防止跳过或改名后出现假绿。

## 已知边界

原生 API 仍可能因另一个进程持有 writer 或暂时故障而拒绝暂停。CXP 对此保留未确认状态与停止意图；不会声称已经停止。CXP 的控制顺序不等于跨所有外部 TUI 的全局锁，其他客户端之后仍可显式改变 goal。

schema 11 按现有 store 策略迁移；旧版 CXP 无法直接打开更新后的状态库。这里没有升级宿主 helper 的状态库。测试不能证明未来协议和外部服务永不变化；真实云端 Teams、云端模型与原生 Windows 执行不包含在离线验证中。
