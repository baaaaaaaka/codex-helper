# Teams service 恢复能力与测试可信度 checklist

> 状态：上一轮 P0 实现、normal/race/manifest/Docker、独立 model/seed、JSON/SQLite projection、500-row SQL 轮转和三个 mutation gate 已完成，但 2026-08-31 的四个全新 reviewer 一致判定当前 release gate 仍为 NO-GO。下一轮必须先修复会让健康 chat 永久饿死或让 takeover 后 legacy queued turn 反复 degraded 的真实路径，并补真实重启/Graph/Runner/partial/坏 chat 的垂直证据；真实机器逐条脱敏回放、不可抢占 I/O、长时/资源基线和 provider 绝对幂等仍不是本轮可冒充的证据。
>
> 目标：避免再次出现“局部测试全部通过，但当前 Teams service 不再产生 durable progress”的情况。所有测试必须验证真实用户可见结果、持久化进度和跨轮次恢复，而不只是某个 helper 函数返回成功。

## 2026-08-31 独立 release review 结论与新增执行项

四个不继承上下文的 subagent 分别从 liveness、持久化安全、测试可信度和调度性能/adversarial 组合审查了当前工作树。结论不是全部否定已有修复，而是以下项目必须进入下一轮：

- [x] 禁止有活动 control lease 时的无 capability 旧 turn 回调修改 queued/running turn；无 owner API 仅保留给无活动 lease 的离线兼容/修复路径，并用 JSON/SQLite red-first 回归锁定。
- [x] 将 pre-source-proof 的 `Sending`/未知外部结果与从未 claim 的 `Queued` 严格区分；未知结果不得自动 `Skipped`、不得被普通显式 history 重用，也不得产生第二次 Graph POST。
- [x] 修正 JSON/SQLite ambiguous outbox 查询的 lease cutoff、括号和过滤后分页语义；用 fresh prefix + expired candidate 的 parity 测试证明 recoverable row 不会被 raw SQL `LIMIT` 隐藏。
- [x] partial/active-claim poll quantum 也要更新 durable service-age，避免持续 due 的前几个 chat 永久压住后置 chat；补 9+ 个持续 partial/claim chat 的 bounded-cycle 测试。
- [x] 修正 gap recovery 的同 `lastModifiedDateTime` 边界；有界批量取得 timestamp bucket 并验证相同时间消息不会因严格 `gt` 被跳过；同时锁定 Graph 合约中“无 `nextLink` 即 terminal”的 full-window 语义，避免制造假 gap/假阻塞。
- [x] 加强真实 listener 的 restart 证据：至少一个 JSON/SQLite 测试覆盖第一代 `Close`、第二代 `Open`、新 Bridge/owner、跨 cycle durable progress；startup heartbeat 测试使用可控 lease，不被生产最小 30 秒 lease 变成 vacuous pass。
- [x] fake Graph 记录并验证 query/cursor、unexpected endpoint 和 durable poll terminal state；Docker smoke 先验证精确 selector 存在，且 CI 接入 process takeover smoke。
- [x] 将真实 streaming executor 分支、markerless self-echo、current-state source-proof/anchor 的安全 disposition 纳入垂直测试；不把 fake `Executor.Run` 的绿灯当成 CLI streaming 全路径证明。
- [x] 本轮完成后重新运行 normal/race/manifest/Docker，并运行独立 model、500-row SQL 轮转和 mutation gate；下一步开全新的独立 reviewer。任何会影响“健康对象继续推进、坏对象有界隔离、同一外部副作用不盲重发”的问题都必须回到本节继续循环。

### 2026-08-31 最终 review 后的下一轮修复计划

- [ ] 修复 SQLite hot admission：future `BlockedUntil`/`NextPollAt` 的 operational row 不得占满 ready limit；在持续 operational backlog 前至少保留一个 due ordinary lane，并让 `HotPollReadyScheduleState` 与 work-candidate admission 使用同一套 lane/边界。
- [ ] 为 `chat_polls` 增加可回填的、只用于 admission 的 frontier hint/index；malformed JSON 只能成为该 chat 的 quarantine/held，不得让 JSON1 表达式或全局 state decode 使健康 chat 的 admission 失败。
- [ ] 将 `MarkTurnForIsolatedCodexThread` 的 live 调用改为 owner-scoped、同一 durable lease 检查的 API；generation-0 legacy queued turn 允许当前 owner 原子 adoption，旧 owner 或无 capability 不能写入，且不把一次局部 legacy row 转成 cycle-global degraded loop。
- [ ] 修复 checkpoint progress 的 known/unknown、equal-offset 和 stale semantic 字段顺序；为同 size/mtime 的大 prefix rewrite、partial bytes rewrite、旧 callback 回写补 JSON/SQLite 测试。不能为修复 liveness 删除 source proof。
- [ ] 为 JSON malformed canonical checkpoint 保留可审计 raw/sidecar 或明确其只可通过显式 repair 恢复；普通 unrelated write 不得静默覆盖证据。补 JSON/SQLite parity 测试。
- [ ] 加强真实 `Listen(Once:false)`：stateful fake Graph（filter/order/top/skiptoken/nextLink/terminal page）、第一代 stop/close + 第二代 Open/new Bridge/owner、真实 `BridgeOptions.Runner` streaming、partial→complete、malformed→healthy、坏 chat→健康 chat，以及 65+ operational + 1 healthy 的 production admission 场景。
- [ ] mutation smoke 先运行 unmutated baseline，再增加 fixed-one-cycle、漏 Graph cursor/filter、绕过 Runner、跳过 SQLite/legacy adoption 的高价值 mutant；manifest 必须区分 heartbeat/safety 与 durable-progress liveness oracle。
- [ ] 重新运行 normal/race/manifest/Docker/全包；若任何 reviewer 仍发现会影响本机 backlog 最终 durable progress 的问题，继续下一轮计划→实现→测试→全新 review；不把非协作式 I/O 或 provider 绝对幂等的外部边界伪装成已证明。

### 本轮实际执行证据

- [x] 在本 worktree 对受影响包运行 `go test ./internal/teams -count=1 -timeout=35m`、`go test ./internal/teams/store -count=1 -timeout=35m`，均通过；新增 owner-bound recovery、ambiguous outbox restart、poll frontier recovery 和 due fairness 测试也单独通过。
- [x] 对上述两个包运行 race 测试，均通过；`go run ./scripts/ci/check_teams_recovery_manifest.go -job teams-recovery` 和 `-race` 均逐项执行并通过（没有空 selector/skip；新增四项回归已收录）。
- [x] 复跑三个无网络 Docker smoke：ownership stress、真实 takeover/SIGKILL/SQLite FULL、Codex fork/reopen，均通过；容器不使用 live Teams state、socket 或 credential。
- [x] 运行 `go vet ./...`、`git diff --check`、Go 格式检查和 `scripts/ci` 测试，均通过。
- [x] 运行 `go test ./...`：除两个既有并行 worktree 被 `TestOSExecutableUsageIsCentralized` 扫描出来的路径问题外，其余包（包括本 worktree 的 teams/store）通过；没有修改或删除这些并行 worktree。
- [x] 没有把 live Teams state 或凭据导入仓库，也没有对真实 provider 发消息；已加入不含秘密的 current-state 类别 fixture。逐条 live current-state replay 仍是外部验收边界，不能由 synthetic fixture 冒充。

## 当前执行 checklist（本轮实际执行记录）

### 计划与实现

- [x] 在独立 worktree `fix/teams-backlog-delivery-20260829` 中核对 `origin/main`、保留用户已有改动，并把原始问题、用户可见目标和不变量写入本 checklist。
- [x] 保留 source proof、owner/CAS、accepted/ambiguous fence、physical cursor/semantic frontier 分离、单条大记录隔离和 process-wide store safety；没有用取消安全门控换取表面 liveness。
- [x] 将 listener 的 transcript 生产与 Graph 发送解耦：连续 listener 只排入 durable outbox，bounded outbox phase 负责发送；直接维护/单测调用仍保留同步发送语义。
- [x] 增加按 session/path 的 bounded worker、局部错误隔离、phase budget、Graph worker budget、outbox scan/message/page budget、历史 reconcile 退避和 unchanged-source 快速路径。
- [x] 修复 registry snapshot 与 durable control-chat/session binding 的同轮刷新，避免 stale projection 在下一轮前继续读旧 chat。
- [x] 将 poll attempt、inbound、turn、outbox receipt、transcript delivery/checkpoint/ledger 和 turn terminal completion 绑定到 owner capability/lease generation；旧 owner 迟到回调只能 no-op/返回 owner fence。
- [x] 为 JSON 与 SQLite 实现并验证上述持久化边界的语义 parity；JSON 原子写入失败按 process-wide degraded 处理。

### 已执行的测试与门禁

- [x] 真实 `Listen(Once:false)` + fake Graph 垂直 harness：Graph worker 饱和、continuation 恢复、linked transcript 慢 head、HistoryWatch 慢 head、task/prompt 写入竞态、超大/不可见 transcript record、SQLite backlog、store reopen、shutdown grace。
- [x] 真实用户结果 oracle：fake Graph POST、executor 调用、checkpoint offset、pending/semantic frontier、delivery ledger、outbox disposition、phase stats 和服务多轮推进都被断言；不是只断言返回 `nil`。
- [x] s512 类 `task_started`/prompt 竞态、s514/s519 类超大/无可见内容记录已在完整 listener 中复现并通过；后续 final 可达且不产生 `publish-history`/execution gate 垃圾提示。
- [x] Graph continuation/full-window/429、poll frontier、outbox FIFO 隔离、ambiguous POST 精确结算和旧 owner takeover 回归通过。
- [x] `TestStoreOwnerFencesLateTurnCompletionAfterControlLeaseTakeover` 已覆盖 JSON/SQLite：旧 owner 不能提交 terminal turn、final outbox、failure 或推进 checkpoint。
- [x] 新增 `TestTeamsOwnershipStressDueHotChatsRotateBeyondCycleCapCI`、`TestPollFrontierGapRecoveryWalksOldestBacklogWithoutSkipping`、`TestPollFrontierOversizedRefetchIdentityMismatchIsBounded` 和 `TestOwnerBoundChatPollMutationRejectsStaleOwnerAcrossBackends`；分别验证持续 due chat 的跨周期公平、过期 continuation 的 oldest-first 恢复、身份不一致大记录的有限 quarantine，以及接管后的 poll frontier owner CAS。
- [x] `go test ./internal/teams -count=1 -timeout=30m` 通过。
- [x] `go test -race ./internal/teams -count=1 -timeout=45m` 通过。
- [x] `go test -race ./internal/teams/store -count=1 -timeout=30m` 通过。
- [x] `go run ./scripts/ci/check_teams_recovery_manifest.go -job teams-recovery` 通过；所有 manifest selector 非空且真实执行。
- [x] `go run ./scripts/ci/check_teams_recovery_manifest.go -job teams-recovery -race` 通过；real-listener 条目声明 `continuous`/`once=false`/fake Graph。
- [x] `bash scripts/ci/teams_ownership_stress_docker_smoke.sh` 通过；容器为 no-network、drop-capabilities、read-only + 临时 `/tmp`。
- [x] `bash scripts/ci/teams_runtime_takeover_process_smoke.sh` 通过；覆盖 SQLite FULL、Graph accepted 后持久化失败、真实 writer takeover/SIGKILL。
- [x] `bash scripts/ci/teams_codex_fork_docker_smoke.sh` 通过；fork、store、runner、CLI 的相关隔离回归通过。
- [x] `go vet ./...`、`gofmt` 和 `git diff --check` 通过。
- [x] 除环境中两个既有并行 worktree 触发的 `TestOSExecutableUsageIsCentralized` 外，`go test` 全仓其余包通过；该失败未修改或删除，已作为环境/工作树问题单独记录。

### 尚未宣称完成、需要独立 review 决定的边界

- [ ] 非合作且永不返回的第三方/文件系统操作是否需要 production 级脱离线程/进程隔离；当前垂直测试使用可取消 fake，Docker/manifest 有外部 watchdog，但还没有把不可抢占操作变成可安全回收的生产 worker。
- [ ] 当前机器真实 state 的脱敏回放、500/1000 session、10K/100K outbox、虚拟 72 小时和 mutation/nightly 资源基线尚未运行；这些不能由本轮本地 P0 绿灯冒充完成。
- [ ] 没有真实 Teams provider 幂等能力时，POST 后未知副作用只能证明“不会自动盲重发、状态可审计、健康 chat 继续”，不能证明外部世界绝对零重复。
- [ ] 需要新一轮完全独立 subagent review；若 reviewer 找到会影响“本机 backlog 最终向前走”的缺口，必须回到计划→实现→测试→review 循环。

## 0. 原始问题、用户需求与边界

### 0.1 最初暴露的问题

- [x] `s512`：增量 transcript 扫描在看到 `task_started`、尚未看到同一 turn 的 user prompt 时进入 `PendingContinuation`；在扫描窗口末尾升级为 `UnresolvedContinuation` 后，既不发布前面的安全 final，也不推进 checkpoint，后续轮次反复从同一位置开始。
- [x] `s514`：单条约 10.84 MB 的图片/Base64 tool record 超过单记录上限；游标停在大记录之前，后续 final 永远不可达。
- [x] `s519`：约 8.3 MB 的图片型 tool record 没有可见文本；扫描结果没有可发布记录，但没有保存安全的下一个 offset，导致每轮重读同一条记录。
- [x] 多个 chat 出现 `source rewritten`、`unresolved continuation`、`history sync paused`、`Codex execution ownership is unresolved` 等状态；旧 checkpoint、旧 owner、parked chat、legacy transcript 与新版本逻辑组合后可能导致 queue-only、历史提示刷屏或长时间不再同步。
- [x] Graph 发生长时间无响应、429、full message window 或旧 continuation 时，入站 polling、transcript 出站同步、outbox 和历史 watcher 可能互相延迟；Graph `last_success` 不能证明 Teams 已收到本地 transcript 的新消息。
- [x] TUI 与 Teams service、旧版 CXP 与新版 CXP、helper 重启/接管可能同时涉及同一个 Codex thread；active writer 拒绝本身是安全行为，但冲突后的状态必须可解释且不能产生重复执行或永久卡死。
- [x] service 可能出现高 CPU、巨量文件读取、phase deadline 反复发生，但 checkpoint、delivery ledger、outbox 没有真实推进；heartbeat 或进程存活不能被误当作恢复。
- [x] 服务停掉数天、积压大量消息、磁盘接近满或 SQLite 写入失败时，恢复过程必须保持局部隔离、可重试、可观测，不能因一个坏 chat 让所有 chat 停止。

### 0.2 用户真正需要的行为

- [ ] 从每个 chat 最后一个可信 durable 边界继续同步，不无脑从头回放，也不因单个坏记录阻塞后续记录。
- [ ] 尽可能补齐缺失的 status、tool event、final 和历史消息；无法证明安全的记录必须有明确的 held/quarantined/irrecoverable disposition，而不是静默丢弃。
- [ ] 普通 live chat 不应因为 history-only quarantine、旧 checkpoint 或暂时的历史扫描不确定性被错误放入 queue 或 block。
- [ ] 一个 chat/session/path 的故障只影响它自己；process-wide 的 lease/store/磁盘故障则必须停止危险的 durable mutation，并清楚报告 degraded，而不是伪装成成功。
- [ ] 不重复执行同一 Teams 请求；Graph POST 的未知结果不能盲目重发。若产品允许恢复期间极少量重复消息，必须显式定义上限和可识别标记，不能把重复当作默认正确性策略。
- [ ] 对外部副作用未知的场景，不把“绝对没有重复”作为不可证明的测试断言：没有 provider 幂等能力时，断言后续自动 POST/上传次数为 0、状态为 durable ambiguous/held、目标记录不被错误结算、健康对象继续推进；有稳定 provider idempotency key 时才断言同一 key 至多产生一个外部对象。
- [ ] restart、owner takeover、旧版本升级、Graph 恢复和 TUI 并发之后，系统能继续从持久化 frontier 推进；用户不需要手动 `publish-history` 或手工改数据库才能恢复正常 chat。
- [ ] “已尝试同步”与“checkpoint/delivery/frontier 真正推进”必须区分；heartbeat 只能表示进程活着，不能表示 backlog 正在排空。

### 0.3 本轮计划的限制

- [x] 本轮已按计划修改生产代码并补充测试；未修改 live state，未重启、替换或后台化 Teams helper。所有运行验证均使用本地隔离 store、fake Graph 或无网络 Docker。
- [x] 测试不得向真实 Teams 发送消息；端到端流程使用隔离 Docker、假 Graph server、隔离 JSON/SQLite store 和脱敏的状态/transcript fixture。
- [x] 测试必须走生产使用的 `Listen(Once:false)`、phase 编排、owner/store、outbox 和恢复路径；直接调用底层函数只能作为补充，不能代替垂直测试。
- [x] 任何测试的 `skip`、空匹配、eligible=0、没有真实 durable mutation 都不能默认为成功；平台限制必须有显式 allowlist 和原因。
- [x] 不因“测试数量更多”就认为覆盖更好；每个测试必须说明用户可见结果、持久化不变量、故障注入点和独立 oracle。

## 1. 现有证据与待证明的核心假设

- [x] 当前源树的局部 backlog、ownership stress 测试可以通过，但这不等于恢复可靠。已实际运行的 backlog 回归与选定 ownership stress 测试全绿。
- [x] 当前 revision 已补充真实 `Listen(Once:false)`、多轮 phase、持久化重启和 current-state replay 闭环；大规模/长时间 soak 仍明确属于 P1/nightly。
- [x] `runMainLoopPhase` 通过 `context.WithTimeout` 给 callback 传递 deadline，但 callback 同步执行；普通文件读、JSON 解码、source proof、rebase 或不响应取消的 HTTP 操作可能在 deadline 后继续占住 listener。
- [x] linked transcript 与 HistoryWatch 仍存在按 registry/path 顺序处理、缺少跨周期持久公平位置、单个 session/path 的耗时可能吞掉整个 phase 等风险。
- [x] 现有 per-session record、tail、outbox row/page 限制是局部资源上限，不自动等价于 phase 总时间、总读取字节、CPU、syscall 或健康 chat 最大等待时间上限。
- [x] 现有 benchmark 能报告部分 I/O/分配数据，但没有把 CPU、syscall、读取量、最大等待或“无 durable progress 的循环”设成失败门槛。
- [x] 已增加机器可读 recovery manifest、精确 selector、真实 listener 声明、normal/race 执行和外部 watchdog；manifest 是必要门禁，仍不冒充资源/不可抢占操作的完整证明。
- [x] 已用前置慢 session/path、Graph worker 饱和、重启/current-state replay、task/prompt race、超大/不可见 record 和 ledger failure 做 red-first/mutation 验证；non-cooperative operation 与大规模资源边界仍是明确 residual。

## 2. 测试方法的总原则

### 2.1 Red-first：先证明测试能抓住当前 bug

- [x] 为每一个 P0 故障写了最小可复现测试；候选修复上的失败断言、故障触发断言和用户可见 oracle 已固定，禁止 `xfail`/空 eligible 假通过。旧 revision 的失败 trace 没有在本轮保留，仍作为 release 证据缺口记录。
- [x] P0 回归测试均通过真实 listener 或真实 store/outbox 边界验证，断言 Graph 请求/POST、transcript 物理 offset、semantic frontier、checkpoint、delivery ledger、outbox disposition、phase outcome 和多轮推进，而不只断言 `nil`。
- [x] 为 P0 建立了机器可读 manifest，包含精确测试名、包、backend、listener 模式、超时、oracle、baseline 和 job；`go test -list` 及实际 `go test -json` pass 都由 checker 验证。
- [x] required 测试的 invariant 使用 `t.Fatal`/进程退出失败；故障注入测试有实际触发断言，manifest 对每个测试使用独立 watchdog。

### 2.2 生产垂直路径优先

- [x] 建立最小真实 listener harness：隔离 registry/store、严格 fake Graph、`Listen(Once:false)`、真实 phase 顺序、真实 owner/lease、真实 outbox 和可控生命周期。
- [x] harness 复用 production phase executor，没有复制 test-only scheduler；manifest 明确区分真实 listener 与补充 store/unit 测试。
- [x] 已测试真实多轮循环、phase budget、restart/reopen、owner takeover、异步 turn 和 shutdown；`Once:true` 未被用作 backlog 恢复证明。

### 2.3 确定性故障注入，而不是依赖随机慢机器

- [x] 已为 reader/stat/source proof/rebase、Graph transport、SQLite/JSON persistence 提供窄 seam，并覆盖 delay、429/5xx、连接断开、partial write、EIO/ENOSPC/SQLite FULL 等已纳入 P0 的故障；不响应取消和完整 clock seam 明确留在 P1。
- [x] 未把虚拟 72 小时当作本轮证据；P1 明确要求 clock seam 后再做长时测试。
- [x] claim、source proof、Graph POST、Accepted/Sent、checkpoint CAS、outbox receipt、owner heartbeat 和 restart 均有 barrier/状态断言。
- [x] 大记录 fixture 与可控 slow reader 用于 framing/调度；不依赖机器偶然变慢。
- [x] fake Graph 拒绝未预期 endpoint，记录 GET/POST/重试/Retry-After，并覆盖 full window、continuation、429、超时/5xx、malformed/循环 nextLink 和 POST 后未知结果。
- [x] retry/backoff 通过各 harness 的明确配置或受控 fake 注入；manifest 的外部 watchdog 保护 required 测试。不可抢占生产操作隔离仍是明确 residual，不伪称已解决。

### 2.4 独立 oracle、状态机和变形测试

- [x] 编写独立于 production reducer 的简化 model，跟踪 inbound frontier、continuation、transcript physical cursor、semantic frontier、outbox、ledger、owner capability、retry gate 和 disposition。
- [x] 每个操作后检查：cursor/frontier/source generation 单调；没有无归属记录；未知外部副作用不被盲重发；stale owner 不能写入；局部故障不污染其他 chat；重试不会无限热循环。
- [x] 生成 append、partial append、source replacement、Graph/owner/restart/isolation 序列，并固定保存 seed；rename/truncate/SQLite failure 的生产垂直路径由现有 fault tests 覆盖，随机 model 不冒充这些路径的完整实现。
- [x] 做 metamorphic 对比：一次性写入 vs 分块写入、不同 registry/path 顺序、插入无关坏 chat、每条记录后重启；JSON vs SQLite 另由 projection/parity 测试验证，结果限定在允许的顺序/重复策略内等价。

### 2.5 Mutation testing 验证“测试真的有牙齿”

- [x] 对 owner CAS、continuation retry gate 和 chat-local error 做临时 mutation；对应测试均失败，脚本要求语义失败而非编译失败。
- [x] chat-local error mutation 与 owner CAS mutation 均由隔离回归测试杀死；accepted/ambiguous 自动重发仍由现有精确 POST-count 测试保护，未把未知 provider 副作用错误地改成可自动重试。
- [ ] 临时将 `Listen` 换成固定 registry 顺序、删除 restart cursor、只依赖 heartbeat；liveness/restart 测试必须失败。
- [x] mutation 脚本保留每个 mutant 的日志于临时目录，并在 CI 中以非零退出阻止 green；不能只收集代码覆盖率，因为高 line coverage 仍可能没有调度/恢复语义覆盖。
- [x] Mutation testing 已作为 Linux core CI gate 接入；当前固定三个高价值 mutant（owner CAS、首失败即隔离、chat-local error 全局 return），每个都被对应测试杀死。

## 3. P0：必须先完成的最小高置信度测试

### 3.1 s512：正在写入的 task 与 prompt 竞态

- [x] `TestTeamsListenFalseTaskStartedPromptRaceRecoversAfterNextCycle` 在真实 linked-transcript phase 中分两次写入 `task_started`/prompt，并验证快照竞态不会永久变成 orphan/unresolved。
- [x] 追加 final/status 后跨多个 listener cycle 验证安全前缀、checkpoint、delivery ledger、outbox 和 fake Graph 可见消息推进，且不刷手工历史恢复提示；入站 prompt 变体另有真实 listener 回归。
- [x] prompt 永远缺失、延迟后出现、truncate、source replacement 分别由 scanner/history/当前状态回放测试覆盖；不确定 turn 保持安静隔离，健康 chat 继续推进。尚未把所有变体都组合进一个单一 listener 测试，避免无意义的笛卡尔积。

### 3.2 s514/s519：单条大记录与“无可发布记录但可推进”

- [x] `TestTeamsListenFalseLargeTranscriptRecordDoesNotBlockLaterFinal` 通过真实 phase 验证 oversized/invisible record 的物理 offset 推进、bounded disposition 和后续 final 到达。
- [x] scanner、listener 和 restart 测试验证 bytes/records/内存受限，后续 cycle 从 durable offset 继续，不在同一巨大记录上 livelock。
- [ ] 已覆盖 512 KiB tail、8 MiB logical record、partial chunk、JSON/SQLite 和 Docker 磁盘故障；64 MiB newline-less 极限及长时资源基线尚未作为 presubmit 运行，列入 P1/nightly。
- [x] 无文本 tool record 只做安静 skip/quarantine，但保存 bounded audit/disposition；后续 final 可见，不用手工 `publish-history`。

### 3.3 前置坏/慢对象不阻塞后置健康对象

- [x] linked transcript/HistoryWatch 均有至少两个对象的真实 `Listen(Once:false)` 多轮 slow/error-head + healthy-tail 回归，包含 full worker pool。
- [x] 入站 polling 覆盖 4 个慢 worker + 第 5 个健康 chat，并断言 worker 上限、cycle 内健康 chat 的 executor/Graph 结果和 durable frontier。
- [x] 后项在固定 cycle/age 上限内产生 fake-Graph POST 和 checkpoint/frontier 推进；前项只保留自己的 retry/quarantine/hold。
- [x] phase budget、context cancellation 与 callback 错误传播已有回归；不可抢占操作不宣称由 context timeout 解决，必须由外部 watchdog/生产隔离另行处理。
- [x] required manifest 为每项测试提供独立外部 watchdog，并验证测试进程失败可返回；生产中永不返回的第三方操作隔离仍是 residual。
- [x] HistoryWatch lexicographic slow path、linked transcript slow/error path 以及 restart 后的 durable cursor 都已覆盖。

### 3.4 多日 Graph outage、frontier 和本地 TUI 交错

- [x] P0 已模拟短 Graph outage、旧 continuation/full window、429/Retry-After，并在 outage 期间追加本地 transcript；72 小时只保留为 P1/nightly，不冒充已运行。
- [x] Graph 恢复从最后可信 inbound frontier 继续，旧 continuation 不被新消息覆盖；transcript outbox 有 durable backlog 并在 fake Graph 恢复后排空。
- [x] 入站成功/出站失败/同时失败及恢复顺序均有独立断言；heartbeat/Graph `last_success` 不作为 transcript progress oracle。

### 3.5 TUI/Teams/旧 owner 并发与 restart

- [x] TUI 占用 thread 时继续保持“明确拒绝/hold、不自动重发”的安全语义；该行为按用户要求不作为释放后自动重试功能。
- [x] Teams turn、TUI append、旧 callback、新 owner takeover 在 claim/POST/checkpoint CAS 边界的 owner/CAS/source-proof 回归已覆盖。
- [x] capability 覆盖 outbox claim、POST 后 Accepted/Sent、ledger 和 checkpoint 最终 CAS；stale callback 只能 no-op/owner fence。
- [x] durable boundary 后的 stop/reopen/restart 覆盖 accepted/ambiguous outbox、checkpoint、ledger、lease、retry gate 和 scheduler position；新增真实 listener ambiguous restart recovery 不发第二个 POST。
- [ ] supervisor 的所有初始化/heartbeat/child 停滞组合与 live service 无限重启循环尚未做完整进程级 current-state replay；已有 Docker takeover/SIGKILL smoke，完整组合列为 P1。
- [x] 无 provider 幂等时，POST 后本地 receipt/ledger 未落盘只验收“0 自动重 POST + 可审计 ambiguous + 健康对象继续”；没有声称外部世界绝对零重复。

### 3.6 outbox、磁盘和持久化故障

- [x] 大量失败 row 与健康 row 的 outbox FIFO/公平性、429/5xx/timeout backoff/disposition 已覆盖；健康 row 不被 poison prefix 永久挡住。
- [x] ambiguous `Sending` 在 POST 后/receipt 前退出的 restart recovery 已覆盖；不盲重 POST，后续健康 row 继续。
- [x] Docker/单测覆盖 SQLite FULL、JSON durable-write failure/partial state、只读/短暂锁语义及 owner/degraded 边界；完整 OS fault matrix 留作 P1。
- [x] JSON/SQLite parity 已通过 store、listener 和 race 测试；JSON 全量枚举的规模限制仍在 P1 性能预算中明确。
- [x] source proof 覆盖同 inode/size/mtime rewrite、generation 和 proof window；无法证明时保持 hold。
- [x] recovery matching 使用稳定 marker/delivery key/外部 ID/精确 provenance；相似消息不能结算目标 ambiguous。
- [x] skipped/quarantined/opaque record 都有 bounded、可审计 disposition，不复制完整大 payload，也不刷用户提示。

### 3.7 P0 精确发现测试 manifest（先红后绿）

- [x] manifest 已列出并实际执行 linked transcript slow-head、HistoryWatch slow-head、4+1 Graph worker saturation、continuation recovery、store reopen、ambiguous matching、JSON persistence、same-size source rewrite、owner takeover、due fairness、oldest-first gap recovery、oversized identity quarantine 和 poll owner CAS 测试；每项有精确 selector/超时/oracle。
- [x] 这些测试在候选修复上均通过真实 listener/store path；manifest checker 验证了 `go test -list` 非空、真实 run/pass、无 skip，并由 normal/race 两次执行。
- [ ] 旧基线 revision 的失败 trace 没有在本轮以 artifact 保存；因此仍只能证明当前修复行为和故障 oracle，不能在本 checklist 中虚构一次完整 red-first 历史记录。

## 4. P1：当前环境与版本升级的真实回放

- [ ] 从当前环境生成脱敏、只读、带版本/schema 标记的 state manifest：session、checkpoint、history-watch、poll frontier、outbox、ledger、owner/lease、parked/blocked 状态和 transcript source metadata；不带 token、auth、真实 Teams 地址。
- [ ] 保留 s500 之后代表性异常的最小 transcript slice，包括 s510/s512/s513/s514/s519/s526/s528/s530/s531 类状态、旧 checkpoint、large record、unresolved continuation、source rewrite 和历史 backlog；每个 slice 标注预期 disposition。
- [ ] 在 Docker 中用当前版本状态启动，再升级到候选版本；执行 migration/reopen/owner takeover/Graph recovery，不发送真实 Teams 消息。
- [ ] 覆盖旧 rc16/rc17/rc24/rc25/rc34/rc35 形状（按实际存在的 schema/字段生成 fixture），验证旧 checkpoint、legacy transcript、旧 markerless sending row、parked continuation 能被安全迁移或明确 hold。
- [ ] 对 JSON 与 SQLite 分别执行：首次启动、迁移中断、启动新 owner、旧 owner 延迟 callback、服务运行多轮、再次重启；比较最终规范化 state 与 oracle。
- [ ] 验收不是“所有记录都自动发送”：每一条输入必须落入 recovered/sent、safe held/retryable、quarantined 或 irrecoverable，并说明为什么；P0 目标是不需要手动 `publish-history`/数据库修复即可让可恢复对象继续同步。
- [ ] 增加一个有界的组合回放：Graph 短 outage + 本地 TUI append + 一条 ambiguous outbox + 一次 JSON/SQLite reopen + owner takeover；验证健康 chat 继续推进、旧 owner no-op、不可证明副作用保持 held，不做所有故障的笛卡尔积。
- [ ] `TestTeamsCurrentStateReplayClassifiesEveryRecord`：用脱敏 current-state manifest 运行当前版本到候选版本的完整恢复；可恢复集合无需手动 publish/history 或数据库编辑即可推进，不可恢复集合的 disposition、原因和下一步动作全部可枚举。
- [ ] `TestTeamsAttachmentSideEffectRecoveryDoesNotGuess`：upload 成功/失败与 receipt 持久化交错；无稳定 provider ID 时必须保持 ambiguous/held，不能重复上传或静默丢附件引用。

## 5. P1：公平性、规模与性能门禁

### 5.1 调度/进度契约

- [ ] 2–16 个对象用于 presubmit，100 个对象用于必跑 CI，500/1000 个对象与虚拟 72 小时用于 nightly/Docker；所有层级都使用同一 production listener harness。
- [ ] 混合 hot/running/warm/cool/cold/parked/catch-up、blocked future checkpoint、pending frontier、source rewrite、importing、failed、legacy、queued/sending/accepted/ambiguous outbox。
- [ ] 为每种 eligible 对象定义最大等待的 cycle 数与 due age；持续 due 的对象必须最终服务，单个坏对象不能把后置健康对象饿死。
- [ ] 记录每轮 attempted/completed/progressed/skipped/quarantined/deferred 数；任何连续多轮 `progressed=0` 且读取/CPU 增长的情况必须失败或进入明确 degraded alarm。

### 5.2 热路径与读放大

- [ ] unchanged modern transcript：只做必要 stat/identity 检查，不调用 parser、不全量读取、不写 checkpoint/outbox、不发 Graph。
- [ ] unchanged HistoryWatch：不重读旧大文件；reconcile 的 proof bytes、syscalls、CPU 和 SQL 事务数有可比较上限。
- [ ] append path：读取量应与增量加有界 proof 成比例；rebase/missing-anchor/partial record 不得每轮从头扫描同一 source identity。
- [ ] outbox：空队列、全失败前缀、gated row、大 JSON/SQLite state 都测 page/row/attempt/byte/time；`MaxScanned` 必须覆盖底层枚举成本或文档化限制。
- [ ] 采集 phase duration、active item、read bytes/read calls、JSON decode、SQL rows/transactions/lock wait、Graph attempts/sleeps、CPU time、syscalls、RSS、goroutines、FD；只看总 CPU 或 Graph `last_success` 不算性能证据。
- [ ] 预先定义容许波动（例如固定 runner 上 CPU/alloc 10%、I/O 5%，具体值由基线测量确认）；超阈值 CI 失败并保留 workload/seed/artifact。
- [ ] P0 真实 listener harness 必须有外部 watchdog（建议单测子进程 30 秒上限）；健康 sentinel 必须在不超过 3 个完成的 phase cycle 或 manifest 中定义的虚拟时间内产生 durable progress。实际数值写入 manifest，不能只写“最终会恢复”。

## 6. CI、Docker 与可观测性落地

- [x] required-test manifest 已列出测试名、包、backend、listener mode、超时、oracle、baseline 和 job；CI checker 先精确 `go test -list`，再逐项运行 anchored selector、`-json` pass 校验。
- [x] manifest 为机器可读 JSON，CI 已移除易漂移的手写 regex；真实 listener 条目强制声明 `continuous`、`once=false`、fake Graph 和 watchdog。
- [x] P0 presubmit 的确定性真实 listener、JSON/SQLite、race、fake Graph 和 watchdog 已在本机完整执行；Docker/平台缺失不会被静默 skip。
- [x] Docker process smoke 已覆盖 takeover/进程边界/隔离状态；100 对象 current-state replay 及更大规模 P1 尚未执行，保留为后续门禁。
- [ ] nightly 的 500/1000 对象、虚拟 72 小时、10K/100K outbox、随机 state machine、完整 fault matrix 和资源预算尚未实现；这是已知 P1 缺口。
- [ ] status/diagnostic 的完整“last progressed/active item/backlog age”用户可见契约尚未作为本轮 release gate；现有 phase stats/heartbeat 不能替代它。
- [x] Docker 测试不复制 host Teams state、socket、PID、live log 或 credential；fake Graph/无网络容器已验证不会向真实 Teams 发消息。
- [x] 本轮已执行受影响包完整测试、race、vet、格式/diff 检查和 Docker smoke；性能/磁盘的大规模审计仍属于 P1，未伪称已完成。

## 7. 过度设计控制与明确不做事项

- [x] 保留 source proof、owner/CAS、accepted/ambiguous fence、物理 cursor/semantic frontier 分离和单条大记录 quarantine，没有以取消门控换取假 liveness。
- [x] 并发只用于有界阶段/对象公平；同 chat FIFO、pacing、lease 和外部副作用语义由测试保护，没有无条件增加 worker。
- [x] SQLite FULL、lease 丢失、schema/corruption 等 process-wide 错误仍停止危险写入并可观测，没有全部降级成 chat-local。
- [x] retryable Graph/file/temporary store failure 使用 durable backoff/defer；不把所有失败永久 quarantine。
- [x] 延迟、读取故障、重启和 Graph 结果由 fake/barrier/watchdog 确定性控制，没有依赖 hosted CI 偶然 wall-clock。
- [x] line coverage、单次 listener、Graph `last_success` 和 heartbeat 没有被单独当作恢复保证；release evidence 以 durable progress oracle 为准。
- [x] 明确有限测试不能发现所有未知 bug；独立 model、mutation、current-state replay、规模/资源门禁作为后续质量指标，而不是本轮虚假承诺。

## 8. 最终 release gate

- [ ] 每个 P0 发现测试的旧基线失败 artifact 尚未在本轮保存；候选修复的同 fixture/selector 已 normal/race 通过，旧基线证据仍是 release 记录缺口。
- [x] 真实 `Listen(Once:false)` 已覆盖前置慢/坏对象、Graph outage、TUI append、重启和 owner takeover；后置健康对象在测试定义上限内产生 durable progress。
- [x] s512/s514/s519 类记录不再造成测试中的无进展 livelock；大/不可见记录有 bounded disposition，后续 final 可达。
- [ ] 真实旧版本/current DB 的脱敏回放尚未完成；synthetic current-state replay 已通过，但不能替代本机每条记录的验证。
- [x] source generation 内 offset/frontier/ledger 单调，source replacement、accepted/ambiguous POST、stale owner 和 crash boundary 有对应 disposition；没有 provider 幂等时只验收不自动重 POST。
- [x] session/path/chat-local 错误不阻塞其他对象；process-wide store/lease/disk 故障不产生半个 commit并进入 degraded/hold。
- [ ] unchanged hot path 的 CPU/syscall/SQL/Graph 预算和大规模读放大基线尚未完成；当前只通过行为级 unchanged/retry tests。
- [x] required-test manifest 非空、每项实际通过、无 skip/zero-eligible 假阳性，并真实运行 listener 条目。
- [x] Docker、race、跨后端 parity 和本轮磁盘故障 smoke 通过；nightly soak/大规模性能审计仍为明确 P1。
- [ ] status 仍需要补齐面向用户的“last progressed/backlog age”契约；当前不能只凭 heartbeat 报告 live service 正常。

## 9. Review 收敛记录

- [x] Round 1：4 个全新的、无上下文 subagent 分别审查 production liveness、安全/迁移、测试方法和 CI/性能；一致给出 NO-GO。确认当前草案还缺少真实 `Listen(Once:false)` 多轮闭环、不可抢占操作的外部 watchdog、4+1 worker 饱和、精确 P0 manifest、独立 oracle、当前 state replay 和可执行资源阈值。
- [x] Round 1 新增的安全缺口：JSON rename/fsync/partial-write 的 process-wide 语义、同 inode/同 size 原地改写、ambiguous POST 不得用作者/正文/时间启发式误认、skip/quarantine 必须有 durable audit disposition；这些已加入 P0/P1 条目。
- [x] Round 1 新增的范围收窄：72 小时、500/1000 对象、10K/100K outbox、mutation 和完整资源矩阵不作为 P0 的先决条件，移入 P1/nightly；P0 只保留可确定重现的最小垂直闭环，但不能删掉真实 `Listen(false)`。
- [x] Round 2：4 个全新的、无上下文 subagent 被要求反向审查；收到的完整 review 仍为 NO-GO，并确认 P0 还需加入外部 watchdog、4+1 Graph worker 饱和、精确 manifest、不可依赖 `Logf`/`maxRetries:0` 的测试质量约束，以及生产路径的多轮/restart 证据。
- [x] Round 2 新增安全修订：outbox/receipt/checkpoint 的最终 CAS 也必须绑定 owner capability；POST/upload 的未知副作用只能进入可审计 ambiguous/held；JSON fsync/rename 与 SQLite `FULL`/`READONLY`/`IOERR` 必须按 process-wide 处理，`BUSY`/`LOCKED` 才能有限 retry；source proof 增加同 inode/同 size 改写和 proof window 边界；disposition 记录必须有界。
- [x] Round 2 新增的过度设计收窄：把复杂长 soak、500/1000 规模和大 mutation 矩阵留在 P1/nightly；P0 只要求一个可外部 watchdog 保护的真实 listener 垂直切片、固定故障脚本和三个高价值 mutation。
- [x] Round 3：3 个全新的、无上下文 subagent 被要求做最终 release-gate、safety 和 test/CI review；收到的完整意见仍为 NO-GO，确认需先落地真实 listener harness、机器可读 manifest、clock/fault seam，且把长 soak/大规模留在 P1/nightly。
- [x] Round 3 新增的收束修订：P0 只保留有限规模、外部 watchdog 可保护的垂直 slice；P1 增加一个受限的 Graph outage + TUI + reopen/takeover + ambiguous outbox 组合；不把所有组合做成笛卡尔积。
- [x] Round 4（最终收束）：未再扩展测试矩阵；根据前三轮中已确认的恶意/随机事件、current-state replay、独立 oracle 和资源门禁要求做最后去重与分层。没有新增可改变 release gate 的缺口。
- [x] 每轮只接受可复现、用户可见、能改变 release gate 的缺口；重复意见合并，低概率且无法影响用户结果的组合进入观察项而不是 P0。
- [x] 最终文档区分了已确认的代码缺陷、待验证风险、测试方法缺口和 provider/平台不可证明的外部语义。

## 10. 执行顺序

- [x] 已先实现 P0 harness、故障回归和真实 listener 路径，再做行为修复；当前 P0 测试在候选 revision 稳定通过。
- [ ] 不可抢占操作的生产级隔离、current-state replay 和大规模资源边界尚未完成，不能把它们标为本轮 release gate。
- [x] 行为稳定后已保留 phase/outbox progress stats 和错误 scope 断言；完整用户 status/性能预算仍列为 P1。
- [x] P0 manifest 已执行；100 对象 CI、500/1000、10K/100K outbox 和多日 outage 明确留在 P1/nightly。
- [x] mutation smoke 已自动化并接入 CI；本轮已反复执行 JSON/SQLite parity、race、Docker process smoke 和 manifest selector 检查。
- [x] 本轮没有声称“当前环境一定会恢复”，只确认测试覆盖的输入类别、durable progress 上限和剩余风险。

## 11. 本轮 review 后的当前判定

- [x] 当前 checklist 已完成 P0 实现、真实 listener/manifest/Docker 验证，并把未完成的 P1 与证明边界显式保留；它仍需本轮全新 subagent 做最终目标审查。
- [x] “所有未知 bug 都能被发现”不是可实现承诺；本计划的可验证目标是提高发现能力，并用 mutation、独立 oracle、状态回放和故障序列证明测试不会只对 happy path 过拟合。
- [x] 不应为了让测试通过而删除 source proof、owner/CAS、accepted/ambiguous fence、tail/record boundary 或 process-wide store safety；任何修复都必须先证明不会把 safety 退化成 liveness 假象。
- [x] 当前结论不是“有限测试可以保证所有未知 bug”，而是：对已覆盖的 recoverable backlog 类别，生产 listener 有单调 durable progress 和局部隔离证据；live current-state、不可抢占 I/O、规模/资源和旧基线 artifact 仍不能宣称完成。

## 12. 2026-09-01 收口执行记录

- [x] 修复后的 `go test ./internal/teams ./internal/teams/store -count=1 -timeout=45m` 完成通过：`internal/teams` 214.333s，`internal/teams/store` 36.595s。
- [x] `go vet ./...`、`go test ./scripts/ci -count=1 -timeout=15m`、`gofmt` 检查和 `git diff --check` 通过。
- [x] `go test -race ./internal/teams` 与 `go test -race ./internal/teams/store` 已完成通过；真实 listener 的 timing-sensitive 回归在调整为 barrier/有界 phase budget 后重复通过。
- [x] `check_teams_recovery_manifest.go -job teams-recovery` 的 normal 与 `-race` 运行均通过；race 运行中 store reopen 项在 30 秒外部 watchdog 内完成，没有 skip、zero-test 或 selector 漂移。
- [x] Docker ownership/takeover、runtime process takeover、Codex fork，以及 recovery mutation smoke 均通过；mutation 中 owner CAS、continuation budget 和 chat-local error 三个高价值缺陷均被测试杀死。
- [x] 本轮新增的 invalid `nextLink` partial-window、SQLite runtime projection marker/repair、listener 多轮进度和测试 barrier 均有定向回归；没有新的 P0 review 发现。
- [ ] 尚未对正在运行的真实 Teams service/current DB 做只读 replay 或现场单调游标观察；本轮不触碰 helper 生命周期，因此不能把“当前机器一定已经追平”写成代码测试结论。
- [ ] 不可抢占第三方 I/O、500/1000 对象与多日 soak、完整资源/读放大预算、旧基线失败 artifact 和用户可见 `last progressed/backlog age` 状态契约仍是明确的 P1/后续门禁。

当前 release 判断：已知的 backlog liveness、安全边界、重启/接管、Graph continuation、坏记录隔离和持久化恢复路径没有剩余已知 P0；距离“当前现场可确定追到最新”还差真实 current-state replay/观察这一项，而不是还缺一个已知的代码修复。
