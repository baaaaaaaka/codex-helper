# Teams 可靠优化与验证 checklist（2026-09-11）

## 目标和不可突破的边界

目标：在不改变 Teams durable 语义的前提下，降低 hot poll/admission 的本地开销，并用可重复、隔离的 Docker 实验证明 durable completion 吞吐是否改善。历史 Docker 的 `0.567 durable completed/s` 只作为旧诊断基线；本计划不读取、重启或测量当前 live `rc.45`。

本计划只在已有 dirty worktree 上做增量修改，保留所有用户改动。

- [x] 不批量合并 inbound claim/complete。
- [x] 不在 durable commit 前推进 cursor、seen、frontier 或 receipt。
- [x] 未知 Graph POST 结果不自动重发。
- [x] 不绕过 owner、lease、attempt、binding、revision、frontier 或 CAS。
- [x] head 与 continuation 不使用两个并发 durable frontier。
- [x] Graph/handler/executor 不放入 SQLite/state 事务或锁内。
- [x] ACK、marker、final 保持独立 durable transition。
- [x] JSON canonical row 仍是最终 authority；scalar projection 只是受 revision/trust fence 保护的 admission hint。
- [x] untrusted、NULL、缺列、旧 schema/trigger、坏 JSON、projection 不一致必须 fail closed 并走有界 fallback/recovery。
- [x] 不通过扩大 worker/quantum、跳过 recovery、删除 canonical fallback 或延长 timeout 掩盖错误。
- [x] 性能实验只用 immutable fixture、独立 mutable runtime、fake Graph/fake executor；不使用 Teams token，不发真实消息，不写当前数据库。

## 第一轮 reviewer 的发现及处理状态

- [x] 已由无上下文 reviewer 审查历史提交、scalar/hydration、backlog SQL、scheduler/maintenance、Docker/E2E。
- [x] `work` trusted lane 已改为 scalar identity/order 读取；selected hydration 才读取 canonical `sessions.json`。
- [x] bridge 保留最多 64 个 scalar headroom，但在最终选定最多 8 个后才 hydration；不会先解码 64 个 session。
- [x] `TeamsOperationalBacklog` 的 canonical fallback 保留；scalar false 不会隐藏 JSON recovery/attempt/pending。
- [x] projection backfill 的 trusted revision 合同和 trigger contract 已有强回归断言；frontier trigger 也改为完整 normalized DDL 比较。
- [ ] foreground pressure/defer 只能压制 optional history/linked discovery，mandatory recovery 的完整 bounded fairness/配额仍需独立长时验证。
- [x] admission 微基准已在同一 2,100-row SQLite fixture 上比较 optimized/compatibility；尚非完整 Graph/durable E2E。
- [ ] phase trace 尚不能完整拆出 SQL、JSON decode、SQLite busy/write lock、state/file lock 和 durable completion 分布。

### 第二轮审查新增的硬性修订

- [x] trusted/fallback 的所有资格谓词把 `NULL/missing` 视为 untrusted，并检查 `projection_revision = canonical_revision`；不只检查 trust/admission bit。
- [x] P2 API/SQL contract 已落地：candidate SQL 不选择 JSON；最终 selection 前只返回 ID/scalar hint，最终 selection 后 hydration，并重新按 durable state 决策。
- [x] trusted/fallback 生产路径已统一为同一 canonical lane/order；shared-chat stale turn/checkpoint 有一致性测试。
- [x] malformed/contradictory session 不能只 `continue` 后由单一 `handled=true` 掩盖；hot admission 已返回 durable corruption disposition，`pollOnce` 写入有界 per-chat recovery gate，且有 only-corrupt-session 回归测试。通用 fallback 的 scan/page/byte budget 仍未实现。
- [ ] `EXPLAIN QUERY PLAN` 必须针对实际生成 SQL、固定 schema/statistics，断言目标 index、无意外 temp sort/full scan；`IF NOT EXISTS` 不能证明旧 index/trigger body 正确。
- [ ] fallback keyset 必须有明确的总扫描/invalid-prefix预算，不得只依赖 context cancel 作为边界。
- [ ] account/global read-429 的 scope、durable gate key、healthy-chat policy、Retry-After 上限和 pending local replay 必须有明确 contract；当前实现仍主要是 chat-local gate。
- [ ] Docker 429 必须走完整 `Bridge.Listen` 主循环；单独调用 poll helper 只能是 unit test。窗口吞吐必须按 terminal completion timestamp 裁剪，teardown drain 单独计数。
- [ ] Docker fixture 必须固化 session content hash、SQLite WAL/logical snapshot 边界、credential denylist/扫描和最小权限；当前 source drift 检查不足以证明跨文件同一时刻。
- [ ] Docker replay 必须记录真实 corpus 的变换/排除比例，并覆盖 attachment/hosted/control/deferred/pending/outbox 形状；CI 未执行该 E2E 时只能标记 manual diagnostic。

### 第二轮审查收敛出的 P0/P2 闭环

- [x] poll attempt/capability/terminal CAS 增加等价的 durable session binding fence；rebind/close/chat migration 后旧 attempt 的 completion/abandon 不能提交，不能只依赖内存 registry。
- [x] projection trust 采用完整 schema/trigger/index contract；旧/部分/过期对象一律 untrusted，不再用字段名 substring 或 `IF NOT EXISTS` 作为安全证明。
- [ ] P0 设定 mandatory recovery 的保留配额、defer 上限和可测最大等待；pending replay、gap、continuation、owner recovery 不得被 optional 或 foreground work 饿死。
- [~] pending-page local replay 已有 scheduler 级证明：普通 durable receipt 可在 Graph gate 下本地 replay，`invalid_record`/`oversized_record` 等仍需 Graph 的页面遵守 deadline；完整 listener、重启和 account/global scope 验收仍未完成。
- [x] hot admission 的 malformed/contradictory session 已有可观察 `durable-corrupt-with-disposition`（blocked/recovery gate），`handled=true` 不再隐藏“唯一坏 session”场景；其他 durable 表和通用 fallback 仍需各自 disposition 矩阵。
- [x] 计划中的 Amdahl 计算统一使用 `1 / ((1-f) + f/S_local)`；本轮只报告 admission 局部实测，不把 microbenchmark 倍数外推到 listener。

## P0：先修 correctness barrier

- [ ] 使用 typed throttling 语义持久化/读取 `Retry-After`、`BlockedUntil` 和 scope；不以错误字符串包含 `429` 作为唯一判断。
- [ ] 明确 backlog 优先级：
  - [x] pending page 和本地可 replay 的 durable work 不受 future Graph 429 阻塞；
  - [x] head/continuation/gap 遵守 `BlockedUntil`；
  - [x] malformed/unknown/untrusted recovery 保守视为 active；
  - [ ] chat/account/global 429 不阻塞健康 chat；
  - [ ] 超大 Retry-After 不得造成永久不可唤醒；重启保留 deadline，到期可重新 admission。
- [ ] 将 optional 与 mandatory maintenance 分开：
  - [ ] pressure/defer 只延后 optional tail/discovery/title；
  - [ ] 廉价 durable probe 保证 mandatory recovery 可获得 bounded quantum；
  - [ ] marker/checkpoint 只在 owner-fenced 成功后清除；
  - [ ] fair cursor/defer 使用 owner + expected-value CAS；
  - [ ] 连续 foreground work 不得让 mandatory history/linked repair 永久饥饿。
- [ ] 修正 projection repair：
  - [x] valid row backfill 后 `projection_trusted=1` 可观察且不被旧 trigger 立即清零；
  - [x] 缺失/过期/不完整 trigger 默认拒绝 trusted fast path；
  - [x] trigger 校验使用完整 normalized DDL body contract，不是字段名 substring；
  - [x] raw writer、旧 writer、ABA replacement、live-writer race 保持 canonical authority。
- [x] session rebind/close 使旧 poll attempt 的 terminal commit 变为 no-op；owner/lease/attempt/binding/revision/frontier CAS 全部保留。

## P1：建立可归因观测和 paired baseline

- [ ] 记录 admission lane、SQL rows scanned/returned、JSON rows/bytes decoded、hydrated IDs、fallback/repair/refill。
- [ ] 记录 SQLite query/transaction/busy/write-lock、state/file/session lock wait/hold。
- [ ] 记录 claim attempt/won、duplicate claim、CAS rejection、turn creation、terminal durable completion。
- [ ] 记录 Graph method/status/scope/Retry-After/RTT/bytes/marshal/decode，以及 outbox 2xx/unknown/429/503；unknown 永不自动重发。
- [ ] 成功事件不能被 `<10ms` 过滤或固定 1024 条上限截断；保留完整计数和分布。
- [ ] 以 terminal durable completion timestamp 计算 `durable_completed/s`；executor start/fake acceptance 仅作辅助。
- [ ] 固定 fixture/source/harness digest、Go/SQLite/CPU/编译参数和窗口；source drift 自动降级为 diagnostic。
- [ ] 用相同 seed、fake RTT/payload、并发和调度边界建立干净 base/candidate 的 cold/warm paired matrix。
- [ ] 建议收集 cold 20 次、warm 100 次或足够长 steady window，报告 median/p95/离散度和消息对账；不能用 teardown 后 `completedDelta` 冒充窗口吞吐。

## P2：真正的 scalar ID-only + final selected hydration

- [x] 以等价的 scalar `SessionContext` admission row 表达 identity/status/chat/order、projection trust/revision 和安全 idle 摘要。
- [x] trusted SQL `SELECT` 不读 `sessions.json`/`chat_polls.json`，不调用 JSON1，不逐行 decode；保留 partial index 和 keyset cursor。
- [x] 保留 64 的 bounded headroom、公平性和 recovery slot；最终排序/决策后的 work quantum（默认 8）在 hydration 前确定。
- [x] 只为最终 selected IDs（最多 8，另加 control/必要 shared-chat/control slot）读取 canonical session/poll/turn/checkpoint；shared chat 的同 chat session 行是为 stale turn/checkpoint fence 保留的必要例外：
  - [x] partial hydration 不传给会删除未选中 map 的全量 Save/Update；
  - [x] hydration 后重新校验 canonical binding/status/frontier/attempt，并重新计算 poll decision；
  - [x] 不一致 row 丢弃并从 headroom refill；已加入 rebound/close 回归测试；
  - [x] missing/malformed/untrusted row 走 bounded canonical recovery，不挤掉健康 quota、不静默漏 due row。
- [x] `decideInboundPoll`、排序/fairness 所需字段无法由 scalar 精确覆盖时保守 fallback，未把隐式 JSON 读取伪装成 ID-only。
- [x] 保留 shared-chat 去重和单 durable frontier；control binding/global mutation 时丢弃旧 candidates，最多进行有界重新 admission。
- [x] targeted refresh 保持保守；shared-chat session/turn/checkpoint stale fence 有 targeted refresh 测试。

## P3：backlog probe 双 lane

- [ ] 先用 `EXPLAIN QUERY PLAN`、query/lock metrics 证明热点，再考虑减少 canonical scan。
- [x] chat_polls trusted scalar lane 覆盖 frontier、pending page、retry/deadline、attempt/validity 等 admission hint；partial indexes 已安装并在 fallback probe 测试中检查。
- [x] chat_polls untrusted/missing/NULL/old-writer row 走同一 canonical fallback；scalar false 不能遮蔽 canonical true。
- [x] row-local trust/revision contract 通过 adversarial parity 后，健康页不再对全部行跑第二次 canonical scan；canonical fallback 本身保留。
- [x] turns trusted scalar active-status lane 使用 partial/index；unknown non-empty、untrusted、NULL、canonical/scalar 冲突保留 active/fallback。
- [x] “无 active turn”遇到 scope 内 untrusted row 会 fail closed，不能仅查 trusted lane 得出 false negative。
- [x] inbound_events 暂不简化为 `status IN (...)`；未完成 turn_id/source projection、terminal parity、旧 writer fallback 前不改动它。
- [x] 不使用 `state_json_revision` 作为 split-table generation；使用 row-local revision/trust。

## P4：暂不扩大调度边界

- [x] 初期保持 `maxWorkChatPollsPerCycle=8`、`maxConcurrentWorkChatPolls=4`、retry/ordinary/operational fairness 和每 chat action 上限。
- [x] 不扩大 flush cap；历史 cap=2 与 cap=8 的 completed 结果相同，主瓶颈不先假定在 flush。
- [x] 没有在 admission 优化中混入并发/quantum 调参；后续若要改变，每次只改一个变量。

## 测试计划：Store / projection / SQL

- [ ] scalar trusted 与 legacy JSON oracle 当前已有混合 lane/顺序测试及 2,100-row benchmark；0/1/8/16/64/255/256/257/500/2100 全矩阵尚未全部固化。
- [x] work trusted lane 断言不读 `s.json`、不调用 JSON1、decoded=0；selected hydration 断言只读最终 quantum 及明确 control/shared-chat slot。
- [x] retry/ordinary/operational 混合、并列排序、连续新到达和 retry lane 不饿死 ordinary 的回归测试已覆盖。
- [x] 缺失 poll、shared chat、binding mismatch、inactive session、invalid prefix、坏 JSON、wrong type、cancel、reopen/restart 已有对应回归；超大组合矩阵仍需后续扩充。
- [x] trusted/missing/NULL/stale projection；JSON-only/scalar-only old writer、旧/缺 trigger、mixed schema、ABA、旧 revision、CAS race 已有覆盖。
- [ ] backfill 0/255/256/257、已有正 revision、crash/restart、cursor/marker CAS、live writer conflict 仍未全部固化；已验证 bounded page、positive revision、marker/cursor 保留及 repair trust=1。
- [x] `EXPLAIN QUERY PLAN` 已断言关键 fallback probe 使用 untrusted partial index；实际生成 SQL 的完整 plan/no-temp-sort 矩阵仍需补齐。
- [x] active turn queued/running/terminal/unknown/NULL session/status conflict 以及 registry/recovery 相关路径已有对应测试。
- [x] chat poll frontier/recovery/attempt/pending/429 及 first no-hit/canonical fallback 关键分支已有回归；全组合矩阵仍需扩充。

## 测试计划：Bridge / scheduler / durable safety

- [x] control no-op、selected mutation、session rebind/close、control binding/global mutation；旧 candidates 不能执行，新 binding 下一轮可 admission。
- [x] selected partial state 不会被全量 Save/Update 覆盖未选中数据。
- [x] owner takeover、lease expiry、old attempt completion/abandon、revision adoption、frontier/receipt CAS 已有回归；旧 owner 只能 no-op，新 owner 可 reclaim。
- [x] crash barrier：Graph page staged、pending page commit、inbound claim、turn create、executor start、terminal completion、POST 前/后、accepted-but-disconnected 已有对应测试。
- [x] pending page 在 future 429 下 local replay、head/continuation 的 deadline gate 和恢复已有测试。
- [ ] 一个 chat 429、另一个 chat 健康时，健康 chat 持续推进；account/global 429 恢复窗口中不丢失已准备的 durable work。
- [ ] mandatory history/linked recovery 在持续 foreground pressure 和 optional defer 下有 bounded fairness；optional 不抢占 durable backlog。
- [x] retry/operational/ordinary 各有保底；chat-local Graph error 不取消兄弟，process-wide store/lease error 停止新 reservation 并完成已启动 worker 的 fenced cleanup。
- [x] ACK、marker、final 独立；unknown POST、429、503、timeout 不自动重发或重复 durable transition。

## 测试计划：metrics / regression / Amdahl

- [ ] 健康 steady path 的 full-state fallback=0；JSON decoded/selected、query rows/selected、lock hold/wait 可比较。
- [ ] 覆盖首行命中、末行命中、完全无命中、大量 invalid prefix；不能只测 LIMIT 1 命中。
- [ ] base/candidate 同一 fixture 的 admission p50/p95、alloc、JSON bytes、logical rows、WAL、lock wait/hold、durable completed/s。
- [ ] 预注册性能门槛：healthy admission p95 至少下降 30%，端到端 durable completion throughput 至少提升 20%；只有局部 admission 变快时必须明确不通过 E2E 门槛。
- [ ] 使用 Amdahl 分解：按实际阶段占比计算上限，不把 microbenchmark 倍数外推到 listener；功能/锁等待/WAL 有回退就停止扩大范围。

## Docker 真实数据形状与故障实验

- [ ] 从宿主复制 SQLite、Codex 最近历史 JSONL 和 sessions 结构到一次性 immutable fixture；复制前后 manifest/content digest，任何 drift 拒绝 acceptance。
- [ ] Docker 只挂载只读 source fixture 和新的 mutable runtime/state；使用 `--network none`；fake Graph/fake executor 仅记录请求和 durable ledger，不读 token、不启动或替换 live helper。
- [ ] 运行完整 listener 主循环，不用 smoke test、单一 poll helper 或只调用 429 poll 函数；固定 `8/4` 调度边界，隔离 admission 变化。
- [ ] 使用真实数据的消息长度、chat/session/turn/inbound/outbox 分布；每条 fixture 消息建立 expected/served/inbound/claim/turn/completion 对账。
- [ ] no-429 paired base/candidate：warm-up 后从明确 cycle boundary 计 terminal durable completion；重复预注册次数，记录每阶段和总吞吐。
- [ ] 高强度 account/global read-429：fake `Retry-After` 采用短、可控、可加速/虚拟时间窗口；覆盖 90%/99% 429、连续窗口、偶尔成功窗口、chat-local/account-global scope。
- [ ] 独立覆盖 POST 429/503/timeout/unknown accepted-disconnect；只验证成功窗口中已准备 durable work 准确推进，不自动重发 unknown POST。
- [ ] restart/owner takeover/source drift/duplicate page/late response 作为独立 fault run；结束核对 zero loss、zero duplicate、zero unauthorized claim、zero orphan attempt、source unchanged。
- [ ] 输出 `durable_completed/s`、2xx observed/s、fake remote modeled/s、GET/POST attempts、429/503/unknown、claim/CAS、query/JSON/lock/phase 分布；不把 fake acceptance 叫真实 Graph accepted。
- [ ] source drift、未完成 drain、对账不一致、混合时刻 snapshot 一律失败或 diagnostic，不能用局部高数字覆盖。

## 二次审查和迭代闸门

- [ ] 将本 checklist 提交至少 4 个全新、无上下文 reviewer：correctness/failure modes、SQLite/query plan、scheduler/429/fairness、Docker/metrics/test coverage。
- [ ] 对每个 reviewer 记录证据、推断、未验证项、阻塞项和具体修订；不接受只有“看起来安全”的结论。
- [ ] 若发现漏发、重复、永久饥饿、stale binding、false-negative fallback 或测量偏差，先修 checklist/测试，再开始下一轮 reviewer。
- [ ] 二次意见一致后，再由至少 2 个 reviewer 做最终 go/no-go；有未解决 blocker 时不发布性能结论。

## 落地顺序与完成条件

- [ ] 先补 P0 correctness tests，并修正确认的 P0 缺口。
- [ ] 再补 P1 metrics 和 paired baseline，确认当前最大本地可控热点。
- [x] 实施 P2 ID-only + final-selected hydration，逐项通过当前 store/bridge/durable 回归；完整故障矩阵仍按下方残留项保留。
- [ ] 实施 P3 backlog trusted dual lanes；每个 query 变更先 parity、再 plan、再压力测试。
- [x] 运行本机 focused normal/race/vet；store 全量 race 未作为通过项伪报，已执行定向 race。
- [ ] 运行隔离 Docker no-429 与高强度 account/global-429；source drift/fixture 不完整时修复 harness/fixture 后重跑，不降低 acceptance。
- [ ] 只有安全边界、正确性、故障矩阵、source immutability、paired performance gate 全部通过才宣布完成；否则保留已证明的局部收益并明确未完成项。

## 本轮执行记录（2026-09-11）

- [x] 复核依据：沿用此前 6 个无上下文 reviewer 对历史提交、SQLite/query、scheduler/429、Docker/E2E 的结论；本轮再次尝试新增 reviewer 时达到 agent thread limit，未把失败的启动当成 reviewer 结论。
- [x] P2 hot path：listener 使用 combined scalar admission；trusted work candidate 不读取 sessions.json，最终最多 8 个 work chat 才做 canonical hydration；control/shared-chat refresh 仍保留。
- [x] correctness：补齐 shared-chat stale turn/checkpoint 清理、selected hydration 后 rebound/close headroom refill、schedule projection marker/cursor 在普通 full-state write 后保留。
- [x] schema：frontier hint trigger 从字段 substring 检查改为完整 normalized DDL body contract；覆盖旧定义和“同时包含字段名但语义错误”的 near-miss trigger。
- [x] fallback：生产 untrusted path 保留单一 canonical JSON lane，避免 trusted-first/双 LIMIT 改变全局排序、重复 malformed quota 或饿死 recovery。
- [x] 回归结果：GOPROXY=off go test ./internal/teams/store -count=1 -timeout=20m 通过（56.158s）；GOPROXY=off go test ./internal/teams -count=1 -timeout=20m 通过（207.573s）；关键 store race 通过（117.055s）；定向 trigger/refill 通过；go vet ./internal/teams ./internal/teams/store、git diff --check、全仓 compile-only 和 Docker script bash -n 通过。
- [x] 局部 benchmark（AMD Ryzen 9 7950X，同一 2,100 chat SQLite fixture，3 次）：optimized combined admission 约 8.299 ms/op、495,696 B/op、3,973 allocs/op；compatibility JSON 约 5.480 s/op、2,585,072 B/op、5,953 allocs/op。这是本地 admission bound，不是 Graph 或端到端 durable completion throughput。

## 尚未闭环的验收项（必须保留）

- [x] 本次 Docker real-data preflight 使用已有 immutable 副本执行，因缺少 /tmp/codex-teams-backlog-audit-226SQG/global-inbound-ledger.json 在启动容器前安全拒绝；没有生成吞吐数字，也没有触碰 live 状态。
- account/global-scope read-429 的 durable gate、短虚拟时间 Docker 主循环实验仍未完成；当前已证明的是 chat-local 429 隔离和 pending local replay。
- 没有可安全使用的“同一时刻完整真实数据” immutable fixture（SQLite WAL、registry、Codex session index/history 等缺少完整配对），因此没有运行真实数据 Docker acceptance；不能用 synthetic smoke 或 live rc.45 替代。
- 完整 phase/query/lock metrics、实际生成 SQL 的所有 EXPLAIN QUERY PLAN contract、fallback 总扫描预算、malformed durable row 可观察 disposition、mandatory recovery 长时公平性尚未达到发布门槛。
- 因上述未闭环项，本轮只确认 scalar admission 的可重复局部加速与正确性回归通过，不宣称 Teams 端到端消息吞吐已提升某个固定倍数，也不宣称所有 429 场景已验收。

## Amdahl 预期（不是承诺）

- [ ] 先用观测得到实际阶段占比，再计算上限；不使用“JSON 子路径快 100 倍”推断全局快 100 倍。
- [ ] 若 work candidate/hydration 占 poll 的 10%–40%，P2 的理论上限约 1.1x–1.7x；实际受 lock contention、Graph RTT、handler 和 maintenance 限制。
- [ ] 若 active-turn/chat-poll 扫描占 durable admission 的 20%–60%，P3 理论上限约 1.25x–2.5x；必须以 query/lock trace 验证。
- [ ] P2/P3 收益有重叠，不能简单相加；只有 paired Docker durable completion/s 达标才称总体加速。
- [ ] 安全修复即使增加本地成本，只要消除死循环/漏发风险也优先保留，并重新计算真实吞吐。

## 2026-09-12 review loop：可靠优化落地计划

### 评审结论与不可越过的验收边界

- [x] 第一轮由四个独立、无上下文 reviewer 分别复核 scalar admission/projection、poll/429/lifecycle、性能/Amdahl、测试/Docker；一致结论为 NO-GO。
- [x] 第一轮确认的高风险点已登记：坏 session-only 行可能返回 `handled=true` 空候选；dedupe 的原生 session 查询不能改变 JSON canonical 语义；pending page 可能在 429 后忙循环；projection fallback 可能因单个不可信行退化为全量 JSON；完整 listener/硬重启/unknown POST/全局 429 尚未被 Docker 验收。
- [x] 第二轮 reviewer 已逐项审查下面的具体计划，并明确“可以落地 / 必须收窄 / 不应落地”；四位 reviewer 一致要求先补齐 durable disposition、residual trust、预算和完整 listener 验收。
- [ ] 任意阶段发现漏消息、重复消息、旧 owner/旧 binding 写入新状态、未知 POST 自动重发、frontier/lease/CAS 语义变化，立即停止性能优化并回退该阶段。

### P0：先修正确性，再开启热路径

- [ ] 为“仅存在坏 due session、没有健康 session”增加 store + `pollOnce` 端到端测试；结果必须是三态 `authoritative / legacy-compatible / durable-corrupt-with-disposition`。坏 durable 行不能用空候选伪装成功，也不能无条件信任陈旧 registry；只能进入明确的 per-chat recovery/quarantine/defer，或在有明确兼容证明时交给 legacy oracle。
- [ ] 为 projection trusted lane 增加 adversarial parity：直接 SQL 同时伪造 canonical JSON、scalar、revision/trust 的 near-miss；trusted lane 必须 fail closed 并回到 canonical JSON oracle。
- [ ] 为 JSON fallback 增加明确的扫描/时间预算和 durable defer/wake 结果；预算耗尽不能静默返回空候选，也不能在下一轮无条件重复同一坏前缀。
- [ ] 复核并测试旧 SQLite backfill 对 JSON 缺失字段的处理；只更新 JSON 明确提供的字段，保留旧 scalar fallback 语义，迁移前后比较 schedule/retry/pending/attempt。
- [ ] 不在本阶段改变 ACK、marker、final、unknown POST、owner/lease/attempt/frontier CAS、head/continuation 单 frontier 或 Graph/SQLite 事务边界。

### P1：低风险、可回退的本地热路径优化

- [ ] 将 `SessionTranscriptDedupeSnapshot` 的调用方 context 透传至所有 SQLite 查询；取消/短 deadline 必须可中止查询，不得重置为 `context.Background()`。
- [ ] 只在新增的 session-identity projection trust/version 或等价 residual-lane 证明成立时使用 `outbox_messages.session_id` scalar index；`trusted-v1` 现有契约允许 `session_id` 缺失，不能直接证明不会漏行。native 读每一行仍做 JSON identity/decode 校验，发现不一致或缺失 residual proof，清空部分结果并完整回到 canonical JSON oracle。
- [ ] 保留 inbound/transcript-delivery/helper-delivery 的 JSON session 语义，除非先建立对应 projection trust contract；不把未经证明的 scalar owner 当作 canonical owner。
- [ ] 为 native/legacy dedupe 建立逐字段 parity、JSON-only/scalar-only、contradictory row、marker revoke、取消和重启测试；为实际 SQL 加 `EXPLAIN QUERY PLAN` 断言目标 session index。
- [ ] 对 history/linked 的重复 backlog gate 只做 snapshot 传递：一次 admission snapshot + phase 边界一次 revision-aware recheck；不得简单删除 phase gate，也不得在 hook 中再调用 Store 读取。

### P2：只在 parity 与 query plan 通过后优化 fallback

- [ ] 将“任意不可信行触发全表 canonical fallback”改为 candidate-aware 的 canonical fallback；必须先取得 trusted 与 untrusted 的同一 lane/排序/全局 LIMIT 合并结果，不能用两个独立 LIMIT 造成 recovery/starvation/order 改变。返回上限、总扫描 row/page/byte/time 上限、连续坏前缀上限必须分开定义；预算耗尽要产生 durable cursor + defer/wake/quarantine disposition，不能返回空成功。
- [ ] untrusted/malformed/stale/contradictory 行必须保留 canonical JSON oracle；健康 trusted 行不能因为别的 chat 的未来不可信行而重复解析。
- [ ] 对 ready/work/selected/outbox chat/page/turn/earlier-unsent 查询分别保存实际生成 SQL 和 `EXPLAIN QUERY PLAN`；断言目标 partial/scalar index、无意外全表 JSON 扫描或 temp sort。
- [ ] 用 barrier 测试 1/2/4/8 个并发 admission/outbox/maintenance/heartbeat，记录 lock wait/hold p50/p95、BUSY、queue depth；只有 wall-clock 下降且 owner freshness 不回退才保留合并 admission。

### P3：故障与公平性不能由性能实验代替

- [ ] 为 pending page 区分 local-only 与 Graph-dependent replay；429 时 Graph-dependent page 必须有 durable gate，不得因 `PendingPage` 直接绕过 `NextPollAt/BlockedUntil`。gate 需带明确 scope/key/deadline，上限和重启唤醒语义；local-only receipt 才可在 gate 下继续消费。
- [ ] 将 chat/account/global read-429 的 typed durable gate 提升为 P0 contract，而不是只做 Docker 场景测试；account/global 429 只阻止相应 scope 的 Graph-dependent 操作，本地 durable work、healthy sibling 和 control progress 仍须继续。
- [ ] 完整 listener 测试覆盖上述 scope、偶尔成功窗口、短虚拟 Retry-After、零/超大/HTTP-date Retry-After；Graph 不可用时不忙循环、不丢 receipt。
- [ ] 完整 listener 测试覆盖 unknown POST 后强杀/重启，确保只 reconcile、不自动 replay；覆盖 rebind/close 与 hydration、attempt commit、Graph response、completion 的 barriered TOCTOU。
- [ ] Docker 使用同一 immutable real-data fixture、source digest、fake Graph/fake executor、`--network none`；只复制数据库/历史，不读 token、不触碰 live DB、不启动 live helper。
- [ ] Docker acceptance 按 terminal durable completion timestamp 计算 steady throughput；teardown drain、429 retry、Graph observed 2xx、durable completion 分开计数；fixture drift、对账不一致和未闭合 executor run 一律不计绿。

### 分阶段实施闸门

- [ ] Gate 0（合同/测试先行）：先写 P0 disposition、pending/429、projection residual、backfill 和 parity 测试；在此阶段禁止切换 native dedupe/fallback，旧 canonical oracle 保持唯一执行路径。
- [ ] Gate A（有效数据）：对 `valid + trusted` fixture，旧 oracle 与新路径逐项相等；对 `malformed/opaque/contradictory` fixture，不要求伪造“相等”，而要求相同的 `durable-corrupt-with-disposition`（不返回部分 dedupe、不信任 stale registry、不自动重发），focused/store/teams/race/vet 通过。
- [ ] Gate B：运行同一 fixture 的 base/candidate 20 次 cold、100 次 warm；报告 query rows、JSON bytes/decoded、alloc、WAL、lock wait/hold、阶段 p50/p95/p99。
- [ ] Gate C：预先定义唯一判定为“candidate steady durable-completion/s 的 bootstrap 95% CI 下界 ≥ base × 1.20”，并同时要求 duplicate/loss/orphan/unauthorized claim 均为 0；仅 microbenchmark 变快不算通过。若 Amdahl 实测占比不足以达到 1.20×，则诚实判定“局部优化通过、E2E 门槛不可达”，不降低门槛。
- [ ] Gate D：运行 Docker no-429 与短虚拟时间 high-intensity chat/account/global 429 完整 listener；无 token、无真实 Graph POST；对账和 source immutability 全通过。
- [ ] Gate E：完整 `go test ./...`、相关 `-race`、`go vet`、`git diff --check` 和 Docker harness syntax/fixture checks 通过后，才总结收益；未通过则只报告局部已证实收益。

### 最终可执行的实现边界

- [ ] Stage 0 只新增契约/测试/观测，不改变运行时语义；任何 Stage 0 blocker 都停止后续优化。
- [ ] Stage 1 先修坏 session、backfill、pending/429 gate 和 fallback budget；每个修复必须有 durable disposition、restart 和完整 listener 测试。未完成时继续使用 canonical JSON，不启用 native dedupe。
- [ ] Stage 2 建立独立 `outbox_session_projection_trust` 版本/审计和 residual lane：JSON 有 canonical `session_id` 但 scalar 缺失/不确定的行必须进入 residual canonical lookup；只有 residual 为空且 marker trusted 才使用 scalar session index。任何 contradiction 清空部分结果并回到 canonical oracle。
- [ ] Stage 3 仅在 Stage 2 parity/EXPLAIN/lock gate 通过后启用 native dedupe；history/linked snapshot 合并和 candidate-aware fallback 仍逐项 feature gate，不能作为隐式副作用。
- [ ] Stage 4 才运行 paired Docker throughput/429/hard-kill acceptance；测试 harness 不得把 fake Graph acceptance 或 teardown drain 计入主吞吐。
- [ ] 所有 Stage 的失败动作都是关闭 feature gate/回 canonical path，而不是放宽校验、重试 unknown POST 或删除 durable fence。

### 本轮 review/执行记录（2026-09-12）

- [x] 第一轮 reviewer 结果已收齐；四人均明确 NO-GO，未修改文件。
- [x] 第二轮计划 reviewer 结果已收齐；四人均明确 NO-GO，要求补齐三态 durable disposition、session identity trust/residual lane、fallback 三类预算、typed account/global 429、pending page Graph gate、完整 listener/hard-kill/digest acceptance。
- [x] 计划修订：将上述项目提升为 P0/硬门槛；native dedupe 和双 lane fallback 在 trust/parity/预算证明前禁止实现；性能门槛改为 paired durable completion 的 95% CI 下界，而非 microbenchmark 倍数。
- [x] 最终计划复审意见已收齐；两位 reviewer 指出 parity/disposition 与性能门槛文字仍有歧义，且当前 pending/429 代码尚未满足硬门槛。
- [x] 最终计划修订：增加 Gate 0 和 Stage 0–4；明确 valid fixture 做 oracle equality、corrupt fixture 做 disposition equality；明确唯一的 95% CI ×1.20 判定；将 session identity residual closure 和 typed pending/429 gate 置于 native dedupe 之前。
- [x] 代码实现：完成 projection trust/session residual 的 fail-closed 修复、full-state write 的 marker/cursor/revision 保留、旧 helper trigger 精确替换、only-corrupt-session durable disposition、control poll 隔离，以及 pending-page local/Graph-dependent 的 scheduler 分流；未实现 fallback 扫描预算和完整 account/global typed gate。
- [x] 测试与基准：store 全量回归、Teams 定向回归、projection/dedupe/control/pending/disposition focused tests 已执行；最新完整 Teams 回归和本轮基准仍在验收中，未以旧结果替代本轮结果。
- [ ] Docker acceptance：待填写。

### 本轮增量修订与验证（2026-09-12，第二轮实现）

- [x] `TeamsOperationalBacklog` 增加 trusted scalar observation lane：turn active 状态、chat-poll recovery/attempt/frontier 和 future-429 gate 均只读取已通过 marker + row-local revision/trust/admission fence 的 scalar；任一不可信行仍回到原 JSON oracle。
- [x] scalar lane 保留 `PendingInbound` 的 canonical JSON/terminal-turn 交叉校验；没有未经证明地把 inbound_events 简化成 status-only 查询，避免 pending inbound false negative。
- [x] 增加 trusted scalar 与 marker-revocation/contradictory JSON parity 回归，覆盖 active turn、pending inbound 和 operational frontier。
- [x] `ForceCatchup` 现在必须服从 durable `BlockedUntil`，不能把 429 retry hint 变成 tight Graph retry；新增 scheduler 回归。
- [x] queue-only poll 的同步/Once executor 旁路已关闭；stats delivery 保留 durable outbox 但不在 read worker flush；staged fork child poll 继承 work queue-only context；control dashboard view 不在 queue-only deferred input 中提前清除。
- [x] SQLite session/turn/inbound/workflow/thread snapshot helper 透传 caller context；取消和 phase deadline 不再被内部 `context.Background()` 覆盖。
- [x] 本增量 focused validation：`GOPROXY=off go test ./internal/teams/store -run 'TestSQLiteOperationalBacklogUsesTrustedScalarsAndFailsClosedOnRevocation|TestSQLiteOperationalBacklog' -count=1 -timeout=15m` 通过；`GOPROXY=off go test ./internal/teams -run 'TestTeamsQueueOnly|TestInboundPoll|TestPollStagedFork|TestRetryAfter|Test.*OperationalBacklog|Test.*Pending.*Page' -count=1 -timeout=20m` 通过；`git diff --check` 通过。
- [ ] 本增量仍不宣称 account/global typed 429 durable gate、完整 listener Docker fault run、hard-kill 同一 fixture 或 paired E2E durable-completion throughput 已闭环；这些仍是发布前硬门槛。

### 本轮 review 收敛与实际落地（2026-09-12，最终 correctness pass）

- [x] 四个第一轮无上下文 reviewer、三位第二轮 reviewer 和最终 correctness/performance/test-Docker reviewer 的 NO-GO 意见已逐项收敛；没有把局部 microbenchmark 当作端到端验收，也没有扩大并发、flush 上限或削弱 owner/lease/attempt/frontier CAS。
- [x] 修复 queue-only 的调用边界：只有真实 `Listen` 的 poll phase 携带 listener marker，后续 queued-turn phase 才启用 work callback 的 queue-only；直接 `pollOnce`/Once 测试和同步调用仍有明确 consumer，不会把 turn 留在无人消费的 queued 状态。普通 work poll 不再被误套整个 poll phase 的 queue-only 标记。
- [x] 修复 pending-page 的 legacy parity：`Dispositions` 缺失但 `RefetchFailures` 为正时保持 Graph-bound；disposition/refetch 数组长度不一致 fail closed；SQLite JSON fallback 与 trusted scalar lane 一样，在 future 429 下仍暴露无需 Graph 的 pending receipt。
- [x] 修复 opaque ChatPoll liveness：类型损坏的顶层字段采用逐字段恢复，保留独立可解析的 pending receipt；无 receipt 的 opaque head 在没有活跃 attempt（或 attempt 已过期）时由 owner-fenced 一次性转换为保守 gap，避免重复 `acquired=false`；活跃 attempt 仍遵守原 TTL/owner fence。
- [x] 修复 malformed pending repair 的比较假阳性：允许 loader 重新派生的 opaque marker 和时间 location 差异，不忽略 PollRevision、ScheduleRevision、frontier、receipt、attempt 或 gap 证据本身。
- [x] 新增/加强测试：legacy refetch failure、disposition length mismatch、SQLite fallback future-429、partial type-corrupt pending receipt、opaque receipt repair、无 receipt gap recovery、direct poll/real listener queue-only 边界。
- [x] 回归证据：`GOPROXY=off go test ./internal/teams -count=1 -timeout=20m` 通过（`314.147s`）；`GOPROXY=off go test ./internal/teams/store -count=1 -timeout=20m` 通过（`80.898s`）；新增关键场景 `-race` 通过（store `2.506s`、Teams `9.182s`）；`go vet ./internal/teams/...`、全仓 compile-only、`git diff --check`、Docker script `bash -n` 通过。
- [x] 同一 2,100-row SQLite fixture 的 3 次 paired local benchmark：work scalar 中位约 `16.195ms` vs JSON `1.925s`（约 `119x`）；ready scalar 中位约 `9.882ms` vs JSON `2.035s`（约 `206x`）；combined scalar 中位约 `9.371ms` vs compatibility JSON `5.620s`（约 `600x`）。这证明 durable admission 的局部热点显著下降；不等价于 Graph 或 Teams 端到端吞吐提升 `600x`。
- [ ] Docker 完整 listener、同一 immutable real-data fixture、account/global read-429 短虚拟时间、POST unknown/hard-kill 和 paired durable-completion/s 尚未闭环；当前 fixture/preflight 缺少完整 ledger/WAL/registry/Codex 配对时仍必须拒绝 acceptance，不能用 synthetic smoke 或 live rc.45 代替。

### 本轮增量修订（2026-09-12，真实数据启动瓶颈与安全边界）

- [x] 复核结论已收敛：三位首轮 reviewer 与四位第二轮 reviewer 均指出，真实数据 Docker 的首轮失败主要暴露了启动 projection 全表 JSON 审计、重复兼容 cleanup 的 owner fence、fixture 权限和 listener 验收边界；这些结论没有把未完成的 Docker 诊断误报为性能验收。
- [x] 大型 SQLite outbox（超过 4096 行或单行 JSON 超过 1 MiB）启动只执行 `COUNT(*) + MAX(length(json))`，持久化 `deferred-v1`，继续使用 canonical JSON fallback；不会在 listener 首轮读取/反序列化整张 outbox 表。
- [x] projection 审计状态在长审计前持久化为 `auditing-v1`；取消/崩溃不会把状态留在会导致每次启动重复全表审计的 `unknown`，显式 `RetryDeferredOutboxProjectionAudit` 才重置并恢复审计。
- [x] projection 的小库/显式维护审计仍一次扫描并独立计算 FIFO/session/turn 三种 trust；trusted 结果发布继续检查数据库路径和 durable marker，untrusted/deferred/auditing 不会启用 native fast path。
- [x] listener startup legacy history-gate cleanup 改为 owner/lease-generation fenced：SQLite 每页在写事务中校验当前 active lease，页提交后重新校验；JSON fallback 在同一 state-lock 的 load/modify/save 内校验；旧 owner takeover 只能得到 `ErrControlLeaseNotHeld`。
- [x] sender-side `import-needs-attention` 判定补回精确 body marker；只有已知旧 history-gate marker 才跳过，真实的 needs-attention 内容不会因为 kind/turn 前缀相同而漏发。
- [x] real-data Docker runtime 改为 host UID/GID 的私有 bind mount，不再对复制的历史 fixture 执行 `chmod a+rX`，删除 disposable Docker volume；继续保持 source fixture 只读、runtime 独立可写、`--network none`、不读取 Teams token。
- [x] 新增回归测试：大 outbox 启动 defer/显式 retry、audit interruption/resume、JSON/SQLite owner fence、SQLite page-boundary takeover、真实 import attention classifier；新增测试及既有 store 全量回归通过。
- [x] 本轮验证：`go test ./internal/teams/store -count=1` 通过（约 135 秒）；`go test ./internal/teams -count=1` 通过（约 460 秒）；新增 projection/cleanup 测试通过；`go vet ./internal/teams/...`、`git diff --check`、Docker `bash -n` 通过。
- [ ] Docker real-data diagnostic 尚待本轮代码完成后重跑；只有首个完整 `Bridge.Listen` cycle、durable completion 对账、source immutability 和无 token/无真实 POST 均通过，才填写 throughput acceptance；source drift 仍只允许诊断，不得转绿。

### 本轮最终安全收敛（2026-09-13）

- [x] 最终无上下文 reviewer 复核了 correctness、SQLite/query plan、scheduler/429/fairness、Docker/metrics/test coverage；结论不是“局部 benchmark 足够”，而是 scalar hot admission 可保留、完整发布仍 NO-GO。
- [x] 按 reviewer 发现补齐上传 session 的未知最终 PUT 结果保护：状态查询返回 404/410 也不再被解释成可新建 session，避免最终 PUT 已成功但客户端丢响应时重复 POST。
- [x] `RequeueTurn` 不再把 `Ignored` inbound 复活；它会把正在运行的 turn 收敛为 `Interrupted`，保留终态并清除陈旧 retry metadata。
- [x] 中断/恢复路径增加 turn、inbound、session/chat provenance 校验；跨 session、跨 chat 或错误 Turn 链接不会清理/执行别人的 inbound，并会进入明确的恢复通知路径。
- [x] `QueueTurn` 对显式但不存在的 inbound 返回 `ErrInboundNotFound`，JSON/SQLite 都不再创建孤儿 Turn；旧的已存在 Turn 仍走幂等读取路径。
- [x] 新增并通过跨 backend 回归：missing-inbound no-orphan、Ignored requeue、foreign inbound interruption、foreign recovery no-Graph，以及 unknown-final-PUT no-second-session。
- [x] semantic projection trigger 已覆盖 JSON/scalar contradiction 与主键变更，projection marker 版本已递增；trusted admission 的 due gate 仍以 canonical JSON 校验，避免 scalar 漂移隐藏真实到期 chat。
- [ ] 仍不能宣称完整 E2E 收口：trusted work/selected hydration 的物理计划仍需实际 SQL observer/scan budget；account/global typed 429、完整 listener hard-kill、同一 immutable real-data Docker fixture 和 paired durable-completion/s 仍是硬门槛。
- [ ] 本轮不得把约 119x/206x/600x 的本地 admission benchmark 外推为 Teams 消息吞吐；必须等真实阶段占比和 Amdahl 上限、无 429 paired durable completion 数据。

### 本轮最终验证补记（2026-09-13）

- [x] 修复 opaque pending page 的最后一个 liveness 缺口：`pendingPageToWindow` 失败时不再只清 recovery marker；在同一 owner/revision-fenced transition 中将不可执行 receipt 收敛为 bounded gap evidence，避免下一轮依据同一坏 JSON 重新生成 marker 并形成 CAS 自我循环。正常 receipt 仍保持原样 local replay；JSON/SQLite 共享该语义并有回归覆盖。
- [x] 修复 SQLite foreground 写事务与 owner heartbeat 的快照竞争：`openSQLiteHandle` 对普通写事务使用 `_txlock=immediate`，让读后写事务在第一次读取前取得写 reservation；显式 `ReadOnly` 事务仍保持 deferred，不把普通读变成写锁。owner-loss race 在单独运行和 `-race` 下通过。
- [x] 最新完整回归（包含上述修复）：`GOPROXY=off CXP_RUNTIME_DISABLE=1 go test ./internal/teams/store -count=1 -timeout=20m` 通过（`134.435s`）；`GOPROXY=off CXP_RUNTIME_DISABLE=1 go test ./internal/teams -count=1 -timeout=20m` 通过（`405.815s`）。
- [x] 最新安全门禁：关键 store race 集合通过（`23.530s`），关键 Teams race 集合通过（`7.943s`）；`go vet ./internal/teams/...`、全仓 compile-only、`git diff --check` 和 Docker script `bash -n` 通过。
- [x] 最新同一 2,100-row fixture 的本地 durable-admission benchmark：work-candidate scalar `165.013ms` vs legacy JSON `3.135s`，约 `19.0x`；optimized combined `173.277ms`，而 compatibility JSON 路径在设定的 2s indeterminate budget 内未完成，因此只能报告 `>11.5x` 下界，不能报告精确 combined 倍数。该结果只证明本地 admission 热点收益，不证明 Graph/handler/端到端 message/s。
- [x] 真实数据 Docker 脚本已实际尝试 point-in-time snapshot；连续 3 次组装期间 live helper 修改了 scope SQLite、control SQLite、global inbound SQLite 和 Codex session inventory，脚本按设计拒绝启动容器并返回非零。没有使用 `ALLOW_SOURCE_DRIFT=1` 冒充 acceptance，也没有触碰 live helper 或 live 数据库。
- [ ] Docker 完整 listener/429/hard-kill/paired durable-completion acceptance 仍未闭环：当前工作区没有同时静止的真实 cross-file fixture；必须使用静态、配套的 SQLite/WAL/registry/ledger/Codex snapshot 后再验收。此前 checklist 中的 `119x/206x/600x` 是旧 benchmark 记录，不能覆盖本节最新的保守数据，也不能外推为消息吞吐。

### 本轮增量计划（2026-09-13，owner audit 与 canonical fallback 锁竞争）

#### 第一轮审查收敛出的必须修订

- [x] 三位无上下文 reviewer 分别复核 owner-fenced audit lifecycle、真实数据 Docker fixture/WAL、Amdahl/吞吐候选；没有把未完成的 Docker 诊断当成 E2E 绿灯。
- [ ] 修复 unowned/offline projection audit 的 TOCTOU：入口没有 active lease 不能替代长扫描完成时的 durable no-owner fence；owner 接管后旧审计不得发布 trusted。
- [ ] 给每个 trusted projection marker 持久保存 database identity、SQLite 物理 revision、outbox generation provenance；旧版本 trusted marker 没有 provenance 时必须重新审计，不能永久直接走 native path。
- [ ] 让 turn audit 与实际 Go decoder 使用同等强度的语义检查；`body` 等 JSON 类型损坏必须使对应 trust marker 失效，不能审计为 trusted 后由读取路径静默丢行。
- [ ] 让 projection guard 的写时检查覆盖完整 JSON decode-admission contract，但只放在写触发器中，不把宽 JSON1 表达式重新放回正常读取关键路径。
- [ ] 将 audit 最终短事务的重试限制为可识别的 SQLite busy/locked/snapshot 瞬态和有限次数/截止时间；schema、路径、关闭、owner/claim 等永久错误必须及时返回并保留可接管的 durable claim。

#### 可落地的性能主线

- [x] `SessionTranscriptDedupeSnapshot` 的 canonical fallback 已改为独立 query-only SQLite reader；Store.mu 只负责短 pointer/physical-identity capture 与 read 后 fence，前台 durable schedule/write 不得被整表 JSON scan 持有。
- [ ] 为该 off-lock snapshot 增加 pointer/path/physical identity 变化、取消、读期间 durable writer、native-to-canonical fallback 的 parity 与 race 测试；发现 snapshot 变化时必须丢弃旧结果并重读/失败关闭。
- [ ] 以 Docker 真实数据诊断重新量化 `SessionTranscriptDedupeSnapshot.outbox`、`inboundEventByID`、`turnByID`、completion/write 的 lock wait/hold；只报告 durable completion/s，不把 Graph fake response 或未闭合 executor run 计入吞吐。
- [ ] 只有在以上锁竞争证据闭环后，才实现 deadline scalar fast path：新增独立版本化 deadline trust/proof，已知 writer 在同一事务维护 JSON/scalar/revision；旧、raw、矛盾行回 canonical oracle。保持 retry/ordinary/operational lane、排序、8/4、owner/lease/attempt/frontier/CAS 不变。
- [ ] deadline fast path 必须先做 SQL/Go parity、旧 writer/backfill/restart/CAS、边界时间（now-1ns/now/now+1ns/zero/null/missing/malformed）、EXPLAIN 无 JSON due 解析和 20×cold/100×warm paired benchmark；未达到端到端 Amdahl 门槛则不扩大 scope。

#### Docker fixture 可靠性修订

- [ ] 将真实数据 Docker 实验明确分成逻辑副本诊断与 point-in-time acceptance；SQLite `.backup` 必须先完成，再从副本读取 projection，WAL/SHM 不纳入伪造的主文件 hash。
- [ ] copied Codex session 的 inode/path 会改变；fixture 必须重建 HistoryWatch/ImportCheckpoint 的 source generation、bounded prefix/read-range proof，或检测到 source-rewrite/recovery 需要完整前缀时拒绝/完整复制，不能用 sparse hole 伪装正常 history tail。
- [ ] Docker 继续 `--network none`、source fixture 只读、runtime 独立可写、fake Graph/executor、不读 auth token；短诊断窗口允许记录“fault 尚未恢复”，完整 acceptance 才要求注入的 429/503 成功窗口真正恢复。
- [ ] 对 fixture 的 copy 前后 source digest/metadata、SQLite quick_check/journal mode、history proof、expected/served/inbound/claim/turn/completion 对账增加辅助测试；任何 drift/混合时刻/不完整 proof 一律不能转绿。

#### 第二轮复审闸门

- [ ] 将上述修订后的具体实现方案交给至少 3 个新的无上下文 reviewer，分别审 owner/marker provenance、Docker snapshot/history proof、性能/Amdahl 与测试覆盖。
- [ ] reviewer 必须检查历史提交语义、现有安全边界和失败后的 durable disposition；只要仍存在无限等待、旧 owner 发布、漏行/重复、source proof 失真或测量偏差，先修计划/测试，不进入下一项优化。

#### 执行顺序与验收

- [ ] A：先补 ownerless TOCTOU、provenance、完整 decode/guard、有限重试测试，再实现修复。
- [ ] B：验证 off-lock canonical session snapshot 的锁竞争收益和取消/替换安全性。
- [ ] C：修正 Docker fixture 证明边界，跑 90s 快速诊断和足够长的 fault-recovery acceptance；短实验不强行伪造“已恢复”。
- [ ] D：若 A–C 的量化结果表明本地 due predicate 仍占显著比例，再单独落地 deadline scalar fast path；否则保留为有证据的后续计划。
- [ ] E：完整 store/Teams 回归、关键 race、vet、compile-only、diff-check、Docker shell/fixture checks 全通过，且 durable completion/s 的 paired Amdahl gate 不回退，才宣布本轮完成。

## 2026-09-14 review loop：第二轮复核与增量修复

### 本轮先决 review 结论

- [x] 四位新的无上下文 reviewer 分别复核正确性、SQLite/性能、测试/Docker 和发布安全；第一结论仍按 NO-GO 处理，未把局部 benchmark 或 fake Graph 结果当成端到端验收。
- [x] reviewer 指出的 upload session 风险已定位到具体状态转换：同一次调用在 durable URL 写入后仍可能重新 POST；HTTP 416 后 status query 的 404/410 也可能被外层当成普通可重建错误。
- [x] reviewer 指出的 transcript 风险已定位到 durable disposition：普通永久 Graph 4xx 不能把 linked transcript 伪装成 `Skipped` 后静默完成来源；但也不能让 rejected row 永久挡住同 chat 的后续消息。
- [ ] durable session binding epoch/active owner 的完整 side-effect race、projection audit provenance/TOCTOU、真实 point-in-time Docker acceptance 仍未证明；这些不是本轮局部修复可以声称已解决的事项。

### 已落地的安全修复

- [x] upload session：成功持久化 session URL 后立即转为 durable witness；后续 chunk 404/410 或 status query 404/410 统一进入 indeterminate/no-new-POST recovery，不在同一调用内创建第二个 session。
- [x] upload session：在已跨 Graph 边界后不再把 400/403/404/410 当作“可以安全 reset pending”；保守保留 started/unknown 证据，并用 401 的既有认证路径维持可恢复语义。
- [x] account/global write gate：过期 gate 清理使用实际命中的 winner row 的 `ChatID`；强度更高的全局 gate 不会误清理请求 chat 的本地 gate，后续调用可继续收敛本地过期行。
- [x] transcript permanent 4xx：新增 `needs_attention` durable status；outbox 进入 terminal `Skipped` 以释放 FIFO，linked transcript/helper/artifact 保留失败/需人工修复证据；普通 background retry 被抑制，显式 `publish-history` repair 才能复用稳定 identity 重新排队。
- [x] helper backfill：全量保存后的 helper 回填优先遵守 linked transcript 的 `needs_attention`，不会再用 outbox `Skipped` 覆盖为普通 `skipped`。
- [x] selected poll identity：selected work poll 的后续 safety check 要求 exact session 仍绑定原 chat 且仍是 active/legacy-active 状态；closed/quarantined/rebound 不得回退到 shared-chat sibling。

### 本轮新增测试与已执行结果

- [x] JSON/SQLite + restart：永久 Graph rejection 的 outbox/transcript/helper/artifact 状态、普通自动抑制、显式 history repair、stale attempt rejection；`TestPermanent*` 通过。
- [x] Bridge + fake Graph：linked transcript 收到永久 HTTP 400 后只产生一次 rejected POST，后续同 chat row 仍 sent，transcript 为 `needs_attention`；JSON/SQLite 通过。
- [x] upload：durable session 后 chunk 404/410、416 后 status 404/410 不重建 session；原有 429/no-replay 和 post-reset 矩阵通过。
- [x] selected poll：closed/rebound selected session 不使用 sibling，selected-only hydration/refill 回归通过。
- [x] `git diff --check`、gofmt、focused Teams/store tests 和本轮 upload/gate/transcript/poll tests 通过。
- [x] 当前本地 2,100-row SQLite 诊断 benchmark：work scalar 约 `169 ms/op`，legacy JSON 约 `3.41 s/op`（约 `20x`）；ready scalar 约 `72 ms/op`，optimized combined 约 `165 ms/op`。这是 durable admission 局部下界，不是 Graph 或真实 message/s。

### 二次复核后的放行边界

- [ ] 未完成的 owner binding epoch、projection trust provenance、deadline scalar fast path、完整 Docker real-data/429/hard-kill ledger，不得打勾或包装成已验收优化。
- [ ] 仍需跑全量 store/Teams 回归、关键 race/vet/compile-only，并由本轮 reviewer 的最终反馈决定是否需要第三轮小范围复核。
- [ ] Docker 当前只能在 immutable、配套的 SQLite/WAL/registry/Codex fixture 可验证时给出 durable completion/s；fixture drift、缺 ledger、缺 history proof 或没有真实全 listener loop 时必须拒绝 acceptance。

## 2026-09-15 第二轮独立复核与生命周期回归

### Review loop

- [x] 已启动三位新的无上下文 reviewer，分别审查 owner/schema lifecycle、Graph account/global 429 fairness 与 candidate refill、测试/Docker acceptance oracle；reviewer 不修改共享工作区。
- [ ] 待收集三位 reviewer 的最终意见；若发现新的安全缺口，必须先补代码与回归测试，再进行下一轮独立复核。

### 本轮已执行的修复与门禁

- [x] owner 丢失后的 replacement 测试遵循真实 listener 启动协议：先显式 `PrepareSQLiteSchemaBeforeOwner`，再 claim lease；不通过放宽 `ClaimControlLease` 的 schema fence 来掩盖启动顺序问题。
- [x] 新增 legacy JSON migration marker 回归：迁移写入的 SQLite pointer 对 owner 可见前，`sqlite_schema_preparation_version` 必须已发布为当前版本；紧接着的 lease claim 必须成功。
- [x] 关键 Teams race 集合通过：Graph 首次请求前 fence、takeover 重检、outbox owner fence、history-watch owner-loss，`9.339s`。
- [x] migration marker 单测通过，`0.186s`；JSON/SQLite opaque recovery、scalar admission malformed-field、projection snapshot 与 audit 重点组合通过，`30.171s`。
- [x] 全仓 compile-only、`go vet ./internal/teams/...`、`git diff --check`、Docker 脚本 `bash -n` 通过。
- [ ] 当前完整 `internal/teams` 回归仍在运行，完成后才能更新最终 package gate。

### 仍然不可宣称的验收项

- [ ] Docker immutable real-data listener/429/hard-kill/paired durable-completion acceptance 仍未闭环；真实 fixture 在组装时发生 live source drift 且约 41GB，不能用缩小 fixture 或 `ALLOW_SOURCE_DRIFT=1` 冒充验收。
- [ ] 不将 reviewer 的局部 race、fake Graph、admission benchmark 或未闭合 executor run 外推为真实 Teams message/s；最终吞吐只能由 durable completion ledger 对账得出。

## 2026-09-15 第三轮复核、SQLite liveness 与附件 POST 边界

### Review 收敛

- [x] 三位无上下文 reviewer 完成 SQLite projection/liveness、Graph 429/attachment side-effect、Docker durable acceptance 复核；没有把局部 fake Graph 或 benchmark 当成真实端到端验收。
- [x] 复核确认并修复了两个会把已可恢复工作长期卡住的具体边界：materialized projection 不完整时 liveness cleanup 不能回退读取陈旧 JSON；附件最终 POST 明确收到 401 后，必须只对 exact attempt 原子地清除 started witness 并重新排队。
- [ ] standalone `send-file` 直接 Graph 路径仍有“Graph 接受后、provenance ledger 写入前进程崩溃”的历史窗口；它不属于 durable Teams listener 的本轮修复，若要消除该窗口需要将 CLI 路径纳入 durable outbox，不能用事后 ledger 写入冒充 crash-safe。

### 已落地的修复与测试

- [x] SQLite materialized projection：缺失 required row fail closed；无 materialized marker 的旧库仍保留兼容性 cold fallback；不相关 malformed runtime row 不再阻止对已证明的 lease/owner 做安全清理，并保留原始 opaque bytes。
- [x] stale DriveItem recovery：只识别 `/me/drive/items/<id>` metadata GET 的明确 404/410；清除 stale identity 只作用于 pending attachment attempt，started final POST 仍保持 unknown/no-replay 边界。
- [x] attachment final POST 401：增加 exact owner/attempt/boundary-token fenced 的原子 reset-and-queue；401 后后续 retry 可重新执行，429/transport/408/409/425 等跨 Graph 边界的不确定结果不自动重发。
- [x] 新增并登记 JSON/SQLite store boundary、bridge fake Graph 401、narrow error classifier 与 materialized liveness regression tests；manifest 通过 JSON 解析和 recovery-manifest 校验。
- [x] 完整 store 包回归通过：`GOPROXY=off CXP_RUNTIME_DISABLE=1 go test ./internal/teams/store -count=1 -timeout=20m`，`563.817s`。
- [x] 完整 Teams 包回归通过：`GOPROXY=off CXP_RUNTIME_DISABLE=1 go test ./internal/teams -count=1 -timeout=20m`，`697.413s`。
- [x] 关键 race 通过：store `36.742s`，Teams `13.848s`；`go vet ./internal/teams/...`、相关文件 gofmt、`git diff --check` 通过。
- [x] 全仓 compile-only 通过：`GOPROXY=off CXP_RUNTIME_DISABLE=1 go test ./... -run '^$' -count=1 -timeout=20m`；所有 Go package 均完成编译。

### Docker / 真实数据验收边界

- [x] 解释并记录 `/tmp/cxp-teams-real-data-n2oe6K/attempt-5SRrg8`：这是 disposable real-data Docker fixture attempt，不是 live Teams runtime 数据库；约 `38.34GiB` 来自低 checkpoint offset 导致的完整 Codex JSONL copy，另有约 `3.1–3.3GiB` SQLite 副本。源文件并非 sparse tail，实际占用因此很大。
- [x] 该尝试在 live helper 修改 scope/control/global SQLite 或 session inventory 时被 source-drift guard 拒绝，并在清理后没有修改 live helper/live database；未使用 `ALLOW_SOURCE_DRIFT=1` 冒充验收。
- [ ] Docker immutable real-data listener、全局/账户级 429、hard-kill at Graph boundary、paired durable-completion ledger、fairness/resource quota 仍未闭环；当前不能据此声称真实 Teams 已恢复或给出可信 durable completion/s。
- [ ] 仍需在可验证的静态配套 fixture 上完成 Docker acceptance；在不能停止或静止当前 live helper 的前提下，不复制完整 42GiB 历史数据、不使用不完整 sparse proof，也不把缩小 fixture 结果外推到当前 backlog。

## 2026-09-15 第四轮：真实副本 outbox admission 热路径修复

### 本轮改动

- [x] 将 native `PendingOutboxChatIDsAt` 候选查询改为固定状态字面量的 chat-order 索引超集；候选阶段不再逐行执行 chat/global rate-limit join 或相关子查询，最终限流、retry、unknown-POST、FIFO 仍由每个 chat 的 canonical head check 决定。
- [x] 为 targeted outbox page 和 distinct-chat admission 增加 `outbox_chat_pending_order_idx(teams_chat_id, created_at, id)` partial index；schema preparation marker 从 `2` 升为 `3`，旧 marker 必须先完成补建索引再允许 `INDEXED BY` native 路径。
- [x] 将 untrusted session fallback 从“任意 untrusted session”收窄为“有 canonical/scalar chat identity 且可能影响 work admission 的 untrusted session”；control/fork staging 的空 chat 行不再让每轮 work admission 退回整表 JSON lane。
- [x] 修复 helper-owned DDL 改变 SQLite schema cookie 后 trusted outbox provenance 仍保留旧 cookie 的边界；schema preparation 成功后只刷新已有有效 provenance，缺失/损坏 proof 仍 fail closed 并进入显式 audit。

### 同一份隔离真实 SQLite 副本的量化结果

- [x] 副本数据规模保持真实：约 `3.2 GiB` SQLite、`133,653` queued outbox、`22` 个 pending chat，其中一个 chat 约 `133,629` 条；所有读取/DDL 只作用于 `/tmp/cxp-drain-probe.LT26zx`，没有向 Graph POST，也没有写 live scope。
- [x] targeted control-chat page 从旧的 `5.03–5.67s` 降到 `42–47ms`；EXPLAIN 从 status-first index + temporary ORDER BY 变为 `outbox_chat_pending_order_idx`，无临时排序。新索引在该副本约 `21.8 MiB`，创建是一次性 schema 成本，不计入稳态消息吞吐。
- [x] work-candidate admission 从 `1.4–1.7s` 的 JSON fallback 降到 `85–92ms`；副本中 `3` 个 untrusted session 全是 control/fork staging、`0` 个可路由，最终 `fallback=false`、返回 `55` 个 candidate。
- [x] distinct pending-chat admission 最终约 `72ms`：纯索引候选约 `54ms`，`24` 个候选的 bounded canonical head 检查约 `18ms`。中间实验曾把每行 rate-limit join 与子查询放在强制索引后，测到 `12.9s`；该方案已删除，不能作为当前实现的性能结果。
- [x] 最终 opt-in hot-path diagnostic（schema 已准备、无 Graph）约 `0.59s`；这是 admission/page/SQLite 局部诊断，不是 durable completion/s，也不能外推为真实 Teams 排空速度。

### 本轮测试与门禁

- [x] native budget、targeted page/index plan、candidate/index plan、chat/global rate-limit head enforcement、schema marker `2 → 3` 补建 index 回归通过。
- [x] 既有 Store outbox/FIFO/限流集合通过：`322.622s`；包含 malformed/duplicate、canonical schedule、unknown POST、FIFO、429 gate、large projection/audit 边界。
- [x] 关键 Store race 通过：`200.369s`；包括 hot-poll admission、schema/provenance lifecycle、pending outbox、rate-limit 和跨 backend parity。
- [x] 完整 Store 包通过：`517.529s`；覆盖本轮新 schema marker/index 变化后的全部 store 测试。
- [x] 完整 Teams 包通过：`701.622s`；覆盖 listener、poll/recovery、Graph fake、outbox durable completion 与生命周期集成场景。
- [x] `go vet ./internal/teams/...`、全仓 compile-only、Docker shell syntax、`git diff --check` 通过。
- [x] 最终关键 Teams race 补跑通过：首次 Graph 请求前后 owner fence、durable takeover、history-watch owner-loss、cooperative turn 和 outbox post 后 takeover，`6.782s`。
- [x] 同一隔离真实 SQLite 副本复测：schema preparation `244ms`；native pending-chat admission `130ms`（candidate SQL `70ms`、bounded head checks 约 `59ms`）；targeted native page `69ms`；work admission `55` 个候选、`fallback=false`。结果仍不包含 Graph 或 durable completion。
- [x] 严格 Docker real-data 尝试被 source-integrity guard 正确拒绝：live helper 在约 `5m` 的 fixture 组装期间改变了 `store.sqlite` 及其 WAL/SHM；未启动 listener、未向 Graph POST、未修改 live SQLite；中断后的约 `41GiB` 临时副本已由脚本清理。该结论不能替代 Docker 吞吐验收。
- [x] 记录 harness 低效：约 `24,561` 个 source-proof range 各自启动一次 `dd`，且部分低 checkpoint offset 使 sparse copy 实际读取完整的大型 Codex session；下一步应先做 range 合并/批量复制或使用预先冻结的静态 fixture，不能重复无效尝试。
- [ ] Docker immutable real-data listener、account/global 429、hard-kill at Graph boundary、paired durable-completion ledger、fairness/resource quota 仍未闭环；本轮副本 probe 不得替代该验收。

## 2026-09-15 第五轮：queued-turn 候选分页与选中会话 hydration

### 目标与安全边界

- [x] 将 listener 的 queued-turn admission 从全量 `QueuedTurnStateSnapshot` 改为有界 session-ID 候选分页，再只 hydration 当前候选会话；该分页只是 acceleration hint，不能取代 `ClaimNextQueuedTurn` 的 FIFO、owner、lease、attempt 和 CAS。
- [x] 保留 startup/recovery 等确实需要完整状态的调用；仅优化普通 listener queued-turn drain，不改 inbound claim/complete、cursor/seen、Graph POST unknown-result、单一 durable frontier 或 ACK/marker/final 边界。
- [x] SQLite typed candidate 只有在 turn projection marker 当前且所有 turn row 通过 generation/semantic trust 时启用；旧库、混合版本、损坏/矛盾 row 一律回退 canonical JSON，不让不可信 scalar 隐藏 queued work。
- [x] 候选页使用稳定的 exclusive session-ID keyset 和 bounded inspection；空队列清除扫描游标，候选耗尽后重新从头开始，避免新增会话或 takeover 后永久落在旧游标之后。

### 实现与测试

- [x] 新增 `Store.QueuedTurnSessionIDs` 及 SQLite typed query；增加 marker/current-row trust、limit+1、running-session 排除、空/非法 session ID、稳定分页和 JSON fallback。
- [x] Bridge 只 hydration 选中的 `SessionsByID`，复用现有 claim-boundary start；保留旧全量 snapshot 作为非 SQLite/不可信 projection fallback；移除 listener 中先 `HasQueuedTurns` 再 candidate scan 的重复 turn trust/query。
- [x] 增加 JSON/SQLite parity、untrusted marker/row fallback、running+queued exclusion、65-row 页边界、bounded start-limit/cursor 以及 SQLite bridge admission 回归。
- [x] 对真实副本增加 opt-in candidate timing/row-count 观测；使用 `/tmp/cxp-drain-probe.LT26zx/state.json`（只读副本、无 queued turn）实测 native capability `27.6ms`、`HasQueuedTurns` `0.87ms`、结果 `false`；因此该副本的 `133,653` 条 outbox backlog 不会经过 queued-turn drain，不能把这项结果计入 durable completion/s，真实吞吐仍必须以 durable terminal ledger 对账。
- [x] 同一只读真实副本的 outbox/admission 细分：pending-chat `86.9ms`（candidate SQL 约 `62.7ms`、19 个 head 检查约 `24.0ms`），ready admission `44.9ms`，selected chat/session/turn/checkpoint hydration 合计约 `12ms`，work admission `108.4ms`，targeted control outbox page `47.7ms` 返回 `64` 条；这些是本地 SQLite/read-only phase，不包含 Graph 或 durable completion/s。

### 验收门禁

- [x] focused store/bridge tests 通过：queued candidate stable/native pages、projection fallback、SQLite bridge selected hydration，以及既有 start-limit/shared-budget 测试。
- [x] 新增路径窄范围 `-race` 通过：Store queued-candidate 三项 `13.725s`、Bridge selected-hydration `17.781s`，无 race detector 报告；普通构建下对应 focused tests 通过。
- [x] 完整 Store/Teams 回归在本轮实现后通过：Store `558.188s`、Teams `719.725s`；新增 opt-in probe 之后全仓 compile-only 仍通过。
- [x] `go vet ./internal/teams/...`、Docker 脚本 `bash -n`、manifest `-list-only` 和 `git diff --check` 通过。
- [ ] broad Store `-race` 仍不能标绿：两个既有 JSON compatibility admission 测试在 race 放大下分别触发生产 2 秒 indeterminate budget；普通构建各约 `0.55s` 通过，未发现 race detector 报告。该测试预算问题需单独处理，不能放宽生产 fail-closed 边界来掩盖。
- [ ] 在 immutable 配套 Docker fixture 上验证无 token、fake Graph/no POST、hard-kill/restart、account/global 429 间歇窗口、durable completion、FIFO/no-duplicate/no-loss、fairness 和资源上限；live source drift 或不完整 history proof 必须拒绝 acceptance。
- [ ] 只有真实副本的 durable completion/s 与本轮基线配对测量后，才报告吞吐收益；局部 queued-turn candidate benchmark 不外推为 Teams message/s。

## 2026-09-15 第六轮：native outbox 游标安全与 recovery sweep 配额

### 本轮发现与改动

- [x] 复现并修复 scalar `created_at` 与 canonical JSON 只差 1ns 时的真实分页漏项：SQLite 的 julianday trigger 容差会让 projection marker 保持 trusted；若直接把 scalar `(created_at,id)` 应用于 canonical `After`，位于游标两侧的消息可能被 SQL 提前排除，后续 recovery cursor 会永久跳过它。
- [x] native pending page 在每个 caller cursor/内部 page boundary 周围执行有界 `±2ms` scalar overlap probe；只要发现精确时间 projection contradiction，或 probe 超过 native admission budget，就返回 canonical fallback sentinel，不把错误的空页/错误 `More` 暴露给 sender。正常 exact projection 仍走索引，不执行全表 JSON scan。
- [x] canonical fallback 对 present JSON 字段使用 canonical hydrator，以 JSON `created_at` 为真实顺序，先完整收集并 canonical sort，再应用 `After`、`Limit`、`More`；因此 fallback 不再沿用可能陈旧的 scalar cursor/order。
- [x] ambiguous recovery 的 `outboxRecoveryMaxPagesPerFlush` 改为整个 sweep 共享，而不是每个 candidate 重置；预算耗尽只产生 durable deferral、保留 unknown/continuation，不会把未知 Graph POST 自动转成新 POST。

### 测试与实证

- [x] 新增 race-enabled `TestSQLiteNativeOutboxCursorDoesNotSkipSubMillisecondProjectionTear`；普通构建与 `-race` 均通过，确认该反例会进入 canonical fallback 并返回消息。
- [x] 新增 `TestBridgeAmbiguousRecoverySharesPageBudgetAcrossCandidates`；普通构建与 `-race` 均通过，两个 candidate 共只消耗一个 sweep budget，8 次假 Graph GET 后无 POST、两行仍保持 ambiguous Sending。
- [x] 两个第六轮新增回归已加入 `scripts/ci/teams_recovery_tests.json`；manifest `-list-only` 与 `scripts/ci` 包测试通过，CI 不会遗漏这两个 selector。
- [x] native pending/FIFO/projection 相关 Store race 集合通过：`259.531s`；完整 Store 回归通过：`518.774s`。
- [x] 完整 Teams 回归通过：`728.821s`；包含 sweep 配额、unknown POST、owner/lease/attempt、FIFO、429 和 listener 集成路径。
- [x] 最终工作树门禁通过：`go vet ./internal/teams/...`、全仓 `go test ./... -run '^$' -count=1` compile-only、`go test ./scripts/ci`、manifest `-list-only` 与 `git diff --check`。
- [x] 同一只读真实数据 Docker fixture、network-none、假 Graph、无 Teams token 的最终代码复测通过：60s 窗口 `58 / 60.459s = 0.959 durable Sent/s`，`Queued 133653 → 133595`，`Sending 38` 保持不变，`Skipped +0`，`58/58` fake POST 与 durable Sent 对齐，重复 POST `0`、429 `0`，fixture 源文件不变；该实测包含本轮共享 recovery sweep budget 修复。

### 仍未放行的范围

- [ ] 上述 Docker 是隔离真实 outbox 的诊断吞吐，不是完整 listener A/B、hard-kill、account/global 429 间歇窗口或长期公平性 acceptance；不能外推为 live Teams 的最终排空速度。
- [ ] broad Store `-race` 中既有 JSON compatibility 测试在 race 放大下仍可能触发生产 indeterminate budget；该测试预算问题不能通过放宽生产 fail-closed 边界解决。
- [ ] 仍保留 owner/CAS、unknown POST、frontier、FIFO、Graph 不在 SQLite 事务和 source-proof 等安全边界；任何后续提高并发/扩大 batch/削弱 canonical fallback 的改动必须新增 paired durable-completion 与 race 证据。

## 2026-09-15 第七轮：多 chat outbox quantum 2+2 实验

### 改动与安全测试

- [x] 保留单 chat 和 global fallback 的 `2` 条上限；多 chat fairness path 改为两个选中 chat 各发送最多 `2` 条，合计最多 `4` 条/轮。
- [x] JSON/SQLite 公平与 FIFO 回归通过：验证 `chat-a` 两条后才切到 `chat-b` 两条，第三条留到后续轮；第三 chat 仍能轮转；process-wide owner failure 仍不会启动第二 chat。
- [x] 新的 `TestTeamsMainLoopOutboxReservesTwoHeadsForAnotherChat` 已加入 recovery manifest；manifest selector 校验通过。
- [x] cap=4 代码下完整 `internal/teams` 回归通过：`GOPROXY=off CXP_RUNTIME_DISABLE=1 go test ./internal/teams -count=1 -timeout=20m`，`759.062s`。

### 同一真实 outbox fixture 的 Docker 实测

- [x] cap=2（临时严格基线）：`49 / 60.491s = 0.810 durable Sent/s`，35 cycles，重复 POST `0`，Skipped `0`。
- [x] cap=4（代码实际为两个 chat 各 2 条）：第一次 `60 / 60.028s = 1.000 durable Sent/s`，28 cycles；第二次 `58 / 60.197s = 0.964 durable Sent/s`，27 cycles；两次均为 `0` duplicate、`0` 429、`0` skipped，fixture 完整性通过。
- [x] cap=4 在有足够可发送 head 时确实达到 `2+2`；但真实 fixture 的 durable throughput 只落在 `0.964–1.000/s`，相对 cap=2 的既有 `0.810–0.981/s` 区间有重叠，不能宣称稳定翻倍；主要剩余成本仍是 SQLite durable lock/transaction 和 recovery/preflight phase。

### 未放行

- [ ] 尚未将 cap=4 作为发布默认值；需要更长窗口、同一代码基线的重复 paired runs，以及 account/global 429、hard-kill、fairness/resource quota acceptance 后再决定是否保留。
