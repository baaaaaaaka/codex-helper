# Teams backlog / delivery design audit checklist

本 checklist 记录对 Teams service 的积压处理、transcript 同步、outbox 发送、Graph polling、ownership 和性能机制的设计审计。目的不是马上重构，而是先区分：

- 哪些机制是为了明确的安全、正确性或公平性目标而加入；
- 哪些机制的目标正确，但实现可能造成重复扫描、低吞吐或永久无进展；
- 哪些机制已经被后续代码淘汰，只剩兼容 fixture；
- 在进行性能优化前，哪些不变量必须由测试保护。

## 审计基线与边界

- [x] 审计对象固定为 `origin/main=138c1ef`（`v0.1.22-rc.34`）；本地 checkout `d250666` 落后 5 个提交，涉及最新 polling frontier 的判断以 `origin/main` 源码和提交为准。
- [x] 已检查相关 git history、`git blame`、实现、测试、CI selector 和既有 safety checklist。
- [x] 本轮未启动、重启、修改或替换 live Teams helper/service；未修改 live SQLite/JSON 状态。
- [x] 当前工作树在本 checklist 之前是干净的；本轮只新增本审计文档。
- [x] 明确区分 Teams 入站 polling 与 Codex transcript 出站同步：两者都使用主循环，但不是同一条消息链路。

### 判定标签

- `INTENTIONAL-SAFETY`：为避免重复执行、丢消息、错误归属或不可逆外部副作用而设计，不能仅因慢就删除。
- `INTENTIONAL-LIVENESS`：为限制单个 chat/历史 backlog 对其他工作的影响而设计，数值可以调，但不变量不能删。
- `IMPLEMENTATION-RISK`：目标合理，但当前调度、持久化或错误隔离方式可能产生高 CPU、低吞吐、重复扫描或无进展。
- `OBSOLETE-COMPAT`：原始机制已被后续代码撤销，当前只保留兼容测试/fixture，不应重新作为运行时策略。
- `DEFERRED-PROTOCOL`：不是当前小优化的必要内容，需要单独的跨进程、Graph 或迁移协议设计。

## 1. 机制来源、目的与当前判断

| 机制 | 引入/演进证据 | 当时要解决的问题 | 当前判断 | 现有测试结论 |
| --- | --- | --- | --- | --- |
| Teams 单 owner、lease、heartbeat、stale takeover | `c8b2fbd`、`e7b5278`，后续 `88fe917`、`3147284` | 多个 helper/进程不能同时写同一控制状态；异常退出后要能接管；Graph/proxy 暂时不可用不应误杀 live child | `INTENTIONAL-SAFETY`；保留 | JSON/SQLite lease、CAS、race、Docker takeover 和 Graph outage 有较强局部覆盖；旧 binary 延迟 callback、真正双进程边界和 PID reuse 仍缺 |
| 入站 polling 状态分层（hot/running/warm/cool/cold/parked/catchup） | 基础状态来自 `c9a0070`；当前调度在 `09ef63e` 和 `036ee72` 演进 | 热 chat 及时读，冷 chat 少占资源，长期无活动 chat 可 park；新消息或恢复时重新唤醒 | `INTENTIONAL-LIVENESS`；保留不变量，调度值待测量 | 状态阈值、park/wake、有限轮次轮换有测试；数百 chat 下的最大等待时间和长期公平性没有硬断言 |
| Graph 每轮最多 8 个 work chat、最多 4 个并发、单请求 10s timeout | `09ef63e`；最新 frontier 在 `036ee72` | 单个 Graph 黑洞不能占住所有 chat；限制请求压力和 store contention | `INTENTIONAL-LIVENESS`；保留，可能需要 due-oriented 查询和更清晰的全局预算 | Graph stall、control/work 隔离和 429 有覆盖；真实 `Listen` 长期运行、数百 chat、永不返回 Graph 的完整闭环缺失 |
| durable polling frontier、continuation page receipt、gap/recovery lane | `036ee72` | full message window、旧 continuation、重复 nextLink、坏页或长 outage 不能让 cursor 永久回退/活锁，也不能让一个 frontier 遮住所有可读消息 | `INTENTIONAL-SAFETY` + `INTENTIONAL-LIVENESS`；保留 | reducer、receipt、CAS、continuation、gap、429 和多 chat stress 很强；跨多日服务、旧 owner 延迟写入和极大规模功能性验证仍缺 |
| linked transcript 10s gate、每 session 每轮最多 8 条 | 初始 `c9a0070`（30s、8 条），`c82b2ce` 改为 10s | 自动同步只做小批量 catch-up，避免启动/主循环被历史记录淹没 inbound polling | `INTENTIONAL-LIVENESS`；保留语义，重新测量数值 | 直接调用同步函数能验证 8 条后继续；没有真实 `Listen` 多 session backlog 的最终排空 SLA、跨 session 公平性和重启连续性 |
| `transcriptSyncMaxAutoBacklogRecords=80` | `c9a0070` 初始 hard gate；`e28a070` 后仅保留 fixture 兼容 | 原始意图是把大历史导入留给显式命令，避免自动 flood | `OBSOLETE-COMPAT`；不可作为优化依据；未来可单独清理 | 应增加“80 不再改变运行时分支”的保护，避免旧逻辑回归 |
| history watcher：10s 增量、5m reconcile、最近 3 天 | `c82b2ce`；`7292ddd` 修复旧文件误当新工作；`0e71c5b` 做 idle gate | 自动发现 TUI 新 chat/final，又避免全量历史反复导入和阻塞 polling | `INTENTIONAL-LIVENESS`；保留，但需要逐文件错误隔离、删除清理和 durable backoff | baseline、recent path、partial、reconcile、dedupe 有覆盖；永久缺失目标、现代 checkpoint 删除、首个坏文件不阻塞后续文件、重复扫描成本和服务重启后的节奏缺失 |
| linked transcript 的跨 session 错误隔离 | 主要沿用 `c82b2ce`/`1477c0c`；`036ee72` 未改变顺序循环 | 单个 source/checkpoint 错误应保守停住该 session，但不应饿死其他 chat | `IMPLEMENTATION-RISK`；局部错误应记录并继续，不能把 source proof 放宽成全局跳过 | 有单 session backlog、live command 隔离测试；缺少“第一个 session 失败、后一个 session 仍推进”的硬断言 |
| 512 KiB history tail budget | `c82b2ce` 首次引入；`150c34f`、`e28a070`、`b658f69` 改为可恢复物理 cursor | 自动维护不能无界读取巨大 transcript；大 tail 不应把主循环拖死 | `INTENTIONAL-SAFETY` + `INTENTIONAL-LIVENESS`；保留“单轮预算”语义，不能当 chat block | tail cap、partial、跨 poll 恢复、safe prefix 有测试；没有总时间/总 bytes/lock/disk 配额和大 backlog 服务级 SLA |
| 8 MiB logical record cap、64 MiB newline-less framing cap | `b3ac75a` 首次加入 record 边界；`150c34f` 加 physical framing | 图片/Base64/tool output 或无换行坏记录不能让 parser 永久重读，也不能错误归属后续 final | `INTENTIONAL-SAFETY`；保留 bounded quarantine/skip；阈值要以真实 payload 统计校准 | oversized、partial、malformed、opaque、后续 final 可达有较强 scanner/bridge 测试；真实主循环、重启、磁盘压力、精确边界和大对象内存上限仍缺 |
| `ExecutionAnchor`、source proof、CAS、unresolved fence | `1477c0c` | 防止 stale answer、重复 terminal、错误 owner callback 或 source rewrite 修改 Teams 状态 | `INTENTIONAL-SAFETY`；保留 | provenance、generation、CAS、mixed-ID、pending-root、restart 和 race 有较强覆盖；跨进程旧 writer、Graph accepted 后本地结果未知、所有 crash boundary 未闭环 |
| history-only quarantine 与 live-turn 解耦 | `e0373d8`、`b3ac75a`、`150c34f`、`e28a070`、`b658f69` | rc8 历史不确定状态不应把正常 Teams turn 变成 queue-only，也不应重复刷恢复提示；物理读取进度和语义安全边界分离 | `INTENTIONAL-SAFETY` + `INTENTIONAL-LIVENESS`；这是当前最重要的正确方向 | s512/s514/s519/s001、旧状态、SQLite reopen、safe prefix 和 admission 测试覆盖较强；完整多 chat、长 outage、migration crash 和 metadata retention 仍缺 |
| history import 每轮 1 batch、主循环 outbox 2 条、workflow notification 1 条 | `7292ddd`；`e28a070` 增加 targeted flush 8 条/512 KiB | 历史 webhook/outbox 不能饿死 inbound poll、control ack 和新 live 请求 | `INTENTIONAL-LIVENESS`；保留 lane 优先级，可能改变执行器结构 | main-loop 小预算、background batch、其他 chat 继续处理有覆盖；targeted/global 并发、1000 backlog、失败状态下的扫描预算和固定 live SLA 缺失；当前 main budget 只按成功发送数计数 |
| 每 chat Graph outbox send 至少间隔 1.2s | `329ca02` 首次进入主线；提交正文主要是 model-profile，没有记录 pacing 的原始依据 | 降低同一 chat 的 Graph 写入 burst/throttle 和客户端渲染压力 | `INTENTIONAL-LIVENESS`，但动机文档不足；限流目标保留，当前并发实现需修正 | 只有 fake sleep 的正向等待检查；未证明真实相邻 POST 间隔、global/targeted 并发、跨进程/重启和多 chat 吞吐；global 与 targeted 可能同时观察旧时间戳 |
| queued-turn drain 的启动预算与 active execution 上限 | `c9a0070` 及后续 listener/backlog 提交；当前 `036ee72` 未形成统一 admission semaphore | 恢复 backlog 时限制新执行启动，避免慢 executor 或 follow-up 让 active goroutine 无界增长 | `INTENTIONAL-LIVENESS` + `IMPLEMENTATION-RISK`；启动速率和 active 数量必须分开约束 | 有单轮 start budget、queued turn 和 follow-up 的局部测试；缺少多 session backlog 下的 active cap、年龄公平和 follow-up 不绕过预算测试 |
| durable outbox、transcript delivery ledger、accepted/ambiguous fence | 通用 outbox 来自早期 Teams mode；transcript replay fencing 在 `da2af9f` 等后续提交强化 | checkpoint 回退、outbox prune、Graph 已接受但本地写失败时不能重复执行/盲目 POST | `INTENTIONAL-SAFETY`；保留；外部语义是条件性 at-least-once，不是假装 exactly-once；未知结果使用现有 `accepted`，或 `sending` 加 ambiguous reason、block flag/skipped disposition 表达 | CAS、ledger、accepted recovery、429/502、Graph ID recovery 有覆盖；provider idempotency、跨系统事务、ledger 写失败和所有 side-effect crash 窗口缺失 |
| SQLite narrow snapshot、registry save gate、减少 discovery/idle I/O | `0e71c5b`、`f301b81` | 降低 idle CPU、state projection 重写和重复历史发现 | 目标是 `INTENTIONAL-LIVENESS`，具体实现存在 `IMPLEMENTATION-RISK` | 有 benchmark 和部分回归；多数只打印 CPU/I/O/alloc，未设回归阈值；HistoryWatch projection、poll due 查询、global ledger 和 legacy JSON 仍可能 O(N) |
| supervisor restart/backoff 和 Graph outage degraded mode | `88fe917`、`3147284` | 进程/初始化确实坏时恢复；Graph 暂时挂时保持 live child，不陷入无限重启 | `INTENTIONAL-SAFETY`；策略保留，必须由状态分类和 restart budget 保护 | 有启动 race、Graph outage、cleanup、heartbeat 测试；“服务连续运行数天、反复故障、heartbeat 不推进、恢复仍有 backlog”完整组合缺失 |

## 2. 有用、应调优、应淘汰的部分

### 必须保留的保护目标

- [x] 单 writer/owner fence：TUI 占用 Codex thread 时，Teams 不应静默重复执行同一请求。
- [x] source identity、read-range proof、generation 和 parent/owner CAS：没有证据时不能把新 source 当成旧 source 的 suffix。
- [x] physical cursor 与 semantic delivery frontier 分离：已消费但不可发布的记录必须有明确 disposition，不能靠回退 cursor 活锁。
- [x] history-only quarantine 不参与 live admission；只有真实 durable live ownership 才能阻止新的 live turn。
- [x] durable outbox、stable delivery key、accepted/ambiguous fence 和 accepted recovery：外部副作用未知时不自动盲重发；不未经协议设计新增状态。
- [x] 单 chat 错误隔离、Graph continuation/page receipt、坏记录 quarantine、有限请求/记录预算。

### 目标正确但实现值得优化

- [x] 10s history/transcript cadence、每轮 8 条、每轮 1 batch、主循环 2 条、targeted 8 条/512 KiB、1.2s pacing：它们都是资源/公平性策略，不应直接删除；应先有吞吐、延迟和不丢失测试再调参。
- [x] main loop 中历史、outbox、poll、queued turn 目前仍有串行阶段；这是最可能造成 backlog 慢和 CPU 高的结构性成本。应以独立、可观测、带预算的 phase/worker 演进，而不是放宽 source proof。
- [x] 这里的“串行”只指 `Listen` 的 phase 编排；work-chat polling 本身已有最多 4 个 worker。优化目标是避免慢 phase 延迟其他 phase，而不是盲目增加 poll 并发。
- [x] blocked/deferred outbox、legacy history、missing target 等无进展路径应持久化下一次尝试时间和原因，避免每轮从头扫描；任何退避都必须允许新消息/文件变化唤醒。
- [x] poll scheduler 应优先查询 due work，而不是每轮解码大量尚未到 `NextPollAt` 的 active rows；优化前要以“不改变 park/wake/fairness”测试锁定行为。
- [x] HistoryWatch projection、legacy JSON、global ledger 的全量 clone/marshal/open/ensure-schema 可能是 O(N) 或重复事务；应先用计数器和 benchmark 定位，再做批量化/窄查询。SQLite 已避免重写 canonical `state_json`，不能把这个问题笼统表述为“每次重写整个 state”。
- [x] `syncLinkedTranscriptsWithDiscovery` 不能因为 registry 顺序中第一个 session 出错就提前结束整轮；应将错误隔离到 session，并保留该 session 的 retry/disposition。
- [x] main outbox 的预算必须限制尝试、扫描或时间，而不只是成功发送数；否则连续 5xx/transport error 可能翻完整个失败 backlog 后才回到 polling。
- [x] 同 chat pacing 必须对 global/main 与 targeted flush 共享 reservation/锁语义；否则两个发送者可以同时越过 1.2s 间隔。修复时不能把不同 chat 不必要地串行化。

### 当前不应继续保留为运行时策略

- [x] `transcriptSyncMaxAutoBacklogRecords=80` 的旧 hard gate 已不再是当前运行时逻辑；不能恢复“超过 80 就跳过到末尾”的做法。
- [x] “所有 history source proof 不通过就阻塞 live chat”不是有用保护，而是 rc8 时代导致 queue-only、垃圾提示和 liveness 丢失的错误耦合。
- [x] “只要删除 tail_too_large 或放大阈值即可解决”不是安全优化：必须保留 bounded framing、可推进 cursor、quarantine 和后续记录可达性。

## 3. 当前测试已经保护的内容

- [x] 本地状态层：JSON/SQLite checkpoint、CAS、lease、owner generation、inbound/outbox/delivery ledger、满盘/WAL/重启的多个边界。
- [x] transcript scanner：partial、EOF 无换行、malformed 中间行、source rewrite、512 KiB tail、8 MiB record、64 MiB newline-less framing、opaque/quarantine 和后续 final 可达。
- [x] history/live 语义：pending root 多轮释放、history-only quarantine、mixed-ID mirror、旧 checkpoint、blocked history 不阻止新 live turn。
- [x] Graph 局部故障：429/502/timeout、旧 continuation、重复 nextLink、control/work 隔离、page receipt 和多 chat 的有限轮次压力。
- [x] outbox 局部语义：FIFO、per-chat flush、accepted recovery、source proof、失败后继续其他 chat、main-loop 小预算。
- [x] 一些 idle/perf benchmark：SQLite snapshot、HistoryWatch、long transcript、WAL、queued turn、outbox 和 parked-chat fixture。

### 已确认的实现风险（不是要删除安全机制的理由）

- [ ] **跨 session early return**：当前 linked transcript 同步循环对部分 per-session 错误直接 `return`，因此 registry 排在后面的正常 session 可能本轮完全没有机会；补一个默认硬失败的两 session 回归测试。
- [ ] **失败 outbox 不消耗 main-loop budget**：main flush 的 `sent` 只统计成功 POST，连续失败 row 会继续翻页；补“chat A 三个以上失败 row、chat B 健康 row”的尝试/扫描上限测试。
- [ ] **pacing 的 global/targeted 竞态**：当前时间戳检查与 sleep/reservation 分离，两个路径可同时 POST；补真实 POST start 时间测试，先让它稳定暴露，再把间隔契约作为门禁。
- [ ] **现代 transcript 删除清理**：普通现代 source `ENOENT` 与已有 HistoryWatch projection 的 reconcile 路径存在不相容，可能留下孤儿 checkpoint 并持续 stat；补删除 source 后普通 sync/reconcile 的 CAS 清理测试。
- [ ] **HistoryWatch 总工作量无预算**：单路径 512 KiB 不等于整轮有界；补 changed-path 数、读取 bytes、final 数和后续 cycle 续跑的计数契约。
- [ ] **due poll 与 queued execution 的规模成本**：普通 active rows 可能在 due 前被大量解码；queued follow-up 也可能绕过启动预算。先用 counters/固定 fixture 证明，再决定是否改查询或 admission semaphore，不以 hosted CI wall time 猜测。
- [ ] **phase 错误范围没有统一模型（P0）**：`pollScopedError` 已能表达一部分 work-chat 错误，但 handler、outbox、linked transcript、HistoryWatch、Save/WAL 的 store/lease/进程级错误没有统一的 scope/disposition 契约；SQLite `FULL`/`ENOSPC` 不能被当成某个 chat 的普通失败或让 watchdog 误判为成功。需要定义足够小的 `PhaseOutcome`（至少区分 chat/session/process、retry/defer/stop-cycle/degraded/restart 和外部副作用是否未知），并验证 process-wide 不可写时不产生半个 commit，也不承诺其他 chat 继续写入。
- [ ] **旧 capability 在最终提交前是否仍有效（P0 安全候选）**：poll attempt 已保存 owner、process incarnation、lease generation，但当前最终匹配与 bridge 的前后检查之间存在需要验证的 TOCTOU 窗口；outbox 也有 claim 后、POST 前的类似窗口。用旧 owner 延迟 callback、takeover、JSON/SQLite 双后端测试先确认是否可复现；复现才改成带 capability 的事务性最终 CAS，未复现也保留该不变量测试，不能用无条件重试掩盖。
- [ ] **source proof 与 claim 之间的竞态（P0）**：claim 后再次 proof 失败的路径不能只标记 `skipped`，否则 checkpoint 看起来已安全推进但 canonical source-rewrite fence 尚未建立；需要用 rewrite barrier 复现并要求先持久化 fence/hold，再决定是否推进物理 cursor。该项与 owner capability 测试分开，避免把两个安全边界混为一个锁。
- [ ] **source identity 快路径（P0）**：modern source 不能只用 size/mtime 判断未变化；同路径、同大小、同 mtime 的 inode/identity 替换必须触发 bounded proof 或 rewrite/hold。补 delete、recreate、atomic rename 和相同 metadata replacement 的真实 scan/reconcile 测试。
- [ ] **`deliver_after` 语义未决（P1）**：SQLite 有兼容列但当前对象/写入/pending predicate 未形成完整协议。先在实现阶段做二选一的决策记录：明确废弃并保持兼容读取，或端到端定义 JSON 字段、SQLite 查询、迁移、唤醒和重启语义；在决策前不能把它当成已存在的退避契约。
- [ ] **terminal-fence predicate parity（P1）**：JSON pending predicate、SQLite `LIKE` predicate 和 terminal-failure 错误前缀并非同一抽象；可能导致 fenced row 继续被选中、空操作消耗预算或诊断与实际选择不一致。应以 durable disposition/共享 predicate 统一语义，并对 JSON/SQLite 做归一化结果对照。
- [ ] **frontier/revision 边界（P1）**：同一 modified timestamp、分页 overlap、重启、schedule update 与旧 poll 完成交错时，现有 timestamp cursor、expected revision 和 CAS 的组合需要明确不变量；补稳定 tie-breaker 或 continuation/overlap 的证明测试。

## 4. 尚未被充分保护的真实不变量

以下不是“再加几个单函数测试”就能替代的边界；应优先增加服务级、可重复、可失败的测试。每项都必须明确适用条件，并对可证明的记录断言不丢、不重、最终推进和资源上限；当前 `OutboxStatus` 只有 `queued/sending/accepted/sent/skipped`，`blocked/ambiguous/uncertain` 只能作为规范化 disposition、block flag 或稳定原因，不能当作字面 enum。遇到 ownership、source-rewrite 或未知外部副作用时，必须使用现有状态加明确原因表达 hold/quarantine，不得未经独立协议设计新增状态或强行清除。对 process-wide 存储故障则断言停止写入和可恢复的 degraded 状态，不把“其他 chat 也成功写入”当成不变量。

### P0：必须补齐的组合场景

- [ ] **确定性 cycle/phase drain harness（P0）**：当前代码没有统一 fake-clock/timer seam，因此先用 fake Graph、显式 `now`、可注入 sleeper 的窄 `runOneCycle`/phase harness，覆盖 3–16 个 chat、有限 cycle，以及 SQLite 和 JSON backend；生产 `Listen` 必须复用同一 phase executor/outcome 逻辑，而不是只让测试专用 harness 通过。只有补齐必要的时钟注入后，才把它扩展成一个短的完整 `Listen` smoke，不替换整个仓库的时间系统。输入同时包含 inbound backlog、transcript backlog、outbox、queued turn、workflow notification。断言每个 phase 都获得机会，健康 chat 不被 chat-local 坏状态饿死，cursor/outbox/ledger 单调，并最终到达现有状态可表达的 `sent`、`accepted+messageID`、`sending/accepted/skipped + 明确原因`、`quarantine/gap` 或 `manual hold` 等结果。这里的“eligible”必须进一步按 outbox row/turn/source proof 定义，不能只按 chat；不能要求测试自动越过 ownership/source-rewrite/ambiguous fence。
- [ ] **统一但窄的 phase outcome 边界（P0）**：先保留现有 `pollScopedError` 和内部错误类型，只在 phase 边界增加最小适配，表达 `scope=chat/session/process`、`action=continue/defer/stop-cycle/degraded/restart`、外部副作用 `not-started/accepted/unknown`、`deadline/budget_exhausted` 原因和是否有 durable progress；保持 `errors.Is/As`，不把所有内部函数改成大型新返回类型。每个 phase 必须有明确 wall-time/attempt/scan/bytes deadline，超限停止该 phase 或本周期并返回可消费 outcome；有副作用未知时不得当普通失败盲重发。生产 `Listen`、watchdog 和 retry loop 必须消费该适配层，不能只在 harness 中分类。
- [ ] **process-wide store failure（P0）**：在 outbox、workflow、poll、transcript、HistoryWatch、Save/WAL 各 phase 注入 `busy/full/ENOSPC/readonly/corrupt` 的代表性错误。chat-local 错误只影响对应对象；process-wide 错误停止本周期的后续 durable mutation、不得调用 success/heartbeat 假设、不得产生半个 ledger/outbox/cursor commit，恢复后下一 cycle 可继续。不能把数据库不可写时“其他 chat 仍成功写入”写成硬要求。
- [ ] **owner capability 最终 CAS（P0）**：这是无条件的安全 gate，不以“实际复现窗口”为前提。模拟 poll/outbox 已 claim 后发生 lease takeover，旧 owner 在最终状态提交或 Graph POST 前恢复；分别验证 JSON/SQLite 中旧 owner 的结果不能覆盖新 owner。失去 capability 后只能进入现有可表达的 hold/accepted/ambiguous 状态，不能盲目重发；若现有顺序检查不足，必须把 `(machineID, process incarnation, lease generation, attempt token)` 中适用的能力纳入 store-side final CAS/POST 前检查，但不引入无必要的全局锁。
- [ ] **claim 后 source-proof fence（P0）**：在 preclaim proof 成功、claim 完成后替换 source 的 barrier 场景，必须先持久化 source-rewrite fence/hold，再标记 skip 或推进物理 cursor；不能让单独的 `skipped` 造成“安全已确认”的假象。测试物理 cursor、semantic disposition、ledger 和后续 source 的可达性。
- [ ] **source identity 与删除（P0）**：strict/reconcile 路径已有部分 `SameFile`/fingerprint proof，但普通 linked idle fast path 是独立风险，必须单独验证。对 modern source 覆盖同路径同大小同 mtime 的 inode replacement、atomic rename、短暂 ENOENT、删除后重建；普通路径不能只因 size/mtime 相等就永久 no-op，必须比较 identity 或进入 bounded proof/hold。删除清理必须用预期 source identity/checkpoint 做 CAS，并允许恢复/重试，不因短暂 replacement 清掉新 source；若现有实现已满足，保留回归测试而不做无谓重写。
- [ ] **Graph blackhole 的服务闭环**：先在 phase harness 中让某 chat 或 control Graph 请求永不返回，区分 inbound poll read deadline、普通 Graph HTTP client timeout 和 outbox flush 位于 poll 之前的调度顺序；验证其他 phase/其他 chat 仍可观察。补齐 timer seam 后再覆盖完整 `Listen`、supervisor degraded 状态和 Graph 恢复后的 frontier 继续，不能无限重启。
- [ ] **坏 chat 局部隔离与首错继续（P0）**：将 malformed/oversized/partial/rewrite/legacy checkpoint、永久缺失 history target、429、send error 分别注入 chat A；chat B/C 同时有新消息和 transcript final。断言 A 的扫描/发送/恢复不会停止 B/C，也不会因第一个错误提前 return 丢掉后续 changed path；特别固定 registry/path 顺序，验证第一个 linked session/path 出错时后一个仍能推进。SQLite busy/full 另做 process-wide degraded-state 测试：不推进 cursor、不产生半个 ledger/outbox commit，不要求数据库不可写时其他 chat 继续写入；生产修复必须保留这个 scope 分界。
- [ ] **外部副作用最小 crash/state matrix（P0）**：覆盖现有 `queued/sending/accepted/sent/skipped` 状态及 block/ambiguous reason/flag，和最关键的重启边界：intent/claim、Graph POST 前后、响应丢失、message ID、delivery ledger、checkpoint CAS、owner heartbeat、WAL commit，以及 attachment upload 成功但本地结果丢失。重启后只能继续有证据的 intent；accepted 或其他未知外部结果不得盲 POST，cursor 不越过未持久化 disposition；ledger/provenance 写失败必须留下可恢复的 hold。该矩阵必须接入真实 sender/store 边界，不能只在 harness 里模拟；完整 provider 协议、真实双进程和更大注入留给 P2/nightly，但最小未知副作用语义不能推迟到 P2。
- [ ] **旧版本迁移后连续运行**：rc16/legacy JSON/SQLite/WAL fixture 迁移后先运行首个 cycle smoke；重复启动、migration 中断、旧 state 字段损坏都要保持 JSON/SQLite parity，不把 legacy history 当 live owner，不重复发布。完整 mixed-phase drain 复用 P0 harness，不重复维护一套迁移版主循环。必须把 runtime layout migration 与 JSON→SQLite backend migration 分成两个契约，并分别覆盖已有 `after-backup`、`after-temp-verified`、`after-db-replace` 等中断点；若新增字段/索引，旧二进制的读取、拒写或安全回退行为必须写入矩阵。
- [ ] **旧 supervisor activation handoff**：升级或恢复过程中验证旧 supervisor marker、activation success、handoff timeout 和旧 child 仍存活的情况；不得启动第二个 child、制造双 writer 或把 Graph outage 当成 restart 条件。该项复用现有 supervisor smoke，不要求第一阶段增加新的后台架构。
- [ ] **多 frontier recovery**：旧 continuation、deferred continuation 和新 head 同时存在；重启、retry deadline、new message、Graph 恢复交错发生。断言状态最终进入可解释的 recovery/gap/parked 结果，不是每轮从同一 head 重放，也不是无证据清除 frontier。

### P1：性能和调度缺口

- [ ] **100 chat / 大 backlog 功能性压力**：100 chat、1K backlog 和 1K HistoryWatch path 在补齐时钟/计数 seam 后进入 Linux Docker；每 chat 混合 hot/warm/cold/parked、短消息、长消息、continuation 和 transcript，使用生产的最多 8 个候选/4 个 worker，验证每个 eligible chat 的最大等待轮数、无重复/无丢失。测试 manifest 必须预先写入每个 lane 的数值 `max_wait_cycles/max_due_age`，不能用“最终会处理”这种无界断言。500/1000 chat、10K/100K 数据和长 soak 放 nightly/manual，不作为普通 presubmit 的前置条件。
- [ ] **service outage/freeze 短状态转换**：先用少量虚拟时间覆盖 service 停止、TUI 继续写入、Graph 恢复、新 Teams 消息、park/freeze 到期和 helper restart；断言恢复从 durable frontier 继续，不从头猜测、不永久 blocked、不重复刷 notice。完整 72h 语义和 supervisor/real-time smoke 放 nightly/manual。
- [ ] **blocked outbox no-progress test**：先用 1K 条全部 unresolved/rate-deferred 的 row，连续 100 virtual tick；记录 page/row/DB/JSON/Graph 次数。10K 规模放 nightly。验证没有 Graph POST、每轮有明确的尝试/扫描/时间上限，并且有持久 backoff 或可解释 disposition；不把一次性 quarantine 作为强制答案，因为某些 anchor 之后仍可能恢复。
- [ ] **outbox 失败尝试预算**：Stage 1 的小 fixture 先作为 P0 gate：chat A 放入 3 个以上 503/transport-failing row，chat B 放健康 row；断言 A 的失败尝试会消耗 main-loop 的 attempts/time budget，B 在固定边界内有机会发送，同时保留 A 的安全状态，不因“继续错误”而翻完整个 backlog。Stage 2 再用 1K row 验证相同上限不会随 backlog 线性失控。
- [ ] **deferred/`deliver_after` 语义**：先完成“废弃兼容列”或“完整端到端协议”的决策，再构造未来可重试时间的 row；确认它不会在每轮被反复扫描。若该字段是退避契约，就必须同时验证 SQLite/JSON 查询、唤醒条件、重启后的持久性和新消息唤醒；若废弃，则测试旧列不改变 pending 选择，不要只增加字段而不接入选择逻辑。
- [ ] **HistoryWatch 永久缺失/坏文件隔离**：一个 path 永久不存在、一个 path 读失败、一个正常 append；连续多个 watch tick 只允许按退避探测坏 path，正常 path 必须继续推进；reconcile 错误不能使其他 path 永久饿死。首错隔离和普通现代 source 删除的 identity-aware CAS cleanup 是 P0 行为 gate，本项的 1K path/重复探测成本属于 P1；避免孤儿路径永久 stat。
- [ ] **HistoryWatch 持久退避与公平续跑**：blocked/deferred/missing path 必须持久化 `NextProbeAt`、reason、attempt/disposition 和公平 continuation cursor；重启后不能因为内存 gate 丢失而立即从头高频探测，也不能让一个坏 path 永久占满本轮。新 append、恢复时间和显式 repair 必须能唤醒它。持久化必须采用窄行/批量或等价方式，不能把每个 path 的退避更新放大成 path 数 × 全 projection 的写入。
- [ ] **due-oriented poll scaling**：SQLite 用 `next_poll_at`/状态索引读取有限 due candidates，再加载有限 session；JSON fallback 明确一次 bounded snapshot/scan 和 continuation 预算，不能假装具有 SQLite 的查询复杂度。必须保留 gap、continuation、park probe、未初始化 chat 等特殊 lane；同时测试 hot/catchup/park fairness 不退化。
- [ ] **global 与 targeted outbox pacing 并发**：同一 chat 的 global flush 和 targeted flush 并发，另有多个独立 chat；使用 fake clock 断言同 chat 的 POST start 间隔不小于 1.2s，独立 chat 不被不必要串行阻塞，取消不会留下错误的 pacing reservation。
- [ ] **terminal fence 与 backend parity**：构造 `status + reason/flag` 组合（accepted、ambiguous、terminal-failed），验证两种 backend 的 pending 查询、flush budget、诊断状态和重启恢复完全按同一规范化 disposition 工作；已 fenced row 不反复扫描，未知副作用不因为 predicate 差异被盲发。
- [ ] **page receipt/replay 成本**：普通、大消息、首条 actionable、全部 deferred、replay page 各测 SQLite/JSON 的 hash/unmarshal/transaction/alloc/read-write 计数；确保 page receipt 的安全成本不会随已处理页无界重复增长。
- [ ] **HistoryWatch projection 与 ledger 规模**：1K rows 测 idle tick、单 checkpoint update、global inbound/outbound lookup、跨 scope backfill；10K/100K 只进 nightly。记录 O(N) 增长，不把单次 benchmark 输出误当性能门禁。
- [ ] **JSON/outbox 与 HistoryWatch 放大路径**：固定相同 backlog，分别测 JSON 每页全量 load/filter/sort、HistoryWatch blocked/deferred 的 stat 预扫描、SQLite `HistoryWatchOriginState` 的 sessions/turns decode 和 cold state load；记录 rows/bytes/decodes/stats/transactions，验证优化前后没有把安全判断改成盲跳过。
- [ ] **长期公平性**：所有 chat 持续 due/hot，运行至少 100 个虚拟 cycle；每个 chat 都必须在规定最大 cycle/age 内被选中，稳定的 ChatID 排序不能造成永久偏向。另测 outbox 全局 FIFO 下一个 chat 的旧 backlog 不会长期占满 main lane。
- [ ] **queued execution admission/follow-up**：多个 session 同时积压 queued turns，executor 故意变慢并产生 follow-up；断言 active execution 数有独立上限，follow-up 不能绕过启动预算，且旧 queued item 按年龄/轮转公平推进。max active、每周期 start、最大 queued age 必须是 manifest 中的数值契约，而不是只记录观测值。
- [ ] **poll frontier 与 schedule revision 交错**：同一 modified timestamp、多页 overlap、重启和计划更新与旧 poll 完成交错；断言稳定 tie-breaker 不漏不重，expected revision/CAS 拒绝旧结果，拒绝后新计划仍会按 due 运行。
- [ ] **资源稳定性 soak**：用虚拟时间和有限真实运行窗口测内存、goroutine、SQLite/WAL、outbox/quarantine metadata、文件句柄和 proc I/O 的增长；连续 Graph 429、partial append 和 restart 不得无界增长。

### P2：协议/平台边界

- [ ] 真实双进程 old/new binary takeover、旧 writer 延迟 callback、PID reuse 和 live WAL/SHM restart。
- [ ] provider 支持时的 Graph idempotency key；不支持时继续明确 at-least-once，并使用现有 `accepted` 或 `sending/skipped` 加 block/ambiguous reason/flag 表达 `uncertain/needs_attention`，不隐式新增状态；其中 `blocked` 只表示规范化 disposition/flag，不是 `OutboxStatus` 字面值。
- [ ] 迁移/repair 的完整 lane/schema、metadata compaction/retention 和无法分类旧状态的人工边界。
- [ ] Windows/macOS 的 process/lock/timeout 语义矩阵；不能用 Linux process-group 测试冒充其他平台证据。

### 实施前的最小契约与顺序

- [ ] 每个真正要落地的 P0/P1 项先补齐以下字段：`owner package/file`、复用的现有 API/seam、fixture/backend、fault injection、expected scope、expected disposition、durable state assertion、exact test name、CI job/script、allowed skip、wall-time/resource budget。没有这些字段的条目只能作为调查项，不能直接改生产代码。
- [ ] **Stage 0：可观测性和窄测试 seam**：先增加 phase/attempt/scan/progress counters、budget exhausted reason、fake Graph、窄 `runOneCycle(ctx, now)` 和可注入 sleeper/clock；生产 `Listen` 调用同一 phase executor，禁止只让测试 harness 通过。不要替换整个仓库的时间系统，也不要引入全局 Store/Graph interface。
- [ ] **Stage 1：P0 正确性与 liveness gate**：先完成 process-wide error scope、linked/HistoryWatch 首错隔离、source identity/delete、claim 后 source-proof fence、owner capability final CAS、最小副作用 crash matrix、旧版本首 cycle/activation handoff；同时将 failed outbox 的 attempts/scan/time budget 纳入本阶段，因为它会阻塞后续 poll。
- [ ] **Stage 2：P1 规模和效率**：基于 Stage 0 的 counters 再做 outbox durable backoff、HistoryWatch 持久退避/公平 cursor、SQLite due query/索引与 JSON bounded scan、queued 统一 admission/active cap、shared per-chat pacing、terminal predicate parity、frontier/revision 和 cycle 内正向 snapshot 复用。每项先有行为基线再优化。
- [ ] **Stage 3：nightly/P2**：在前两阶段通过后再做 500/1000-chat、10K/100K 数据、长 soak、virtual 72h、完整 crash/双进程/provider/platform 矩阵；100-chat Docker/full-Listen 属于 Stage 2 的规模证据。若没有证据表明 phase barrier 仍是瓶颈，不拆独立 sender worker。
- [ ] **状态与后端兼容契约**：未知外部结果、source rewrite、ownership conflict、普通失败和显式跳过必须映射到当前已有的 `queued/sending/accepted/sent/skipped` 加稳定 reason/flag/disposition；`blocked/ambiguous/uncertain` 只能是规范化语义，不是当前 `OutboxStatus` 字面值。只有完成独立 schema/protocol 设计并验证迁移后，才允许新增状态。JSON/SQLite 比较规范化的 pending、disposition、cursor、receipt、重启结果，不要求底层 SQL/文件读写复杂度相同，也不把 `handled=false`、eligible 为空、CI selector 零匹配和业务 hold 混为一谈。
- [ ] **持久化变更规则**：若引入 HistoryWatch `NextProbeAt`、attempt/disposition、fair cursor 或 outbox retry 字段，必须说明 schema/version、旧字段默认值、旧二进制读取行为、幂等升级、备份/回滚和中断恢复；不允许 active listener 正在使用 store 时在线替换 schema。`deliver_after` 在做完“废弃兼容列”或“完整端到端协议”的决策前，不得作为退避功能的实现依据。
- [ ] **CI 交付契约**：P0/P1 的每个测试必须登记到精确 manifest（package、OS/build tag、test name、job/script、timeout）；先 `go test -list`/`-test.list` 做 exact diff，再用 anchored selector 执行，并解析 `go test -json` 确认 required test 不是零匹配或未允许的 skip。新增公共 API、store schema、锁或 timer seam 后，至少触发现有 Linux/macOS/Windows compile/full-test matrix；P2 只推迟大规模运行时证据，不推迟编译兼容性。

## 5. 性能回归门禁设计

- [ ] 为每个 workload 先保存同一 runner/容器的基线：idle 100/1000 chat、1000 backlog、long transcript、oversized record、Graph blackhole、429、outbox drain、migration 和 legacy JSON/SQLite；区分 presubmit 的小 fixture 与 nightly 的大 fixture。
- [ ] 测试输出至少包括：每 phase tick 数、phase wall/CPU time、scan row/page/bytes/attempts、JSON marshal/unmarshal 次数、SQLite statement/transaction/row 数、lock wait、Graph request/post 数、checkpoint/frontier 前进量、outbox disposition、oldest eligible age、skipped/blocked count、active execution、allocs、RSS、proc logical/physical I/O。预算必须同时限制成功数、失败尝试数、扫描页/行数和时间，不能只统计成功 POST。
- [ ] 正确性测试用确定性 fake clock 和 fake Graph；不要用“真实等待三天”或不稳定的绝对 wall-clock 作为唯一门槛。
- [ ] 先用基线差分发现热点，再为稳定指标设置分层阈值；presubmit 只放短、确定性的计数/不变量门禁，nightly Docker 才跑大规模/race/soak，避免把环境噪声变成 flaky CI。
- [ ] 性能回退必须让 CI 失败，而不是只 `ReportMetric`/`Logf`；同时保留原始指标用于诊断。
- [ ] 不仅检查 selector 组非空：为每个 CI 组维护精确测试/benchmark manifest，逐项执行 `go test -list '^TestName$'`（缺失立即失败），再用锚定的 `-run '^(TestA|TestB)$'` 执行；按 OS/build tag 维护允许集合，避免测试改名后命令仍成功但实际零匹配。
- [ ] `ubuntu-stress`/Docker smoke 仍应使用生产默认的最多 8 个 poll candidate、4 个 worker；若为缩短 fixture 而放宽上限，必须另有一条不放宽上限的回归测试，不能把“测试通过”误解为生产调度通过。
- [ ] 先明确“优化不能改变”的契约：不重复、不丢失、物理 cursor 单调、semantic frontier 可解释、单 chat 隔离、Graph/磁盘故障可恢复、TUI 冲突不自动重试。
- [ ] 任何新缓存都必须先证明其失效条件；禁止用长期 negative ownership/source cache 隐藏新写入的 legacy/interrupted turn。优先复用同一 cycle 的正向安全快照，并将 backoff/continuation 持久化。
- [ ] 独立发送 worker 不是本阶段的默认答案：只有在 phase/预算/指标优化后仍有证据表明主循环编排是瓶颈，且已用测试锁定 owner、FIFO、pacing、accepted/ambiguous 和 supervisor 语义，才单独评审拆分方案。

### 初版 required-test manifest（实现时必须逐项落地）

下表是计划的最小可执行入口，不代表这些测试已经存在或已经通过。测试名、job 和脚本一旦确定就作为 CI 合同；若实际 package 需要调整，必须同步修改 manifest，不能只放宽 selector。

| lane | package | exact test name | fixture/backend | 必须断言 | 允许 skip |
| --- | --- | --- | --- | --- | --- |
| P0 presubmit | `internal/teams` | `TestTeamsCyclePhaseScopeAndProgress` | 3–16 chat，fake Graph，JSON/SQLite | phase deadline、chat/process scope、cursor/ledger/disposition 单调 | 否 |
| P0 presubmit | `internal/teams` | `TestTeamsOwnerCapabilityTakeover` | poll/outbox claim barrier，JSON/SQLite | 旧 owner final CAS/POST 前被拒，不覆盖新 owner | 否 |
| P0 presubmit | `internal/teams` | `TestTeamsTranscriptClaimSourceRewriteFence` | claim 后 rewrite barrier | 先 durable fence/hold，不能仅 `skipped` 越过 proof | 否 |
| P0 presubmit | `internal/teams` | `TestTeamsTranscriptSourceIdentityReplacement` | delete/recreate、same size/mtime、rename | 普通 idle path 不把新 source 永久当 unchanged | 否 |
| P0 presubmit | `internal/teams` | `TestTeamsExternalSideEffectRecovery` | sender/store fault hooks，JSON/SQLite | POST 后未知结果不盲发，ledger/provenance 失败可恢复 | 否 |
| P0 presubmit | `internal/teams` | `TestTeamsHistoryWatchErrorIsolation` + `TestTeamsLinkedTranscriptMissingSourceDoesNotStarveHealthySession` | 首个坏 path/session + 后续健康对象 | local 首错不阻塞后续，process-wide 错误停止写入 | 否 |
| P1 Linux/Docker | `internal/teams` | `TestTeamsOutboxAttemptBudgetAndFairness` | 失败 row + 健康 chat，生产 2/8 lane | attempts/rows/pages/time 有界，健康 chat 在数值 SLA 内推进 | 否 |
| P1 Linux/Docker | `internal/teams` | `TestTeamsHistoryWatchBackoffAndResume` | missing/bad/append path，重启 | `NextProbeAt`/cursor 持久，append/repair 可唤醒 | 否 |
| P1 Linux/Docker | `internal/teams` | `TestTeamsOutboxPacingReservation` | global/targeted 并发，fake sleeper | 同 chat 实际 POST start 间隔满足契约，不串行化不同 chat | 否 |
| P1 Linux/Docker | `internal/teams` | `TestTeamsQueuedAdmissionBoundedStart` + `TestTeamsQueuedAdmissionPhaseDoesNotCancelStartedTurn` + existing `TestBridgeQueuedTurnFollowUpStaysWithinCompletedSession` | 慢 executor，多 session/follow-up，阶段 deadline | 新增测试锁定 active/start 上限和已启动 turn 不被阶段 context 取消；既有测试锁定 follow-up 不绕过 admission | 否 |
| P1 backend | `internal/teams/store` | `TestStoreJSONSQLiteDispositionParity` | 相同 outbox/checkpoint/frontier fixture | pending、receipt、cursor、重启结果规范化一致 | 否 |
| P1 backend | `internal/teams/store` | `TestStorePollFrontierAndScheduleRevisionRace` | same timestamp、overlap、revision update | 不漏不重，旧 revision CAS 被拒且新计划可运行 | 否 |
| P1 Linux/Docker | `internal/teams` | `TestTeamsDuePollAndPhaseDeadline` | future active rows、Graph blackhole | due candidate/decode 有界，phase 超时可恢复且不假报健康 | 否 |
| nightly/manual | `internal/teams` | `TestTeamsBacklogScaleAndVirtualOutage` | 100/500/1000 chat、72h virtual time | 最大等待/资源/恢复契约；超大规模不进入普通 presubmit | 仅平台明确不支持时按 allowlist |

P0 测试不允许因 Docker、平台或环境能力缺失而静默 skip；P1/P2 的平台限制必须有 allowlist 和原因。CI 先用 `go test -list`/`-test.list` 做 exact diff，再执行锚定 selector，并从 `go test -json` 验证每个 required test 实际 `pass`，不能把零匹配、业务 eligible=0 或未允许的 `skip` 当作成功。

### P0 release gate

- [ ] P0 只有在生产 `Listen` 与测试 harness 共用同一 phase executor、所有 P0 测试在 JSON/SQLite 需要的 backend 上实际 `pass`，并且没有未解释的 `skip` 后才算完成；只让测试专用 `runOneCycle` 通过不算完成。
- [ ] P0 的最终报告必须同时给出每个输入记录的 durable disposition、cursor/frontier/ledger 结果、Graph POST 次数和 scope/outcome；不能只报告“没有 panic”或“最终函数返回 nil”。
- [ ] P0 失败时，发布策略是停止对应 phase/进入可观测 degraded/hold 并在恢复后继续，不是扩大重试、清除 fence、回退 cursor 或盲目重发。P1/P2 的吞吐优化不能先于这些 gate。

## 6. 多轮 review / 收敛流程

- [x] **Round 1 — 设计动机与现有测试盘点**：已用 git history/blame、实现、测试和 CI selector 核对；多份独立审查一致认为安全局部覆盖较强，但真实主循环、多日/大规模、性能阈值不足。
- [x] **Round 2 — 安全与 liveness review**：由全新的、无上下文 subagent 审查本 checklist，确认 P0 应验证局部隔离和最终可解释状态，不能用“无条件清空 backlog”替代 fail-closed fence；补充跨 session early return、失败 outbox budget 和普通 transcript ambiguous POST 的覆盖。
- [x] **Round 3 — 性能与过度设计 review**：由全新的 subagent 检查是否能通过一个调度/预算/可观测性改动解决多类热点；将 100 chat 放入普通 CI，把 500/1000、10K/100K、长 soak、双进程和 provider protocol 降到 nightly/诊断；收窄“主循环串行”和 SQLite projection 的表述。
- [x] **Round 4 — CI/Docker/迁移 review**：由全新的 subagent 核对 Docker 无网络约束、旧版本 fixture、race/平台边界、测试时长和 selector 非空；将完整 `Listen`/supervisor 拆成需要 clock seam 的后续工作，将精确 selector manifest、默认 8/4 调度上限和计数型硬断言加入计划，避免无法落地或引入 flaky 门禁。
- [x] **Round 5 — 安全范围、性能预算与实现落地性 review**：由三个全新的、无上下文 subagent 分别审查 safety/liveness、性能/架构和实现/CI/兼容性。共同确认必须增加 process-wide store error 的统一分类、HistoryWatch 持久退避/公平 cursor、最小外部副作用 crash matrix、terminal-fence backend parity；同时把 100/1K vertical slice 与 500+/长 soak 分层，禁止把完整 Listen fake-clock、独立 worker 或 provider 协议一次性塞入 P0。
- [x] **Round 6 — 最终 release-gate 反向 review**：由三个全新的、无上下文 subagent 审查修订版 checklist。结论仍要求补齐 phase deadline/超限 disposition、P0 的真实 owner capability CAS、claim 后 source-proof fence、普通 linked idle path 的 source identity/delete 测试、attachment/ledger crash 边界、准确的 status+reason/flag 映射，以及可执行的 required-test manifest；同时确认 due query、queued admission、pacing、HistoryWatch backoff 和 100-chat/500+ 分层方向正确。
- [x] 每轮 review 后都重新检查：测试是否能失败、是否测试了用户可见结果、是否区分 safety 与 liveness、是否包含重启/Graph/磁盘组合、是否记录性能计数。
- [ ] 只有同时满足“覆盖真实历史故障 + 不削弱安全边界 + 能在 CI/Docker 稳定执行 + 性能指标可比较”才把该项从计划移入实现。

## 7. 当前结论

- [x] 有用的核心是 source proof、owner/CAS、durable outbox/ledger、history/live 分离、bounded parser、per-chat isolation 和 durable frontier；这些不是导致慢的根因，不能为了性能删除。
- [x] 目前最可疑的低效点是：`Listen` 的串行 phase 编排（不是所有 work-chat polling 都串行）、坏/blocked row 的重复扫描、history watcher/linked sync 的全量候选与 early return、due 前 active chat 的重复解码、每消息多层 ledger/JSON/SQLite 操作、同 chat pacing 的同步等待，以及仅报告不门禁的性能测试。
- [x] `tail_too_large` 的正确方向不是取消阈值，而是把它限定为单轮资源预算，并保证物理 cursor、quarantine/disposition 和后续消息可达。
- [x] 当前证据不足以声称“服务在数百 chat、数天 outage、Graph 黑洞、磁盘故障交叉下最终一定清空”；P0/P1 测试完成前不应作此承诺。
- [ ] 本文档中的未完成测试计划不能被现有 benchmark 名称或单函数测试替代；实现任何性能优化前必须先完成对应的行为基线。
- [x] 第二轮 review 还确认：`pollOnce` 已有最多 4 个 work-chat worker，测试应验证 phase 之间的隔离与全局公平，而不是假设所有 chat 都串行；`HistoryWatch` 的 SQLite canonical state 不应被描述为每次全量重写，但 projection、文件探测、JSON backend 和 origin lookup 仍需计数验证。
- [x] 第二轮 review 还确认：失败 outbox 的目标不是盲目 quarantine，而是限制 attempts/scan/time、持久化 retry/disposition，并保持新消息和其他 chat 可继续；Graph pacing 的目标不是把所有 chat 串行化，而是只约束同 chat 的实际 POST。
- [x] 第三轮 review 还确认：完整 `Listen` 的虚拟时间测试必须先有 Clock/Timer seam；当前可立即落地的是小规模 `runOneCycle`/phase harness。SQLite busy/full 属于 process-wide degraded state，不能套用 work-chat 隔离断言；普通现代 source 删除必须通过真实 reconcile/scan 路径验证，而不是只直接调用清理函数。

## 执行记录

- 2026-08-29：完成 Round 1 源码/历史/测试/CI 审计；未修改生产代码，未操作 live Teams service。
- 2026-08-29：加入本 checklist，完成 Round 2 安全/liveness、Round 3 性能/过度设计和 Round 4 CI/Docker/迁移独立 review；确认跨 session early return、失败 outbox budget、pacing 竞态、HistoryWatch 删除/全局预算等风险，并收窄 CI/nightly 范围；仍未修改生产代码，未操作 live Teams service。
- 2026-08-29：完成 Round 5 和 Round 6 的六份全新、无上下文 review；根据反馈加入 process-wide `PhaseOutcome` 边界、phase deadline、owner capability/source-proof P0 gate、source identity/delete、真实副作用 crash matrix、迁移/状态兼容契约、HistoryWatch 持久退避、due query、queued admission、pacing 和 required-test manifest；仍未修改生产代码，未操作 live Teams service。

## 8. 外部运行报告的价值核对

这部分把另一轮运行报告中的观察与 `origin/main=138c1ef` 的源码证据分开，避免把合理假设误当成已定位的热点。

### 已确认且值得保留的内容

- [x] “transcript 同步、HistoryWatch、outbox 和 polling 共用 `Listen` 的 phase 编排”是正确的。phase 顺序是 outbox、workflow、poll、queued turns、linked transcript、HistoryWatch；单个 phase 的耗时会推迟后续 phase，但 work-chat polling 内部仍有最多 4 个 worker。
- [x] “单个错误可能让后续工作得不到机会”是高价值发现，但范围应限定为当前直接 `return` 的 linked-transcript/HistoryWatch 路径和 phase 级错误；Graph work-chat 的部分错误已经有 chat-scoped 隔离，store/lease 错误则是 process-wide degraded case。
- [x] “Graph `last_success` 不代表 transcript 在推进”是重要的运行诊断原则。必须同时观察 checkpoint/frontier、delivery ledger、outbox disposition 和可见 POST；不能用 Graph 入站成功替代 backlog drain 成功。
- [x] “CPU 高、读取量大、durable progress 为零”是强烈的读放大/重复扫描信号。若采样对象确实是 bridge 进程，约 106% CPU、约 33 GB read 和约 790 万 syscall 在 15 分钟内非常值得保留为故障现场；但它们只能证明异常成本和缺少进度，不能单独区分 linked scanner、HistoryWatch、store projection 或其他线程。
- [x] “未变化输入必须快速返回”和“按 chat/path 隔离进度”是正确优化方向。当前源码已有部分 stat/idle fast path 和 per-session checkpoint，但仍存在 early return、重复 stat、origin 全量查询和全局无预算路径，不能认为目标已经完全实现。
- [x] “增加阶段级计数和耗时指标”是风险最低、价值最高的下一步。至少应记录 phase 时长、session/path 数、读取 bytes、解析记录数、SQL rows/transactions/lock wait、Graph request/post/attempt、checkpoint/frontier 推进量、outbox disposition 和 queue depth。

### 需要收窄或修正的内容

- [x] 每 session 8 条和 1.2 秒发送间隔确实存在，但“单 chat 必然不到每秒一条”只适用于每条记录都变成同 chat 的独立 Graph POST；它不是所有 transcript/backlog 的统一吞吐结论。1.2 秒的 pacing 还存在 global/targeted 并发 reservation 竞态，不能直接把等待时间当成有效吞吐。
- [x] `history_watch_projection` 约 2.7 MB 本身不是 CPU 高的充分证据。SQLite 已避免每次重写 canonical `state_json`，但 projection 的 clone/compare/marshal、JSON backend 全量 load、目录/stat 探测和 `HistoryWatchOriginState` 的 sessions/turns decode 仍值得用计数器验证。
- [x] source proof、legacy 检查和大记录处理的重复读取有安全原因；不能用一个长期负 ownership cache 解决，因为负缓存可能隐藏新写入的 legacy interrupted turn。应优先复用同一 cycle 的安全快照、增加 durable disposition/backoff，并验证 unchanged fast path。
- [x] “把 Graph 发送全部移到独立后台 worker”是可能的架构方向，但不是无条件的第一步。现有 durable outbox 已把持久化与发送解耦；如果再拆 worker，必须保留 owner lease、per-chat FIFO、pacing、accepted/ambiguous fence 和主循环公平性。先做 attempts/scan/time budget、错误隔离和指标，再决定是否拆 worker。
- [x] “只要一直等就能排空”不能从报告推导。15 分钟无 checkpoint/frontier 推进且 read/syscall 持续增长，应优先判为疑似 livelock/读放大并触发诊断；只有观察到 durable frontier 单调增长，才能把它称为慢但会排空。

### 对后续性能工作的直接约束

- [ ] 先建立按 phase 的最小观测，再做优化；不能只看进程总 CPU 或 Graph `last_success`。
- [ ] 优先验证并修复四个确定性问题：跨 session/path early return、失败 outbox 不消耗预算、global/targeted pacing 竞态、现代 source 删除留下孤儿 checkpoint。
- [ ] 然后测量全局工作量：HistoryWatch 总 path/bytes/time、linked transcript 总 session/record/bytes、outbox page/row scan、due poll candidate decode、queued active execution 和 JSON/SQLite ledger I/O。
- [ ] 任何优化必须保持 source proof、ownership fence、accepted/ambiguous 语义、物理 cursor 单调、单 chat 隔离和 TUI 冲突不自动重试；性能指标必须以固定 fixture 的计数或基线差分形成可失败门禁。

## 9. 本轮执行状态（2026-08-29）

本轮实际落地的是一个可独立交付的 P0/P1 垂直切片；本节不把尚未实现的长期计划标成完成。

### 已完成的生产改动

- [x] **阶段边界与恢复**：为主循环阶段增加有限 wall-time budget、deadline outcome 和阶段统计；阶段超时只结束当前阶段/周期，不触发错误的 watchdog 健康假设或无限重启。已启动的异步 queued turn 使用 listener 生命周期，而不是阶段短 context，避免“启动后被阶段超时取消”。
- [x] **局部隔离与继续推进**：HistoryWatch 和 linked transcript 对 chat/path/session-local 错误记录首错后继续处理其他对象；process-wide store/lease 错误仍停止后续 durable mutation，保留安全边界。现代 source 删除会通过 identity-aware CAS 清理对应孤儿 checkpoint。
- [x] **outbox 有界扫描与持久退避**：main/targeted flush 同时限制成功数、扫描 row 数和 page 数；失败 queued row 通过 `NextAttemptAt`（SQLite 兼容使用现有 `deliver_after`）持久化 retry gate，避免坏 row 反复占满热循环，同时不把未知外部副作用盲重发。
- [x] **发送节流竞态**：同一 chat 的 global/targeted flush 在等待前预留 pacing 时隙，避免并发 POST 撞车；不同 chat 不共享不必要的等待。
- [x] **owner capability 与 source-proof 安全**：poll frontier 的最终 mutation 使用 owner/process/lease capability；transcript claim 后再次 proof 失败会先建立 rewrite fence/hold，不会仅写 `skipped` 假装安全推进；source identity replacement 会进入 bounded proof/rewrite 路径。
- [x] **后端兼容**：JSON/SQLite 对 outbox retry gate、pending 查询、清除 rate-limit 和 poll capability 保持等价语义；保留旧 API 包装，避免现有调用方和旧 fixture 被无谓破坏。

### 已新增并实际验证的回归测试

- [x] `internal/teams`：`TestTeamsCyclePhaseScopeAndProgress`、`TestTeamsOwnerCapabilityTakeover`、`TestTeamsTranscriptSourceIdentityReplacement`、`TestTeamsTranscriptClaimSourceRewriteFence`、`TestTeamsExternalSideEffectRecovery`。
- [x] `internal/teams`：`TestTeamsHistoryWatchErrorIsolation`、`TestTeamsLinkedTranscriptMissingSourceDoesNotStarveHealthySession`、`TestTeamsOutboxAttemptBudgetAndFairness`、`TestTeamsOutboxPacingReservation`。
- [x] `internal/teams`：`TestTeamsQueuedAdmissionBoundedStart`、`TestTeamsQueuedAdmissionPhaseDoesNotCancelStartedTurn`、既有 `TestBridgeQueuedTurnFollowUpStaysWithinCompletedSession`。
- [x] `internal/teams`：`TestTeamsDuePollAndPhaseDeadline`、`TestTeamsBacklogScaleAndVirtualOutage`（当前为小规模/虚拟 outage smoke，不等价于 500+ chat 或 72h soak）。
- [x] `internal/teams/store`：`TestStoreJSONSQLiteDispositionParity`、`TestStorePollFrontierAndScheduleRevisionRace`。
- [x] 测试断言覆盖用户可见 liveness 和 durable safety：健康 chat/session/path 不被坏对象饿死；cursor/frontier/ledger/disposition 不倒退；未知 Graph 结果不盲发；retry gate 不热循环；阶段结束不取消已获准 turn；同 chat POST pacing 不竞态。

### 本轮验证结果

- [x] 受影响包完整回归：`CXP_RUNTIME_DISABLE=1 go test ./internal/teams/store ./internal/teams -count=1` 通过。
- [x] 新增 manifest 的精确 `go test -list` selector、精确执行 selector、受影响包 `-race` 和 `go vet ./internal/teams ./internal/teams/store` 均通过；当前测试改名/新增后的 selector 已重新核对，没有零匹配。
- [x] `git diff --check` 通过；没有触碰 live Teams helper、服务进程或运行时数据库。
- [x] 已尝试全仓库回归；Teams/store 相关包均通过。仅剩两个与本切片无关的已知环境/其他 worktree 问题：`internal/helperpath/TestOSExecutableUsageIsCentralized` 扫描到其他 worktree 的旧源码，以及 `internal/helperruntime/TestLaunchKeepsExplicitSameBasePrereleaseActive` 的既有启动 fixture 失败。另一个选定 benchmark 的 TempDir cleanup 失败属于既有 benchmark fixture 问题；其余选定 benchmark 已产生可用读数。

### 原始用户问题与测试证据的对应关系

- [x] `s512` 的 pending root/扫描快照竞态：既有 `TestBridgeSyncLinkedTranscriptPublishesSafePrefixBeforePendingRootMarker` 验证安全前缀会发布、checkpoint 会推进、不会刷 `helper publish-history`，并且重复扫描不重发。
- [x] `s514` 的单条超大记录：既有 `TestBridgeSyncLinkedTranscriptAdvancesPastOversizedRecord` 和 `TestHistoryTieredScanTailAdvancesPastOversizedRecord` 验证大记录被作为 opaque/quarantine 消费并推进物理 cursor，后面的 final 可在下一轮到达，不把整个 chat 永久 block。
- [x] `s519` 的超大 tail/空可发布记录：既有 `TestBridgeSyncLinkedTranscriptAdvancesOversizedTailIncrementally` 与 `TestHistoryTieredScanTailDrainsOversizedPartialRecordAcrossPolls` 验证扫描按有界批次推进，后续 final 可达，不因本轮没有可发送记录而重复读取同一位置。
- [x] 本轮新增的跨对象 liveness 问题：`TestTeamsLinkedTranscriptMissingSourceDoesNotStarveHealthySession` 固定“首个缺失 source、后续健康 session”的顺序，验证前者的本地 source 问题不会阻塞后者；`TestTeamsHistoryWatchErrorIsolation` 对应坏 path。
- [x] 本轮新增的无进度/读放大风险：`TestTeamsOutboxAttemptBudgetAndFairness`、`TestTeamsBacklogScaleAndVirtualOutage`、`TestTeamsDuePollAndPhaseDeadline` 验证失败/门控 row 有扫描和时间边界、健康对象可继续、Graph 黑洞不会把 phase 变成无限等待；这些是确定性 smoke，不等价于完整多日 soak。
- [x] 本轮新增的并发/恢复安全边界：`TestTeamsOwnerCapabilityTakeover`、`TestStorePollFrontierAndScheduleRevisionRace`、`TestTeamsTranscriptClaimSourceRewriteFence`、`TestTeamsExternalSideEffectRecovery` 和 `TestTeamsQueuedAdmissionPhaseDoesNotCancelStartedTurn` 分别保护旧 owner、source rewrite、未知 Graph 副作用和阶段 deadline 下的已启动 turn。

### 明确未在本轮冒充完成的范围

- [ ] HistoryWatch 持久化 `NextProbeAt`/公平 continuation cursor；当前已做到单路径错误隔离，但没有声称长期坏 path 的探测成本已完全有界。
- [ ] SQLite due-oriented poll 查询、完整 phase `PhaseOutcome` 协议和正式 metrics/CI 性能阈值；当前先落地窄的 budget/counter/安全边界。
- [ ] 完整 `Listen` fake-clock harness、真实双进程 takeover、provider idempotency、attachment/ledger 全 crash matrix、500/1000 chat、10K+ backlog、virtual 72h Docker soak；这些仍按原计划属于后续 P1/P2/nightly。
- [ ] 因此本轮可以确认原始已暴露的“无消息推进/坏对象阻塞健康对象/失败 outbox 无限扫描/phase 超时取消已启动 turn/source-proof 与 owner 竞态”在覆盖路径上有回归保护，并显著降低 CPU/读放大风险；不能据此宣称所有多日 outage、超大规模和平台边界已被证明。

### 本轮执行日志补充

- 2026-08-29：精确 manifest `-list` 与 anchored selector 均无零匹配；新增 Teams/store 回归、原始 s512/s514/s519 回归和对应 `-race` 已通过。
- 2026-08-29：`go test ./internal/teams/store ./internal/teams -count=1`、精确 `-race` 和 `go vet ./internal/teams ./internal/teams/store` 通过；`go test ./... -count=1` 的退出码仍受上面列出的两个无关失败影响，但 `internal/teams` 与 `internal/teams/store` 在该全仓库运行中通过。
