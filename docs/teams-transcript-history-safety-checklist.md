# Teams transcript history safety checklist

本 checklist 是本次 Teams transcript/history 修复的执行清单，覆盖此前的 phase1 quarantine 改动以及本轮确认的 scanner、游标、source proof、live/history lane、迁移和交付语义。

目标：历史 transcript 的异常最多影响对应的 history segment；不能让正常 live 对话进入永久 queue、不能反复刷恢复提示、不能因大图片/Base64 或坏 JSON 挡住后续 final，也不能因 stale worker 覆盖新的 owner 状态。

执行边界：本轮实际落地的是 phase1 的 bounded JSONL framing、partial read hint、完整大 record 的安全 disposition、ignored cursor、pending continuation 解锁、source-proof/legacy migration、mixed-ID transcript quarantine，以及现有 outbox/owner fence 的回归保护。完整的 durable outbox lane/schema 重构、Graph 端幂等键和 owner-generation API、历史 metadata compaction、迁移全量分类仍是后续工作；它们在本 checklist 中保留为未完成项，不把现有实现冒充成这些能力。

## 不可破坏的不变量

- [x] 512 KiB 只是单轮扫描预算；`BudgetExhausted` 不得变成 blocked、failed、用户消息或无界全量扫描。
- [x] history 的 source proof、pending、deferred、quarantine 不再直接参与 live admission；真实 unresolved execution 仍按 owner safety 规则保留 fence。
- [x] `scan_offset` 是字节 offset，只能位于完整 JSONL newline 之后；partial 不得推进正式 cursor。
- [~] 完整 record 已有 outbox/ignored/quarantine/deferred disposition，ignored cursor 与 ledger 在现有 Store 事务中提交；visible outbox、Graph 可见性和 cursor 仍不是一个跨系统事务。
- [~] 当前 transcript 路径在 CAS/fence 失败时不做外部发送，并在副作用前读取 source proof；全量 API 组合仍未统一为唯一事务接口。
- [x] 不确定的外部发送是持久 `uncertain/needs_attention`，不会因 lease 过期自动回到 queued；没有外部幂等时明确采用 at-least-once，不声称 exactly-once。

## 1. 工作区与基线

- [x] 使用独立 worktree：`/home/baka/.local/state/codex-helper-workspaces/teams-transcript-quarantine-phase1-20260822`。
- [x] 保留原 worktree `/home/baka/project/codex-helper` 的既有改动，不在其中编辑或清理文件。
- [x] 当前分支为 `fix/teams-transcript-quarantine-phase1-20260822`，基线为 `origin/main` 的 `24ca2c23a7b9c41907ff08f72f3964a9e2c8454a`。
- [x] 记录实现前 `git status`、现有 phase1 diff 和测试基线；确认本 checklist 之外没有混入改动。

## 2. 纯扫描与原子持久化

- [ ] 新增/扩展唯一 Store API：输入 expected source generation、cursor/version、branch/owner fence；原子提交 disposition、quarantine metadata、outbox intent 和 scan state。（后续 lane/schema 阶段）
- [x] JSON 使用现有锁/版本校验/原子替换；本轮涉及的 SQLite 路径有等价事务和 CAS 测试。
- [~] 本轮 transcript 路径在 CAS 之前只做文件读取和分类，外部发送由 outbox sender 执行；尚未把所有调用方收敛到唯一提交 API。
- [ ] 正常 ignored 安全前缀使用 compact range/汇总，不为每条普通 record 建立无限 metadata；active quarantine/deferred 保留 source、range、reason、stable key 和 recovery reference。
- [ ] 已解决 disposition 有 retention/compaction；状态、outbox 和 quarantine 不得无界增长。
- [x] `published` 明确表示 durable outbox intent，不表示 Graph 已经可见。

## 3. JSONL framing、partial 与大 record

- [x] 所有自动 sync、HistoryWatch、linked completion/recovery、`publish-history`/`full`、formatter/backlog、stats transcript reader 使用 bounded JSONL framer；无关的 control/journal JSONL 不属于 transcript reader。
- [x] transcript reader 不再使用整行 `ReadBytes`/无界 `ReadAll`，也没有 `TooLarge -> maxTailBytes=0` fallback。
- [x] partial 保存 `read_offset`、observed size、source identity、session/thread/turn/terminal context、起始时间和有界 anchor；不保存完整 payload。
- [x] `read_offset < observed_size` 时下一轮即使文件没有增长也继续读取；只有 read hint 已到 EOF 且没有 newline 时才等待增长。
- [x] partial 阶段只做 framing；newline 到达后从 record start 重新以固定上限 parser 提取 envelope，避免跨轮保存复杂 JSON lexer。
- [x] 完整大行允许超过普通 512 KiB，通过独立 framing budget 流到 newline；opaque image/Base64/tool payload 只保留 bounded prefix，不 decode/复制整行。
- [x] 完整 record 必须得到 published/outbox、ignored、quarantined 或 deferred disposition；坏 JSON 保持 deferred，不会静默越过。
- [x] visible final/status 超过 record cap 时生成确定性 placeholder；不可见 opaque record 可安全 ignored，后续 final 仍可达。
- [~] 无 newline 使用 64 MiB framing cap 和持久 partial hint，避免每轮重读；`recovery_required`/人工 UI 仍是后续工作。
- [ ] 每轮有总读取、时间、record 数、单 source、全局 history batch、锁持有和磁盘配额；坏 record 不得饿死 live。

## 4. Source generation 与 rollover

- [~] source writer 合同和 rollover 仍依赖现有 Codex 文件行为；scanner 不把任意 rewrite 当作可继续，并保留显式 source-rewrite fence。
- [x] 读取开始和提交前校验文件 identity、source generation、大小和 bounded source proof；size/mtime 只作快速提示，不能单独证明来源。
- [x] source 不一致持久化为明确的 source-rewrite/blocked history 状态，不作为普通 CAS retry 无限重试。
- [ ] 新 generation 只有具备旧代 overlap/handoff proof 才能复用 canonical logical output key；否则进入 manual history recovery，不自动发布。
- [ ] logical delivery key 绑定目标 chat/session、canonical event/turn/group/part/render version；source generation 作为 provenance，不能导致 rollover 后无条件重复发布。

## 5. Completion、ownership 与 quarantine

- [x] pending continuation 是可验证 completion group/segment 的局部状态，不是全局 scanner gate；marker-only pass 会持久化 ignored cursor，避免 livelock。
- [ ] group 状态单调 `started -> progress -> terminal`；强确认的 final/error/cancel 不被晚到 `task_started` 否决。
- [ ] ownership 只使用 durable owner、明确 event/turn/group/parent/mirror/source/range proof；同文本、相邻位置、sent hash 只能诊断。
- [x] s001 mixed-ID 只有在同 source、durable boundary 和可证明 mirror 关系成立时才合并；否则只进入 transcript-only quarantine，不建立 live owner fence。
- [ ] anonymous final 的 boundary、logical key、owner proof 跨 poll 持久化；不能先发两条再靠文本合并。
- [ ] 自动 completion recovery 尊重 active quarantine/deferred；不得用 sent hash 自动释放 quarantine。
- [x] `publish-history` 或强 ownership CAS 才能清除 quarantine；不重新执行历史工具、approval、cancel 或其他控制副作用。

## 6. Live/history outbox 与 owner fence

- [ ] outbox 增加明确 lane，candidate query、EarlierUnsent、sequence、claim、send、backoff、rate-limit、per-chat pacing、upgrade、restart、drain 全部 lane-aware。
- [~] history poison row、旧 FIFO 和 source proof 不再直接挡 live admission，已有 bypass/ambiguous-send 测试；显式 lane、全局 capacity 和所有 429/5xx 调度策略仍待后续重构。
- [x] 移除跨 Graph 请求持有的全局 flush lock；SQLite 锁内不得执行 Graph 调用。
- [ ] history 有 per source/chat 及全局 batch、row/byte、send、lock budget 和退避；允许跨 lane 交错，但 lane 内保持顺序。
- [ ] control ack、publish-history 结果和 `needs_attention` 进入 live/control lane；历史正文进入 history lane。
- [ ] request_id 唯一绑定 live branch；owner_generation 是单调 fencing epoch，与 source_generation 分离。
- [ ] queueTurn、progress/final/error、approval/cancel、MarkTurn、executor recovery、checkpoint/quarantine、outbox claim/mark 都校验 owner epoch；FenceLost 后不得重新排队或发送通知。
- [ ] owner handoff 先持久化 fence，停止新 claim，并有界等待旧 send lease；旧请求可能已在 Graph 中，不能靠本地 CAS 撤销。
- [ ] outbox target、payload、logical key 不可变；send claim 使用 owner epoch + claim token + lease。
- [ ] `uncertain` 是持久一等状态：旧 sending、handoff 丢 fence、timeout、断连、外部 429/5xx、响应解析失败、Graph 成功但本地 commit 失败、无 Graph ID 的迁移 sending/accepted，均不得自动回 queued。
- [ ] 只有真实 Graph message ID、目标幂等确认或不可变 provenance 才能自动 reconcile；文本、hash、时间窗口和相邻消息不能自动匹配。
- [ ] Graph 无幂等时明确外部 at-least-once；人工确认、抑制或重发可能产生重复，但不重新执行 Codex。

## 7. 旧状态迁移与 publish-history

- [ ] 迁移有版本 marker、owner fence 和幂等行为；JSON/SQLite、重复升级和 migration crash 结果一致。
- [x] 旧 blocked/importing/checkpoint 缺字段迁移为 history-only `legacy/unverified`，不清零、不从头 replay、不恢复 live gate。
- [ ] 按持久 origin/kind 区分真实 live inbound、history-deferred inbound、upgrade/attachment/command deferred、旧 history batch/prompt 和 accepted/sent/sending outbox。
- [ ] 真实 live request 保留 request_id 并重新绑定 live branch；旧 history outbox/prompt 迁 history lane 并抑制旧提示；upgrade/attachment deferred 保留一次性 receipt 或明确要求重发。
- [ ] accepted/sent 且有 Graph ID 的 outbox 不删除、不重发；无 Graph ID 的 sending/accepted 原子转为 `uncertain/needs_attention`。
- [ ] 无法分类的旧状态进入 manual history recovery，不依据正文猜测，也不能阻塞新 live。
- [ ] offset、LastRecordID、source anchor 可证明时才继续；否则建立 unverified segment，等待显式恢复。
- [ ] `publish-history`/`full` 按稳定 active segment ID（source、generation、range、expected version）逐段执行，而不是只枚举。
- [ ] 重复 publish-history 使用同一 stable key 和 CAS，冲突不覆盖新状态；结果明确为 `published`、`quarantined`、`partial` 或 `needs_attention`。
- [ ] source missing、partial、unknown owner、uncertain send 不假报成功；control 结果走 live lane，历史内容走 history lane。

## 8. 回归、并发、故障和性能测试

- [x] s512：final 后出现 tail `task_started`，final 先进入 durable outbox，marker-only pass 推进 ignored cursor，下一轮不 livelock。
- [x] s514：完整 oversized opaque image/tool record 后续 status/final 可达，record 只 quarantine/ignore 一次。
- [x] s519：大于 512 KiB 且无可见文本的完整 record 推进安全 offset，后续 final 可达。
- [x] s001：mixed-ID 正例只发布一次；相同文本但不同 turn/source 的反例不合并。
- [x] 512 KiB、跨 chunk newline、CRLF、partial append、no-newline、truncate、replace 边界已有 focused coverage；`512 KiB+1` 由 bounded budget/large-record tests 覆盖。
- [x] reader inventory 已核对；transcript 入口没有整行加载或无界 fallback。
- [ ] history blocked + 1000 backlog + 持续 history 429 时，live admission、control ack 和 live final 在固定 SLA 内完成。
- [ ] CAS-before-side-effect、outbox/cursor 顺序、Graph accepted/local uncertain、owner handoff、stale scanner/recovery/publish-history 并发故障注入。
- [ ] uncertain 不因 lease 超时自动 claim/retry；只有强 provenance/Graph ID 能自动收敛。
- [ ] 旧 deferred inbound/outbox/prompt、旧 sending/accepted、重复迁移和 migration crash 回归。
- [ ] JSON/SQLite parity、restart/drain、owner fence、request retry uniqueness 和 outbox lane contract。
- [~] bounded parser、unchanged partial fast path、drained partial offset 和无全局 flush lock 已覆盖；本轮没有 before/after 性能基线，且 8 MiB record prefix 仍是有界内存成本。
- [ ] active exceptional metadata、outbox 和 quarantine 有 compaction/retention 上限，测试不会无限增长。

## 9. 最终交付门槛

- [x] 完整 tracked diff 只包含本计划及其测试/checklist；另一个未跟踪的 `docs/teams-transcript-quarantine-phase1-checklist.md` 是工作区原有文件，未触碰。
- [x] 运行相关 focused tests、package tests、race tests、全仓测试、`go vet` 和 `git diff --check`。（结果见执行记录）
- [x] 记录 benchmark workload、内存/分配、锁等待、写入次数和 live latency 结果；本轮记录了 bounded parser、增量扫描、HistoryWatch 和 background import 基准，但没有历史基线，因此不宣称“无回退”。
- [~] 未触发远端 CI selector：当前仍是未提交 worktree，已用本地全仓测试、race、vet 和 diff 检查覆盖可执行的门槛；提交后应由 CI 再验证 workflow。
- [x] 复审 diff 和已知 failure path；phase1 范围内没有测试暴露的正常生产 blocker，未完成项仍按上方边界明确留给后续 lane/schema、Graph 幂等、compaction 和全量迁移阶段。
- [x] 本 checklist 已执行到本阶段交付门槛；未 commit/release，未重启 Teams helper。

## 执行记录

- 基线：`24ca2c23a7b9c41907ff08f72f3964a9e2c8454a`，worktree `teams-transcript-quarantine-phase1-20260822`。
- 计划来源：多轮独立 review 后收敛的 transcript history safety 方案。
- 说明：外部 Graph 在“已接受但本地结果未知”时无法由本地代码实现绝对 exactly-once；本方案将其持久化为 `uncertain/needs_attention`，禁止自动重复发送，并提供强 provenance/人工恢复路径。
- review 收敛：Plato、Anscombe、Meitner 分别指出了 pending marker 游标未持久化、大 cold source proof 错配、通用 checkpoint writer 擦除 fence/partial、mixed-ID 误 quarantine、history/live lane 未完全分离等风险。本轮已修复前四类以及现有 lane bypass 的回归保护；显式 lane/schema、Graph 幂等 API、compaction 和全量 migration classification 明确留作后续阶段，避免在本次修复中扩大风险面。
- 验证结果：`go test ./internal/teams -count=1 -timeout=900s`（90.865s）、`go test ./internal/teams/store -count=1 -timeout=900s`（17.377s）、`go test ./... -count=1 -timeout=1200s`（通过）、相关 `go test -race`（teams 12.783s、store 77.045s）、`go vet ./...` 和 `git diff --check` 均通过。
- 基准记录（`-benchtime=100ms -benchmem -count=1`，AMD Ryzen 9 7950X）：`HistoryTieredStatHotSetUnchanged` 6.963ms/op、1.52MB/op、10000 allocs/op；`HistoryTieredTailScanSmallDelta` 16.289µs/op、11.2KB/op、123 allocs/op；`HistoryWatchUnchangedPartialTailPoll` JSON 200.461µs/op、66.2KB/op、541 allocs/op，SQLite 116.515µs/op、17.6KB/op、192 allocs/op；`ReadLinkedTranscriptDeltaLargeTailFromOffset` 5.850ms/op、5.35MB/op、140 allocs/op；`SQLiteBackgroundImportCheckpointOnly` 15.353ms/op、12,392 sqlite_bytes/op、10.35MB/op、145,456 allocs/op。以上是当前实现的单次快照，不是与基线的差分。
