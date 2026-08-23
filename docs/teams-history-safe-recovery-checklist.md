# Teams transcript safe-recovery checklist

目标：让 transcript 自动同步异常只隔离受影响的历史范围，不阻塞实时 Teams 请求、不丢后续 final/status、不在不确定的外部操作后自动重试，也不因旧版本继承的 checkpoint 反复刷屏或重新生成错误的 execution owner。

执行约束：本 checklist 属于 `teams-history-recovery-plan-20260823` worktree；不修改原 worktree、live Teams service 或 live state。所有 durable state 变更必须保持 JSON/SQLite 一致、CAS/owner lease 安全，且 unchanged poll 不产生写放大。

## 0. 基线与范围

- [x] 使用最新 `origin/main` 建立独立 worktree，并确认本目录的改动与原目录隔离。
- [x] 读取 `cxp-development` 的 repository map、testing/performance 和 real-process isolation 要求。
- [x] 保留并审计已有针对 s001/s508/s512/s514/s519、mixed-ID mirror、large/malformed/opaque record、Graph no-replay 的局部修复。
- [x] 记录已确认的旧 writer/数据形状：history-only quarantine、runtime execution anchor、legacy `RecoveryReason`、partial/oversized/opaque transcript record。
- [ ] 完成完整 before/after 数值基线：unchanged poll durable writes、JSON/SQLite lock/write volume、outbox flush fairness；本轮已完成同机同命令的扫描/热路径 benchmark 对照，未把尚未测量的 p95 指标冒充完成。

## 1. 明确状态模型：物理进度、语义发布和 ownership 正交

- [x] 增加紧凑的 `ContextGapState`，同时镜像到 linked import checkpoint、history-watch checkpoint 和 scanner state；包含 source generation、gap ID、frontier identity、起点、exclusive consumed-through offset、精确 raw-range proof、phase/schema version。
- [x] 定义并校验 gap phase：`none`、`detected`、`drained`、`reset_applied`、`post_gap_committed`、`retired`；当前自动 scanner 直接提交已验证的 `post_gap_committed`，未伪装成已完成 operator migration transition。
- [x] 保留 `TranscriptQuarantine` 作为审计观察，但不把它当作 live owner 或全局 chat gate；不建立无界 range ledger。
- [x] 增加有界 active pending history range/disposition（每个 checkpoint 固定一个 active range）；不会丢弃或回退物理 cursor。
- [x] 明确物理 cursor 单调推进，语义 publish boundary / pending range 独立；source generation 切换由文件 identity/read proof 驱动，不能因为 rewrite 猜测 offset 0。
- [x] 增加独立 terminal-only boundary（record identity/start/end/generation/raw proof），不依赖是否产生 visible final，并沿 linked observation CAS 持久化。
- [x] 增加 orthogonal predicates：context gap、history-only ambiguity、runtime ownership required/proven/unknown、delivery outcome unknown；禁止用一个 `RecoveryReason` 或 `quarantine != nil` 代替这些维度。

## 2. Scanner 与 history-watch：一次隔离、持续可达、可重启

- [x] 完整 malformed line 消费到 newline 并推进物理 cursor；完整 oversized/opaque record 不猜 status/final，重置 parser context。
- [x] unterminated partial record 不跳过，保存 proof-checked partial read hint，追加 newline 后可恢复。
- [x] 同一 gap 在同一 source generation 只 reset 一次；达到 exclusive end 且提交 gap state 后，后续 poll/restart 沿用 post-gap parser context。
- [x] exact raw-range proof 覆盖 gap 起止字节；path/inode replacement、truncate、source generation 变化在扫描/发布窗口 fail closed，并保留旧 generation 的显式边界，不自动猜 offset 0。
- [x] terminal-only boundary 在无 visible final 时也能持久化并参与下一轮 ownership 判断。
- [x] s512 pending root、s514 oversized image/base64、s519 opaque tool output 均有多轮恢复回归；后续 safe final 可达，opaque gap 另有 restart 回归。
- [ ] 追加双 scanner/CAS race、equal cursor stale semantic state、crash-at-observation-commit 测试。
- [x] 保持当前 bounded scan/8 visible pacing；不靠无限增大 `tail_too_large` 阈值掩盖 framing/progress 问题。
- [x] 对 newline-less hard cap 定义不热循环、不阻塞 live admission 的行为：每轮按固定 64 MiB partial-read byte budget 分块并持久化 `PartialReadOffset`，追加 newline 后可继续消费；已有回归测试覆盖跨轮恢复。当前不额外引入 time-based parked 状态，因为 live admission 已与历史物理进度解耦。

## 3. 原子 observation commit 与存储 parity

- [x] linked/history-watch 的生产 observation writer 复用现有 backend-neutral `UpdateImportCheckpoint`/parent-fence transaction，在闭包内校验 expected source generation/cursor、next physical cursor、gap state、bounded pending disposition 和 parser/terminal state；unchanged poll 不写 durable state，物理 cursor 不回退，同 offset stale semantic state 拒绝。
- [x] 发送前保留 durable intent；Graph POST 在 store transaction 外执行；response unknown 保持 ambiguous，non-idempotent POST 不再同调用 401 refresh 或 429/5xx replay。
- [x] JSON store 与 SQLite store 的 checkpoint transaction、序列化、重启恢复和失败行为保持 parity；生产 writer 不再维护一套未使用的第二 CAS API。
- [ ] 增加 crash-at-commit、stale CAS、JSON/SQLite reopen/parity、outbox intent 与 source proof 不匹配测试。

## 4. Typed runtime ownership、legacy migration 与 anchor resurrection

- [x] 为 `ExecutionAnchor` 增加 provenance：`runtime`、`history_only`、`legacy_unknown`；新 runtime path 必须显式写 `runtime`。
- [ ] 为 Turn 增加 typed disposition 或等价字段，但所有门控使用正交 predicates；`RecoveryReason` 只作 legacy input，不能作为新状态机 authority。
- [x] 审计 startup/recovery/retry/claim/callback、Store JSON/SQLite probe/materializer、outbox/warning cleanup 的 anchor creation/lookup 路径；旧 SQLite materializer 明确标记 `legacy_unknown`，不冒充 runtime proof。
- [ ] 在 migration/control generation barrier 和 anchor generation/CAS 下阻止旧 Turn 或 SQLite probe 重新生成已清除的 synthetic anchor。
- [ ] 先做 dry-run inventory；只有持久化 history-only writer signature + exact source generation/cutoff/quarantine/canonical mirror 等高置信证据才能自动迁移；local Completed/Failed、mixed mirror 或旧 reason 单独不足以证明 history-only。
- [x] `legacy_unknown` 不会被 history-only scanner 自动 clear 或转换为 fresh branch；SQLite materializer 明确标记它并继续 fail-closed，仍需 manual/typed proof 才能处理；真实 runtime anchor 不触碰。
- [ ] 高置信 migration 在 owner lease 下原子完成：验证 anchor generation/turn/source/outbox relation，写入 history-only disposition/evidence/version，保留历史 frontier，清除 synthetic anchor，并让旧路径不能复活；失败只 defer 当前 session。
- [ ] warning cleanup 只处理精确关联且 Queued 或 lease-expired Sending 且无 Teams message ID 的旧 warning；Fresh Sending/Accepted/Sent/unknown relation 保持不动；new warning 使用稳定 anchor generation/logic ID，已发送消息不回撤。
- [ ] 增加 migration idempotency/restart/lease/CAS/crash、branch/thread reuse、所有 resurrection path 和 warning status matrix 测试。

## 5. Live/history sender fairness 与用户体验

- [x] history-only/context-gap 新数据永不触发 live gate；runtime ownership unknown 仍 fail-closed；history quarantine 不伪装成普通 queued turn。`legacy_unknown` 的一次性用户 receipt/migration 仍是后续工作。
- [x] history flush 设 per-chat/per-cycle count+byte 预算（8 条、512 KiB，首条大消息例外），保留 live/control capacity，不因 history backlog monopolize chat lane；不引入无界全局 scheduler。
- [ ] 复查 queue drain、startup recovery、executor claim、callback、retry、outbox flush 的 lane fairness 和 sequence predecessor 依赖。
- [ ] 增加 live-vs-history starvation、large history backlog、unknown owner/deferred-not-queued、重复 inbound、stale callback 和 Graph accepted-then-lost no-replay 测试。
- [x] 已移除旧 80-record 全局 hard gate，并保留当前安全的每轮 pacing；本 checklist 要求继续避免恢复为全局 block。

## 6. 回归测试矩阵

- [x] s001 mixed-ID final mirror：正常 completed runtime 不被 history scanner 误报为 orphan；history-only path 不反复产生 warning。
- [x] s002/s508 类场景：history-only blocked/quarantined 时新 Teams request 可 live admit，不创建 execution owner，不进入错误 queue。
- [x] s512：PendingContinuation 不 livelock；safe prefix 与后续新 root 可达。
- [x] s514：真实 oversized image/base64 被隔离并推进；后续 final 不被吞；newline-less cap 仍由 bounded partial-read 逻辑处理。
- [x] s519：大 opaque tool record 消费后 checkpoint 推进，后续 status/final 可同步。
- [ ] source rewrite/truncate/same-inode rewrite、missing frontier、two gaps、exact proof、restart、concurrent scanner、crash commit。
- [x] JSON/SQLite parity 与受影响包 `go test -race`。
- [x] Graph 401/429/5xx no-replay：保持一次 non-idempotent POST、不自动重放；后续 reconciliation 继续只认 exact marker/provenance，legacy unmarked 仍 manual。

## 7. 性能与发布门槛

- [x] unchanged poll 保持零 durable writes；现有 no-op 回归和当前 benchmark 覆盖代表性 unchanged/增量路径。
- [x] 保持当前 512 KiB scan budget 和 8 visible pacing，并明确 history flush count/byte budget；没有通过放大 tail threshold 掩盖 framing/progress 问题。
- [ ] 记录 JSON/SQLite p95 lock time、write bytes、alloc、reopen latency；无解释的 >5% 热路径回退不得接受。
- [x] active pending range、scanner state 和 targeted outbox flush 均有固定上限；没有引入无界 history ledger 或 scheduler。
- [x] 运行 focused tests、`go test ./internal/teams/... -count=1`、`go test ./... -count=1`、affected race、`go vet ./...`、`git diff --check`。
- [x] 检查 CI 的实际 selector/平台 job；本地通过不替代 gated installer/release job。
- [x] 核心实现门槛通过后进入独立提交/prerelease 流程；发布动作不重启/更新 live helper，也不发送真实 Teams 消息。

## 8. 执行记录

- [x] 当前 partial patch 的 focused tests、`go test ./internal/teams/...`、全量测试和既有 race 检查曾通过；实施新状态模型后已重新运行。
- [x] 已有的 malformed/oversized/opaque progress、history-only no-live-gate、Graph no-replay 代码和测试保留为本轮基线。
- [x] 新增状态模型、生产路径的原子 observation CAS、fairness 和 JSON/SQLite parity 处理；完整自动 legacy migration 仍保守留在后续风险。
- [x] 新增测试矩阵并通过全部验证。
- [x] 完成性能审计与最终 diff review；已用同一基线提交和同一机器完成 benchmark 对照，但仍未测量 JSON/SQLite lock p95、write bytes 和 outbox fairness p95。
- [x] 完成 checklist 回填；未实现的项目已在下方明确列为后续风险。

### 8.1 本轮验证结果与保留风险

- [x] `go test ./... -count=1` 通过；`go test -race ./internal/teams ./internal/teams/store -count=1` 通过；`go vet ./...` 和 `git diff --check` 通过。
- [x] 完成同机同命令 benchmark 对照（各 `-count=3`；单次结果有正常抖动）。基线 `e657248`：`HistoryTieredStatHotSetUnchanged` 约 7.57–9.30 ms、1.44–1.52 MB/op、10,000 allocs/op；`HistoryTieredTailScanSmallDelta` 约 18.9–31.0 µs、11.2 KiB/op、123 allocs/op；`CXPPerfModelSQLiteLiveProgressGuardProfiles/large-history` 约 31.4–33.4 µs、3.7 KiB/op、43 allocs/op。当前：分别约 6.65–6.71 ms、1.52 MB/op、10,000 allocs/op；28.8–29.1 µs、45.9 KiB/op、155 allocs/op；24.4–24.8 µs、3.7 KiB/op、43 allocs/op。热路径时间与 SQLite 分支未见回退；tail scanner 的增量路径增加了 proof/state 开销，需结合真实 workload 再评估，不能只凭三次微基准宣称全局无回退。生产 scan writer 的 checkpoint refresh 已收敛为每轮至多一次，而非每条记录一次。
- [ ] 没有在本轮实现自动把所有 `legacy_unknown` interrupted Turn 迁移成 history-only；这是有意的 fail-closed 保守边界，后续必须先有逐条 source-proof/delivery-ledger inventory，再做带 generation/CAS 的幂等 migration。
- [ ] 没有把 local outbox intent 与 transcript observation 合并成一个跨 Graph 的事务；Graph 的 accepted-then-lost 仍通过现有 delivery ledger/reconciliation 处理，不能由本地事务伪造远端结果。
- [ ] 没有运行真实 Teams/live state 或 Docker Graph canary；本轮只做隔离的本地 JSON/SQLite/mock Graph 测试，未发送真实 Teams 消息。

## 明确不做

- [x] 不重启、reload、kill、替换或后台运行 Teams helper/service。
- [x] 不修改 live state、不发送真实 Teams 消息；真实流程只允许隔离 Docker/mock Graph canary。
- [x] 不用删除 checkpoint、清空数据库或无限提高 tail threshold 规避问题。
- [x] 不把 local lease expiry、旧 recovery reason、混合 ID mirror 或普通 history quarantine 当作远端 runtime terminal proof。
- [x] 不在没有 runner/workspace/tool 隔离证明时自动创建并发 branch。
