# Teams hot admission 安全优化 checklist

目标：在不改变 Teams durable 语义的前提下，降低 `chat_polls`/`sessions` hot admission 的扫描、排序、JSON 解码和锁持有开销，并用同一代表性 fixture 证明端到端收益。

## 已完成的审查和基线

- [x] 使用多个无上下文 subagent 审查当前实现、历史提交、测试和 CI。
- [x] 对最近 profiling 结果做交叉核对，确认实施前 2,100-chat fixture 的 legacy admission 约 6.4s，并把端到端收益与 admission 子路径收益分开验证。
- [x] 建立 focused baseline：`internal/teams/store` 和 `internal/teams` 受影响回归测试通过。
- [x] 确认当前工作树有大量既有未提交改动；本计划只做增量修改，不覆盖用户改动。

## 本轮落地和验证结论（2026-09-11）

- [x] 落地 row-local projection trust/revision、版本化 trigger、bounded keyset backfill、trusted scalar admission、独立 JSON recovery lane、selected-only hydration 和 schedule/candidate 合并读取。
- [x] 保留 JSON canonical authority、最终 Go 校验和原有 durable owner/lease/attempt/frontier/CAS；本轮没有扩大 flush/poll quantum，也没有把 Graph 请求放进 SQLite transaction。
- [x] 同一 2,100-chat fixture 的 3 次复测：work-candidate scalar 为 12.85–13.74ms，legacy JSON 为 1.992–2.024s；ready-chat scalar 为 8.45–8.68ms，legacy JSON 为 1.957–1.975s。该结果证明 admission 子路径有显著收益，不等同于端到端吞吐倍数。
- [x] `go test ./... -count=1`、新增回归测试的 focused race、`go vet ./...` 和 `git diff --check` 通过；新增回归测试已加入精确 CI selector。
- [ ] store 全量 `-race` 在 10 分钟上限内未完成，调用栈停在大状态迁移的 SQLite trigger/turn 写入，没有 race 报告；需作为独立测试耗时问题处理，不能伪报为通过。
- [x] Docker 真实数据诊断使用宿主快照、无 token、`--network none`、临时 runtime volume；60 秒窗口完成 27 条 measured durable completion（0.450/s），观测到 1 次 429、4 次 503、1 次 unknown POST 且无重复 unknown POST。
- [ ] Docker point-in-time acceptance 尚未通过：运行期间宿主 helper 持续修改输入，安全快照检查拒绝报告 acceptance；允许 drift 的 60 秒诊断样本只完成 33 条、低于默认 100 条门槛，5 分钟诊断虽完成 183 条但发现混合时刻快照使一个 history offset 回退 4,107 字节。两者都是实验/环境门槛，不是把它们伪报成绿色验收。

## 不可突破的安全边界

- [x] JSON canonical row 仍是最终 authority；不可信 projection 不能静默丢掉 due chat。
- [x] 不批量合并 inbound claim/complete，不提前推进 cursor/seen。
- [x] 不自动重发未知 Graph POST 结果。
- [x] 保留 owner、lease、attempt、frontier 和 execution CAS。
- [x] 一个 chat/session 路径不创建两个并发 durable frontier。
- [x] Graph 请求不进入 SQLite transaction；ACK、marker、final 不粗暴合并。
- [x] 不以扩大 flush quantum、跳过 predecessor 或删除 recovery read 掩盖查询开销。

## 实施阶段

### A. 观测和可重复基线

- [ ] 为 hot ready/work admission 记录完整生产级 lane、query、returned、decoded、fallback、repair、lock wait/hold 和 cold-state load 指标；当前已有 listener/SQLite/poll trace，但还没有完整稳定的 production counter schema。
- [x] 固定同一 2,100-chat 脱敏 fixture，报告 scalar/legacy、分配量和 3 次重复结果；cold/warm、logical read、WAL 写放大和 fixture digest 仍待补齐。
- [x] 增加健康/坏行/partial-index `EXPLAIN QUERY PLAN` 回归断言；保存可比较的跨环境计划 artifact 仍待补齐。

### B. Row-local projection trust/revision

- [x] 为 `sessions` 和 `chat_polls` 增加版本化的 canonical revision、projection revision 和 trust 状态；旧 schema、缺列、缺 trigger、混合版本默认 untrusted。
- [x] 明确 revision 与业务 `PollRevision`/`ScheduleRevision` 分离；已知 writer 在一个 durable transaction 内同步 JSON、scalar、revision/trust。
- [x] 用版本化 trigger/writer fence 让旧/raw JSON 或 scalar writer 只能使 projection 失信，不能伪造 trusted；不依赖 `CREATE TRIGGER IF NOT EXISTS` 覆盖旧 trigger。
- [x] 将 session/chat-poll backfill 改为稳定 PK high-water mark、raw JSON/revision CAS、事务内 cursor/完成 marker；冲突可重试，崩溃可继续。
- [x] 禁止 hot path 使用全表 delete/reinsert 作为 projection 更新；保留 opaque/corrupt row 证据。

### C. Trusted scalar admission

- [x] 健康 ready/work lane 只使用可信 scalar 列和匹配 lane/order 的 covering index。
- [x] 健康 SQL 不调用 JSON1、不读取每行 JSON；只返回最终候选所需的 ID、revision、binding/sort key。
- [x] untrusted/malformed/stale row 进入独立有界 repair lane；repair 不占健康 quota，不因 invalid page 静默丢 due row，并具有 bounded wake/fairness 语义。
- [x] combined admission 使用现有 state/file lock 作为等价一致性边界；任何 Graph 请求前读路径已结束，repair 写入使用独立 durable transaction。

### D. Selected hydration 和 claim 安全

- [x] selected refresh 同时 hydration canonical session binding/status、chat route、poll revision；不能保留可能过期的 session map。
- [x] control close/rebind/status mutation、owner takeover 或 revision 变化会丢弃 snapshot 并重新 admission。
- [x] Graph/claim 前再次校验 owner、lease、attempt、frontier、binding 和 revision；不改变现有 frontier/receipt 规则。
- [x] 保持当前一 chat 一 durable frontier 约束；共享 chat 冲突必须确定性拒绝或确定性路由，不引入未定义的多 session 路由。

### E. Outbox 仅做已证明安全的局部修复

- [x] 修复并测试 missing-scalar fallback 的 `AfterChatID` distinct-chat keyset 语义，并用 canonical oracle 对比。
- [x] 保留 canonical page、同 turn FIFO/FIFO-with-bypass、ambiguous predecessor、owner/CAS 和 unknown POST 不重发。
- [x] 本次不扩大主循环 flush quantum，不把 outbox microbenchmark 倍数外推到 poll admission。

### F. 测试、CI 和 Docker 验收

- [x] canonical oracle parity：正常、坏前缀、64+/500 chat、并列排序、stale/missing/partial projection、混合版本均有回归覆盖；持续新到达仍需更长 soak。
- [x] migration/reopen、bounded backfill、WAL/旧 trigger、backfill 与 live writer 冲突、旧/raw writer 失信已有测试覆盖；逐个 255/256/257 的长 crash 矩阵仍待扩展。
- [x] selected control/session mutation、shared chat、two-owner/CAS barrier、restart/resume、无 orphan attempt 有测试覆盖。
- [x] 429/503/Retry-After、pending replay、accepted-disconnect、unknown POST、queue-only mutation=0 有测试覆盖；账户级 429 Docker 诊断已通过（2 个真实快照 chat、8 次 429、后续成功恢复、2 条 durable completion）。
- [x] 新增核心测试加入精确 CI selector；本机 normal/race/适用包已实际执行。
- [ ] Docker 已使用真实数据形状、完整 listener、隔离 fixture 和 fault replay；但 point-in-time acceptance 仍被宿主 source drift 拒绝，故不勾选验收完成。
- [ ] 最终核对全部 fixture chat 的 durable 状态，零静默漏发、零重复、零越权 claim。

### G. 性能验收

- [ ] 固定同一 fixture 做至少 20 次 cold、100 次 warm 或足够长 steady window，报告 median/p95；当前为 3 次重复微基准和 60 秒真实数据窗口。
- [ ] healthy admission p95 至少比基线降低 30%，durable completion throughput 至少提升 20%；当前 admission 的 3 次样本远超 30%，但尚未形成严格 p95/端到端对照门槛。
- [ ] alloc、logical read、WAL/写放大、lock wait/hold 不出现有意义回退；当前已报告 alloc 和 listener lock/phase trace，logical read/WAL 仍待补齐。
- [x] 性能结果以 durable completed/s 计算，不以 executor starts/s 代替。
- [ ] `go test`、`go test -race`、适用 Docker 测试、`go vet` 和 `git diff --check` 全部通过后才结束本计划；本机项已通过，Docker acceptance 尚未通过。

## 明确延期

- [x] 释放全局 state/file lock 做锁外 Graph/handler 并行（明确延期）。
- [x] 增加 work-chat worker 或直接提高每轮 poll/flush quantum（明确延期）。
- [x] 删除 JSON fallback、把 projection 当无条件 authority（明确延期）。
- [x] 跳过 history/linked-transcript 的 mandatory recovery（明确延期）。
