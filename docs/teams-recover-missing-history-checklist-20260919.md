# Teams missing-history recovery checklist

目标：在不回退任何已确认的 source frontier、不重发未知 Graph POST、不中断正常 Work chat 的前提下，让 rc50 之后错过的 Codex 顶层 JSONL 可被发现，并让当前明确遗漏的对话在下一次服务启动后进入既有安全的 history/import/outbox 流程。

安全边界：

- 不改动 `sending`/未知结果；当前 45 条保持原状。
- 不推进 cursor/seen，不伪造 Graph 结果，不清空正常 chat。
- 不回退或重置已有 `HistoryWatchCheckpoint`、`ImportCheckpoint`、poll frontier、owner/lease/attempt。
- 不在 SQLite 事务中调用 Graph；本地 repair 只写 history-watch 唤醒元数据。
- apply 前必须服务停止、做 SQLite 备份、完成 dry-run inventory，并在写后重新读取验证。

## A. 基线与现状证据

- [x] 在 rc50 `origin/main` 上建立隔离 worktree。
- [x] 确认当前 Teams 服务保持停止。
- [x] 记录当前 SQLite 表计数、45 条未知 `sending`、已有历史/导入 checkpoint 和三条已确认遗漏 JSONL。
- [x] 确认三条遗漏 JSONL 是顶层用户会话，不是 subagent，并记录 cwd/session id。
- [x] 对当前 live DB 完成只读差集 inventory：Codex 最近一个月顶层 JSONL、sessions、import checkpoints、history-watch。

## B. 代码修复：最近一个月遗漏 JSONL 的可达 discovery

- [x] 增加独立的月度 recovery/discovery 时间窗，保留 3 天 hot scan，不把旧于该窗口的历史误当成新会话。
- [x] reconcile 时不要 baseline 最近一个月内未登记的文件；只对更老且无 durable 记录的文件建立 EOF baseline。
- [x] 月度 discovery 使用有界 batch，并复用 owner-fenced、CAS、既有 history-watch scanner/publish/import/outbox 路径。
- [x] 月度 discovery 使用独立 durable fair cursor；游标只表示调度提示，不授权 checkpoint、frontier 或 Graph 操作。
- [x] cursor 在候选集合变化后仍能从 lexical successor 继续，避免删除已完成前缀后反复重试同一前缀导致尾部饥饿。
- [x] Teams backlog 下继续跳过普通 history/linked maintenance；仅在已有 fairness quantum 中允许月度 discovery，不能让 cold work 抢占 poll/写消息资源。
- [x] 保持 subagent、Teams-origin、source rewrite、unknown POST、owner/lease/CAS 的现有 fail-closed 语义。

## C. 代码测试

- [x] 最近一个月但超过 3 天的未登记顶层 JSONL 被 discovery 选中并发布；超过月度窗口的旧 JSONL 仍只 baseline，不发送。
- [x] 月度候选超过 batch 上限时，多个 reconcile/重启能够覆盖全部候选；首个候选失败不会永久阻塞尾部。
- [x] 并发 recovery worker 注册新 Codex session 时使用 single-flight registry publish；race 检测确认不会重复分配 session ID 或产生重复 registry session。
- [x] reconcile/baseline 失败后候选仍可达，成功后 checkpoint 与 outbox/import 状态可重启恢复。
- [x] backlog fairness 选择月度候选，且不执行普通 cold history work。
- [x] subagent JSONL 永不进入用户会话 discovery；Teams-origin 文本不重复发布。
- [x] stale owner/lease、CAS 冲突、context cancellation 不推进 durable cursor；未知 Graph 结果不产生自动重发。
- [x] JSON/SQLite history-watch projection parity、dry-run no-write、apply backup/verification 测试。
- [x] 运行受影响的 `internal/teams` 与 `internal/teams/store` 全包测试、定向 race、vet、Docker acceptance、`gofmt`/`git diff --check`；未把未受影响包的结果冒充为本轮全仓验收。

## D. 当前 live DB 的安全 repair

- [x] 在 live DB 写入前创建带时间戳的 SQLite/sidecar backup，并记录 hash/大小。
- [x] dry-run 只报告候选；只允许当前三条已核实顶层 JSONL 作为 apply 目标。
- [x] apply 仅为缺失路径插入 `Offset=0/Size=0` 的 history-watch 唤醒 checkpoint，不改 sessions/import/frontier/outbox/sending。
- [x] apply 后读取 projection 和 canonical view，确认三条路径存在、物理 cursor 仍为 0、45 条 sending 未变化、其他计数/关键字段未变化。
- [x] repair 拒绝未初始化的 history-watch store；未完成全量 baseline 时不接受人工子集唤醒。
- [x] 在服务仍停止的状态下完成 repair；不从本轮自动启动或重启 Teams helper。

## E. 最终验证与交付

- [x] 使用临时副本/fixture 验证三条 checkpoint 会进入既有 scanner/import/outbox 路径，且重复运行是 no-op。
- [x] 用真实 `DiscoverProjectsContext`、复制的真实 SQLite/JSONL 和本地 Graph recorder/Docker 进行无真实 Teams POST 的恢复实验，覆盖启动、月度 discovery、checkpoint/CAS 和单调推进；现有 Teams mock Graph 回归测试继续覆盖 429/unknown POST no-replay。
- [x] 复查完整 diff、测试输出、SQL 变更边界和剩余风险；本轮已通过受影响的 `internal/teams`/`internal/teams/store` 包测试、定向 `-race`、`go vet ./internal/teams/...`、Docker acceptance、`gofmt`/`git diff --check`；未把未受影响包的结果冒充为全仓验收。
- [x] 本轮未进入 prerelease，也未自动重启 live service；如需安装到本机，必须由后续明确的发布/升级操作完成。

## F. Same-inode append recovery livelock

- [x] 用超过 4 MiB 的 JSONL、durable blocked checkpoint 和同 inode append 重现基线：source rewrite recovery 每次从约 4 MiB 重新扫描，永远到不了 anchor/EOF。
- [x] 在 linked-transcript `ImportCheckpoint` 路径复现同样的 same-inode append cursor reset。
- [x] 修复仅允许“同一 source identity + pending scan + 当前 size 严格增长 + append-only change-time evidence”的 continuation；同尺寸 rewrite、truncate、replacement、marker-only retry 仍 fail closed 并清空不可信扫描进度。
- [x] 增加 SQLite durable migration、fresh `Bridge` restart 后继续推进的 HistoryWatch/linked-transcript 回归测试。
- [x] 本地验证：teams/store package、定向 race、vet、gofmt 和 `git diff --check` 通过。
- [x] bounded Docker 验证：account/global/intermittent 429、unknown POST no-replay、positive completion，以及两个 same-inode append 回归均通过。
- [x] 真实数据 Docker 双进程诊断通过：fixture 使用当前 SQLite/历史 JSONL，容器 `--network none`、无 Teams token、只读挂载；第一进程 measured inbound/completed 均为 95（约 0.792/s），第二进程新增 inbound 95、completed 88（约 0.792/0.733/s），无重复、无 owner/phase deadline；source drift 已明确标记，因此不是静态 point-in-time acceptance。
- [x] 确认 Docker fixture、容器和临时 runtime 已清理；live Teams service、live SQLite 和 live Codex JSONL 未被本轮测试写入。

## G. Real-data Docker coverage for every lagging chat and >3-day discovery

- [x] 修正 copied-fixture missing-history acceptance 的索引错误，并要求每个真实 >3 天 witness 同时具备：发现路径、唯一 registry session、可用 Teams chat ID、durable transcript import checkpoint。
- [x] 增加 opt-in `CXP_TEAMS_DOCKER_REAL_DATA_CHAT_COVERAGE=1`：从真实 SQLite 的全部 ordinary queued work-chat corpus 为每个 chat 保留一条真实 body/author/order representative message；普通 throughput corpus 不变。
- [x] coverage 使用 production listener、真实 durable poll/inbound/turn/outbox 路径和本地 fake Graph，要求 complete mode 关闭每个代表消息，检查每条消息都被 Graph 服务、durable inbound、turn 完成且无重复；不读取或挂载 Teams token。
- [x] real-data Docker wrapper 在同一个隔离 fixture 中依次运行 missing-history、all-lagging-chat completion、full backlog throughput/restart；每个实验使用独立 runtime，失败不提前停止，以便一次暴露多个问题。
- [x] 运行新的 Docker 实验并记录三项独立证据：`missing-history` 在真实复制数据上发现并注册 3 个 >3 天 witness，且每个都有唯一 registry session 与 durable import checkpoint；`chat-coverage` 从真实 7,457 条 queued Teams payload/357 个 chat 选出每 chat 一条 representative message，最终 357/357 inbound、357/357 completed、0 failed/queued/running、0 duplicate durable turn；`throughput/restart` 两个进程均通过，重启后旧边界没有遗留 running/queued，未知 POST 没有重发。两次真实数据 Docker wrapper 的容器测试本身均 PASS；外层返回非零仅因为 live rc50 helper 在复制/运行期间持续改变 SQLite WAL/source manifest，脚本按设计将其标为 diagnostic 而非 point-in-time acceptance，未将源漂移伪装成绿色验收。
- [x] 实验结束后确认 fixture、runtime、container/image 清理；live Teams service、live SQLite、live Codex JSONL 和真实 Teams Graph 均未被 Docker 写入。测试期间只读取 live source，所有 durable 写入都落在临时 fixture/runtime；当前 live helper 仍由环境自行运行，未被本轮停止、重启或替换。

## H. Docker outbound duplicate guard

- [x] fake Graph 按 durable outbox ID 统计每次 message POST，并在 real-data acceptance 中拒绝同一 outbox 跨越 Graph POST 边界超过一次；unknown POST resume 仍单独保留“不得重发”断言。
- [x] 增加 duplicate-post audit 单测；`gofmt`、`git diff --check`、受影响定向测试和定向 `-race` 通过。

## I. Exhaustive lagging-chat proof

- [x] 纠正证明口径：357 不是当前 live 数据的固定总数；以 immutable fixture 中 `teams + queued` 的 actionable inbound 建立 chat manifest，并把 queued 但已有 terminal turn 的旧 provenance 单独统计，不能当作可执行 backlog。
- [x] 当前只读盘点确认：排除 control 后共有 360 个 queued chat ID；其中 359 个仍有 active session，另 1 个是已完成 turn 的旧 chat provenance，不应再次执行；359 个 active session 中有 2 个只有空 terminal provenance，因此 ordinary replayable backlog 是 357 个 chat。
- [x] 增加严格 manifest audit：没有 active session 的非 terminal queued chat 会使 exhaustive acceptance 立即失败，而不会被 replay corpus 静默排除。
- [x] 增加逐 chat/逐消息 drain audit：每个 manifest chat 的每条 replay message 必须分别满足 Graph served、唯一 durable inbound、唯一 terminal completed turn；全局总数不能互相抵消。
- [x] 用当前 immutable fixture 运行 representative-per-chat complete acceptance：源快照识别 360 个 queued chat ID、359 个 active-session chat、1 个 terminal-only orphan；其中 2 个 active chat 只有空 terminal provenance，排除后 357 个 ordinary actionable chat 全部满足 `Graph served=357`、`durable inbound=357`、`completed turn=357`，逐 chat audit `mismatches=[]`，且 `failed/queued/running/interrupted=0`。这证明每一个可执行 lagging chat 都至少有一条 backlog 消息能够闭环；不是把多个 chat 的总数相互抵消。
- [ ] 用同一 fixture 运行 full-corpus complete acceptance，确认全部 backlog message、所有 chat、分页/重启/429/unknown-POST 断言全部闭合。
- [x] 对 terminal queued provenance 做独立 integrity 报告：360 个 queued chat provenance 中 1 个是无 active session 的 terminal-only orphan，另有 2 个 active chat 只有空 terminal provenance；这些记录没有被重放、删除或伪造成 actionable backlog，实验仅对 357 个 ordinary actionable chat 做代表消息闭环验证。

本次严格 Docker coverage 结果（使用 live SQLite/历史 JSONL 的隔离副本、`--network none` 和 fake Graph；没有 Teams token，也没有真实 Graph POST）还观察到：20 分钟 measured window 内 `measured_completed=351`，窗口内完成速率约 `0.292 msg/s`，窗口结束后的短 drain 完成剩余 6 条，总计 `357/357`。Graph 注入了 `429=1`、`503=4`，但最终没有 unknown POST、重复 durable turn、owner change、poll/history/linked deadline 或 phase error。普通 backlog 存在时 history/linked maintenance 只发生 suppression 记录，没有 unsuppressed cold work。

该结果是“每个可执行 chat 的代表消息”证明，不是 7,457 条原始 queued payload 的 full-corpus 证明；full-corpus 项仍保持未勾选。复制期间 live helper 继续写入 source SQLite/WAL，因此脚本把本次结果标注为 diagnostic，而不是静态 point-in-time acceptance；Docker 的 durable 写入仍全部落在临时 fixture/runtime。
