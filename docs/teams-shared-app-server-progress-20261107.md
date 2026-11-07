# Teams 与原生 Codex TUI 共享会话

## 范围与架构

实现位于独立工作区的 `feature/shared-codex-input` 分支，基于 main `ad05790`。原开发目录的修改和已安装 helper 保持不动。

沿用现有 conversation、app-server pool、SQLite 队列、增量 transcript mirror 和更新 drain。每个已打开的 Work conversation 使用 pool 中的一个原生 app-server；不另建常驻消息转发进程，也不把全部会话集中进一个后端。

Teams 直接连接原生 app-server 的 authenticated loopback WebSocket。`cxp tui` 恢复相同会话时，通过本机 helper 的小型 HTTP attach 接口取得该后端的地址、capability token 和生命周期租约，然后也直接连接原生后端。HTTP 接口只做 attach，不转发 RPC 或 token 流。TUI 仍是原生 Codex TUI。

服务端负责原子输入路由和通知广播。CXP 不复制 Codex 的任务状态机，只区分普通输入、显式队列、输入结果是否确定，以及本程序是否拥有该任务的输出。

## 用户行为

| 输入或操作 | 行为 |
| --- | --- |
| Teams 普通消息，当前空闲 | 原生 start-or-steer；通常新建任务 |
| Teams 普通消息，当前有任务 | 指定 expectedTurnId 的 steer，立即追加到现有任务 |
| `helper queue <prompt>` | 保存在 CXP 队列中，独立执行，绝不转换成 steer |
| TUI Enter / Tab | 保留原生追加 / 排队行为 |
| helper 已接受 queue，但 TUI 先启动任务 | helper 输入在原生队列等待，不追加到 TUI 任务 |
| 正在生成 final_answer 时收到普通消息 | 以原生任务是否仍活动为准，不根据文字或界面推测阶段 |
| steer 时目标任务恰好结束 | 仅在明确拒绝时恢复为待启动输入；未知结果不重发 |
| 多条普通消息 | 每个 conversation 串行提交 steer；附件未就绪的早期普通输入阻挡后续普通输入 |
| 显式 queue 前面还有普通消息 | 普通输入仍可追加到活动任务；不被显式 queue 阻挡 |
| 取消尚未启动的 helper queue | 删除本条原生 queue，不中断另一个 TUI 任务 |
| 取消已经启动的 helper 请求 | 使用确切 native turn ID 中断 |
| 关闭 TUI | 释放租约；helper 和当前任务继续运行 |
| 追加请求要求返回文件 | 沿用当前 helper 任务附件目录；TUI 发起的任务使用稳定的 native turn 附件目录 |

`helper queue` 后面的文本是 prompt，不再次按 helper 命令解析；附件、语音和群组权限沿用既有流程。缺少 prompt 时返回用法提示。

追加消息使用专门的 Teams safety envelope：继续当前任务。它不会把当前任务标成已经完成，也不会把状态询问误写成取消任务的指令。输出只由现有 execution observer 或增量 mirror 中的一方发布，避免每次追加产生重复最终答案。

## 队列只有一个准入点

CXP SQLite 保留持久化等待队列、文件就绪、暂停、并发上限和出站依赖。仅在现有 runner 准入后，才将队首单条输入交给原生 queue。原生 queue 用来处理与 TUI 的原子启动竞争；add 成功不等于任务已开始。

不使用“检查 idle，再 turn/start”的两步做法，因为原生 turn/start 在竞争中可能变成 steer。也不把全部 CXP backlog 提前放入原生队列。

边界：

- 已交给原生 queue 的一条输入属于当前已准入请求；之后暂停 helper 不能撤回它，需要取消该请求。
- 保留各自已准入队列的顺序，不承诺 Teams 未准入 backlog 与 TUI Tab 之间的全局 FIFO。
- 原生 queue 使用执行时的线程设置，不保存每条输入的 model/effort 快照。helper 在交接前应用请求设置；随后用户在同一 TUI 修改设置可能影响该输入。这与原生 Tab 一致，真实协议测试覆盖此行为。

## 生命周期、升级与失败

- 没有可用的本机兼容 helper 时，`cxp tui` 沿用独立启动流程。共享路径不再启动无用的第二个 proxy。
- 已由独立 Codex 窗口打开的会话不会被强行抢占、杀进程或热迁移；退出旧窗口后，通过 `cxp tui` 重新打开才能加入共享后端。
- 共享入口覆盖 CXP 会话选择器的恢复和新建，以及 fork 创建完成后的子会话。直接启动原生 Codex、原生 picker 保留独立路径。
- 本地历史会话可以在 Teams Work chat 创建前 attach；只绑定会话并建立当前文件游标，不重播旧历史。后续新输入由原有 mirror 创建 Work chat。
- 计划更新等待 TUI 租约和原生任务结束。等待窗口关闭期间仍接受正常工作；最后一个租约释放后，再原子关闭 attach 准入并进入 drain。
- TUI 窗口存在时由 TUI 处理审批；最后一个窗口关闭时，未解决的请求交回 helper，已由 TUI 解决的请求不会重复处理。
- 正常退出会关闭后端。Linux 共享后端设置 parent-death signal；Windows 使用 kill-on-close Job Object。helper 意外终止会中断后端连接；这不是无缝热升级。
- 其他平台保留按 host 记录 PID 并在同机下次启动时清理的机制。macOS 强制崩溃后即时清理没有运行验证，不承诺跨机器接管能清理旧机器的孤立进程。
- 断开连接不等于任务没有收到输入。未知提交结果按 clientUserMessageId 查询；无法证明时暂停该 conversation，不盲目重发、不因为网络不确定性杀掉仍在用的 TUI。
- 重启恢复先移除本 helper 尚未执行的 native queue，再核对历史。输入已发生则只恢复结果，不重新运行；input-only steer 记录不接管整个原生任务的最终答案。
- SQLite v1 升到 v2 增加常量默认列；旧未完成请求默认仍为 queue。没有重写历史 JSON、复制大型数据库或重新扫描全部历史记录的迁移。

## 资源开销

- 不增加第二个 native backend，不中转 TUI token 流；多开窗口仍会产生原生协议自身的连接、序列化和 UI 开销，不能声称零开销。
- attach 使用一个连接和少量租约状态；原生 token 只保留在内存中；本机 attach 入口使用另一枚 token，保存在受保护的 endpoint 描述文件中。后端启动用的 token 文件在初始化成功后立即删除。
- 无 attach 心跳写盘；没有每条 token 写 SQLite。数据库仅持久化输入提交、确定结果等状态转换。
- 等待 native queue 时每 5 秒检查 queue 元数据；只有 queue 已不含本条输入且错过 dispatch 通知时，才查询最近 5 个 turn。正常路径由通知直接唤醒，不增加消息调度轮询延迟。
- 恢复未绑定的本地会话时，仅在 attach 时按文件名查找一次，再读取目标 session_meta；不解码旧对话内容。现有 mirror 继续增量读取。
- 查询 NextSteer 使用现有 open-turn 索引；没有可追加输入时只读，不制造重复状态写入。

本地性能证据：814 MB SQLite fixture，5,800 conversations、91,000 turns、1,180,000 inbound、450,000 outbox、80,000 sources。包含 NextSteer 的 hot-cycle 中位数为空库 0.909 ms、填充库 0.545 ms；p90 分别 1.578 ms、0.601 ms。该结果证明此 fixture 未出现随历史体积增大的明显退化，不代表生产吞吐量或零 RSS 增长。27.8 MB 旧库迁移耗时约 59 ms，文件增加 4 KB；50 次无变化 NextSteer 事务均无行写入。

## 测试分层和复现

1. package 测试：解析与去重、附件阻塞、steer 并发、完成边界、拒绝/未知结果、取消、恢复、mirror 去重与附件、更新租约、旧库迁移和只读路径。
2. 黑盒 E2E：隔离 Docker 中运行 source-built helper、fake Graph 和 fake Codex；覆盖双向共享、单后端 PID、TUI 先于 Work chat、queue 不变成 steer、关闭 TUI 不停止任务、两个新窗口互不合并、崩溃恢复及既有场景。
3. 真实原生协议：Codex 0.166.0、隔离 CODEX_HOME、离线 Responses fixture；两个客户端连接同一后端，验证输入真正进入模型请求、final_answer 边界、并发空闲提交、steer ID 防护、queue 自动执行/取消/恢复和设置语义。生产 Go transport/queue observer 也直接调用真实原生后端。每组运行三轮。
4. race、vet、跨平台 build/vet，以及 SQLite 大体积 fixture 审计。GitHub CI 新增必需的 shared-codex-contract job，固定 native 版本并保存协议测试日志。

```sh
bash scripts/ci/run_shared_codex_contracts.sh /absolute/path/to/native/codex
```

本地全量测试须在无外网 Docker 内运行，使用独立可写源码、测试用户及其 HOME、只读 Go module cache 和专用 build cache。不能以 root 执行权限测试，也不能让 helper 生命周期测试访问宿主机状态。

尚未以真实 Teams 账户、真实模型或真实交互终端执行端到端验收；fake Codex 不模拟模型对追加文字的理解，真实协议测试弥补的是输入路由而非推理质量。Windows/macOS 只有编译和 vet 覆盖。其他 native Codex 版本尚未验证；不要把 0.166.0 的协议结论推广为所有版本兼容。

## 本地验收记录（2026-11-08）

- 独立、离线、非 root Docker：`go test -p 4 ./... -count=1 -timeout=600s` 全部通过；关闭 golden 更新后验证，包含三个新增共享 E2E。
- `go test -race ./internal/core/... ./internal/channel/...` 通过；CLI、新建/恢复/fork 入口及审批交接另跑 race；最后的 steer envelope 变更也通过针对性 race 测试。
- 原生 Codex 0.166.0 的协议与生产客户端测试各三轮通过，包含共享关闭不等待 stdin EOF 的回归。
- Linux 全仓库 vet，以及 Windows amd64、macOS amd64/arm64、Linux arm64 的生产代码 build 和非 E2E vet 通过。Unix 信号驱动的黑盒 E2E 只在 Linux 运行。
- Python 脚本测试 127 项通过，其中 9 项因相应环境条件跳过；shell 语法、CI YAML 和新增必需 job 依赖检查通过。
- 大历史库、旧库迁移和无变化读取写入量检查通过；工作区 diff 格式检查通过。

验证期间修正的失败包括：测试用户/HOME 和源码挂载权限导致的环境失败；旧 E2E 对普通消息仍按 queue 的假设；被杀 helper 留下共享后端；控制通知与后续用户消息之间不稳定的测试顺序。最后一轮全量测试没有已知失败。

以上是本地测试结果。GitHub CI 未触发，未发布或安装此修改，当前运行的 Teams helper 保持原样。
