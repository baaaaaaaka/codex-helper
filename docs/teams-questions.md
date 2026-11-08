# Teams 中的 Codex 问题

在 `feature/shared-codex-input` 已有共享 app-server、steer 和显式 queue 的基础上，Teams 支持两类原生问题：`item/tool/requestUserInput` RPC，以及 `delivery: async` 且包含 `questions` 的 agent message。保留原生后端和已有 runner、发送器、通知器，不另建任务调度器。

## 用户交互

每个问题单独一条消息，使用数据库分配的稳定 `Q<number>`。引导文字使用英文，问题与选项保留 agent 的原文。用空行分隔状态、问题、选项、操作。

```text
❓ Q42 · Question from Codex

▶️ Reply when ready. Codex does not wait for your answer.

选择哪个测试环境？

A. 测试环境

B. 生产环境

↩️ Answer: Quote this message and reply with A, B, or your own answer.
Or use `helper answer Q42 <answer>`.

⏭️ Skip question: `helper answer Q42 --skip`
```

同步问题的状态为 `Codex is waiting for your answer.`。没有选项时只提示文字回复。回答后原消息保留问题本身，并显示已记录、已提交、跳过、失效或提交结果未知。长答案只显示简短预览，原始答案完整提交。

- 引用该问题并回复，或使用 `helper answer Q42 <answer>`，才算回答。字母 A/B 对应选项原文，也可自由输入。
- `helper answer Q42 --skip` 跳过。引用回复的字面 `--skip` 仍是文字答案；显式命令可用 `helper answer Q42 -- --skip` 发送这个字面值。
- 普通未引用消息保留普通追加/新任务行为；显式 `helper queue <prompt>` 继续排队，即使带有问题引用。
- 不猜测自然语言意图，不把转发当作回答。一次引用多个不同问题会要求用户明确指定。
- 答案支持文字。附件会被明确拒绝。敏感问题只显示安全提示和跳过操作，不引导用户在 Teams 发送秘密。
- 问题消息不附加取消任务、新指令或排队操作的提示。

## 同步问题

保留原始 RPC。多问题的答案在 SQLite 收集，全部回答/跳过后，向同一 RPC 提交一次 `answers` 映射；跳过的 ID 省略。提交前可修改已记录答案。回答在独立且按顺序处理的命令通道执行，不消耗任务并发名额，不被慢命令堵住。

`isBlocking` 缺失时按 true。false 时，完全没有用户参与的请求在约两分钟后提交空答案，不自动选择推荐项；第一次回答或跳过后取消 Teams 的超时。若同时打开原生 TUI，TUI 仍可能按自己的规则解决该 RPC，Teams 不能替 TUI 延长时间。

暂停或升级 drain 不妨碍向仍存活的 RPC 回答。原生 `serverRequest/resolved` 关闭 Teams 问题。写入失败属于结果未知，不能自动重放。服务重启后没有存活后端的 RPC 失效；已经进入提交状态但不能确认结果的请求标记为未知。

## 异步问题

收到完成的 async item 就发布问题，不等最终答案。它不进入最终答案或进度候选，transcript 的 `task_complete` 回退也不会把问题当作最终答案。

文字答案使用原生 `send_user_message_question_reply` envelope，包含 `questionItemId`、问题和答案，不套普通 Teams prompt wrapper。ID 为原生规定的 JSON 字符串 `["request_user_input_async", itemId, questionIndex]`。

答案在记录的同一事务中进入已有持久化输入队列。原生任务仍在运行时 steer；空闲时在同一会话启动后续任务。目标任务恰好结束，沿用已有“明确拒绝才转新任务、未知结果不重发”的流程。暂停/drain 时等待正常准入。控制聊天的异步答案也支持此路径，同时保留普通控制 Q&A 的历史 envelope。

原生 user message 中确认的 question ID 会更新问题状态。正常完成后允许七天内回答；失败或取消使相关问题失效。发送前再次校验会话与失效状态：已入队但尚未执行的答案，在问题失效或会话换绑后不会执行，也不会进入下一条正在运行的任务。Skip 只在本地关闭问题，不发送新输入；原生没有跨端 skip API，因此 TUI 可能仍显示该异步问题。

原生 RPC 由后端裁定第一份回答。异步回答本质是普通输入：两个客户端同时发出不同答案时，不承诺跨客户端的 exactly-once 或全局互斥。

## 通知与恢复

每次原生问题调用/问题组产生一次通知；全部问题消息发送后，使用已有 Workflow 事件通知，失败或不可用时使用控制聊天 @mention。消息丢弃时发送 delivery-unconfirmed 提示，不永久等待。发送前发现问题已解决时抑制通知；网络发送与用户回答之间仍存在无法原子化的极短窗口。

通知不触发最终答案重命名流程，不在 token 流中逐字通知。重复原生事件通过 group key 和 outbox key 去重。先记录后崩溃但尚未发布的问题由维护流程补齐；已存在的消息不重复渲染。临时状态编辑沿用现有编辑队列，丢失编辑不影响真实答案状态或造成重发。

通知能否成为系统推送仍受 Teams、Workflow 和用户通知设置影响；本地 E2E 验证请求与 @mention，不验证手机/桌面系统通知。

## 存储与性能

SQLite v3 新增三张 typed table：question groups、questions、options。历史表不重建。原生单次问题 payload 限制 128 KiB、最多 16 个问题、每题 32 个选项、每个 conversation 最多 64 个未解决问题组。正文渲染复用有界 sender worker，不阻塞原生 app-server 读取；渲染结束后再次检查问题状态，避免发布已经失效的操作引导。

普通 prompt 对 question inbound ID 的查询、活动问题查询及终态清理均使用索引。原生事件只加载对应会话的问题；等待状态刷新只读取会话 ID，不解码问题正文和选项。问题正文仅首次发布或缺失恢复时渲染，不按 token 写盘，不扫描完整 rollout。状态变化才更新问题行；终态保留 30 天。仍关联未执行输入的记录暂不清理，避免长时间暂停后丢失原生回复的来源标记。已失效后端的 RPC 引用在维护时释放。

性能回归测试使用 10,000 组历史问题检查查询计划、无变化读取零行写入、终态不可被过期维护快照覆盖。另提供 100/10,000 组历史的普通输入查询 benchmark。

## 测试与 CI

新增 38 个普通测试、7 个黑盒 E2E 场景和 2 个原生 Codex 合同测试；每个测试内另有状态和输入组合的子用例。

普通测试覆盖格式、原文与操作引导、命令与引用分类、多问题收集及修改、skip/秘密/附件、执行并发满额、暂停/drain、同步超时与部分回答、同步提交未知结果、重启孤立 RPC、异步完成/中断/另一端回答、换绑、通知成功与降级、提前回答和重复事件、存储索引/保留/容量、异步 final 去重。另测问题失效后已排队的答案不得执行，以及慢渲染不阻塞原生事件或其他聊天。

黑盒 E2E 在技能提供的临时 Docker 中运行真实 source-built CXP、fake Graph 和 fake Codex，验证同步引用/多问题/通知、异步 steer/queue/完成后回答、原生 TUI 先回答、取消、重启和普通消息/queue/skip 不串义。

离线原生 Codex 合同测试使用本地 Responses fixture，核对真实 RPC 的两端广播、另一端回复与 resolved 事件，以及带 async 能力的测试模型产生的真实异步问题和后续答案。CI 已有 `TestRealCodexShared*` 选择器自动覆盖这些新增测试；普通测试由 unit-linux/core race job 覆盖，黑盒场景由现有 E2E 包分片自动覆盖。按完整 turns 包实测约 122 秒更新了分片权重，三个分片的估计权重为 351/361/359。

原生事件适配另测缺失线程/消息 ID、空问题、超量问题/选项、超大 payload，以及重复或无关的 resolved 事件，防止错误关联其他问题。

本机性能样本：27.8 MB 历史库从 v1 升级到 v3 用时约 8 ms，文件增长 44 KiB；普通输入来源查询在 100/10,000 组历史下约为 15/16 µs，每次分配约 1.9/1.8 KiB。该结果用于检查历史规模增长带来的退化，不代表端到端消息延迟或跨机器性能保证。

本次修改不更新、替换或重启已安装的 Teams helper。是否发布和安装需另行执行发布流程。

## 本轮验证记录（2026-11-08）

- 隔离 Docker、Go 1.25.11：`go test -p 4 ./... -count=1 -timeout=600s -parallel=4` 全仓通过。
- Go 1.26.3：`go test -race ./internal/core/... ./internal/channel/... -count=1 -timeout=600s -parallel=8` 全部通过，包括 goroutine 泄漏检查。
- 原生 Codex 0.166.0、本地 Responses fixture：两个包的 `TestRealCodex(Shared|NativeQueue)` 连续三轮通过，无真实模型推理。
- 七个新增黑盒问题场景：全仓一轮及独立重复两轮全部通过；完整 turns 包用时约 120 秒。进程清理、重复消息、CPU 和速率约束沿用默认检查。
- `go vet ./...`、Teams 用户目录隔离检查及 `git diff --check` 通过。
- `CGO_ENABLED=0` 的 Windows amd64、macOS amd64/arm64、Linux arm64 编译及 core/channel vet 通过。
- 一万组历史问题的索引、无重写、活动会话范围、保留及迁移测试通过，性能样本见上文。

容器内使用可执行的 `/var/tmp` 临时目录和受限的 Go 并行度，避免容器的 noexec 与 PID 配额影响测试。运行中的宿主机 Teams helper 未参与测试。
