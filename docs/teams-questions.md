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

## 当前问题入口

会话最新的可编辑消息承载顶部 `❓Pending agent question` 标记和末尾 pending questions 区域。区域列出本会话所有尚未回答的问题，以 Q 编号、简短标题和原问题消息链接展示；回答、跳过、失效后移除。已记录答案等待其他题回答或正在提交，不再计入待回答数量。

确认、命令回复、错误、文件、实时镜像和历史合并消息都可承载入口。接收回执附加到用户消息时，目标是该用户消息，但它仍保持用户消息身份。问题状态替换正文后会恢复入口；语音转写、回执和冻结提示保留入口并置于其前面。回读时剥离这个 helper 区域，避免进入用户 prompt 或改变正文去重哈希。

问题尚未发布时显示发送中；私密问题不重复标题。长列表先缩短标题，再使用紧凑编号链接；仍然超限时通过现有 outbox 发送完整分片目录，末尾链接到各页。问题组尚在发布期间不反复生成不完整目录。

只维护最新目标及一次迁移中的旧目标，不持续更新历史消息。新目标编辑成功后清理旧目标；重启可继续未完成迁移。其他成员的消息不编辑，跨会话通知保留原有来源语义，不汇总所有工作会话的问题。

## 存储与性能

SQLite v3 新增三张 typed table：question groups、questions、options。历史表不重建。原生单次问题 payload 限制 128 KiB、最多 16 个问题、每题 32 个选项、每个 conversation 最多 64 个未解决问题组。正文渲染复用有界 sender worker，不阻塞原生 app-server 读取；渲染结束后再次检查问题状态，避免发布已经失效的操作引导。

普通 prompt 对 question inbound ID 的查询、活动问题查询及终态清理均使用索引。原生事件只加载对应会话的问题；等待状态刷新只读取会话 ID，不解码问题正文和选项。问题正文仅首次发布或缺失恢复时渲染，不按 token 写盘，不扫描完整 rollout。状态变化才更新问题行；终态保留 30 天。仍关联未执行输入的记录暂不清理，避免长时间暂停后丢失原生回复的来源标记。已失效后端的 RPC 引用在维护时释放。

性能回归测试使用 10,000 组历史问题检查查询计划、无变化读取零行写入、终态不可被过期维护快照覆盖。另提供 100/10,000 组历史的普通输入查询 benchmark。

## 测试与 CI

新增 39 个普通测试、7 个黑盒 E2E 场景和 2 个原生 Codex 合同测试；每个测试内另有状态和输入组合的子用例。

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

发布前的 GitHub race 检查发现一个测试同步问题：断言状态编辑的用例必须先等待问题消息送达。已修复该时序，并新增阻塞渲染的用例，明确覆盖发布前提交结果未知时不重发、不发布过时操作。相关用例带 race 连续十轮通过，全量 core/channel race 再次通过；生产代码未因此改变。


## 本轮验证记录（2026-11-18）

源代码：`fix/teams-pending-question-links` 工作区，基于上游 `90e8642b` 的未提交变更。只修改源代码和测试，未更新、重启或替换宿主机 helper。

行为边界：仅装饰最新可编辑目标；问题原文、回答协议、附件、mention、转写和接收回执保持原有职责。普通命令回复实际使用 `OutControl`，因此资格判断覆盖该路径，内部 inbox 仍按会话排除。旧测试中排除普通通知和用户镜像的断言已改为最新可见消息规则，并由命令回复和用户回执黑盒用例验证。

验证入口：

- Go 1.26.3/Linux：`go test ./internal/channel/... ./internal/core/store ./internal/core/engine -count=1 -timeout=300s` 分批通过；首次 store 测试编译错误在测试代码中修正后完整 store 包通过。
- 扩展 race：`go test -race ./internal/core/... ./internal/channel/... -count=1 -timeout=600s -parallel=8` 中所有包通过，除未修改的 `core/codex` 环境继承测试因大小写代理变量同时存在而失败。随后执行 `env -u https_proxy -u http_proxy -u all_proxy -u no_proxy go test -race ./internal/core/codex -count=1 -timeout=180s`，整个包通过。这是测试环境冲突，未修改该包。
- 最终送达顺序与状态改动后，重跑 `go test -race ./internal/core/store ./internal/core/engine -run '^Test(QuestionMarkerTarget|PendingQuestion|LatestQuestionMarker|Unknown)' -count=1 -timeout=180s`，通过。确定时序的测试覆盖新目标已写入而旧目标清理失败、恢复后只保留一个标记、长目录完整发布，以及未发布完毕时不生成重复目录。
- Teams adapter 的完整 race 包测试通过，包含幂等、Teams 标签归一化、HTML 引用保护、截图和附件保留、接收回执顺序、回读排除装饰，以及 1,024 个问题的目录分片。
- 最终隔离 Docker 问题黑盒回归：`go test ./internal/e2e/scenarios/turns -run '^(TestQuestions|TestAsyncQuestionMarker)' -count=1 -timeout=300s -parallel=3` 通过，8 个场景共约 51 秒。使用最终送达顺序实现，包含普通命令回复及原始用户消息接收回执上的问题链接与标记迁移。
- `go vet ./internal/core/engine ./internal/core/store ./internal/channel/...`、`git diff --check` 和改动文件的 gofmt 检查通过。`CGO_ENABLED=0` 的 Windows amd64、macOS arm64 两个受影响包交叉编译通过；没有执行 Windows/macOS 原生测试。

Docker 使用技能提供的临时容器框架、空 CXP fixture 配置和 fake Graph/fake Codex，无真实模型推理。框架默认 JSON 配置路径在本机不存在，使用独立 fixture；Docker 凭据助手所需 WSL socket 最初被沙箱阻止，获准后构建成功。容器继承的本机代理无法连接，最终仅在临时框架副本中增加对宿主机 Go 工具链和依赖缓存的只读挂载，离线运行测试，不修改仓库测试框架或已安装 helper。容器内测试使用 Go 1.26.3、`CGO_ENABLED=0`、`TMPDIR=/var/tmp`。

性能与写入：pending 导航只读取当前会话的未回答问题及有索引的消息 ID，不加载选项、答案或历史 rollout；不增加轮询。成功发送期间的目标标量更新并入已有发送结果事务，不增加独立提交。迁移只保存当前/旧目标，原来的目标 ID 键保持单一 ID 格式；无 schema 迁移或历史重写。目录仅在溢出后按当前有界列表哈希去重，走原有 outbox。1 万组历史的只读测试确认零行写入；最终 benchmark 的 100/10,000 组历史分别约 27.8/28.6 µs、1.5 KB、31/32 次分配。未发现随历史规模明显增长的退化。

CI 覆盖：普通测试由 `ci.yml` 的 `unit-linux` 及跨平台 unit job 收集；engine/store/Teams 测试进入 `race-core-exec` 的对应包分组；问题场景属于现有 turns E2E 包分片。无需新增 workflow 选择器。本轮未提交或触发 GitHub CI，未验证真实 Teams 客户端的列表预览和点击导航。

### 同日补充：回执与 question 合并 PATCH

仍在上述未提交工作区实现，不新增数据库状态、等待窗口或编辑批次。`Annotation` 支持可选 pending 状态，独立刷新和旧入口清理继续执行。回执路径只给应成为最新目标的原始用户消息附加 question；已有回执重试时仍能更新问题列表。超长装饰不会导致回执降级为单独 POST，独立刷新保留原有目录处理职责。转写和冻结提示没有增加主动状态查询。

最终验证：

- `go test -race ./internal/channel/teams ./internal/core/store ./internal/core/engine -run '^Test(PendingQuestion|QuestionMarker|PromptConfirmation|Copied|Transcript)' -count=1 -timeout=240s` 三包通过。新增测试覆盖一次合并 PATCH、写入成功但响应丢失后的幂等重试、独立刷新零额外 PATCH、回执不变但问题变化、nil 保留/空值清除、超长不影响回执，以及迟到旧回执不成为目标。engine 包实际运行了已有回执和问题恢复用例。
- `go test -race ./internal/channel/teams -count=1 -timeout=180s` 完整包通过。
- 隔离 Docker 中执行 `go test ./internal/e2e/scenarios/turns -run '^(TestQuestions|TestAsyncQuestionMarker)' -count=1 -timeout=300s -parallel=3`，8 个场景通过，约 53 秒。新增断言要求原始用户消息的接收回执和 question 合计只有一次 PATCH；既有迁移场景仍通过。环境沿用上述临时 Docker 框架、Go 1.26.3 和 fake Graph/fake Codex，宿主机 helper 未参与。
- `go vet ./internal/core/engine ./internal/core/store ./internal/channel/...`、`git diff --check`，以及 Windows amd64/macOS arm64 的 `CGO_ENABLED=0 go build ./internal/core/engine ./internal/channel/teams` 通过。交叉编译不代表原生执行。
- 新测试沿用现有 unit、core/channel race 和 turns E2E CI 入口，无需增加选择器。本轮未触发 GitHub CI，未实测真实 Teams 客户端。

性能审查：只在存在 pending questions 的追加回执路径增加有索引的原消息时间读取、现有目标标量读取和当前未回答问题读取；不轮询、不扫描历史正文、不增加写事务。独立刷新可能保留一次 GET，但相同装饰不再 PATCH。数据库写入、目录持久化及恢复流程不变。

发布前补充：在隔离 Docker 使用 `go test -p 4 ./... -count=1 -timeout=600s -parallel=4` 运行全仓，core engine/store 及 turns E2E 通过；命令 E2E 的 `TestEffortCommands` 因临时路径长度使控制上下文在 “These settings” 中途截断，原 golden 掩码只接受完整行而失败。场景内的实际命令回复和 effort 断言全部通过。已将该掩码限定为接受这一行的截断版本，未改变产品行为或移除回复断言；隔离 Docker 中单独重跑该场景通过。

修正后实际运行 `bash scripts/ci/run_e2e_shard.sh 0` 通过；容器使用 `GOFLAGS=-p=2 GOMAXPROCS=2` 以适应 PID 配额。第一次运行该分片时默认编译并行度触发 `resource temporarily unavailable`，调整容器编译并行度后通过；与功能测试失败分开记录。
