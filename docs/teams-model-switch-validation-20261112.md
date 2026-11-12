# Teams model/effort 切换实施验证记录

日期：2026-11-12。本文记录本地实现与验证，不代表已上线或 GitHub CI 已通过。

2026-11-13 发布候选：将补丁应用到上游 `b6ee90ed6e728654fc3e97a8c3ae8f8b8ded6fe2`，应用无冲突；上游未修改本补丁涉及的核心包。隔离 Docker 上 Teams channel 单元测试通过，E2E 第二、第三分片通过。第一分片暴露 `CMD-EFFORT` 的历史截断位置随临时路径长度变化：功能断言通过，golden 在被截断的行末相差数个字符。该场景只规范化紧邻截断标记的那一行，并额外断言用户收到的完整说明；修正后按 CI 入口重跑第一分片，五个包全部通过。

`rc.15` 云端 CI 进一步发现两个遗漏：黑盒 E2E 不得直接导入产品内部提示词包，内置 skill 命令表仍需列出兼容别名。修正为在 E2E harness 独立声明预期协议文本，并补充 `helper model <model> [effort]`。修正后执行 CI 原入口 `bash scripts/ci/run_linux_full_go_test_collect.sh`、两个受影响 E2E 场景及黑盒边界和 skill 测试，均通过。首次本地覆盖率重跑把临时构建目录放在源码树内，导致格式、源码扫描与进程路径测试失败；将临时目录移到容器 HOME 下后完整重跑通过。

## 实现与范围

- 基于上游 main `b4b9ca122b2924912ddb4bc52a0dd428c54c9a5c`，分支 `investigate/teams-model-switch-20261112`。工作目录：`/home/baka/.local/state/cxp-model-review.a21C6mrW/worktree`。所有变更尚未提交，原用户 worktree 未修改。
- 支持 `model use <model> [effort]`，先匹配完整模型 ID/显示名，再解析最后一个 effort；保留旧别名、effort-only、reset 和新聊天默认快照规则。model/effort 一次校验和事务提交，非法参数不改设置、不中断。
- 设置命令已入 CXP 队列即阻挡后续普通输入进入旧执行，包括目录校验尚未返回时。每会话至多一条持久化切换记录，绑定旧任务与 attempt；命令 worker 不等待整个中断过程。
- 针对旧 client ID 中断一次，等待其执行返回与终态核对；沿用同一 thread/cwd/权限/输出目录，使用准入事务读到的最新有效设置继续。切换不强杀后端、不调用实验性 `turn/settings/update`，不启用 `step_model_switching`。
- 新的当前任务普通输入按原序合并；有待处理附件/语音先等待。没有新输入才发送内部续接指令，已接受的原请求或补充不重放。显式排队任务、问题与审批保留原语义。Control 后续问题仍各自排队，不作为当前任务补充。
- 自然完成或取消不复活；连续设置在准入前合并，准入后形成下次切换。执行获准启动但尚未登记 runner 的窗口内取消，也会在实际调用后端前核对并停止。容量重试保留退避，重复同一目标不提前唤醒重试；切换不会与自动重试各启动一次。
- 无法确认启动、停止或输入保留时暂停并通知，不猜测重发。第一版仅覆盖 CXP 管理的普通任务；外部 TUI 附着、原生 goal/自续接执行在中断前拒绝切换，不承诺子代理与跨机器同步切换。

模型切换和两处自动失败重试共用 `internal/continuation/prompt.go`：

> Continue the active unfinished task from its current state. Keep the previous response language unless the user requests otherwise. Verify interrupted actions before retrying; do not repeat completed work.

提示词不会回滚工具副作用，也不能证明外部操作恰好执行一次。中断命令可能已产生结果，续接需核对；内部指令不镜像为用户消息。

## 原生协议事实

使用真实 Codex CLI **0.167.1** 与可控的模拟 Responses 服务，在离线 Docker 中测试；这是原生协议执行，不是云端模型推理。

- 普通与原生 queued 执行：旧执行中断终态之后，使用相同 thread、新 client/new turn，以新 model/effort 启动；模型服务观测到两次且仅两次请求。
- **steer RPC 成功不代表输入已经进入可恢复历史。** 本版本中，接受但尚未采样的 steer 在立即中断后可能查不到；已采样输入可查到。新增原生测试分别控制两种时序，engine 缺失历史测试要求暂停，不能补发或静默丢弃。
- 复用 `Lookup/FindTurn`：先读取最近 20 个 native turn，必要时沿用既有完整 thread 读取回退；这仅在切换收尾或恢复核对时发生，不放入正常轮询。大量旧历史且缺失 client ID 时仍可能触发一次完整读取，这不是幂等证明。

## 验证环境与入口

宿主纯单元测试使用 fake agent；真实 CXP/Codex 进程全部在开发 skill 的临时 Docker harness 中运行，独立 HOME、PID、网络、配置和状态，不挂载宿主 Teams 状态或进程资源。原生契约容器使用 `--network none`。Windows 检查只做交叉编译，不代表原生 Windows 运行通过。

为运行 Go 测试，使用 harness 的临时副本，增加 Go/GCC 与无凭证构建缓存；安装的 skill 未修改。容器 `/tmp` 为 noexec，因此 `TMPDIR/GOTMPDIR` 指向容器自己的源码工作目录；后续限制 Go package 并行编译为 2，避免测试容器的 PID 额度耗尽。未放宽宿主权限或修改宿主服务。

| 检查 | 入口 | 结果 |
| --- | --- | --- |
| engine/store 全包单元 | `go test -count=1 ./internal/core/engine ./internal/core/store` | 通过；最后的调度修改已由最终 engine 全包 race 覆盖 |
| 最新 engine race | `go test -race -count=1 ./internal/core/engine -timeout=300s` | 最终源码全包通过，181.494s；包含取消准入窗口修正 |
| codexrunner 全包 | Docker：`go test -count=1 ./internal/codexrunner -timeout=240s` | 通过，13.177s |
| 核心 race 分组 | Docker：`bash scripts/ci/race_core_packages.sh {store,engine,migrate}` | 三组通过；store 195.125s，engine 217.709s，ASR 175.073s，migrate 313.088s；最后的调度修改另由最终 engine 全包 race 覆盖 |
| 最后定向 race | `go test -race -count=1 ./internal/core/engine -run '^(TestQueuedAck\|TestSwitch\|TestBusyModelSwitch\|TestModelAndEffort)' -timeout=180s` | 通过，35.577s；含中断失败/无终态、输入保留与 ACK 回归 |
| CI 脚本单元 | `python3 -m unittest discover -s scripts/tests -p 'test_*.py'` | 149 项，成功；9 项按平台/环境条件跳过，包含新增原生必跑检查的 skip/零匹配失败断言 |
| frozen v5 升级补充 | Docker：`go test -count=1 ./internal/e2e/scenarios/entry -run '^TestEntryTypedSQLiteUpgradePreservesAttemptedNotice$' -timeout=120s` | 最终 v9 预期通过，9.772s；最终第三分片也已执行通过 |
| E2E 三分片 | Docker：分别执行 `bash scripts/ci/run_e2e_shard.sh 0`、`1`、`2` | 最终源码的 15 个包全部通过，三个入口退出码均为 0；取消设置 `E2E_UPDATE_GOLDEN` 与 `E2E_GOLDEN_OUT`，使用正常比较模式，`GOFLAGS=-p=2`、`GOMAXPROCS=4` |
| 原生 Codex 契约 | `bash scripts/ci/run_shared_codex_contracts.sh <native-codex>` | 通过；Shared/NativeQueue/Title 计数 3，另含 GatewayRecoveryTUI、NewChatTUI |
| 最终原生产品验收 | 最新 `CGO_ENABLED=0 go build ./cmd/codex-proxy` 与 realcodex probe，在 `--network none` Docker 中执行 `^TestRealCodex(NewChat\|GatewayRecovery)TUI$` | 通过；GatewayRecoveryTUI 2.07s、NewChatTUI 3.88s，覆盖最后一次调度修改后的实际 CXP 二进制 |
| CI 可达性 | `.github/workflows/ci.yml` 既有 race、三分片、shared-codex 入口 | 新增测试实际被选中；原生脚本还验证测试存在且 PASS，不能空匹配或 skip 后通过 |
| Windows | `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test -c`，engine/codexagent/codex | 交叉编译通过；engine 已按最终源码复核 |
| 格式、静态与 diff | 全部变更 Go 文件 `gofmt -l`、`bash -n scripts/ci/run_shared_codex_contracts.sh`、受影响包 `go vet`、`git diff --check` | 通过 |

异步回归使用 channel 同步点控制校验、中断、输入、恢复与准入顺序。覆盖 A→B→C、A→B→A、有效后无效、准入后再切换；输入快照前后消息、未就绪语音及取消；自然完成、容量退避、未知启动、崩溃后已接受/未确认、过期中断身份。store 覆盖事务回滚、唯一续接、输入所有权和 v8→v9 兼容迁移。

最终 E2E 各分片耗时最长的包分别为 commands 173.136s、sharedhome 186.764s、entry 255.716s。执行前已比对宿主与容器内 34 个变更 Go/sh/Python 文件的内容，全部一致。原始日志保留先前 fixture 生成阶段的失败记录；修正后正常比较的三个独立入口全部返回 0，不能把早期汇总驱动保留的失败码解释为最终入口失败。测试容器已清理，原始日志未作为附件导出。

## 契约修改依据

- 繁忙切换 E2E 原先要求“当前执行继续旧模型”，现改为阻塞旧任务、中断、同 thread 自动续接后再运行显式排队任务；断言新 client、目标 model/effort、输入不重放、内部续接不镜像及无执行重叠。确认消息可能编辑原消息，测试沿用现有 `PromptConfirmations`，不假定 ACK 一定另发消息。
- 自动重试不再断言字面量 `continue`；改为共享英文文本、保留回复语言、无原始请求 token、同 thread/client 和内部输入不镜像的语义断言。重试条件和预算不变。
- 帮助/status 的旧“下条消息生效”文案改为目标与运行状态说明；相关 golden 需要随契约更新，具体差异在最终结果中记录。
- 已逐项审查并导入 13/161 份 golden：commands 9 份（帮助、状态、组合命令、繁忙自动续接），dashboard 1 份（model status 文案），turns 3 份（原字面量 `continue` 改为共享短指令）。Control 历史摘录仍按原长度截断，较长的新状态文案导致摘录截断位置变化；未放宽截断/消息顺序断言，未改不相关 fixture。
- E2E 首次语义执行暴露续接后的排队 ACK 缺少任务名称，已通过原任务关联修正，并新增“仍显示原任务名称”断言。续接答案不能沿用按原输入 token 筛选 native 调用的等待函数，因为续接不重发该输入；测试改为核对续接答案送达且仅送达一次，没有放宽产品约束。
- frozen v5 升级 E2E 原先硬编码期望 v8，现明确期望 v9，原绑定、已尝试通知只走一次 fallback、已完成 CLI 结果及重启不重放的断言全部保留。其余测试未出现待解释的产品断言变化。
- 开发中定向测试暴露并修正：无排队输入时 handoff 没有调度唤醒、首次 Control 设置缺少绑定、未知启动被误视为明确未提交、续接容量失败后的未接受输入关联未转移，以及同目标命令绕过退避。对应回归保留，未用宽松断言掩盖故障。

## 性能与持久化

- schema 8→9：兼容默认/nullable 字段、每会话一条未完成切换、每条补充一条关联。终态记录沿用原有历史保留策略；无需维护第二套阶段状态或复制消息正文。
- 迁移不重写历史正文；新建两个 partial index 会在迁移时读取既有 turns，不能称作完全不扫描历史。
- 正常调度新增小型索引查询，读取不写行；实际执行前增加一次按 turn 主键读取，封住取消与 runner 登记间的窗口，不随历史长度扫描。相同有效设置不更新 thread，不创建切换，不清除容量退避。每次真实设置变更才递增目标版本。没有配置 JSON/JSONL 全文重写。
- `TestModelSwitchReadsDoNotWriteOrScanHistory`：5000 条已完成命令，反复读取后 `total_changes()` 不增；热 SQL 的 EXPLAIN 校验通过。
- `BenchmarkModelSwitchIdleReads -benchmem`：历史 0 条 23702 ns/op、2721 B/op、73 allocs/op；历史 5000 条 25006 ns/op、2716 B/op、73 allocs/op。此对比证明该新增查询不随已完成命令历史线性增长，**不是整个功能改动前后的端到端性能对比**。

## 未验证与环境失败

- 真实云端语言/工具行为补充测试使用 harness `codex` 模式（官方 `gpt-5.6-luna`，standard/default tier）。在推理前因 `loopback proxy variables do not map to a copied CXP instance` 被 harness 拒绝：隔离测试配置缺少对应代理 profile，模型未被调用。没有绕过隔离、借用宿主运行状态或改动用户凭证。因此只能证明提示词含语言约束，不能声称实测模型始终遵守。
- 首次完整 E2E 在并发编译时出现 `resource temporarily unavailable/newosproc`，属于测试容器 PID/线程额度不足，不是产品断言失败；旧源码测试容器已停止。后续降低构建并发重新执行，结果单列。
- Docker 构建最初受沙箱内 Windows credential-helper/vsock 限制，精确授权后可构建；容器 noexec 临时目录已按上述方式解决。没有未解释的沙箱绕过。
- 没有执行原生 Windows/macOS 进程测试、真实 Teams 云端收发、生产安装或服务重启。未提交、推送、创建 PR、发布或部署。
