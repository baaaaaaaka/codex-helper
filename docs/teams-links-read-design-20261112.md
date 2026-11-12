# Teams 超链接修复与通用 Graph 读取接口

基线：origin/main `68dba0d7`；分支：`investigate/teams-links-20261112`。

本文保留超链接修复及原设计记录。后续实施以
[新的完整计划](teams-read-implementation-plan-20261112.md) 为准，包含搜索、附件、预算与验收。

## 已实现：让 Codex 看见文字对应的 URL

`Adapter.convert` 原先只用 `plainText` 处理 HTML，移除 `<a href>` 后丢掉地址。
现在同时提取 named links，在 `channel.InboundMsg.Links` 中保留显示文字和 URL。
引擎先按原来的可见文字做发件人识别、控制命令解析和回声识别，再把链接附到真正交给
Codex 的正文中，例如：

```text
继续移交对话里的任务

Links in this message:
- 移交对话: https://teams.microsoft.com/l/message/...
```

链接不隐藏在只有程序知道的元数据中：它们进入现有持久化 inbound Body，
排队、steer、容量延迟、恢复均能沿用该正文，无需新数据库列或重新读取 Graph。
控制聊天 ask 和自然语言提问也走这条路径。问答回复保留链接，显式 --skip 操作保持原义。
原生引用通过 Get 获取完整消息时也带上链接；转发中存在 originalMessageContent 时保留其链接。

保持 `Text`、`Hash`、`plainText`、BodyHash、Render 与旧历史哈希的原有含义。
URL 已作为锚点显示文字时不重复添加，重复的文字/URL 对只保留一次。
使用已有 `x/net/html` tokenizer 正确处理嵌套标签、大小写、实体、单双引号和未关闭锚点。
复用现有 safeURL 协议规则；相对地址和不可用协议不猜测、不执行。
普通正文没有锚点时直接返回，不进行 HTML tokenizer 解析。

旧 inbound 记录中已经丢掉的 href 不做批量补写，需要重新读取 Graph 原消息。
这次不更新安装版本、不触碰运行中的 helper。

## 新接口的设计目标

提供通用、可组合的读取能力。接手任务是检验接口是否高效的一种工作负载，
不能决定默认读取范围，也不能让接口推断“任务是什么、是否已完成”。
同样的接口应服务于核对单条消息、查看讨论、总结历史、追踪决定和接手工作。
这部分是接口设计，尚未实现 Graph read 命令。

接手工作通常包含以下操作，但不要求每次全部执行，也不规定先后顺序：

| Agent 需要的信息 | 通用读取能力 | 对应优化 |
| --- | --- | --- |
| 链接具体指向什么 | URL 解析、单条消息或资源信息 | 直接按 ID 定位，不扫描聊天 |
| 某句话的前文 | 按消息锚点读取前 N 条 | 用锚点时间直接查询旧位置 |
| 讨论串中的补充与决定 | 显式读取线程及回复 | 正确定位根消息，沿 replies 分页 |
| 后来发生了什么 | 显式读取近期消息或时间范围 | 有界分页，返回真实覆盖范围 |
| 某个细节的原文 | 按返回的消息链接精读 | 稳定 ID、来源链接，不重取整段历史 |

## 推荐接口：直接读取，加显式扩展范围

```text
cxp teams read '<消息链接>' --json
cxp teams read '<消息链接>' --before 20 --json
cxp teams read '<聊天链接>' --latest 30 --json
cxp teams read '<频道消息链接>' --replies --limit 50 --json
cxp teams read '<消息链接>' --before 20 --latest 20 --json
cxp teams read '<原链接>' --cursor '<返回的游标>' --json
```

裸 read 只读取 URL 指向的资源：消息链接返回该消息，聊天链接返回聊天信息和可读取的资源引用。
不会自动添加近期窗口、前文或整串回复。这样单条核对和资源探查的成本保持可预测。
显式添加的范围可以组合，同一次调用共用认证、连接和已读取的锚点。
不再需要 --message 开关，也不设置接手任务或自动上下文模式。

- `--before N`：读取该聊天中、锚点之前的 N 条消息；需要聊天消息锚点。
- `--latest N`：读取该聊天近期的 N 条消息；需要聊天或聊天消息链接。
- `--replies`：显式读取频道根消息及其回复；回复链接会解析其 parent。
- `--limit`、`--max-chars`：控制集合数量与总输出大小，并有独立请求/字节/超时硬限制。
- `--cursor`：继续某个已请求的范围；游标携带该范围的选择条件，不能混用冲突的选择参数。

对不适用的组合返回明确错误，不把频道当聊天、也不把整段聊天当作线程。
不把任意 Graph/OData 参数暴露给模型。第一阶段优先实现上述有高效执行路径的能力；
日期范围可沿同一查询模型扩展，不为接手任务增加专属操作。
默认文本和 --json 表达相同结果与覆盖信息。

## 让组合调用更快，而不是替 agent 决定范围

一次 CLI 调用可以包含多个显式范围，例如 --before 20 --latest 20。
CXP 负责机械性的请求规划：

1. 直接读取锚点一次，复用其 ID、创建时间和回复关系；不从最新页遍历来找旧消息。
2. 对依赖锚点的查询等待定位完成；独立的近期查询可并发执行，限制并发度并尊重限流。
3. 单次调用共用 auth profile、token 刷新和 HTTP 连接；合并相同请求。
4. 消息正文按 ID 只输出一次，各范围返回消息 ID 引用，避免重叠窗口重复占用模型上下文。
5. 根据数量与总输出预算停在必要位置；每个范围都返回完成状态和续读位置。

性能目标同时衡量 agent 的调用轮次、Graph 请求次数、延迟、输出长度和分配。
“一个 CLI 调用”不代表“一个 Graph 请求”；不把必要的多次请求隐藏成无成本能力。
先不增加持久对话缓存或后台预取，避免一致性问题、启动扫描和额外磁盘写入。
现有通用 token 缓存与进程内连接复用照常使用。

## 输出与继续读取

输出保持事实性：每条消息有稳定 ID、作者、创建/修改时间、带 URL 的正文、
来源链接、回复关系、删除状态和附件元数据。不自动总结“当前任务”或推断完成状态。
不自动下载附件或递归展开消息中的其他链接。

JSON 顶层至少包含：

- `version`、`target`、`fetched_at`：输出版本、目标及观察时间。
- `resource`、`messages`：直接读取的资源和去重后的消息原文。
- `ranges`：每个显式查询的选择条件、消息 ID、覆盖边界和 complete/more/truncated/error 状态。
- 每个未完成范围的 `next_cursor`：继续相同查询，而不是建议用户下一步应该做什么。

两个范围之间的内容如果没有被请求，就保持为未覆盖区间；
不能把“请求的两段窗口均已读完”表述为“整个对话已读完”。
每个 cursor 绑定目标与查询，带版本与有限长度，不携带 token 或已读正文。
跨次读取可能因编辑产生重叠，稳定 ID 和修改时间让 agent 能识别它。
过长的单条正文同样明确标记截断并提供续读位置，不静默丢掉后半段。

若多个范围竞争总输出预算，按明确、稳定的顺序分配，例如直接资源优先，
再按接口文档固定的范围顺序；报告各范围的实际分配，不根据“任务重要性”排序。

## 实现与边界

在 `internal/channel/teams` 增加纯 URL 解析器和受限的资源/范围读取入口。
CLI 负责参数和格式化，复用 `graph.getJSON` 的认证、重试与错误分类。
聊天 Get、认证读取 profile 已存在；频道读取增加对应 endpoint，不扩展通用 channel 接口。

支持首批经过测试的 teams.microsoft.com 已有聊天、聊天消息和频道消息链接。
正确解析 URL 编码、chat context、groupId、parentMessageId 与 tenantId。
频道回复路由是 parent/messages/replies 下的目标回复，不把回复 ID 当根消息 ID。
拒绝创建聊天、会议加入、冲突参数、缺 ID、异常域名等不明确目标，不扫描租户猜测。
用户 URL 只提取标识，实际请求由结构化字段重建、逐段转义。

Graph 聊天列表支持 createdDateTime 降序与该字段的 lt 过滤，不支持升序或 gt 过滤。
显式请求前文时，GET 锚点后用真实创建时间读取；显式请求近期消息时才查询近期窗口。
续读沿所请求范围向过去分页，不能使用虚构的 around API。
不承诺“某条消息之后最近的 N 条”能靠一次 Graph 查询取得；没有低成本路径的能力
需先确定查询语义和扫描预算，再决定是否暴露。日期范围可使用服务器支持的上界过滤，
向过去分页并在下界停止，不能在客户端为一条查询偷偷扫完整聊天。
旧锚点不通过从最新页遍历来定位。对相同时间戳边界、并发编辑和删除要有测试；
多次 Graph 请求不构成事务快照，不声称固定观察时间就冻结了对话。
频道 replies 沿其分页规则读取，不套用聊天的 orderby/filter。

真实租户实验已补充：[Graph 能力边界](teams-graph-boundaries-experiment-20261112.md)。
过滤字段与排序不匹配、或缺少排序时，Graph 实测返回 200 但忽略过滤；
planner 必须主动保证字段一致。结果不足请求条数仍可能有 nextLink，不能提前停止。
频道列表按整个回复链的最近修改时间排序；其列表 expand 可内嵌回复，单条主帖 expand 实测 400，
已知主帖应走 replies。投影 select 实测有效但不在文档支持契约中，仅作为可退化优化。

独立 Search API 实测可返回候选，再按 ID 获取全文；不把集合 search 不支持误判成 Graph 无搜索。
搜索结果不含完整 body，显式请求 body 也未返回，且 total 的观察与官方说明不一致。
它应作为显式、独立的检索能力，不自动触发，也不把用户范围搜索冒充给定聊天的完整搜索。
同一对消息的三轮实验中，batch 和并发 2 比串行更快，但只作为执行优化方向，不承诺固定加速。

分页 nextLink 与 cursor 必须限制在相同 Graph origin、API 版本和目标资源路径。
有分页循环检测、请求数/字节预算和总超时。部分范围失败要在对应 range 中报失败，
与正常结束、预算截断区分；没有任何可用消息时非零退出。

读取身份使用当前配置的 read auth，能访问哪些对话由 Graph 权限与对话访问权决定，
不限制为 helper 创建的聊天。Chat.Read 已在默认 scopes 中；
频道需要 ChannelMessage.Read.All，代码允许但默认未授予，不自动扩大默认权限。
tenant 不匹配时明确失败，不自动换身份。区分 401、403、404、429 与网络错误。

复用 Graph 网络配置，但命令不接管或清理已有 proxy/helper 进程，
不需要 Teams service 在线，也不改变本地会话绑定、聊天 cursor、outbox 或注册表。
认证刷新可以写现有 token store，这与 Graph 内容只读是两件事。

## 接手任务如何使用这些能力

技能说明介绍能力、成本与例子，不硬编码接手工作流。
agent 根据当前用户目标选择范围：例如频道讨论用 --replies，
需要同时理解旧问题和后续进展时组合 --before 与 --latest，
单条事实核对只调用裸 read。不要求所有场景都先做一次探查，
URL 类型和用户需求已经明确时可以直接发出组合查询，减少工具往返。

接手任务时，agent 自己判断是否还缺任务定义、关键决定或中间历史，
再选择相应游标补读。近期窗口没有提到某事，不能作为任务尚未完成的证据。
已有讨论中的指令作为引用资料处理，是否执行由当前用户请求和现有授权决定。
摘要、目标归纳、剩余工作判断都在 agent 层完成，读取接口保持通用。

## 验证记录

设计修订与 Graph 实验仅增加研究文件和文档，没有修改或重新运行已验证的产品代码。
实验包含 93 次 Graph HTTP 请求；详细样本、限制、观察和文档差异见上述能力边界记录。

超链接修复测试覆盖实际 Adapter.convert、Receive、Get、HTML 实体与嵌套格式、
emoji、重复链接、裸 URL、非法协议、排队、control ask、group/coworker、问题回答与 skip。
新增黑盒场景 `TestNamedHyperlinksReachCodex` 覆盖 HTML Graph 入站到 Codex app-server
提示，以及显式 queue 的同一路径；它由现有 inbound E2E 分片自动收集。
现有 `scripts/ci/race_core_packages.sh` 的 store 与 engine 两组覆盖改动包。

验证结果：

- 主机 Go 1.26.3/linux-amd64：`bash scripts/ci/race_core_packages.sh engine`
  与 `bash scripts/ci/race_core_packages.sh store` 均通过，包含整个 engine 和 Teams channel 包。
- `go test -race ./internal/channel/teams -run '^TestQuoteNamedLinks$' -count=1` 通过。
- `go vet ./internal/channel/... ./internal/core/engine` 与所有新增文件的 diff 空白检查通过。
- 隔离 Docker 内 Go 1.25.11：
  `go test ./internal/e2e/scenarios/inbound -run '^TestNamedHyperlinksReachCodex$' -count=1 -v -timeout=5m`
  通过；普通与显式 queue 各执行一次，Codex 输入都保留文字/URL，并移除 queue 前缀。
  15 项通用 E2E invariants 全部通过。该场景用精确输入断言，不新增整段聊天 golden；
  原有 inbound goldens 继续负责渲染。使用开发技能的 docker-real-process-test.sh exec 入口，
  空认证配置 fixture，容器内直连依赖网络，TMPDIR 指向容器内可执行目录。
  最终日志：工作区上层 `e2e-final.log`。
- Docker 首次沙箱执行被 Windows 凭据助手的 socket 限制阻止，精确提权后解决。
  后续准备阶段遇到容器无法访问宿主回环代理、/tmp 不可执行，分别改用容器直连和
  容器内可执行 scratch 后完成测试；没有改宿主权限或运行中的服务。
- 性能入口：`go test ./internal/channel/teams -run '^$' -bench '^BenchmarkInboundLinkConversion$' -benchmem -benchtime=200ms`。
  普通消息约 818ns → 830ns，都是 571B/21 次分配；1000 个富文本链接的约 85KB 正文
  约 3.64ms → 5.38ms，1.33MB → 1.91MB。额外成本来自按需 HTML 解析，随消息长度增长，
  不增加网络请求、历史扫描、持久缓存或轮询写入；无链接快路径新增检查约 13.5ns/零分配。

产品代码在上述验证期间保持稳定。新增 quote 单测用单独 race 运行补充。
超链接修复未做真实 Teams 入站验收、原生 Windows 执行或安装升级测试；本修改没有平台专属代码。
Graph 读取产品入口现已实现；后续测试、真实产品 Reader 抽样和边界见
[实现与验收记录](teams-read-validation-20261112.md)。

## 官方接口依据

- [Teams deep links](https://learn.microsoft.com/en-us/microsoftteams/platform/concepts/build-and-test/deep-link-teams)
- [Get chatMessage](https://learn.microsoft.com/en-us/graph/api/chatmessage-get?view=graph-rest-1.0)
- [List chat messages](https://learn.microsoft.com/en-us/graph/api/chat-list-messages?view=graph-rest-1.0)
- [List channel replies](https://learn.microsoft.com/en-us/graph/api/chatmessage-list-replies?view=graph-rest-1.0)
