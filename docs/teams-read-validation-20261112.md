# Teams 读取功能：实现与验收记录

基线：`68dba0d7`；分支：`investigate/teams-links-20261112`。
工作区：`/home/baka/.local/state/cxp-teams-links.e28JgFAX/worktree`。
以下记录本地功能验收时的工作区状态；提交和 prerelease 发布随后由独立发布流程完成。
运行中的 Teams helper 未安装或重启。发布说明见 `docs/releases/v0.1.23-rc.1.md`。
源码／测试／skill／CI 共 29 个改动文件的聚合 SHA-256：
`28138546a043892aca3fbd2d86a14becf2107426786efd228eccc8b9b0124c5b`
（路径排序，以路径、NUL、内容、NUL 依次输入；研究及验收文档不计入）。

## 已实现行为

- 命名超链接保留文字与 URL，通过普通消息、queue、control ask、引用及共同入站路径进入
  Codex；先识别命令，再附加链接。原可见文本、echo hash 和历史状态兼容。
- `cxp teams read`：裸读目标、显式聊天 before/latest、频道根帖与回复、多个消息精读。
  不默认展开历史或假设任务目标。重叠范围共用消息表，范围 ref 按创建时间排序。
- `cxp teams search`：当前身份的 Teams 索引候选、摘要、more 与游标；可定位候选返回 read ref。
  缺少可靠频道 parent 的候选返回 unresolved 和原因，不猜测端点。
- `cxp teams fetch`：显式选择来源消息中的文件／hosted 图片；支持聊天、频道根帖及回复路由。
  校验来源、版本、大小和 Range，302 后不携带 Graph token，拒绝已有输出，完成后以硬链接
  原子建立最终文件名。返回实际大小、类型与 SHA-256。
- 单次调用共享认证、连接、缓存及预算；before/latest 初页并发 2，多消息 GET batch 每批至多
  20 项。重试和 batch 子请求计费；429 不立即重试。无后台抓取、持久消息缓存或新数据库。
- 游标绑定目标与条件，保留页中位置及原始页查询；长正文按 Unicode 字符续读并检查内容版本。
  同源、API 版本、资源路径、查询条件和分页循环均有校验。部分失败保留结果，非零退出。
- 默认 read/search：20 逻辑请求、8MiB 响应、60000 字符、200 条、60s；编码输出另限 512KiB。
  fetch 默认 100MiB/300s，硬上限 2GiB/10m。
- 已更新随软件发布的 `internal/skills/builtin/cxp/SKILL.md`、命令地图和
  `references/teams-read.md`。没有直接改当前安装的 skill。

实现复用已有 Graph 认证、网络、错误分类、下载流接口与大小硬上限。
新下载器保留独立的来源／版本状态、频道定位和无覆盖完成策略；没有将新 ref 塞入旧的聊天
FileRef 和 `.part` 格式，也没有改变旧 Adapter.FetchFile 的历史行为。这是对计划中公共下载
重构范围的收缩，避免让新接口同时改变在线附件处理路径。

hosted 图片通常直接从消息识别；正文包含图片但缺少 hosted ID 时按需列举。
列举失败保留正文并报告 unresolved；额外 hosted 续页不静默展开。

## 回归测试与实际入口

以下测试均禁用 Go 测试结果缓存，构建缓存保留。

| 环境 | 入口 | 结果 |
| --- | --- | --- |
| Linux amd64，Go 1.26.3 | `go test -count=1 ./internal/channel/... ./internal/skills ./internal/e2e/fakegraph` | 通过 |
| Linux amd64 | `bash scripts/ci/race_core_packages.sh store` | 通过；最终 Teams 包另作完整 race 复验 |
| Linux amd64 | `bash scripts/ci/race_core_packages.sh engine` | 整组通过；engine 201.124s，ASR 149.021s |
| Linux amd64 | `go test -race -count=1 -timeout=180s ./internal/channel/teams ./internal/e2e/fakegraph` | 最终源码通过 |
| Linux amd64 | `go test -race -count=1 ./internal/channel/teams ./internal/cli -run '^TestTeams(Read\|Search\|Fetch\|CommandWiresPlannedSubcommands)'` | 通过 |
| Linux amd64 | `git diff --check`、所有新文件空白检查与 `gofmt -l` | 通过 |
| Linux amd64 | `go vet ./internal/channel/... ./internal/cli ./internal/core/engine ./internal/e2e/fakegraph` | 通过 |
| Docker，Go 1.25.11，CGO_ENABLED=0 | `go test -count=1 -timeout=300s ./internal/cli` | 完整 CLI 包通过，49.948s |
| 同一隔离 Docker | entry 包 `TestTeamsReadSearchFetchWithoutHelper` | 通过；真实源码 CLI、19 次 fake Graph 请求、无 helper、无消息／文件修改 |
| 同一隔离 Docker | inbound 包 `TestNamedHyperlinksReachCodex` | 通过；普通与 queue 输入均保留文字和 URL |
| 原生 Windows amd64，CGO_ENABLED=0 | 最终 teams/cli 测试二进制，`^TestTeams(Read\|Search\|Fetch\|CommandWiresPlannedSubcommands)` | 通过，包含下载、续传、原子完成与子命令检查，无相关用例跳过 |

Docker 使用开发技能的 `docker-real-process-test.sh exec`，复制空 Codex/CXP/SSH fixture；
测试进程、端口和状态在临时容器中。复用现有 Go 1.25.11 工具链副本，容器内直接访问依赖源，
TMPDIR 指向经过验证的容器可执行目录。没有复制生产 Teams 状态或登录凭据。
Windows 通过 PowerShell 直接调用交叉编译的测试程序，在独立 Windows 临时目录实际执行；
不是只做交叉编译。未改变 Windows 执行策略。

测试覆盖 URL 编码与冲突、租户与 ref/cursor 校验、稀疏／空续页、循环与外域分页、服务端忽略
过滤、真实时间比较、页中续读、正文编辑、并发窗口的受控重叠、部分 batch 403/429、乱序／缺项、
20 项分批、请求／字节／时间预算、搜索摘要与跨窗口继续、频道回复分页、下载重定向无 token、
真实中断后续传、过期临时地址后重新解析、200 忽略 Range、错误 Content-Range、完成的 partial
恢复、源版本变化、大小上限、已有目标与 symlink 拒绝。

新增用例进入 CI：channel 包属于现有 store race 组；CLI 新测试加入 Teams race 选择表达式；
完整跨平台单测继续收集 CLI；E2E entry/inbound 分别进入自动包分片 2/0。没有禁用原有 invariants。
本地执行的是上述相关 E2E 用例，没有重复运行全部 E2E 分片，也没有运行远端 GitHub CI。

## 修复与测试环境记录

- 完整 CLI 测试最初发现旧的固定子命令清单不包含 read/search/fetch。
  只增加三个预期项；保留精确清单检查和 listen 兼容性检查，未放宽断言。
- 新增 50 条消息、首次只返回 45 条的回归，发现按剩余数量重建 `$top` 会重放已读消息。
  现在保留当前页的完整原查询，后续返回剩下 5 条，并保持一次页请求。
- 下载已完成但最终校验中断时，不再卡在 `Range=文件长度-`；文件复用已完成 partial，
  无大小元数据的 hosted 内容遇到 416 后只从头重试一次。
- 容器准备曾因运行镜像缺 Go 工具链失败；仅带入预编译测试仍被 harness 内部 Go 构建要求阻止。
  带入现有工具链后完成测试。它们属于环境准备失败，不是产品测试失败。
- Windows 首次参数被 PowerShell 拆分，随后 `-File` 被系统签名策略拒绝；
  使用显式字符串数组直接执行测试程序后通过，没有更改或关闭签名策略。
- 本轮没有未解决的沙箱访问失败；Docker 与原生 Windows 的精确执行权限由工具审批路径获得。

## 真实 Graph 抽样：调用产品 Reader

使用现有 read 配置和私有 SQLite 认证快照，独立 Go 探针调用正式 Reader API。
没有启动源码 helper、发送消息、修改文件或增加 scopes。消息正文、资源 ID、tokens 和临时下载
地址未进入报告。仅把下载字节写入私有测试目录，不作为附件交付。

| 产品操作 | 观察结果 | 用量／单次耗时 |
| --- | --- | --- |
| 按 ID 裸读 | 1 条，complete | 1 请求，736ms |
| before 3 + latest 3 | 4 条去重消息，三个范围 complete | 3 请求，1186ms |
| 长正文续读 | 20 字符 + 剩余 115 字符 | 各 1 请求，版本检查通过 |
| 多消息直接读 | 2 条 | 2 逻辑请求，628ms |
| search + 精读 | 3 个候选，more；精读 3 条原消息 | search 1 请求／1201ms；精读 3 逻辑请求／940ms |
| 频道线程 | 根帖和 7 条回复 | 2 请求，1003ms |
| SharePoint 引用文件 | 完整 17991 字节，SHA-256 已记录于私有脱敏报告 | 5 请求含无认证重定向与末次版本检查，3175ms |
| hosted JPEG | 完整 116230 字节 | 3 请求含末次版本检查，2936ms |
| 真实页中游标 | 先 45 条，续读 5 条；无重复，最终 complete | 初次 2 请求；续读 1 请求 |

这些是单个当前身份与样本的功能观察，不是延迟分布或最小权限结论。
额外游标探针首轮只记录了失败与零条数，诊断不足，不能归因为特定 Graph 能力边界；
补充结构化错误／用量诊断后的实际抽样通过 45+5 条续读，未复现该失败。

## 性能与磁盘写入审计

`BenchmarkTeamsReadCombinedLargeFixture`：100 条富文本消息，before 49 + latest 50 + 锚点，
组合去重，无持久缓存。最终约 4.925ms/op、3750017 B/op、27500 allocs/op；
119050 响应字节、89671 编码输出字节、固定 3 个逻辑请求。
这是本机 loopback fixture，不能代替实际网络延迟。基准把 planner 和最终 JSON 编码一起测量。

`BenchmarkInboundLinkConversion` 的普通消息：原转换 816ns／571B／21 次分配，附加链接检测后
811ns／572B／21 次分配，未见明显回退。1000 个富文本链接、约 85KB 正文：
原转换约 3.51ms／1.33MB，保留链接后约 5.31ms／1.91MB；额外解析用于恢复 href，
只在含锚点时执行，成本随正文增长。没有增加轮询、网络或历史扫描。

read/search 的消息表、缓存、去重与响应预算仅在本次调用内存中存在；输出只序列化一次。
正常读操作只允许认证层刷新既有 token 缓存。fetch 显式写私有 staging、一个有界来源／ETag
状态文件和内容文件，不反复重写随历史增长的 JSON。失败不发布最终文件；成功原子建立最终名
后清理本次 staging。没有新增 daemon、索引库或后台任务。

## 尚未证明的范围

未做安装升级或发布验收；当前已安装 helper 尚不包含这些工作区改动。
未验证来宾、跨租户、共享频道、其他用户文件、最小 scopes、实际限流与长期索引延迟、深搜索、
2GiB 大文件及特殊文件系统的硬链接行为。Graph 不是事务快照，跨调用出现新增／编辑仍可能改变
范围。Windows 已做真实单测；真实租户网络抽样在 Linux 执行，频道 hosted 下载有路由单测，
真实下载样本为聊天图片。没有把 fake Graph 的字面检索当作真实 KQL 或索引完整性证明。

## Prerelease 提交准备

2026-11-12：按用户要求提交并准备 `v0.1.23-rc.1`。最新已发布且非草稿的 Release 是
正式版 `v0.1.22`（published_at=2026-11-12T01:58:17Z），因此源码基础版本改为
`v0.1.23`。同步 GitHub main 后基线仍为 `68dba0d7`，无需合并其他改动。
功能源码指纹与上轮验收一致；本轮额外变更为版本号、发布说明和本记录。
提交、标签、CI 与 Release 的最终结果由 GitHub 对应对象记录；不把准备阶段当作已经发布。
