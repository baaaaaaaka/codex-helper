历史阶段记录：本文反映当时的基线、实现与测试结果，不代表当前版本的状态。

# Teams SQLite 与 TUI JSONL 缓存检查

> 这是实施前检查记录。当前改动及测试结果见 SQLITE_JSONL_IMPLEMENTATION.txt；下文的“未修改产品代码”仅描述当时的检查阶段。

检查日期：2026-11-17。上游：`origin/main`，提交 `ecd719f9e839e1f86e07ba6f704545959179044c`。

独立分支：`audit/sqlite-jsonl-20261117`。工作区：`/home/baka/project/codex-helper/.worktrees/audit-sqlite-jsonl-20261117`。

本次为代码检查和隔离样本测量。未修改产品代码，未提交、发布或操作运行中的 Teams helper。保留两个检查探针，便于复现。

## 结论

1. Teams 主业务状态、服务状态、supervisor 配置和 token 已使用 SQLite 字段表，不再把整份业务状态作为 JSON 数据库存储。不过，数据库内仍有 JSON 消息正文，不能声称“SQLite 内完全没有 JSON”。
2. 存在明确的逻辑读放大：发送 token 查找缺少专用索引；TUI 每次读取未变更的 history 缓存仍加载整张来源对应的索引。没有发现这些主路径仍在反复重写整份 JSON 状态的情况。
3. 缓存已经有重要优化，但没有达到极致。小增量预览更新仍复制完整文本和去重集合，重开缓存仍加载全部预览，名称索引变化后仍全文件解析。

## 存储格式核对

| 范围 | 当前实现 | 说明 |
| --- | --- | --- |
| Teams 主状态 | `internal/core/store/schema.go`、`engine_schema.go` | conversations、turns、inbound、outbox、通知、附件引用、source 等使用字段表及子表 |
| 主存储入口 | `internal/cli/teams_newcore_store.go:131` | `openTeamsCoreStore` 调用 `corestore.OpenWith`；旧 state.json 仅用于首次转换判断和迁移 |
| service restart/watchdog | `internal/cli/teams_service_state_sqlite.go` | 按 host 读写具体列，UPSERT 带变更条件 |
| supervisor 状态和进程信息 | `internal/cli/teams_service_supervisor_sqlite.go` | 状态是字段表；args/environment 是子表；身份未变时不重建子表 |
| supervisor 配置 | `internal/cli/teams_service_supervisor_config_sqlite.go` | 配置字段和 environment 子表；environment 相同时跳过重写 |
| 认证配置及 token | `internal/channel/teams/auth_sqlite.go`、`auth_migration.go` | 配置和 token 字段拆列；JSON tags 用于 OAuth 或旧数据导入 |
| 状态发布及 CLI exchange | `internal/cli/teams_newcore_snapshot_sqlite.go`、`teams_exchange_sqlite.go` | 原生字段表；状态发布使用独立小型 SQLite 快照 |
| TUI catalog 与 preview | `internal/codexhistory/persistent_catalog_sqlite.go`、`session_preview_store_sqlite.go` | 元数据、消息、去重信息拆表；源 Codex JSONL 仍是原始数据 |

仍有 JSON 的具体边界：

- **正常运行时，outbox.body 是 Teams 发送 JSON。** `internal/channel/teams/render.go:140` 将 Graph 消息编码为 JSON，放入 Part.Body；engine 把消息正文持久化，发送时由 `internal/core/engine/sender.go:470` 取出。因此，若标准是“SQLite 中任何地方都不存 JSON”，目前不满足。它是单条网络消息的发送载荷，不是把会话状态、队列或数据库整体塞入 JSON blob。字段类型为 TEXT，而不是 SQLite BLOB。
- `schema.go` 中 `body_hash BLOB` 是二进制哈希，不是 JSON。
- `notifications.payload` 名称虽通用，当前旧通知迁移在 `internal/core/migrate/convert.go:1249` 写入的是标题等拼接文本，不能仅凭列名判定其为 JSON。
- `state.json`、旧 service JSON、旧 token JSON 的代码用于兼容迁移；`renderTeamsServiceLocalSupervisorConfig` 的 JSON 是展示输出。Codex 自身的 history/session JSONL 和 Graph/OAuth JSON 也不能等同于 CXP 用 JSON 作为状态数据库。

## 需要优先处理的读放大

### 1. 发送 token 查询随会话历史增长

位置：`internal/core/store/engine_api.go:26`，调用位置 `internal/core/engine/receive.go:834`。

SQL 按 `conv_id` 和 `attempt_token` 查找，按 seq 排序后取一行。当前索引没有 attempt_token；查询计划为：

```text
SEARCH outbox USING INDEX sqlite_autoindex_outbox_2 (conv_id=?)
```

即先定位会话，再逐条检查 token。未命中或靠后的 token 会访问大量已保留历史。带本 helper marker 的消息分类会执行这条查询。主存储保留历史，问题不会随清理自然消失。

本次新增探针的未命中测量：

| 同一会话的 sent 历史 | 每次查询 | 分配 |
| --- | --- | --- |
| 1,000 条 | 0.241 ms | 5,720 B，121 次 |
| 100,000 条 | 15.099 ms | 5,304 B，120 次 |

建议：评估 `(conv_id, attempt_token, seq)` 专用索引，包括 `attempt_token IS NOT NULL` 的部分索引；保留现有排序语义，同时验证新索引的写入成本。此次没有修改 schema。

### 2. 未变更的 history 缓存每次仍加载全量记录

位置：`internal/codexhistory/history_index.go:79`、`persistent_catalog_sqlite.go:443`。

`loadHistoryIndexContext` 先调用 `loadHistory`，后判断源文件是否完全命中。`loadHistory` 已经把来源对应的全部 history_session 行读入 map。即使 history.jsonl 没有变化，每次目录发现仍执行这项工作。

| 不同 session 数量，文件完全未变 | 每次加载 | 分配 |
| --- | --- | --- |
| 1,000 | 0.639 ms | 242,670 B，8,121 次 |
| 100,000 | 61.042 ms | 19,800,560 B，800,636 次 |

这是 SQLite 到 Go 的逻辑读取及对象分配，不代表每次产生同等物理磁盘读取。OS 和 SQLite 页缓存会影响磁盘量。

建议：先读 header/generation，再复用进程内只读索引；变化时更新增量。必须继续处理其他进程更新、文件替换和缓存关闭。

### 3. session_index.jsonl 只有内存完整文件缓存

位置：`internal/codexhistory/thread_name_index.go:37`、`:83`。

文件 key 未变时直接复用 map；一旦发生追加或其他变化，重新从文件头解析全部名称记录。新进程也需全读。建议增加持久 offset 和文件身份校验后的增量解析，保留“最后一个有效非空名称生效”的兼容语义。此项为代码证据，未做独立规模测量。

## 写放大检查

已验证的保护：

- 主 store 对未变更 poll/scan/meta 和无新工作调度不写入。`TestUnchangedPollAndScanWriteNothing` 同时检查 `total_changes()`、另一连接的 `data_version` 及 WAL 大小。
- catalog 元数据更新按发现过程批量事务提交；history 追加只写变化 session；相同输入不重复提交。
- preview 的 SQLite 追加只增加消息和去重增量，没有每次重写旧消息；小增量默认累计到 64 KiB 或 30 秒，在后续读取时判断是否提交。该时间条件不是后台定时刷盘保证。exact hit 不写数据库。
- supervisor 状态 UPSERT 比较字段；身份相同时不重建 args/environment 子表；配置 environment 相同时也跳过子表重写。对应 quiet/rollback 测试通过。

仍存在的开销及边界：

- SQLite 页、索引和 WAL/rollback journal 本身会使物理写入大于业务变化字节。本次没有对物理块设备写入量给出比例。
- `internal/sqlitestate/snapshot.go:13` 每次发布状态时新建数据库、建表、提交并 rename；这属于完整重建小快照。`teams_newcore_state.go:176` 已按提交唤醒、状态比较和自适应 250 ms 至 5 s 间隔限制频率，空闲时不持续写。它服务于跨主机读取隔离，是明确设计成本。
- CLIResult lines、dashboard items、supervisor environment/identity 等集合发生变化时，部分实现删除后重建对应子集合。未变化会跳过，但单项变化也可能写完整子集合；多数属于低频控制面，需要按实际集合大小判断优化价值。
- preview 的数量限制为 1,024 个 session，内存限制为 64 个条目及估算的 16 MiB 内容预算；数据库没有按总字节或单个会话消息数设硬上限。主 Teams 历史按设计保留，不能把 vacuum 当成历史淘汰。

因此，当前不能说“完全没有写放大”；也不能把已存在的页级持久化成本直接判成错误。此次最明确、可测的缺口集中在读放大和内存复制，而非整库重写。

## TUI 缓存效率

目录入口是 `internal/cli/history.go:45` 到 `DiscoverProjectSummariesContext`，不是每次都先完整解析所有会话。summary 每个源文件最多读取 128 KiB，找到首条 prompt 后提前退出；完整 catalog 命中时可直接复用。preview 在选择会话时准备。

本机 Linux amd64、Ryzen 9 7950X 的现有基准，5 次定长采样：

| 工作负载 | 每次耗时 | 每次分配 |
| --- | --- | --- |
| 8,800 条可见消息，进程内 preview 热命中 | 0.0288 ms | 4,801 B，24 次 |
| 相同预览，已有磁盘缓存但重开连接和进程内状态 | 21.432 ms | 22,751,363 B，159,100 次 |
| 相同预览，追加一小条并强制立即落库 | 7.116 ms | 6,408,094 B，432 次 |
| 64 MiB transcript，开头已有 meta 和 prompt 的 summary | 0.0127 ms | 6,984 B，63 次 |

注意：cold-open 不是首次从 JSONL 创建缓存；small-append 基准把 flush delay 设为零，包含追加文件和强制落库成本，不能当作默认每次刷新耗时。summary 基准的 prompt 位于开头，不能代表所有文件布局。

明确的后续优化点：

1. `session_preview_store_sqlite.go:144` 克隆整个 seen 集合；`:156` 复制已有 messages，`:157` 拼接完整预览文本；`:427` 再遍历整个 seen 集合计算差量。源 JSONL 和 SQLite 写入虽是增量，CPU 和分配仍随已缓存历史增长。可考虑分块文本及消息、解析时直接记录 seen delta，并保留失败隔离和并发 CAS。
2. `:765` 磁盘命中仍读出整个会话的可见消息，`:123` 随后还预读全部去重状态。仅展示文本不需要立即加载所有去重信息，可延迟到首次追加。
3. 未完成一行时会走 uncached 路径；preview 可能全量重读原文件。应保留完整边界前的缓存，仅处理或延后未完成尾部。此项是条件路径检查，未量化实际触发频率。
4. 延迟落库期间，append base 使用上次持久化位置，连续更新可能重复解析尚未落库的尾段。这通常受刷新阈值控制，但不是严格“每次只处理最新追加”。
5. 目录发现仍 WalkDir、stat 每个文件、分组排序；冷启动已有 catalog 时还有逐文件元数据查询。可评估批量元数据加载或目录增量发现，但不能在跨进程和网络文件系统场景中只依赖文件通知。

## 验证与复现

所有命令在独立工作区执行，TMPDIR 和 GOCACHE 指向预先创建并检查可写的 `.audit-state/tmp` 与 `.audit-state/go-build`；未使用 GPU。以下省略相同的环境前缀。

```sh
go test ./internal/core/store ./internal/codexhistory -count=1
go test ./internal/cli ./internal/channel/teams -run '^(Test(ServiceSQLite|SupervisorSQLite|AuthSQLite|LoadHistoryTuiProjectsUsesBoundedMetadataColdCache|LoadHistoryTuiProjectsFiltersDiscoveredTempProjects))' -count=1 -v
go test ./internal/core/store -run '^TestAuditTokenLookupPlan$' -v -bench '^BenchmarkAuditTokenLookup$' -benchmem -benchtime=10x -count=1
go test ./internal/codexhistory -run '^$' -bench '^BenchmarkAuditUnchangedHistory$' -benchmem -benchtime=5x -count=1
go test ./internal/codexhistory -run '^$' -bench 'Benchmark(SessionSummaryLargeTranscript|SessionPreviewSQLite(WarmLoad|ColdOpen|SmallAppend)LargeVisible)$' -benchmem -benchtime=5x -count=1
git diff --check
```

以上均通过。前两个包完整测试用时分别约 44.9 s、2.24 s；service/supervisor/auth 及 TUI 所选测试实际运行并通过。额外探针文件：`internal/core/store/audit_token_lookup_test.go`、`internal/codexhistory/audit_history_read_test.go`。

本次没有运行生产数据回放、原生 Windows、真实 NFS、块设备 I/O 追踪或整套 CI/E2E，也没有访问当前 helper 的活跃数据库。所报耗时为单机短基准，用于验证成本随规模变化，不是生产延迟承诺。没有遇到阻止完成检查的沙箱、依赖、网络或目标机器错误。
