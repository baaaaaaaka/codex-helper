# Graph 附件访问补充实验

2026-11-12 03:26 UTC，以当前账户的 read auth 做独立只读实验。
沿用私有认证库副本；没有修改在线配置、授权或 helper，也没有发送消息。
本轮共 10 次 HTTP GET：8 次 Graph、2 次跟随 Graph 返回的 SharePoint 下载地址。
从最近聊天的两页共 89 条消息中，选择一个内嵌图片和一个文件引用。
不保存消息正文、文件内容、原始资源 ID、下载 URL 或 token。

## 实测

| 对象／操作 | 结果 |
| --- | --- |
| 图片 `.../messages/{id}/hostedContents/{contentId}` 元数据 | 200 |
| 图片 `.../hostedContents/{contentId}/$value` | 200、`image/jpeg`，实际读取 4097 字节后停止 |
| 文件 `.../shares/{encodedUrl}/driveItem` 元数据 | 200，识别为文件，大小 28884 字节，取得 driveId/itemId |
| `/drives/{driveId}/items/{itemId}/content` | 302 到 SharePoint HTTPS 临时下载地址 |
| 跟随上述地址，不携带 Authorization，`Range: bytes=0-4095` | 206、4096 字节，`Content-Range: bytes 0-4095/28884` |
| `/shares/{encodedUrl}/driveItem/content` | 同样 302；跟随后 206，两条路径的样本 SHA-256 相同 |

文件响应类型为 `application/x-zip-compressed`。只验证下载路径及内容样本，
没有完整下载或验证整个压缩包。该样本不能证明其他用户所有文件均可访问。
图片请求虽然带 Range，服务器仍返回 200 而非 206，必须用客户端字节上限控制读取。

## 能力与权限边界

- 普通文件附件通常是消息 `attachments` 中的 `reference`，contentUrl 指向文件；
  文件内容通过 OneDrive／SharePoint 的 DriveItem API 读取。
- 内嵌图片等消息托管内容走 hostedContents；列举或读取元数据不等于读取二进制，
  二进制使用 `/$value`。频道主帖和回复有各自的 hostedContents 路由。
- 卡片、代码片段、转发引用等附件可能直接携带 JSON/content，并非都对应可下载文件。
- 能读消息不等于能读其文件：Graph scopes 与目标文件访问权都必须满足。
  当前 read token 含 Files.Read、Files.Read.All、Sites.Read.All 等多项权限，
  本次不证明最小权限，也没有扩大授权或设置兑换共享链接的 Prefer 头。
- Graph 内容下载的 302 是正常路径；下载地址本身带临时访问能力，不应输出到模型或日志，
  跟随时不携带 Graph Bearer token。本次只跟随 Graph 返回的 SharePoint HTTPS 地址。

官方依据：

- [消息附件类型](https://learn.microsoft.com/en-us/graph/api/resources/chatmessageattachment?view=graph-rest-1.0)
- [读取 hosted content 与二进制](https://learn.microsoft.com/en-us/graph/api/chatmessagehostedcontent-get?view=graph-rest-1.0)
- [下载 DriveItem 内容、权限与重定向](https://learn.microsoft.com/en-us/graph/api/driveitem-get-content?view=graph-rest-1.0)

## 对 CXP 的含义

源码现有 `internal/channel/teams/files.go` 的 `Adapter.FetchFile` 已有聊天 hosted、
SharePoint share 和自有 drive 文件下载分支；`receive.go` 提取入站文件引用。
这说明不需要重新设计一套文件传输机制，但不代表尚未实现的通用 Graph read 命令已经开放下载。
频道 hosted 内容需要保留 team/channel/root/reply 路由信息，不能只复用聊天 hosted 引用格式。

通用接口宜默认列出附件类型、名称、大小与稳定引用，由 agent 显式请求下载所需附件；
文件按独立大小、超时和请求预算处理，不因读取一段对话就自动下载所有文件。
本次没有修改产品代码，未验证频道 hosted 图片、音视频、其他用户文件、跨租户或超大文件。
