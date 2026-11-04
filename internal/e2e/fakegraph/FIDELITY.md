# Fake Graph fidelity gaps

The E2E scenarios are only as good as the fakes they talk to. This file
lists where `fakegraph` (and, briefly, the fake Codex app-server) is
**known** to differ from the real service, and which behaviours are
**assumed** without having been checked against real Microsoft Graph.
A scenario verdict that depends on an assumed row is evidence about the
helper under that assumption, not about production.

Rules the fake enforces strictly (the request is answered with a
`graph-contract` violation and the scenario fails) are listed in
`server.go` (`violationResponse(rec, "graph-contract", ...)`). Routes and
query options the fake does not model at all answer with an `unmodeled`
violation instead of guessing.

## Verified against real Graph or real Teams data

| Behaviour | Source |
|---|---|
| `GET /chats/{id}/messages` supports only `$top` (1..50), `$skiptoken`, `$orderby` (`lastModifiedDateTime desc` or `createdDateTime desc`) and a matching `$filter` (`lastModifiedDateTime gt/lt`, `createdDateTime lt`); results are newest first with an `@odata.nextLink` | Graph docs for *List messages in a chat*; old-core request shapes |
| Message ids are epoch-millisecond strings | real store snapshots |
| Online meetings: `POST /me/onlineMeetings` and `/createOrGet` (requires `externalId`) create a meeting chat whose topic is the subject | Graph docs; old-core registry code |
| `<at id="n">` tags in the body must match `mentions[]` entries one to one | Graph docs (400 otherwise) |
| Shares: `/shares/u!{unpadded base64url(url)}/driveItem[/content]` | Graph docs |

## Assumed, not yet checked (read-only probes would settle them)

| # | Behaviour | What the fake does | Why it matters | Scenarios affected |
|---|---|---|---|---|
| A1 | HTML comments in a posted body (`<!-- codex-helper-outbox:<id> -->`) come back unchanged on GET | Kept. `Options.StripHTMLComments` can drop them, off by default | The whole provenance design (CLEANUP-PLAN §8.3/8.3b) and R-7 depend on it | BREAKER (simulates a stripped marker explicitly), every R-7/R-2 check |
| A2 | `lastModifiedDateTime` changes on edit/PATCH and on nothing else (no reactions, no read receipts) | Changes only on PATCH | Old core cursors on it; a reaction bump would re-deliver | R-4, R-6, freeze-notice PATCH |
| A3 | Ordering and visibility are immediate and strictly monotonic: a message is listed as soon as the POST returns, and timestamps never tie or go backwards | Monotonic 1 ms stamps, no replication lag | Real Graph is eventually consistent; the old core keeps a 2-minute overlap window because of this. Ordering findings (M1 F2) assume no lag | R-4, R-6, B-5 |
| A4 | `replyWithQuote` exists on v1.0 and quotes exactly the given `messageIds` | Implemented as POST + `QuotedIDs` | R-43 ACK check | R-43 |
| A5 | `hostedContents/{id}/$value` and `/shares/.../driveItem/content` return 200 with the bytes | 200 with bytes (real Graph may 302 to a pre-authenticated URL) | The old core fetches `/content` twice when it gets 200 directly; with a 302 the cost and failure modes differ | R-37 |
| A6 | `GET /chats/{id}/members` reflects a newly added member immediately | Immediate | Group-chat audience switch (R-43); the old core caches members 5 min | R-43 |
| A7 | The message body limit is ~100 KB of HTML (`maxMessageContentBytes` = 102289) and the error is a 413/400 | Rejects above the limit | Chunking target (R-36: 32 KiB parts) is far below, so only a regression would hit it | R-36 |
| A8 | A meeting chat created with `createOrGet` is listed by `GET /me/chats` and readable by the owner at once | Yes | Registry/inbox scenarios | INBOX |
| A9 | `GET /me/chats` with `$expand=lastMessagePreview` and `$orderby=lastMessagePreview/createdDateTime desc` | Modelled (M3). `$expand` accepts only `lastMessagePreview`, `$orderby` only `lastMessagePreview/createdDateTime desc` (anything else is `graph-contract`); `$filter` stays `unmodeled`. Each chat carries `lastMessagePreview@odata.context` and a `chatMessageInfo` (`id`, `createdDateTime`, `isDeleted`, `messageType`, `eventDetail`, `body`, `from`) or explicit `null`; chats without a message sort last; `@odata.nextLink` keeps `$expand`/`$orderby`/`$top`. `Options.PreviewLag` / `SetPreviewLag` hide messages younger than the lag from the preview and the ordering (default 0). Assumed: the preview body is the full stored body (real Graph may shorten it), and the real preview latency is unknown | The new core's activity scan (CLEANUP-PLAN §8.2) needs it; scenarios should set a non-zero lag to prove they do not rely on an instant preview | IDLE, PARK, INBOX (new core) |
| A10 | Throttling: 429 with `Retry-After` per account, applies to reads and writes alike | Only when a scenario injects a fault; no ambient throttling | Idle cost scenarios count requests but nothing pushes back | IDLE, INBOX |
| A11 | eTags on drive items are opaque `"{GUID},n"` strings | Fixed GUID | Upload/patch flows (not covered yet) | none yet |
| A12 | A message posted "as another helper on the same account" is indistinguishable from the owner's own message except for content | Same `from.user`, origin recorded only in the fake | BREAKER | BREAKER |
| A13 | Outbound files (M3): `PUT /me/drive/root:/{path}:/content` (default conflict behaviour replace; `?@microsoft.graph.conflictBehavior=fail` gives 409 `nameAlreadyExists`; 201 new / 200 replaced, body a driveItem), `POST .../{path}:/createUploadSession` (body `{"item":{...}}` with optional conflict behaviour and a `name` that must match the path; 200 with `uploadUrl` on the SharePoint host, `expirationDateTime`, `nextExpectedRanges ["0-"]`), fragment `PUT uploadUrl` with `Content-Range` and **no** `Authorization` (non-final fragments 320 KiB-aligned, < 60 MiB, in order or 416; 202 + `nextExpectedRanges`, last fragment 201/200 + driveItem), `GET uploadUrl` (status; 404 once complete), `DELETE uploadUrl`, `GET /me/drive/items/{id}[?$select]` and `/content` | Modelled after the Graph docs for *upload small files* / *createUploadSession*. Assumed: `webDavUrl` only on `$select` and equal to `webUrl`; the eTag is `"{GUID},n"` with a per-item GUID (A11) that changes neither on replace nor across versions; a 200 from `/content` instead of a 302 (A5); `rename` conflict behaviour is `unmodeled` | R-37-out |
| A14 | A posted reference attachment must point at a drive item (`contentUrl` = its `webUrl`/`webDavUrl`), carry the GUID of that item's eTag as `id`, have a `name`, and be referenced by an `<attachment id>` tag | Rejected as `graph-contract` otherwise. Real Teams' exact checks are not documented; the eTag-GUID rule is what Teams clients do | R-37-out |
| A15 | Teams Workflow webhook (M3): pre-signed `https://<env>.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/<id>/triggers/manual/paths/invoke?api-version=1&sp=...&sv=1.0&sig=...`; POST `application/json` `{"type":"message","attachments":[{"contentType":"application/vnd.microsoft.card.adaptive","content":{"type":"AdaptiveCard","version":...}}]}` up to 28 KB, no `Authorization` | 202 Accepted with an empty body; wrong `sig` 401; unknown workflow 404; anything else `workflow-contract`. Faults by `Host: fakegraph.WorkflowHost`. The card is recorded, not posted into a chat | NOTIFY |

## Not modelled at all

- Channel messages, chat creation by other users, message deletion and
  soft-delete tombstones, edits by the user (except `EditAsUser`),
  reactions, read receipts and system events beyond `PostSystemEvent`.
- Where a Workflow card lands (the flow posts it as the Workflows bot);
  the fake only records the cards it accepted.
- Token lifetime: tokens never expire unless a scenario calls
  `Env.ExpireToken`.

## Fake Codex app-server

Shapes follow `codex_app_server_protocol.v2.schemas.json` (real Codex
0.145-0.166): `thread/*`, `turn/start|interrupt`, item notifications,
`Turn{id,status,items,startedAt,completedAt,error}`, `TurnError`
with `codexErrorInfo`, and `clientUserMessageId` echoed as `clientId`
(thread/read) and `client_id` (rollout `item_completed` UserMessage).
Known gaps:

| # | Gap | Effect |
|---|---|---|
| C1 | Turns finish in about a second unless scripted to block | Races between the turn-completion path and the linked-transcript sync are more likely than in real use (see the "transcript observation conflict" lines in SUT output) |
| C2 | No tool calls, no command output, no subagents; commentary is one item | Progress-rendering guarantees (F-TRN-15) are out of scope |
| C3 | Rollouts written by the fake are record-for-record copies of real 2026-11 rollouts for the records it writes, but it never writes compaction, `turn_context` changes mid-turn or images | Linked-transcript mirroring is exercised only with plain text turns |
| C4 | **Verified (M3 probe, real Codex 0.166.0 and 0.145.0): an in-flight turn never survives its client.** On stdin EOF, and when the client process is SIGKILLed (which is the same EOF to Codex), the app-server cancels the model stream within ~1-4 ms, writes `turn_aborted` to the rollout, persists the turn as `interrupted`, and exits ~5 s later. When the whole process group is SIGKILLed, the rollout ends after the user message (no `turn_aborted`, no `task_complete`); a fresh app-server's `thread/read` still reports the turn `interrupted`, and the next `turn/start` on the thread works (no stale writer lock). The fake's default (abandon on EOF -> interrupted + `turn_aborted`) matches. `SurviveHelper` is therefore **counterfactual**: it models a Codex that does not exist in 0.145/0.166 | B-5 chat X (the only `SurviveHelper` user) tests a situation production cannot reach with the helper's stdio-piped app-server; its guarantee is moot until a detached runner exists |
| C5 | **Verified (0.166.0): another process holding the thread.** Codex 0.166 keeps a per-thread writer lock for as long as a process has the thread loaded (running *or idle*). A second app-server's `thread/resume` fails with JSON-RPC `{"code":-32600,"message":"thread <id> already has an active writer"}`; a `turn/start` without a successful resume fails with `{"code":-32600,"message":"thread not found: <id>"}`. After the owner exits, resume and turn/start work. 0.145.0 had **no** lock: two processes could run turns on one thread at once and interleave the rollout. The fake rejects at `turn/start` (its brain is consulted only there) with the real code and text (`FailActiveWriter`) | Active-writer scenarios see the real error text, but on `turn/start` instead of `thread/resume` |
| C6 | **Verified (0.166.0): `turn/start` on a thread whose turn is running in the same process is accepted and steered** into the running turn: the response carries the running turn's id (`status inProgress`, `itemsView notLoaded`), the extra `userMessage` (with its `clientId`) joins that turn, and the one final answer covers both inputs. (0.145.0 instead started a new turn.) The fake now steers the same way (it used to reject with "already has an active turn"); the brain is not told about the steered input | A helper that sends two prompts to one thread back to back gets one turn and one answer |
| C7 | **Verified (0.145.0 and 0.166.0): provider capacity errors.** A provider HTTP 503 `server_is_overloaded` and an SSE `response.failed` with `server_is_overloaded` both end the turn `failed` with `error {"message":"Selected model is at capacity. Please try a different model.","codexErrorInfo":"serverOverloaded","additionalDetails":null,"misalignment":null}`; a 429 (after Codex's own retries, here 0) gives `"exceeded retry limit, last status: 429 Too Many Requests"` with `codexErrorInfo {"responseTooManyFailedAttempts":{"httpStatusCode":429}}`. Each failure is preceded by an `error` notification `{error, willRetry:false, threadId, turnId}`. The fake matches (`FailCapacity`, new `FailRateLimited`) and now sends the `error` notification | Capacity-retry scenarios exercise the real shapes |
| C8 | **Verified (0.166.0): `turn/completed` carries the turn's agent messages** as `items` with `itemsView: "summary"` (0.145.0: `items: []`, `itemsView: "notLoaded"`). The fake now sends the final `agentMessage` there too | none (the helpers read the final from `item/completed`) |
| C9 | **Verified (0.145.0 and 0.166.0): `clientUserMessageId` round trip.** `turn/start` `clientUserMessageId` comes back as `clientId` on the `userMessage` item in `item/started`/`item/completed` and in `thread/read` (`includeTurns`), and as `client_id` on the rollout `event_msg item_completed UserMessage` record, also for interrupted turns. The fake matches | B-5 key check |

The probe that produced C4-C9 is committed as `internal/e2e/realcodex` and is
opt-in: `E2E_REAL_CODEX=/path/to/codex go test -v -run RealCodex
./internal/e2e/realcodex/` inside an offline container. It writes a throwaway
`CODEX_HOME` whose only model provider is a loopback fake Responses API
(retries off) and logs every observation as an `EVIDENCE` line.
