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
| A9 | `GET /me/chats` with `$expand=lastMessagePreview` and `$orderby=lastMessagePreview/createdDateTime desc` | **Unmodeled** (answers `unmodeled`) | The new core's activity scan (CLEANUP-PLAN §8.2) needs it; the fake must model it, with preview latency, before the new core runs these scenarios | IDLE, PARK, INBOX (new core) |
| A10 | Throttling: 429 with `Retry-After` per account, applies to reads and writes alike | Only when a scenario injects a fault; no ambient throttling | Idle cost scenarios count requests but nothing pushes back | IDLE, INBOX |
| A11 | eTags on drive items are opaque `"{GUID},n"` strings | Fixed GUID | Upload/patch flows (not covered yet) | none yet |
| A12 | A message posted "as another helper on the same account" is indistinguishable from the owner's own message except for content | Same `from.user`, origin recorded only in the fake | BREAKER | BREAKER |

## Not modelled at all

- Channel messages, chat creation by other users, message deletion and
  soft-delete tombstones, edits by the user (except `EditAsUser`),
  reactions, read receipts and system events beyond `PostSystemEvent`.
- Outbound file upload (`helper file`): drive upload sessions are not
  implemented, so R-37's outbound half is untested.
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
| C4 | `SurviveHelper` keeps a turn running after stdin EOF and lets it finish (persisted, rollout written). Whether real `codex app-server` finishes an in-flight turn after its client dies, or drops it, is **unverified** | B-5 chat X covers the "finished while the helper was dead" case; if real Codex always drops the turn, chat X degenerates to chat Y |
