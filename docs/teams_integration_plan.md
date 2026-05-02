# Teams Integration Plan

Status: active product and architecture plan; proof of concept succeeded, core implementation is in progress, and later product requirements are tracked here before implementation.

Implementation tracker: `docs/teams_integration_execution_plan.md`.

## Current Findings

- Azure Bot creation is blocked by the organization.
- Teams channel writes require permissions that are not currently available.
- Slack app creation is blocked by IT.
- Slack and other chat transports remain future fallback options, not part of
  the active implementation path, because they would need separate app,
  permission, security, and state-mapping decisions.
- Microsoft Graph delegated chat access is available through Teams-compatible public clients.
- A single-member Teams group chat can be created and used as a private Codex conversation surface.
- A control chat plus one on-demand Teams work chat per Codex session is the
  current best fit for thread-like session management under the observed
  permissions.
- Explicit outbound file transfer is feasible by uploading to OneDrive/SharePoint
  and sending a Teams reference attachment. It requires a separate opt-in
  file-write auth profile.
- Live chat message-size testing found a hard HTML body boundary around
  `102 KiB`; production sends must chunk well below that.
- Live self-mention testing confirmed the logged-in user can be notified by a
  Teams self-mention built with Graph `mentions`.
- Official Codex 0.125.0 source shows `codex exec` is implemented on top of Codex's app-server thread/turn protocol, so app-server is the best candidate for lower latency and richer session control.
- Codex 0.125.0 sets `prompt_cache_key` from the session `conversation_id` for normal responses requests and reports `cached_input_tokens` in exec JSON usage. Stable `thread_id`, stable session configuration, and native thread resume remain central to maximizing prompt/KV cache reuse, but cache behavior must be measured from usage events rather than inferred from Teams/helper metadata.
- `codex mcp-server` is not the best primary control plane for this feature: `codex-reply` looks up an already-loaded thread inside the current MCP server process, which makes cross-process restart recovery weaker than app-server `thread/resume`.

## Goal

Add a Teams mode to `codex-helper` so the user can create and manage Codex conversations from Microsoft Teams while preserving existing `codex-helper` behavior, install paths, proxy behavior, yolo mode, history behavior, and upgrade flow.

## Recommended Shape

- Use Teams as a transport only. Keep Codex session ownership, state, locking, and recovery inside `codex-helper`.
- Use one control chat per `machine_id + Teams account + helper profile` as a
  dashboard and command surface. Its title must clearly identify the helper
  profile, host, and that it is the machine's main/control chat. The default
  title should use an obvious presentation marker equivalent to `[control]`;
  installations may choose an emoji marker, but durable identity must come from
  stored ids, never from the title text.
- Use one single-member Teams work chat per Codex session, created on demand
  when the user asks for Teams access to that session.
- Session chat titles must be recognizable from Teams search and the chat list:
  include a work marker equivalent to `[work]`, the session number/id, and a
  sanitized topic or cwd basename. Do not put full local paths, raw long
  prompts, or sensitive prompt text in chat titles by default.
- Treat helper messages as delegated user messages, not bot messages.
- Make control-chat workflows helper-handled and cheap. Listing workspaces,
  listing sessions, selecting by number, creating directories, and creating
  sessions should not invoke Codex.
- Keep session chats helper-prefixed for helper actions, so Teams native slash
  commands do not interfere:
  - `helper status` / `!status`
  - `helper close` / `!close`
  - `helper retry <turn-id>` / `!retry <turn-id>`
  - `helper cancel <turn-id>` / `!cancel <turn-id>`
- Keep service-level controls such as `/pause`, `/resume`, and `/drain` in the
  control chat or local CLI. They are high-impact operations and should not be
  accepted as accidental session-chat text.
- Treat normal messages in a session chat as Codex input. `help` may be handled
  locally as a safe affordance; potentially destructive or ambiguous actions
  such as `status`, `close`, `retry`, `cancel`, `file`, and `rename` require a
  `helper ...` or `!` prefix.
- Use "close" for stopping a session. The tool should not imply it can archive or remove the Teams chat itself.
- Make the control chat the dashboard for online/offline state, active sessions, queue depth, active turn, last poll, last error, and Codex readiness.
- Return an immediate ACK only after a Teams prompt has been durably persisted,
  a turn has been created, and the helper has confirmed it will run Codex. The
  ACK is a durable outbox message before the final/error output, should include
  the session id and turn id, stay short, and avoid owner mentions. Do not ACK
  control commands, unsupported input, duplicate inbound messages, historical
  replay, local catch-up, or turns initiated directly from local Codex.

## Product Requirements Pulled Forward

- Multiple machines may run `codex-helper`. Each `machine_id + Teams account +
  helper profile` owns one main Teams control chat, titled with a stable machine
  label, and never competes for another identity's control chat. Rebinding or
  renaming a machine identity must be explicit.
- The control chat supports numbered navigation:
  - list known work directories with stable short numbers
  - list Codex sessions under a selected directory with stable short numbers
  - open an existing session by number
  - create a new session in an existing or newly-created directory
  - show enough title/cwd/time/thread data to choose without invoking Codex
- Numbered selections are scoped to the current dashboard view. A bare `1`
  means "the first item in the latest control-chat menu"; in work chats, `1`
  is always Codex input. Expired or ambiguous menu selections should ask the
  user to refresh the view instead of guessing.
- Control-chat commands should be English natural commands first, for example
  `projects`, `project 1`, `sessions`, `open 2`, `new ...`, `continue ...`,
  `mkdir ...`, `details ...`, and `status`. Bare numbers remain scoped to the
  current control dashboard view. Legacy `/...`, `!`, `cx ...`, and
  `codex ...` forms may remain compatibility aliases but should not be the
  primary user-facing path.
- Every active Codex session should have a corresponding Teams work chat when
  the user asks for Teams access. Existing historical sessions are not mirrored
  into Teams by default.
- If the user asks to publish an existing Codex session to Teams and no Teams
  chat exists yet, create one and import the existing transcript in order.
  Label user vs assistant turns clearly. The first imported message should be
  the session title unless the session was originally created inside Teams.
- The helper must detect sessions created or resumed outside Teams and reconcile
  them into the control-chat index. It should not spam Teams with historical
  chats unless the user explicitly opens or publishes them.
- Mixed use is expected. A user may talk in Teams and also use local Codex.
  The helper must detect missing Codex records, send them to Teams in the
  correct order when appropriate, and avoid duplicating or reordering messages.
- Local Codex discovery and catch-up should feel near-real-time while the
  service is active, but correctness must not depend on a watcher firing. Use a
  startup/recovery full scan, lightweight refreshes on dashboard commands, and a
  periodic background scan. Reasonable default targets are within one poll
  interval for linked active sessions and within about a minute for dashboard
  discovery; a missed refresh may delay display, but must not lose or duplicate
  transcript records.
- The helper should remind the owner about important events with self-mentions:
  long-running turn completion, action-required recovery, and selected service
  failures. It should not mention on ACKs, routine status, or imported history.
- Codex can return files or images through a helper-readable artifact manifest.
  The helper is responsible for validating, hashing, staging, uploading, and
  sending the artifacts. The model's visible text should remain visible to the
  user; do not hide model output as a fragile filtering mechanism.
- Artifact uploads should live under a controlled helper/Teams outbound root,
  use collision-resistant names, and avoid cluttering normal OneDrive usage.
  The local default root should stay under the user cache/state area, while the
  remote default upload folder should be the Teams-compatible
  `Microsoft Teams Chat Files` folder unless the operator explicitly overrides
  it. Generated upload names should include a stable session/turn prefix plus a
  content digest or other collision-resistant suffix.
- Rich Teams formatting is useful but secondary. The renderer should make user
  turns, assistant replies, code blocks, tool/status text, and artifact links
  easy to distinguish while staying within Teams HTML limits.
- Formatting must describe visible transcript categories only. Hidden model
  reasoning, helper routing metadata, Teams ids, and checkpoint data must not be
  injected into the user-visible Codex prompt or Teams transcript.

## Core Design

- Introduce durable state before expanding the transport layer:
  - `SessionContext`
  - `Turn`
  - `OutboxMessage`
  - schema version
  - turn ids
  - state transitions
- Use file locks, session locks, and atomic writes.
- Store durable state under an application state directory, not an ephemeral cache directory.
- Migrate existing registry data into the durable state store before enabling
  machine dashboards, historical import, ACKs, or notification behavior. The
  old registry may remain as a projection or migration source only; it must not
  continue to be the source of truth for control chats, session/chat mappings,
  seen message ids, or sent message ids.
- Schema v2 should establish the stable semantic backbone in one migration:
  machine identity, workspace identity, session/work-chat/thread mapping, Teams
  inbound ledger, Codex transcript ledger, Teams outbox ledger, Graph ingestion
  checkpoint, import/catch-up checkpoints, per-chat sequence, and rate-limit
  retry state.
- Persist inbound Teams events before dispatch.
- Persist Codex turn state before and after execution.
- Persist outbound messages before sending.
- Render, split, validate, and persist all parts of a long Teams message before
  sending the first part. Ordering must be driven by durable per-chat sequence,
  not timestamps.
- Recover pending work on startup.
- Do not automatically replay a turn if the process died after Codex may have executed but before the result was saved; surface it as interrupted and require explicit retry.
- Automatic recovery should restart polling, reacquire owner heartbeat, flush
  unsent outbox records, resume known Codex threads, and recover queued work
  that was durably accepted but not started. It must not silently rerun an
  ambiguous Codex turn; those require explicit `/retry` or a local recovery
  command after the owner is notified.
- Keep state schema migration explicit and forward-compatible. New fields must
  be optional until migrated, unknown future fields must not corrupt old state,
  and semantic flags such as notification intent or import origin must not be
  inferred from rendered message text.
- Store stable ids for machine, workspace, session, Teams chat, Codex thread,
  imported transcript position, and outbox part number so future migrations can
  rebuild indexes without changing user-visible history.
- Accept at-least-once Teams send semantics. If Graph accepts a message and the
  helper crashes before marking the outbox item sent, the message may be resent.
  The design should use outbox sequence, rendered body hashes, send leases, and
  diagnostics to make that window visible, but it must never rerun Codex just
  to repair a Teams send marker.

## Teams Transport

- Keep Graph auth, polling, rendering, and send logic in a Teams-specific package.
- Use least-privilege delegated scopes where possible.
- Redact tokens, URLs, message bodies, and full local paths from logs.
- Handle 401 by refreshing tokens.
- Handle 429 and 5xx with bounded exponential backoff and jitter.
- Keep same-chat processing serialized.
- Use Graph batch only where it helps and continue respecting per-subrequest throttling.
- Treat Teams chat message size as a hard transport limit, not a formatting
  detail. Live testing on 2026-04-30 found `POST /chats/{id}/messages`
  succeeds with `102,289` bytes of HTML `body.content` and fails at
  `102,290` bytes with `HTTP 400 BadRequest`.
- Split outbound text by final rendered Teams HTML byte length, not raw rune
  count. Use a conservative chunk target around `72 KiB` and keep each final
  message below an `80 KiB` safety line so HTML escaping, part labels, and
  tenant/client behavior changes do not make sends fail.
- Label multi-message output as `part i/n` and preserve outbox ordering so a
  retry or restart cannot reorder a long Codex answer.
- Apply per-chat rate limiting. A long `Retry-After` for one chat must not
  block unrelated chats, and a retry must not let later messages overtake
  earlier ACKs, imports, chunks, artifact links, or notifications in the same
  chat.
- A live self-mention probe on 2026-04-30 confirmed that Graph accepts a
  normal chat message `mentions` payload for the logged-in user and the Teams
  client can notify the user. Use this only as an owner-notification primitive;
  do not let Codex-supplied text create arbitrary mentions.

## Codex Runner

- Do not make Teams mode bypass existing `codex-helper` launch behavior.
- Extract a shared headless Codex runner from existing command paths.
- Preserve:
  - managed Codex install behavior
  - proxy setup and teardown
  - yolo mode
  - effective path resolution
  - root and sudo identity handling
  - `CODEX_HOME` / `CODEX_DIR`
  - self-update and upgrade guards
  - proxy-health termination behavior
- Introduce an internal runner interface before expanding Teams behavior:
  - `StartThread`
  - `ResumeThread`
  - `StartTurn`
  - `InterruptTurn`
  - `ReadThread`
  - `ListThreads`
- Implement `ExecRunner` first as the stable fallback:
  - use `codex exec --json` for new sessions
  - use `codex exec resume --json <thread-id>` for existing sessions
  - capture `thread.started`, `turn.started`, final assistant messages, failures, and token usage
  - do not rely on `codex exec --json` exposing a turn id; Codex 0.125.0's exec `turn.started` event has no turn-id payload
  - store and resume by exact `thread_id`; never automate with `--last`
- Treat `ReadThread` and `ListThreads` as optional capabilities. The current
  reliable transcript source for publishing and mixed-use catch-up is local
  Codex JSONL history discovered through the existing history code. Runner
  read/list data may enrich metadata, but must not replace local transcript
  ordering unless it exposes stable ordered transcript item ids.
- Implement `AppServerRunner` as the preferred low-latency path behind an experimental gate:
  - start and supervise `codex app-server`
  - initialize the newline-delimited app-server protocol
  - use `thread/start`, `thread/resume`, `turn/start`, `turn/interrupt`, `thread/read`, and `thread/list`
  - align request/response structs with the generated Codex app-server schema for the installed Codex version; do not pass CLI args through as unknown protocol fields
  - treat `turn/completed` items as potentially empty and backfill via `thread/read includeTurns=true` or local JSONL before publishing final transcript output
  - probe protocol compatibility at startup and automatically fall back to `ExecRunner` if unsupported
  - keep one warm app-server per compatible Codex environment instead of starting a new Codex process for every Teams message
- Keep `codex mcp-server` as a compatibility/reference path only. It is useful when another MCP client wants to call Codex as a tool, but it does not provide the best durable UI/session-management surface for Teams mode.
- Directly invoking the native Codex binary can be considered later as a small startup optimization, but only after preserving the Node wrapper's vendor `PATH` setup and managed-install environment behavior.
- Do not implement this feature by calling OpenAI APIs directly. That would bypass Codex's sandboxing, approval model, config loading, MCP/tools, persistence, and upgrade behavior.

## Codex Session Recovery

- Persist the Codex `thread_id` as the durable session handle.
- Persist the Codex session file/fingerprint and stable transcript item
  checkpoint when a Teams work chat is linked to a local Codex session.
- Persist the Teams chat id, latest known Codex turn id, Codex version, cwd, effective Codex home, profile, model, sandbox, proxy/yolo mode, and runner kind.
- On startup:
  - start or reconnect the selected runner
  - resume each active session by exact `thread_id`
  - use `thread/list` and local Codex session files only as reconciliation or diagnostic fallbacks
  - read final turn state before sending duplicate output
- Treat a process death during an active turn as ambiguous unless Codex state proves completion or failure.
- Do not silently replay ambiguous turns. Mark them interrupted and require `/retry <turn-id>` or an explicit recovery command.
- Store outbound Teams messages separately from Codex completion state so a restart can resend unsent output without rerunning Codex.
- Maintain three ledgers for mixed use: Teams inbound events, Codex transcript
  items, and Teams outbox messages. Use stable transcript item ids for
  import/catch-up dedupe; timestamp or content hashes are fallback diagnostics,
  not primary identity.
- Do not let local Codex catch-up jump ahead of pending Teams-origin ACK/final
  outbox messages in the same work chat. Catch-up appends only; it never edits
  prior Teams messages.
- Store notification intent separately from message body in the outbox, for
  example "mention owner on send", so retries, schema migrations, and future
  renderers do not have to infer reminder behavior from HTML text.
- Use owner self-mentions for attention-worthy events: completed long-running
  turns, stuck/interrupted turns that need action, and selected service errors.
  Avoid mentioning on routine ACKs, replayed history, or every helper status
  line.

## Latency And Cache Strategy

- Prefer warm `AppServerRunner` for long-running Teams mode because it avoids per-message wrapper/native process startup and preserves more in-memory Codex session state.
- Keep `ExecRunner` available for correctness, compatibility, and recovery when app-server protocol probing fails.
- Keep model, provider, reasoning effort, profile, cwd, sandbox policy, permissions, and tool/MCP configuration stable within a Codex session.
- Put only the user's new message into the Codex turn input.
- Keep Teams metadata out of the prompt. Store Teams message ids, timestamps, URLs, and chat/thread metadata in helper state instead.
- Avoid injecting changing status headers into every turn.
- Artifact instructions must not add changing Teams metadata to every prompt.
  Prefer a fixed helper artifact root and sidecar manifest contract. If the
  stable runner cannot inject that contract without changing the prompt, keep
  automatic artifact handoff behind the later runner/manifest work and rely on
  explicit `/send-file` until then.
- Let Codex's own resume and compaction mechanisms manage history; do not reconstruct the conversation transcript manually.
- Parse and record `cached_input_tokens` from Codex JSON events so cache-hit behavior can be measured during development. Treat prompt-cache behavior as an observed metric, not as a helper-side correctness assumption.
- Add local latency probes before making app-server the default:
  - new `codex exec --json` turn latency
  - `codex exec resume --json <thread-id>` latency
  - warm app-server `turn/start` latency
  - cached input token ratio for repeated turns
- Treat Graph ingestion cursors, import checkpoints, per-chat sequence, and
  outbox retry/rate-limit state as durable ingestion or delivery state, not
  disposable cache.
- Cache only derived indexes that can be rebuilt from durable truth:
  - workspace/session discovery projections and display ordering; the stable
    numbers the user can reference remain durable state
  - Codex session metadata by cwd/thread id
  - Teams chat/session registry projections
  - dashboard projections, search indexes, and sort orders
  - Graph diagnostic windows and full-window warning details
  - renderer previews and upload-hash acceleration
- Avoid excessive Graph/Codex/history scanning by using TTLs, mtimes, durable
  cursors, and stale-while-revalidate behavior. A stale cache may delay a
  dashboard refresh, but must not lose turns, create duplicate Codex work, or
  skip a user message.
- Every cache file needs a schema version, source fingerprint, generated time,
  and atomic write discipline. Bad cache should be ignored and rebuilt; it
  should not block the bridge unless durable state itself is corrupt.
- Add a per-chat outbound rate limiter. Long message chunks, imported history,
  and artifact notifications must preserve order while respecting Teams rate
  limits and `Retry-After`.
- Every durable checkpoint update must be tied to the operation that makes it
  true. For example, history import checkpoints advance only after the
  corresponding Teams outbox item is sent; Graph ingestion checkpoints do not
  advance after a failed or throttled poll.

## Operations

- `teams run`: foreground service.
- `teams status`: local state, auth state, active sessions, queue depth, current Codex readiness.
- `teams doctor`: Graph auth, permissions, state locks, Codex binary, proxy readiness.
- `teams pause`: stop accepting new turns.
- `teams resume`: accept new turns.
- `teams drain`: finish active work and stop.
- `teams recover`: inspect and repair interrupted local state.
- `teams auth status`: show auth state without secrets.
- `teams logout`: remove local Teams tokens.

## Background Service And Upgrades

- Service supervision is user-level and must not require root:
  - Linux `systemd --user` where the user manager is available
  - WSL per-user Windows Scheduled Task by default, with optional
    `systemd --user` when explicitly requested
  - Windows per-user Task Scheduler
  - macOS LaunchAgent
- Upgrade protocol:
  1. pause new Teams work
  2. finish or park the active turn
  3. flush ledger and outbox
  4. release locks
  5. upgrade/restart
  6. recover pending state
- Foreground `teams run` is for testing and short sessions. Durable operation
  should use the service path so terminal close, SSH proxy disconnects, and
  transient network failures recover from state. On Linux without linger, full
  logout/reboot survival is an OS boundary outside this no-root helper; WSL
  should prefer the Windows Scheduled Task backend when the goal is Windows
  login based autostart.
- Remote or Teams-triggered upgrade should require explicit confirmation or a
  local operator policy before replacing the helper binary. Upgrade must not
  start while the bridge owns an ambiguous active turn unless the user chooses a
  recovery path.

## Deferred Work

- Arbitrary automatic file transfer remains deferred. The next phase should
  implement a narrow helper artifact manifest for Codex-generated files/images;
  explicit outbound file send is already covered by `teams send-file` and
  session `/send-file`.
- Streaming partial Codex output.
- Rich dashboard formatting beyond the practical renderer planned for the next
  phase.
- Slack or other non-Teams transports. They need a separate app-permission path,
  threat model, and durable transport adapter before implementation.
- Channel support if `ChannelMessage.Send` becomes available.
- Bot identity if Azure Bot creation becomes available.
