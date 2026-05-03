# Teams Integration Execution Plan

Status: active implementation tracker.

This document breaks the archived Teams research plan into implementation modules that can be assigned, reviewed, merged, and tested independently.

## Leadership Rules

- Keep this file as the task tracker while the feature is being built.
- Do not let Teams mode bypass existing `codex-helper` Codex launch behavior.
- Keep each implementation task in a disjoint write scope unless the lead explicitly merges or refactors boundaries.
- Prefer small mergeable slices over a broad rewrite.
- Every worker patch must include focused tests for the files it changes.
- Do not silently replay a Codex turn after an ambiguous crash. Preserve explicit recovery semantics.
- Treat Teams as a transport. Codex session ownership, locks, state, and recovery belong to `codex-helper`.
- Keep `AppServerRunner` experimental until protocol compatibility probes and fallback behavior are tested.
- Do not require Teams channel send, Azure Bot, or Slack app creation for the MVP.
- Keep durable Teams identity and ledgers as the source of truth. The legacy
  cache registry may remain as a projection and compatibility source, but
  control routing, session routing, seen ids, sent ids, and accepted turns must
  be recoverable from durable state.
- All bridge-originated Teams output must be represented in durable outbox
  before sending. Local terminal auth prompts and local CLI diagnostics are the
  only expected exceptions.
- Use helper-prefixed commands in session chats. Plain text, including words
  like `status`, must be treated as Codex input unless it starts with
  `helper ...`, `!`, a legacy slash command, or the safe bare `help`.
- Use "close" for a session lifecycle action. Do not imply that the tool can archive or delete the Teams chat itself.
- Treat the control chat as the product dashboard, not just a URL printer.
- Treat Teams sends as at-least-once. A Graph send accepted before a local
  `MarkOutboxSent` crash may duplicate in Teams, but it must not rerun Codex.

## Module Dependency Graph

1. `M0`: plan and tracker.
2. `M1`: durable local state, locks, ledger, and outbox.
3. `M2`: Codex runner abstraction and stable `ExecRunner`.
4. `M3`: Teams transport hardening.
5. `M4`: bridge/orchestrator state machine.
6. `M5`: CLI operations and operator UX.
7. `M6`: experimental `AppServerRunner`.
8. `M7`: background service, upgrade drain/recover protocol.
9. `M8`: broad verification matrix.
10. `M9`: explicit outbound attachment send.
11. `M10`: machine control chat, numbered navigation, and workspace/session discovery.
12. `M11`: historical session import and cross-surface reconciliation.
13. `M12`: notifications, Teams rendering, and automatic artifact handoff.
14. `M13`: cache, rate-limit, and schema-migration hardening.

`M1`, `M2`, and `M3` can start in parallel. `M4` should wait for their interfaces or stubs. `M6` should wait until `M2` defines the runner interface. `M7` should wait until `M1` and `M4` are stable. `M9` depends on the Graph allowlist in `M3` and the operator/session command surfaces in `M4`/`M5`. `M13a` must land before `M10`/`M11`/`M12` product rollout because those modules need migrated machine/workspace/session identity, ACK, notification, transcript, and outbox metadata. `M10` depends on migrated state, `M4`, and the existing `codexhistory` discovery code. `M11` depends on `M10` plus the new local transcript parser; runner read/list support is an enhancement, not the primary dependency. `M12` depends on `M3`, `M4`, `M9`, and the artifact/notification schema fields. `M13` can start as soon as the target state records are identified, but migration tests must land before broad product rollout.

## Current Priority Order

P0 completed in the current implementation slice:

- Teams session prompts now send a short ACK only after inbound and turn state
  are durable. ACK send failure leaves retryable outbox state and does not block
  Codex execution.
- The bridge can rebuild its legacy registry projection from durable state, so
  deleting the cache registry no longer loses control chat binding, session
  routing, seen inbound ids, or sent outbox ids.
- Teams-origin prompts recorded in the local Codex transcript are skipped during
  mixed Teams/local catch-up using durable inbound text hashes, preventing the
  user's Teams prompt from being re-imported as a local history duplicate.
- Inbound reference attachments now support workspace-relative prompt aliases
  and an explicit tenant-aware SharePoint host allowlist through
  `CODEX_HELPER_TEAMS_ALLOWED_SHAREPOINT_HOSTS`.
- Unrecognized control-chat text or slash commands now ACK after durable
  inbound/turn state, route to a durable control fallback Codex thread, add
  hidden helper-context instructions, and use
  `gpt-5.3-codex-spark` by default without projecting the control thread as a
  normal work session.
- Durable state now carries a `scope_id` derived from the OS user, Teams
  account, helper profile/config, and Codex home. Default bridge state and
  registry paths are scope-specific, while status/upgrade/recover can discover
  scoped state files under the shared Teams state root.
- Shared-storage machine arbitration now has `MachineRecord` and
  `ControlLease` records with primary/ephemeral priority. Ephemeral helpers
  remain running in standby instead of exiting, and they do not create control
  chats, poll Graph, dispatch Codex, or send outbox while a primary lease is
  live.
- Lease generation is recorded on owner heartbeats, inbound events, turns, and
  outbox messages. Bridge loops refresh and validate the active lease before
  polling, running Codex turns, or sending outbox, so a preempted process stops
  taking new work.
- Helper upgrade now writes durable upgrade state, drains all discovered scoped
  Teams states, stops/restarts the active user service on every supported
  service backend, and defers new Teams input during the upgrade window instead
  of treating it as ignored. Deferred input keeps the original text and is
  replayed by the restarted bridge after drain clears.
- User service management now has backend abstractions for Linux
  `systemd --user`, macOS LaunchAgent, and Windows per-user Task Scheduler,
  plus WSL doctor guidance. Default service units let the bridge choose scoped
  registry/state paths instead of forcing a shared legacy registry path.
- Non-mention outbox sends now use the Teams renderer path, so assistant,
  helper/status, imported, and error output are consistently labelled and HTML
  escaped at send time.
- Teams token caches now default to profile-scoped paths and only fall back to
  the legacy default cache when that cache passes the current scope allowlist,
  avoiding accidental reuse of over-broad delegated tokens.
- Service installs preserve the scoped runtime environment needed by Teams mode
  (`CODEX_HOME`, helper config/profile, Teams machine identity, token cache
  overrides, and proxy variables) on Linux, macOS, and Windows. Upgrade
  finalization now delays the Teams service restart when the binary replacement
  requires the current helper process to exit, and macOS/Windows active-service
  checks query the supervisor instead of trusting generated config files.
- Outbox delivery now flushes durable per-chat sequence order instead of sending
  newly queued messages ahead of older ACK/final/error messages. Chunk planning
  uses the actual Teams renderer budget, and owner-mention messages use the same
  rendered part labels and newline handling.
- Helper-upgrade drain now defers control `/new` and control fallback work,
  replays them after drain clears, preserves same-chat deferred ordering, and
  refuses to replay session slash commands or attachment-bearing messages as
  plain Codex prompts. Those cases produce an explicit post-upgrade message
  asking the user to rerun/resend the command or attachment.
- App-server runner ambiguity is surfaced as an interrupted turn when Codex may
  already have accepted a turn id/thread id, so the helper does not mark it as a
  safely failed retryable request.
- Inbound attachment limits now fail visibly instead of silently dropping files
  or images beyond the per-message limit.
- Outbound file/image attachments now queue a durable Teams attachment outbox
  record before upload. The outbox stores the local file identity and content
  hash before Graph upload, records the uploaded Drive item reference before
  Teams send, and can recover both "not uploaded yet" and "uploaded but not sent
  yet" restart windows.
- WSL installs now use a rootless per-user Windows Scheduled Task launcher by
  default when `systemd --user` is unavailable or undesired. The task starts
  `wsl.exe` with the current distro, Linux user, working directory, scoped
  helper environment, and `teams run`, so closing the launching terminal does
  not stop the bridge. Task names and config files include Linux user, Teams
  profile, and a stable short hash, and install leaves the task disabled until
  the user explicitly enables or starts it.
- Additional P1 verification covers half-written Codex JSONL tails, mixed
  Teams/local catch-up, artifact handoff failures, derived cache rebuilds,
  rate-limit recovery, renderer matrices, app-server probe cleanup, scoped
  upgrade drain, primary/ephemeral standby behavior, WSL Scheduled Task service
  generation, WSL delayed restart, durable pre-upload attachment recovery, and
  durable attachment replay after Teams send rate-limits.
- Live Teams test entrypoints fail closed before any chat read, message send,
  self-mention, or file upload unless
  `CODEX_HELPER_TEAMS_LIVE_JASON_WEI_ONLY=jason-wei-only` is set, the signed-in
  Graph user is `Jason Wei`, and `CODEX_HELPER_TEAMS_LIVE_CHAT_ID` resolves to
  a single-member group or meeting chat whose only member is that same AAD user.
- Control-chat bootstrap records the durable control binding first, sends an
  initial self-mention through durable outbox so Teams clients surface the
  meeting chat, then sends the ready message through durable outbox. Pending
  outbox flushing keeps same-chat FIFO checks enabled, so a fresh `sending`
  predecessor blocks later queued messages after restart instead of allowing
  overtaking.
- Graph status errors redact dynamic chat, message, hosted-content, share,
  drive-item, and upload-path values before they can be surfaced in local
  diagnostics or helper Teams replies.
- Outbound bridge/session/artifact attachment delivery stages accepted files
  into a private helper-managed outbox directory before queuing the durable
  upload record, so pre-upload recovery no longer depends on the original user
  file remaining unchanged.
- WSL service doctor performs a non-destructive readiness probe for
  `powershell.exe`, `wsl.exe`, and Windows Scheduled Task cmdlets before
  reporting the WSL Task Scheduler backend as usable. In WSL it can fall back to
  `/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe` when
  `powershell.exe` is not present in the Linux `PATH`.

P1 external validation remaining:

- Run live product tests for multi-machine control chats, mixed Teams/local
  usage, owner completion reminders, and automatic artifact handoff.
- App-server compatibility remains experimental, but the currently available
  Codex path has been revalidated. Broader multi-version/multi-path probing
  requires additional installed Codex versions or explicit `--codex-path`
  targets.

P2 deferred:

- Product UX for notification threshold configuration and richer rendered
  transcript labels everywhere.
- Large-file upload sessions and broader local-path attachment UX.
- Running-turn interrupt for the stable foreground `exec` runner.
- Remote upgrade confirmation/PIN semantics.

Parking/resume UX requirement:

- Parked or frozen work chats must never rely on the current dashboard number
  as the durable resume handle. Dashboard numbers are view-local and can change
  after discovery refreshes, helper restarts, or activity on another machine.
- Each park action must create a durable resume intent with a stable short key
  derived from durable identity, for example `r 8f3c9a2d`. The key should be
  long enough to avoid collisions across this helper profile and machine, but
  short enough to type on mobile. Resolve collisions by extending the key, not
  by falling back to numbers.
- The control chat may still show numbered dashboard shortcuts for immediate
  navigation, but parked-chat recovery messages must show the stable key as the
  primary command.
- The work chat freeze message must be written for a first-time, distracted
  Teams user. Do not mention internal concepts such as durable state, polling,
  checking, resume intents, registry, dashboard, or helper mechanisms. Required
  fields: this chat will not reply anymore, Codex work is safe, the
  plain-language reason, the exact command to copy, and where to send it.
- Preferred freeze message shape:

  ```text
  🧊 This chat is paused
  ⚠ **Messages here will not get a reply.**
  Your Codex work is safe. Paused after 6h idle.

  ▶️ **Continue chat:**
  Step 1: Open Control chat
  Step 2: Send: `r 8f3c9a2d`
  ```

- Keep the work-chat freeze notice to at most about 6 short lines on mobile.
  Put the exact command in inline code, avoid numbered alternatives, and avoid
  any secondary commands.

- Do not show raw Teams URLs in the freeze notice body. If the helper can create
  a control-chat resume-intent message and capture its Teams message id, the
  freeze notice may use a compact button/card/link affordance that displays only
  `Control chat`; otherwise show plain `Open Control chat` plus the stable
  resume command.
- Resume commands are idempotent. Reusing an already active key should return
  the active work chat link instead of creating another Teams chat or rerunning
  Codex.

## M0: Plan And Tracker

Owner: lead.

Write scope:

- `docs/teams_integration_plan.md`
- `docs/teams_integration_execution_plan.md`
- `docs/teams_security_threat_model.md`

Tasks:

- Keep research findings and execution tasks synchronized.
- Track module ownership, status, blockers, and merge order.
- Review every worker result before merging.
- Maintain a single final integration branch.

Acceptance:

- The plan names every module, dependency, constraint, and test gate.
- Each worker has a bounded write scope and can proceed without editing unrelated modules.

Status:

- Completed.

## M1: Durable State, Locks, Ledger, And Outbox

Owner: state worker.

Suggested write scope:

- New package under `internal/teams/state` or `internal/teams/store`.
- Tests for that package only.
- Avoid editing `internal/teams/bridge.go` until `M4`.

Responsibilities:

- Define durable records:
  - `MachineContext`
  - `WorkspaceContext`
  - `SessionContext`
  - `Turn`
  - `InboundEvent`
  - `OutboxMessage`
  - `ChatIngestionCheckpoint`
  - `TranscriptLedger`
  - `ImportCheckpoint`
  - `PerChatSequence`
  - `ArtifactRecord`
  - `UploadRecord`
  - schema version
- Persist one control chat per `machine_id + Teams account + helper profile`
  and keep machine labels stable across restarts unless the user explicitly
  renames or rebinds them.
- Persist workspace display numbers and session display numbers as durable
  user-facing references so the user can select by number without making Codex
  interpret the command.
- Persist Teams chat id to Codex `thread_id` mapping.
- Persist latest Codex turn id, runner kind, Codex version, cwd, Codex home, profile, model, sandbox, proxy mode, and yolo mode.
- Persist stable dashboard view state for bare-number control-chat selections:
  view id, item mapping, source fingerprint, generated time, and expiry.
- Persist session origin and sync state:
  - created from Teams vs imported from existing Codex history
  - whether historical transcript has been sent to Teams
  - last imported Codex item/turn position
  - latest Teams message id sent for each outbox part
- Add v2 identity and ledger fields in one migration:
  - `MachineContext`: `machine_id`, `machine_label`, account id/principal,
    helper profile/config fingerprint, control chat id/url/topic, timestamps,
    and rebind metadata
  - `WorkspaceContext`: workspace id, stable display number, cwd fingerprint,
    private cwd, short label, root/sudo identity marker, and timestamps
  - `SessionContext`: workspace id, stable display number, work chat
    id/url/topic, Codex thread id, Codex session file/fingerprint, origin,
    title source, sync status, and creation inbound id
  - `InboundEvent`: source chat kind, author user id, Graph created/modified
    timestamps, processing status, ignored reason, and dedupe key
  - `Turn`: source, attempt number, retry parent, transcript item ids, runner
    config fingerprint, Codex turn id, and token usage including
    `cached_input_tokens`
  - `OutboxMessage`: chat id, per-chat sequence, kind enum, semantic flags,
    dedupe key, part index/count, renderer version, rendered byte length,
    send lease/retry state, notification intent, import item key, and artifact id
  - `ChatIngestionCheckpoint`: seeded state, last processed modified time,
    last processed message id, overlap window, and last error
  - `TranscriptLedger`: Codex item key, role/type, source, timestamp, inbound
    event linkage, outbox linkage, and dedupe key
  - `ImportCheckpoint`: last imported item key, status, retry/error state
  - `PerChatSequence`: next durable sequence number for each chat
- Support atomic JSON writes with `0600` files and `0700` directories.
- Add a lock discipline for process-wide state and per-session mutation.
- Provide transitions for:
  - session created
  - inbound persisted
  - turn queued
  - ack outbox queued
  - ack sent or pending
  - turn running
  - turn completed
  - turn failed
  - turn interrupted
  - outbox queued
  - outbox sent
- Store enough state to avoid duplicate Codex execution and duplicate Teams send.
- Provide process ownership metadata for the running service:
  - pid
  - hostname
  - executable path
  - helper version
  - started at
  - last heartbeat
  - active session/turn when available
- Support scoped locks beyond a single process lock:
  - global service lock
  - per session lock
  - per Codex thread lock

Constraints:

- Do not use the OS cache directory for durable state.
- The existing Teams registry in the OS cache directory is a migration source
  and optional projection only. After migration, it must not be used as durable
  truth for control chat binding, session routing, seen inbound ids, sent ids,
  or next session numbers.
- Do not store access tokens in this state package.
- Do not log message body, Teams URLs, access tokens, refresh tokens, or full local paths.
- Do not silently repair ambiguous turn state by replaying a prompt.
- Keep schema migration explicit and testable.
- Never infer durable behavior from rendered Teams text. Persist behavior flags
  such as `notify_owner`, `history_import`, `artifact_upload`, and `ack` as
  structured fields.
- Treat Graph ingestion checkpoints, import/catch-up checkpoints, per-chat
  sequence, and outbox retry/rate-limit state as durable state, not cache.
- A stale lock can be recovered only when ownership metadata proves the owner is gone or the user runs an explicit recovery command.

Tests:

- New store load when missing.
- Atomic save/load round trip.
- Directory and file permissions on Unix.
- Duplicate inbound event is idempotent.
- Ambiguous running turn becomes interrupted on recovery.
- Outbox resend does not create a new Codex turn.
- Stale owner heartbeat is reported and recoverable.
- Live owner lock is refused.
- Schema migration preserves older session/outbox records and initializes new
  machine/workspace/sync/notification fields safely.
- v1 store plus registry fixture migrates into v2 with control chat,
  session/chat/thread mapping, sent markers, and display numbers preserved.
- Future schema versions fail closed with a clear diagnostic and do not
  load-save away unknown fields.
- Deleting the old registry after migration does not break control routing,
  session routing, or next display-number allocation.
- ACK outbox send failure does not block the persisted turn from moving to
  queued/running.
- Long message split persists all parts and sequence numbers before any part is
  sent, and restart preserves the same order.
- Graph poll errors or throttling do not advance ingestion checkpoints.
- Import checkpoints advance only after the corresponding Teams outbox item is
  sent.
- Per-chat sequence allocation is race-safe.
- Numbered workspace/session indexes survive restart and are rebuilt safely
  when the underlying Codex history changes.

Status:

- Completed first implementation slice.
- Completed M1b owner heartbeat and stale-lock metadata.
- Completed P0 recovery slice: durable state now stores enough inbound text hash
  and binding data for the bridge to recover control routing, session routing,
  seen inbound ids, and sent outbox ids when the legacy registry projection is
  missing.
- Completed P1 migration slice: raw pre-v2 state fixtures now validate owner,
  session, turn, inbound, queued outbox, and accepted outbox migration; future
  schema versions fail closed and failed updates do not rewrite the file; legacy
  registry projections migrate into durable control/session/seen/sent records
  before the registry is treated as optional.
- Follow-up before broad product rollout: add more fixture samples from older
  local machines if they contain additional pre-v2 state shapes. Decide whether
  outbox bodies need payload minimization or encryption.

## M2: Codex Runner Abstraction And ExecRunner

Owner: runner worker.

Suggested write scope:

- New package under `internal/codexrunner` for protocol-neutral types, JSONL parsing, and runner interfaces.
- A small CLI-side adapter file only if needed to preserve current launch behavior.
- Focused tests under `internal/codexrunner`.

Responsibilities:

- Define runner interface:
  - `StartThread`
  - `ResumeThread`
  - `StartTurn`
  - `InterruptTurn`
  - `ReadThread`
  - `ListThreads`
- Implement stable `ExecRunner` using:
  - `codex exec --json`
  - `codex exec resume --json <thread-id>`
- Parse Codex JSONL events:
  - `thread.started`
  - `turn.started`
  - `turn.completed`
  - `turn.failed`
  - final assistant message
  - token usage including `cached_input_tokens`
- Do not require a turn id from `codex exec --json` events. Codex 0.125.0's
  exec `turn.started` payload is empty; turn identity is reliable only on the
  app-server protocol path or from local transcript reconciliation.
- Provide read/list data in a helper-friendly shape:
  - cwd/project path
  - thread id
  - title/topic when available
  - created/updated timestamps
  - ordered user/assistant/tool/status items when available
  - stable item ids suitable for import checkpoints
- Add a local Codex transcript parser for JSONL session files:
  - stable item key built from session id, file fingerprint, item id or content
    hash, and line/offset fallback
  - role/type/kind for user, assistant, tool, tool result, status, and visible
    reasoning summaries when present
  - incomplete-tail detection that prevents checkpoint advancement
  - `ReadSessionTranscript` and `ReadSessionTranscriptSince` helpers for
    publish and mixed-use catch-up
- Preserve exact `thread_id`. Never use `--last` for automation.
- Provide structured errors that distinguish Codex failure, launch failure, timeout, and parse failure.
- Prefer stdin or a private pipe for long prompts when the shared launch path supports it, so prompt text is not exposed as a process argument.

Constraints:

- The production runner must not call `exec.LookPath("codex")` directly as the only launch path.
- The production runner must preserve managed install, proxy setup, yolo mode, effective path resolution, root/sudo identity, `CODEX_HOME`/`CODEX_DIR`, self-update guard, and proxy-health termination.
- Avoid importing `internal/cli` from `internal/teams`; prevent cycles by keeping CLI-specific launch adaptation at the CLI boundary.
- Direct OpenAI API calls are out of scope.
- Do not make a direct native-binary path the default until wrapper environment behavior is preserved.
- `ExecRunner.ReadThread` and `ExecRunner.ListThreads` may remain unsupported
  for MVP, but historical import and mixed-use reconciliation must fall back to
  the local Codex history parser rather than treating unsupported read/list as a
  product failure.
- Do not use array offsets or timestamps as the only transcript identity.
  They are diagnostics or fallback hints, not durable dedupe keys.

Tests:

- JSONL parser extracts thread id, turn id, final message, failure, and cached input tokens.
- Thread read/list fixtures cover enough metadata for control-chat numbering
  and historical transcript import.
- Transcript parser fixtures cover stable item keys, file fingerprint changes,
  half-written JSONL tails, tool/status records, and `ReadSessionTranscriptSince`
  checkpoint behavior.
- Resume command includes exact thread id and never emits `--last`.
- Timeout/cancel is surfaced distinctly.
- CLI adapter tests prove the Teams runner uses the same launch resolver path as existing Codex commands.

Status:

- Completed first implementation slice.
- Completed M2b CLI boundary adapter for existing managed install, proxy, yolo, effective path, and self-update behavior.
- Follow-up before AppServerRunner: add real `codex exec --json` fixtures,
  local session JSONL transcript fixtures, and compatibility probes.

## M3: Teams Transport Hardening

Owner: transport worker.

Suggested write scope:

- `internal/teams/auth.go`
- `internal/teams/graph.go`
- `internal/teams/text.go`
- transport-focused tests.
- Avoid bridge state-machine changes until `M4`.

Responsibilities:

- Keep delegated Graph auth and token cache safe.
- Preserve device-code login and WSL browser handoff.
- Add explicit auth status/logout support if missing.
- Harden Graph retry behavior:
  - refresh on 401
  - bounded retry for 429 and 5xx
  - respect `Retry-After`
  - jittered backoff
- Keep the Graph request allowlist.
- Ensure Teams HTML rendering is safe and simple.
- Keep same-chat processing serializable by exposing transport primitives that do not hide concurrency.
- Provide transport errors that can be rendered as short user-facing next actions.
- Detect unsupported attachments and return an explicit "not supported yet" result instead of silently ignoring them.
- Split outbound chat messages by final rendered Teams HTML byte length.
  Live testing on 2026-04-30 found the delegated Graph chat send path accepts
  `102,289` bytes of HTML `body.content` and rejects `102,290` bytes with
  `HTTP 400 BadRequest`; production sends should target about `72 KiB` and
  keep each final HTML body below an `80 KiB` safety line.
- Provide a per-chat send scheduler/rate limiter that respects `Retry-After`
  and preserves order for long-message chunks, imported history, ACKs, and
  notifications.
- Keep request retry policy separate from delivery scheduling. Graph client
  retry handles one HTTP request; the outbox scheduler owns per-chat FIFO,
  send leases, `blocked_until`, poison messages, and cross-chat fairness.
- Expose a narrow send primitive for owner self-mentions. The primitive should
  build the Graph `mentions` array from the authenticated `User` record, not
  from model-supplied HTML.
- Expose redaction helpers for Graph paths, Teams ids, drive item ids, Teams
  URLs, local paths, message bodies, and token-like strings before errors reach
  status, doctor, logs, or Teams helper replies.
- Validate token cache files and parent directories: private permissions,
  non-symlink path components where practical, expected scopes, and separated
  chat vs file-write auth profiles.

Constraints:

- Do not broaden Graph permissions without an explicit plan update.
- Do not print tokens, refresh tokens, raw Graph errors containing secrets, full message bodies, or full local paths.
- Do not add channel-send dependency to the MVP path.
- Do not add app-only auth for normal chat operations.
- Default scope handling must fail closed on unexpected broad scopes unless an explicit unsafe override is added later.
- Do not silently truncate attachment lists. If count, size, total-size, host,
  type, or policy limits reject some attachments, tell the user exactly that the
  turn was not run or which supported subset was processed.
- Do not pass full temp paths to Codex when a stable neutral alias can represent
  an attachment.

Tests:

- Token cache file permission.
- Refresh flow keeps old refresh token if a new one is not returned.
- 401 refresh path.
- 429 `Retry-After` path.
- 5xx bounded retry path.
- Graph allowlist rejects unexpected endpoints.
- HTML escaping and plain-text extraction.
- Attachment placeholder behavior.
- Long-message chunking by rendered HTML bytes, including escaped text and
  multi-part labels.
- Per-chat ordered send scheduling under simulated 429/Retry-After.
- Self-mention payload generation with only the authenticated owner as the
  mention target.
- Token cache parent-directory permission and symlink checks.
- Scope-status tests that show elevated `Files.Read`/`Files.ReadWrite` auth
  separately from normal chat auth.
- Redaction tests for Graph errors containing chat ids, message ids, drive ids,
  Teams URLs, local paths, and message snippets.
- Attachment over-count, aggregate-size, mixed supported/unsupported, and
  explicit-rejection behavior.

Status:

- Completed first implementation slice.
- Completed attachment safety slice: inbound Teams file/link attachments are detected, persisted as ignored session inbound events, and answered with an explicit unsupported-transfer message instead of being silently dropped or sent to Codex without the file.
- Completed hosted-content slice: inline Teams hosted content referenced from message HTML, such as pasted images or snippets, is downloaded through allowlisted Graph `$value` endpoints into private local temp files and added to the Codex prompt for that turn.
- Completed reference-file slice: ordinary Teams file attachments are supported only for `reference` attachments whose HTTPS SharePoint URL can be converted to Graph `/shares/{shareId}/driveItem/content`, and only when the user explicitly adds `Files.Read` or `Files.ReadWrite`.
- Completed attachment policy slice: reference-file SharePoint hosts can be
  restricted with `CODEX_HELPER_TEAMS_ALLOWED_SHAREPOINT_HOSTS`, and downloaded
  attachment paths passed to Codex prefer workspace-relative hidden aliases
  instead of full local temp paths.
- Completed durable poll-cursor slice: chat polling records durable seeded state, last modified cursor, last success/error, and full-window diagnostics in Teams state; subsequent polls use `lastModifiedDateTime` filtering with a small overlap and safe nextLink pagination.
- Completed P1 backlog-continuation slice: when Graph returns more than the
  bounded page cap for an already-seeded chat, the bridge stores the allowlisted
  nextLink continuation path and resumes from it on the next poll instead of
  replaying the same first ten pages forever.
- Completed live message-size probe: in a dedicated probe chat, `102,289`
  bytes of HTML `body.content` succeeded and `102,290` bytes failed. Current
  implementation splits by rendered HTML byte length with a `72 KiB` chunk
  target and an `80 KiB` safety line, and labels multi-part messages.
- Completed live self-mention probe: Graph accepted a message containing a
  `mentions` payload for the authenticated user, and the Teams client notified
  the user. Product implementation still needs a durable outbox-level
  notification flag before reminders are enabled broadly.
- Follow-up before production use: add explicit live Graph smoke tests behind opt-in environment variables.

## M4: Bridge And Orchestrator State Machine

Owner: orchestrator worker after `M1` and `M2` interfaces are available.

Suggested write scope:

- `internal/teams/bridge.go`
- new orchestrator files under `internal/teams`.
- bridge/orchestrator tests.

Responsibilities:

- Route control chat commands.
- Create or reuse one single-member Teams meeting work chat for a Codex session on
  demand when the user opens or publishes that session in Teams.
- Treat non-slash session messages as Codex input.
- Treat slash-prefixed commands as helper commands.
- Serialize work per session chat.
- Persist inbound before dispatch.
- Persist turn state before starting Codex.
- Persist output before sending Teams messages.
- Mark outbox sent only after Graph send succeeds.
- Persist all bridge-originated Teams messages before sending, including control
  replies, helper errors, ACKs, status replies, history import, final chunks,
  artifact links, and owner notifications.
- Render/split/validate every part of a long message and persist the full part
  set before sending the first part.
- Persist owner-notification intent in the outbox separately from message text,
  then render it as a Teams self-mention at send time.
- Mention the owner only for completion or action-required events, not routine
  ACKs, historical replay, session-list output, or every status line.
- Send a short ACK when a Teams session prompt has been durably accepted for
  Codex execution. The ACK should include session id, turn id, and queued/running
  state. Do not ACK local Codex turns or imported history.
- ACK state must be observable as: inbound persisted -> turn queued -> ACK
  outbox queued -> ACK sent or pending -> turn running -> final/error outbox
  queued -> final/error sent. ACK send failure leaves the ACK retryable but
  does not block Codex execution after the turn is durably queued.
- Provide explicit commands:
  - control chat: `projects`, `project <n>`, `sessions`/`history`, `open
    <n|session-id>`, `continue <n|session-id>`, `new ...`, `mkdir ...`,
    `details ...`, `status`, and `ask <question>`
  - work chat: `helper status`, `helper close`, `helper retry <turn-id>`,
    `helper cancel <turn-id>`, `helper file <relative-path>`, and
    `helper rename <title>`; `!` aliases are accepted for desktop use
  - legacy `/...`, `cx ...`, and `codex ...` forms stay compatible where
    possible but are not the primary user-facing path

Constraints:

- Do not execute Codex before inbound state is durable.
- Do not send Teams output that is not represented in the outbox.
- Do not process messages from other Teams users.
- Do not replay interrupted turns unless the user explicitly asks.
- Do not reserve plain words like `status` or `close`; only slash commands are reserved.
- Do not claim a closed session chat has been removed from Teams.
- Do not allow work-chat slash commands to perform service-wide pause, resume,
  drain, or upgrade actions.
- Do not allow model output, user prompt text, or replayed history to inject
  arbitrary Teams mentions. Mentions are bridge-controlled metadata.
- Do not use Codex to interpret dashboard selections or numbered menu choices.
  Those are helper commands.
- Unknown control-chat input may be routed to Codex, but recognized helper
  commands, bare-number dashboard selections, and empty/help messages must stay
  helper-owned.

Tests:

- First poll seeds the durable ingestion checkpoint without executing
  historical messages.
- New session creates state and anchor outbox.
- Session message queues and runs one turn.
- Duplicate Teams message id is ignored.
- Crash recovery with queued outbox sends without rerunning Codex.
- Ambiguous running turn requires explicit retry.
- Plain `status` in a session chat is sent to Codex.
- `helper status` in a session chat is handled by the bridge.
- Long-running turn completion queues a durable outbox message with
  owner-notification intent and sends a valid self-mention.
- Outbox replay after restart preserves whether a reminder should mention the
  owner and does not duplicate already-sent mention messages.
- Teams prompt ACK is sent after inbound/turn persistence and before Codex
  execution, while duplicate inbound does not generate duplicate ACKs.
- Control replies, helper errors, and status output also pass through outbox
  and preserve per-chat sequence.
- A paused/draining Teams prompt is persisted as ignored/blocked inbound,
  returns a helper blocked message, and does not create a Codex turn or ACK.
- Graph send success followed by `MarkOutboxSent` failure may duplicate a Teams
  message on restart, but must not create a new Codex turn.

Status:

- Completed first durable MVP slice:
  - session plain messages persist inbound, queue turns, execute runner, persist outbox, send, and mark sent
  - duplicate inbound flushes existing outbox without rerunning Codex
  - helper-prefixed session commands are enforced, with legacy slash commands still accepted
  - `helper close` uses closed wording rather than archive wording
  - control chat `open <session-id>` returns the session URL and Codex thread details when available
- Completed first recovery-command slice:
  - `helper cancel <turn-id>` can interrupt queued turns and records a durable canceled outbox message
  - `helper retry <turn-id>` can rerun failed/interrupted turns by refetching the original Teams message through Graph, avoiding local prompt storage
  - running-turn cancellation remains explicit unsupported behavior for the foreground exec path
- Completed Teams prompt ACK slice:
  - Teams-origin session prompts queue an ACK outbox item only after inbound and
    turn records are durable
  - ACK send failure is recorded for retry but does not block Codex execution
  - duplicate inbound messages do not create duplicate ACKs or rerun Codex
- Completed control fallback slice:
  - unrecognized control-chat input creates/uses a hidden durable control
    fallback session and resumes its Codex thread across restarts
  - the fallback prompt includes helper command context and artifact handoff
    instructions without sending those instructions directly to Teams
  - the fallback ACK and final/error replies use the same durable outbox path as
    normal Teams turns
- Completed local notification slice:
  - long-running completion outbox can mention the owner through a real Graph
    mention payload, and owner mention policy keeps ACKs, status, import, and
    history replay unmentioned
  - live owner-completion reminder behavior remains part of the blocked live
    product validation matrix

## M5: CLI Operations

Owner: CLI worker after `M1` and `M4` APIs settle.

Suggested write scope:

- `internal/cli/teams.go`
- CLI command tests.

Responsibilities:

- Expose operator commands:
  - `teams run`
  - `teams status`
  - `teams doctor`
  - `teams pause`
  - `teams resume`
  - `teams drain`
  - `teams recover`
  - `teams auth status`
  - `teams logout`
- Keep `teams listen` only as a compatibility alias if needed.
- Make diagnostics useful without exposing secrets.
- Ensure commands work in WSL.
- Make `teams run` the normal user entrypoint: authenticate if needed, ensure the control chat exists, and start the foreground service.
- Keep `teams auth` and low-level polling commands available as debug/advanced commands.

Constraints:

- Do not start background services implicitly during auth/status commands.
- Do not print secrets.
- Do not make shell/profile changes from Teams commands.
- Do not imply remote upgrade is available until local confirmation/PIN semantics exist.

Tests:

- Root command includes Teams subcommands.
- Each command validates flags and reports expected status.
- `doctor` distinguishes auth, Graph permission, state lock, Codex readiness, and proxy readiness.
- `status` shows online/offline, last poll, active sessions, queued turns, active turn, last error, and Codex readiness without secrets.

Status:

- Completed CLI MVP slice:
  - `teams run` is the normal foreground entrypoint
  - `teams listen` remains a compatibility alias
  - `teams run --upgrade-codex` upgrades the managed Codex CLI once before polling starts, refuses `--codex-path`, and refuses to run while another Teams bridge owns the state
  - `teams run --runner appserver` exposes the experimental app-server runner while `exec` remains the default
  - `teams run --control-fallback-model` controls the model for unrecognized
    control-chat requests and defaults to `gpt-5.3-codex-spark`
  - `teams status`, `teams doctor`, `teams recover`, `teams auth status`, and `teams auth logout` are wired
  - `teams pause`, `teams resume`, and `teams drain` persist local service-control state
  - `teams status` includes poll diagnostics for chat cursor health, poll errors, and full-window warnings
  - `teams doctor --live` explicitly opts into Graph `/me` and control-chat read checks; default doctor remains local-only

## M6: Experimental AppServerRunner

Owner: app-server worker after `M2`.

Suggested write scope:

- `internal/codexrunner/appserver_*`
- tests with fake app-server protocol streams.

Responsibilities:

- Start and supervise `codex app-server`.
- Initialize newline-delimited app-server protocol.
- Implement:
  - `thread/start`
  - `thread/resume`
  - `turn/start`
  - `turn/interrupt`
  - `thread/read`
  - `thread/list`
- Match the generated Codex 0.125.0 app-server schema:
  - `thread/list` returns `data`, `nextCursor`, and `backwardsCursor`, not a
    top-level `threads` array
  - `thread/read` must send `includeTurns`
  - `Thread.turns` and `Turn.items` are selectively populated; do not assume a
    `turn/completed` notification contains final items
  - use camelCase/v2 request fields such as `threadId`, `approvalPolicy`,
    `permissionProfile`, and `sandboxPolicy`
  - translate known CLI options into app-server fields or fall back to
    `ExecRunner`; do not forward raw CLI `extra_args` as unknown protocol
    fields
- Probe compatibility at startup.
- Fall back to `ExecRunner` if initialization or protocol probes fail.
- Keep one warm app-server per compatible Codex environment.

Constraints:

- Keep this path behind an experimental setting until tested across Codex versions.
- Do not make app-server required for MVP correctness.
- Use newline-delimited JSON-RPC 2.0 framing for current Codex app-server versions; verify generated schema compatibility before broadening support.

Tests:

- Initialize handshake.
- Thread list probe with the real v2 `data` envelope, including the empty-list
  case and at least one non-empty thread fixture so schema drift cannot pass
  silently.
- Thread read request includes `includeTurns` and parses populated `turns/items`.
- Start/resume/start-turn request encoding.
- Notification stream parsing.
- Crash and fallback behavior.

Status:

- Partial: experimental `AppServerRunner` exists with JSON-RPC request framing, fake-protocol tests, initialization probe, request encoding, notification parsing, structured errors, transport crash handling, real stdio process transport, and fallback on initialization/probe failure.
- Partial: CLI opt-in exists through `teams run --runner appserver`; default remains stable `exec`.
- Completed current-install probe: `teams doctor --appserver-probe` performs a no-model app-server initialize/thread-list compatibility check and closes the warm process before returning. Local probe on this workstation passed in 582ms.
- Completed P1 latency probe slice: `teams doctor --appserver-probe-runs N`
  now performs repeated cold app-server initialize/thread-list probes and
  reports min/max/total timings for the selected Codex path.
- Completed: schema-alignment pass against Codex 0.125.0 generated app-server
  protocol for `thread/list`, `thread/read includeTurns`, `turn/start`
  immediate response plus later completion notifications, `turn/completed`
  failed/interrupted states, nested error notifications, token usage updates,
  and unsupported interleaved server requests.
- Pending: broader real Codex protocol compatibility probes across versions and performance comparison before making app-server the default.

## M7: Background Service And Upgrade Compatibility

Owner: service worker after `M1`, `M4`, and `M5`.

Suggested write scope:

- service files under `internal/teams` or `internal/cli`.
- docs only if command behavior changes.

Responsibilities:

- Provide foreground `teams run` first.
- Add user-level service supervision after the durable core is stable.
- Handle terminal close, SSH/proxy/network instability, and process restart.
- Support upgrade protocol:
  1. pause new Teams work
  2. finish or park active turn
  3. flush ledger and outbox
  4. release locks
  5. upgrade/restart
  6. recover pending state
- Surface upgrade availability as advisory Teams output first. Remote upgrade is deferred until there is a local confirmation design.

Constraints:

- Do not install or enable services without explicit user action.
- Do not block normal `codex-helper` upgrade flow.
- Do not leave stale locks that prevent recovery.
- Do not install services implicitly from `teams run`.

Tests:

- Drain parks active work.
- Restart recovers pending outbox.
- Locked state reports a clear diagnostic.
- Upgrade drain path leaves state recoverable.

Status:

- Completed current P0/P1 slice: foreground `teams run` records service owner
  heartbeat and clears it on clean exit; long-running turns keep owner heartbeat
  active and expose active session/turn metadata; local `pause`, `resume`, and
  `drain` reject new session work without invoking Codex and record durable
  control output; `teams recover` refuses live owners by default, allows stale
  owners, and requires `--force` for live-owner override.
- Completed current P0/P1 service slice: Linux `systemd --user`, macOS
  LaunchAgent, Windows per-user Task Scheduler, and WSL Windows Scheduled Task
  service backends support install/uninstall/status/start/stop/restart plus
  enable/disable where applicable. Install does not enable or start
  automatically, and WSL task names are isolated by distro, Linux user, profile,
  and stable short hash. WSL service doctor now checks Windows-side supervisor
  readiness with a non-destructive PowerShell probe before declaring the backend
  usable, and falls back to the standard Windows PowerShell path when the
  executable is not exported into the WSL `PATH`.
- Completed current P0/P1 upgrade slice: helper `upgrade` asks active Teams
  bridges to drain before binary replacement, stops an active user service after
  drain to avoid supervisor restart races, restarts or schedules delayed restart
  after the update, times out with an operator diagnostic, and restores only the
  upgrade-owned drain state.
- Completed: `docs/teams_security_threat_model.md` documents terminal/SSH
  disconnect behavior, user-service expectations, upgrade drain/restart
  semantics, token risks, local-state risks, WSL/browser boundaries, attachment
  boundaries, and residual risks.
- Deferred to P2: mid-turn interrupt for the stable foreground `exec` runner and
  remote upgrade confirmation/PIN semantics.

## M9: Explicit Outbound Attachment Send

Owner: transport/CLI worker and lead.

Write scope:

- `internal/teams/auth.go`
- `internal/teams/graph.go`
- `internal/teams/outbound_attachments.go`
- `internal/teams/bridge.go`
- `internal/cli/teams.go`
- focused tests and docs for outbound attachment behavior.

Responsibilities:

- Support a separate opt-in file-write auth profile, while allowing the one-shot
  full auth token to satisfy file upload when the user chooses the simpler setup.
- Upload only explicit files selected by the operator or session helper command.
- Default session `helper file <relative-path>` to relative paths under the Teams outbound root.
- Allow local arbitrary paths only through the local CLI with `--allow-local-path`.
- Upload to the default Teams/OneDrive chat-file folder and send a Teams `reference` attachment to the target chat.
- Enforce bounded file size, extension allowlist, symlink rejection, safe upload names, private token cache, and Graph path allowlist checks.
- Provide an opt-in live smoke test for the real Graph upload/send path.

Status:

- Completed MVP: manual Graph proof of concept uploaded `.txt` and `.png` files to OneDrive and sent Teams reference attachments to the test chat.
- Completed implementation: `teams auth file-write`, `teams auth file-write-status`, `teams auth file-write-logout`, `teams send-file`, and session `helper file`.
- Completed one-shot auth follow-up: `teams auth full` can request read, send,
  meeting-chat, and file-upload scopes once; runtime read/chat/file clients use
  split tokens when present and fall back to the full token, or to an existing
  broad chat token whose cached scopes explicitly cover the full feature set.
- Completed tests: auth-profile separation, Graph upload/send payloads, outbound path restrictions, bridge helper-command upload path, CLI target resolution, and opt-in live Graph upload smoke hook.
- Completed stress hardening: safe token-cache status checks, cached-token scope validation, outbound root permission repair, symlink-directory and TOCTOU-resistant file reads, subsecond upload names, production Graph `nextLink` normalization, drive item id validation, helper-prefixed attachment messages, pause/drain checks before attachment download or `helper retry`, outbox send lease, and active-service stop/restart during upgrade even when the Teams state file is absent.
- Completed durable delivery hardening: `helper file` and Codex artifact
  manifest uploads now create attachment outbox records before upload, store the
  staged helper-owned file identity and content hash, fill the Drive item reference after
  upload, preserve per-chat order, respect rate-limit blocks, and retry the
  Teams attachment message without losing the accepted attachment.
- Deferred from `M9`: automatic upload of arbitrary Codex output, richer attachment
  selection UX, large-file upload sessions, and policy prompts for broad
  tenant-granted file scopes. A narrow helper artifact-manifest path for
  generated Codex files/images is tracked separately in `M12`.

## M10: Machine Control Chat, Numbered Navigation, And Discovery

Owner: product/orchestrator worker after `M1`, `M4`, and the history discovery APIs are stable.

Suggested write scope:

- `internal/teams/registry.go`
- `internal/teams/bridge.go`
- new discovery/index files under `internal/teams`
- small CLI glue only if command flags are needed
- focused dashboard/navigation tests

Responsibilities:

- Create and maintain one main/control Teams chat per `machine_id + Teams
  account + helper profile`.
- Title the main chat with a stable machine label and an obvious main/control
  marker. The default shape should be equivalent to `🏠 Codex Control -
  {machine_label}` with a short profile or machine suffix if needed for
  uniqueness. The marker is presentation only; installations may use emoji or
  another visual marker, but routing must use durable machine/chat ids.
- Title each work chat with a work/session marker, stable helper session id, and
  sanitized topic or cwd basename. The default shape should be equivalent to
  `💬 Codex Work - {machine_label} - {session_id} - {topic_or_cwd}`.
- Provide `helper rename <title>` so the user can replace a sanitized title without
  exposing raw prompts or full paths.
- Build a helper-owned control-chat index:
  - known work directories
  - sessions under a selected directory
  - active/running/queued/closed state
  - Teams chat availability
  - Codex thread id when known
- Refresh the control-chat index from durable state plus local Codex history:
  - full scan on startup, explicit recover, and explicit dashboard refresh
  - lightweight refresh on `projects`, `project`, `sessions`, `open`,
    and `continue`
  - periodic background refresh with a default target of roughly one minute for
    dashboard discovery
- Give work directories and sessions stable short numbers for selection. Numbers
  should stay stable across refreshes when possible and be regenerated safely
  when the underlying set changes.
- Keep a durable current-view record for control-chat bare-number selections.
  Bare numbers are valid only in the control chat, only for the latest view,
  and only until the view expires.
- Handle these dashboard workflows without invoking Codex:
  - list work directories
  - list sessions under a directory
  - create a new directory and start a new session there
  - create/open a Teams work chat for an existing session on demand
  - publish an existing local Codex session to Teams on demand
  - show a concise machine status summary
- Prefer English natural control commands: `projects`, `project <n>`,
  `sessions`, `open <n|session-id>`, `continue <n|session-id>`, `new ...`,
  `mkdir ...`, `details ...`, and `status`. Bare numbers remain scoped to the
  control chat only. Slash and short forms are compatibility aliases only.

Constraints:

- Do not mirror all historical sessions into Teams automatically.
- Do not use Codex to interpret numbered selections or dashboard commands.
- Do not leak full local paths in routine dashboard output when a shorter label
  is enough; provide exact paths only when the user asks for details.
- Preserve existing `codex-helper` history discovery behavior and root/sudo
  identity semantics.
- Do not use chat titles as durable identity. Titles are mutable presentation;
  state must use machine, workspace, session, chat, and thread ids.

Tests:

- Machine control chat title and id persist across restart.
- Multiple machine identities do not reuse each other's control chat.
- Workspace/session numbering stays stable across refresh and rebuilds safely
  after deletions or new sessions.
- Dashboard commands do not invoke the Codex runner.
- Creating a session in a missing directory creates the directory only when the
  user explicitly requested it.
- Work-chat messages `1`, `status`, and `close` are Codex input; helper
  actions use `helper ...` or `!` prefixes. Bare `help` is a safe local
  affordance.
- Routine dashboard output hides full paths, chat ids, drive ids, and token
  scope details; `details` or local doctor may reveal more.
- Dashboard refresh discovers a locally-created Codex session without invoking
  Codex, and refresh misses do not change stable numbers or create duplicate
  session records.

Status:

- Partial implementation completed:
  - title and privacy helpers for machine/control/work chat titles
  - helper-owned dashboard model with stable numbered workspaces/sessions and
    expiring current-view selection
  - control/work command parser that keeps plain work-chat text as Codex input
  - bridge integration for `projects`, `project <n>`, `sessions`, bare
    control-chat number selection, and local history-backed session listing
  - dashboard commands are covered by tests and do not invoke the Codex runner
  - `new <directory>` creates the directory, binds the session cwd,
    and routes Codex turns from that work chat in the selected directory
  - `mkdir <directory>` creates operator-requested work directories
  - work-chat `helper rename <title>` updates the Teams group-chat topic and durable
    session metadata
  - `details` and work-chat `helper details` provide session state, cwd, Teams URL,
    and Codex thread/turn ids without involving Codex
- Still pending:
  - multi-machine live Graph verification beyond unit tests
  - richer machine-level dashboard details for service health and queue state

## M11: Historical Session Import And Cross-Surface Reconciliation

Owner: sync worker after `M10` and the local Codex transcript parser.

Suggested write scope:

- new sync/import files under `internal/teams`
- integration tests with fake Codex history/session data
- minimal bridge hooks for user commands

Responsibilities:

- Discover Codex sessions created or resumed outside Teams and add them to the
  control-chat index.
- Watch for linked active sessions with a near-real-time target while the bridge
  is running. Use polling and file mtimes as the portable baseline; optional
  filesystem notifications may accelerate discovery but are not correctness
  requirements. The default target is within one Teams poll interval for linked
  active sessions, with full reconciliation on startup and recover.
- Use existing Codex history discovery and local JSONL session files as the
  primary transcript source. Runner `ReadThread`/`ListThreads` may enrich
  metadata, but unsupported runner read/list must not block listing, publishing,
  or catch-up when local history is available.
- Publish an existing Codex session to Teams only when requested by the user.
- If a requested existing session has no Teams work chat, create one and import
  transcript history in order.
- Label imported user, assistant, tool/status, and artifact records clearly.
  Include a title as the first imported message unless the session was
  originally created inside Teams. The title outbox item should be first in
  per-chat sequence for a published history.
- Keep import checkpoints so repeated publish/sync operations do not duplicate
  history.
- Reconcile mixed Teams/local usage:
  - detect Codex records missing from Teams
  - send missing records in original order when the session is linked to Teams
  - avoid interleaving local history ahead of already queued Teams outbox
  - keep Teams-origin ACKs and helper status separate from transcript import
  - skip Teams-origin prompts that already have a `TeamsOriginMap` entry in the
    Codex transcript ledger
- Treat uncertain or incomplete Codex records as diagnostics instead of guessing
  and corrupting history order.
- Prefer JSONL file order over timestamps. Timestamps are for display and
  diagnostics only.
- If Codex state proves that a previously interrupted turn completed, queue the
  missing transcript/output through the Teams outbox. If Codex execution is
  ambiguous, do not rerun it automatically; record an interrupted state and
  notify the owner according to `M12`.

Constraints:

- Do not automatically import every historical session.
- Do not call Codex model APIs just to reconstruct history.
- Do not rewrite old Teams messages; append ordered catch-up messages.
- Do not infer import checkpoints from rendered message text.
- Do not use timestamp-only or content-only dedupe for confident import. If a
  stable transcript item id is unavailable, fall back to a diagnostic state
  instead of guessing and corrupting the Teams transcript.
- Do not advance an import or catch-up checkpoint until the corresponding Teams
  outbox item is sent.

Tests:

- Existing Codex session is listed but not mirrored until requested.
- Publish creates a Teams chat and imports title, user turns, and assistant
  turns in order.
- Re-running publish resumes from the checkpoint without duplication.
- Local Codex turns added after Teams linkage are caught up in order.
- Teams-origin turns are not re-imported as local history duplicates.
- Import failure leaves a retryable checkpoint and does not mark missing output
  as sent.
- Half-written JSONL tail records are not imported and do not advance
  checkpoints.
- Teams-origin prompt appears in Codex JSONL but is not re-imported as a local
  user message.
- Existing pending ACK/final outbox is flushed or kept ahead of local catch-up
  so catch-up cannot reorder the conversation.
- Linked-session catch-up discovers a local Codex turn within the target refresh
  window and appends it after any older pending Teams-origin ACK/final outbox.
- Ambiguous local turn state is surfaced as interrupted and does not create a
  duplicate Codex execution.

Status:

- Partial implementation completed:
  - local Codex JSONL transcript parser with stable fallback item ids,
    diagnostics for bad/half-written lines, and file-order preservation
  - `continue <number|session-id>` bridge path that creates a Teams work chat
    only on explicit user request and imports existing transcript records in
    order
  - existing published sessions are detected by Codex thread id and are not
    recreated
  - durable import checkpoints are written after transcript outbox sends
  - linked active sessions are reconciled on the bridge poll loop; new local
    Codex transcript records are appended to Teams in file order
  - helper startup recovers unfinished local state: queued turns are rerun from
    the original Teams message, while ambiguous running turns are marked
    interrupted and notify the work chat with a `helper retry` command
  - Teams-origin user prompts are not re-imported as local transcript catch-up
    duplicates when the durable inbound ledger has the matching normalized text
    hash
- Still pending:
  - half-written local JSONL recovery tests at the bridge integration layer
  - richer transcript renderer labels once all outbox sends use the renderer

## M12: Notifications, Teams Rendering, And Automatic Artifact Handoff

Owner: UX/transport worker after `M3`, `M4`, and `M9`.

Suggested write scope:

- `internal/teams/text.go`
- `internal/teams/graph.go`
- `internal/teams/bridge.go`
- `internal/teams/outbound_attachments.go`
- focused renderer, mention, and artifact tests

Responsibilities:

- Implement durable owner-notification metadata on outbox messages and render it
  as a Graph self-mention at send time.
- Mention the owner for:
  - completed long-running turns
  - interrupted/stuck turns that need action
  - selected service/auth/recovery failures
- Use a default long-running threshold before completion mentions, for example
  `>=60s`, and record per-turn notification state so replay does not mention
  again.
- Do not mention for:
  - ACKs
  - routine dashboard/status output
  - imported historical transcript
  - every chunk of a long output
- Improve Teams rendering while staying simple and safe:
  - clear labels for user vs assistant vs helper/system text
  - readable code blocks where Teams HTML supports them
  - safe HTML escaping
  - deterministic part labels for long chunks
  - no attempt to expose hidden model reasoning; render only visible transcript
    categories available in Codex output/history
- Add a helper artifact manifest protocol for Codex output:
  - prefer a fixed helper artifact root and fixed manifest filename so Teams
    metadata does not change every Codex turn
  - inject the manifest instruction through helper/runner plumbing when the
    runner supports it; if the stable runner can only put the instruction in
    the user prompt, keep automatic artifact handoff gated and use explicit
    `/send-file` for MVP
  - preserve the model's visible reply text; do not rely on filtering it out
  - accept only explicit manifest entries under allowed outbound roots
  - hash/stage/upload artifacts with collision-resistant names
  - use the current `M9` default remote upload folder,
    `Microsoft Teams Chat Files`, unless the operator explicitly overrides it
  - send files/images through the `M9` upload path
  - send the assistant text before artifact links in the same per-chat sequence

Constraints:

- Do not let model output create arbitrary Teams mentions or HTML.
- Do not automatically upload arbitrary filesystem paths.
- Do not hide model-visible artifact metadata if filtering would risk losing
  user-visible information; prefer robust sidecar parsing.
- Do not notify the owner for replayed historical messages.
- Do not process absolute paths, `..`, symlinks, directories, oversized files,
  or disallowed extensions from an artifact manifest.
- Do not let artifact handling inject dynamic Teams ids, message ids, upload
  paths, or routing metadata into every Codex prompt.

Tests:

- Self-mention payload targets only the authenticated owner.
- Completion reminder is sent once and not duplicated on outbox replay.
- Long output mentions at most once according to policy.
- Renderer escapes HTML and keeps each chunk below the conservative byte limit.
- Artifact manifest accepts allowed generated files and rejects paths outside
  the outbound root, symlinks, oversized files, and disallowed extensions.
- Artifact upload names include stable session/turn context plus a
  collision-resistant digest or suffix, and uploads default to the Teams
  attachment folder rather than an arbitrary OneDrive location.
- Uploaded artifact messages preserve order relative to the assistant text.
- Manifest missing or invalid does not hide or block the assistant text; it
  creates a helper warning outbox item when needed.

Status:

- Partial implementation completed:
  - safe Teams renderer and chunk planner pure functions with tests
  - owner-mention policy helpers
  - Graph `mentions` send primitive for authenticated owner self-mentions
  - bridge outbox integration for `MentionOwner` metadata
  - long-running turn completion mentions on the first final-output chunk only
  - helper artifact manifest parser/validator pure functions with upload-name
    seed generation
  - Codex Teams prompts include a stable artifact-manifest contract pointing to
    the fixed local Teams outbound root
  - bridge preserves the assistant's visible artifact manifest text, then
    uploads listed files through the `M9` OneDrive/Teams attachment path with
    session/turn/hash upload names
  - live outbound attachment smoke passed against the existing Teams work chat
    using the cached delegated token
- Still pending:
  - using the renderer for every bridge outbox message instead of the legacy
    `HTMLMessage` wrapper
  - product UX for configuring long-running notification threshold
  - richer artifact UX for large-file upload sessions and broad file scopes

## M13: Cache, Rate-Limit, And Schema-Migration Hardening

Owner: reliability worker. Can start once target records are defined; migration
must land before product rollout.

Suggested write scope:

- new cache/index files under `internal/teams`
- `internal/teams/store`
- transport scheduler tests
- docs when migration semantics change

Responsibilities:

- Move durable delivery/ingestion state out of cache and into schema v2:
  machine identity, control chat binding, workspace/session numbering,
  chat/session/thread mapping, inbound ledger, transcript ledger, outbox
  ledger, Graph ingestion checkpoint, import/catch-up checkpoints, per-chat
  sequence, and rate-limit retry state.
- Cache only derived data that can be rebuilt from durable truth:
  - workspace discovery indexes
  - Codex session metadata
  - Teams chat/session registry projections
  - dashboard search/sort projections
  - Graph diagnostic windows and full-window warning details
  - rendered previews
  - upload-hash acceleration when the artifact/upload ledger is authoritative
- Use atomic writes, schema versions, source fingerprints, generated timestamps,
  TTLs, and safe invalidation.
- Make cache refresh strategy explicit:
  - short TTL or mtime checks for linked active sessions
  - longer stale-while-revalidate TTL for dashboard/search projections
  - full rebuild on schema/source-fingerprint mismatch
  - no checkpoint advancement based only on derived cache contents
- Make bad cache non-fatal. Ignore and rebuild corrupted derived caches; fail
  only when durable state itself is corrupt.
- Add a per-chat outbound scheduler that respects Teams/Graph rate limits and
  `Retry-After`.
- Add a per-chat inbound polling scheduler. Do not scan every active chat on
  every loop. Each chat stores durable `next_poll_at`, `last_activity_at`,
  `state`, `blocked_until`, and continuation metadata, and the loop polls only
  due chats within the global read budget.
- Default inbound polling states and thresholds:

  | State | Trigger | Target interval | Downgrade |
  | --- | --- | --- | --- |
  | `hot` | user just sent a message, work chat just resumed/created, or helper just sent a final answer | `1s`, respecting per-chat 1 rps | after `2 min` idle |
  | `running` | Codex turn is running | `3s` | turn completion moves to `hot`; no user/helper activity for `15 min` moves to `warm` unless the turn is still running |
  | `warm` | recent conversation, no active turn | `5s` | after `15 min` idle |
  | `cool` | quiet but still likely to be resumed soon | `10s` | after `4h` idle |
  | `cold` | old conversation kept as low-frequency listener | `30s` | after `48h` idle |
  | `parked` | explicitly closed/parked or idle for `48h` | not polled | user sends stable `r <hash>` in control chat |
  | `catchup` | startup, reconnect, resume, or continuation path exists | budgeted ASAP, top `50` | after catch-up completes, return by latest activity |
  | `blocked` | Graph 429 or transport backoff | not polled | `blocked_until`/`Retry-After` expires, then return to prior state |

- Remove the earlier `deep_cold` tier. `cool` is the middle tier between
  `warm` and `cold`; `parked` starts at `48h` idle by default.
- Control chat has its own schedule and never parks: `1s` for `2 min` after a
  control message, otherwise `5s`, except when blocked by `Retry-After`.
- Keep ordering guarantees across long chunks, imported history, artifact
  messages, ACKs, and notifications.
- Add explicit migrations for new state fields before enabling features that
  depend on them.
- Migrate the current cache registry into durable state and stop depending on
  it for routing, seen ids, sent ids, or next session numbers.
- Accept at-least-once Graph sends. Use body hashes, sequence, send leases, and
  diagnostics to make duplicate risk visible, but do not try to repair it by
  rerunning Codex.

Constraints:

- Cache must never be the only source of session truth.
- Cache misses must not cause duplicate Codex turns or duplicate Teams sends.
- Do not cache secrets, full message bodies beyond what durable outbox already
  intentionally stores, or unnecessary full local paths.
- Migrations must be forward-compatible with missing fields and fail closed on
  unsupported schema versions.
- A corrupted derived cache is rebuilt. A corrupted durable store fails closed
  and requires recovery or backup inspection.

Tests:

- Corrupted derived cache is ignored and rebuilt.
- Stale workspace/session cache refreshes without changing stable numbers
  unnecessarily.
- Rate-limit scheduler preserves order through simulated 429/Retry-After.
- Schema migration from the current state version initializes new fields without
  changing existing sessions, turns, or outbox sent markers.
- Unsupported future schema versions fail with a clear diagnostic.
- v1 store plus registry fixture migrates to v2 and survives registry deletion.
- Graph ingestion checkpoint is not advanced on poll error or 429.
- Import checkpoint advances only after sent outbox.
- Per-chat sequence allocation has no duplicates under concurrent queueing.
- Active-session cache refresh does not miss a newly-written JSONL record after
  an mtime change, and stale dashboard cache refreshes without changing durable
  selection ids.

Status:

- Partial implementation completed:
  - state schema v2 semantic backbone fields for machine/control/workspace/view,
    transcript/import ledgers, per-chat sequence, rate-limit state, artifact
    records, and notification records
  - v1 state migration initializes v2 metadata without losing existing sessions,
    turns, or outbox records
  - outbox records now receive per-chat sequence, part metadata, rendered hash,
    ack/notification/artifact metadata fields, and send-error diagnostics
  - bridge long-output path persists all chunks before the first send
  - pure send scheduler and derived-cache policy models with tests
  - direct helper sends now queue through durable outbox before Graph send
  - Graph 429 failures record a per-chat rate-limit block so one throttled chat
    does not block other chats
  - successful Graph sends are marked `accepted` with the Teams message id
    before final `sent`, so restart recovery can promote accepted messages
    without posting again
  - drain/upgrade unfinished-work checks include queued, sending, and accepted
    outbox states
  - bridge startup can restore the legacy registry projection from durable
    state, so cache-registry loss does not break control chat binding, session
    routing, seen inbound ids, or sent outbox ids
- Completed P1 scheduler/retry slice:
  - direct sends respect durable per-chat rate-limit blocks before claiming an
    outbox lease
  - Graph 429 records the blocking outbox id in durable chat rate-limit state
  - global outbox flushing continues to unblocked chats after one chat is
    throttled, preserving cross-chat progress
  - ACK 429s leave the ACK retryable but do not let a low-value ACK block the
    final Codex reply behind a durable chat block
- Still pending:
  - broader stress tests against live Graph throttling behavior and real
    recovery windows

## M8: Broad Verification Matrix

Owner: verification worker and lead.

Write scope:

- Tests only, plus docs if gaps are found.

Responsibilities:

- Build a feature-level test matrix across:
  - direct mode
  - proxy mode
  - yolo mode
  - custom `CODEX_HOME` / `CODEX_DIR`
  - root/sudo identity behavior
  - WSL auth/browser handoff
  - managed Codex install
  - token refresh
  - Graph throttling
  - Graph message-size splitting
  - owner self-mention notification payloads
  - crash/restart recovery
  - duplicate Teams messages
  - outbox retry
  - machine control chat uniqueness
  - workspace/session numbering stability
  - historical session import checkpoints
  - Teams/local mixed-use reconciliation
  - owner self-mention notification policy
  - rendered Teams HTML and artifact handoff
  - derived-cache rebuild and migration behavior
  - cached token usage parsing
  - command parsing ambiguity
  - control-chat current-view numbering expiry
  - terminal close or SSH disconnect recovery
  - stale service lock recovery
  - service environment differences from interactive shell
  - Graph accepted-send followed by local `MarkOutboxSent` failure
  - per-chat `Retry-After` blocking one chat while other chats continue
  - corrupted derived cache rebuild vs corrupted durable store fail-closed
  - local Codex JSONL half-write and stable transcript item ids
  - Teams-origin prompt dedupe during local history catch-up
  - artifact manifest path escape, symlink, oversize, and restart retry
- Run local tests:
  - `go test ./...`
  - targeted managed-install integration when launch paths change
  - Windows compile check when CLI or path behavior changes

Constraints:

- Tests should not call live Graph or Codex model APIs by default.
- Live smoke tests must require explicit opt-in environment variables.
- Keep secrets out of test logs.

Status:

- Passing locally for the current integration slice:
  - `go test ./...`
  - `GOOS=windows GOARCH=amd64 go test -exec=/bin/true ./...`
  - `CODEX_INSTALL_TEST=1 go test ./internal/cli -run TestEnsureCodexInstalledIntegrationManagedNode -count=1 -v`
- Completed opt-in smoke hooks:
  - `teams doctor --live` checks Graph `/me` and configured control-chat read access only when requested.
  - `teams doctor --appserver-probe` checks local Codex app-server protocol compatibility without starting a model turn.
  - `TestLiveGraphSmokeOptIn` is skipped by default and runs only with `CODEX_HELPER_TEAMS_LIVE_TEST=1`; optional `CODEX_HELPER_TEAMS_LIVE_CHAT_ID` checks chat read only after the Jason-Wei-only safety gate passes.
  - `TestLiveGraphOutboundAttachmentOptIn` is skipped by default and runs only with `CODEX_HELPER_TEAMS_LIVE_OUTBOUND_TEST=1` plus `CODEX_HELPER_TEAMS_LIVE_CHAT_ID`.
  - `TestLiveBridgeSendFileDurableOutboxOptIn` is skipped by default and runs
    only with `CODEX_HELPER_TEAMS_LIVE_BRIDGE_OUTBOUND_TEST=1` plus
    `CODEX_HELPER_TEAMS_LIVE_CHAT_ID`; it exercises the product bridge
    `/send-file` durable outbox path against live Graph. Outbound live tests
    validate the target chat with the normal chat token before creating a local
    test file or uploading anything with the file-write token.
  - Earlier live `/me`, message-size, self-mention, and outbound-attachment
    probes passed on this workstation. Current live revalidation is blocked
    until fresh profile-scoped chat and file-write token caches are restored.
  - Bounded backlog tests cover Graph page-cap truncation and bridge full-window diagnostics so large recovery windows are visible instead of silently treated as complete.
  - Subagent stress review plus local follow-up covered `-race`, high-count repeated tests, path traversal, token cache safety, Graph pagination/retry, attachment upload/download, owner recovery, pause/drain, outbox retry, service restart, and upgrade compatibility.
  - Live Teams chat message-size probe found `102,289` bytes accepted and
    `102,290` bytes rejected for HTML `body.content`; local tests now cover
    rendered-HTML-byte chunking.
  - Live Teams self-mention probe sent successfully, and the user confirmed
    that Teams produced a notification for the self-mention.
  - Current post-integration verification passed:
    `go test ./...`, `go test -race ./internal/teams ./internal/teams/store`,
    `go test ./internal/teams ./internal/teams/store -count=20`,
    `GOOS=windows GOARCH=amd64 go test -exec=/bin/true ./...`,
    `teams doctor --live --appserver-probe`, `TestLiveGraphSmokeOptIn`, and
    `TestLiveGraphOutboundAttachmentOptIn`.
  - Current P0 follow-up verification passed:
    `go test ./internal/teams ./internal/teams/store -count=1`,
    `go test ./...`, `go test -race ./internal/teams ./internal/teams/store`,
    `go test ./internal/teams ./internal/teams/store -count=20`, and
    `GOOS=windows GOARCH=amd64 go test -exec=/bin/true ./...`.
  - Current P1 local verification passed:
    `go test ./internal/teams/store ./internal/teams ./internal/codexrunner ./internal/cli -count=1`,
    `go test ./...`, `go test -race ./internal/teams ./internal/teams/store ./internal/codexrunner`,
    `go test ./internal/teams ./internal/teams/store ./internal/codexrunner -count=20`,
    and `GOOS=windows GOARCH=amd64 go test -exec=/bin/true ./...`.
  - Current app-server cold probe passed on this workstation:
    `teams doctor --appserver-probe --appserver-probe-runs 3` reported min
    `804ms`, max `831ms`, total `2.458s`.
  - Current P0/P1 local and skip-gated completion verification passed:
    `go test ./internal/teams -run 'TestBridgeSessionSendFile' -count=1`,
    `go test ./internal/teams -run 'TestBridgeSessionSendFile(CommandUploadsOutboundAttachment|AttachmentUsesDurableOutboxOnRateLimit|QueuesDurableOutboxBeforeUpload)|TestBridgeUploadsArtifactManifestFromCodexResult' -count=1`,
    `go test ./internal/teams -run 'TestLive(GraphSmokeOptIn|GraphOutboundAttachmentOptIn|BridgeSendFileDurableOutboxOptIn)' -count=1` (default skip-gated behavior only unless live env vars are set),
    `go test ./internal/cli -run 'TestTeamsService(DoctorReportsWSLHint|InstallWritesWSLWindowsTask|WSLTaskNameSeparatesUsersAndProfiles)|TestScheduleDelayedTeamsServiceStartUsesWSLWindowsTask' -count=1`,
    `go test ./...`,
    `go test -race ./internal/teams ./internal/teams/store ./internal/codexrunner`,
    `GOOS=windows GOARCH=amd64 go test -exec=/bin/true ./...`,
    `GOOS=darwin GOARCH=amd64 go test -exec=/bin/true ./...`, and
    `teams doctor --appserver-probe --appserver-probe-runs 1`.
  - Current live revalidation status: blocked on fresh profile-scoped auth.
    `teams auth status` and `teams auth file-write-status` both report missing
    token caches under `~/.cache/codex-helper/teams/profiles/default/`.
    `CODEX_HELPER_TEAMS_LIVE_TEST=1 go test ./internal/teams -run
    TestLiveGraphSmokeOptIn -count=1` fails before any Graph call because the
    chat token cache is absent. A device-code login was started for the chat
    token but did not complete before the local wait was cancelled.
  - Current app-server revalidation passed on the only Codex path currently
    available on this workstation: `/home/baka/.npm-global/bin/codex`
    reports `codex-cli 0.125.0`, and `teams doctor --appserver-probe
    --appserver-probe-runs 5` reported 5 successful cold probes with min
    `661ms`, max `680ms`, total `3.361s`.
  - Current pre-v2 state fixture follow-up passed: the local POC
    `schema_version: 1` state shape found under
    `~/.config/codex-helper/teams/state.json` was sanitized into
    `TestLoadMigratesLocalPOCV1StateShape`, covering active sessions,
    completed turns, inbound events, and service-control timestamps.
  - Current safety/reliability follow-up passed:
    `go test ./internal/teams -run 'TestGraphAllowlistRejectsUnexpectedEndpoints|TestGraphGetChatAndListMembers|TestLiveJasonWeiSingleMemberChatValidation|TestBridgeEnsureControlChatQueuesReadyMessageDurably|TestBridgeFlushPendingOutboxDoesNotOvertakeFreshSendingMessage|TestBridgeSessionSendFileQueuesDurableOutboxBeforeUpload' -count=1`
    and
    `go test ./internal/cli -run 'TestTeamsServiceDoctorReportsWSLHint|TestTeamsServiceDoctorReportsWSLReadinessFailure' -count=1`.
  - Current post-safety stress verification passed:
    `go test ./...`,
    `go test -race ./internal/teams ./internal/teams/store ./internal/codexrunner -count=3`,
    `go test ./internal/teams ./internal/teams/store ./internal/codexrunner ./internal/cli -count=10`,
    `GOOS=windows GOARCH=amd64 go test -exec=/bin/true ./...`,
    `GOOS=darwin GOARCH=amd64 go test -exec=/bin/true ./...`,
    `go run ./cmd/codex-proxy teams service doctor`, and
    `go run ./cmd/codex-proxy teams doctor --appserver-probe --appserver-probe-runs 3`.
- Environment-dependent validation still pending:
  - broader Codex app-server compatibility/performance probes across additional
    installed versions or explicit paths
  - deeper live-product tests for owner self-mention reminders
  - broader v1 registry/state to v2 durable identity and ledger migration
    fixtures from real local states
  - end-to-end product tests for multi-machine control chats, mixed Teams/local
    usage, automatic artifact handoff, and derived-cache rebuilds

## Current Merge Order

1. `M0` tracker.
2. `M1` state package.
3. `M2` runner interface and `ExecRunner`.
4. `M3` transport hardening.
5. `M1b` owner heartbeat/stale-lock metadata.
6. `M2b` CLI launch adapter for existing Codex launch behavior.
7. `M4` bridge integration.
8. `M5` CLI operations.
9. `M8` broad verification pass.
10. `M7a` foreground owner heartbeat and clean exit.
11. `M6` app-server experimental runner.
12. `M7` background service and upgrade integration.
13. `M9` explicit outbound attachment send.
14. `M13a` state schema v2 migration for machine/account/profile identity,
    workspace/session indexes, dashboard view state, transcript ledgers,
    import/catch-up checkpoints, per-chat sequence, outbox part metadata, ACK
    kind, artifact/upload skeleton, and owner-notification intent.
15. `M3/M4 outbox scheduler slice`: render/split/persist-all, per-chat FIFO,
    send leases, chat-level `Retry-After`, accepted-send/mark-sent diagnostics,
    and all bridge messages through outbox.
16. `M3/M4 notification slice`: Graph self-mention send primitive, durable
    notification outbox metadata, and Teams-origin ACK behavior.
17. `M10` machine control chat, title/privacy policy, current-view numbered
    dashboard navigation, explicit discovery refresh strategy, and
    create/open/publish session workflows.
18. `M11` local Codex transcript parser, historical session import, and
    cross-surface reconciliation with defined catch-up/recovery boundaries.
19. `M12` rendering polish and gated automatic artifact manifest handoff.
20. `M13b` derived-cache rebuild, rate-limit scheduler stress, and migration
    stress tests.

## Current Blockers

- `M6` depends on Codex app-server protocol compatibility and must stay optional until more versions are probed.
- Threat-model decisions are documented in `docs/teams_security_threat_model.md`; keep that file updated before service mode becomes default.
- Background service install/start/stop/restart exists for Linux systemd user services, and helper upgrade handles active-service stop/start around the binary replacement.
- `AppServerRunner` remains optional and experimental; the current installed Codex app-server probe passes, but broader version compatibility and latency probes are still pending.
- Explicit outbound upload transfer now has an MVP behind `teams send-file` and
  `/send-file`; automatic generated-artifact handoff is implemented for the
  narrow manifest contract under the Teams outbound root. Large-file upload
  sessions, broad local-path upload UX, running-turn interrupt for the stable
  exec runner, and streaming output remain deferred.
  Ordinary inbound Teams reference files are supported only through the narrow
  SharePoint/Graph `/shares` path with file scope enabled.
- Long-message transport is now constrained by the measured Teams HTML body
  limit and uses conservative chunking. Owner self-mention reminders are
  experimentally verified and should continue to use durable outbox metadata,
  not ad hoc HTML injected into message bodies.
- The next product phase depends on broader explicit migration fixtures and
  stress coverage. Do not add new machine/workspace indexes, history-import
  checkpoints, or notification behavior as ad hoc fields without migration
  tests.
- Historical import and mixed-use reconciliation depend on a local Codex JSONL
  transcript parser with stable item ids. Runner `ReadThread`/`ListThreads`
  are useful enrichments, but unsupported runner read/list must not block the
  product path when local session files are available. Teams-origin prompt
  dedupe now has a normalized-text durable inbound fallback, but broader real
  transcript fixtures are still needed.
- Automatic artifact handoff now uses a fixed manifest contract and does not
  filter the assistant's visible manifest text. It still depends on delegated
  file-write auth and remains limited to files under the Teams outbound root.
- Slack and other chat transports are not a shortcut around Teams permission
  limits in this plan. They require a separate transport adapter, app/permission
  path, and threat model before implementation.
