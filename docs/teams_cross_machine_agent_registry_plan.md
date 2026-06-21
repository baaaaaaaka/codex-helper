# Teams Cross-Machine Agent Registry Plan

Status: registry/store foundation, per-machine inbox locators, model-facing `cxp delegate` command surface, inbox-backed delegation records, remote thread new/reuse tokens, delegation reducer/idempotency rules, prompt hint, built-in skill reference, active-helper machine heartbeat publishing, target-side delegated worker, execution fencing, claim recheck, effective write-auth fallback, paged inbox/registry reads, sparse progress/question records, local retention pruning, and CI coverage are implemented. Remaining hardening is live Graph load validation.

Prototype evidence:

- `internal/teams.GraphClient.CreateOrGetMeetingChat` can use `onlineMeetings/createOrGet` with a stable `externalId` and return the meeting chat thread id.
- `internal/teams.GraphClient.CreateOrGetMeetingChatWindow`, `GetOnlineMeeting`, and `UpdateOnlineMeetingWindow` provide the long-window create/get/refresh surface needed to keep the registry meeting writable.
- `internal/teams/machineregistry` owns the reusable machine-card schema, per-machine inbox locator, liveness observation, PATCH-slot heartbeat publisher, stable registry ids, and low-write local cache.
- `internal/teams/delegation` owns task specs, candidate tokens, remote thread tokens, deterministic request ids, claim/result fencing, progress/question status records, cancel/expiry handling, loop prevention, and content-sensitive local state for tests/debug.
- `cxp delegate resolve/start/status/wait/cancel/claim/progress/question/result --json` is the model-facing command contract. `resolve` returns compact thread reuse candidates and a new-thread token; `start` binds exactly one chosen token. Active `cxp teams run` helpers publish machine capability heartbeats automatically; `cxp delegate machine publish-once` and `cmd/teams-registry-probe` are debug/admin surfaces, not the normal model path.
- The built-in `cxp` skill now points cross-machine natural-language requests to `references/delegation.md`, so the normal Teams prompt only needs a small capability hint.
- `cmd/teams-registry-probe` proved that Docker containers can converge on the same Teams-only registry chat, publish machine heartbeats, and classify a stopped container as stale.
- `cmd/teams-registry-probe --page-size` follows Graph pagination instead of treating the first page as the registry window; `--top` remains a deprecated alias for compatibility.
- The probe now uses a per-machine heartbeat slot during each run: the first heartbeat appends the slot message, and later heartbeats patch that same message. Append is only a replacement path when the slot patch fails.
- Probe settings: `heartbeat=15s`, `ttl=45s`, A ran for 105s, B ran for 35s, watcher ran for 110s.
- Probe result: all containers observed the same registry chat hash; B moved from online to stale after its last heartbeat aged past the TTL.

## Goals

- Let a Teams Work chat naturally delegate part of a task to another `cxp` agent running on another machine.
- Keep the user-facing trigger as natural language. The user should be able to say "ask the Windows machine to check this" or "let B look at the GPU side" without remembering a command.
- Keep the transport Teams-only for the first production version. Do not require a new server, database, cloud app, or non-Teams storage.
- Let each machine publish a capability card and heartbeat so the resolver can answer "which machines are currently usable?"
- Prevent agent-agent loops, duplicate delegation, and unbounded back-and-forth.
- Keep Teams/Graph tokens local to machines. Registry payloads must never contain tokens, raw chat ids for unrelated chats, refresh tokens, full paths, or secrets.

## Non-Goals

- Do not make this a general distributed filesystem or durable knowledge store.
- Do not use the hidden registry chat as a long-term audit log. Machine cards and heartbeats are ephemeral leases.
- Do not let a remote machine directly control another machine's helper service lifecycle.
- Do not make explicit user commands the primary product experience. CLI commands are allowed for debug and recovery only.
- Do not use broad chat search as the normal way to find the registry chat.
- Do not add a helper-side intent detector before Codex starts. The model should decide when delegation is useful, guided by the prompt hint and the `cxp` skill.

## Core Shape

Each `cxp` installation uses the same deterministic registry locator:

```text
tenant_id + user_id + registry_kind -> externalId
Graph /me/onlineMeetings/createOrGet(externalId) -> onlineMeeting
onlineMeeting.chatInfo.threadId -> Teams registry chat
```

The registry chat is only the discovery and heartbeat plane. Machines publish compact capability records there:

- registry marker
- machine capability card
- heartbeat / liveness update
- per-machine inbox locator

The model does not scan Teams. The helper exposes a resolver surface that returns a small candidate list.

```text
User natural language
  -> Agent A recognizes delegation need
  -> helper resolver reads Teams registry
  -> Agent A sees top candidates only
  -> helper posts delegation envelope to target machine inbox
  -> Agent B claims envelope from its inbox and works
  -> Agent B posts result/progress back to the same inbox
  -> Agent A incorporates result and answers user
```

## Registry Locator

Use a stable, opaque helper-derived external id. The exact hash width is an implementation detail; callers should route through the registry helper instead of reconstructing ids:

```text
registryKey = machineregistry.RegistryKey(tenant_id, user_id)
externalId = machineregistry.ExternalID(registryKey)
```

The subject should be recognizable for diagnostics but must not be the routing key:

```text
CXP Registry
```

Rules:

- Every machine first tries its local cache of `registry_chat_id`.
- Cache hits must be validated by a lightweight Graph read.
- Cache misses call `CreateOrGetMeetingChat`.
- `createOrGet` is the normal rendezvous; `/me/chats` scanning is repair-only.
- If `createOrGet` returns a new meeting after old meeting expiry, treat that as a new registry generation and republish current cards.
- Refresh the online meeting before expiry by patching the meeting window. The current registry store default is a 45-day window and a 7-day refresh cadence; production can tune this without changing the message schema.

The local cache should store:

```json
{
  "schema_version": 1,
  "tenant_id_hash": "...",
  "user_id_hash": "...",
  "registry_key": "...",
  "external_id": "...",
  "meeting_id": "...",
  "registry_chat_id": "...",
  "registry_generation": "...",
  "slot_message_id": "...",
  "slot_machine_id": "...",
  "validated_at": "...",
  "refreshed_at": "...",
  "next_refresh_at": "..."
}
```

Do not print the real chat id in ordinary status output. Use a short hash unless the user runs an explicit diagnostic command.

Cache writes must be content-sensitive: if the serialized cache has not changed, do not rewrite the file. Normal heartbeat PATCHes must not touch local disk. Only stable registry identity changes, replacement slot creation, capability fingerprint changes, or meeting refresh state changes should write the cache, using a temp file plus atomic rename.

## Machine Inbox

Each accepting machine creates or reuses a per-machine hidden meeting chat before it publishes an accepting heartbeat:

```text
registry_key + machine_id + inbox_kind -> inbox externalId
Graph /me/onlineMeetings/createOrGet(externalId) -> onlineMeeting
onlineMeeting.chatInfo.threadId -> Teams inbox chat
```

The machine card publishes `inbox_ref` and `inbox_generation`. `inbox_ref` is the stable meeting external id; it is enough for another signed-in helper to open the same hidden inbox through Graph `createOrGet`. The raw chat id stays in the local cache and debug output.

The inbox is append-only:

- A appends `request` and `tombstone`.
- B appends `claim`, `status`, and `result`.
- No helper patches inbox records.
- Registry heartbeat PATCHes and inbox appends are separate Graph messages/chats, so they cannot overwrite each other.

## Capability Card

Each machine publishes a card on startup, on material capability changes, and periodically as a low-frequency refresh.

```json
{
  "kind": "cxp.machine-card.v1",
  "registry_key": "cxp-registry-v1",
  "machine_id": "m_windows_4090",
  "machine_label": "Windows 4090",
  "aliases": ["B", "win-gpu"],
  "helper_profile": "default",
  "cxp_version": "0.x.y",
  "platform": {
    "os": "windows",
    "arch": "amd64",
    "wsl": false
  },
  "capabilities": ["gpu", "windows", "fgx", "codex"],
  "workspaces": [
    {
      "kind": "git",
      "name": "codex-helper",
      "root_hash": "..."
    }
  ],
  "skills": ["cxp", "fgx-tin-workflow"],
  "model_profiles": ["gpt-5.5-medium"],
  "load": {
    "running_turns": 0,
    "queued_turns": 0
  },
  "heartbeat_interval_seconds": 30,
  "ttl_seconds": 90,
  "published_at": "...",
  "expires_at": "...",
  "revision": 42
}
```

Rules:

- No tokens, absolute secret paths, raw prompt text, or unrelated chat ids.
- Workspace data should be fingerprints and names, not full path dumps.
- If the machine has sensitive local paths, publish path aliases or repo names only.
- The card is advisory; the resolver must still check liveness and load before auto-selecting it.

## Heartbeat And Availability

Production defaults:

- heartbeat interval: `5m`
- online TTL: `15m`
- stale TTL: `2h`
- jitter: +/- 20% on publish and refresh timers

State classification:

```text
online: last heartbeat age <= 15m
stale: 15m < age <= 2h
offline: age > 2h or explicit tombstone
draining: machine says it is finishing work but not accepting new delegation
busy: online but above load threshold
```

Auto-delegation may use only `online` and `accepting=true` machines. `stale` machines may be shown only as explanatory candidates, never auto-selected.

Heartbeat publication must use per-machine slot messages:

- machine stores its own slot `message_id` locally
- normal heartbeat patches that slot instead of sending a new message
- if patch fails because the message disappeared or the local cache is lost, append a new slot
- resolver picks latest by `machine_id`, `revision`, and `published_at`
- old slots are ignored and can be compacted later

This avoids high message volume while remaining recoverable.

## Resolver

The model should not receive the whole registry. The helper exposes a resolver tool or internal command:

```text
resolve_delegate_target(query, current_workspace, current_task, hints)
```

Return only the top candidates:

```json
{
  "candidates": [
    {
      "machine_id": "m_windows_4090",
      "label": "Windows 4090",
      "aliases": ["B"],
      "state": "online",
      "accepting": true,
      "confidence": 0.86,
      "matched_reasons": [
        "user mentioned B",
        "machine alias contains B",
        "has Windows and GPU capabilities"
      ],
      "safe_to_auto_delegate": true
    }
  ],
  "decision": "auto"
}
```

Decision policy:

- confidence >= 0.80 and exactly one strong candidate: auto-delegate
- confidence 0.50-0.80 or multiple strong candidates: ask one clarification
- confidence < 0.50: do not delegate
- stale/offline candidates never auto-delegate

Scoring inputs:

- direct alias match
- OS/GPU/capability match
- workspace/repo match
- recently successful delegation for this user
- machine availability
- load and current queue
- model profile availability
- negative signals from recent failures or rate limits

## Natural-Language Trigger

The primary trigger is not a user command. The user says things like:

- "让 B 看一下 Windows 这边"
- "去另一台 GPU 机器上确认这个 kernel"
- "让实验室那台 Linux agent 查一下日志"

Implementation:

- Teams Work chat prompt includes only a small capability hint: cross-machine delegation exists and is documented by the `cxp` skill.
- The built-in `cxp` skill tells Codex to load `references/delegation.md` for natural-language cross-machine requests.
- Codex calls model-facing commands when useful:
  - `cxp delegate resolve --query <text> --json`
  - `cxp delegate start --candidate-token <token> --new-thread-token <token> --task-file <path> --json`
  - `cxp delegate start --candidate-token <token> --thread-token <token> --task-file <path> --json`
  - `cxp delegate status --id <delegation_id> --json`
  - `cxp delegate wait --id <delegation_id> --timeout <duration> --json`
  - `cxp delegate cancel --id <delegation_id> --json`
- Resolver output is compact JSON with an `action`, a candidate token when safe, a fresh remote-thread token, reusable remote-thread summaries, and top candidate summaries. Raw registry messages are not injected into the model prompt.
- If `resolve` returns `ask_user`, Codex asks one clarification in natural language. If it returns `do_not_delegate`, Codex continues locally or explains why no usable remote machine is available.

## Remote Thread Reuse

Remote thread reuse is deliberately model-mediated, not hard-coded by a helper-side intent detector:

- The helper returns at most a few reuse candidates for the selected target machine.
- Each reuse candidate has a short title, summary, last result summary, source session, workspace fingerprint, confidence reasons, and a short-lived `thread_token`.
- The helper also returns a short-lived `new_thread_token` for the same candidate, so the model can choose a fresh remote thread when reuse is uncertain.
- `start` accepts exactly one of `--new-thread-token` or `--thread-token` and validates target machine, inbox generation, workspace fingerprint, local known thread state, active status, and thread generation.
- The target helper uses `remote_thread_id` as the Codex session id, giving the remote agent a stable context key across related delegations.
- If the reused context is wrong, Agent B replies with `CXP_REUSE_REJECTED: <reason>`; the helper publishes a `reuse_rejected` terminal result and Agent A retries with a new thread.

This keeps prompt cost low: the model sees only compact candidate summaries and tokens, while detailed registry and inbox state stays in local helper stores.

## Delegation Envelope

Delegation is a structured record in the target machine inbox, not free-form chat between agents and not a registry message.

```json
{
  "kind": "cxp.delegation.request.v1",
  "delegation_id": "del_...",
  "source_key": "src_...",
  "machine_id": "m_b",
  "source_session_id": "s123",
  "source_turn_id": "turn_456",
  "parent_id": "",
  "path": ["m_a", "m_b"],
  "inbox_ref": "cxp-inbox-...",
  "inbox_generation": "...",
  "remote_thread_id": "rth_...",
  "thread_policy": "new",
  "thread_generation": "gen_...",
  "created_at": "...",
  "expires_at": "...",
  "spec": {
    "title": "Check Windows GPU repro",
    "objective": "...",
    "constraints": [
      "do not restart Teams helper",
      "do not modify files unless needed",
      "return findings only"
    ],
    "allowed_actions": ["read-only"],
    "artifact_refs": []
  },
  "spec_hash": "...",
  "reply_policy": "sparse-status-then-terminal"
}
```

The target machine claims the delegation by writing:

```json
{
  "kind": "cxp.delegation.claim.v1",
  "delegation_id": "del_...",
  "machine_id": "m_b",
  "claim_id": "claim_...",
  "claim_epoch": 1,
  "worker_instance_id": "worker_...",
  "claimed_at": "..."
}
```

Claiming must be idempotent:

- deterministic request key: `source_session_id + source_turn_id + parent_id + target_machine_id + task_hash`
- first valid claim wins by claim epoch and created time
- terminal results are accepted only when `claim_id`, `claim_epoch`, `worker_instance_id`, `machine_id`, and payload hash match the winning claim
- duplicate Teams deliveries must not start duplicate Codex runs
- active workers wait a short claim recheck window after publishing a claim and re-reduce inbox state before starting expensive work
- manual `cxp delegate claim --json` users must only execute when `winning=true` and `should_execute=true`

## Agent-Agent Interaction Policy

Avoid unrestricted conversation. The production protocol is bounded request/status/result:

- A sends one request.
- B may publish sparse `running` progress when it has new evidence or a real state transition.
- B may publish a sparse `question` only when it is blocked on source/user input.
- B returns one terminal `complete`, `blocked`, or `reuse_rejected` result for the winning claim.
- There is no free-form agent-agent chat loop.
- B must not delegate back to A for the same delegation id.
- Any nested delegation gets a child id and must count against the parent budget.

Loop guards:

- `delegation_id` and `parent_delegation_id`
- seen message hashes per delegation
- max hops
- max wall time
- no echo rule: do not reply with only a paraphrase of the previous message
- progress threshold: each non-question response must include new evidence, a concrete artifact, or a terminal state
- source machine is the coordinator for final synthesis

Default frequency:

- B sends progress only on state transitions, a question, or final result.
- For long tasks, B may send heartbeat/progress every 5 minutes, not every step.
- A waits on the target inbox route stored at `start` time; it does not rescan the registry for active delegation state.
- `wait` returns on `question` by default so Agent A can ask the user once, or on terminal status including `reuse_rejected`.

## Security And Privacy

- Registry messages are visible to the signed-in user's Teams/tenant systems and retention/eDiscovery. Hidden meeting chats are not a secrecy boundary.
- Tokens stay in each machine's local token cache.
- Docker/live CI must copy token cache into isolated temp directories, provide a CA bundle for Graph TLS, and never print token JSON.
- Registry payloads must use redacted ids or hashes in ordinary logs.
- The resolver returns summaries and reasons, not raw registry records.
- A machine can claim only delegations addressed to its `machine_id` or a verified alias mapping.
- A stale machine must not claim new work.
- If the authenticated user changes, the registry key changes.

## Failure Handling

Registry meeting expired:

- `createOrGet` may return a new meeting generation.
- online machines republish cards.
- old chat history is not required for active liveness.

Graph 401:

- refresh token once through existing auth manager.
- if refresh fails, mark registry sync blocked and surface local auth action.

Graph 429:

- respect `Retry-After`.
- back off registry publication independently from user-visible work chat polling.
- do not mark a machine offline just because one heartbeat send was throttled; TTL allows missed beats.

Graph 5xx/network:

- retry bounded with jitter.
- keep last-known local registry cache, but do not auto-delegate if liveness has expired.

Slot patch failed:

- append a replacement slot only when the failure proves the slot is gone, such as 404/410 or equivalent "message not found".
- on 429, 5xx, or network failures, return/back off without appending so production runtime does not create heartbeat message storms.
- rotate long-lived slots on a low-frequency schedule so Teams retention/pruning cannot silently remove the only heartbeat slot forever.

Clock skew:

- use receiver observation time for age where possible.
- `expires_at` from publisher is advisory.
- reject heartbeats too far in the future, but do not crash parsing.

Duplicate cards:

- choose newest by revision, then published time, then Teams message creation time.

Malformed cards:

- ignore and count as diagnostics.
- never let malformed registry messages block reading healthy cards.

## Implementation Plan

Implemented foundation:

- Keep `cmd/teams-registry-probe` as an experimental live tool.
- Use it to reproduce Graph/Teams behavior before changing product paths.
- `internal/teams/machineregistry` contains card schema, external id derivation, parsing, liveness classification, heartbeat slot publishing, cache validation, and low-write cache persistence.
- `internal/teams.MachineRegistryGraphAdapter` keeps Teams Graph types outside the registry core, so the active helper runtime can publish without an import cycle.
- `internal/teams/delegation` contains task spec validation, candidate tokens, remote thread tokens/state, deterministic source keys, reducer state, claim/progress/question/result fencing, loop guards, cancellation, expiry, local route/outbox/cursor/backoff state, and retention pruning.
- `cxp delegate` exposes the model-facing JSON workflow and writes delegation request/status records to the target machine inbox by default; `--store` is local test/debug fallback.
- `cxp delegate claim/progress/question/result` provides target-side debug/recovery primitives over the same inbox-backed route.
- Active `cxp teams run` helpers create or reuse their per-machine inbox, publish a capability card with `inbox_ref`, `instance_id`, `host_label`, capability fingerprint, and protocol versions, PATCH the same heartbeat slot every 5 minutes, poll their own inbox with exact-top head checks and paged drains, claim open requests, recheck the winning claim before execution, execute via the configured Teams executor behind a durable execution fence, use `remote_thread_id` as the Codex session id when present, publish sparse running/final status, convert bad thread reuse into `reuse_rejected`, and best-effort publish `draining` on shutdown. If effective Teams write auth is unavailable, normal Teams helper operation continues without registry presence. `cxp delegate machine publish-once` remains available for diagnostics.
- Teams prompt and built-in `cxp` skill expose the capability without helper-side intent detection.
- CI and release preflight run focused registry, delegation, CLI, and Graph tests.

Remaining production hardening:

- Add live Graph load validation for inbox exact-top polling, 429 backoff, and meeting refresh behavior.
- Keep explicit Teams/chat commands only for debug and recovery.

## Test Plan

### Unit Tests

Graph:

- `CreateOrGetMeetingChat` sends stable external id and self attendee.
- `CreateOrGetMeetingChatWindow` sends the requested long registry window.
- `GetOnlineMeeting` and `UpdateOnlineMeetingWindow` use only the expected allowlisted Graph paths.
- `CreateOrGetMeetingChat` rejects empty external id before Graph call.
- response without `chatInfo.threadId` fails closed.
- Graph path allowlist permits only expected onlineMeeting path.

Registry encoding:

- card round-trip through Teams HTML/plaintext.
- malformed base64 ignored.
- wrong `kind` ignored.
- wrong `registry_key` ignored.
- duplicate machine cards choose newest revision.
- future timestamps bounded.
- missing optional fields accepted.

Liveness:

- online/stale/offline thresholds.
- exactly-on-boundary tests.
- missed heartbeat under TTL remains online.
- stale machine cannot be auto-delegated.
- draining machine is visible but not accepting.
- repeated heartbeat publication patches one slot message instead of appending.
- registry observation pages through Graph continuations instead of trusting only the newest 50 messages.
- Graph message `lastModifiedDateTime` is preferred for heartbeat liveness so publisher clock drift is not authoritative.
- patch failure appends exactly one replacement slot and updates the local slot id.
- 429/5xx patch failure does not append a replacement slot.
- due slot rotation appends one replacement slot without rewriting on ordinary patch heartbeats.
- heartbeat publisher path does not create local files or rewrite durable state.
- local cache save is a no-op when content is unchanged.
- normal heartbeat PATCHes do not rewrite the cache file.
- cache hit validates the meeting/chat by GET before reuse.
- transient validation failure does not create a replacement registry.
- due meeting refresh patches the meeting window and persists the next refresh time.

Resolver:

- alias exact match wins.
- capability match without alias works at lower confidence.
- stale alias match asks clarification or refuses auto-delegation.
- multiple equal candidates asks clarification.
- load and recent failure reduce confidence.
- no candidates returns no delegation.

Delegation:

- duplicate request message does not enqueue duplicate work.
- deterministic source key gives stable `delegation_id` for repeated `start`.
- new/reuse thread tokens bind to the selected target machine and expire.
- `resolve` returns a fresh thread token and only valid reusable threads.
- `start` binds remote thread fields to the request and tracks the local thread as active.
- invalid, stale, unknown, active, or mismatched reuse tokens are rejected.
- duplicate claim from same machine is idempotent.
- conflicting claim is rejected or ignored.
- losing claim terminal results are rejected.
- `reuse_rejected` is a terminal result for the winning claim.
- sparse progress/question status records are retained and terminal results still win.
- cancel wins over late claim/result.
- open request expires after TTL.
- max hops terminates.
- B cannot delegate back to A with same parent id.
- local retention pruning removes old terminal state and stale cursors/backoffs without deleting still-open routes only because they are old.
- stale target cannot claim new work.
- final result is associated with the original source turn.

CLI:

- `cxp delegate resolve --json` returns `start` with a candidate token for one strong online accepting candidate.
- `resolve` returns `ask_user` for multiple strong candidates.
- `resolve` can score capability/skill matches and binds candidate token metadata.
- `start` is idempotent for the same source turn and task file.
- `start` rejects stale candidate tokens when the target inbox generation or capability fingerprint changed.
- `status`, `wait`, and `cancel` reduce through the same protocol state.
- `wait` returns on a target-side `question` by default.
- terminal status syncs remote thread summary and clears active delegation state.
- `status` and `wait` can read delegation records that have fallen behind the first inbox page.
- `claim`, `progress`, `question`, and `result` use the same claim/result fencing as the reducer.
- active delegated workers use the remote thread id as the Codex session id and publish `reuse_rejected` when Agent B rejects a bad reused context.
- `machine publish-once` publishes a capability card without requiring the background runtime loop.
- active Bridge machine registry runtime appends one slot, patches later heartbeats, uses a 5-minute retry floor on transient patch failures, and best-effort patches `draining` without affecting owner heartbeat.
- looping paths are rejected before a request is written.

Security:

- card rendering does not include token-looking fields.
- ordinary status redacts chat id and message id.
- resolver output contains candidate summaries, not raw records.
- logs redact Graph paths containing chat/message ids.

### Integration Tests With Fake Graph

Use `httptest.Server` or a small fake Graph server:

- `createOrGet` idempotently returns the same meeting for same external id.
- two simulated machines publish heartbeats to same registry.
- watcher observes both online.
- one machine stops; watcher sees stale after TTL.
- message pagination includes registry cards beyond the first page.
- 401 refresh path succeeds.
- 429 on heartbeat backs off without marking all machines offline.
- 5xx on publish retries bounded.
- lost slot `message_id` causes append replacement.
- malformed/corrupt registry messages do not block healthy cards.

### Docker Tests

Default CI Docker test should not call Microsoft Graph.

Use a fake Graph server reachable from containers:

```text
container A -> fake Graph -> same registry thread
container B -> fake Graph -> same registry thread
container watcher -> fake Graph -> list recent registry messages
```

Assertions:

- all containers report same chat hash
- A and B become online
- B becomes stale after TTL when stopped
- no token files are mounted
- output contains no access token shape

The existing live probe command can be reused by allowing a configurable Graph base URL in tests. Production Graph base URL remains the default.

### Live Graph Tests

Live tests must be opt-in only.

Add a workflow similar to `teams-live-lossless-edit.yml`:

```text
.github/workflows/teams-live-registry.yml
  workflow_dispatch:
    inputs:
      nonce: required
      heartbeat_seconds: default 15
      ttl_seconds: default 45
```

Live job:

- prepares Teams token cache from GitHub secrets
- builds `teams-registry-probe`
- runs A, B, watcher in Docker
- uses a unique registry key containing the nonce
- mounts CA bundle if needed
- does not print token JSON
- fails unless:
  - all containers converge on same chat hash
  - B is online before stopping
  - B is stale by final observation
  - A remains online

Live workflow should be manually dispatched or protected by an environment. It should not run on pull requests.

## CI Plan

Default `ci.yml`:

- Add a "Teams cross-machine registry regressions" step near existing Teams Graph/keepalive checks.
- Run:

```bash
go test ./internal/teams/machineregistry -count=1 -v
go test ./internal/teams/delegation -count=1 -v
go test ./cmd/teams-registry-probe -count=1 -v
go test ./internal/cli -count=1 -run 'Test(Delegate|CandidatesFromMachineStatuses|RootCommandWiresExpectedSubcommandsAndFlags)' -v
go test ./internal/teams -count=1 -run 'Test(BridgeMachineRegistry|Graph(CreateOrGetMeetingChat(UsesStableExternalID|WindowUsesProvidedWindow)|OnlineMeetingWindowRefresh))$' -v
```

Container CI:

- Extend the existing container test area with fake-Graph registry Docker smoke.
- Build a standalone probe binary.
- Run Ubuntu container A/B/watcher against fake Graph.
- Keep this Linux-only.

Perf and stress coverage:

- Stress repeated slot heartbeat updates and assert one append plus bounded PATCHes.
- Stress `observe` over a full recent registry window.
- Keep benchmarks for slot heartbeat publication and registry-window parsing:

```bash
go test ./internal/teams/machineregistry -run '^$' -bench 'Benchmark(HeartbeatSlotPublisher|ObserveMessages50Cards)$' -benchmem
go test ./cmd/teams-registry-probe -run '^$' -bench 'Benchmark(HeartbeatSlotPublisher|ObserveRegistryWindow)$' -benchmem
```

Release workflow:

- Mirror the default unit tests.
- Run fake-Graph Docker smoke in the existing release container matrix if runtime is acceptable.
- Do not run real Teams live tests during release automatically.

Live workflow:

- New manual workflow for real Graph validation.
- Requires Teams token secrets.
- Uses one-shot nonce.
- Keeps all created registry payloads under a probe-specific key.

Required CI gates before merging production integration:

- unit registry tests green
- fake-Graph integration tests green
- Docker fake-Graph smoke green
- manual live Graph registry workflow run green at least once for the release candidate
- no default PR workflow writes to Teams

## Open Decisions

- Whether capability summaries should include recent Codex history indexes. Recommendation: publish only compact skill/workspace summaries first; add searchable summaries later.
- Whether delegated work creates a new Teams Work chat or stays hidden. Recommendation: target machine work should have a normal local/Teams durable session, but the user-facing final remains in source chat unless the user asks to open the target work chat.
- Whether registry records live in the same hidden meeting chat as delegation records. Decision: no. Registry keeps only discovery/heartbeat; delegation records live in the target machine inbox so workers poll only their own queue and heartbeat PATCHes cannot contend with request/result appends.
