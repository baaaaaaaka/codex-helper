# Teams frontier recovery checklist

This checklist is the implementation contract for the Teams Graph polling recovery repair. It is deliberately scoped to the known frontier/livelock failures. Items marked as deferred are explicit non-goals for this minimal liveness repair; they are not silently treated as implemented.

## 本轮执行范围

- Base: `origin/main` / `d25066644f201acb125dd224db6ac92a8425a13a`.
- Worktree: `teams-frontier-recovery-20260828` on `fix/teams-frontier-recovery-20260828`.
- The implementation keeps the normal poll hot path on targeted SQLite row writes, adds a durable one-page receipt and fenced per-chat attempt, and changes failure handling to local gap/retry state instead of process-wide restart.
- All user-visible safety claims remain conservative: Graph does not provide exactly-once delivery, and a record that cannot be proven/re-fetched is retained as bounded evidence rather than guessed or replayed.

## Contract and safety boundaries

- [x] One chat has one durable next action, with precedence: pending page, active continuation, gap-recovery lane, normal head.
- [x] A poll quantum performs one logical page operation; an HTTP client's internal retries are counted separately.
- [x] No head request is issued while an operational continuation exists.
- [x] A page/frontier/cursor advances only after every required record has a terminal durable disposition.
- [x] Empty and all-deduplicated pages still commit continuation progress.
- [x] No claim of exactly-once external execution or Graph-wide no-loss outside documented provider assumptions.
- [x] Stable, replayable provider records receive durable, idempotent intake; uncertain external effects become manual hold.
- [x] Work-chat failures remain chat-scoped; control/store/lease failures retain process-wide significance.
- [x] Local/TUI writer conflicts remain fail-closed with no automatic Teams retry or re-execution.
- [x] Irrecoverable history is explicit gap/degraded state, never silent success or repeated user spam.

## 1. Baseline and source map

- [x] Record base commit, branch, worktree, and clean unrelated-change status.
- [x] Map every current `ChatPollState` writer and reader, including normal, parked, auto-park, fork, cleanup, quarantine, notice, and final-answer paths.
- [x] Map JSON state, SQLite state, pointer/generation, inbound ledger, outbox, claims, and pruning boundaries.
- [x] Identify existing tests that assert head-first polling, dual operational frontiers, generic 404/410 stale classification, blocked work errors, or park-clears-frontier behavior.
- [ ] Capture baseline focused tests, Teams package tests, full Go tests, race tests, and relevant CI selectors.
- [ ] Record a baseline benchmark for unchanged head, continuation page, empty page, and state write counts.

  These two pre-change measurements were not captured before implementation and are recorded as a deferred evidence item below; post-change runs are recorded in sections 7–9.

## 2. Frontier reducer and durable state

- [x] Add a pure reducer/normalizer for P/D/page/gap state; it must not perform I/O.
- [x] Make P the only normal runtime continuation frontier.
- [x] Collapse P==D without creating a second request.
- [x] Promote D-only durably before any Graph request.
- [x] Preserve legacy P!=D as bounded FIFO `[P,D]`; if ordering or validity is ambiguous, create an explicit legacy gap without dropping either identity.
- [x] Define frontier epoch and exact opaque request identity/fingerprint; preserve raw query values without re-encoding.
- [x] Add poll-data revision and separate schedule revision with explicit schedule merge semantics.
- [x] Add process-incarnation, attempt, lease-generation, expiry, and expected frontier/page/gap fields.
- [x] Add bounded pending-page metadata and record disposition state.
- [x] Add SafeCursor as a conservative normal-head boundary; document timestamp/visibility/ID limitations.
- [x] Add GapRecoveryCursor and an explicit directional recovery boundary; it is never presented as SafeCursor proof.
- [x] Add bounded frontier edge history and no-progress budget; overflow becomes explicit gap state.
- [ ] Add durable gap epoch/reason/evidence and one-notice-per-epoch bookkeeping.
- [ ] Define pure reducer invariants and table-test every state combination.

## 3. Page staging, inbound ledger, and side effects

- [x] Define PendingPage precedence at poll start: replay locally before any Graph request.
- [x] Persist a page receipt independent of attempt ID and bind it to chat, poll revision, frontier epoch, and request identity.
- [x] Persist immutable normalized message envelopes before handlers or external effects.
- [x] Include stable ID, timestamps, body/attachments, and payload hash in the replay envelope; sender/provenance is revalidated by the normal classifier.
- [x] Define missing-ID, duplicate-ID, changed-payload, and malformed-record behavior; ambiguous cases hold/quarantine/gap.
- [x] Reuse existing inbound/provenance/outbox records; the page receipt is bounded and is not a second unbounded queue.
- [ ] Distinguish every external-effect phase as a single durable ledger state; this repair relies on the existing inbound/outbox/claim ledgers and does not introduce a new intent ledger.
- [x] Make existing inbound lookup replay received-but-unhandled persisted records while suppressing linked/terminal records.
- [x] Use existing stable idempotency keys scoped by account/chat/message/action.
- [x] Use contiguous terminal-prefix ordering for actionable messages; later records cannot bypass an unresolved predecessor.
- [x] Protect active/page-referenced claims from the existing TTL/count pruning path.
- [x] Release a failed claim or complete it durably; an uncertain external effect is not silently reclaimed.
- [ ] Define and implement a new cross-store intent/recovery protocol; this repair preserves the existing ledgers and explicitly defers a new cross-store intent state machine.
- [ ] Reconcile every possible commit error after reopen before any retry; the existing outbox/claim reconciliation remains the authority for external effects.
- [x] Define bounded page/record byte limits; whole-response overflow and decoded-record overflow have explicit separate outcomes.
- [x] Ensure a staged page cannot be replayed into a new seed/gap epoch; tombstone protection remains deferred.

## 4. Fenced poll attempt

- [x] Implement authoritative `BeginChatPollAttempt` using a short transaction and a fresh row read.
- [x] Reject an existing unexpired attempt, including same-process concurrent goroutines.
- [x] Carry control lease holder, process incarnation, lease generation, and expiry through the capability; the bridge validates the active control lease before and after I/O.
- [x] Use the current control lease generation for every new process owner, including same-machine takeover.
- [x] Return a capability containing attempt ID, owner, process incarnation, lease generation, data/schedule revisions, and expected page/frontier/gap identity.
- [x] Keep Graph and handler I/O outside store locks.
- [x] Require poll-owned local mutation to carry the attempt ID and expected revision.
- [x] Implement fenced `CommitChatPollAttempt` with exact attempt/revision/expiry checks; owner/lease validation is performed by the bridge, not claimed as a store-only CAS.
- [ ] Return a public typed committed/stale/conflict outcome API; current internal booleans and sentinel errors are sufficient for this path and the broader API redesign is deferred.
- [x] Ensure stale results cannot write error, retry schedule, notice, cleanup, or cursor/frontier state.
- [x] Handle attempt expiry during Graph, handler, and final commit without cursor advancement; replay remains durable.
- [x] Fence scheduler, park/unpark, notices, auto-park, quarantine, final-answer boost, rate-limit, fork, staged-child, and direct poll-row writers.
- [ ] Add tombstone/recreation epoch protection against stale resurrection.
- [ ] Add a separate JSON generation token for every caller holding a snapshot outside `Store.Update`; JSON Store.Update already serializes normal writers, but the extra cross-process generation protocol is deferred.
- [x] Replace unconditional SQLite poll-row hot writes with targeted revision-aware writes.
- [x] Replace blind SQLite `chat_polls` delete/recreate behavior with revision-aware merge preserving newer rows and rejecting equal-revision conflicts.
- [ ] Add a universal storage-error taxonomy for CAS loss, SQLITE_BUSY, I/O, and ENOSPC; current tests preserve the distinction at the exercised store boundaries.

## 5. Gap, Graph error, and scheduler behavior

- [x] Classify Graph errors using request context and structured status/code, not generic error strings.
- [x] Allow only token-specific continuation 400/404 or documented 410 to open an immediate gap.
- [x] Retain P and back off for generic 400/404/410, auth, 429, timeout, transport, and 5xx initially.
- [x] Apply a finite attempt/age/no-progress budget to permanent continuation failures; then open `unverified-continuation` gap and switch to recovery.
- [x] Validate persisted paths as same-account/chat/messages-collection paths before Graph I/O.
- [x] Preserve opaque nextLink query bytes and use only bounded/redacted diagnostics.
- [x] Keep gap open across later successful recovery pages and restart.
- [x] Define directional recovery boundary, overlap, timestamp tie policy, full-window behavior, and second recovery failure.
- [x] Permit newer-record progress during a gap only through the separate recovery cursor; SafeCursor remains conservative.
- [x] Do not advance SafeCursor over unresolved gaps or empty-page-without-watermark cases.
- [x] Detect self-loop, changing-link same-page cycles, and no-progress overflow without silently dropping page messages.
- [x] Use frontier-aware due/park/auto-park decisions for P, D, PendingPage, gap, and recovery P.
- [x] Make ordinary park/resume scheduling-only; only explicit gap/reseed handling retires frontier state.
- [x] Separate frontier intake/drain from Codex execution and queue admission.
- [x] Make local/TUI conflict fail closed and leave the message for explicit/manual handling rather than automatic same-thread re-execution.
- [x] Return only control/store/lease failures from `pollOnce`; work-chat outcomes stay per-chat.
- [ ] Add a universal storage-degraded gate for ENOSPC/SQLITE_FULL and failed durable writes across every outbound Graph path; exercised SQLite_FULL boundaries are covered, but the universal gate is deferred.
- [x] Use fair bounded backoff so poisoned chats cannot starve healthy chats or consume the cycle cap.
- [ ] Deduplicate all user/control diagnostics to one per gap epoch; gap state is durable, but the full diagnostic dedupe API is deferred.

## 6. Migration and compatibility

- [x] Verify actual current state/pointer schema versions before changing constants.
- [x] Design idempotent state and SQLite migrations for all new fields, columns, indexes, and legacy P/D forms.
- [ ] Keep load-only normalization read-only; current legacy pointer reads perform a metadata-only pointer upgrade before normal writes, so a strict read-only normalizer is deferred.
- [x] Build and verify a new DB generation before atomic pointer publication; do not replace an old DB inode in place.
- [x] Flush DB/WAL, pointer, and containing directories at the publication boundary where the existing migration API supports it.
- [ ] Add a durable store-generation write gate and quiesce/fence already-open old writers.
- [x] Make new writers migrate or fail before writing new fields; old writers reject the newer pointer schema.
- [x] Preserve old generations for rollback without pointer rollback over newer writes.
- [ ] Test migration crashes before/after every database and pointer publication boundary; existing migration hooks cover selected boundaries, not the full matrix.
- [x] Test idempotent retry, JSON/SQLite equivalence, and conservative inbound-status backfill.

## 7. Regression and stress tests

- [x] Port the known frontier reproductions into tracked positive liveness tests.
- [x] Test exact request traces and actual HTTP retry counts.
- [x] Test P-first, no-head-while-P, P==D, D-only, parked operational frontiers, and legacy P!=D.
- [x] Test empty/all-deduped pages, page action limit, handler failure, unresolved claim, and first-unresolved-record ordering.
- [x] Test immutable page replay when the subsequent Graph response changes or is unavailable.
- [x] Test stable IDs, missing IDs, timestamp/order metadata, changed payload hashes, duplicate IDs, and malformed records.
- [x] Test oversized individual record, oversized whole response, page cap, and explicit quarantine/gap behavior.
- [x] Test token-specific versus generic continuation error matrix, malformed local path, and opaque query preservation.
- [x] Test permanent generic failure budget, changing-link cycles, gap recovery with newer records, recovery failure, and explicit reseed behavior.
- [ ] Test crash injection at every listed side-effect boundary; deterministic restart/outage/claim/SQLite_FULL cases are covered, but the full injection matrix is deferred.
- [ ] Test every sidecar/uncertain-effect reconciliation branch; existing claim/outbox tests cover the current implementation, while a new reconciliation protocol is deferred.
- [x] Test same-process goroutine race, same-machine ABA takeover, cross-process stale result, lease expiry, scheduler race, fork, and full-state race.
- [x] Test park/resume/auto-park/quarantine/unquarantine preserving the operational frontier.
- [x] Test one poisoned gap, parked legacy chat, manual TUI hold, healthy chats, and control-chat failure for fairness/watchdog isolation.
- [x] Test long service outage with many TUI chats/messages and restart from durable state.
- [x] Test Graph timeout/429/5xx, delayed/out-of-order responses, full message window, and continuation expiration.
- [ ] Test ENOSPC/SQLITE_FULL at every page, ledger, outbox, diagnostic, and final commit boundary; SQLite_FULL owner/outbox/transcript boundaries are covered, but the universal matrix is deferred.
- [ ] Test old/new binary migration while an old writer is already open; pointer rejection and offline takeover are covered, but fencing a live old writer is deferred.

## 8. CI, Docker, and performance

- [x] Use deterministic fake Graph barriers and bounded test fixtures for mandatory tests; the long-running stress jobs remain supplemental and no production network is used.
- [x] Update CI and release selectors with exact-name or aggregate-subcase guards; fail on zero/missing matches.
- [x] Add the liveness/recovery suite to the owning Linux normal Teams/store job; the complete local race run covers the race contract.
- [x] Add Docker process/restart smoke against local fake Graph with no host Teams/state access.
- [x] Keep the broad stress suite supplemental to deterministic contract tests.
- [x] Benchmark not-due/unchanged head, SQLite long transcript, targeted quarantine, provenance matrix, and unchanged-tail paths.
- [x] Assert not-due has zero requests/writes, P-only has one continuation/no head, and pending replay has zero Graph requests in focused tests.
- [ ] Assert a numeric allocation/latency regression threshold against a captured pre-change baseline; current benchmarks show bounded targeted writes and no observed hot-path regression, but no pre-change baseline was captured.
- [x] Assert bounded page/history/gap memory, targeted writes, and no hot-path full-state rewrite/payload copy in the exercised paths.
- [x] Run focused tests, package tests, race tests, full Go tests, diff checks, CI selector checks, Docker stress, runtime-safety shards, and final performance smoke.

## 9. Final review and handoff

- [x] Review the complete tracked and untracked diff for unrelated changes, stale APIs, and unhandled writers; the unused legacy implementation is retained only as a clearly marked non-runtime reference and is not wired into production.
- [x] Confirm every unchecked item has a documented scope/reason below rather than being silently omitted.
- [x] Confirm all known current tests have been updated to the new contract rather than weakened.
- [x] Confirm no production Teams helper/service was restarted or modified during development.
- [x] Record remaining provider limitations and any manual recovery path.

## 本轮明确延期（经 review 确认不阻塞当前自动脱困修复）

- [x] No pre-change benchmark baseline exists; retain post-change benchmark output in the handoff and run the same selectors on the next candidate for comparison.
- [x] A universal ENOSPC/SQLITE_FULL gate across every outbound path is not implemented; the tested SQLite_FULL owner/outbox/transcript boundaries remain fail-closed, and expansion is a separate change.
- [x] Full crash injection across all side-effect boundaries, a new cross-store intent ledger, and deterministic commit-after-error reconciliation are not implemented; current inbound/outbox/claim mechanisms remain authoritative.
- [x] Tombstone/recreation fencing and a live old-binary writer quiesce protocol are not implemented; new writers reject the schema-10 pointer and existing runtime takeover paths are covered.
- [x] Store-level owner/lease identity CAS and a public typed attempt-result API are not implemented; the bridge checks the active lease around I/O and store CAS fences attempt/revision/expiry.
- [x] Pure reducer exhaustive table coverage and complete diagnostic dedupe are not implemented; current targeted tests cover each exercised transition and no user-facing gap notice is emitted by this repair path.
