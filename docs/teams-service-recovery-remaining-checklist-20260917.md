# Teams service remaining recovery blockers — 2026-09-17

This is the executable checklist for the remaining blockers observed after the
latest local-service incident.  The live Teams service is stopped while this
work is performed.  No live SQLite database or Teams message is modified by
the implementation and test work.

## Evidence and safety boundary

- [x] Confirm the active local service was manually stopped and no active
      duplicate fallback process remains after the user's cleanup.
- [x] Read the active scope database read-only: SQLite is readable and has
      durable state, but contains 45 `sending` rows, 2 `queued` turns, and 1
      `running` turn.  `sending` rows remain unknown/ambiguous until exact
      remote evidence is found; they must not be replayed automatically.
- [x] Confirm the observed lease failures were owner-generation fencing events,
      not evidence that the SQLite file itself was corrupt.
- [x] Confirm Graph read 429 and the unresolved running turn are separate
      durable conditions from the duplicate-launcher problem.
- [x] Never mutate the live scope database, clear a running turn without
      source/owner proof, replay an uncertain POST, or send a real Teams
      message during this checklist.

## P0 — eliminate duplicate service writers

- [x] Make local-supervisor startup retire both WSL Scheduled Tasks and the
      matching Windows Startup fallback before publishing a new supervisor.
      Cleanup failure must prevent the new writer from starting.
- [x] Make local-supervisor uninstall retire the same conflicting launchers;
      do not leave a fallback service running after the authoritative backend
      is uninstalled.
- [x] Make Windows-backend uninstall return Startup fallback cleanup errors
      instead of discarding them.
- [x] Keep cleanup suffix-scoped: remove only markers and processes belonging
      to the current WSL task identity/prefix; preserve unrelated startup
      entries and disabled backups.
- [x] Add CLI/backend regressions for successful cleanup, cleanup failure,
      unrelated-marker preservation, and fail-closed start ordering.

## P0 — isolate durable execution ambiguity without unsafe repair

- [x] Add a read-only diagnostic/report for `running` turns whose execution
      ownership is unresolved, including the exact durable reason and related
      session/chat counts without exposing message bodies.
- [x] Verify that one unresolved turn cannot prevent unrelated queued inbound
      turns, work-chat polling, or outbox delivery from being admitted.
- [x] Preserve fail-closed semantics: only an explicit source-proofed repair
      may settle the affected turn; no automatic `running -> queued` or retry
      may rerun Codex.
- [x] Add JSON/SQLite, restart, stale-owner, foreign-session, and healthy-chat
      isolation tests.

## P0 — account/global Graph read throttling

- [x] Audit every read-429 call site and ensure account/global read gates park
      only read/recovery work; independent durable outbox writes remain
      eligible when the write gate is clear.
- [x] Add a deterministic account/global 429 sequence with short virtual time:
      repeated read failures, an intermittent successful read window, and
      successful writes during that window.  Assert no tight retry loop, no
      cursor advance past unhandled inbound, no duplicate POST, and no lost
      outbox row.
- [x] Cover ambiguous POST + read 429 together: recovery may observe exact
      remote evidence, but may never issue an unproven second POST.
- [x] Keep provider `Retry-After` durable and bounded; tests must not sleep for
      real minutes.

## P1 — remove secondary phase/SQLite starvation

- [ ] Add phase-level measurements for schedule admission, candidate
      hydration, worker wait, Graph request, SQLite transaction/lock wait, and
      durable completion.  Report wall time, CPU time, rows, JSON bytes,
      transactions, and Graph requests.
- [ ] Confirm optional history/linked maintenance is durably deferred while a
      Teams backlog is due, and that wake/fairness state survives restart.
- [ ] Optimize only measured local work: selected-only hydration, scalar
      indexed admission, and off-lock canonical fallback.  Preserve owner,
      lease, attempt, frontier CAS and the single durable frontier.
- [ ] Re-run a paired immutable Docker fixture and report completed durable
      messages per second; do not count executor runs that are not closed.

## P1 — control-chat backlog isolation

- [x] Inspect queued control rows by durable kind/marker without reading or
      deleting normal work-chat rows.
- [x] Add an explicit, auditable suppression/drop path for obsolete control
      status output only; ordinary control replies and all work-chat output
      remain protected.  Only `controlFallbackSessionID + queued-status + the
      current control chat + Queued` qualifies; the drop is owner/CAS fenced.
- [x] Prove restart/idempotency and absence of an infinite enqueue loop for the
      `I already handled this new request...` response.

## P1 — startup/migration and acceptance coverage

- [x] Verify schema preparation and migration markers before any owner-scoped
      operation; stale preparation must fail with an actionable repair path,
      not enter a retry loop.
- [x] Make WSL Startup fallback retirement idempotent when the current task
      suffix has no launcher or watchdog process.  Keep process-discovery and
      post-cleanup verification fail-closed so historical residue cannot hide
      a second writer.
- [ ] Add Docker tests using a point-in-time copy of SQLite/WAL, registry,
      ledger, and Codex history; source fixture is read-only, Graph is fake,
      network is disabled, and no Teams token is read.
- [ ] Exercise no-429, account/global read-429, intermittent recovery, unknown
      POST, restart/takeover, malformed/unsupported read query, and backlog
      drain.  Require positive durable completion, stable owner generations,
      no duplicate remote identities, and source immutability.

## Verification gate

- [x] Run focused lifecycle, ownership, 429, and unresolved-turn tests.
- [x] Run `go test ./internal/cli ./internal/teams/store ./internal/teams
      -count=1` and the relevant serial race tests.
- [x] Run `go vet ./...`, `git diff --check`, and the manifest check.
      Docker shell/fixture acceptance remains pending until the immutable
      point-in-time fixture is selected and the container runtime is verified.
- [x] Review the final diff for live-state writes and confirm no service
      lifecycle command was executed by the development session.

## Execution log

- 2026-09-17: read-only incident review found the local-supervisor cleanup
  gap, 45 ambiguous `sending` rows, 2 queued turns, 1 unresolved running turn,
  and 3 currently blocked 429 poll rows.  No live state was changed.
- 2026-09-17: fixed the duplicate-launcher cleanup gap.  Focused lifecycle
  tests and `go test ./internal/cli -count=1` passed; targeted ownership,
  read-429, ambiguous-POST, backlog, and unresolved-execution tests passed.
- 2026-09-17: added metadata-only `teams status` reporting for unresolved
  execution fences and ambiguous outbox rows.  It explicitly reports that
  automatic retry/replay is disabled and never prints message bodies.
- 2026-09-17: made `teams recover` prepare an unready SQLite schema before its
  owner read; a temporary migrated-then-unprepared SQLite recovery test passed.
- 2026-09-17: made the duplicate `/new` response use a stable outbox ID derived
  from the original Teams message ID.  Replaying the same inbound three times
  now produces one durable duplicate notice and one Graph POST; ordinary direct
  control notifications retain their existing unique-call behavior.
- 2026-09-17: stopped creating control-fallback `queued-status` rows and added
  a sender-side CAS drop for old still-Queued rows.  Ordinary control replies,
  all work-chat rows, and any Sending/Accepted row remain on the normal
  fail-closed path; focused suppression, fairness, and idempotence tests pass.
- 2026-09-17: fixed the real-data Docker history inventory so a valid non-zero
  history cursor is no longer unconditionally converted to `required_start=0`.
  The live scope still has a genuinely large history tail, so Docker acceptance
  remains pending rather than being declared green from a partial fixture.
- 2026-09-17: the first real-data Docker attempt was stopped before Docker test
  startup after its disposable fixture reached about 45 GB while copying the
  current session/checkpoint corpus.  The remaining cost is fixture assembly,
  not Teams runtime; the runner is still not accepted until the point-in-time
  snapshot path is made bounded and source-stable.
- 2026-09-17: verification gate passed: the full CLI/store/Teams run passed
  (93.978s / 358.943s / 808.747s), followed by focused control suppression and
  idempotence tests, `go vet ./...`, shell syntax, manifest validation, and
  `git diff --check`.  No live service command was run.
- 2026-09-17: prerelease CI exposed a hosted race in the concurrent linked-
  transcript handoff: a safe SQLite snapshot invalidation was returned as a
  phase error after its bounded retries.  The reader remains fail-closed, while
  that explicit session-local condition is now durably deferred for the next
  poll; parent cancellation is still propagated.  The targeted race regression
  passed locally.
- 2026-09-17: the live WSL start failure was narrowed to the Windows-side
  Startup fallback cleanup command returning exit status 1 even though the
  current suffix had no active launcher or process.  Cleanup now performs a
  read-only presence/process check before mutating fallback files, and verifies
  that any cleanup leaves no matching file or process.  The focused lifecycle
  suite and a controlled live WSL probe passed without changing SQLite or Teams
  messages.

## Review revision — 2026-09-17 (three independent review rounds)

The review loop found that the earlier checklist was safe in intent but was too
optimistic about recovery and end-to-end evidence.  The items below supersede
any earlier `[x]` that only had unit-level or partial evidence.  This section is
the current implementation and acceptance plan; it does not authorize a live
database mutation or a service restart.

### Review gate and phase separation

- [x] Complete three rounds of independent read-only review covering deferred
      control replay, source rewrite, unresolved execution/outbox, liveness,
      history, and historical commit intent.
- [x] Record the review decision: conditional GO for offline implementation and
      tests; NO-GO for release, automatic live recovery, or claiming that Work
      synchronization is restored.
- [ ] Keep three phases separate: code fix; immutable-fixture verification;
      per-row live recovery and synchronization acceptance.
- [ ] Before any listener startup recovery, put legacy rows with missing retry
      metadata, unknown operation provenance, or ambiguous outbox outcome into
      report-only/hold mode.  They must not be implicitly adopted by startup.
- [ ] Define the capability split explicitly: listener owner capability,
      replacement-owner capability, typed operator-maintenance capability, and
      report-only capability.  No unbound compatibility path may mutate a
      running turn, checkpoint, retry gate, or outbox.

### P0-A — deferred control replay and error taxonomy

- [ ] Re-check the allowlist at the deferred replay entry point, immediately
      before dispatch.  Unknown, ambiguous, lifecycle, configuration, publish,
      fork, rename, webhook, restart/reload/update, and other operations without
      a written idempotency contract go to a durable manual hold; they must not
      re-enter the generic control handler.
- [ ] For deliberately supported operations (including only narrowly proven
      control-fallback or `/new` paths), document the operation contract and
      require a stable operation key/provider idempotency key.  Do not classify
      every control command as unsafe or every control command as replayable.
- [ ] Implement an observable durable state machine at the external-operation
      boundary: `Deferred -> Claimed/Prepared -> RequestStarted/Unknown ->
      Accepted/Completed`.  A replacement owner may adopt only a row still
      proven to be before the side-effect boundary; `RequestStarted/Unknown`
      is excluded from ordinary candidates and enters reconciliation/hold.
- [ ] Bind claim, operation key, attempt, and completion to an immutable
      `(MachineID, LeaseGeneration)` capability.  An old capability must fail;
      a new owner may adopt a safe `Deferred` row with an expected-row CAS.  A
      changed generation is not, by itself, a reason to reject safe adoption.
- [ ] Replace the broad `GraphStatusError`/`io.EOF` row-local predicate with a
      method- and phase-aware typed disposition: proven read/preflight failure,
      proven no-request rejection, external-outcome-unknown, resolver-local,
      stale-row/not-claimed, and owner/store failure.
- [ ] Permit ordinary row-local backoff only for a proven no-side-effect read
      or preflight/resolver failure.  POST/PATCH after request start, timeout,
      EOF, 408/409/425/429/5xx, or response loss must be ambiguous/uncertain;
      never retry, requeue, or skip it automatically.
- [ ] Make retry-gate/claim/completion writes owner-fenced and atomic.  A stale
      snapshot/not-claimed conflict must stop that row or hand off safely; a
      lease loss, untrusted lease, schema/integrity failure, or failed durable
      disposition must stop the old generation.  Do not turn ordinary
      `SQLITE_BUSY`/bounded stale-snapshot conflicts into a global freeze, and
      do not turn an owner/store failure into row-local progress.
- [ ] Ensure a failed gate write immediately prevents subsequent side effects
      in that recovery pass; no later row may be processed under an invalid
      capability.
- [ ] On model-profile failure, reuse only an already durable, semantically
      matching fallback snapshot.  Explicit profile requests must not be
      silently substituted.  New-session resolver failure gets an owner-fenced
      durable backoff before any external side effect.

### P0-B — source rewrite and linked rebase

- [ ] Classify every blocked checkpoint without mutation: source kind,
      source-generation/identity, ordinary versus `ignored:*` anchor, unique
      versus ambiguous anchor, semantic fences, pending outbox, and logical
      delivery overlap.
- [ ] Limit automatic rebase to the narrow proof case: the same intended
      source is established by a writer-provided epoch or immutable snapshot;
      the ordinary logical anchor is unique; prefix/range and anchor bytes are
      proven; no overlapping semantic fence exists; and full expected-row CAS
      succeeds.  SameFile/stat/bounded-near-cursor checks alone are insufficient
      for a concurrent same-inode rewrite.
- [ ] Treat `ignored:*`, missing/ambiguous anchors, partial or changing source,
      missing prefix coverage, and any overlapping ContextGap,
      PendingHistoryRange, PendingRoot, TranscriptQuarantine,
      CompletionPending, continuation, TerminalBoundary/Seen, or
      UnresolvedExecution state as typed hold/explicit recovery.  Never reset
      offset, clear a flag, delete a checkpoint, or infer terminality from EOF.
- [ ] Fix linked rebase when one physical JSONL line expands to multiple
      logical items: an anchor at the first item must not move the cursor past
      later items on that line.  Add duplicate/ambiguous logical-ID detection.
- [ ] Give linked rebase the same bounded byte/record/time budget and durable,
      source-bound scan cursor as history-watch.  It must resume after restart,
      not rescan large files from byte zero on every cycle, and must yield to
      eligible Work admission.
- [ ] Use a full expected checkpoint/recovery CAS covering recovery identity,
      source generation, cursor/range/fingerprint, and every semantic fence
      that the rebase could affect.  Revalidate source epoch/snapshot, anchor
      bytes/range, and cursor boundary immediately before CAS; a concurrent
      change must leave the row held.
- [ ] Separate `logical_delivery_key` (chat/thread, logical event/turn,
      group/part, canonical body/render version) from `source_observation`
      (generation, path, physical range, fingerprint).  A new generation must
      reconcile an old terminal/uncertain logical event first; generation alone
      must neither deduplicate nor force a second POST.  Same logical event with
      conflicting payload is a hold, not an automatic merge.
- [ ] After a successful rebase, reload the checkpoint and run the normal
      suffix admission path.  Rebase success alone is not recovery evidence;
      require exactly-once durable delivery and a second-pass no-new-delivery
      result.

### P0-C — unresolved execution, history-only, and unknown outbox

- [ ] Define durable typed provider terminal/cancel evidence with unique
      `(evidence_id, anchor_generation, outer_turn_id)` and the session/thread/
      Codex-turn/source-proof/time fields needed for audit.  Evidence creation
      and fence clear must be validated by the current owner in one expected
      CAS; recovery reason, status string, transcript final, or restart cannot
      self-prove execution completion.
- [ ] Make startup recovery report-only without a listener capability.  A
      typed operator-maintenance capability may perform a specifically scoped
      recovery, but never through an unbound legacy write path; an old callback
      cannot mutate a new generation.
- [ ] Add a non-terminal `uncertain/needs-attention` disposition for Sending,
      markerless Accepted, expired-lease, and other unknown outbox outcomes.
      They must be excluded from ordinary send candidates and cannot become
      `Skipped` merely because a later FIFO item exists.  Only exact remote
      reconciliation may settle them; no unproven POST is issued.
- [ ] Make transcript/outbox identity source-safe without using source
      generation as the sole dedupe key; bind logical event, payload/version,
      exact range, and operation provenance, and explicitly hold conflicting
      generations.  Preserve independent ACK, marker, and final boundaries.
- [ ] Provide a genuinely read-only history preview/inventory.  It must not
      write SQLite/WAL, registry, drain state, ledger, or Graph, and `--force`
      replay namespaces are prohibited for this incident.
- [ ] Make history-only publish a distinct typed operation that never invokes
      executor, cancel, `processQueuedTurns`, or runtime execution-anchor clear.
      While an execution fence is unresolved, queued turns remain queued; only
      a separate evidence-backed owner-CAS operation can release dispatch.
- [ ] Bound all history import fallback by bytes, records, and wall time.  Keep
      attention until the associated delivery is terminal or exact
      reconciliation is complete.

### P0-D — scoped hold, causal frontier, and liveness

- [ ] Store manual-hold/uncertain rows with reason, required evidence, next
      action, next reconcile time, wake condition, and attempt metadata.  They
      must be absent from ordinary due candidates but reachable by an explicit,
      owner/CAS-fenced recovery operation.
- [ ] Define causal scope: a held control/execution row may block only its
      dependent suffix on the same chat/thread/frontier; unrelated chats and
      independent lanes continue.  Do not allow a suffix to cross a held
      predecessor merely to improve a metric, and do not let one held row turn
      into a process-wide admission fence.
- [ ] Preserve read/write separation under bare chat and trusted account/global
      GET 429.  Read gates may park read/recovery/refetch work, but independent
      durable writes remain eligible; ambiguous receipts remain protected.
- [ ] Persist history/linked deferral, fairness cursor, and wake state.  A
      maintenance backlog must not consume all Work admission or outbox quantum.
- [ ] Delay flush-cap, 8/4, head/backlog quantum, scalar hydration, or other
      performance tuning until a positive Work outbox exists and the admission
      fence is shown to be open.  Throughput is measured only from closed
      durable completions, not executor starts or phase liveness.

### Tests required before any live recovery

- [ ] Error taxonomy and deferred replay: JSON/SQLite parity; safe row-local
      failure plus healthy row; durable backoff across cycles/restart; no retry
      before `NextAttemptAt`; lifecycle/unknown/ambiguous command matrix;
      same PID/new generation; replacement-owner adoption; owner-loss and gate
      CAS race; gate failure stops later side effects; model resolver failure,
      recovery, and explicit-profile protection.
- [ ] External boundary: GET 429/5xx/EOF versus POST/PATCH preflight failure,
      known rejection, timeout/EOF/408/409/425/429/5xx after request start;
      assert no second POST, no skip, no requeue, and durable operation linkage.
- [ ] Source: linked same-row multi-item, duplicate/ambiguous IDs, atomic
      same-size replacement, new inode/same path, same-inode edit, mtime
      restoration, truncation/partial/malformed/oversized tail, proof/CAS
      barrier, writer epoch/snapshot, semantic-fence matrix, bounded resume,
      restart, rebase-to-suffix exactly-once, and second-pass idempotence.
- [ ] Execution/outbox: typed evidence uniqueness and atomic clear; no-owner
      startup is report-only; two owners plus delayed old callback; unknown
      Sending/Accepted never reposts or becomes Skipped; accepted-before-CAS
      crash; logical/source-generation conflict; history preview is read-only;
      history publish invokes neither executor/cancel/queued dispatch; ACK,
      marker, and final crash/restart boundaries remain independent.
- [ ] Fairness/429: fake clock with repeated account/global and bare-chat GET
      failures plus intermittent success; trusted-scope gating; independent
      writes; ambiguous refetch; 8 chats/4 workers; oldest eligible Work
      progression; maintenance starvation; gate/wake persistence; SQLite
      `BUSY`/stale snapshot versus corruption/lease failure classification.
- [ ] Run the relevant existing regression suites and audit/port only applicable
      tests from `/home/baka/.local/state/codex-helper/worktrees/cxp-perf-gap-latest-20260812`;
      do not copy tests that assume a different schema or weaken the current
      safety boundaries.  Run JSON/SQLite parity and `-race` for new paths.

### Bounded Docker acceptance and live recovery gate

- [ ] Build a point-in-time authoritative fixture containing the needed SQLite
      main/WAL/SHM, registry/ledger/checkpoints, and relevant Codex JSONL ranges;
      avoid copying the entire multi-tens-of-GB history corpus.  Bind source
      read-only and runtime/state to an isolated disposable location.
- [ ] Use fake Graph and fake executor with no Teams token and no real network.
      Record method/path/query, rate-limit scope, Retry-After, operation key,
      whether the fake remote committed an object, and response loss/429/5xx.
      Use deterministic virtual time; do not wait real rate-limit minutes.
- [ ] Exercise no-429, account/global GET 429, intermittent read success,
      unknown POST, deferred lifecycle command, model-profile failure, source
      rewrite, partial JSONL, two-owner takeover, restart, 8/4 fairness, and
      history/linked backlog in one bounded scenario plus focused variants.
- [ ] Require positive durable terminal completion from an eligible Work chat,
      oldest-due frontier progress, inbound/outbox reconciliation, no duplicate
      fake remote identity, zero unproven second POST, no old-owner mutation,
      no unsafe cursor advance, source immutability, and no Graph call inside a
      SQLite transaction.  Zero new Work outbox or incomplete run accounting is
      an automatic NO-GO.
- [ ] Only after all fixture gates pass, generate a read-only live recovery
      manifest.  Reconcile unknown rows first; then recover one explicitly
      authorized row/range at a time with typed capability and expected CAS,
      reload, normal suffix admission, and read-only verification.  Keep all
      unsupported/ambiguous rows in hold; do not use `--force`, bulk clear,
      bulk skip, bulk resend, or automatic listener mutation.

### Final status

- [x] Plan revised after three review rounds; no new unresolved design defect
      remains in the offline implementation structure.
- [x] Offline code changes and the complete regression/fixture test matrix are
      implemented and green.
- [x] The isolated Docker diagnostic produces positive, closed durable Work
      completions with the required safety invariants; its strict source
      snapshot gate remains separately pending below.
- [ ] Live per-row recovery is separately authorized, executed, and verified.
- [ ] Teams Work synchronization is declared restored only after the final two
      gates above; until then the correct status is **NO-GO**.

## Execution log — 2026-09-18

The following items are the implementation and verification status for the
current recovery plan.  The live rc.49 helper and its database were not
restarted or mutated by this work.

### Durable recovery and source-fence changes

- [x] Preserve row-local deferred control failures as durable retry/hold state;
      bound one recovery phase to eight rows and keep the retry wake durable.
- [x] Add preflight/error classification for missing or invalid Graph auth so
      the same request is not retried as a hot loop; preserve the no-replay
      boundary for unknown POST results.
- [x] Keep account/global and bare-chat read throttling independent from
      durable write eligibility; retain provider retry timing durably.
- [x] Make supported deferred control operations owner/CAS fenced and keyed by
      the immutable inbound Teams identity; legacy rows that crossed an unknown
      external boundary remain held rather than being replayed.
- [x] Carry source change-time together with the cursor through both the normal
      checkpoint writer and the legacy transcript-delivery writer.  This avoids
      treating an ordinary append as an unverified source rewrite on the next
      pass.
- [x] Carry the new source change-time into an incomplete HistoryWatch partial
      checkpoint, so a second pass can release a verified prefix instead of
      re-quarantining the same tail forever.
- [x] Normalize invalid optional HistoryWatch recovery proofs from typed callers
      to `RecoveryProofUnusable` before validation.  Identity/cursor corruption
      remains strict; the unusable proof is retained as a diagnostic fence and
      cannot drive automatic recovery.

### Regression evidence

- [x] Targeted linked-transcript, HistoryWatch partial-tail, ownership-stress,
      and JSON/SQLite quarantine round-trip tests pass.
- [x] Added an explicit assertion that an invalid optional HistoryWatch proof
      survives round-trip as an unusable fence rather than being treated as a
      trusted recovery range.
- [x] Both full Go test partitions passed with the runtime-dispatch environment
      left enabled: partition 0 passed 112 jobs and partition 1 passed 108 jobs.
      The earlier `helperruntime` false failure was caused by injecting
      `CXP_RUNTIME_DISABLE=1` into a test that must exercise the dispatcher; the
      standalone test passes with that variable unset.
- [x] `go vet ./...` and the focused `-race` gate for linked transcript,
      HistoryWatch quarantine, JSON/SQLite round-trip, and ownership stress
      passed (`internal/teams` 176.547s; `internal/teams/store` 4.848s).
- [x] The Docker real-data experiment passed its in-container business
      assertions after the latest fixes: 38 durable completions overall, 31
      measured closed completions in a 60-second window (`0.517 msg/s`), zero
      failed/running/interrupted rows at teardown, zero duplicate fake-Graph
      identities, zero repeated unknown POSTs, one injected unknown POST, one
      injected 429, four injected 503s, and four durable pagination witnesses.
      It also recorded no poll/history/linked phase deadline or error.
- [ ] Strict point-in-time Docker acceptance is still pending: the live helper
      changed the source SQLite/WAL while the fixture was being assembled, so
      the script correctly downgraded this run to diagnostic evidence.  The
      disposable fixture/container was the only state written; no live Teams
      request was made.  A strict run requires a source snapshot boundary that
      does not race a live writer.

### Current gate

- [x] Offline implementation and full regression matrix are green.
- [x] Latest isolated Docker run demonstrates forward progress and the safety
      invariants under fake Graph 429/503/unknown-POST conditions.
- [ ] Live per-row recovery and strict point-in-time acceptance remain
      intentionally unexecuted.  Until those separately authorized gates are
      completed, the live-service status remains **NO-GO**, not because the
      code path is proven deadlocked, but because the evidence boundary is not
      yet strict enough for an unattended live repair.

### Release-preparation validation — 2026-09-18

- [x] The affected CLI, Teams store, and Teams packages passed their complete
      local runs; the Teams package took 546.507s and the store package took
      315.931s in the first gate.
- [x] The default `go test ./...` run exposed only the existing 10-minute
      per-package timeout on the long Teams package.  The same complete
      repository gate passed serially with `go test ./... -p=1 -count=1
      -timeout=20m`; Teams completed in 537.853s and store in 315.361s.
- [x] `go vet ./...`, `git diff --check`, focused deferred-recovery/SQLite
      selectors, and the no-credential bounded Docker acceptance passed.
- [x] The CI core-b shard now explicitly selects the new deferred recovery,
      SQLite outbox hot-path, and bounded Docker acceptance tests.
- [x] The first CI attempt exposed and the follow-up patch fixed a Windows-only
      source-rewrite proof gap: `FileInfo.Sys()` exposes
      `Win32FileAttributeData.LastWriteTime`, while the generic adapter only
      recognized Unix `Ctim/Ctimespec`; the Windows adapter now queries the
      stronger handle-based `FILE_BASIC_INFO.ChangeTime` revision; if that
      query is unavailable, the automatic proof remains disabled rather than
      treating writable `LastWriteTime` as ctime.
- [ ] Strict point-in-time real-data Docker acceptance and live per-row
      recovery remain separate from this prerelease; no live service or
      database was restarted or mutated during validation.
