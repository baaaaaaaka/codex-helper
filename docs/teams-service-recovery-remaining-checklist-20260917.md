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
