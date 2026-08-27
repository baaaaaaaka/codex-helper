# Teams recovery safety fix checklist

This checklist records the implementation and verification of the three
source-level regressions reproduced from the recent Teams incidents:

1. a normal transcript write window could be classified as an unresolved
   execution;
2. one malformed SQLite checkpoint could stop unrelated sessions and could be
   overwritten by an ordinary read/update path;
3. a local supervisor could wait on an inherited pipe or restart before the old
   child identity was confirmed gone.

The worktree is based on the freshly fetched `origin/main` commit
`8b6521f027e8919fb98caf03785e9a8dae4b6d56`:

- worktree: `teams-recovery-safety-20260827`
- branch: `fix/teams-recovery-safety-20260827`
- host Teams helper/service: not restarted, reloaded, replaced, or modified
- original dirty worktree: not modified

## Scope decision

The patch is intentionally limited to the defects that were reproduced in the
source-level regression tests. It completes the local scanner, SQLite
checkpoint, and supervisor lifecycle fixes below. Graph/network/Docker tests,
disk-full fault injection, SQLite BUSY/FULL/IO failpoints, PID-reuse races,
instance-token/status-revision protocol changes, and a fully transactional
cross-system delivery redesign are follow-up work; they are not silently
counted as completed by this checklist.

## Acceptance invariants for this patch

- [x] A malformed or identity-conflicting SQLite checkpoint is isolated to its
  own row/session; healthy sessions remain readable.
- [x] No automatic path guesses a cursor, replays an unproven execution, or
  turns a history-only quarantine into a live execution owner.
- [x] A quarantine can advance a physical frontier only when it has a complete
  source/range proof; metadata-only quarantine remains non-advancing.
- [x] The exact raw bytes and nullable SQL metadata of an opaque checkpoint are
  preserved by ordinary full-state rewrites.
- [x] An opaque checkpoint can change only through an explicit raw-byte CAS
  repair operation; stale repair input loses with a conflict.
- [x] A supervisor uses bounded pipe draining and does not start a replacement
  while the old child/process group is not confirmed gone.
- [x] Existing TUI-vs-Teams single-writer safety was not relaxed.
- [x] The normal linked-transcript idle path has no new full-prefix hash, N+1
  query, or measurable allocation regression against the test baseline.

## 0. Baseline and workspace

- [x] Fetch and inspect the latest `origin/main` before creating the worktree.
- [x] Create the isolated worktree and implementation branch from that commit.
- [x] Inspect all affected scanner, bridge, store, supervisor, test, and CI
  call sites before editing.
- [x] Run the three original regression scenarios against the baseline and
  record the expected failures: legacy-generation history sync rejected an
  invalid recovery proof; malformed canonical SQLite JSON aborted the read;
  and detached-pipe supervisor restart reached the context deadline.
- [x] Record the affected platform split: common supervisor lifecycle code,
  Unix process-group verification, and Windows compile-safe stubs.

## 1. SQLite malformed-checkpoint isolation

### Decoder and read contract

- [x] Add a SQLite-only row decoder that reads nullable `id`, `session_id`,
  `status`, `updated_at`, and raw JSON without losing the original row.
- [x] Define explicit dispositions for valid, malformed canonical,
  identity-conflict, foreign/noncanonical, missing, provenance-invalid, and
  infrastructure-error outcomes.
- [x] Treat only `sql.ErrNoRows` as missing; SQL/driver errors remain errors.
- [x] Allow a deterministic malformed fallback only for a canonical
  `transcript:<session>` SQL identity whose SQL session matches the key.
- [x] Reject canonical key/session conflicts even if the conflicting JSON is
  otherwise syntactically valid.
- [x] Keep valid operation-specific rows such as
  `transcript:<parent>:subagent:<id>` compatible with normal updates.
- [x] Make the opaque fallback contain only trusted SQL identity/metadata, a
  fixed history quarantine, `LegacySourceUnverified`, and an unresolved
  session-local fence.
- [x] Strip all unproven cursor/source/range/record/proof fields from the
  malformed/identity-incomplete fallback.
- [x] Keep the generic JSON backend strict; the explicit opaque repair API is
  SQLite-only.

### Reader and writer behavior

- [x] Route SQLite full, selected, scoped, single-row, snapshot, ownership,
  dedupe, thread-binding, queue, completion, delivery, and transactional
  checkpoint reads through the specialized decoder.
- [x] Keep malformed, foreign, and provenance-invalid rows from being treated
  as healthy typed checkpoints or as an absent checkpoint that permits a
  replay.
- [x] Keep a bad session from aborting unrelated healthy-session reads.
- [x] Remove read-time/queue-time fallback upserts.
- [x] Preserve opaque raw JSON and nullable SQL metadata across ordinary
  `Store.Update`/full-state rewrites.
- [x] Keep SQLite migration verification compatible with row-local checkpoint
  isolation: preserve the untrusted row for explicit repair while comparing
  only the typed projection that normal readers are allowed to expose.
- [x] Fail the ordinary typed update with `ErrOpaqueCheckpoint` instead of
  silently serializing a synthetic fallback over the raw row.
- [x] Add explicit `RepairOpaqueImportCheckpoint` raw-byte CAS repair with
  identity checks, exact raw compare, `RowsAffected == 1`, idempotent retry,
  and conflict handling.
- [x] Fix the compatibility regression found during implementation so valid
  noncanonical operation/subagent checkpoint rows are still updateable.

### SQLite regression tests

- [x] Cover malformed JSON, empty/null JSON, invalid typed fields, nullable SQL
  columns, and canonical/foreign/noncanonical identities.
- [x] Cover one malformed row alongside multiple healthy sessions through
  full, selected, scoped, snapshot, and ownership-related reads.
- [x] Assert no malformed-row claim/upsert and unchanged raw bytes after an
  unrelated update.
- [x] Cover explicit repair success, idempotent retry, and stale raw CAS
  conflict.
- [x] Cover valid noncanonical operation checkpoint create/update/read.
- [x] Cover canonical SQL-key/session identity conflict isolation.
- [x] Run the focused SQLite regression suite and include it in the Linux CI
  behavior step.

## 2. Transcript source proof and progress

### Source observation and scanner

- [x] Keep scanner parsing and bounded range-proof hashing on one open source
  file descriptor.
- [x] Revalidate descriptor identity, pathname identity, and non-shrink after
  parsing/hashing; discard records/proofs when the source is replaced or
  becomes unstable.
- [x] Avoid reopening the path to recompute the same range proof in the normal
  scan path.
- [x] Preserve the append-only/atomic-replacement threat model: a bounded
  digest is not claimed to detect arbitrary same-size in-place mutation when
  no prior proof exists, so uncertain observations fail closed.

### Legacy and live transcript behavior

- [x] Carry a fresh source generation onto history-only quarantine state when a
  legacy checkpoint is observed.
- [x] Make a metadata-only quarantine valid for storage but unusable as a
  physical frontier.
- [x] Attach a quarantine range frontier only after the exact scanned range
  can be fingerprinted; otherwise retain explanation/candidate metadata only.
- [x] Treat a trailing `task_started` without its subsequently written prompt
  as a transient pending root marker and retry/release it when the prompt is
  observed, including across SQLite store reopen.
- [x] Preserve mixed-ID `event_msg`/`response_item` mirror handling without
  requiring equal protocol IDs.
- [x] Keep explicit history recovery able to clear the old history-only
  quarantine once the operator-selected range is durably processed.
- [x] Ensure malformed/oversized complete records advance to a deterministic
  next byte boundary so later status/final records remain reachable.

### Transcript regression tests

- [x] Reproduce the legacy-generation mixed-ID final case and verify it remains
  history-only, does not replay the consumed prefix, and does not publish an
  ambiguous final.
- [x] Reproduce the delayed `task_started`/prompt race across multiple polls,
  one-poll completion, and SQLite store reopen.
- [x] Cover metadata-only quarantine and assert it cannot advance the
  scanner frontier.
- [x] Cover explicit history recovery from the stored pending range.
- [x] Cover malformed records and large invisible complete records advancing
  the cursor without livelock.
- [x] Run these tests in the exact Linux CI behavior command and in `-race`.

## 3. Supervisor bounded cleanup

### Lifecycle behavior

- [x] Set an explicit `exec.Cmd.WaitDelay` before child start to bound inherited
  pipe draining.
- [x] Route context shutdown, health restart, normal exit, and cleanup failure
  through common cleanup/status helpers.
- [x] Give cleanup a deadline independent of the canceled supervisor context.
- [x] Revalidate the recorded child/process-group identity before signaling.
- [x] Verify leader and original process-group disappearance before clearing
  child status.
- [x] Persist `cleanup_failed` and stop the restart loop when cleanup cannot be
  proven complete.
- [x] Reconcile durable child PID/PGID state before a new supervisor starts;
  refuse a new child when the old leader is gone but its recorded group is
  still alive.
- [x] Keep Windows/macOS boundary code compiling without claiming Unix
  process-group semantics on Windows.

### Supervisor regression tests

- [x] Run the detached-pipe health-restart regression and assert bounded return,
  child status clearing, and no context-deadline hang.
- [x] Keep the existing supervisor health-restart and stress/fail-closed tests
  green.
- [x] Run the supervisor regressions in `-race`.
- [x] Compile the affected CLI package for Linux, macOS amd64/arm64, and
  Windows amd64.

## 4. CI, compatibility, and performance

- [x] Add required Linux CI behavior tests for transcript, SQLite, and
  supervisor regressions; keep compile-only checks separate from behavior
  evidence.
- [x] Parse `.github/workflows/ci.yml` after adding the behavior step and fix
  the indentation regression found by the YAML validation pass.
- [x] Run `go test ./... -count=1` successfully after the final source/test
  changes.
- [x] Run the affected regression tests with `-race` successfully.
- [x] Run the affected platform compile checks successfully.
- [x] Compare the linked-transcript idle, history-watch, legacy backfill, and
  SQLite checkpoint benchmarks against the production-equivalent baseline.
- [x] Re-check the normal idle benchmark after the SQLite decoder optimization:
  allocations returned to baseline range (for example, many-short idle
  sessions were about 2861 allocs/op versus about 2862 in baseline).
- [x] Run `git diff --check` after formatting and edits.

## 5. Explicitly deferred follow-ups

These are intentionally not marked complete because they require a larger
fault-injection harness or a separate protocol/state redesign. They are not
needed to make the three reproduced source regressions pass.

- [ ] Docker/Graph end-to-end runs with no real Teams sends.
- [ ] Graph stall, full-window/backlog recovery, and multi-day service outage
  combined with chat-freeze scenarios.
- [ ] SQLite BUSY/FULL/IO/context-cancellation failpoints and commit-unknown
  tests for the repair API.
- [ ] Disk-full and transcript write-failure simulation.
- [ ] Concurrent scanner/CAS failpoints covering every outbox/ledger crash
  ordering and a formal at-least-once duplicate boundary.
- [ ] PID-reuse races, startup interruption before identity persistence, and
  status revision/instance-token CAS for late old-supervisor writes.
- [ ] A complete SQL query-count/IO-counter gate in CI rather than benchmark
  comparison.

## 6. Final handoff

- [x] Review the complete diff, including untracked regression tests and CI.
- [x] Confirm no host Teams service/helper lifecycle action was performed.
- [x] Confirm no original dirty-worktree files were touched.
- [x] Record the final test results in the handoff response.
- [x] Commit/release the branch after the explicit user request received for
  this turn.
