# Teams backlog performance and recovery plan

Date: 2026-09-06

Scope: improve Teams backlog recovery from the current measured `0.176 msg/s`
without weakening durable ownership, deduplication, frontier, or unknown-POST
recovery semantics. The implementation target is the current main worktree. The
older worktree at `/home/baka/.local/state/codex-helper/worktrees/cxp-perf-gap-latest-20260812`
is a source of candidate tests and ideas only; it is not safe to merge as a
whole because it is a large mixed WIP based on an older commit.

## Non-negotiable safety boundaries

- [x] Never batch away per-message inbound claim, heartbeat, completion, release, token, or TTL fencing.
- [x] Never advance cursor, registry `seen`, or semantic disposition before durable handling/completion.
- [x] Never automatically replay a Graph POST/PATCH whose external result is unknown.
- [x] Preserve owner, control lease, process incarnation, attempt, receipt, frontier, and revision CAS checks.
- [x] Keep one durable frontier per chat; head and continuation must not become independent concurrent writers.
- [x] Keep Graph I/O outside SQLite transactions.
- [x] Keep ACK, marker, and final as separate semantic outbox records; no body-level coalescing.
- [x] Preserve JSON/SQLite parity and fail closed on native/compatibility disagreement.

## Baseline and acceptance gates

- [x] Inspect the current worktree and the older performance worktree without modifying either during review.
- [ ] Add a deterministic cycle/phase observation harness before claiming a throughput improvement.
- [ ] Count `completed`, `failed`, `held`, `quarantined`, and `deferred` dispositions so every input has one terminal or explicit nonterminal disposition.
- [ ] Report durable completed inbound messages per second, not executor starts per second.
- [ ] Run the same snapshot/corpus against baseline and candidate at least three times; use a long steady window and a complete drain for the final gate.
- [ ] Record phase wall time, Graph calls/statuses, SQLite transaction count and lock wait, frontier progress, oldest eligible age, and outbox disposition.
- [ ] Verify no duplicate, loss, cursor regression, frontier regression, or unexplained count mismatch.

## Implementation sequence

### 1. Make backlog admission semantics explicit

- [x] Make the production durable-frontier poll path honor `AllowBacklogDrain`.
- [x] Do not suppress pending-page replay or directional gap recovery when backlog drain is disabled.
- [x] For a plain old continuation that is not allowed to drain, use a bounded durable deferral/backoff rather than repeatedly re-reading it every cycle.
- [x] Add tests for cold/disabled-drain, pending-page, gap, restart, and owner-fenced schedule states; existing scheduler/ownership tests cover queued/running/active selection. Idle cold/parked operational frontiers are explicitly allowed to drain before auto-park.

### 2. Defer optional history maintenance durably

- [ ] Detect when a chat has operational Teams backlog without loading the whole state.
- [ ] Defer only optional linked-transcript/history scans while backlog is eligible.
- [ ] Persist next probe time/reason and retain a restart-safe wake path.
- [ ] Do not defer deletion/source-proof checks, active-turn completion, rewrite proof, or recovery-cursor work.
- [ ] Add tests for append-without-watcher-event, restart, source rewrite/delete, partial/incomplete history, and recovery after backlog drain.

### 3. Add owner-fenced schedule batching

- [x] Add a bounded owner-fenced batch API for schedule updates.
- [x] Validate one machine, process/lease generation, and active lease inside the transaction.
- [x] Apply duplicate chat updates in input order and invalidate the relevant attempt for changed rows.
- [x] Preserve frontier/revision CAS; owner fencing alone is insufficient.
- [x] Keep external I/O out of the transaction and preserve JSON/SQLite behavior.
- [x] Replace the per-update owner-bound loop only after the API and failure semantics are tested.

### 4. Remove avoidable send-path stalls

- [ ] Move pace reservation after durable send claim and all local pre-POST checks.
- [ ] Restore a queued/retryable state if a pre-POST wait is canceled or loses ownership.
- [ ] Apply a second pace reservation before deterministic quote fallback.
- [ ] Keep 429, 5xx, timeout, transport failure, and empty-ID results ambiguous/non-replayed.
- [ ] Replace marker full-store/list scans with bounded targeted durable outbox lookup while checking chat/message/kind/provenance.

### 5. Make backlog ACK/marker delivery asynchronous

- [ ] For backlog continuation only, durable-queue ACK and marker rows without synchronously issuing Graph POST.
- [ ] Keep live/new inbound ACK behavior explicit and separately tested.
- [ ] Preserve deterministic IDs, per-chat FIFO, predecessor rules, send lease, global outbound ledger, and unknown-result recovery.
- [ ] Add restart, duplicate poll, queue failure, outbox recovery, and same-chat final ordering tests.

### 6. Evaluate bounded SQLite/read optimizations

- [ ] Reuse only compatible candidate/pre-filter and bounded read ideas from the older WIP.
- [ ] Preserve per-message claim/complete semantics if a writer or lookup is batched.
- [ ] Add lock/transaction/write-volume benchmarks on representative large fixtures.
- [ ] Do not import the older WIP's native full delete/reinsert steady-state writer.
- [ ] Do not import native/compatibility JSON paths until parity and cutover tests pass.

### 6a. Remove redundant full-state startup loads

- [x] Extend runtime metadata with machine identity and keep the read path bounded for JSON and SQLite.
- [x] Add an `IsSQLite` backend probe and SQLite-only session/scope projections without materializing inbound/outbox/history rows.
- [x] Make `RecordScope` a no-op/targeted update when the SQLite scope already matches; retain the full JSON compatibility path.
- [x] Make machine-hostname restoration use bounded metadata and preserve machine/scope identity checks.
- [x] Make registry restore use control-chat/session projections on SQLite and stop rebuilding seen/sent ledgers from all durable rows.
- [x] Skip registry-to-store back-projection once SQLite authority is established; preserve the legacy JSON-to-SQLite cutover ordering.
- [x] Validate an already-published SQLite store by pointer/schema/table checks without closing the active handle or loading all business rows.
- [x] Add a SQLite indexed unfinished-turn probe before startup recovery; retain full recovery only when unfinished/unknown turns exist.
- [x] Preserve fail-closed behavior for pointer, schema, projection, scope, owner, and lease mismatches.
- [x] Add regression tests proving the existing-SQLite startup path never invokes the full SQLite state loader.

### 6b. Bound the first-poll cross-scope outbound backfill

- [x] Preserve the cross-scope helper-outbound backfill required for self-echo
      suppression; do not remove the global ledger correctness boundary.
- [x] Skip reopening the current SQLite scope during compatibility backfill;
      local durable `MessageLookup` remains authoritative for that scope.
- [x] Add an SQLite bounded projection for scope/control identity plus accepted
      and sent outbox identities and helper provenance; queued bodies and
      unrelated inbound/turn/history rows are not decoded.
- [x] Fail closed when the bounded SQLite projection is incomplete, using the
      compatibility state path only for legacy/incomplete stores.
- [x] Add a regression proving corrupt unrelated SQLite rows do not break the
      bounded global-outbound read and the full-state loader is not invoked.
- [x] Preserve sibling-scope suppression after SQLite cutover with a bridge
      black-box test.

### 7. Tune scheduler only after measurement

- [ ] Keep `8 chats/cycle`, `4 workers`, and one action/chat as the safe baseline during the first phases.
- [ ] Measure candidate cap, worker waves, phase barriers, SQLite lock wait, and executor capacity independently.
- [ ] Only then A/B `8/4`, action quantum, fast-poll wakeups, and any outbox/workflow flush limits one variable at a time.
- [ ] Never introduce concurrent head/continuation durable frontiers.

## Tests to add or adapt

### New current-main regression tests

- [x] Production `AllowBacklogDrain=false` does not consume a plain continuation, while pending pages and gaps remain recoverable.
- [x] Disabled backlog drain does not spin: the chat gets a durable bounded wake/backoff.
- [x] Backlog defer survives restart and resumes after backlog eligibility changes.
- [x] Direct backlog-drain safety matrix covers plain cold/running continuation, pending page, directional gap, and active attempt.
- [x] Runtime backlog deferral survives an actual SQLite close/reopen and resumes through the bridge poll path.
- [x] A deferred continuation survives transient Graph 429/503 failures and later clears only after a successful retry.
- [x] Owner schedule batch is atomic, owner/lease fenced, revision/frontier fenced, duplicate-order stable, and no-op write-free.
- [x] Concurrent owner schedule batches have exactly one CAS winner across JSON and SQLite, with no mixed final state.
- [x] JSON and SQLite schedule batch parity.
- [ ] Pace cancellation/claim loss does not leave `Sending` or consume a future slot.
- [ ] Quote deterministic fallback observes a second pace; unknown outcomes never issue a second POST.
- [ ] Marker targeted lookup does not hydrate the full store and still rejects wrong chat/message/provenance.
- [x] Existing-SQLite startup metadata/registry/migration paths use targeted reads; legacy JSON migration still performs the required full conversion.
- [x] Existing-SQLite startup with missing required tables or malformed pointer fails closed without falling back to stale JSON.
- [x] Scope mismatch, machine identity mismatch, registry restore, and unfinished-turn recovery preserve their current safety semantics.
- [x] Cross-scope outbound backfill reads only the bounded SQLite projection and still suppresses a sibling-scope helper message.
- [ ] Backlog ACK/marker queue-only produces no Graph POST in the poll handler and sends exactly once during outbox recovery.
- [ ] Same-chat ACK/marker/final FIFO and ambiguous-POST behavior remain intact.
- [x] Add JSON/SQLite vertical guards for a durable queued turn plus a live history tail; the current implementation is intentionally red because it starts `history-watch` before the Teams backlog drains.
- [x] Add an opt-in real-data Docker experiment with read-only snapshot, local Graph/executor, owner-generation tracing, durable turn accounting, phase deadlines, and unknown-route assertions.
- [ ] Benchmark/observation counters close all dispositions and distinguish durable completion from executor start.

### Candidate tests from the older performance worktree

Carry or adapt only after checking current-main APIs:

- [ ] `internal/teams/bridge_poll_prefilter_test.go`: adapt the three session-hint/quarantine tests if the current main still has the same prefilter helpers.
- [x] Adapt the owner-fenced schedule/CAS semantics from `internal/teams/store/owner_poll_fence_test.go` into `internal/teams/store/chat_poll_schedule_owner_test.go`; the old file's `ExecutionOwnerFence` API is not present on current main and was not copied wholesale.
- [ ] `internal/teams/store/owner_fence_mutation_test.go`: selectively carry stale-owner cleanup and checkpoint non-regression cases that use current APIs.
- [ ] `internal/teams/store/outbox_execution_fence_test.go`: carry cold-field targeted-read tests if they compile against current store projections.
- [x] Verify current-main equivalents for fresh-inbound cursor, claim loss, heartbeat/claim completion, and blocked-candidate/frontier behavior; these already exist in `poll_window_joined_regression_test.go`, `ownership_stress_ci_test.go`, `bridge_graph_429_stress_test.go`, and `poll_frontier_test.go`. The older WIP's global-outbound hooks/empty-ID APIs are not present on current main, so those tests were not copied without their implementation.
- [x] Retain current-main `internal/teams/inbound_ledger_test.go` and ownership stress coverage for claim/token/TTL fencing; do not import the older batch-writer assumptions.
- [ ] `internal/teams/perf_model_test.go`: carry only deterministic bounded backlog fixture/benchmark cases after baseline comparison.
- [ ] `internal/teams/inbound_cycle_writer_benchmark_test.go`, `message_dedupe_batch_test.go`, and `perf_amplification_test.go`: use as optional benchmarks only after the corresponding current-main APIs exist; they are not correctness proof.
- [ ] Do not copy the older worktree's native SQLite authority/history watcher/delegation test families wholesale; they depend on stale WIP APIs and mixed semantics.

### Existing current-main tests that must remain green

- [x] `poll_frontier_test.go` pending-page, gap, continuation, cursor, and partial-quantum tests.
- [x] `poll_scheduler_test.go` cap, fairness, parked, running, and continuation tests.
- [x] `store/backlog_delivery_regression_test.go` owner, outbox, frontier, and takeover tests.
- [x] `bridge_graph_429_stress_test.go` poll/outbox availability and no-retry-loop tests.
- [x] `thread_recovery_test.go` self-echo/provenance and history/live separation tests.
- [x] `history_tiered_scan_test.go`, `history_watch_proof_test.go`, and transcript quarantine tests.

## Throughput forecast

- [ ] Baseline reference: current experiment reports `0.176 executor starts/s`, but its `48 runs / 47 completed` accounting is not a valid durable-completion gate.
- [ ] After history/linked durable defer, the measured cycle model is approximately `45.46 - 14.55 - 5.28 = 25.63s`, or `8 / 25.63 = 0.312 msg/s` before local optimizations.
- [ ] First safe implementation target: `0.30–0.35 durable completed msg/s`.
- [ ] After schedule batch, send-path fixes, targeted lookup, and backlog queue-only: expected `0.35–0.50 msg/s`, workload dependent.
- [ ] `0.5–0.8 msg/s` is a later A/B target only if scheduler/SQLite/Graph data supports tuning `8/4` or action quantum.
- [ ] `1 msg/s` requires eight effective actions to complete in under roughly eight seconds, or a safely larger action budget; it is not a first-phase promise.

## Final validation and handoff

- [x] Run focused and full tests for each changed package; performance benchmarks remain gated on the observation harness.
- [x] Run `go test ./internal/teams` and `go test ./internal/teams/store`; these full package gates passed.
- [x] Run the repository-wide `go test ./... -count=1` gate; all packages passed.
- [x] Run `git diff --check` and inspect the complete unpublished diff.
- [x] Do not run the live helper or use the real Teams token; use isolated fake Graph/temporary SQLite fixtures.
- [x] Report changed files, carried tests, test commands/results, measured throughput, and residual uncertainty; the newly added backlog guard is expected to fail until the production phase gate is implemented.

## Execution log

- 2026-09-06: Added `ChatPollScheduleUpdate` revision CAS and `UpdateChatPollSchedulesForOwner`; JSON and SQLite use one owner-fenced batch transaction, with stale revision rejection before commit.
- 2026-09-06: Replaced owner-bound per-chat schedule writes in `pollOnce` with the batch API. A cold/parkable chat with an operational continuation is still drained when it has no running/queued turn; an active/queued chat gets a durable retry deferral.
- 2026-09-06: Added `internal/teams/backlog_drain_deferral_test.go` for disabled-drain behavior and restart wake, and `internal/teams/store/chat_poll_schedule_owner_test.go` for JSON/SQLite owner/lease/CAS/atomicity/duplicate/no-op behavior.
- 2026-09-06: Expanded regression coverage with the disabled-drain frontier matrix, SQLite runtime close/reopen wake, transient Graph 429/503 recovery, and concurrent JSON/SQLite schedule CAS races.
- 2026-09-06: Final gates passed: focused new tests repeated 10 times; owner batch tests passed under `-race`; `go test ./internal/teams/store -count=1` in 51.276s; `go test ./internal/teams -count=1` in 263.754s; repository-wide `go test ./... -count=1` passed (`internal/teams` 279.396s, `internal/teams/store` 67.043s); `git diff --check` passed.
- 2026-09-06: The real-data Docker experiment copied a read-only snapshot containing 7,457 replayable inbound messages across 357 chats, but the current listener did not reach its first poll. It acquired the disposable lease and owner heartbeat, then stalled in `migrateRegistryProjectionToStore` while loading the large SQLite-backed state; `Graph GET=0`, `executor_runs=0`, and the reported `0 msg/s` is not a valid steady-state throughput result. The copied database passed `PRAGMA quick_check`, and the experiment used no Teams token or live Graph request.
- 2026-09-06: Replaced existing-SQLite startup full loads with bounded runtime metadata, scope/session projections, indexed unfinished-turn detection, pointer/schema/table validation, and an SQLite-authority registry boundary. Legacy JSON still uses the original full migration path; malformed pointers and missing required tables fail closed.
- 2026-09-06: Added `internal/teams/store/startup_bounded_regression_test.go` and a bridge black-box regression that corrupt unbounded inbound/outbox rows after SQLite cutover; bounded startup operations still succeed and the full SQLite loader hook remains unused. New store/bridge tests, store package, teams package, and focused race tests passed.
- 2026-09-06: The first Docker run after the startup fix reached readiness in `13.122s`, but the first poll still took `47.864s` and made no progress. Investigation found the remaining hidden full-state load in cross-scope global-outbound backfill (`shouldIgnoreMessage` -> `ensureGlobalOutboundBackfilled`), not in startup migration itself.
- 2026-09-06: Added the bounded SQLite global-outbound projection and skipped the current scope during compatibility backfill. The read retains accepted/sent outbox identities and helper provenance, preserves sibling-scope self-echo suppression, ignores queued/unrelated business rows, and has no full-loader invocation in regression tests.
- 2026-09-06: The second isolated real-data Docker run reached readiness in `14.722s`, began its first execution in `16.195s`, and reported `replayed_inbound=16`, `completed=16`, `failed=0`, `executor_runs=16`, `wall_inbound_per_sec=0.177`, `steady_executor_per_sec=0.216`, `poll_last_duration=3.340s`, and `history_watch_last_duration=14.103s`. This proves the first-poll full-load stall is removed and progress resumes, but it is not a complete-drain acceptance result: the `7,457`-message snapshot ran for `90s` with only two poll cycles, while history/linked-transcript maintenance still consumed roughly `14s` phases and emitted timeout/cancel errors.
- 2026-09-06: After the bounded global-outbound change, focused tests, both package gates, race-focused tests, repository-wide `go test ./... -count=1`, and `git diff --check` all passed. The Docker experiment used a disposable read-only snapshot, fake Graph/local executor, `--network none`, and no Teams token; the live helper and live database were not modified by the experiment.
- 2026-09-06: Added `listener_backlog_optional_maintenance_regression_test.go` in JSON and SQLite variants. Both reproduce the current bug deterministically: with one blocked running turn and one durable queued turn, the listener enters `history-watch`; the test fails before any lease-timeout timing is needed.
- 2026-09-06: Added `docker_real_data_experiment_test.go`, fixture support, and `teams_real_data_docker_experiment.sh`. A 90-second run against a fresh snapshot of the current environment replayed 7,457 ordinary queued messages across 357 chats, reached 16 durable inbound rows at `0.178 inbound/s` and `0.230 executor/s`, then failed the new hard gates because owner generations changed `5291 -> 5292`; it also recorded history/linked-transcript cancellation/deadline errors. No unknown Graph route was observed, and no Teams token/network was used.
- 2026-09-06: Repeated the same 90-second run after fixing the shell runner's failure-path integrity check. It reached 16 inbound / 15 completed / 1 interrupted (`0.178 inbound/s`, `0.218 executor/s`), changed owner generations `5298 -> 5299`, recorded a 15.407s history-watch deadline and a 14.626s linked-transcript cancellation window, and failed the durable-completion gate. The post-run source hash check reported that the live source pointer/database changed while the real helper was concurrently active; the experiment itself only read the source into a disposable `.backup`/runtime copy, so the hash delta cannot be attributed to the container. The corrected runner now always reports this condition before returning the test failure.
- Not implemented in this execution: durable linked/history deferral, pace/quote/marker send-path changes, queue-only backlog ACK/marker, full phase/disposition observation harness, and scheduler cap/worker/action tuning. These remain gated on measurements and the safety tests listed above.
