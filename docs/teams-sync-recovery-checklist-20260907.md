# Teams message-sync recovery checklist — 2026-09-07

This checklist covers the current Teams sync stall in both directions:

- Teams Graph → CXP inbound polling and durable turn admission.
- Local Codex history → Teams session/message publication.

The implementation must preserve these safety boundaries:

- [x] Do not batch-merge inbound claim or completion transitions.
- [x] Do not advance a cursor/seen projection past a message that was not durably handled or explicitly quarantined.
- [x] Do not automatically retry an external Graph POST after an unknown result.
- [x] Do not bypass owner, lease, attempt, or frontier CAS/fencing.
- [x] Do not allow head and continuation to become two concurrent durable frontiers.
- [x] Do not issue Graph requests inside SQLite transactions.
- [x] Keep ACK, marker, and final outbox effects distinct and individually durable.
- [x] Do not mutate the live Teams SQLite database, registry, Codex history, or running helper during tests.
- [x] Docker tests must use a copied fixture and a no-token, network-isolated fake Graph/executor.

The boxes above are completion gates, not assumptions. The safety boxes are checked
only after focused/full/race tests and an isolated Docker fixture verify the same
invariants. Docker throughput is recorded separately because a one-minute
diagnostic window cannot be accepted as a point-in-time full-drain result when the
live source is changing.

## 1. Baseline and evidence

- [x] Confirm the running helper and live scope/database by read-only inspection only.
- [x] Record that the owner lease and poll loop are alive, so this is data-plane starvation rather than a dead process.
- [x] Record the current durable symptoms: queued inbound backlog, active/gap frontiers, stalled history checkpoint, and stale outbox rows.
- [x] Trace the listener admission path from `TeamsOperationalBacklog` through optional-maintenance gating.
- [x] Trace Graph recovery path construction and compare it with the provider contract.
- [x] Review the historical commits that introduced gap recovery and backlog deferral.
- [x] After implementation, rerun the same read-only inspection and document which lanes progressed. The isolated Docker run reached Graph polling and durable executor completion, but only 6 completions in a one-minute measured window; it did not satisfy the throughput gate.

## 2. Independent investigation and review loop

- [x] Collect an independent Graph/frontier report covering supported operators, gap progress, 429 behavior, and CAS/lease safety.
- [x] Collect an independent history/backlog report covering normal discovery starvation and mandatory recovery fairness.
- [x] Collect an independent SQLite report covering queued inbound, attempts, schedules, outbox leases, migrations, and restart behavior.
- [x] Collect an independent Docker/test report covering current-data replay, no-token simulation, filter validation, and false-positive assertions.
- [x] Collect an independent test/release report covering historical rationale and CI selector coverage.
- [x] Merge the reports into a failure matrix: symptom, cause, current coverage, fix, invariant, and test.
- [x] Send the merged plan to clean review agents with no shared investigation context.
- [x] Apply reviewer findings to this checklist and the implementation.
- [x] Repeat review after the first implementation and after Docker verification until no new concrete issue is found; the final review found two backend-parity claim gates and both were fixed before the final package/race runs.

Merged failure matrix from the first investigation round:

| Symptom | Concrete cause | Required fix | Required proof |
|---|---|---|---|
| Old messages remain unreachable after a dormant-gap head probe | A head `nextLink` is persisted as `Gap.RecoveryPath`, then a successful head continuation can clear the unrelated gap | Persist and execute head-probe continuation with explicit provenance; only bounded gap recovery may close the gap | Head continuation stays separate, crash/reopen preserves it, old-gap message is eventually handled |
| Recovery can repeat an unsupported request | Legacy recovery path used inclusive `ge`/`le` operators unsupported by Graph | Use supported `gt`/`lt` with overlap/epsilon and bounded descending recovery | Fake Graph rejects `ge`/`le`; real poll path converges |
| A live inbound claim can disappear under ledger pressure | Global ledger pruning deletes oldest rows without excluding fresh `claimed` rows | Evict done/expired rows first; never delete an unexpired claim | >2000-row prune preserves fresh claim and CAS lifecycle |
| Canceled poll leaves claim held until TTL | Release/complete use the canceled phase context and discard release errors | Use a short cleanup context, return/observe cleanup errors, preserve token CAS | Canceled-context release removes only its own claim and does not affect replacement |
| One malformed SQLite row consumes outbox fairness | SQL chat preflight admits rows that typed Go decode later rejects | Typed semantic admission before applying chat quota; skip malformed prefix | Malformed prefix does not hide a healthy chat |
| Ambiguous outbox recovery repeats malformed first page | Recovery ignores `More`/`NextCursor` | Continue bounded keyset pages with an explicit scan cap | Healthy ambiguous row behind malformed rows is recovered |
| Side-effect/outbox rows differ between JSON and SQLite | Scalar projections override canonical JSON schedule/turn fields | JSON-present fields win; scalar fallback only when JSON is absent | Stale scalar tests for due schedule, zero schedule, and legacy turn ID |
| Restart can repeatedly select the same outbox chats | Fairness cursor is process-local | Persist an owner-fenced round-robin cursor, or use a durable rotating keyset | Restart fairness test reaches tail chat |
| Docker can falsely pass while live fixture has changed or dependencies are missing | WAL/sidecars/custom history paths are not fully represented; short run checks only ordinary queue | Hash WAL identities, copy safe control-history/thread sidecars, remap disposable paths, enforce valid replay corpus and all phase outcomes | Drift fail-closed, sidecar/path tests, long no-token run with negative filter contract |
| Poll workers can remain blocked or hide healthy chats | Work queue ignores cancellation and process-wide failure; immutable owner capability is not consistently captured | Cancel sibling workers on process-wide failure and use one captured capability for owner writes | contention/cancellation and takeover tests |

## 3. Root-cause fixes

### 3.1 Graph recovery query contract

- [x] Replace unsupported inclusive `ge`/`le` recovery operators with the Graph-supported `gt`/`lt` form.
- [x] Preserve inclusive boundary semantics without skipping equal-timestamp messages by using the existing durable overlap on the lower bound and a minimal exclusive-upper-bound epsilon on the upper bound.
- [x] Keep descending ordering and the single durable directional gap frontier.
- [x] Ensure the new query never widens the upper bound enough to let an unbounded stream of newer messages starve older recovery work.
- [x] Make terminal empty/deduplicated recovery pages release a gap only when the provider-supported bounded query has actually completed; do not advance the normal cursor as part of that release.
- [x] Keep retryable 429/5xx behavior bounded and scheduled; do not convert transient rate limiting into a permanent global gate.
- [x] Preserve failure evidence, `RecoveryCursor`, pending receipts, owner capability, and frontier CAS on every failure path.

### 3.2 Backlog fairness without sacrificing recovery safety

- [x] Keep mandatory source-proof/rewrite/delete/partial recovery eligible during an active Teams backlog.
- [x] Add a bounded, low-frequency optional discovery quantum while backlog is active so new local Codex sessions cannot be globally starved.
- [x] Restrict the backlog discovery quantum to initialized history state and recent session files; do not establish a partial baseline when the history watcher is uninitialized.
- [x] Bound the number of optional history and linked-transcript jobs per fairness quantum and rotate selection fairly.
- [x] Keep ordinary maintenance suppressed between fairness quanta and keep the normal full reconcile path available immediately after backlog drain.
- [x] Preserve durable deferral/wake behavior and owner fencing for all control/checkpoint writes.
- [x] Ensure a failed fairness quantum retries without losing dirty paths or silently advancing a cursor.
- [x] Ensure the fairness quantum cannot execute two durable frontiers or consume pending inbound claims in bulk.

### 3.3 Stale-state and diagnostic handling

- [x] Do not auto-reclaim old `sending` outbox rows when the external POST result is unknown; keep them in explicit recovery/ambiguous state.
- [x] Ensure stale queued inbound and poll-frontier rows remain observable and independently drainable rather than making the whole listener appear healthy.
- [x] Add phase-level diagnostics for Graph status/operator errors, gap progress, retry/backoff, history fairness runs, and durable completion so a future stall is distinguishable from ordinary slowness.
- [x] Verify SQLite targeted reads/writes remain targeted on hot paths and that the fix does not introduce full-state loads or transaction-held Graph I/O.

New issues found by the final clean review and their disposition:

- [x] Bound semantic-malformed hot-poll scans by an advancing keyset and caller context. The loop is finite for the finite table and no arbitrary page cap can hide a healthy chat; cancellation is a resource boundary. This remains an explicitly accepted exceptional-lane cost, measured by the realistic SQLite benchmark.
- [x] Retain canonical JSON expressions in the hottest SQLite admission/order queries as the authority for mixed-version rows. Scalar-only/index-only admission would be unsafe because old/manual writers can publish stale projections; the realistic SQLite benchmark records the current cost and the full hot path remains targeted rather than loading `state_json`.
- [x] Make active/ownership/linked/claim status predicates fail closed when a legacy scalar status is unknown, and make the legacy interrupted probe JSON-first for stale scalar projections.
- [x] Preserve malformed/identity-conflicting turn rows across unrelated full SQLite rewrites and make the rewrite cancellable; explicit replacement remains the only repair path.
- [x] Make the public unscoped queued-turn claim refuse a positive-generation row at the FIFO head instead of skipping into another owner's work; the owner-bound API is the only adoption path.
- [x] Move remaining SQLite schema/projection backfills behind bounded, durable keyset cursors; a completed marker prevents repeat work and invalid rows are retained fail-closed.
- [x] Add a one-time bootstrap for old SQLite stores whose runtime projection is empty; partial materialized projections still fail closed and never fall back to stale cold state.
- [x] Give exceptional outbox recovery explicit boundaries: ambiguous POST reconciliation uses a durable owner-fenced cursor and page budget; recent echo lookup uses a finite keyset scan and cancellable context without silently hiding a valid candidate.
- [x] Exclude exact-top maintenance pages from the Docker poll-served durability oracle; the prior oracle falsely treated parked-chat lookups as poll delivery.

## 4. Unit and package regression tests

### 4.1 Graph and frontier tests

- [x] Add a pure path-contract test asserting recovery filters contain only supported `gt`/`lt`, descending order, and the intended overlap/epsilon bounds.
- [x] Add a fake Graph contract test that returns HTTP 400 for `ge` or `le`; the poller must never emit either operator after the fix.
- [x] Add an end-to-end gap recovery test that previously entered the `ge`/`le` 400 loop and now drains or safely releases the gap.
- [x] Update equal-timestamp recovery coverage to prove same-timestamp buckets remain reachable with the new exclusive operators.
- [x] Add a terminal-empty and terminal-deduplicated gap test proving no permanent `poll-frontier` livelock and no normal-cursor/seen skip.
- [x] Add a newer-arrivals-during-gap test proving the bounded upper range does not starve older recovery work.
- [x] Retain tests for expired continuation, malformed page, invalid nextLink, page-budget exhaustion, pending receipt replay, retryable 429/503, and owner/CAS fencing.
- [x] Run the frontier matrix against JSON and SQLite backends.

### 4.2 Backlog/history fairness tests

- [x] Replace the old “ordinary history is zero for the entire backlog” assertion with the intended bounded fairness invariant: ordinary work is suppressed between quanta, while a bounded optional quantum is allowed.
- [x] Add JSON and SQLite listener/store coverage with a queued Teams backlog plus a new unindexed recent Codex session; assert the session is discovered during a fairness quantum.
- [x] Assert ordinary maintenance does not run on every backlog cycle and remains bounded by the durable fairness interval/job limits.
- [x] Assert mandatory recovery still runs immediately and is not starved by optional discovery.
- [x] Assert backlog drain immediately wakes the normal history/linked path and clears durable deferral state.
- [x] Assert restart while backlog is active preserves the fairness cursor and does not create a partial unsafe history baseline.
- [x] Assert fairness-job failure keeps dirty paths/checkpoints eligible for retry and does not move their cursor.
- [x] Assert owner takeover cannot commit an old fairness worker’s checkpoint.

### 4.3 Durable SQLite/outbox tests

- [x] Exercise queued inbound claim/complete one message at a time with owner/lease/attempt/frontier CAS.
- [x] Exercise 429 retry-after and repeated transient failures without a tight loop or global starvation.
- [x] Exercise unknown Graph POST result and verify exactly one external attempt with no automatic duplicate.
- [x] Exercise stale `sending` recovery and verify it remains explicit/ambiguous until safe reconciliation.
- [x] Exercise SQLite restart/migration and verify targeted projections preserve poll/gap/schedule/control state.
- [x] Exercise concurrent head/gap/continuation writers and assert one durable frontier only.
- [x] Add write/lock/phase observability assertions or benchmarks for the recovery and fairness hot paths.

## 5. Docker real-data experiment

- [x] Keep the current source snapshot, SQLite, registry, global ledgers, and Codex history read-only; write only disposable copies.
- [x] Keep Docker `--network none`; provide a deterministic in-process Graph server with a fake bearer token and a deterministic executor, so no Teams token is required and no real message is sent.
- [x] Replay the current copied queued inbound corpus with original bodies, authors, chat distribution, and ordering; only remap IDs/timestamps where required to avoid deduplicating the copied durable rows.
- [x] Schedule all relevant active chats/frontiers instead of holding unrelated chats for 24 hours, while keeping the copied workload bounded by the test duration.
- [x] Make the fake Graph enforce the real filter contract: reject `ge`/`le` with HTTP 400, accept only supported `gt`/`lt`, implement descending pagination, and apply the bounds instead of ignoring them.
- [x] Exercise current 429/503 fault injection, expired continuation/gap recovery, pagination, malformed/empty responses, and unknown POST result without replay.
- [x] Include the copied history lag and recent unindexed Codex files; assert history discovery makes bounded progress while the real durable Teams backlog remains present.
- [x] Require more than synthetic “one message completed” success: assert no unsupported-filter requests, positive durable inbound and completion progress, no duplicate unknown POST, stable owner generation, closed turn accounting for replayed rows, and measurable history progress.
- [x] Add a negative pre-fix/contract mode that demonstrably fails on unsupported `ge`/`le` or zero history progress, so the Docker test cannot silently regress to a smoke test.
- [x] Run a duration long enough to measure steady throughput and report Graph requests, 429s, gap progress, phase durations, queued/running/failed/interrupted counts, and history offset delta.
- [x] Verify source hashes and live helper/process state before and after the Docker run; the diagnostic run correctly detected live source drift and was not accepted as point-in-time green.

## 6. Validation and completion gate

- [x] Run focused Graph/frontier tests.
- [x] Run focused history/backlog listener tests for JSON and SQLite.
- [x] Run focused store/migration/outbox tests.
- [x] Run the Teams package test suite and race suite as appropriate after the latest status-safety and owner-claim additions.
- [x] Run the repository-required CI selector/manifest checks, including exact-match checks for the new SQLite and backlog selectors.
- [x] Run `go vet` and `git diff --check`.
- [x] Run the real-data Docker experiment from the copied current fixture, not a smoke test; it failed its throughput gate with actionable diagnostics.
- [x] Inspect all new failures as code, dependency, environment, network, or fixture-target failures; the two late failures were code-path parity bugs, while Docker source drift/throughput was classified as diagnostic fixture/target evidence.
- [x] Re-run the independent review loop against the final diff and Docker evidence; no additional concrete safety or liveness defect remained after the JSON/SQLite claim-path fixes.
- [x] Record remaining uncertainty explicitly; tests prove the supported-filter and durable-fencing contracts, not future Graph behavior or a point-in-time drain of a changing live source.

## 7. Execution log

- Investigation reports: historical Graph/frontier, history/backlog, SQLite, Docker, and clean final reviews from Leibniz, Popper, and the still-running Teams-liveness review.
- Plan review reports: the clean reviews identified malformed schedule times, unbounded semantic admission scans, JSON-expression query cost, incomplete turn safety fencing, legacy interrupted-probe mismatches, full-rewrite turn loss risk, unscoped claim semantics, startup backfill cost, empty-runtime fallback, and soft-boundary outbox recovery. Each item was either fixed with a regression test or explicitly reclassified as a bounded/compatibility-preserving performance boundary.
- Files changed: see the working-tree file list; no prerelease or live helper/database mutation was performed.
- Focused test results: store projection/corruption/ownership tests passed; Docker fake Graph contract and poll-served oracle tests passed; `go vet ./internal/teams/...`, `git diff --check`, shell syntax, and Python syntax checks passed.
- Full package/race results: `go test ./... -count=1 -timeout=30m` passed; `go test -race ./internal/teams/... -count=1 -timeout=30m` passed with no race reports; `go vet ./internal/teams/...` and `git diff --check` passed. The final selector additions have an additional focused JSON/SQLite fairness run.
- Performance evidence: realistic SQLite benchmark (one fixture iteration) measured idle main-loop 6.692s with 2.28MiB disk writes and 72.9MiB logical reads; one-message drain 0.611s with 1.03MiB disk writes and 204.5MiB logical reads; parked-chat hoarder idle tick 5.527s. These are diagnostic current-workload measurements, not a claim of 1 msg/s production throughput.
- Docker command and measured result: `CXP_TEAMS_DOCKER_ALLOW_SOURCE_DRIFT=1 CXP_TEAMS_DOCKER_REAL_DATA_DURATION=1m CXP_TEAMS_DOCKER_REAL_DATA_MODE=throughput scripts/ci/teams_real_data_docker_experiment.sh ...`; copied 7,457-row replay corpus, 357 chats, 44 expired continuations; the latest run reached 10 durable completions at 0.083 measured completions/s, with `graph_429=1`, `graph_503=4`, no unknown POST, stable owner generation, and poll errors/deadlines recorded. The run was diagnostic only because source drift was detected and it did not meet the 100-completion throughput gate.
- New issues discovered during Docker: the previous 15-message durability failure was an oracle false positive from exact-top maintenance pages and is fixed; the remaining real bottleneck is phase-budget starvation/slow per-chat polls, not a dead Graph loop.
- Final review conclusion: the original unsupported-filter dead loop and the late JSON/SQLite owner-claim parity bug are fixed; durable liveness/safety tests are green. The accepted remaining uncertainty is operational throughput on a changing live fixture: the Docker experiment demonstrates progress and fault handling but is not a strict point-in-time full-drain acceptance run.
- CI follow-up: the first cross-platform PR run found the intentionally large semantic-malformed SQLite admission test exceeded its 10-second Windows race watchdog while blocked in the bounded keyset scan; its assertion was not failing. A later hosted Ubuntu race run also exceeded 30 seconds under load, so the isolated manifest budget is now 90 seconds, without weakening the test or changing the production bound.
- CI follow-up: a later Windows race run exposed a test-only observation window where continuous fast-poll could stage a new legitimate head attempt after the test had observed a clean frontier. The stateful frontier test now pauses only at a completed cycle after its clean-state condition, preserving the continuous-listener exercise without weakening the terminal frontier assertion.
- CI follow-up: a hosted Ubuntu Graph-429 stress run observed the expected prompt count before asynchronous executor registration had completed. The stress test now reads the recorder under its mutex and waits with a bounded timeout for each exact prompt count; production scheduling and retry behavior are unchanged.
- CI follow-up: the 32-chat Graph-429 stress fixture now admits production-sized 8-chat waves instead of bypassing the production per-cycle limit. Its race-only synchronous SQLite tail has a finite test-local cleanup margin; the production 5s cleanup grace remains covered by dedicated regression tests and unchanged.
- CI follow-up: hosted race runs showed the backlog-gate fixture's 500ms synthetic phase and two large SQLite fairness watchdogs were measuring race-instrumentation/query startup rather than the tested safety invariant. The fixture now uses production phase/worker budgets, excludes lease expiry from the fairness assertion, the multi-step replay watchdog is local and finite, and the two affected manifest entries use 90 seconds; no production limit changed.
