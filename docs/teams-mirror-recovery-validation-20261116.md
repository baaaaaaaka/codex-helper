# Teams mirror recovery validation

## Verified restart after replacement

The schema 14 implementation supersedes automatic recovery posting. A changed device or inode, a shorter file, or a mismatch in the bytes before the cursor requires verification. The mirror compares the session ID, a bounded window before the durable cursor, and the bytes already read for an open turn. If the cursor moved, it also requires a unique matching pair of turn start and completion records. A verified source resumes at the corresponding byte and line. A source without enough evidence stays paused until the original file is restored or an operator checks the file and explicitly requests `helper publish-history full`. A replacement that invalidates the byte offsets of an active import also requires a new import request.

Verification runs outside the mirror loop, with a worker reserved for active chats. A transient I/O failure is retried with backoff. A stable file with insufficient evidence is checked again when it changes. A queued final for a completed Teams turn can be released once both the final and the detected transcript obstruction are at least ten seconds old. The final keeps its delivery key and late transcript progress for that turn is suppressed. New tasks on the blocked thread can use callback progress. Normal sender ordering and Teams retry timing still apply.

Schema 14 keeps the body, delivery keys, and batch evidence for old automatic recovery records. Queued records require explicit full-history publication to become eligible again. Sending and unknown records remain subject to read-back. Confirmed absence moves a legacy batch to review. A forced replay already uses a new delivery namespace and leaves the old queued review records on hold.

The isolated WSL audit used a copy of the repaired schema 13 database and read-only access to 593 recorded rollouts totalling 27,312,204,099 bytes. The files matched the repair manifest. Creating simulated checkpoints took 230 ms. Two serial verification passes took 71 ms and 68 ms over 49,140,246 logical evidence bytes per pass, before buffered read-ahead. These measurements are for local same-position verification with a warm file cache. They exclude polling, SQLite writes, Teams transport, and scans to relocate an anchor. The migrated copy passed SQLite quick and foreign-key checks and kept all 593 source rows. A separate engine rehearsal opened a fresh copy of the repaired database, ran 40 simulated poll ticks against the original files, saved 593 checkpoints, paused zero chats, and added zero outbox rows. The existing repaired database, installed helper, and real Teams chats were outside the write tests.

For three warm idle polling runs over the same 593 files, the original implementation took 1.37–1.40 ms per pass. The candidate took 1.44–1.50 ms. Both used approximately 199 KB and 1,193 allocations per pass. The fixture had no store connection, so a pass did not write to SQLite or read transcript records.

### Candidate validation

The worktree based on `09e862bf` passed the engine, store, and transcript suites under `go test -race -count=1` with Go 1.26.3 on Linux/WSL. The backpressure restart regression also passed three consecutive focused race runs after the verification grace started tracking the obstruction time. The E2E framework and catalog tests, `go vet` for the three core packages, and `git diff --check` passed.

The three `scripts/ci/run_e2e_shard.sh` shards ran in separate Docker harness containers with fake Graph and fake Codex. They reported 189 of 190 scenarios passing. The copy, replacement, database upgrade, and transcript progress scenarios passed. `CMD-EFFORT` passed its functional checks and differed from its golden at the control prompt truncation boundary. The same difference reproduced in an archived copy of `09e862bf` in the same test environment.

The transcript tests cross-compiled for Windows amd64 and macOS arm64. Native execution on those platforms and live Teams delivery were outside this validation.

## Previous recovery behavior and validation

The remainder of this record describes the superseded implementation.

### Behavior

Replacing or truncating a tracked rollout sets a fixed recovery boundary at the observed end of the file. Missing records through that boundary remain eligible for delivery. Existing outbox identities continue to suppress represented records. No timestamp rule discards uncertain content.

The sender packs adjacent complete recovery records into its existing frozen outbox batches. Each record retains its delivery identity. Repeated identical records carry an explicit count. Incompatible payloads still use the normal delivery path. Current messages can pass queued recovery while in-flight sends retain conversation ownership. The existing fairness counter gives aged recovery a send opportunity. Recovery alone does not wake parked conversations.

Content already present at the first observation of a replacement is recovery, including any newly appended content. It is delivered to the work chat without live completion notifications. A subsequent append is live. A missing outbox record means prior delivery cannot be proved, so a labeled recovery can repeat a previously delivered record.

### Validation

The implementation was first tested against upstream 0d0fdb48594fadaee09c3ddd027d6eea47e0d8eb, then rebased by fast-forward to upstream ae3fc99ccdfdfb20aec0fbb02765faf8bc77ac9c. The changed tests are selected by the CI race groups and end-to-end shards. A prior isolated run completed all three shards: 182 scenarios and 1,542 explicit checks. The replacement with incomplete ledger scenario recovered 702 compaction records in three posts, preserved the full offline question, progress, and answer, continued live delivery, and did not replay on restart. A separate test recovered 864 records across 12 parked chats in 12 batches.

After the fast-forward to ae3fc99c, the bundled isolated Docker harness ran `bash scripts/ci/run_e2e_shard.sh 0`, `1`, and `2` with Go 1.25.11: all passed (186 scenario executions, including the separately selected recovery scenario). On Linux/WSL with Go 1.26.3, `bash scripts/ci/race_core_packages.sh engine` passed (engine 282.405 s, asr 147.090 s); `go test -race -count=1 -timeout=15m -parallel=8 ./internal/core/store ./internal/core/transcript ./internal/channel/...` passed (store 183.548 s). The harness used fake Graph and Codex, without real Teams chats. `git diff --check` passed.

After the first CI run caught a forbidden internal transcript import in the black-box rewrite test, the test was changed to use standard file information and the stored source checkpoint. `go test ./internal/e2e ./internal/e2e/catalog ./internal/e2e/harness -count=1` and the isolated rewrite scenario both passed after this correction.

The existing rewrite test now waits for the replacement checkpoint before appending the next live turn; its original exact-once assertions remain. The moved-home fixture writes its new turn before the first observation, so its expected transcript now has one labeled recovery post; explicit assertions verify both the full question and answer. The recovery batch scenario asserts exact content, counts, bounded posts, restart behavior and transport invariants instead of a golden transcript because batch boundaries depend on sender timing.

In a 20,000-row retained-history fixture with 256 pending records, ordinary polling measured about 67–68 µs before and 74–75 µs after, with unchanged 5.2 KB and 120 allocations. Recovery with a live message behind the backlog measured 222–227 µs versus 68–69 µs for the prior FIFO lookup. The additional ordering examines active rows only. Repeated read-only polling did not write the database; recovery boundary updates occur only when the boundary changes.

Windows amd64 tests were cross-compiled with CGO disabled; Windows/macOS native execution and real Teams delivery were not tested locally. The separate inbound identification issue, where old helper messages can be treated as user requests, is not changed here.
