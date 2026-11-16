# Teams mirror recovery validation

## Behavior

Replacing or truncating a tracked rollout sets a fixed recovery boundary at the observed end of the file. Missing records through that boundary remain eligible for delivery. Existing outbox identities continue to suppress represented records. No timestamp rule discards uncertain content.

The sender packs adjacent complete recovery records into its existing frozen outbox batches. Each record retains its delivery identity. Repeated identical records carry an explicit count. Incompatible payloads still use the normal delivery path. Current messages can pass queued recovery while in-flight sends retain conversation ownership. The existing fairness counter gives aged recovery a send opportunity. Recovery alone does not wake parked conversations.

Content already present at the first observation of a replacement is recovery, including any newly appended content. It is delivered to the work chat without live completion notifications. A subsequent append is live. A missing outbox record means prior delivery cannot be proved, so a labeled recovery can repeat a previously delivered record.

## Validation

The implementation was first tested against upstream 0d0fdb48594fadaee09c3ddd027d6eea47e0d8eb, then rebased by fast-forward to upstream ae3fc99ccdfdfb20aec0fbb02765faf8bc77ac9c. The changed tests are selected by the CI race groups and end-to-end shards. A prior isolated run completed all three shards: 182 scenarios and 1,542 explicit checks. The replacement with incomplete ledger scenario recovered 702 compaction records in three posts, preserved the full offline question, progress, and answer, continued live delivery, and did not replay on restart. A separate test recovered 864 records across 12 parked chats in 12 batches.

After the fast-forward to ae3fc99c, the bundled isolated Docker harness ran `bash scripts/ci/run_e2e_shard.sh 0`, `1`, and `2` with Go 1.25.11: all passed (186 scenario executions, including the separately selected recovery scenario). On Linux/WSL with Go 1.26.3, `bash scripts/ci/race_core_packages.sh engine` passed (engine 282.405 s, asr 147.090 s); `go test -race -count=1 -timeout=15m -parallel=8 ./internal/core/store ./internal/core/transcript ./internal/channel/...` passed (store 183.548 s). The harness used fake Graph and Codex, without real Teams chats. `git diff --check` passed.

After the first CI run caught a forbidden internal transcript import in the black-box rewrite test, the test was changed to use standard file information and the stored source checkpoint. `go test ./internal/e2e ./internal/e2e/catalog ./internal/e2e/harness -count=1` and the isolated rewrite scenario both passed after this correction.

The existing rewrite test now waits for the replacement checkpoint before appending the next live turn; its original exact-once assertions remain. The moved-home fixture writes its new turn before the first observation, so its expected transcript now has one labeled recovery post; explicit assertions verify both the full question and answer. The recovery batch scenario asserts exact content, counts, bounded posts, restart behavior and transport invariants instead of a golden transcript because batch boundaries depend on sender timing.

In a 20,000-row retained-history fixture with 256 pending records, ordinary polling measured about 67–68 µs before and 74–75 µs after, with unchanged 5.2 KB and 120 allocations. Recovery with a live message behind the backlog measured 222–227 µs versus 68–69 µs for the prior FIFO lookup. The additional ordering examines active rows only. Repeated read-only polling did not write the database; recovery boundary updates occur only when the boundary changes.

Windows amd64 tests were cross-compiled with CGO disabled; Windows/macOS native execution and real Teams delivery were not tested locally. The separate inbound identification issue, where old helper messages can be treated as user requests, is not changed here.
