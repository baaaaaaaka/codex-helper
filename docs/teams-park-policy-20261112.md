# Teams automatic park policy

Worktree base: `ce75760d69b420d1dfcae05f495a9429972a1bd0`.
Branch: `feat/teams-park-policy`.

## Commands

| Location | Command |
| --- | --- |
| Work chat | `helper park on`, `helper park off`, `helper park status` |
| Control chat | `park s003 on`, `park s003 off`, `park s003 status` |
| Terminal | `cxp teams service park s003 on`, `off`, or `status` |

All existing and new work chats default to automatic park **on**. A new
independent conversation, including a fork, uses this default. Setting **off**
survives restarts and chat recreation. It prevents idle automatic park; manual
park still works. Bare `helper park` and `park s003` keep their previous meaning.
Invalid extra arguments do not park a chat.

Changing the policy does not wake a parked chat, activate a held/closed chat,
or keep an agent process alive. Resume a parked chat with `r s003` in the control
chat; messages received while parked can then run. The control chat and the
delegation inbox do not expose this policy.

The policy setter does not change the activity timestamp. A command or message
posted in the target work chat still counts as ordinary user activity; terminal
and control-chat policy changes do not manufacture activity in the target chat.

## Receive timing

Normal timing now uses the complete receive tiers with or without an activity
scanner. Running requests and the first two minutes after activity use 2s;
2–15 minutes use 5s; 15 minutes–4 hours use 10s; older active chats use 30s.
After 48h, an on chat is eligible for automatic park, subject to open turns,
pending sends and the maintenance cadence. An off chat continues the cold tier.

The control interval stays 5s. Activity scans remain at 5s and may advance a
chat read. Explicit interval configuration, read pacing, Retry-After, scan
failure fallback and error backoff still apply. Off chats keep their normal
tiers during scan outages; default-on chats retain the existing scan fallback.
These are target intervals,
not a deadline under overload. Idle reads are more frequent than the previous
10-minute scan safety reads; the shared read rate is not increased.

The previous default retry base remains 1s even though hot polling is now 2s.

No new phase scheduler, timer per chat, worker pool, HTTP limiter or burst rule
is introduced. The existing receive jitter, priority rules and scan-generation
handling remain in place.

## Storage and concurrency

Schema 8 adds `conversations.auto_park_off INTEGER NOT NULL DEFAULT 0` with a
constant default. Existing chat state and history are retained. Repeated settings
do not update the conversation row. Startup loads the field with the existing
conversation collection; polling does not query it from SQLite.

CLI setters use the existing exchange and the owning helper. Policy queries use
a store read transaction and do not create CLI progress rows in the business
store; the exchange still records request delivery. Chat commands necessarily
record their inbound completion and reply. Cache publication follows a successful
policy commit. Automatic park checks the persisted policy in its write transaction,
so a stale candidate cannot override an already committed off setting. Manual
park intentionally does not check the automatic policy.

## Validation

Local Linux tests used Go 1.25.11 with session-owned build cache and temporary directories.

- `go test -race -count=1 -timeout=15m ./internal/core/store ./internal/core/engine`
  passed (store 161.962s). After the final changes, the complete engine race
  package passed again (150.125s).
- `go test -race -count=1 -timeout=3m ./internal/cli ./internal/core/engine -run
  'Test(TeamsServicePark|TeamsPollIntervalTiming|ParkPolicy|OnlyIdleChatsTakeASafetyRead|SafetyReadsSpreadOverTheWindow|ScanFailuresFallBackToDirectReads)'`
  passed, including the maintenance check against a deliberately stale candidate.
- The final focused policy/CLI race run passed (CLI 1.033s, engine 11.090s),
  including recreate/fork defaults, status display and failed-transaction cache
  publication.
- `go test -race -count=1 -timeout=5m ./internal/core/migrate` passed (280.898s).
- `go test -count=1 -timeout=3m ./internal/skills` passed (1.050s).
- `go vet ./internal/core/store ./internal/core/engine ./internal/cli` passed.
- `CGO_ENABLED=0 GOOS=windows go build -o <session>/cxp-windows.exe ./cmd/codex-proxy`
  passed. This is cross-compilation, not native Windows execution.
- `go test ./internal/core/engine -run '^$' -bench
  BenchmarkParkPolicyInterval10000Chats -benchmem -count=1`: about 0.170ms per
  10,000 interval calculations, 0 B/op and 0 allocs/op. The same workload on the
  base revision measured 0.104ms. The additional work is about 0.066ms per 10,000
  calculations; it adds no database reads or allocations. This microbenchmark
  does not measure Graph throughput or network latency.
- Store regressions cover default-on migration, restart persistence, unchanged
  timestamps, no writes for repeated settings and reads, rollback, manual park,
  and both transaction orders between policy changes and automatic park.

Two long-history E2E scenarios use a 500ms fast-test interval instead of 250ms.
With full polling tiers, repeated history posts can warm the chat; multiple HTTP
pages per 250ms logical read exceeded the unchanged 10 HTTP reads/s test cap.
The 500ms fixture remains faster than production and retains that cap. The new
restart test waits for the restarted control read before issuing its CLI query.

The existing scan-recovery test now expects the normal cold tier after recovery,
instead of no direct reads for ten minutes. The jitter test retains its original
long custom period to test distribution independently of production cadence.

The new tests are covered by the existing whole-package core/store and engine
race jobs. `TeamsServicePark` is added to the CLI race selection. The lifecycle
E2E package is discovered by the existing E2E shards.

Docker checks use the session-only copy of the bundled real-process harness.
Final checks compare transcripts with `E2E_UPDATE_GOLDEN` unset. Complete package
results:

| E2E package | Result |
| --- | --- |
| lifecycle (including PARK-POLICY) | passed, 135.085s |
| commands | passed, 307.871s |
| dashboard | passed, 166.674s |
| chats | passed, 117.600s |
| edits | passed, 83.766s |
| faults | passed in a separate complete rerun, 82.846s |

`go test -p=1 -parallel=2 -count=1 -timeout=3m
./internal/e2e/scenarios/entry -run TestEntryControlCommandNotReplayed` passed
separately (13.874s), including snapshot comparison for active, parked and held
work chats after legacy migration.

The faults rerun followed review of one missed `Auto park: on` status transcript
line in R-5b. The migration entry scenario also exposed the new default policy
in its held-chat status line; that expected line was reviewed separately.
No behavior assertion, rate cap or request invariant was removed or relaxed.
No installed helper, service or live state is changed by these tests.

The first Docker setup attempts exposed environment limits, not product failures:
the runtime image lacked Go, sandboxed Docker credential discovery could not use
the WSL socket, `/tmp` was not executable, and unrestricted Go build parallelism
hit the container process limit. A session-only copy of the bundled harness added
the Go toolchain/module cache, used the required Docker access, selected an
executable scratch directory and limited build parallelism. No permissions on
live state were changed. The test image used empty configuration and private
helper state.

These tests run real helper processes with fake Graph and fake Codex endpoints.
Live Microsoft Graph limits and native Windows/macOS execution were not tested.
At the time of the local validation, no GitHub CI run, commit, push, prerelease or installed-helper update had been performed.

## Validation after synchronizing upstream

After updating the base to `8f4c0d40`, the complete engine and store race
packages passed (162.092s and 161.452s). The selected CLI race tests passed.
The complete lifecycle E2E package and legacy migration entry scenario passed
in the isolated Docker harness (135.797s and 7.980s).

The upstream Codex runtime environment test saw the host sandbox inject its
proxy into child processes, so its proxy-value assertions failed locally. The
complete Codex package passed without race instrumentation in the isolated
container; all remaining Codex tests passed with race instrumentation on the
host. The container has no C compiler, so it cannot run Go race binaries. Native
platform and complete race coverage remain part of the release CI gates.

## Candidate CI corrections

The v0.1.23-rc.5 CI found three test integration errors: the new lifecycle
scenario's behavior catalog mapping was inaccurate, one migration assertion
still expected schema 7, and the `--interval` scenario counted work-chat and
other GETs instead of control-chat reads. The catalog mapping and schema
expectation were corrected, and the interval assertion now measures the control
chat without relaxing its bounds. The catalog test passed locally, and the
affected entry and lifecycle scenarios passed in the isolated Docker harness
(44.928s and 29.302s). A new commit and candidate tag carry these corrections;
the original candidate tag is unchanged.
