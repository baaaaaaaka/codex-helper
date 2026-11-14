# Teams read credential fallback

The helper can borrow its full Graph credential when the separate read
credential receives an HTTP 429 with unknown scope. A successful alternate
request remains a normal successful channel read; the core account gate closes
only when the adapter cannot complete the read.

## Boundaries

- Enabled by the helper channel constructor only when separate client IDs are
  configured. Generic read/search/fetch clients do not enable it.
- Covers owner lookup, receive/activity scan, delivery read-back, history pages,
  and individual chat-message reads. Every continuation page uses the same policy.
- Actual access-token claims must show the same tenant and user and different
  applications. This is a restriction on trusted OAuth token sources, not a
  substitute for token authentication. Opaque/unreadable tokens disable borrowing.
  Every full token, including a refreshed token, is checked before it is sent.
- Explicit shared/chat throttles, HTTP 503, transport errors, and authorization
  failures do not initiate switching. Writes and file transfers never switch.

## Routing and recovery

Normal reads remain concurrent and use the primary credential. An unknown-scope
429 records its Retry-After deadline and enters a single cancellable degraded
lane. That lane serializes alternate requests and recovery probes. Every physical
borrowed HTTP request, including a 401 retry, is paced to at most one start per
second. Writes bypass the lane; observed account/unknown write throttles suspend
borrowing from full for their cooldown.

When the primary cooldown ends, one real read probes recovery. A successful probe
returns normal routing. Initial credential identity proof and recovery probes are bounded to five seconds (or one quarter of
an explicitly shorter request timeout), so it leaves time for full to complete
the read. A transient transport/plain-5xx recovery failure keeps the working full
route and delays the next primary probe for five seconds. It does not initiate
fallback on an otherwise normal connection. Earlier in-flight responses cannot clear a newer primary
cooldown. There are no probe workers, timer loops, or extra health-check requests.
Lane waits and credential checks have the configured request timeout and honor
caller cancellation. A zero or past Retry-After has a one-second minimum.

If full also fails, it is cooled for its Retry-After, or 30 seconds for a failure
without a throttle deadline. The adapter returns an account read throttle based
on the earliest usable credential; existing core pacing, jitter and pause rules
still apply. A failure that finishes after a deadline is still an error, never
an empty successful response. Both credentials cooling generates no HTTP calls.

Credential selection is fixed across each request's 401 refresh. A failed page
is retried with identical parameters, not by restarting pagination. The existing
whole-window admission and cursor rules remain in force: a failed receive
returns no partial window.

All routing state is in memory. No token cache format, core schema, or persistence
cadence changes. Existing persisted core gates are not bypassed on restart.
Transition logs contain no tokens or message bodies. `ReadFallbackStats` exposes
primary throttle, physical alternate request, alternate success/failure, and
recovery counts without logging normal reads.

## Validation

Initial validation base: upstream `57d0dd02`. Before submission the feature was
rebased without conflicts onto upstream `f93a1b55` (an independent shared-TUI
approval change), on branch `feat/teams-read-fallback`. Targeted adapter/engine
race tests and the related CLI tests are rerun after that rebase. Tests run in the
isolated worktree `/home/baka/.local/state/cxp-read-fallback-worktree`, with
`GOCACHE` and `GOTMPDIR` under the validated session scratch directory
`/home/baka/.local/state/cxp-fallback-test.maAv2q`. No source-built helper process
is run on the host and no live Teams traffic is used by these tests.

Regression tests cover unknown versus explicit throttles; no fallback for generic
reads, writes, preauthenticated URLs, or 503/403; credential mismatch; source
pinning and changed identity after refresh; physical retry pacing; cancellation;
write isolation and write-aware cooldown; one recovery probe; late primary
responses; zero-delay/concurrent failures; both-client cooldown with no HTTP
storm; earliest recovery; activity scans; exact-page retry; and atomic receive
failure. The 4,000-message / 80-page fixture verifies complete ordered delivery
while the primary recovers between pages. These tests fail if the alternate is
never selected, cooldowns permit extra requests, or a partial receive is admitted.

The engine integration test combines the real adapter HTTP path, core scheduler,
SQLite store and fake agent, using an in-memory chat fixture for the surrounding
lifecycle. It checks prompt admission with working full credentials, and a
closed account gate with no further HTTP requests when both credentials fail.

Final validation uses the repository's pinned Go 1.25.11 on Linux/amd64.
The default host toolchain Go 1.26.3 also passed package-level validation during
review. Prefix commands below with these session-specific settings:

```sh
export GOTOOLCHAIN=go1.25.11
export GOCACHE=/home/baka/.local/state/cxp-fallback-test.maAv2q/go-cache
export GOTMPDIR=/home/baka/.local/state/cxp-fallback-test.maAv2q
```

Final entry points: all passed on Go 1.25.11. The full Teams race suite also
passed after adding the final concurrent-cooldown and pacing-wait regressions.

```sh
go test -race ./internal/channel/teams ./internal/core/engine -count=1 -timeout=15m
go test ./internal/cli -run 'Test(Teams(Read|Search|Fetch|NewCoreReadFallback)|NewCoreChannel)' -count=1 -timeout=3m
GOOS=windows GOARCH=amd64 go vet ./internal/channel/teams ./internal/core/engine ./internal/cli
GOOS=darwin GOARCH=arm64 go build ./internal/channel/teams ./internal/core/engine ./internal/cli
git diff --check
```

During review, a controlled transient-recovery regression failed before the fix:
a working full route became unusable when the primary recovery request returned
HTTP 500. After the fix, that test and a separate bounded-timeout test pass.
The concurrent recovery test also caught and fixed repeated recovery counting.
No existing test expectation was weakened to obtain a pass. Additional
controlled tests verify that a late shorter throttle cannot shorten a longer
cooldown and that cancellation or a write throttle stops a paced request
before any alternate HTTP request is sent.

Performance/I/O audit: normal routing adds one short in-memory lock and no token
inspection, persistence write, or extra HTTP request. State is fixed-size per
adapter. A mock-HTTP hot-path benchmark on Go 1.26.3 measured 32 allocations and
3,568 bytes per request both with and without fallback enabled; one run measured
2.405 versus 2.919 microseconds. These microbenchmarks do not measure real Graph
latency. Fallback adds at most one alternate credential path for a failed primary
request, and physical full requests are bounded to one per second. Existing
whole-window page/size limits and pooled response buffers remain unchanged.

The repository's existing CI selects all channel and engine tests in
`race_core_packages.sh` (`store` and `engine` groups), and CLI tests in the Linux
unit job. No new test-name selector is needed. The new tests require no external
services or credentials.

Not verified locally: packaged helper black-box E2E and native Windows/macOS execution.
There is no usable local Docker connection: both `/var/run/docker.sock` and
`/run/docker.sock` are absent, and the configured `desktop-linux` named-pipe
context reports `protocol not available` from this Linux client. Repository policy requires real helper process
tests to run in the isolated Docker harness, so those tests were not moved onto
the host. This is an environment limitation, not a sandbox denial or a test
failure. A full deployed fallback soak and its effect on shared service-side
write quotas also remain unverified; local serialization cannot reserve Graph's
upstream capacity.
