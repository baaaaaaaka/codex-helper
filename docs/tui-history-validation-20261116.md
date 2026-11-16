# TUI history startup and cache identity validation

## Change and boundaries

Worktree: `investigate/tui-history-20261116`, based on upstream `9a533187`.
The original measurements used `0d0fdb48`; the two upstream commits merged
afterward only update the Codex compatibility table. The affected race and CLI
suites were rerun after the fast-forward.

The incident evidence is a native-device change from 2128 in older cache entries
to 2096 on the current WSL/ext4 mount. For 155 existing sampled files, inode,
size, mode, mtime and ctime were unchanged. Changing only the device field in a
private catalog copy reproduced a cache miss and full transcript parsing.

Linux/ext4 file cache keys use the filesystem UUID-derived `statfs.f_fsid`
instead of the mount's device number. The existing SQLite TEXT field stores a
kind tag (`ext4-fsid:`); native device/volume IDs keep the legacy decimal form.
Both exact and append checks compare the identity kind. Unsupported filesystems,
zero IDs and statfs errors keep the conservative native-device behavior. Windows
and macOS identity collection is unchanged. Legacy rows are not guessed to be
new-style identities, and the cache schema/version/directory is not reset.

TUI directory discovery shares the existing enumeration, enrichment, grouping,
subagent association and sorting code, but uses a bounded summary reader. It
first reuses valid full metadata from the existing persistent catalog. A miss
stops after the first usable user prompt or 128 KiB per transcript. Summaries use
file modification time for recency and the existing title indexes for enrichment.
A count of -1 means unknown; it is hidden in the preview header and is not treated
as an empty session. An unavailable prompt falls back to thread name/session ID.
The existing in-process per-file cache holds summaries separately from full
metadata. Partial summaries never enter the persistent full-metadata catalog.
Full history/list/show/statistics callers retain the full reader. No new database,
background rebuild queue, native Codex index dependency, or snapshot state exists.

Ctrl+N can create a session in the default directory while initial discovery is
pending. Enter still waits for a known selection; existing cancellation and
selection-preserving refresh behavior remain in place.

A same-size timestamp change now rejects append reuse: head/tail fingerprints
cannot detect a middle edit. This also corrects an existing stale-preview case.
Consequently a timestamp-only change conservatively reparses content and may
commit a refreshed preview. Its small-fixture budget is one commit / four pages;
unchanged reads, steady-state polling, close and reopen retain zero-write checks.

## Functional and concurrency validation

The focused `internal/codexhistory` and `internal/tui` package suites pass with
`-count=1`. They cover the following new behavior as well as existing cache,
preview, prompt filtering, fork/subagent, sorting and TUI behavior:

- Ext4 identity survives simulated native-device renumbering; different filesystem
  IDs and legacy identity kinds do not match; unsupported/error/zero fallback is
  conservative. Catalog and preview persistence round trips preserve the tag.
- Bounded reads include giant individual JSON records, missing prompts and valid
  final records without a newline. Unknown counts stay visible, and summaries
  cannot poison exact metadata. Appends invalidate the in-memory summary.
- Cold discovery works with an unavailable catalog path and with a catalog held
  under BEGIN EXCLUSIVE. The latter uses 32 sessions and rejects any attempted
  full metadata window, so a per-file source fallback cannot pass unnoticed.
- Existing prompt injection filtering and helper-session visibility still apply,
  including an older control prompt after 70 KiB of injected context.
  Directory reads with no history index create no persistent catalog and perform
  no metadata commits, including repeated process-memory-cold passes.
- Same-size middle rewrites with unchanged bounded fingerprints invalidate reuse;
  the public preview API returns the changed content.
- A controlled blocked initial loader cannot prevent Ctrl+N, and exiting the
  selection cancels that loader. Enter/loading and refresh selection tests remain.

The focused CLI history/TUI suite passes, including actual cold-directory loader
wiring with a large transcript, normal list output/thread names/forks, context
cancellation, proxy/AAA persistence, and the existing open/new/fork orchestration
mocks. No real helper lifecycle process is started by these focused tests.

## Performance and I/O evidence

Read-only measurement against this machine's approximately 7,997 JSONL files /
76.7 GB, with a private copied catalog for the history-index dependency:

- Invalid persistent identity and empty in-process session cache: 5.210 s.
- Second discovery in the same process: 44.804 ms; no additional session opens.
- Valid persistent catalog with empty process cache: 301.704 ms, with five changed
  or uncached source files opened for bounded summaries. This fixture rewrites
  identities only in a private copied database to model an already refreshed
  catalog. Production does not reinterpret legacy identity rows.
- A separate visible-directory comparison took 5.151 s. After the existing user
  visibility filter, both paths returned 169 projects, 863 top-level sessions and
  7,949 graph nodes. No graph member, parent relationship or project assignment
  changed. The reference uses previously cached full metadata, not a new 76 GB scan.
- 260 visible-node prompt/title fields differed from the full catalog; bounded
  discovery does not promise complete prompts. Before visibility filtering there
  were 13 top-level entries without either a prompt or thread name; their IDs
  remain available. All 69 project-less promoted orphans also exist in the reference.
- `BenchmarkSessionSummaryLargeTranscript`, 100 iterations: 7,937 ns/op,
  6,984 B/op and 63 allocs/op for a transcript with a 64 MiB tail. This is an
  in-memory parsing benchmark, not a disk-cold startup measurement.
- Full metadata-window reads were forbidden during the live summary measurement.
  Source sessions were never rewritten. OS page caches were not cleared; these
  numbers are not a guarantee for cold disks or remote filesystems.
- `scripts/ci/session_preview_sqlite_io_smoke.sh` passes the syscall trace checks:
  exact/steady-state reads have zero cache writes; session append 16,924 bytes,
  history append 25,124 bytes, preview threshold append 98,860 rollback bytes.

The opt-in local diagnostics and earlier investigation records remain in the
worktree's untracked `.diagnostics/` directory, outside production sources.

## Validation entry points

The following local commands passed (Go test-result caching disabled). CLI tests
use orchestration mocks; they do not launch a real helper lifecycle:

```sh
go test ./internal/codexhistory ./internal/tui -count=1
go test -race ./internal/codexhistory ./internal/tui -count=1 -timeout=5m
go test ./internal/cli -run '^(TestLoadHistoryTuiProjects|TestFilterHistoryTui|TestHistoryTui|TestRunHistoryTui|TestHistoryListCmd)' -count=1
go vet ./internal/codexhistory ./internal/tui ./internal/cli
go test . -run '^Test(GofmtClean|CIWorkflow|NightlyWorkflow|CITitle)' -count=1
python3 scripts/tests/test_ci_targeted_shards.py
TMPDIR=<worktree>/.diagnostics bash scripts/ci/session_preview_sqlite_io_smoke.sh
go test ./internal/codexhistory -run '^$' -bench '^BenchmarkSessionSummaryLargeTranscript$' -benchtime=100x -count=1 -benchmem
git diff --check
```

The new release selectors were also executed explicitly. Controlled temporary
mutations proved that the identity regression fails without stable ext4 identity,
the CLI cold-load regression fails with the old full-reader wiring, and both
middle-rewrite tests fail without the same-size append guard. All mutations were
restored. The timestamp-only write-budget expectation changes because a possible
in-place edit must be reparsed; unchanged input still produces zero cache writes.

With `CGO_ENABLED=0`, codexhistory test binaries cross-compile for Windows/amd64,
macOS/arm64 and Linux/arm64; the Linux CLI also builds. These are compilation
checks, not native execution or packaged-runtime tests.

## CI reachability and limits

The Linux CI full Go suite already discovers the new tests, and its existing TUI
race job covers the new controlled-ordering test. Release's cross-platform TUI
step now explicitly runs the new identity, summary, CLI wiring and Ctrl+N tests,
and the bounded-summary benchmark. The existing SQLite release selectors include
the same-size rewrite and mutation-budget tests.

Docker process/E2E tests are unavailable on this machine: `/var/run/docker.sock`
and `/run/docker.sock` do not exist, and `docker info` cannot connect. This is a
missing local test environment, not a sandbox permission, code, dependency or
network failure. No daemon/helper was started or restarted to bypass it. Native
Windows/macOS execution and an actual WSL reboot have not been performed.
Cross-compilation and deterministic device-renumbering tests have narrower scope.
No release was published, no installed runtime was replaced, and the active Teams
helper was not restarted, replaced or signaled.

## Follow-up: scheduled-wait CI failure

Release candidate `v0.1.23-rc.35` exposed a failure in
`TestScheduledNativeWaitSuppressesHintsAndClearsOnCompletion` under the full
nightly race partition. The log showed a native-wait notice followed by an
ordinary silence notice. The original test also assumed no silence notice could
legitimately precede the wait, although a slow agent can cross its 100 ms test
threshold before emitting wait evidence.

The silence watcher could choose a notice from a snapshot, then commit it after
the wait state changed. Wait transitions and the final state check plus notice
commit now share a per-turn mutex. The global engine mutex is released before
the SQLite write; other turns do not share the new mutex. Both ordinary and
expired-wait hints recheck the snapshot. Existing polling, notice keys, database
writes and schemas are unchanged. A slow SQLite write can delay the wait callback
for the same turn; callbacks already wrote their initial wait notice to SQLite.

Regression coverage forces an old decision to be used after a wait transition,
and holds a real store transaction while a notice and a wait compete, verifying
the commit order. The integration-style test measures hints after the wait notice
and continues to check ordinary behavior in a second conversation. Removing the
new state guard caused the stale-decision test to fail as intended. The focused
scheduled/silence race suite, `go test -race ./internal/core/engine -count=1
-parallel=8 -timeout=15m` and `go vet ./internal/core/engine` passed; Linux CI and
nightly full race partitions discover the new tests automatically. Engine test
binaries cross-compiled for Windows/amd64 and macOS/arm64. GitHub CI provides the
process isolation unavailable on this host, where Docker is absent.
