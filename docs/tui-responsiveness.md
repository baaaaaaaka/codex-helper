# TUI responsiveness

The selector keeps project/session list snapshots on its event goroutine. Project
loads advance a revision, filters and the default directory are cache keys, and
subagent expansion advances a separate revision. Keyboard actions and drawing
use the same snapshots. Automatic refresh restores the identity selected when
its result is applied; explicit refresh retains the previous reset semantics.
Loaders return owned snapshots and must not mutate them after returning.

In `SelectSession`, preview reads, wrapping/search, and refresh each have one
worker. Each worker holds at most one running job, one pending job, and one
completed result. A replacement cancels the previous context and replaces pending
work. Results carry a generation; layout additionally checks the complete
content/metadata/width key and search term before applying. Workers do not access
`uiState` or draw. Full event queues retry the wakeup with cancellation support.
Automatic refresh ticks coalesce; an explicit refresh supersedes a pending one.
The event loop also checks cancellation directly, so a full notification queue
cannot strand it after the last queued input event.

The preview reader currently has no context parameter. An active read can finish
after cancellation, but its result is rejected and its worker starts no further
reads after exit. This bounds concurrency, not the duration of slow filesystem or
SQLite operations. Cancellation does not roll back already started cache writes.
Wrapping checks cancellation between lines and during long lines; search checks
between wrapped lines.

Complete preview contents, Unicode wrapping and case-insensitive search remain
available. A new selection/layout can show `Preparing preview...` while its
layout is pending; it never displays another session's body. Lists, selection
and quit remain usable. Preview scroll is retained while the placeholder is
shown and clamped only when the complete layout is ready. Preview navigation
waits for that layout; search navigation becomes available after matching ends.

Recent raw previews are limited to six entries and 24 MiB. One selected preview
larger than the budget is retained alone to preserve full-history behavior.
Wrapped history uses the existing six-entry/24 MiB cache; an oversized current
layout is usable but not admitted to that cache. These are retention budgets,
not total RSS caps: running/pending work, search results, current oversized
layouts, the history package's caches, and temporary allocations also contribute
to peak memory. No persisted cache schema or flush policy changes are included.

## Validation

Focused regression tests live in `internal/tui/async_test.go`, alongside existing
selection/preview/refresh tests. They cover list invalidation, retained selection,
A/B/A results with unchanged file metadata, cancelled pending reads, coalescing,
slow refresh navigation/quit, full notification queues, worker shutdown,
Unicode/layout equivalence, footer equivalence, resize/content/search changes,
retained scroll, and cache accounting/eviction. CI's existing Linux race job also
runs the TUI package. Release runtime preflight also selects the new worker/state
tests on Linux, macOS and Windows. Local cross-compilation checks compile only;
they are not real terminal tests.

Useful commands (use validated scratch directories for `TMPDIR` and `GOCACHE`):

```sh
go test ./internal/tui ./internal/codexhistory
go test -race ./internal/tui -count=20
go test ./internal/tui -run '^$' -bench 'Benchmark(ListViewsNavigation|TUIKeyAndFrame)$' -benchmem
```

Run repository-wide checks with helper/runtime environment markers removed and
an isolated HOME/XDG state. Keep temporary Go source/build files outside the
repository: the repository-wide formatting test recursively inspects it. On WSL,
Windows PowerShell availability does not imply the interop socket is usable.
Real lifecycle tests require the development skill's isolated Docker harness.

An isolated 160x50 simulation replay at 20 keys/second used the previously
measured history shape (879 top-level/orphan sessions plus 7,086 attached
subagents). On WSL2 with 32 logical CPUs and Go 1.26.3, ordinary-key P95 was
1.39 ms versus 11.63 ms before this change; an 8.61 MB preview switching load was
2.09 ms versus 391.79 ms before. All 120 injected keys completed without queue
rejection. After switching stopped, full preview display settled in about 70 ms.
These measure enqueue-to-next-Show, not terminal transport or instant full-body
availability. Real cached visible records were re-encoded as a private fixture;
the roughly 8 GB original source was not fully cold-read. Original snapshots and
conversation content are not part of the repository. The public benchmarks use
synthetic data and do not require private history.

With GOMAXPROCS limited to 2, the same long-preview replay measured P95 1.50 ms,
with full display settling 72 ms after switching stopped. This limits the Go
scheduler; it is not a benchmark on different physical hardware. A matching
private-shape direct key/frame microbenchmark measured 0.97 ms per key and
0.239 MB allocated per key, compared with 10.42 ms and 7.97 MB before. Microbenchmark
samples contain two keys; their means and allocation totals are divided by two,
while replay percentiles are measured on individual keys.
