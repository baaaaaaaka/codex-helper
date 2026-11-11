# Teams message edits and startup validation

## Behavior

An owner request receives its User label and initial Helper receipt in one
message read and one PATCH. The adapter keeps the original HTML, attachment
JSON and mentions. Hosted screenshots are editable; unknown cards still select
the existing separate-message fallback. Voice transcripts retain their own edit.

Helper is ordinary text. The Received at timestamp is the receipt link. It uses
the admission transaction time, local offset and seconds, persisted with the
existing rendered outbox body. Retry and restart do not generate another time.
A pause uses its persisted ParkedAt event, including on the separate-post path.
Transcript links identify the message and clip content. Quoted/code examples and
ordinary self-links do not acknowledge an annotation. The rc.119 linked Helper
header remains readable only with its matching receipt body.

References have no separate count limit. Existing media/file limits, paced
reads, preview fallback and long-reference files remain. References are not
recursively expanded.

A desktop gateway may listen while its backend recovers. TUI startup additionally
waits up to one minute for the registered instance to report healthy. It does not
start a competing gateway or retry whole Codex launches. Cancellation and failure
include the backend cause, port and log location.

Windows protocol probes report the physical entry and bounded child output.
An entry too old for the bootstrap ABI must use the explicit installer path;
ordinary upgrades never bypass protocol admission. Installer maintenance stops
the watchdog before the bridge and checks both executable destinations before
changing either. A task denial or a surviving target process stops installation.
This does not repair existing Scheduled Task ACLs or make two executable
replacements one transaction.

## Validation

Candidate worktree: fix/teams-edits-and-startup-20261111, based on main 28c65daf.
The upstream shared CI gates and race partitioning are retained. No live helper
was restarted or replaced. Process tests use offline Docker with isolated home,
state, ports and fake Graph/model services; unit tests also run locally.

- Adapter/engine unit and race tests cover copied receipts, applied-but-lost
  PATCH, normalized HTML, restart, current-message links, screenshots, mentions,
  incomplete labels, Graph wrapper elements, code examples, different pause
  events, symbol-only transcripts and stable time. Transcript identity hashes
  exact clip content rather than text normalized for fuzzy matching.
  The adapter checks the actual single message GET and PATCH, not only a mock
  method invocation. Pre-fix HTML/screenshot/self-link regressions failed.
- All three scripts/ci/run_e2e_shard.sh entry points passed (177 scenario
  reports). Reviewed goldens reflect the merged edit, screenshots, timestamp
  links and reference policy. TestQuotedMessages also combines repeated long
  references, a failed read, a forward and distinct full-file paths.
- scripts/ci/run_shared_codex_contracts.sh uses native Codex 0.166.0/0.167.1 and PTY.
  New chat covers first/second turns, approvals, two windows and resume.
  GatewayRecoveryTUI controls recovery, checks no app-server starts beforehand,
  then verifies the native input interface and first response. The gate rejects
  skipped or missing acceptance tests. Both versions passed the isolated gateway
  acceptance test locally, including normal TUI and broker shutdown.
- Restoring listener-only readiness deliberately made TestTUIGatewayWaitsForBackend
  fail. Its handshake requires a second failed probe before recovery is released.
- Owner-only Graph PATCH/GET preserved receipt, pause and transcript links
  (including event fragments), while keeping Helper plain. A prior image probe
  preserved bytes/order despite URL normalization. Client visual inspection is
  separate from these API/HTML checks.
- Native Windows CI owns non-terminating PowerShell permission errors, an actual
  executable lock, detached watchdog cleanup and interactive-session preservation.
  The boot gate adds a checksummed published rc.83 physical entry plus a frozen
  rc.96 runtime, and tests explicit bootstrap recovery with configuration intact.
  Linux cross-compilation is not native Windows execution evidence.
- The full Linux coverage entry passed except the existing chmod-000 test under
  root. That package and the E2E import boundary passed in an ordinary-user
  container. Native CI is required before merge; first-run failures found a
  PowerShell interpolation error, escaped Windows diagnostic paths and abrupt
  test shutdown. These were corrected rather than skipped.

Why old CI missed these defects: screenshot refusal and two edits were encoded
as expected behavior; HTML fixtures used one serialization; self-links/text were
treated as identity; gateway recovery was tested separately from TUI startup;
Windows probes began with a newer entry and script checks did not observe actual
process/lock outcomes. Tests now assert the user result across those boundaries.

## Performance

No schema, queue, history scan or background worker was added. The event time is
stored in the existing outbox transaction/body. An unchanged receipt causes no
PATCH or persistence write. Gateway polling does one read-only registration
load, then bounded HTTP probes at the existing interval, without database writes.

For 1,000 HTML paragraphs (about 47 KB), BenchmarkConfirmationRetryLarge measured
135457 ns/op, 5440 B/op, 20 allocations on rc.119 and 176811 ns/op, 6105 B/op,
45 allocations on this candidate, on the same local CPU/cache. This legacy-retry
path spends about 0.04 ms more validating block identity. Normal request delivery
removes a full Graph read/PATCH pair. The streaming parser does not build a DOM
or reserialize the original body. E2E keeps its existing CPU and idle-I/O budgets.
