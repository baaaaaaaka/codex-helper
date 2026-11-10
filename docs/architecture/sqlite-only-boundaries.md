# SQLite-only storage boundaries

Storage and cutover contract for the v2 migration. Release verification must
include the native OS, real NFS and packaged-artifact gates below. Production
deployment is a separate operator action.

## Ownership and representation

| Owner / lifetime | Native database | Record boundary |
| --- | --- | --- |
| Core lease generation | existing core SQLite | 14 former engine meta objects; attachment, quote and source fields; pending notifications |
| Delegation worker / CLI operation | delegation/state.sqlite | seven concrete parent tables and ordered TaskSpec / path children |
| User configuration | config.sqlite | singleton header, profiles / SSH arguments, individual instances / recovery fields |
| Teams credentials, per profile | teams-auth.sqlite / teams-tokens.sqlite | explicit auth profile; individual full/read/chat-write credential rows |
| Skills manager | skills.sqlite | subscriptions, installed sources/files, review and report rows |
| Exported skill directory | .cxp-skill-manifest.sqlite | immutable manifest published with its directory |
| Machine registry | teams-machine-registry.sqlite | individual registry records and import provenance |
| App gateway | gateway.sqlite | registrations / ordered ports; current and previous event windows |
| Core service restart policy | core-scoped service-state.sqlite | restart streak and deadline per host |
| Global service runtime | teams/service/service-state.sqlite | watchdog, supervisor / process identities, activation per host |
| Installed supervisor configuration | local-supervisor.sqlite | singleton config with environment children |
| Managed helper installation | install.sqlite | install record and ordered shims |
| Helper update directory | update-state.sqlite | cooldown, pending update, immutable invocation inputs, activation owners/results |
| Managed Windows ChatGPT directory | current.sqlite | installed package and executable identity |
| ASR process cache | run/servers.sqlite | host / PID plus exact child and owner identities |
| ASR verified installation directory | .asr-install.sqlite | immutable header and ordered files |
| Math font directory | font.sqlite | immutable font identity and checksum |
| Codex pool, per core / host | existing PID path + .sqlite | individual app-server identity keyed by PID; index for affected conversation keys |

Peripheral mutable stores use one reusable connection, rollback journals and
FULL synchronization. Core and delegation retain their separate lease/WAL
policies. Rollback journals do not establish that an arbitrary network
filesystem implements reliable SQLite locking. Native Windows, WSL shared paths
and a real supported NFS target remain acceptance gates.

Small immutable installation manifests are separate databases because their
permission and atomic directory-publication lifetime differs from mutable
runtime state. They are closed before the existing directory rename. They are
not separate pools for each business object.

The unused `engine_usage:<conversation>` accumulator has been removed, along
with its legacy keys. User statistics continue to come from Codex rollouts;
there was no reader of that duplicate accumulator. Historical `notifications`
remain an archive; the live queue uses `pending_notifications` and never
replays archived records as new notices.

## Admission and lost databases

A mutable database receives a fixed, immutable `.published` seal after its
schema/data transaction commits and before a legacy source is archived. The
seal contains only `CXP-SQLITE-1` and a newline. It does not duplicate schema,
state, generation, queue contents or execution progress.

This is a minimum publication record outside the protected business database:
without independent evidence, a missing database cannot be distinguished from
a new installation. Opening a sealed but absent, empty or schema-zero database
fails; the caller must not classify this error as an ordinary missing file.
A valid seal is never rewritten during polling or same-value saves. Failed
migration before commit leaves no seal and can be retried by the migration
entry. Corrupt native state never falls back to legacy data.

Core uses the existing published lease generation as that independent evidence.
It checks the predecessor schema before initialization; missing, empty and
schema-zero predecessors all stop startup without a replacement database.
Read-only configuration commands do not migrate or silently return defaults
when a legacy configuration exists. A normal writable configuration operation
must complete the one-time migration first.

Installation manifests use their containing directory's existing publication
boundary instead. Their contents must be verified before the artifact is used;
regenerating an invalid verified cache must not reset a business queue or a
process ownership record.

## Coordination and bootstrap

The lease remains outside the database it protects. Its fixed slots now use
CXL2 explicit binary fields and the same CRC, owner and takeover algorithm.
Only the migration decoder reads old CXL1 JSON. Status, poll, TUI endpoint and
Windows install-lock owner are small immutable native SQLite snapshots.
Publication retains the existing throttling and same-value suppression.

Snapshot readers copy at most 64 KiB through a file handle that shares delete
access, close that handle, then query the native database in memory with
query_only enabled. They never open another host's live WAL. This avoids
Windows SQLite handles blocking atomic replacement; it does not change the
locking policy of mutable databases. Snapshot size caps are admission limits,
not an invitation to serialize business collections into one field.

CLI requests, ordered results and Workflow handoffs share cli-exchange.sqlite
with rollback journaling. Core tables retain execution evidence and deduplication.
The helper reuses its mailbox connection. Completion and request removal share
one transaction; Workflow acknowledgement compares the revision. Keyset pages
prevent an inflight prefix from hiding later requests.

Candidate updater input is an immutable row in update-state.sqlite. Protocol 2
passes database path and request ID; stdout JSON is still the wire result.
A repeated ID cannot replace input. The caller deletes only that input row on
exit. New invocations reap at most 128 inputs older than seven days in their
insertion transaction; they never replay execution from an expired input.
Boot state and source-version fencing remain authoritative.

Boot protocol 2 and config reader floor 7 require explicit installer cutover
from older formats. The verified physical executable admits itself using
--cxp-install-bootstrap before normal CLI initialization. The one-time importer
archives protocol-one boot evidence and drops incompatible rollback/trial targets.
The native boot.sqlite snapshot is the sole selection authority. Updates probe
both candidate and physical bootstrap before creating update state. Legacy
pending JSON cannot activate a runtime or recover a lost native database.
Interrupted sealing or archival can be retried through the explicit installer.

Historical downloaded-candidate version checks are refused unless the caller
supplies the protocol-two probe marker. Old updaters also set runtime-disable,
so that variable is not an admission bypass. The marker is ephemeral, carries
no credentials, and is removed from user subprocesses. Obsolete automatic
legacy preflight, post-parent repair and stable-entry replacement paths have
been removed; they are not alternative runtime storage backends.

Cutover requires old writers to have exited or be isolated. This implementation
does not claim that incrementing user_version can constrain a historical writer
that ignores it. The release artifact gate exercises actual old binaries and
checks refusal before replacement, then explicit bootstrap/config migration.
Released artifacts and native OS runs remain required evidence; changing the
version embedded in a new build only tests the new protocol's behavior.

## Explicit non-state JSON boundaries

Graph/OAuth requests and responses, delegation's published wire protocol,
--json output and external Codex formats retain their protocol encodings.
The control-chat context history is a bounded plain-text export generated from
SQLite and never read as business state. Attachment drive references use scalar
item IDs plus native attachment ID / URL columns; only the one-time migrator
parses the old nested drive JSON string. No runtime Store backend selector or
whole-Store save path remains.

## Release gates

The startup-recovery selector requires the migration, lost-database, concurrency
and storage-boundary tests by name. CI runs the ordinary suite, core and selected
CLI race tests, and three repetitions of critical migration/crash/notification
E2E. Native Windows and macOS jobs check path, lock and publication semantics;
cross compilation does not replace those runs. The existing nightly NFS smoke
also puts the native lease, snapshots and CLI mailbox on its verified mount.
Release checks actual checksum-verified old and new artifacts for updater refusal,
explicit cutover and native configuration reopen. A source build with a changed
version string is not evidence for that historical compatibility check.

## Verification and performance contract

The source plan remains the acceptance contract: one business change keeps one
transaction, related children are atomic, and unrelated records are untouched.
Reads and same-value saves must produce no SQL changes or publication writes.
Nonempty native files must have a valid SQLite header before admission; short
corrupt files must not be treated as an empty new database.
No SQL transaction spans network calls or process termination. Process cleanup
reads bounded pages and closes cursors before process I/O.

Ordinary settings and profile edits use header/profile point operations.
Instance pruning reads its candidate snapshot, ends the transaction, inspects
processes, then deletes only still-identical instances in one transaction. A
new owner or heartbeat between inspection and deletion preserves that record.

Notification event checks use indexed IDs/ranges and keyset pagination, without
loading outbox bodies. Recovery scans remain proportional to pending records;
small LIMIT values alone do not prove bounded scanning. Keep existing publisher
and poll throttling. Do not force VACUUM or checkpoint per operation.

The existing unknown-delivery resolver still performs separate Graph checks
for pending sends. A large unknown backlog therefore retains proportional
network work; this storage change does not alter its retry policy. The backlog
E2E checks the new answer/card and preserved unknowns, and budgets those existing
resolver reads separately from notification-storage measurements.

Local Linux measurements use synthetic 1 / 1,000 / 100,000-record fixtures,
warm filesystem cache, and the synchronization policy of each owner. The
nightly scripts repeat the workloads and collect CPU, allocations, process
high-water RSS, system-call writes and sampled peak directory size.

| Workload | Observed cost / regression boundary |
| --- | --- |
| 100 config heartbeats, at each history size | 3,332,400 bytes in pwrite calls, 300 sync calls, 100 transactions |
| 100 config settings edits, at each history size | 1,692,400 bytes in pwrite calls, 300 sync calls, 100 transactions |
| Read-only and same-value config operations | zero mutation system calls |
| 100 complete core turn/send/card storage chains, with 100,000 unknown notices | about 216 ms wall / 242 ms CPU; 4.99 MB cumulative allocation; about 16.3 MB physical writes |
| 100 complete delegation storage chains, with 100,000 old tasks | about 419 ms wall / 265 ms CPU; 1.89 MB process read characters; about 23 MB physical writes |
| 20 fresh native `--version` / config-list processes | about 121 / 137 ms total wall; no state rewrites or output disk blocks |
| Reader retains a core WAL snapshot across 50 writes | 206,032 bytes / 50 frames; after reader close and explicit test checkpoint, zero bytes / frames |

These are observations, not fixed latency promises. Core chain measurements
exercise storage, not Graph latency; the backlog E2E separately checks delivery.
The cold-process child high-water RSS was about 16 MiB and includes setup in the
kernel's cumulative child accounting. Cumulative allocations are not RSS, and
warm-cache read_bytes=0 does not mean the process performed no reads.

The delegation retention query uses a strict index lower bound above an empty
timestamp. Its regression test checks the actual seek, because an equivalent
filter with LIMIT previously scanned the entire 100,000-row empty prefix on
every append. No retention policy or deduplication evidence was removed.

## Cutover and recovery

Stop or isolate every old writer that can reach the affected profile or shared
scope before cutover. A schema number cannot fence an old executable that
ignores it. Do not run old and new helpers concurrently against that state.
Watchdog migration ownership remains part of the existing lease contract.

Keep a consistent backup before migration. With every writer stopped and all
handles closed, back up the complete state and matching runtime/configuration.
For a live SQLite database use its supported backup API; copying only the main
file can omit committed WAL data. The installer admits the verified physical
bootstrap explicitly before the new CLI may initialize application state.
Production installation and service changes are a separate operator action.

Downgrade is not an automatic recovery path. After native migration, restoring
an old executable alone is unsupported. Stop all new writers and restore the
matching pre-migration backup and executable together. Archived JSON is evidence
for the one-time importer, not a second live authority. A missing or corrupt
published database fails closed and requires explicit recovery.

Reserve space for the original database, new pages and migration journal plus
any separate backup. No automatic full VACUUM follows migration. In the local
100,000-record synthetic fixtures, sampled directory peaks were approximately
391 MB for the core upgrade and 493 MB for delegation, excluding an operator
backup. Sampling is a lower bound, not a capacity ceiling. The core's old
108 MB single JSON value still requires materializing that legacy value before
streaming members; its measured process high-water RSS was about 352 MiB.
Delegation's row-wise conversion used about 22 MiB. Do not apply the latter
memory figure to the core upgrade or claim that migration memory is constant.
