# Beacon Mode Plan

Beacon mode lets a stable coordinator, normally the Teams service on a login or jump host, dispatch Codex work to scheduler-backed worker machines. The coordinator owns user interaction, Teams delivery, profile selection, recovery, upgrade decisions, and cleanup. Workers only execute fenced jobs and write results to a shared queue.

This document records the current compatibility constraints with the existing Teams bridge, outbox, recovery, and upgrade code.

## Goals

- `new` without an explicit beacon profile runs locally on the machine where the current `cxp` or Teams service is running.
- Users can explicitly choose an execution profile such as CPU, GPU, Slurm, or LSF.
- Users can mark a job as shared or exclusive. Shared is the default.
- Users can list leased machines, see which conversations/jobs are running on each machine, release a machine, and kill or quarantine affected jobs.
- The same profile and lease model must be usable from Teams commands, TUI, and CLI options.
- Slurm and LSF are first-class providers, but the worker protocol must stay scheduler-neutral.
- Workers use a long-lived worker plus shared-file queue by default. SSH/dropbear is only a fallback for login semantics, not the main execution path.

## Non-Goals

- A beacon worker must not send Teams or Graph messages directly.
- A beacon worker must not own Teams control chat polling.
- A beacon worker must not mutate helper state unless the write is scoped to a fenced beacon job result.
- A beacon profile is not the same object as the existing SSH proxy profile.
- A beacon execution profile is not the same object as `CODEX_HELPER_TEAMS_PROFILE`, which scopes Teams state and control chat identity.

## Profile Model

Use separate namespaces:

- SSH proxy profile: existing `config.Profile`, used by the current proxy stack.
- Teams scope profile: `CODEX_HELPER_TEAMS_PROFILE`, used to separate Teams state/control chats.
- Beacon execution profile: new scheduler/execution profile for local, Slurm, LSF, immutable image identity, resource shape, queue or partition, exclusivity, and proxy selection.

Profile creation must ask for proxy behavior explicitly:

- no proxy
- use an existing SSH proxy profile

Provider-specific creation:

- Slurm asks for nodes, GPU count, partition, image, and duration.
- LSF asks for queue name. Other provider details are derived by the provider adapter.

The simple prompts are the default user experience, not the complete internal
schema. Provider adapters may derive or require site-specific fields such as
account, QoS, reservation, project, GPU type, CPU count, memory, walltime
limits, container runtime, mount map, modules, environment, and license tags.
The provider preview must show the derived command/resource shape before the
profile becomes ready.

Teams and TUI may create structured beacon profiles. They must not accept raw provider shell commands, secrets, or unapproved advanced fields. A created profile remains draft until:

- the user confirms it
- proxy selection resolves
- provider preview succeeds
- doctor checks pass

Draft profiles are first-class user-visible objects. Teams, TUI, and CLI must
support listing drafts, showing the exact blocking reason, resuming setup,
editing structured fields, deleting drafts, re-running doctor, and confirming
the final provider preview. A draft can never be selected implicitly by `new`
or by a profile switch.

Profile creation UX must preserve the simple prompts:

- Slurm default wizard asks only nodes, GPU count, partition, image, and duration.
- LSF default wizard asks only queue name.

Those prompts are acceptable only when the provider adapter or site policy can
derive the complete resource shape. If LSF needs project, resource string,
GPU/container options, walltime, span policy, or application profile and no
site policy can derive them, provider preview must fail with an editable draft
instead of creating a ready profile.

## User Interaction Contract

Beacon commands must be available through Teams, TUI, and CLI with the same
semantics. Teams has two command scopes:

- control chat: global beacon/profile/machine administration
- work chat: conversation-local execution target, status, and profile switch

Wrong-chat commands should not mutate state. They return a short pointer to the
right scope and, when safe, a read-only status summary.

Minimum command surface:

- `new`: local execution on the current `cxp` or Teams service machine unless
  `--beacon-profile` is explicitly provided.
- `new --beacon-profile <name> [--beacon-isolation shared|exclusive]`: create a
  conversation with an explicit beacon target.
- `beacon profile list|create|status|edit|delete|doctor|confirm`: profile
  lifecycle, including drafts.
- `beacon status`: current target, pending target, turn snapshot, proxy route,
  allocation/lease/job ids, provider state/reason, and next action.
- `beacon machine list|status|release|kill`: machine/lease operations.
- `beacon switch-profile <name> [--fork]`: change the current conversation's
  default target for future turns, or fork when the execution signature is
  incompatible.

Command context outside Teams:

- TUI work-scoped commands operate on the selected conversation.
- CLI work-scoped commands require an explicit `--session` or `--conversation`
  unless the invocation is already inside an unambiguous current conversation.
- Global profile/machine commands do not require a conversation target.

Teams mobile and retry behavior must be idempotent. Mutating commands are keyed
by Teams message id plus normalized command body and confirmation token. A
duplicate `new`, profile confirmation, switch, release, or hard kill must
return the existing operation result instead of submitting another scheduler
job or repeating a kill.

All status output must show execution target and proxy route as separate
fields. The combinations `local + no proxy`, `local + existing SSH proxy`, and
`beacon + profile-selected proxy` are all valid and must be visible in Teams,
TUI, CLI, and JSON output.

## Execution Target Selection

New conversation behavior:

- default target is local
- legacy proxy preferences do not make `new` remote
- explicit beacon profile selects beacon execution
- execution target and network/API proxy are separate user-visible fields
- explicit beacon failures never silently fall back to local execution

Existing conversation profile switch:

- target profile must be ready
- idle compatible switch applies immediately
- already queued or claimed turns keep their execution target snapshots
- a switch issued while work is running becomes the default for later turns
- incompatible resume requires an explicit fork

Every queued turn stores an execution target snapshot. Later profile switches do not mutate already queued or claimed work.

Profile-switch acknowledgement must be explicit:

- current/running turn stays on the original target
- turns queued before the switch keep their original snapshot
- a normal compatible `beacon switch-profile <name>` while work is queued or
  running schedules the pending target by default; no hidden mode is required
- turns queued after the switch use the pending target
- status shows current target, pending target, and per-turn snapshot
- incompatible switches show the exact fork command

The execution signature is a canonical structured record, not a display string.
It includes provider, queue or partition, resource shape, GPU type/count or MIG
slice, image digest, mount map, environment digest, Codex install target,
helper/worker protocol, proxy route, and isolation mode. Signature mismatches
block resume unless the user explicitly forks.

## State Layout

Beacon state must not be embedded in legacy Teams state JSON. Existing Teams code can rewrite state through structs that do not preserve unknown fields. Beacon state needs its own store, with its own schema version and locking/fencing rules.

Recommended shared layout:

```text
~/.cache/codex-helper/beacon/
  profiles.json
  scopes/<scope-id>/
    leases/
    jobs/
    workers/
    events/
    tombstones/
```

The exact base path can change, but it must be shared by the login host and worker machines.

The shared filesystem contract must be explicit:

- same logical mount visible to coordinator and worker
- atomic create or `mkdir` for claims
- atomic rename for completed immutable files
- durable close/fsync where the filesystem supports it
- no correctness dependency on cross-host `flock`
- parse-size limits for every JSON file
- clock-skew tolerance; file mtimes are advisory only
- no reuse of job paths after tombstone

Workers receive only per-job write capability. Coordinator secrets, Teams/Graph
tokens, and long-lived HMAC signing keys must not be mounted into workers or
stored in shared beacon state.

Shared-state cleanup must handle zombie files conservatively:

- ignore and later reap temp files that have no complete manifest
- reject partial JSON, oversized JSON, future write versions, and malformed
  event chains without deleting live jobs
- never reuse a job path after tombstone
- never rely on mtime alone for liveness, retention, or duplicate detection
- reserve per-job bytes and inodes before process start so ENOSPC during
  terminal/result write is less likely

If ENOSPC still happens after process start, quarantine markers or attention
outbox entries may also fail. The coordinator must surface this as a
store-needs-attention condition and avoid replay until an operator frees space
and reconciles.

## Roles

Active coordinator:

- dispatch jobs
- acquire/release leases
- reconcile workers
- clean tombstoned files
- enqueue Teams outbox
- flush Teams outbox through existing Teams bridge paths
- apply profile switches
- decide whether to reuse or allocate machines

Standby coordinator:

- read state for status
- wait for Teams control lease
- do not dispatch, clean, or enqueue Teams messages

Worker:

- heartbeat
- claim fenced jobs
- run bootstrap doctor before becoming accepting
- execute Codex
- write event chain, terminal result, and artifact metadata
- never call Graph or Teams send APIs

All coordinator mutations must carry a control lease epoch and use compare-and-
swap or equivalent fencing. Losing the Teams control lease turns an active
coordinator into a read-only standby before it can dispatch, cleanup, enqueue
Teams outbox, release/kill leases, or promote upgrades.

The coordinator writes an append-only audit log with a hash chain. It records
profile approval, BYO attach, allocation submit/cancel/kill, worker claim,
terminal accept/quarantine, artifact accept/reject, outbox enqueue/send,
upgrade blocker/force decision, and Codex target promotion. Audit payloads must
redact prompts, secrets, proxy credentials, tokens, and raw environment values.
The audit head, sequence, and last hash are checkpointed under the coordinator
CAS/control epoch so tail truncation, duplicate sequence, stale coordinator
append, and reorder can be detected during recovery.

## Allocation Lifecycle

Scheduler resource requests are separate from leases. A request can exist before
any worker process exists.

Allocation request states:

- submitted
- pending
- running
- failed
- canceled
- expired

Pending is a normal queue state, not a hung worker. It must be listable and
cancelable, it must survive coordinator restart, and it must not create duplicate
provider jobs unless policy explicitly allows parallel requests.

Allocation submit has a strict crash protocol:

1. persist allocation intent with a deterministic request id
2. submit using a deterministic Slurm job name or LSF job name keyed by that id
3. persist provider job id as soon as it is known
4. on restart, query scheduler by deterministic name/request id before resubmit

If the coordinator crashed after submit but before provider id persisted, the
next coordinator must discover or ask about the existing provider job. It must
not blindly submit another allocation. Resubmit is allowed only after a durable
negative scheduler query under a uniqueness rule. Empty, failed, eventually
consistent, or multiple scheduler query results keep the request in waiting or
needs-attention state.

Provider-native state is projected separately from internal lease state. User
status should show provider job id, queue/partition, raw provider state, pending
reason, host list when known, and the active internal action.

Provider `RUN` or Slurm allocation `R` is not the same as an accepting beacon
lease. A lease becomes accepting only after the worker process starts inside the
allocation/job step, emits a valid heartbeat with provider membership proof, and
passes bootstrap doctor.

## Worker Protocol

Use at-least-once file transport with explicit fencing:

- job id
- job attempt
- worker id
- scheduler/provider job id
- scheduler allocation id
- scheduler job step id or provider step equivalent
- scheduler run incarnation, such as provider run id, start time, host, and cgroup/allocation membership proof
- lease epoch
- claim epoch
- protocol read/write version
- execution signature
- per-job/attempt MAC or equivalent integrity check
- monotonic event sequence with previous-hash chain

The claim epoch is the fencing boundary for worker terminal writes. It must not be the current Teams coordinator epoch, because the coordinator can restart while a worker is still valid.

Terminal writes are accepted only when all fencing fields and event-chain integrity checks match. Late writes, conflicting duplicate terminal records, event gaps, MAC failures, and future write versions are quarantined. An exact byte-identical duplicate terminal envelope is idempotent: accept the already-recorded terminal once and ignore the duplicate.

The worker never receives a long-lived signing key. The coordinator creates a
per-job/attempt write capability bound to job id, attempt, worker id, lease
epoch, claim epoch, provider job id, allocation id, step id, run incarnation,
execution signature, and protocol. The coordinator keeps verification metadata
outside worker-writable shared state. A sibling worker cannot use its own
capability to write another job's terminal.

Bootstrap doctor must pass before a lease becomes accepting:

- shared root mounted and writable
- atomic create/rename probe passes
- enough free bytes and inodes for queue/result/artifact staging
- Codex/cxp binaries available
- auth and HOME behavior valid for the target image
- proxy route works when selected
- resolved image digest matches the execution snapshot
- worker protocol is compatible
- provider membership proof confirms the process is inside the allocation/job
  cgroup/environment, not a login-node or system-sshd session
- container runtime, modules, bind mounts, writable tmp, writable or expected
  HOME behavior, auth path, and proxy environment are valid inside the actual
  compute container

If doctor fails, the lease stays non-accepting and the user gets a protected,
actionable status. Explicit beacon execution must not fall back to local.

## Recovery

Current Teams `recover` interrupts queued/running Teams turns. Beacon cannot rely on normal Teams turn status as the source of truth for remote execution.

Beacon-aware recovery must reconcile beacon state first:

- queued or claimed before execution: requeue
- claim heartbeat only, before process start: requeue if the worker proves Codex did not start
- start intent without process-start ack: launch-timeout logic decides requeue only when Codex definitely did not start; otherwise ambiguous
- process started with live worker: monitor and keep the Teams projection delegated
- started with dead worker and no valid terminal: ambiguous, no replay
- valid terminal: queue protected result delivery
- corrupt or late terminal: quarantine

The coordinator must not auto-replay a job after Codex may have started. Replay requires explicit user action.

Crash-window tests must cover turn queued, job created, job claimed, start
intent, process-start ack, terminal written, result ingested, and outbox queued.

The durable launch order is:

1. claim job by atomic create/mkdir
2. fsync claim metadata
3. write and fsync start-intent
4. launch Codex
5. write and fsync process-start ack after exec has begun

Requeue after a dead worker is automatic only before start-intent, or when the
worker can durably prove Codex was never launched. `jobClaimed` without proof is
not enough if the launch protocol may have passed start-intent.

Provider DONE/EXIT can race shared filesystem visibility. DONE/EXIT/COMPLETING
enters a short `finalizing` state first. The coordinator scans terminal/event
files until the finalization grace expires; a valid terminal found during that
window is accepted and protected, not quarantined.

## Outbox Rules

Beacon outbox must use existing Teams outbox semantics:

- progress/status: `status-*` or `progress-*`, transient and upgrade non-blocking
- final answer: kind containing `final` or `answer`, protected
- artifacts: kind containing `artifact` or attachment metadata, protected
- quarantine or needs-attention: protected, not transient

Workers write result files and metadata. The active coordinator turns those records into Teams outbox entries.

Beacon should use fixed delivery classes or fixed kind constants verified
against real Teams outbox code. Do not rely on ad hoc future strings for
protected delivery.

Artifact ingestion is coordinator-owned. Workers may publish only artifact
metadata and per-job file references. They may not provide Teams
`AttachmentPath`, `DriveItemID`, or other delivery-ready fields. The coordinator
must open the artifact relative to the artifact root with no-follow semantics,
`fstat` the opened file, reject hardlinks, hash from the opened file descriptor,
stage from that same descriptor through the normal Teams outbound path, and
enqueue a protected outbox message. Missing, changed, too-large, out-of-root,
symlinked, hardlinked, or upload-failed artifacts become protected
needs-attention results.

Beacon needs-attention outbox uses a fixed protected kind such as
`final-beacon-needs-attention`. Beacon must not rely on `helper` plus
`NotificationKind=needs_attention` for upgrade blocking unless the Teams store
protected predicate is explicitly changed.

## Upgrade Compatibility

Helper reload, helper restart, helper binary upgrade, pending helper
replacement, pre-listen `--upgrade-codex`, and per-target beacon Codex upgrades
must all query a shared beacon blocker provider.

Block helper reload/restart/upgrade when any of these exist:

- active beacon job
- started beacon job without terminal
- protected beacon result/artifact outbox
- protocol-mismatch job that still needs reconciliation
- normal Teams running turns
- normal protected Teams outbox

Normal queued Teams turns follow the current helper behavior: they may be
preserved/deferred for helper binary restart when existing code supports that,
but they still block Codex upgrade paths that would change the execution binary
for queued work.

Do not block helper reload/restart/upgrade for:

- idle workers
- drained workers with no active jobs
- transient beacon status/progress messages

Force options may recover stale owners and drain idle workers only after beacon
reconciliation has classified every active marker. They must not bypass queued
or claimed work, started beacon jobs, protected beacon outbox,
protocol-mismatch reconciliation, ambiguous started work, or any marker whose
scheduler/claim/outbox state is not yet reconciled. Only a proven stale owner
marker with no provider job, no valid claim, and no protected outbox may be
forced through.

Upgrade blocker checks are operation-specific:

- helper reload/restart/pending replacement may preserve queued normal Teams
  turns when current helper behavior supports it
- helper reload/restart/pending replacement still blocks on running normal
  Teams turns, active beacon work, protected outbox, protocol mismatch, and
  unreconciled markers
- pre-listen `--upgrade-codex` and per-target Codex upgrade block queued or
  running work that would use the changed Codex target
- per-target Codex upgrade does not block on active jobs using other install
  targets

Codex upgrade must be scoped to one install target. It requires a managed persistent target, per-target lock, no active job on that target, and a maintenance worker or local maintenance path. Read-only images, unknown origins, fixed paths, and ephemeral overlays are not auto-upgraded.

Every beacon job snapshot records resolved Codex path, install origin, install
target id, image mutability, and helper/worker protocol. Codex upgrade requests
must name one target. Active jobs on the same target block the upgrade; active
jobs on other targets do not.

Install target resolution must handle wrappers and symlinks and must preserve
legacy default behavior for local/non-beacon upgrades. A target promotion is
staged, self-checked, version-verified, audited, and rollback-capable if the
promotion partially fails. Per-target locks survive coordinator restart.

## Machine Lifecycle

Lease states:

- starting
- accepting
- draining
- drained
- expired
- lost
- incompatible

Provider state projection:

- pending: allocation is waiting; do not mark worker lost
- running: lease may accept work after bootstrap doctor passes
- suspended: pause monitoring, block new claims, do not replay or cleanup
- finalizing: provider says completing/done/exit but terminal visibility is
  still inside grace window
- done with valid terminal: completed/drained
- done without terminal after process start: ambiguous/quarantine
- failed, preempted, requeued, node fail: requeue only before Codex start; otherwise ambiguous/quarantine

The scheduler is authoritative. A fresh worker heartbeat cannot keep a lease
alive if Slurm or LSF says the job is gone. If the scheduler is unknown, drain
conservatively and surface the uncertainty.

Raw provider mapping must preserve scheduler-specific state and reason text:

- Slurm: `PD`, `R`, `CG`, `CD`, `CA`, `F`, `TO`, `NF`, `OOM`, `PR`,
  `S/STOPPED`, and `REQUEUE` variants.
- LSF: `PEND`, `RUN`, `DONE`, `EXIT`, `SSUSP`, `USUSP`, `PSUSP`, `UNKWN`,
  `ZOMBI`, and PEND-after-RUN.

Suspended states are not heartbeat loss. They pause claims and monitoring
timeouts without replay. Resume to RUN restores monitoring and keeps the same
job snapshot unless the provider reports a new run incarnation.

Provider-only projection must not drive claim eligibility directly. `RUN` means
"allocation/job is running"; accepting still requires worker heartbeat,
membership proof, bootstrap doctor, protocol compatibility, signature match,
resource availability, and TTL budget.

LSF `PEND` after a previous `RUN` for the same provider job is not the same as
initial pending. Treat PEND-after-RUN as suspended or ambiguous depending on
started/process state and run incarnation. It must not become claimable pending
and must not trigger automatic replay after Codex may have started.

Runtime failure handling:

- allocation denied or invalid profile: fail the request with actionable text; no local fallback
- temporary scheduler failure: retry allocation with bounded backoff before worker start
- OOM before Codex process start: requeue is safe
- OOM, walltime kill, admin cancel, node reboot, or container kill after process start: ambiguous unless a valid terminal exists
- GPU OOM reported by Codex/tool with a valid terminal: failed turn, protected delivery
- shared disk full before claim: do not claim; show status/action
- disk full while writing terminal after process start: needs attention/ambiguous; do not replay
- terminal written before walltime/job end: accept and protect delivery

Cleanup rules:

- never delete live scheduler jobs
- never delete started ambiguous work
- tombstone before delete
- delete only after retention expires
- manual release drains by default and cancels only pending or idle allocations
- hard kill requires exact machine/lease/job id plus a confirmation token
- release/kill must list impacted chats and jobs before acting
- late terminal after kill is rejected or superseded according to fencing state

## User-Facing Error Policy

Every error shown in Teams, TUI, or CLI should be actionable and should include:

- phase: profile, allocation, bootstrap, queued, running, result, outbox, cleanup
- target: local or beacon profile
- provider state and job id when available
- affected conversation/job ids
- whether retry is automatic, safe manually, unsafe, or requires fork
- next command/action

No explicit beacon request should silently fall back to local execution. Status
views must include current target, pending target, turn snapshot, beacon job id,
lease/machine id, provider job id, profile, proxy route, shared/exclusive mode,
and raw provider reason when known.

Beacon progress messages must distinguish allocation pending, worker bootstrap,
Codex running, finalizing, and ambiguous-started states. Scheduler pending text
must show provider job id/reason and must not imply that a local Codex cancel is
the only remedy.

Failure messages need golden tests for bad proxy, invalid profile, incomplete
provider derivation, image digest mismatch, allocation denied, allocation
pending too long, disk full before start, disk full after start, missing shared
root, missing Codex, and ambiguous started work.

## Provider Notes

Slurm:

- provider profile records nodes, GPU count, partition, image, duration
- provider can use `submit_job --just_alloc` or equivalent allocation mode
- worker starts inside allocation/container and writes lease metadata to shared storage

LSF:

- provider profile records queue name
- provider derives host/job/container environment from LSF metadata
- direct SSH to system sshd is not a primary execution path because it may not run inside the allocation cgroup/environment
- system sshd execution is rejected unless job membership is proven; dropbear or
  an RPC worker launched inside the scheduler job is acceptable

## BYO Attach

Beacon supports managed allocation and BYO attach as separate modes.

Managed allocation means the coordinator submits and may cancel provider jobs.
BYO attach means the user is already inside `srun`, `submit_job`, `bsub`, `qsub`,
or equivalent and starts a worker from that environment.

Attach admission requires:

- same user and Teams scope/profile boundary
- verified scheduler environment such as `SLURM_JOB_ID` or `LSB_JOBID`
- provider query confirms ownership and RUN state
- host/cgroup/allocation membership proof
- shared root and doctor pass
- image/signature match
- worker identity challenge-response using the per-job attach capability

BYO release drains and deregisters the worker. It must not `scancel`/`bkill` or
kill an external allocation unless the user explicitly opted into provider
ownership. Unattested BYO workers are tainted: they cannot run Codex upgrades or
publish artifacts until attestation passes.

BYO attach records two separate facts:

- same scheduler user owns the provider job, which is required for attach
- coordinator owns or may cancel the provider job, which is false by default for
  BYO and true only for managed allocations or explicit opt-in

## Required Tests

Simulation tests must cover:

- default local target despite legacy proxy preference
- structured profile creation in Teams/TUI/CLI
- queued target snapshot across profile switch
- shared and exclusive reservations
- Slurm and LSF provider profile validation
- allocation submit crash after provider job creation but before provider id
  persistence
- provider RUN without worker heartbeat/doctor still not accepting
- Slurm allocation id vs step id vs run incarnation fencing
- provider membership proof rejecting login-node/system-sshd workers
- live worker recovery without interrupting the remote job
- valid terminal after coordinator restart
- idempotent exact duplicate terminal and quarantined conflicting duplicate
- active beacon jobs blocking helper/Codex upgrade
- helper reload/restart/pending replacement blocker matrix
- protected result outbox blocking upgrade
- transient progress not blocking upgrade
- real Teams outbox contract for final/artifact/needs-attention/progress/status
- active coordinator only dispatching, cleaning, and touching Teams outbox
- split-brain coordinator epoch/CAS rejection
- standby coordinator not dispatching
- worker never sending Teams outbox directly
- lease expiry, long-running jobs, hung workers, dead scheduler jobs, preemption, requeue, node failure
- multiple workers racing to claim jobs
- cleanup of tombstones and zombie files, including no immediate delete before
  tombstone and no path reuse
- allocation submitted/pending/running/canceled/expired transitions
- OOM, walltime kill, admin cancel, node reboot, disk full, and inode exhaustion
- ENOSPC during event append, terminal temp write, fsync, rename, quarantine
  marker, artifact staging, and outbox enqueue
- bootstrap doctor failures for missing shared root, missing Codex, bad proxy, image digest mismatch, and low disk
- bootstrap doctor failures for missing container runtime/modules/binds/auth
- provider-native state projection for pending, suspended, done, failed, preempted, requeued, and node fail
- Slurm and LSF raw state mapping tables
- finalizing grace for provider DONE/EXIT before shared terminal appears
- release and hard-kill confirmation semantics
- canonical execution signature mismatch matrix
- outbox delivery class compatibility with real Teams outbox recover/upgrade
- artifact symlink/hardlink/out-of-root/changed/too-large/missing/upload-failed
  ingestion failures
- Teams mobile duplicate command idempotency
- command-scope/wrong-chat behavior across Teams control chat and work chat
- draft profile list/resume/edit/delete/doctor/confirm UX
- exact failure message shape for common profile, allocation, bootstrap, result,
  and cleanup failures
- append-only audit log redaction and hash-chain tamper detection
